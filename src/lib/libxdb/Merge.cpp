// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "primitives/Primitives.h"
#include "operators/Operators.h"
#include "xdb/Merge.h"
#include "xdb/Xdb.h"
#include "xdb/MergeGvm.h"
#include "table/Table.h"
#include "gvm/Gvm.h"
#include "dag/Dag.h"
#include "util/TrackHelpers.h"
#include "operators/OperatorsXdbPageOps.h"
#include "libapis/LibApisRecv.h"
#include "xdb/XdbInt.h"
#include "usr/Users.h"

Status
Operators::mergeSetupColumns(const char *tableName,
                             XdbId xdbId,
                             XdbMeta **retXdbMeta,
                             MergeTableType mergeTableType,
                             TableMgr::ColumnInfoTable *columns)
{
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status;
    TableMgr::ColumnInfo *columnInfo = NULL;

    status = xdbMgr->xdbGet(xdbId, NULL, retXdbMeta);
    BailIfFailedMsg(moduleName,
                    status,
                    "Error getting table %s %u metadata: %s",
                    tableName,
                    mergeTableType,
                    strGetFromStatus(status));

    status =
        TableMgr::createColumnInfoTable(&(*retXdbMeta)->kvNamedMeta, columns);
    BailIfFailedMsg(moduleName,
                    status,
                    "Error creating column info for table %s %u: %s",
                    tableName,
                    mergeTableType,
                    strGetFromStatus(status));

    if (mergeTableType == MergeDeltaTable) {
        // Required to accomodate XcalarBatchIdColumnName
        if (columns->getSize() + 1 >= TupleMaxNumValuesPerRecord) {
            status = StatusFieldLimitExceeded;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to accomodate %s into internal IMD schema in "
                        "delta table %s: %s",
                        XcalarBatchIdColumnName,
                        tableName,
                        strGetFromStatus(status));

        // Verify RankOver
        columnInfo = columns->find(XcalarRankOverColumnName);
        if (!columnInfo) {
            status = StatusMissingXcalarRankOver;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to find %s column in delta table %s: %s",
                        XcalarRankOverColumnName,
                        tableName,
                        strGetFromStatus(status));

        if (columnInfo->getType() != DfInt64) {
            status = StatusTypeMismatch;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Delta table %s column %s type %s invalid: %s",
                        tableName,
                        XcalarRankOverColumnName,
                        strGetFromDfFieldType(columnInfo->getType()),
                        strGetFromStatus(status));

        // Verify Opcode column
        columnInfo = columns->find(XcalarOpCodeColumnName);
        if (!columnInfo) {
            status = StatusMissingXcalarOpCode;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to find %s column in delta table %s: %s",
                        XcalarOpCodeColumnName,
                        tableName,
                        strGetFromStatus(status));

        if (columnInfo->getType() != DfInt64) {
            status = StatusTypeMismatch;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Delta table %s column %s type %s invalid: %s",
                        tableName,
                        XcalarOpCodeColumnName,
                        strGetFromDfFieldType(columnInfo->getType()),
                        strGetFromStatus(status));
    } else {
        assert(mergeTableType == MergeTargetTable);

        // Disallow RankOver column
        columnInfo = columns->find(XcalarRankOverColumnName);
        if (columnInfo != NULL) {
            status = StatusDisallowedRankover;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Target table %s column %s disallowed: %s",
                        tableName,
                        XcalarRankOverColumnName,
                        strGetFromStatus(status));

        // Disallow Opcode column
        columnInfo = columns->find(XcalarOpCodeColumnName);
        if (columnInfo != NULL) {
            status = StatusDisallowedOpCode;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Target table %s column %s disallowed: %s",
                        tableName,
                        XcalarOpCodeColumnName,
                        strGetFromStatus(status));
    }

    // Disallow XcalarBatchId column
    columnInfo = columns->find(XcalarBatchIdColumnName);
    if (columnInfo != NULL) {
        status = StatusDisallowedBatchId;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Table %s column %s disallowed: %s",
                    tableName,
                    XcalarBatchIdColumnName,
                    strGetFromStatus(status));

    // Disallow FatPtrs in delta and target table
    for (TableMgr::ColumnInfoTable::iterator it = columns->begin();
         (columnInfo = it.get()) != NULL;
         it.next()) {
        if (columnInfo->getType() == DfFatptr) {
            status = StatusDisallowedFatPtr;
        }
        BailIfFailedMsg(moduleName,
                        status,
                        "Table %s column %s cannot have fatptrs: %s",
                        tableName,
                        columnInfo->getName(),
                        strGetFromStatus(status));
    }

CommonExit:
    return status;
}

Status
Operators::merge(const char *tableName[Operators::MergeNumTables],
                 XdbId xdbId[MergeNumTables],
                 TableNsMgr::IdHandle *idHandle[Operators::MergeNumTables])
{
    Status status;
    XdbMeta *xdbMeta[MergeNumTables] = {NULL};
    TableMgr::ColumnInfoTable columns[MergeNumTables];
    TableMgr::ColumnInfo *columnInfo[MergeNumTables] = {NULL};
    bool doPrepare = false;
    bool doCommit = false;

    status = mergeSetupColumns(tableName[MergeDeltaTable],
                               xdbId[MergeDeltaTable],
                               &xdbMeta[MergeDeltaTable],
                               MergeDeltaTable,
                               &columns[MergeDeltaTable]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Error getting delta table %s metadata, target table %s: "
                    "%s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    status = mergeSetupColumns(tableName[MergeTargetTable],
                               xdbId[MergeTargetTable],
                               &xdbMeta[MergeTargetTable],
                               MergeTargetTable,
                               &columns[MergeTargetTable]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Error getting delta table %s metadata, target table %s: "
                    "%s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    // DHT must match
    if (xdbMeta[MergeDeltaTable]->dhtId != xdbMeta[MergeTargetTable]->dhtId) {
        status = StatusDhtMismatch;
        xSyslog(moduleName,
                XlogErr,
                "Dht mismatch delta table %s, target table %s: %s",
                tableName[MergeDeltaTable],
                tableName[MergeTargetTable],
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Keys must match
    if (xdbMeta[MergeDeltaTable]->numKeys !=
        xdbMeta[MergeTargetTable]->numKeys) {
        status = StatusKeyMismatch;
        xSyslog(moduleName,
                XlogErr,
                "Num keys mismatch, delta table %s has %u keys, target table "
                "%s has %u keys: %s",
                tableName[MergeDeltaTable],
                xdbMeta[MergeDeltaTable]->numKeys,
                tableName[MergeTargetTable],
                xdbMeta[MergeTargetTable]->numKeys,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Accomodate XcalarRankOverColumnName & XcalarBatchIdColumnName into
    // internal IMD XDB schema created to manage IMD.
    if (xdbMeta[MergeDeltaTable]->numKeys + 2 >= TupleMaxNumValuesPerRecord) {
        status = StatusFieldLimitExceeded;
        xSyslog(moduleName,
                XlogErr,
                "Delta table %s has %u keys, cannot accomodate columns %s and "
                "%s as keys: %s",
                tableName[MergeDeltaTable],
                xdbMeta[MergeDeltaTable]->numKeys,
                XcalarRankOverColumnName,
                XcalarBatchIdColumnName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < xdbMeta[MergeDeltaTable]->numKeys; ii++) {
        if (strcmp(xdbMeta[MergeDeltaTable]->keyAttr[ii].name,
                   xdbMeta[MergeTargetTable]->keyAttr[ii].name) != 0) {
            status = StatusKeyNameMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Key idx %u mismatch, delta table %s key %s does not match "
                    "target table %s key %s: %s",
                    ii,
                    tableName[MergeDeltaTable],
                    xdbMeta[MergeDeltaTable]->keyAttr[ii].name,
                    tableName[MergeTargetTable],
                    xdbMeta[MergeTargetTable]->keyAttr[ii].name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (xdbMeta[MergeDeltaTable]->keyAttr[ii].type !=
            xdbMeta[MergeTargetTable]->keyAttr[ii].type) {
            status = StatusKeyTypeMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Key idx %u mismatch, delta table %s key %s type %s does "
                    "not match target table %s key %s type %s: %s",
                    ii,
                    tableName[MergeDeltaTable],
                    xdbMeta[MergeDeltaTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeDeltaTable]->keyAttr[ii].type),
                    tableName[MergeTargetTable],
                    xdbMeta[MergeTargetTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeTargetTable]->keyAttr[ii].type),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (xdbMeta[MergeDeltaTable]->keyAttr[ii].ordering !=
            xdbMeta[MergeTargetTable]->keyAttr[ii].ordering) {
            status = StatusOrderingMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Key idx %u mismatch, delta table %s key %s type %s "
                    "ordering %s does not match target table %s key %s type %s "
                    "ordering %s: %s",
                    ii,
                    tableName[MergeDeltaTable],
                    xdbMeta[MergeDeltaTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeDeltaTable]->keyAttr[ii].type),
                    strGetFromOrdering(
                        xdbMeta[MergeDeltaTable]->keyAttr[ii].ordering),
                    tableName[MergeTargetTable],
                    xdbMeta[MergeTargetTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeTargetTable]->keyAttr[ii].type),
                    strGetFromOrdering(
                        xdbMeta[MergeTargetTable]->keyAttr[ii].ordering),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (xdbMeta[MergeDeltaTable]->keyAttr[ii].ordering !=
                PartialAscending &&
            xdbMeta[MergeDeltaTable]->keyAttr[ii].ordering !=
                PartialDescending) {
            status = StatusOrderingNotSupported;
            xSyslog(moduleName,
                    XlogErr,
                    "Key idx %u unsupported ordering, delta table %s key %s "
                    "type %s ordering %s does not match target table %s key %s "
                    "type %s ordering %s: %s",
                    ii,
                    tableName[MergeDeltaTable],
                    xdbMeta[MergeDeltaTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeDeltaTable]->keyAttr[ii].type),
                    strGetFromOrdering(
                        xdbMeta[MergeDeltaTable]->keyAttr[ii].ordering),
                    tableName[MergeTargetTable],
                    xdbMeta[MergeTargetTable]->keyAttr[ii].name,
                    strGetFromDfFieldType(
                        xdbMeta[MergeTargetTable]->keyAttr[ii].type),
                    strGetFromOrdering(
                        xdbMeta[MergeTargetTable]->keyAttr[ii].ordering),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Delta and Target table schemas must match.
    // XXX Todo Later, we would want to allow Delta table schema to be a subset
    // of Target table.
    // XXX Todo Later, we would want to allow Target table schema evolution

    // OpcodeColumn and RankOver columns are only present in delta table
    if (columns[MergeDeltaTable].getSize() - 2 !=
        columns[MergeTargetTable].getSize()) {
        status = StatusInvalDeltaSchema;
        xSyslog(moduleName,
                XlogErr,
                "Invalid delta table %s column count %u and target table %s "
                "column count %u: %s",
                tableName[MergeDeltaTable],
                columns[MergeDeltaTable].getSize(),
                tableName[MergeTargetTable],
                columns[MergeTargetTable].getSize(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (TableMgr::ColumnInfoTable::iterator it =
             columns[MergeDeltaTable].begin();
         (columnInfo[MergeDeltaTable] = it.get()) != NULL;
         it.next()) {
        if (strcmp(columnInfo[MergeDeltaTable]->getName(),
                   XcalarOpCodeColumnName) == 0) {
            // OpcodeColumn is only present in delta table
            continue;
        }
        if (strcmp(columnInfo[MergeDeltaTable]->getName(),
                   XcalarRankOverColumnName) == 0) {
            // RankOverColumn is only present in delta table
            continue;
        }
        columnInfo[MergeTargetTable] = columns[MergeTargetTable].find(
            columnInfo[MergeDeltaTable]->getName());
        if (!columnInfo[MergeTargetTable]) {
            status = StatusColumnNameMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Delta table %s column %s not found in Target table %s: %s",
                    tableName[MergeDeltaTable],
                    columnInfo[MergeDeltaTable]->getName(),
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (columnInfo[MergeDeltaTable]->getIdxInTable() !=
            columnInfo[MergeTargetTable]->getIdxInTable()) {
            status = StatusColumnPositionMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Delta table %s column %s position %lu does not match "
                    "Target table %s column %s position %lu %s",
                    tableName[MergeDeltaTable],
                    columnInfo[MergeDeltaTable]->getName(),
                    columnInfo[MergeDeltaTable]->getIdxInTable(),
                    tableName[MergeTargetTable],
                    columnInfo[MergeTargetTable]->getName(),
                    columnInfo[MergeTargetTable]->getIdxInTable(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (columnInfo[MergeDeltaTable]->getType() !=
            columnInfo[MergeTargetTable]->getType()) {
            status = StatusTypeMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Delta table %s column %s type %s does not match Target "
                    "table %s column %s type %s %s",
                    tableName[MergeDeltaTable],
                    columnInfo[MergeDeltaTable]->getName(),
                    strGetFromDfFieldType(columnInfo[MergeDeltaTable]->getType()),
                    tableName[MergeTargetTable],
                    columnInfo[MergeTargetTable]->getName(),
                    strGetFromDfFieldType(columnInfo[MergeTargetTable]->getType()),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = mergeInit(tableName, xdbId, idHandle);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    doPrepare = true;
    TableMgr::ColumnInfoTable *colPtrs[MergeNumTables];
    colPtrs[MergeDeltaTable] = &columns[MergeDeltaTable];
    colPtrs[MergeTargetTable] = &columns[MergeTargetTable];
    status = mergePrepare(tableName, xdbId, idHandle, colPtrs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergePrepare delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    status = mergeCommit(tableName, idHandle);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeCommit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));
    doCommit = true;

    // Merge post commit failure is not a hard failure, since commit already
    // happened.
    mergePostCommit(idHandle[MergeTargetTable], PostCommitType::Inline);

CommonExit:

    if (!doCommit && doPrepare) {
        // Merge abort failure does not matter, since it just leaks resources.
        // Prepare artifacts can be cleaned out later.
        mergeAbort(tableName, xdbId, idHandle);
    }

    columns[MergeDeltaTable].removeAll(&TableMgr::ColumnInfo::destroy);
    columns[MergeTargetTable].removeAll(&TableMgr::ColumnInfo::destroy);
    return status;
}

MustCheck Status
Operators::mergeInit(const char *tableName[MergeNumTables],
                     XdbId xdbId[MergeNumTables],
                     TableNsMgr::IdHandle *idHandle[MergeNumTables])
{
    Status status;
    MergeGvm::MergeInitInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    TableNsMgr::IdHandle updateRecord;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->init);

    memcpy(&updateRecord,
           idHandle[MergeTargetTable],
           sizeof(TableNsMgr::IdHandle));

    updateRecord.nextVersion = updateRecord.nextVersion + 1;

    updateRecord = tnsMgr->updateNsObject(&updateRecord, status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target "
                    "table %s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    memcpy(idHandle[MergeTargetTable],
           &updateRecord,
           sizeof(TableNsMgr::IdHandle));

    if (updateRecord.consistentVersion > TableNsMgr::StartVersion) {
        // mergeInit must be already done
        goto CommonExit;
    }

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(MergeGvm::MergeInitInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed mergeInit delta table %s metadata, target table "
                  "%s: %s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed mergeInit delta table %s metadata, target table "
                  "%s: %s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    input = (MergeGvm::MergeInitInput *) gPayload->buf;
    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        status =
            input->tableInfo[ii].init(tableName[ii], idHandle[ii], xdbId[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergeInit delta table %s metadata, target "
                        "table %s: %s",
                        tableName[MergeDeltaTable],
                        tableName[MergeTargetTable],
                        strGetFromStatus(status));
    }

    gPayload->init(MergeGvm::get()->getGvmIndex(),
                   (uint32_t) MergeGvm::Action::InitMerge,
                   sizeof(MergeGvm::MergeInitInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->initFailure);
    }
    return status;
}

MustCheck Status
Operators::mergePrepare(const char *tableName[MergeNumTables],
                        XdbId xdbId[MergeNumTables],
                        TableNsMgr::IdHandle *idHandle[MergeNumTables],
                        TableMgr::ColumnInfoTable *columns[MergeNumTables])
{
    Status status;
    MergeGvm::MergePrepareInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    TableMgr *tmgr = TableMgr::get();
    TableMgr::ColumnInfoTable *imdCols = NULL;
    TableMgr::ColumnInfo *imdColInfo = NULL;
    TableMgr::TableObj *tableObj = NULL;

    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->prepare);

    if (XcalarConfig::get()->imdPreparePctFailure_) {
        uint64_t curPrepareStat =
            MergeMgr::get()->getStats()->prepare->statUint64;
        uint64_t pctVal =
            percent(curPrepareStat, XcalarConfig::get()->imdPreparePctFailure_);
        if (pctVal && !(curPrepareStat % pctVal)) {
            status = StatusFaultInjection;
            goto CommonExit;
        }
    }

    // Schema order of delta XDB must match with schema of IMD pending
    // and IMD commit XDB. If not, just fail the prepare.
    tableObj = tmgr->getTableObj(idHandle[MergeTargetTable]->tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed mergePrepare delta table %s metadata, target table %s: "
                "%s",
                tableName[MergeDeltaTable],
                tableName[MergeTargetTable],
                strGetFromStatus(status));
        goto CommonExit;
    }

    imdCols = tableObj->getImdColTable();

    for (auto iter = imdCols->begin(); (imdColInfo = iter.get()) != NULL;
         iter.next()) {
        const char *colName = imdColInfo->getName();
        if (strcmp(colName, XcalarBatchIdColumnName) == 0) {
            // BatchId column will be available only in the IMD Xdb.
            continue;
        }

        TableMgr::ColumnInfo *colInfo = columns[MergeDeltaTable]->find(colName);
        if (colInfo == NULL) {
            status = StatusInvalDeltaSchema;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed mergePrepare delta table %s metadata, target table "
                    "%s, column %s not found in delta table: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    colName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (colInfo->getIdxInTable() != imdColInfo->getIdxInTable()) {
            status = StatusInvalDeltaSchema;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed mergePrepare delta table %s metadata, target table "
                    "%s, column %s order %lu in delta table does not match "
                    "order %ld in IMD table: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    colName,
                    colInfo->getIdxInTable(),
                    imdColInfo->getIdxInTable(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (colInfo->getType() != imdColInfo->getType()) {
            status = StatusTypeMismatch;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed mergePrepare delta table %s metadata, target table "
                    "%s, column %s type %s in delta table does not match "
                    "type %s in IMD table: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    colName,
                    strGetFromDfFieldType(colInfo->getType()),
                    strGetFromDfFieldType(imdColInfo->getType()),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(MergeGvm::MergePrepareInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed mergePrepare delta table %s metadata, target table "
                  "%s: %s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed mergePrepare delta table %s metadata, target table "
                  "%s: %s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    input = (MergeGvm::MergePrepareInput *) gPayload->buf;

    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        status =
            input->tableInfo[ii].init(tableName[ii], idHandle[ii], xdbId[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergePrepare delta table %s metadata, target "
                        "table %s: %s",
                        tableName[MergeDeltaTable],
                        tableName[MergeTargetTable],
                        strGetFromStatus(status));
    }

    gPayload->init(MergeGvm::get()->getGvmIndex(),
                   (uint32_t) MergeGvm::Action::PrepareMerge,
                   sizeof(MergeGvm::MergePrepareInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergePrepare delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->prepareFailure);
    }
    return status;
}

MustCheck Status
Operators::mergeCommit(const char *tableName[MergeNumTables],
                       TableNsMgr::IdHandle *idHandle[MergeNumTables])
{
    Status status;
    TableNsMgr::IdHandle updateRecord;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->commit);

    if (XcalarConfig::get()->imdCommitPctFailure_) {
        uint64_t curCommitStat =
            MergeMgr::get()->getStats()->commit->statUint64;
        uint64_t pctVal =
            percent(curCommitStat, XcalarConfig::get()->imdCommitPctFailure_);
        if (pctVal && !(curCommitStat % pctVal)) {
            status = StatusFaultInjection;
            goto CommonExit;
        }
    }

    memcpy(&updateRecord,
           idHandle[MergeTargetTable],
           sizeof(TableNsMgr::IdHandle));

    updateRecord.consistentVersion = updateRecord.nextVersion - 1;
    updateRecord.mergePending = true;

    updateRecord = tnsMgr->updateNsObject(&updateRecord, status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeCommit delta table %s metadata, target "
                    "table %s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    memcpy(idHandle[MergeTargetTable],
           &updateRecord,
           sizeof(TableNsMgr::IdHandle));

CommonExit:
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->commitFailure);
    }
    return status;
}

Status
Operators::mergePostCommit(TableNsMgr::IdHandle *idHandle,
                           PostCommitType pcType)
{
    Status status;
    MergeGvm::MergePostCommitInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    bool revertIdHandle = false;
    TableNsMgr::IdHandle curIdHandle;
    TableNsMgr::IdHandle updateRecord;
    Dag *sessionGraph = NULL;
    DagTypes::NodeId targetNodeId;
    memcpy(&curIdHandle, idHandle, sizeof(TableNsMgr::IdHandle));

    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->postCommit);

    if (XcalarConfig::get()->imdPostCommitPctFailure_ &&
        pcType == PostCommitType::Inline) {
        uint64_t curPostCommitStat =
            MergeMgr::get()->getStats()->postCommit->statUint64;
        uint64_t pctVal =
            percent(curPostCommitStat,
                    XcalarConfig::get()->imdPostCommitPctFailure_);
        if (pctVal && !(curPostCommitStat % pctVal)) {
            status = StatusFaultInjection;
            goto CommonExit;
        }
    }

    // For merge post commit, if the Ns lock is not Excl, it needs to be
    // promoted.
    if (curIdHandle.nsHandle.openFlags != LibNsTypes::WriterExcl) {
        revertIdHandle = true;

        // XXX TODO, Add a native libNs lock promote interface instead of
        // separate libNs->close and libNs->open calls.
        tnsMgr->closeHandleToNs(&curIdHandle);

        status = tnsMgr->openHandleToNs(&curIdHandle.sessionContainer,
                                        curIdHandle.tableId,
                                        LibNsTypes::WriterExcl,
                                        &curIdHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergePostCommit target table %lu: %s",
                        curIdHandle.tableId,
                        strGetFromStatus(status));
    }

    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(MergeGvm::MergePostCommitInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed mergePostCommit target table %lu: %s",
                  curIdHandle.tableId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed mergePostCommit target table %lu: %s",
                  curIdHandle.tableId,
                  strGetFromStatus(status));

    input = (MergeGvm::MergePostCommitInput *) gPayload->buf;
    memcpy(&input->idHandle, &curIdHandle, sizeof(TableNsMgr::IdHandle));

    gPayload->init(MergeGvm::get()->getGvmIndex(),
                   (uint32_t) MergeGvm::Action::PostCommitMerge,
                   sizeof(MergeGvm::MergePostCommitInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergePostCommit target table %lu: %s",
                    curIdHandle.tableId,
                    strGetFromStatus(status));

    memcpy(&updateRecord, &curIdHandle, sizeof(TableNsMgr::IdHandle));
    updateRecord.mergePending = false;

    updateRecord = tnsMgr->updateNsObject(&updateRecord, status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergePostCommit target table %lu: %s",
                    curIdHandle.tableId,
                    strGetFromStatus(status));

    memcpy(&curIdHandle, &updateRecord, sizeof(TableNsMgr::IdHandle));

    // If lock was promoted here, needs to be demoted.
    if (revertIdHandle) {
        // XXX TODO Add libNs support to demote lock.
        tnsMgr->closeHandleToNs(&curIdHandle);

        // Revert to the original open flag
        status = tnsMgr->openHandleToNs(&curIdHandle.sessionContainer,
                                        curIdHandle.tableId,
                                        idHandle->nsHandle.openFlags,
                                        &curIdHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergePostCommit target table %lu: %s",
                        curIdHandle.tableId,
                        strGetFromStatus(status));
    }

    // XXX ENG-8502 update the target table object with latest information
    // also no need to make another 2PC to get the op details;
    // postCommit output should have this information
    status = UserMgr::get()
                 ->getDag(&curIdHandle.sessionContainer.userId,
                          curIdHandle.sessionContainer.sessionInfo.sessionName,
                          &sessionGraph);
    BailIfFailed(status);

    status =
        sessionGraph->getNodeIdFromTableId(curIdHandle.tableId, &targetNodeId);
    DCHECK(status.ok());
    BailIfFailed(status);

    status = sessionGraph
                 ->updateOpDetails(targetNodeId,
                                   NULL,
                                   &curIdHandle.sessionContainer.userId,
                                   &curIdHandle.sessionContainer.sessionInfo);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to updateOpDetails target table %lu: %s",
                    curIdHandle.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->postCommitFailure);
    }

    memcpy(idHandle, &curIdHandle, sizeof(TableNsMgr::IdHandle));

    return status;
}

void
Operators::mergeAbort(const char *tableName[MergeNumTables],
                      XdbId xdbId[MergeNumTables],
                      TableNsMgr::IdHandle *idHandle[MergeNumTables])
{
    Status status;
    MergeGvm::MergeAbortInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->abort);

    if (XcalarConfig::get()->imdAbortPctFailure_) {
        uint64_t curAbortStat = MergeMgr::get()->getStats()->abort->statUint64;
        uint64_t pctVal =
            percent(curAbortStat, XcalarConfig::get()->imdAbortPctFailure_);
        if (pctVal && !(curAbortStat % pctVal)) {
            status = StatusFaultInjection;
            goto CommonExit;
        }
    }

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(MergeGvm::MergeAbortInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed mergeAbort delta table %s metadata, target table %s: "
                  "%s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed mergeAbort delta table %s metadata, target table %s: "
                  "%s",
                  tableName[MergeDeltaTable],
                  tableName[MergeTargetTable],
                  strGetFromStatus(status));

    input = (MergeGvm::MergeAbortInput *) gPayload->buf;

    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        status =
            input->tableInfo[ii].init(tableName[ii], idHandle[ii], xdbId[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergeAbort delta table %s metadata, target "
                        "table %s: %s",
                        tableName[MergeDeltaTable],
                        tableName[MergeTargetTable],
                        strGetFromStatus(status));
    }

    gPayload->init(MergeGvm::get()->getGvmIndex(),
                   (uint32_t) MergeGvm::Action::AbortMerge,
                   sizeof(MergeGvm::MergeAbortInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeAbort delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

CommonExit:
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->abortFailure);
    }
}

class MergeWorkBase : public Schedulable
{
  public:
    MergeWorkBase(TrackHelpers::WorkerType workerTypeIn,
                  unsigned workerIdIn,
                  TrackHelpers *trackHelpers,
                  Xdb *targetXdb,
                  XdbMeta *targetXdbMeta,
                  MergeMgr::MergeInfo *mergeInfo,
                  VersionId consistentVersion,
                  VersionId nextVersion,
                  TableMgr::ColumnInfoTable *colTable,
                  TableMgr::ColumnInfoTable *imdColTable)
        : Schedulable("MergeWorkBase"),
          trackHelpers_(trackHelpers),
          workerType_(workerTypeIn),
          workerId_(workerIdIn),
          targetXdb_(targetXdb),
          targetXdbMeta_(targetXdbMeta),
          mergeInfo_(mergeInfo),
          consistentVersion_(consistentVersion),
          nextVersion_(nextVersion),
          colTable_(colTable),
          imdColTable_(imdColTable)
    {
        imdCommittedXdb_ = mergeInfo->getImdCommittedXdb();
        imdCommittedXdbMeta_ = mergeInfo->getImdCommittedXdbMeta();
        imdPendingXdb_ = mergeInfo->getImdPendingXdb();
        imdPendingXdbMeta_ = mergeInfo->getImdPendingXdbMeta();

        targetKvMeta_ = &targetXdbMeta_->kvNamedMeta.kvMeta_;

        imdPendingKvMeta_ = &imdPendingXdbMeta_->kvNamedMeta.kvMeta_;
        imdCommittedKvMeta_ = &imdCommittedXdbMeta_->kvNamedMeta.kvMeta_;

        batchIdx_ = imdColTable->find(XcalarBatchIdColumnName)->getIdxInTable();
        opCodeIdx_ = imdColTable->find(XcalarOpCodeColumnName)->getIdxInTable();
        rankOverIdx_ =
            imdColTable->find(XcalarRankOverColumnName)->getIdxInTable();
        batchIdValue_.int64Val = nextVersion_;
    }

  protected:
    TrackHelpers *trackHelpers_;
    TrackHelpers::WorkerType workerType_;
    unsigned workerId_;

    Xdb *targetXdb_;
    XdbMeta *targetXdbMeta_;
    MergeMgr::MergeInfo *mergeInfo_;
    VersionId consistentVersion_;
    VersionId nextVersion_;

    Xdb *imdCommittedXdb_;
    XdbMeta *imdCommittedXdbMeta_;
    Xdb *imdPendingXdb_;
    XdbMeta *imdPendingXdbMeta_;
    TableMgr::ColumnInfoTable *colTable_;
    TableMgr::ColumnInfoTable *imdColTable_;

    NewKeyValueMeta *targetKvMeta_;
    NewKeyValueMeta *imdPendingKvMeta_;
    NewKeyValueMeta *imdCommittedKvMeta_;
    uint64_t batchIdx_;
    uint64_t opCodeIdx_;
    uint64_t rankOverIdx_;

    DfFieldValue batchIdValue_;

    void transImdPendingToCommitted(uint64_t slot);
};

void
MergeWorkBase::transImdPendingToCommitted(uint64_t slot)
{
    XdbMgr *xdbMgr = XdbMgr::get();

    XdbAtomicHashSlot *imdCommittedSlot =
        &imdCommittedXdb_->hashSlotInfo.hashBase[slot];
    XdbAtomicHashSlot *imdPendingSlot =
        &imdPendingXdb_->hashSlotInfo.hashBase[slot];
    XdbHashSlotAug *imdPendingAugSlot =
        &imdPendingXdb_->hashSlotInfo.hashBaseAug[slot];
    XdbHashSlotAug *imdCommittedAugSlot =
        &imdCommittedXdb_->hashSlotInfo.hashBaseAug[slot];

    XdbPage *imdPendingPage = xdbMgr->getXdbHashSlotNextPage(imdPendingSlot);
    if (!imdPendingPage) {
        return;
    }

    xdbMgr->clearSlot(imdPendingSlot);
    xdbMgr->setXdbHashSlotNextPage(imdCommittedSlot,
                                   imdCommittedXdb_->getXdbId(),
                                   slot,
                                   imdPendingPage);
    imdCommittedSlot->sortFlag = 0;

    if (!imdCommittedAugSlot->keyRangeValid) {
        imdCommittedAugSlot->minKey = imdPendingAugSlot->minKey;
        imdCommittedAugSlot->maxKey = imdPendingAugSlot->maxKey;
        imdCommittedAugSlot->keyRangeValid = true;
    } else {
        DataFormat::updateFieldValueRange(imdPendingXdbMeta_->keyAttr[0].type,
                                          imdPendingAugSlot->minKey,
                                          &imdCommittedAugSlot->minKey,
                                          &imdCommittedAugSlot->maxKey,
                                          DontHashString,
                                          DfFieldValueRangeValid);

        DataFormat::updateFieldValueRange(imdPendingXdbMeta_->keyAttr[0].type,
                                          imdPendingAugSlot->maxKey,
                                          &imdCommittedAugSlot->minKey,
                                          &imdCommittedAugSlot->maxKey,
                                          DontHashString,
                                          DfFieldValueRangeValid);
    }

    imdCommittedAugSlot->numRows += imdPendingAugSlot->numRows;
    imdCommittedAugSlot->numPages += imdPendingAugSlot->numPages;

    // Reset IMD pending XDB slot
    new (imdPendingAugSlot) XdbHashSlotAug();
}

class MergePrepareWork : public MergeWorkBase
{
  public:
    MergePrepareWork(TrackHelpers::WorkerType workerTypeIn,
                     unsigned workerIdIn,
                     TrackHelpers *trackHelpers,
                     Xdb *deltaXdb,
                     XdbMeta *deltaXdbMeta,
                     Xdb *targetXdb,
                     XdbMeta *targetXdbMeta,
                     MergeMgr::MergeInfo *mergeInfo,
                     OpKvEntryCopyMapping *mapping,
                     VersionId consistentVersion,
                     VersionId nextVersion,
                     TableMgr::ColumnInfoTable *colTable,
                     TableMgr::ColumnInfoTable *imdColTable)
        : MergeWorkBase(workerTypeIn,
                        workerIdIn,
                        trackHelpers,
                        targetXdb,
                        targetXdbMeta,
                        mergeInfo,
                        consistentVersion,
                        nextVersion,
                        colTable,
                        imdColTable),
          deltaXdb_(deltaXdb),
          deltaXdbMeta_(deltaXdbMeta),
          mapping_(mapping)
    {
        deltaKvMeta_ = &deltaXdbMeta->kvNamedMeta.kvMeta_;
        new (&deltaKvEntry_)
            NewKeyValueEntry(&deltaXdbMeta->kvNamedMeta.kvMeta_);
        new (&imdPendingKvEntry_) NewKeyValueEntry(
            &mergeInfo->getImdPendingXdbMeta()->kvNamedMeta.kvMeta_);
    }
    virtual void run();
    virtual void done() { delete this; }

  private:
    Xdb *deltaXdb_;
    XdbMeta *deltaXdbMeta_;
    OpKvEntryCopyMapping *mapping_;
    NewKeyValueMeta *deltaKvMeta_;
    NewKeyValueEntry deltaKvEntry_;
    NewKeyValueEntry imdPendingKvEntry_;

    Status setup() { return StatusOk; }
    void tearDown() {}
    Status processSlot(uint64_t slot);
    Status insertOnePage(XdbPage *xdbPage, uint64_t slot);
};

void
MergePrepareWork::run()
{
    Status status = StatusOk;
    status = trackHelpers_->helperStart();

    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = setup();
    // if setup fails we want to go ahead and mark all work items as complete
    // then report a bad status at the end

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        status = processSlot(ii);

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    tearDown();
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
MergePrepareWork::processSlot(uint64_t slot)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbAtomicHashSlot *deltaHashSlot = &deltaXdb_->hashSlotInfo.hashBase[slot];
    XdbPage *deltaPage = XdbMgr::getXdbHashSlotNextPage(deltaHashSlot);
    XdbAtomicHashSlot *imdPendingSlot =
        &imdPendingXdb_->hashSlotInfo.hashBase[slot];

    // If there are any records in IMD pending XDB, check if it's need to be
    // committed into IMD committed XDB or discarded.
    XdbPage *imdPendingPage = xdbMgr->getXdbHashSlotNextPage(imdPendingSlot);
    if (imdPendingPage != NULL &&
        mergeInfo_->getImdPendingVersionNum() == consistentVersion_) {
        transImdPendingToCommitted(slot);
    } else {
        xdbMgr->xdbDropSlot(imdPendingXdb_, slot, true);
    }

    while (deltaPage) {
        status = insertOnePage(deltaPage, slot);
        BailIfFailed(status);

        deltaPage = (XdbPage *) deltaPage->hdr.nextPage;
    }

CommonExit:
    return status;
}

Status
MergePrepareWork::insertOnePage(XdbPage *deltaXdbPage, uint64_t slot)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numFields = imdPendingXdb_->tupMeta.getNumFields();
    DfFieldValue key;
    bool isKeyValid = false;
    bool refGrabbed = false;

    XdbAtomicHashSlot *imdPendingSlot =
        &imdPendingXdb_->hashSlotInfo.hashBase[slot];
    XdbHashSlotAug *imdPendingSlotAug =
        &imdPendingXdb_->hashSlotInfo.hashBaseAug[slot];

    status = deltaXdbPage->getRef(deltaXdb_);
    if (status != StatusOk) {
        return status;
    }
    refGrabbed = true;

    NewTuplesCursor tupCursor(deltaXdbPage->tupBuf);

    while ((status = tupCursor.getNext(deltaKvMeta_->tupMeta_,
                                       &deltaKvEntry_.tuple_)) == StatusOk) {
        // XXX TODO This condition is insufficient b/c Tables can have keys
        // composed of multiple columns
        key = deltaKvEntry_.getKey(&isKeyValid);
        // XXX TODO Add below integrity constraints with Merge prepare phase.
        // * IMD record key needs to be valid.
        // * IMD rank over field needs to be valid.
        // * IMD rank over field must be unique.
        if (!isKeyValid) {
            status = StatusInvalTableKeys;
            goto CommonExit;
        }

        imdPendingKvEntry_.init();
        Operators::shallowCopyKvEntry(&imdPendingKvEntry_,
                                      numFields,
                                      &deltaKvEntry_,
                                      mapping_,
                                      0,
                                      mapping_->numEntries);
        imdPendingKvEntry_.tuple_.set(batchIdx_, batchIdValue_, DfInt64);

        status = xdbMgr->xdbInsertKvCommon(imdPendingXdb_,
                                           &key,
                                           &imdPendingKvEntry_.tuple_,
                                           imdPendingSlot,
                                           imdPendingSlotAug,
                                           slot);
        BailIfFailed(status);
    }
    BailIf(status != StatusOk && status != StatusNoData);
    status = StatusOk;

CommonExit:
    if (refGrabbed) {
        xdbMgr->pagePutRef(deltaXdb_, slot, deltaXdbPage);
        refGrabbed = false;
    }
    return status;
}

Status
Operators::mergeInitLocal(const char *tableName[MergeNumTables],
                          TableNsMgr::IdHandle *idHandle[MergeNumTables],
                          XdbId xdbId[MergeNumTables])
{
    Status status;
    TableMgr *tmgr = TableMgr::get();
    Xdb *xdb[MergeNumTables] = {NULL};
    XdbMeta *xdbMeta[MergeNumTables] = {NULL};
    XdbMgr *xdbMgr = XdbMgr::get();
    TableNsMgr::TableId tableId[MergeNumTables] = {TableNsMgr::InvalidTableId};
    MergeMgr::MergeInfo *mergeInfo = NULL;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);
    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->initLocal);

    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        // Delta table cannot be dropped, because the source node grabs a shared
        // lock on the table
        // Target table cannot be dropped, because the source node grabs an Excl
        // lock on the table
        status = xdbMgr->xdbGet(xdbId[ii], &xdb[ii], &xdbMeta[ii]);
        assert(status == StatusOk);
        tableId[ii] = idHandle[ii]->tableId;
    }

    TableMgr::TableObj *tableObj =
        tmgr->getTableObj(idHandle[MergeTargetTable]->tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    status = tableObj->initColTable(xdbMeta[MergeTargetTable]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));

    if (!tableObj->getMergeInfo()) {
        mergeInfo = new (std::nothrow) MergeMgr::MergeInfo();
        BailIfNullMsg(mergeInfo,
                      StatusNoMem,
                      moduleName,
                      "Failed mergeInit delta table %s metadata, target table "
                      "%s: %s",
                      tableName[MergeDeltaTable],
                      tableName[MergeTargetTable],
                      strGetFromStatus(status));

        status = mergeInfo->init(xdb[MergeDeltaTable], xdb[MergeTargetTable]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed mergeInit delta table %s metadata, target "
                        "table "
                        "%s: %s",
                        tableName[MergeDeltaTable],
                        tableName[MergeTargetTable],
                        strGetFromStatus(status));

        tableObj->setMergeInfo(mergeInfo);
        mergeInfo = NULL;
    }

    status = tableObj->initImdColTable(
        tableObj->getMergeInfo()->getImdCommittedXdbMeta());
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergeInit delta table %s metadata, target table "
                    "%s: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    strGetFromStatus(status));
CommonExit:
    if (mergeInfo) {
        delete mergeInfo;
        mergeInfo = NULL;
    }
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->initLocalFailure);
    }
    logOperationStage(__func__, status, stopwatch, End);
    return status;
}

Status
Operators::mergePrepareLocal(const char *tableName[MergeNumTables],
                             TableNsMgr::IdHandle *idHandle[MergeNumTables],
                             XdbId xdbId[MergeNumTables])
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    TrackHelpers *trackHelpers = NULL;
    OpKvEntryCopyMapping *mapping = NULL;
    unsigned imdNumFields = 0;
    XdbMgr *xdbMgr = XdbMgr::get();
    VersionId consistentVersion = idHandle[MergeTargetTable]->consistentVersion;
    VersionId nextVersion = idHandle[MergeTargetTable]->nextVersion - 1;
    Xdb *xdb[MergeNumTables] = {NULL};
    XdbMeta *xdbMeta[MergeNumTables] = {NULL};
    TableMgr *tmgr = TableMgr::get();
    MergePrepareWork **scheds = NULL;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);
    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->prepareLocal);

    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        // Delta table cannot be dropped, because the source node grabs a shared
        // lock on the table.
        // Target table cannot be dropped, because the source node grabs an Excl
        // lock on the table.
        status = xdbMgr->xdbGet(xdbId[ii], &xdb[ii], &xdbMeta[ii]);
        assert(status == StatusOk);
    }

    unsigned numScheds =
        Operators::getNumWorkers(xdb[MergeDeltaTable]->numRows);

    // each slot is a chunk
    uint64_t numChunks = xdb[MergeDeltaTable]->hashSlotInfo.hashSlots;

    TableMgr::ColumnInfoTable *colTable = NULL;
    TableMgr::ColumnInfoTable *colImdTable = NULL;
    MergeMgr::MergeInfo *mergeInfo = NULL;
    TableMgr::TableObj *tableObj =
        tmgr->getTableObj(idHandle[MergeTargetTable]->tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed mergePrepare delta table %s metadata, target table %s "
                "for version %lu, %lu: %s",
                tableName[MergeDeltaTable],
                tableName[MergeTargetTable],
                consistentVersion,
                nextVersion,
                strGetFromStatus(status));
        goto CommonExit;
    }

    colTable = tableObj->getColTable();
    colImdTable = tableObj->getImdColTable();
    mergeInfo = tableObj->getMergeInfo();
    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    imdNumFields = mergeInfo->getImdPendingXdbMeta()->getNumFields();
    mapping = Operators::getOpKvEntryCopyMapping(imdNumFields);
    BailIfNull(mapping);

    Operators::initKvEntryCopyMapping(mapping,
                                      xdbMeta[MergeDeltaTable],
                                      mergeInfo->getImdPendingXdbMeta(),
                                      0,
                                      imdNumFields - 1,
                                      true,
                                      NULL,
                                      0);

    scheds = new (std::nothrow) MergePrepareWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            MergePrepareWork(ii == 0 ? TrackHelpers::Master
                                     : TrackHelpers::NonMaster,
                             ii,
                             trackHelpers,
                             xdb[MergeDeltaTable],
                             xdbMeta[MergeDeltaTable],
                             xdb[MergeTargetTable],
                             xdbMeta[MergeTargetTable],
                             mergeInfo,
                             mapping,
                             consistentVersion,
                             nextVersion,
                             colTable,
                             colImdTable);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "mergePrepareLocal operation failed for delta table %s, "
                    "target table %s for version %lu, %lu: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    consistentVersion,
                    nextVersion,
                    strGetFromStatus(status));

    // Set the version number of Records in IMD pending Xdb.
    mergeInfo->setImdPendingVersionNum(nextVersion);

    Xdb *imdPendingXdb;
    imdPendingXdb = mergeInfo->getImdPendingXdb();
    mergeInfo->resetImdXdbForLoadDone(imdPendingXdb);
    status = xdbMgr->xdbLoadDoneScratchXdb(imdPendingXdb);
    BailIfFailedMsg(moduleName,
                    status,
                    "mergePrepareLocal operation failed for delta table %s, "
                    "target table %s for version %lu, %lu: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    consistentVersion,
                    nextVersion,
                    strGetFromStatus(status));

CommonExit:
    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (mapping != NULL) {
        memFree(mapping);
        mapping = NULL;
    }

    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->prepareLocalFailure);
    }

    logOperationStage(__func__, status, stopwatch, End);

    return status;
}

class MergePostCommitWork : public MergeWorkBase
{
  public:
    MergePostCommitWork(TrackHelpers::WorkerType workerTypeIn,
                        unsigned workerIdIn,
                        TrackHelpers *trackHelpers,
                        Xdb *targetXdb,
                        XdbMeta *targetXdbMeta,
                        MergeMgr::MergeInfo *mergeInfo,
                        VersionId consistentVersion,
                        VersionId nextVersion,
                        TableMgr::ColumnInfoTable *colTable,
                        TableMgr::ColumnInfoTable *imdColTable)
        : MergeWorkBase(workerTypeIn,
                        workerIdIn,
                        trackHelpers,
                        targetXdb,
                        targetXdbMeta,
                        mergeInfo,
                        consistentVersion,
                        nextVersion,
                        colTable,
                        imdColTable)
    {
        new (&tgtKvEntry_)
            NewKeyValueEntry(&targetXdbMeta->kvNamedMeta.kvMeta_);
        new (&resultKvEntry_)
            NewKeyValueEntry(&targetXdbMeta->kvNamedMeta.kvMeta_);
        new (&imdCommKvEntry_) NewKeyValueEntry(
            &mergeInfo->getImdCommittedXdbMeta()->kvNamedMeta.kvMeta_);
        new (&imdPrevEntry_) NewKeyValueEntry(
            &mergeInfo->getImdCommittedXdbMeta()->kvNamedMeta.kvMeta_);
    }

    virtual void run();
    virtual void done() { delete this; }

  private:
    NewKeyValueEntry tgtKvEntry_;
    NewKeyValueEntry resultKvEntry_;
    NewKeyValueEntry imdCommKvEntry_;
    NewKeyValueEntry imdPrevEntry_;
    uint64_t resultNumRows_ = 0;
    uint64_t resultNumPages_ = 0;
    DfFieldValue resultMinKey_;
    DfFieldValue resultMaxKey_;
    bool resultKeyRangeValid_ = false;

    XdbPage *xdbPagesAllocBatch_[XdbMgr::BatchAllocSize] = {NULL};
    int curXdbPageAllocIdx_ = -1;
    unsigned curNumXdbPagesAlloc_ = 0;
    unsigned curNumAllocBatches_ = 0;

    XdbPage *xdbPagesFreeBatch_[XdbMgr::BatchFreeSize] = {NULL};
    int curXdbPageFreeIdx_ = -1;

    Status setup() { return StatusOk; }
    void tearDown() {}
    MustCheck Status processSlot(uint64_t slot);
    MustCheck Status implementImd(uint64_t slot);
    MustCheck Status processResult(NewKeyValueEntry *resultKvEntry,
                                   DfFieldValue opCode,
                                   XdbPage **resultPages);
};

void
MergePostCommitWork::run()
{
    Status status = StatusOk;
    status = trackHelpers_->helperStart();

    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = setup();
    // if setup fails we want to go ahead and mark all work items as complete
    // then report a bad status at the end

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        status = processSlot(ii);

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    tearDown();
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
MergePostCommitWork::processResult(NewKeyValueEntry *resultKvEntry,
                                   DfFieldValue opCode,
                                   XdbPage **resultPages)
{
    Status status;
    bool retValid;
    DfFieldValue key = resultKvEntry->getKey(&retValid);
    assert(retValid && "Key must be valid");
    XdbMgr *xdbMgr = XdbMgr::get();
    int prevXdbPageIdx = curXdbPageAllocIdx_;

    // Process DeleteOpCode
    if (opCode.uint64Val == DeleteOpCode.int64Val) {
        // skip rows that have been marked as deleted
        goto CommonExit;
    }

    while (true) {
        if (curXdbPageAllocIdx_ == -1) {
            curNumXdbPagesAlloc_ =
                xdbMgr->xdbAllocXdbPageBatch(xdbPagesAllocBatch_,
                                             XdbMgr::BatchAllocSize,
                                             Txn::currentTxn().id_,
                                             targetXdb_,
                                             XdbMgr::SlabHint::Default,
                                             BcHandle::BcScanCleanoutToFree);
            if (!curNumXdbPagesAlloc_) {
                status = StatusNoXdbPageBcMem;
                goto CommonExit;
            }
            curNumAllocBatches_++;
            curXdbPageAllocIdx_ = 0;
        }

        status = opInsertPageChainBatch(xdbPagesAllocBatch_,
                                        curNumXdbPagesAlloc_,
                                        curXdbPageAllocIdx_,
                                        &curXdbPageAllocIdx_,
                                        targetKvMeta_,
                                        &resultKvEntry->tuple_,
                                        targetXdb_);
        if (status == StatusNoXdbPageBcMem) {
            prevXdbPageIdx = -1;
            curXdbPageAllocIdx_ = -1;
            continue;
        } else if (status != StatusOk) {
            goto CommonExit;
        } else {
            if (prevXdbPageIdx != curXdbPageAllocIdx_) {
#ifdef DEBUG
                uint64_t rows = 0;
                uint64_t pages = 0;
                XdbPage *cur = *resultPages;
                while (cur != NULL) {
                    rows += cur->hdr.numRows;
                    pages++;
                    cur = cur->hdr.nextPage;
                }
                assert(rows == resultNumRows_ && pages == resultNumPages_);
#endif  // DEBUG
                assert(curXdbPageAllocIdx_ ==
                       (prevXdbPageIdx + 1) % (int) curNumXdbPagesAlloc_);
                xdbPagesAllocBatch_[curXdbPageAllocIdx_]->hdr.nextPage =
                    *resultPages;
                *resultPages = xdbPagesAllocBatch_[curXdbPageAllocIdx_];
                resultNumPages_++;
            }
        }
        break;
    }

    // Process InsertOpCode and UpdateOpCode.
    //
    // XXX TODO
    // Need to add support for DeleteStrictOpCode, InsertStrictOpCode
    // and UpdateStrictOpCode.
    resultNumRows_++;

    if (!resultKeyRangeValid_) {
        resultMinKey_ = key;
        resultMaxKey_ = key;
        resultKeyRangeValid_ = true;
    } else {
        DataFormat::updateFieldValueRange(targetXdbMeta_->keyAttr[0].type,
                                          key,
                                          &resultMinKey_,
                                          &resultMaxKey_,
                                          DontHashString,
                                          DfFieldValueRangeValid);
    }

CommonExit:
    return status;
}

//
// Assuming target Xdb and IMD Xdb slots have already been sorted,
// * Just cursor the target and IMD Xdbs and look for key match. Note that
// key match deliberately excludes the Rankover and BatchId columns.
// * Once there is key match, skip all the records in the IMD XDB until
// the most recent BatchId and RankOve. This is possible b/c the IMD Xdb
// key includes BatchId and RankOver column.
// * Now that we have the most recent IMD record for a given key on target
// table IMD ops can be calculated.
//
// XXX TODO
// * Note that we always prepare a new set of XdbPages for target table
// and insert the XdbPages into the target table slot, only when the entire slot
// worth of IMD records are fully processed.
//
// * Smarter way to do this would require performing IMD inline and splicing
// XdbPages together. However this is tricky to clean out on failures. So can
// be implemented in next iteration.
//
// * Without Index on each slot, appling IMD is O(N), i.e. requires full scan
// of all records in the target table and applying IMD and re-writing the entire
// target table slot. So IMD is still a function of target table size and NOT
// delta table size. Once we have index on a slot, finding a record to perform
// IMD would be O(log(N)) and IMD performance would be a function of delta table
// size.
//
Status
MergePostCommitWork::implementImd(uint64_t slot)
{
    Status status;
    XdbPage *resultPages = NULL;
    bool tgtCursorInited = false;
    TableCursor tgtCursor;
    bool imdCommCursorInited = false;
    TableCursor imdCommCursor;
    bool tgtAvail = false;
    bool imdAvail = false;
    bool retIsValid;
    DfFieldValue opCode = MaxOpCode;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbAtomicHashSlot *targetSlot = &targetXdb_->hashSlotInfo.hashBase[slot];
    XdbHashSlotAug *targetAugSlot = &targetXdb_->hashSlotInfo.hashBaseAug[slot];
    uint64_t imdCount = 0;
    uint64_t tgtCount = 0;
    uint64_t applyImdCount = 0;
    uint64_t procResult = 0;
    uint64_t trackImd[MaxOpCode.int64Val] = {0};

    resultNumRows_ = 0;
    resultNumPages_ = 0;
    resultKeyRangeValid_ = false;
    curXdbPageAllocIdx_ = -1;
    curNumXdbPagesAlloc_ = 0;
    curNumAllocBatches_ = 0;
    curXdbPageFreeIdx_ = -1;

    status = xdbMgr->createCursorFast(targetXdb_, slot, &tgtCursor);
    if (status == StatusOk) {
        tgtCursorInited = true;
        status = tgtCursor.getNext(&tgtKvEntry_);
    }

    if (status == StatusOk) {
        tgtCount++;
        tgtAvail = true;
    } else if (status == StatusNoData) {
        status = StatusOk;
    } else if (status != StatusNoData) {
        goto CommonExit;
    }

    status = xdbMgr->createCursorFast(imdCommittedXdb_, slot, &imdCommCursor);
    if (status == StatusOk) {
        imdCommCursorInited = true;
        status = imdCommCursor.getNext(&imdCommKvEntry_);
    }

    if (status == StatusOk) {
        imdCount++;
        imdAvail = true;
    } else if (status == StatusNoData) {
        status = StatusOk;
    } else if (status != StatusNoData) {
        goto CommonExit;
    }

    while (tgtAvail || imdAvail) {
        int ret;
        bool applyImd = false;
        bool advanceTarget = false;
        opCode = InsertOpCode;

        if (!tgtAvail) {
            ret = 1;
        } else if (!imdAvail) {
            ret = -1;
        } else {
            ret =
                DataFormat::fieldArrayCompare(imdCommittedXdbMeta_->numKeys - 2,
                                              NULL,
                                              targetXdbMeta_->keyIdxOrder,
                                              targetKvMeta_->tupMeta_,
                                              &tgtKvEntry_.tuple_,
                                              imdCommittedXdbMeta_->keyIdxOrder,
                                              imdCommittedKvMeta_->tupMeta_,
                                              &imdCommKvEntry_.tuple_);
        }

        if (ret > 0) {
            // Key exists only in imdCommittedXdb. Just init result record.
            resultKvEntry_.init();
            applyImd = true;
        } else if (ret < 0) {
            // Key exists only in targetXbd. Just propagate target record to
            // result record.
            tgtKvEntry_.tuple_.cloneTo(targetKvMeta_->tupMeta_,
                                       &resultKvEntry_.tuple_);
            advanceTarget = true;
        } else {
            // key exists in both imdCommittedXdb & targetXbd. Apply target
            // record to result record first.
            tgtKvEntry_.tuple_.cloneTo(targetKvMeta_->tupMeta_,
                                       &resultKvEntry_.tuple_);
            applyImd = true;
            advanceTarget = true;
        }

        while (applyImd) {
#ifdef DEBUG
            // Prepare stage of Merge operator must validate this.
            DfFieldValue batchId =
                imdCommKvEntry_.tuple_
                    .get(batchIdx_,
                         imdCommittedKvMeta_->tupMeta_->getNumFields(),
                         DfInt64,
                         &retIsValid);
            assert(retIsValid &&
                   "Imd records needs to have valid batchID column");
            assert(batchId.uint64Val >= TableNsMgr::StartVersion &&
                   batchId.uint64Val <= consistentVersion_);

            DfFieldValue rank =
                imdCommKvEntry_.tuple_
                    .get(rankOverIdx_,
                         imdCommittedKvMeta_->tupMeta_->getNumFields(),
                         DfInt64,
                         &retIsValid);
            assert(retIsValid &&
                   "IMD records need to have valid Rank over column");
            assert(rank.int64Val >= InitialRank.int64Val);
#endif  // DEBUG

            imdCommKvEntry_.tuple_.cloneTo(imdCommittedKvMeta_->tupMeta_,
                                           &imdPrevEntry_.tuple_);

            status = imdCommCursor.getNext(&imdCommKvEntry_);
            if (status == StatusNoData) {
                imdAvail = false;
                status = StatusOk;
                break;
            }
            BailIfFailed(status);
            imdCount++;

            ret =
                DataFormat::fieldArrayCompare(imdCommittedXdbMeta_->numKeys - 2,
                                              NULL,
                                              imdCommittedXdbMeta_->keyIdxOrder,
                                              imdCommittedKvMeta_->tupMeta_,
                                              &imdCommKvEntry_.tuple_,
                                              imdCommittedXdbMeta_->keyIdxOrder,
                                              imdCommittedKvMeta_->tupMeta_,
                                              &imdPrevEntry_.tuple_);
            if (ret != 0) {
                break;
            }
        }

        if (applyImd) {
            applyImdCount++;
            applyImd = false;
            imdPrevEntry_.tuple_.cloneTo(targetKvMeta_->tupMeta_,
                                         &resultKvEntry_.tuple_);
            opCode = imdPrevEntry_.tuple_.get(opCodeIdx_, &retIsValid);
            assert(retIsValid &&
                   "IMD records needs to have valid opCode column");
        }

        procResult++;
        trackImd[opCode.uint64Val]++;
        status = processResult(&resultKvEntry_, opCode, &resultPages);
        BailIfFailed(status);

        if (advanceTarget) {
            status = tgtCursor.getNext(&tgtKvEntry_);
            if (status == StatusNoData) {
                tgtAvail = false;
                status = StatusOk;
            }
            BailIfFailed(status);
            tgtCount++;
        }
    }
    assert(status == StatusOk);

    // Reverse XdbPage list
    {
        uint64_t rows = 0;
        uint64_t pages = 0;
        XdbPage *cur = resultPages;
        XdbPage *prev = NULL, *next = NULL;
        while (cur != NULL) {
#ifdef DEBUG
            rows += cur->hdr.numRows;
            pages++;
#endif  // DEBUG
            next = cur->hdr.nextPage;
            cur->hdr.nextPage = prev;
            prev = cur;
            cur = next;
        }
        resultPages = prev;
        assert(rows == resultNumRows_ && pages == resultNumPages_);
    }

    // Need to destroy the cursors before the xdbPages are dropped
    // or they'll be pointing to stale pages
    if (tgtCursorInited) {
        CursorManager::get()->destroy(&tgtCursor);
        tgtCursorInited = false;
    }
    xdbMgr->xdbDropSlot(targetXdb_, slot, true);
    xdbMgr->setXdbHashSlotNextPage(targetSlot,
                                   targetXdb_->getXdbId(),
                                   slot,
                                   resultPages);

    if (imdCommCursorInited) {
        CursorManager::get()->destroy(&imdCommCursor);
        imdCommCursorInited = false;
    }
    xdbMgr->xdbDropSlot(imdCommittedXdb_, slot, true);

    targetAugSlot->minKey = resultMinKey_;
    targetAugSlot->maxKey = resultMaxKey_;
    targetAugSlot->keyRangeValid = resultKeyRangeValid_;
    targetAugSlot->numPages = resultNumPages_;
    targetAugSlot->numRows = resultNumRows_;

    resultPages = NULL;

CommonExit:
    if (tgtCursorInited) {
        CursorManager::get()->destroy(&tgtCursor);
        tgtCursorInited = false;
    }

    if (imdCommCursorInited) {
        CursorManager::get()->destroy(&imdCommCursor);
        imdCommCursorInited = false;
    }

    if (resultPages) {
        XdbPage *xdbPage = resultPages;
        curXdbPageFreeIdx_ = 0;
        while (xdbPage) {
            if (curXdbPageFreeIdx_ == XdbMgr::BatchFreeSize) {
                xdbMgr->xdbFreeXdbPageBatch(xdbPagesFreeBatch_,
                                            curXdbPageFreeIdx_);
                curXdbPageFreeIdx_ = 0;
            }
            xdbPagesFreeBatch_[curXdbPageFreeIdx_++] = xdbPage;
            xdbPage = xdbPage->hdr.nextPage;
        }
        if (curXdbPageFreeIdx_) {
            xdbMgr->xdbFreeXdbPageBatch(xdbPagesFreeBatch_, curXdbPageFreeIdx_);
        }
    }

    if ((unsigned) (curXdbPageAllocIdx_) + 1 < curNumXdbPagesAlloc_) {
        xdbMgr
            ->xdbFreeXdbPageBatch(&xdbPagesAllocBatch_[curXdbPageAllocIdx_ + 1],
                                  curNumXdbPagesAlloc_ -
                                      (curXdbPageAllocIdx_ + 1));
        curXdbPageAllocIdx_ = -1;
        curNumXdbPagesAlloc_ = 0;
    }

    return status;
}

Status
MergePostCommitWork::processSlot(uint64_t slot)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbAtomicHashSlot *targetSlot = &targetXdb_->hashSlotInfo.hashBase[slot];
    XdbAtomicHashSlot *imdCommittedSlot =
        &imdCommittedXdb_->hashSlotInfo.hashBase[slot];
    XdbAtomicHashSlot *imdPendingSlot =
        &mergeInfo_->getImdPendingXdb()->hashSlotInfo.hashBase[slot];
    XdbPage *targetPage = NULL;
    XdbPage *imdPendingPage = NULL;
    XdbPage *imdCommittedPage = NULL;
    bool slotLocked = false;

    // There is  no need for slot lock, since there is EXCL table lock on the
    // target table.

    // Move pending IMD records from IMD pending XDB to IMD committed XDB.
    imdPendingPage = xdbMgr->getXdbHashSlotNextPage(imdPendingSlot);
    if (imdPendingPage != NULL) {
        transImdPendingToCommitted(slot);
    }

    // Sort the slot for IMD committed XDB.
    if (!imdCommittedSlot->sortFlag) {
        status =
            xdbMgr->sortHashSlotEx(imdCommittedXdb_, slot, &imdCommittedPage);
        if (status == StatusNoData) {
            // No IMD on this slot
            status = StatusOk;
        }
        BailIfFailed(status);
    } else {
        imdCommittedPage = xdbMgr->getXdbHashSlotNextPage(imdCommittedSlot);
    }
    if (imdCommittedPage == NULL) {
        // No IMD on this slot
        status = StatusOk;
        goto CommonExit;
    }

    // Sort the slot for target XDB.
    slotLocked = true;
    xdbMgr->lockSlot(&targetXdb_->hashSlotInfo, slot);
    if (!targetSlot->sortFlag) {
        status = xdbMgr->sortHashSlotEx(targetXdb_, slot, &targetPage);
        if (status == StatusNoData) {
            // No IMD on this slot
            status = StatusOk;
        }
        BailIfFailed(status);
    } else {
        targetPage = xdbMgr->getXdbHashSlotNextPage(targetSlot);
    }

    // working on this slot to do imd
    mergeInfo_->incImdSlotsProcessedCounter();
    mergeInfo_->updateImdPendingMergeRowCount(
        imdCommittedXdb_->hashSlotInfo.hashBaseAug[slot].numRows);
    status = implementImd(slot);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (slotLocked) {
        xdbMgr->unlockSlot(&targetXdb_->hashSlotInfo, slot);
    }
    return status;
}

Status
Operators::mergePostCommitLocal(TableNsMgr::IdHandle *idHandle)
{
    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->postCommitLocal);
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    TrackHelpers *trackHelpers = NULL;
    Xdb *targetXdb = NULL;
    XdbMeta *targetXdbMeta = NULL;
    VersionId consistentVersion = idHandle->consistentVersion;
    VersionId nextVersion = idHandle->nextVersion - 1;
    TableMgr *tmgr = TableMgr::get();
    TableMgr::ColumnInfoTable *colTable = NULL;
    TableMgr::ColumnInfoTable *imdColTable = NULL;
    unsigned numScheds = 0;
    uint64_t numChunks = XdbMgr::xdbHashSlots;
    MergeMgr::MergeInfo *mergeInfo = NULL;
    bool doTeardown = false;
    uint64_t targetRowCount = 0;
    MergePostCommitWork **scheds = NULL;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    TableMgr::TableObj *tableObj = tmgr->getTableObj(idHandle->tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed mergePostCommit target table %lu for version %lu, %lu: "
                "%s",
                idHandle->tableId,
                consistentVersion,
                nextVersion,
                strGetFromStatus(status));
        goto CommonExit;
    }

    colTable = tableObj->getColTable();
    imdColTable = tableObj->getImdColTable();
    mergeInfo = tableObj->getMergeInfo();

    if (!mergeInfo ||
        mergeInfo->getImdPendingVersionNum() > consistentVersion) {
        // No committed IMD records found to apply IMD
        goto CommonExit;
    }

    assert(mergeInfo->getImdPendingVersionNum() == consistentVersion &&
           "IMD committed version must be consistent version");

    targetXdb = mergeInfo->getTargetXdb();
    targetXdbMeta = mergeInfo->getTargetXdbMeta();
    targetRowCount = targetXdb->numRows;
    numScheds = Operators::getNumWorkers(
        targetRowCount + mergeInfo->getImdPendingMergeRowCount());

    doTeardown = true;
    mergeInfo->setupPostMerge();

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    scheds = new (std::nothrow) MergePostCommitWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            MergePostCommitWork(ii == 0 ? TrackHelpers::Master
                                        : TrackHelpers::NonMaster,
                                ii,
                                trackHelpers,
                                targetXdb,
                                targetXdbMeta,
                                mergeInfo,
                                consistentVersion,
                                nextVersion,
                                colTable,
                                imdColTable);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed mergePostCommit target table %lu for version %lu, "
                    "%lu: %s",
                    idHandle->tableId,
                    consistentVersion,
                    nextVersion,
                    strGetFromStatus(status));

    xSyslog(moduleName,
            XlogInfo,
            "MergePostCommitWork on target table %lu, "
            "target xdb row count %lu, pending merge row count %lu, "
            "slots processed %lu",
            idHandle->tableId,
            targetRowCount,
            mergeInfo->getImdPendingMergeRowCount(),
            mergeInfo->getImdSlotsProcessedCounter());

CommonExit:
    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->postCommitLocalFailure);
    }

    if (doTeardown) {
        mergeInfo->teardownPostMerge(status);
    }
    logOperationStage(__func__, status, stopwatch, End);

    return status;
}

class MergeAbortWork : public MergeWorkBase
{
  public:
    MergeAbortWork(TrackHelpers::WorkerType workerTypeIn,
                   unsigned workerIdIn,
                   TrackHelpers *trackHelpers,
                   Xdb *targetXdb,
                   XdbMeta *targetXdbMeta,
                   MergeMgr::MergeInfo *mergeInfo,
                   VersionId consistentVersion,
                   VersionId nextVersion,
                   TableMgr::ColumnInfoTable *colTable,
                   TableMgr::ColumnInfoTable *imdColTable)
        : MergeWorkBase(workerTypeIn,
                        workerIdIn,
                        trackHelpers,
                        targetXdb,
                        targetXdbMeta,
                        mergeInfo,
                        consistentVersion,
                        nextVersion,
                        colTable,
                        imdColTable)
    {
    }

    virtual void run();
    virtual void done() { delete this; }

  private:
    Status setup() { return StatusOk; }
    void tearDown() {}
    Status processSlot(uint64_t slot);
};

void
MergeAbortWork::run()
{
    Status status = StatusOk;
    status = trackHelpers_->helperStart();

    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = setup();
    // if setup fails we want to go ahead and mark all work items as complete
    // then report a bad status at the end

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        status = processSlot(ii);

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    tearDown();
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
MergeAbortWork::processSlot(uint64_t slot)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    xdbMgr->xdbDropSlot(imdPendingXdb_, slot, true);
    return status;
}

Status
Operators::mergeAbortLocal(const char *tableName[MergeNumTables],
                           TableNsMgr::IdHandle *idHandle[MergeNumTables],
                           XdbId xdbId[MergeNumTables])
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    TrackHelpers *trackHelpers = NULL;
    TableMgr *tmgr = TableMgr::get();
    VersionId consistentVersion = idHandle[MergeTargetTable]->consistentVersion;
    VersionId nextVersion = idHandle[MergeTargetTable]->nextVersion - 1;
    Xdb *xdb[MergeNumTables] = {NULL};
    XdbMeta *xdbMeta[MergeNumTables] = {NULL};
    XdbMgr *xdbMgr = XdbMgr::get();
    MergeAbortWork **scheds = NULL;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);
    StatsLib::statNonAtomicIncr(MergeMgr::get()->getStats()->abortLocal);

    for (unsigned ii = Operators::MergeDeltaTable;
         ii < Operators::MergeNumTables;
         ii++) {
        // Delta table cannot be dropped, because the source node grabs a shared
        // lock on the table.
        // Target table cannot be dropped, because the source node grabs an Excl
        // lock on the table.
        status = xdbMgr->xdbGet(xdbId[ii], &xdb[ii], &xdbMeta[ii]);
        assert(status == StatusOk);
    }

    TableMgr::ColumnInfoTable *colTable = NULL;
    TableMgr::ColumnInfoTable *imdColTable = NULL;
    unsigned numScheds = 0;
    uint64_t numChunks = XdbMgr::xdbHashSlots;
    MergeMgr::MergeInfo *mergeInfo = NULL;
    TableMgr::TableObj *tableObj =
        tmgr->getTableObj(idHandle[MergeTargetTable]->tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Failed mergeAbort delta table %s metadata, target table %s "
                "for version %lu, %lu: %s",
                tableName[MergeDeltaTable],
                tableName[MergeTargetTable],
                consistentVersion,
                nextVersion,
                strGetFromStatus(status));
        goto CommonExit;
    }
    mergeInfo = tableObj->getMergeInfo();
    if (mergeInfo->getImdPendingVersionNum() <= consistentVersion) {
        // Nothing to abort
        goto CommonExit;
    }

    colTable = tableObj->getColTable();
    imdColTable = tableObj->getImdColTable();
    numScheds = Operators::getNumWorkers(xdb[MergeDeltaTable]->numRows);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    scheds = new (std::nothrow) MergeAbortWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] =
            new (std::nothrow) MergeAbortWork(ii == 0 ? TrackHelpers::Master
                                                      : TrackHelpers::NonMaster,
                                              ii,
                                              trackHelpers,
                                              xdb[MergeTargetTable],
                                              xdbMeta[MergeTargetTable],
                                              mergeInfo,
                                              consistentVersion,
                                              nextVersion,
                                              colTable,
                                              imdColTable);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "mergeAbortLocal operation failed for delta table %s, "
                    "target table %s for version %lu, %lu: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    consistentVersion,
                    nextVersion,
                    strGetFromStatus(status));

    Xdb *imdPendingXdb;
    imdPendingXdb = mergeInfo->getImdPendingXdb();
    mergeInfo->resetImdXdbForLoadDone(imdPendingXdb);
    status = xdbMgr->xdbLoadDoneScratchXdb(imdPendingXdb);
    BailIfFailedMsg(moduleName,
                    status,
                    "mergeAbortLocal operation failed for delta table %s, "
                    "target table %s for version %lu, %lu: %s",
                    tableName[MergeDeltaTable],
                    tableName[MergeTargetTable],
                    consistentVersion,
                    nextVersion,
                    strGetFromStatus(status));

CommonExit:
    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            MergeMgr::get()->getStats()->abortLocalFailure);
    }
    logOperationStage(__func__, status, stopwatch, End);

    return status;
}
