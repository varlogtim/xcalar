// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <new>

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "msg/Message.h"
#include "config/Config.h"
#include "usrnode/UsrNode.h"
#include "operators/Operators.h"
#include "operators/OperatorsTypes.h"
#include "operators/GenericTypes.h"
#include "df/DataFormat.h"
#include "xdb/Xdb.h"
#include "dataset/Dataset.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "callout/Callout.h"
#include "table/ResultSet.h"
#include "util/IntHashTable.h"
#include "msg/Xid.h"
#include "constants/XcalarConfig.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"
#include "strings/String.h"
#include "operators/OperatorsXdbPageOps.h"

static constexpr const char *moduleName = "resultSetTable";

Status
TableResultSet::getNext(uint64_t numEntriesRequested, json_t **entriesArrayOut)
{
    Status status = StatusOk;
    json_t *record = NULL;
    NodeId dstNode;
    uint64_t localOffset;
    uint64_t numEntriesRemaining = numEntriesRequested;

    json_t *entries = json_array();
    BailIfNull(entries);

    while (numEntriesRemaining > 0 && currentEntry_ < numEntries_) {
        uint64_t entriesAdded = 0;
        getNodeIdAndLocalOffset(&dstNode, &localOffset);

        if (Config::get()->getMyNodeId() == dstNode) {
            status = getNextLocal(entries,
                                  localOffset,
                                  xdb_,
                                  numEntriesRemaining,
                                  &entriesAdded);
            BailIfFailed(status);
        } else {
            status = getNextRemote(dstNode,
                                   entries,
                                   localOffset,
                                   numEntriesRemaining,
                                   &entriesAdded);
            BailIfFailed(status);
        }

        numEntriesRemaining -= entriesAdded;
        currentEntry_ += entriesAdded;
    }

CommonExit:
    *entriesArrayOut = entries;

    if (record) {
        json_decref(record);
    }

    return status;
}

Status
ResultSetDemystifySend::processDemystifiedRow(void *localArgs,
                                              NewKeyValueEntry *srcKvEntry,
                                              DfFieldValue rowMetaField,
                                              Atomic32 *countToDecrement)
{
    Status status = StatusOk;
    XdbMeta *srcMeta = (XdbMeta *) localArgs;
    const NewTupleMeta *tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t numFields = tupMeta->getNumFields();
    TableResultSet::RowMeta *rowMeta =
        (TableResultSet::RowMeta *) rowMetaField.opRowMetaVal;
    json_t *jsonVal = NULL;
    int ret;

    for (unsigned ii = 0; ii < numFields; ii++) {
        DfFieldType type = tupMeta->getFieldType(ii);
        DfFieldValue fieldVal;
        bool isValid;
        fieldVal = srcKvEntry->tuple_.get(ii,
                                          numFields,
                                          tupMeta->getFieldType(ii),
                                          &isValid);
        if (!isValid) {
            continue;
        }

        assert(jsonVal == NULL);
        if (type == DfFatptr) {
            status = DataFormat::get()
                         ->fatptrToJson(fieldVal.fatptrVal,
                                        srcMeta->kvNamedMeta.valueNames_[ii],
                                        &jsonVal);
            BailIfFailed(status);
        } else {
            status = DataFormat::jsonifyValue(tupMeta->getFieldType(ii),
                                              &fieldVal,
                                              &jsonVal);
            BailIfFailed(status);
        }

        ret = json_array_set_new(rowMeta->rowArray, ii, jsonVal);
        BailIfFailedWith(ret, StatusJsonError);
        jsonVal = NULL;
    }

CommonExit:
    if (jsonVal) {
        json_decref(jsonVal);
    }
    return status;
}

Status
TableResultSet::demystifyRows(TableCursor *cur,
                              uint64_t numEntries,
                              XdbMeta *srcMeta,
                              XdbId dstXdbId)
{
    Status status;
    bool validIndices[TupleMaxNumValuesPerRecord];
    memset(validIndices, true, sizeof(validIndices));

    TransportPage *demystifyPages[MaxNodes];
    memZero(demystifyPages, sizeof(demystifyPages));

    OpInsertHandle insertHandle;

    TransportPageHandle transPageHandle;
    CreateScalarTableDemystifySend *demystifyHandle =
        new (std::nothrow) CreateScalarTableDemystifySend(srcMeta,
                                                          demystifyPages,
                                                          dstXdbId,
                                                          validIndices,
                                                          &transPageHandle,
                                                          NULL,
                                                          NULL);
    if (!demystifyHandle) {
        return StatusNoMem;
    }

    status = transPageHandle.initHandle(MsgTypeId::Msg2pcDemystifySourcePage,
                                        TwoPcCallId::Msg2pcDemystifySourcePage1,
                                        NULL,
                                        dstXdbId,
                                        NULL);
    assert(status == StatusOk);

    RowMeta *rowMeta = NULL;
    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);

    XdbMeta *dstMeta;
    status = XdbMgr::get()->xdbGet(dstXdbId, NULL, &dstMeta);
    assert(status == StatusOk);

    MemoryPile *rowMetaPile = MemoryPile::allocPile(sizeof(*rowMeta),
                                                    Txn::currentTxn().id_,
                                                    &status,
                                                    1);
    BailIfFailed(status);

    status =
        opGetInsertHandle(&insertHandle, &dstMeta->loadInfo, XdbInsertSlotHash);
    BailIfFailed(status);

    insertHandle.slotId = 0;

    for (uint64_t numRecs = 0; numRecs < numEntries; numRecs++) {
        DfFieldValue rowMetaField;

        rowMeta = (RowMeta *) MemoryPile::getElem(&rowMetaPile,
                                                  sizeof(*rowMeta),
                                                  &status);
        BailIfFailed(status);

        rowMeta->rowArray = json_array();
        BailIfNull(rowMeta->rowArray);

        rowMeta->recNum = numRecs;
        atomicWrite32(&rowMeta->numIssued, 0);

        status = cur->getNext(&srcKvEntry);
        BailIfFailed(status);

        rowMetaField.opRowMetaVal = (uintptr_t) rowMeta;

        // XXX: change this to actually do batch demystification
        status = demystifyHandle->processDemystifiedRow(srcMeta,
                                                        &srcKvEntry,
                                                        rowMetaField,
                                                        NULL);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

Status
TableResultSet::demystifyRowsIntoXdb(Xdb *xdb,
                                     uint64_t localOffset,
                                     uint64_t numEntries,
                                     Xid *xdbIdOut)
{
    Status status;
    XdbMeta *srcMeta = XdbMgr::xdbGetMeta(xdb);
    XdbId scalarXdbId = XidMgr::get()->xidGetNext();
    const NewTupleMeta *srcTupMeta;
    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    unsigned numFields = srcTupMeta->getNumFields();
    TableCursor cur;

    const char *immediateNames[TupleMaxNumValuesPerRecord];

    NewTupleMeta tupMeta;
    tupMeta.setNumFields(numFields);

    for (unsigned ii = 0; ii < numFields; ii++) {
        DfFieldType srcType = srcTupMeta->getFieldType(ii);
        if (srcType == DfFatptr) {
            tupMeta.setFieldType(DfString, ii);
        } else {
            tupMeta.setFieldType(srcType, ii);
        }

        immediateNames[ii] = srcMeta->kvNamedMeta.valueNames_[ii];
    }

    status = XdbMgr::get()->xdbCreate(scalarXdbId,
                                      DsDefaultDatasetKeyName,
                                      DfInt64,
                                      NewTupleMeta::DfInvalidIdx,
                                      &tupMeta,
                                      NULL,
                                      0,
                                      immediateNames,
                                      numFields,
                                      NULL,
                                      0,
                                      Random,
                                      XdbLocal,
                                      DhtInvalidDhtId);
    BailIfFailed(status);

    status = CursorManager::get()->createOnTable(srcMeta->xdbId,
                                                 Unordered,
                                                 Cursor::InsertIntoHashTable,
                                                 &cur);
    BailIfFailed(status);

    status = cur.seek(localOffset, Cursor::SeekToAbsRow);
    BailIfFailed(status);

    status = demystifyRows(&cur, numEntries, srcMeta, scalarXdbId);
    BailIfFailed(status);

CommonExit:
    *xdbIdOut = scalarXdbId;

    return status;
}

Status
TableResultSet::getNextLocal(json_t *entries,
                             uint64_t localOffset,
                             Xdb *xdb,
                             uint64_t numEntries,
                             uint64_t *numEntriesAdded)
{
    Status status = StatusOk;
    TableCursor cur;
    bool cursorInit = false;
    XdbMeta *srcMeta = XdbMgr::xdbGetMeta(xdb);
    const NewTupleMeta *tupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t numFields = tupMeta->getNumFields();
    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);
    json_t *jsonVal = NULL;
    uint64_t numRecs = 0;
    json_t *recJson = NULL;

    status = CursorManager::get()->createOnTable(srcMeta->xdbId,
                                                 Unordered,
                                                 Cursor::IncrementRefOnly,
                                                 &cur);
    BailIfFailed(status);
    cursorInit = true;

    status = cur.seek(localOffset, Cursor::SeekToAbsRow);
    BailIfFailed(status);

    for (numRecs = 0; numRecs < numEntries; numRecs++) {
        int ret;
        status = cur.getNext(&srcKvEntry);
        BailIfFailed(status);

        assert(recJson == NULL);
        recJson = json_object();
        BailIfNull(recJson);

        for (unsigned ii = 0; ii < numFields; ii++) {
            DfFieldType type = tupMeta->getFieldType(ii);
            const char *name = srcMeta->kvNamedMeta.valueNames_[ii];
            DfFieldValue fieldVal;
            bool isValid;
            fieldVal = srcKvEntry.tuple_.get(ii, numFields, type, &isValid);
            if (!isValid) {
                continue;
            }

            if (type == DfFatptr) {
                status = DataFormat::get()->fatptrToJson(fieldVal.fatptrVal,
                                                         name,
                                                         &jsonVal);
                BailIfFailed(status);

                ret = json_object_update(recJson, jsonVal);
                if (ret != 0) {
                    // Because of horrible jansson code an error (non-zero ret)
                    // on this path -usually- means jsonVal has already had
                    // json_decref called on it, but there are also rare cases
                    // where it hasn't.  It's impossible to tell from the return
                    // value, so just risk leaking (improbably) jsonVal rather
                    // than risk corruption (which we've actually observed).
                    jsonVal = NULL;
                    status = StatusJsonError;
                    goto CommonExit;
                }

            } else {
                status = DataFormat::jsonifyValue(tupMeta->getFieldType(ii),
                                                  &fieldVal,
                                                  &jsonVal);
                BailIfFailed(status);

                // json_object_set_new steals ref
                ret = json_object_set_new(recJson, name, jsonVal);
                jsonVal = NULL;  // Explanation above (grep for "horrible")
                BailIfFailedWith(ret, StatusJsonError);
            }
        }

        // json_array_append_new steals ref
        ret = json_array_append_new(entries, recJson);
        recJson = NULL;  // Explanation above (grep for "horrible")
        BailIfFailedWith(ret, StatusJsonError);
    }
CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (cursorInit) {
        CursorManager::get()->destroy(&cur);
        cursorInit = false;
    }

    if (jsonVal) {
        json_decref(jsonVal);
        jsonVal = NULL;
    }
    if (recJson) {
        json_decref(recJson);
        recJson = NULL;
    }

    *numEntriesAdded = numRecs;

    return status;
}

Status
TableResultSet::getByKey(json_t *rows, DfFieldValue key, DfFieldType keyType)
{
    return StatusUnimpl;
}
