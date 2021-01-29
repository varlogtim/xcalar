// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/TableService.h"
#include "table/TableNs.h"
#include "sys/XLog.h"
#include "usr/Users.h"
#include "dag/Dag.h"
#include "operators/Operators.h"
#include "libapis/ProtobufUtil.h"

using namespace xcalar::compute::localtypes::Table;
using namespace xcalar::compute::localtypes::ColumnAttribute;

ServiceAttributes
TableService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
TableService::getHandleToHashTree(const char *tableName,
                                  LibNsTypes::NsOpenFlags flags,
                                  HashTreeRefHandle *refHandle)
{
    Status status;
    bool handleInit = false;
    status =
        HashTreeMgr::get()->openHandleToHashTree(tableName,
                                                 flags,
                                                 refHandle,
                                                 HashTreeMgr::OpenRetry::True);
    BailIfFailed(status);
    handleInit = true;

    if (!refHandle->active) {
        status = StatusPubTableInactive;
        goto CommonExit;
    }

    if (refHandle->restoring) {
        status = StatusPubTableRestoring;
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (handleInit) {
            HashTreeMgr::get()->closeHandleToHashTree(refHandle,
                                                      refHandle->hashTreeId,
                                                      tableName);
        }
    }
    return status;
}

Status
TableService::addIndex(const IndexRequest *req, google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    bool handleInit = false;
    HashTreeRefHandle refHandle;

    status = getHandleToHashTree(req->table_name().c_str(),
                                 LibNsTypes::ReadSharedWriteExclReader,
                                 &refHandle);
    BailIfFailed(status);
    handleInit = true;

    status = HashTreeMgr::get()->addIndexToHashTree(req->table_name().c_str(),
                                                    refHandle.hashTreeId,
                                                    req->key_name().c_str());
    BailIfFailed(status);

CommonExit:
    if (handleInit) {
        HashTreeMgr::get()->closeHandleToHashTree(&refHandle,
                                                  refHandle.hashTreeId,
                                                  req->table_name().c_str());
    }

    return status;
}

Status
TableService::removeIndex(const IndexRequest *req,
                          google::protobuf::Empty *empty)
{
    Status status = StatusOk;
    bool handleInit = false;
    HashTreeRefHandle refHandle;

    status = getHandleToHashTree(req->table_name().c_str(),
                                 LibNsTypes::ReadSharedWriteExclReader,
                                 &refHandle);
    BailIfFailed(status);
    handleInit = true;

    status =
        HashTreeMgr::get()->removeIndexFromHashTree(req->table_name().c_str(),
                                                    refHandle.hashTreeId,
                                                    req->key_name().c_str());
    BailIfFailed(status);

CommonExit:
    if (handleInit) {
        HashTreeMgr::get()->closeHandleToHashTree(&refHandle,
                                                  refHandle.hashTreeId,
                                                  req->table_name().c_str());
    }

    return status;
}

Status
TableService::publishTable(const PublishRequest *publishRequest,
                           PublishResponse *publishResponse)
{
    XcalarApiUdfContainer *sessionContainer = nullptr;
    TableNsMgr *tableNsMgr = TableNsMgr::get();
    UserMgr *userMgr = UserMgr::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    auto workbookSpec = publishRequest->scope().workbook();
    bool inc = false;

    Status status =
        userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
    BailIfFailed(status);
    inc = true;

    status = userMgr->getSessionContainer(publishRequest->scope()
                                              .workbook()
                                              .name()
                                              .username()
                                              .c_str(),
                                          publishRequest->scope()
                                              .workbook()
                                              .name()
                                              .workbookname()
                                              .c_str(),
                                          &sessionContainer);
    BailIfFailed(status);

    xSyslog(ModuleName,
            XlogInfo,
            "publishTable: user %s session %s table %s",
            publishRequest->scope().workbook().name().username().c_str(),
            publishRequest->scope().workbook().name().workbookname().c_str(),
            publishRequest->table_name().c_str());

    status = tableNsMgr->publishTable(sessionContainer,
                                      publishRequest->table_name().c_str());
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed publishTable: user %s session %s table %s: %s",
                publishRequest->scope().workbook().name().username().c_str(),
                publishRequest->scope()
                    .workbook()
                    .name()
                    .workbookname()
                    .c_str(),
                publishRequest->table_name().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = tableNsMgr->getFQN(fullyQualName,
                                sizeof(fullyQualName),
                                sessionContainer,
                                publishRequest->table_name().c_str());
    BailIfFailed(status);

    try {
        publishResponse->set_fully_qual_table_name(fullyQualName);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (inc) {
        (void) userMgr->trackOutstandOps(&workbookSpec,
                                         UserMgr::OutstandOps::Dec);
    }
    return status;
}

Status
TableService::unpublishTable(const UnpublishRequest *unpublishRequest,
                             google::protobuf::Empty *empty)
{
    XcalarApiUdfContainer *sessionContainer = nullptr;
    TableNsMgr *tableNsMgr = TableNsMgr::get();
    UserMgr *userMgr = UserMgr::get();
    auto workbookSpec = unpublishRequest->scope().workbook();
    bool inc = false;

    Status status =
        userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
    BailIfFailed(status);
    inc = true;

    status = userMgr->getSessionContainer(unpublishRequest->scope()
                                              .workbook()
                                              .name()
                                              .username()
                                              .c_str(),
                                          unpublishRequest->scope()
                                              .workbook()
                                              .name()
                                              .workbookname()
                                              .c_str(),
                                          &sessionContainer);
    BailIfFailed(status);

    xSyslog(ModuleName,
            XlogInfo,
            "unpublishTable: user %s session %s table %s",
            unpublishRequest->scope().workbook().name().username().c_str(),
            unpublishRequest->scope().workbook().name().workbookname().c_str(),
            unpublishRequest->table_name().c_str());

    status = tableNsMgr->unpublishTable(sessionContainer,
                                        unpublishRequest->table_name().c_str());
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed unpublishTable: user %s session %s table %s: %s",
                unpublishRequest->scope().workbook().name().username().c_str(),
                unpublishRequest->scope()
                    .workbook()
                    .name()
                    .workbookname()
                    .c_str(),
                unpublishRequest->table_name().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (inc) {
        (void) userMgr->trackOutstandOps(&workbookSpec,
                                         UserMgr::OutstandOps::Dec);
    }
    return status;
}

Status
TableService::listTables(const ListTablesRequest *listRequest,
                         ListTablesResponse *listResponse)
{
    char **tableNames = NULL;
    size_t numTables;
    TableNsMgr *tableNsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *sessionContainer = NULL;
    auto scope = listRequest->scope();
    auto workbookSpec = scope.workbook();
    bool inc = false;
    UserMgr *userMgr = UserMgr::get();
    Status status;

    switch (scope.specifier_case()) {
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kGlobl: {
        status = tableNsMgr->listGlobalTables(listRequest->pattern().c_str(),
                                              tableNames,
                                              numTables);
        BailIfFailed(status);
        break;
    }
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kWorkbook: {
        UserMgr *userMgr = UserMgr::get();
        status =
            userMgr->trackOutstandOps(&workbookSpec, UserMgr::OutstandOps::Inc);
        BailIfFailed(status);
        inc = true;
        status = userMgr->getSessionContainer(scope.workbook()
                                                  .name()
                                                  .username()
                                                  .c_str(),
                                              scope.workbook()
                                                  .name()
                                                  .workbookname()
                                                  .c_str(),
                                              &sessionContainer);
        BailIfFailed(status);

        status = tableNsMgr->listSessionTables(listRequest->pattern().c_str(),
                                               tableNames,
                                               numTables,
                                               sessionContainer);
        BailIfFailed(status);
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

    try {
        for (size_t ii = 0; ii < numTables; ++ii) {
            TableMetaRequest metaRequest;
            TableMetaResponse metaResponse;

            metaRequest.set_table_name(tableNames[ii]);
            metaRequest.mutable_scope()->CopyFrom(scope);

            status = tableMeta(&metaRequest, &metaResponse);
            // table might got dropped meanwhile, it's ok
            // don't add it to the response list
            if (status != StatusTableNotFound) {
                listResponse->add_table_names(tableNames[ii]);
                google::protobuf::MapPair<std::string, TableMetaResponse>
                    tabMapEntry(tableNames[ii], metaResponse);
                listResponse->mutable_table_meta_map()->insert(tabMapEntry);
            }
            // XXX ENG-8491 we should fail list tables if any table meta request
            // fails Currently, tables are backed by dag nodes and dag nodes can
            // share one xdb(in case of synthesize operator dagnodes),
            // So, if we drop a dag node which is the source of xdb, xdb also
            // gets dropped currently (BUG), so we cannot get table meta of
            // other dag nodes as it's underlying xdb is dropped (stale xdb
            // handles).
            status = StatusOk;
        }
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (tableNames) {
        for (size_t ii = 0; ii < numTables; ++ii) {
            delete[] tableNames[ii];
        }
        delete[] tableNames;
    }
    if (inc) {
        (void) userMgr->trackOutstandOps(&workbookSpec,
                                         UserMgr::OutstandOps::Dec);
    }
    return status;
}

Status
TableService::tableMeta(const TableMetaRequest *metaRequest,
                        TableMetaResponse *metaResponse)
{
    Status status;
    TableMgr::FqTableState fqTableState;
    TableNsMgr::TableHandleTrack handleTrack;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    TableMgr::TableObj *tableObj = NULL;
    auto scope = metaRequest->scope();
    auto tableName = metaRequest->table_name().c_str();

    status = fqTableState.setUp(tableName, &scope);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed fqTableState.setUp for \"%s\": %s",
                    tableName,
                    strGetFromStatus(status));

    status = fqTableState.graph->getTableIdFromNodeId(fqTableState.nodeId,
                                                      &handleTrack.tableId);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    fqTableState.nodeId,
                    strGetFromStatus(status));

    // XXX ENG-8492 should be able to grab ref instead of Ns Lock
    status = tnsMgr->openHandleToNs(&fqTableState.sessionContainer,
                                    handleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &handleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    handleTrack.tableId,
                    strGetFromStatus(status));
    handleTrack.tableHandleValid = true;

    tableObj = TableMgr::get()->getTableObj(handleTrack.tableId);
    if (tableObj == NULL) {
        status = StatusTableNotFound;
    }
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed to getTableObj for table "
                    "%s: %s",
                    tableName,
                    strGetFromStatus(status));

    try {
        status = tableObj->getTableMeta(&handleTrack,
                                        fqTableState.graph,
                                        metaResponse->mutable_schema(),
                                        metaResponse->mutable_attributes());
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed to get schema and attributes for table "
                        "%s: %s",
                        tableName,
                        strGetFromStatus(status));

        status = tableObj->getTableAggrStats(fqTableState.graph,
                                             metaResponse
                                                 ->mutable_aggregated_stats());
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed to get aggregated stats for table "
                        "%s: %s",
                        tableName,
                        strGetFromStatus(status));

        // per node stats are bit costly then others as this requires
        // fetching stats from all nodes
        if (metaRequest->include_per_node_stats()) {
            status =
                tableObj->getTablePerNodeStats(fqTableState.graph,
                                               metaResponse
                                                   ->mutable_stats_per_node());
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed to get per node stats for table "
                            "%s: %s",
                            tableName,
                            strGetFromStatus(status));
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed get table meta for table "
                        "%s: %s",
                        tableName,
                        strGetFromStatus(status));
    }

CommonExit:
    metaResponse->set_status(strGetFromStatus(status));
    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }
    return status;
}
