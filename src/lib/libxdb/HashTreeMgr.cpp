// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <math.h>
#include <fcntl.h>
#include <dirent.h>

#include "StrlFunc.h"
#include "strings/String.h"
#include "primitives/Primitives.h"
#include "table/Table.h"
#include "xdb/HashTree.h"
#include "msg/Message.h"
#include "xdb/Xdb.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "util/Math.h"
#include "df/DataFormat.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "msg/Xid.h"
#include "transport/TransportPage.h"
#include "gvm/Gvm.h"
#include "ns/LibNs.h"
#include "cursor/Cursor.h"
#include "xdb/XdbInt.h"
#include "libapis/LibApisCommon.h"
#include "log/Log.h"
#include "util/FileUtils.h"
#include "dag/DagLib.h"
#include "libapis/WorkItem.h"
#include "kvstore/KvStore.h"
#include "queryparser/QueryParser.h"
#include "runtime/Async.h"
#include "querymanager/QueryManager.h"
#include "udf/UserDefinedFunction.h"
#include "usr/Users.h"
#include "HashTreeInt.h"
#include "service/TableService.h"

static constexpr const char *moduleName = "hashTreeMgr";

HashTreeMgr *HashTreeMgr::instance = NULL;

HashTreeMgr *
HashTreeMgr::get()
{
    return instance;
}

Status
HashTreeMgr::init()
{
    assert(instance == NULL);
    instance = (HashTreeMgr *) memAllocExt(sizeof(HashTreeMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) HashTreeMgr();

    return instance->initInternal();
}

Status
HashTreeMgr::initInternal()
{
    Status status;
    init_ = true;

    status = LibHashTreeGvm::init();
    return status;
}

// Called once at server shutdown. This function waits for all Xdbs
// to be dropped before it frees up Xdb meta data.
void
HashTreeMgr::destroy()
{
    init_ = false;
    hashTreeNameTable_.removeAll(NULL);
    hashTreeIdTable_.removeAll(&HashTreeEntry::destroy);

    if (LibHashTreeGvm::get()) {
        LibHashTreeGvm::get()->destroy();
    }

    instance->~HashTreeMgr();
    memFree(instance);
    instance = NULL;
}

Status
HashTreeMgr::recoverFromSnapshot(const char *publishedTableName,
                                 RestoreInfo *restoreInfo,
                                 Xid hashTreeId,
                                 const XcalarApiUserId *userId,
                                 int64_t *snapshotBatchIdOut,
                                 Dag *sessionDag)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;

    char *value = NULL;
    size_t valueSize = 0;
    int64_t batchId = 0;
    json_int_t unixTS;
    const char *snapshotName = "";
    Dag *outputDag = NULL;

    char key[XcalarApiMaxKeyLen + 1];
    json_t *recoveryInfo = NULL;
    json_t *snapshotInfo = NULL;
    bool hashTreeCreated = false;

    status = strSnprintf(key,
                         sizeof(key),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishedTableName,
                         SnapshotResultsSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed recoverFromSnapshot for publish table %s %lu user "
                    "%s: %s",
                    publishedTableName,
                    hashTreeId,
                    userId->userIdName,
                    strGetFromStatus(status));

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       key,
                                       &value,
                                       &valueSize);
    if (status == StatusKvEntryNotFound) {
        // no snapshots found for this table, non-fatal
        xSyslog(moduleName,
                XlogInfo,
                "No recovery info for key %s publish table %s %lu user %s",
                key,
                publishedTableName,
                hashTreeId,
                userId->userIdName);
        status = StatusOk;
        goto CommonExit;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed recoverFromSnapshot for publish table %s %lu user "
                    "%s: %s",
                    publishedTableName,
                    hashTreeId,
                    userId->userIdName,
                    strGetFromStatus(status));

    json_error_t err;
    recoveryInfo = json_loads(value, 0, &err);
    if (recoveryInfo == NULL) {
        status = StatusJsonError;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to parse recovery info json, "
                      "source %s line %d, column %d, position %d, '%s' "
                      "recoverFromSnapshot for publish table %s %lu user %s: "
                      "%s",
                      err.source,
                      err.line,
                      err.column,
                      err.position,
                      err.text,
                      publishedTableName,
                      hashTreeId,
                      userId->userIdName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    snapshotInfo = json_object_get(recoveryInfo, LatestSnapshotKey);
    if (snapshotInfo == NULL) {
        // latest snapshot is not found. This case can happen when the first
        // snapshot failed for any reason. This is non-fatal and we should
        // drive on to restore from batchid 0.
        xSyslog(moduleName,
                XlogInfo,
                "No latest snapshot key %s for publish table %s %lu user %s",
                LatestSnapshotKey,
                publishedTableName,
                hashTreeId,
                userId->userIdName);
        status = StatusOk;
        goto CommonExit;
    }

    {
        const char *retName = "";
        json_t *parametersJson = NULL;

        int ret = json_unpack_ex(snapshotInfo,
                                 &err,
                                 0,
                                 SnapInfoJsonUnpackFormatStr,
                                 SnapshotNameKey,
                                 &snapshotName,
                                 SnapshotRetinaNameKey,
                                 &retName,
                                 SnapshotBatchIdKey,
                                 &batchId,
                                 SnapshotUnixTSKey,
                                 &unixTS,
                                 SnapshotRetinaParamsKey,
                                 &parametersJson);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to parse recovery info json, "
                          "source %s line %d, column %d, position %d, '%s' "
                          "recoverFromSnapshot for publish table %s %lu user "
                          "%s: %s",
                          err.source,
                          err.line,
                          err.column,
                          err.position,
                          err.text,
                          publishedTableName,
                          hashTreeId,
                          userId->userIdName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        unsigned numParams = json_array_size(parametersJson);
        if (numParams > XcalarApiMaxNumParameters) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed recoverFromSnapshot for publish table %s "
                          "%lu user %s: %s",
                          publishedTableName,
                          hashTreeId,
                          userId->userIdName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        XcalarApiParameter params[numParams];
        unsigned ii;
        json_t *param;

        json_array_foreach (parametersJson, ii, param) {
            json_t *paramName, *paramValue;

            paramName = json_object_get(param, "paramName");
            BailIfNullMsg(paramName,
                          StatusJsonQueryParseError,
                          moduleName,
                          "Failed recoverFromSnapshot for publish table %s "
                          "%lu user %s: %s",
                          publishedTableName,
                          hashTreeId,
                          userId->userIdName,
                          strGetFromStatus(status));

            paramValue = json_object_get(param, "paramValue");
            BailIfNullMsg(paramName,
                          StatusJsonQueryParseError,
                          moduleName,
                          "Failed recoverFromSnapshot for publish table %s "
                          "%lu user %s: %s",
                          publishedTableName,
                          hashTreeId,
                          userId->userIdName,
                          strGetFromStatus(status));

            status = strStrlcpy(params[ii].parameterName,
                                json_string_value(paramName),
                                sizeof(params[ii].parameterName));
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed recoverFromSnapshot for publish table %s "
                            "%lu user %s: %s",
                            publishedTableName,
                            hashTreeId,
                            userId->userIdName,
                            strGetFromStatus(status));

            status = strStrlcpy(params[ii].parameterValue,
                                json_string_value(paramValue),
                                sizeof(params[ii].parameterValue));
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed recoverFromSnapshot for publish table %s "
                            "%lu user %s: %s",
                            publishedTableName,
                            hashTreeId,
                            userId->userIdName,
                            strGetFromStatus(status));
        }

        status = executeRestoreRetina(retName, numParams, params, &outputDag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed recoverFromSnapshot for publish table %s %lu "
                        "user %s: "
                        "%s",
                        publishedTableName,
                        hashTreeId,
                        userId->userIdName,
                        strGetFromStatus(status));

        // populate snapshot retina name in restore info
        strlcpy(restoreInfo->snapshotRetinaName,
                retName,
                sizeof(restoreInfo->snapshotRetinaName));
    }

    {
        XcalarApiTableInput srcTable;
        DagNodeTypes::Node *node;

        node = outputDag->getLastNode();
        srcTable.tableId = XidInvalid;
        srcTable.xdbId = node->xdbId;
        status = strStrlcpy(srcTable.tableName,
                            snapshotName,
                            sizeof(srcTable.tableName));

        status = createHashTreeInt(srcTable,
                                   publishedTableName,
                                   hashTreeId,
                                   CreateReason::RestoreFromSnap);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed recoverFromSnapshot for publish table %s %lu "
                        "user %s: %s",
                        publishedTableName,
                        hashTreeId,
                        userId->userIdName,
                        strGetFromStatus(status));

        hashTreeCreated = true;

        size_t size, numRows;
        size = node->dagNodeHdr.opDetails.sizeTotal;
        numRows = node->dagNodeHdr.opDetails.numRowsTotal;

        xSyslog(moduleName,
                XlogInfo,
                "Recover publish table %s from snapshot, source table %s "
                "user %s, numRows %lu, batchId %lu",
                publishedTableName,
                srcTable.tableName,
                userId->userIdName,
                numRows,
                batchId);

        status = updateInMemHashTree(srcTable,
                                     hashTreeId,
                                     publishedTableName,
                                     unixTS,
                                     true,
                                     size,
                                     numRows,
                                     batchId,
                                     UpdateReason::RestoreFromSnap);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed recoverFromSnapshot for publish table %s %lu "
                        "user %s: "
                        "%s",
                        publishedTableName,
                        hashTreeId,
                        userId->userIdName,
                        strGetFromStatus(status));
    }

CommonExit:
    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (value) {
        memFree(value);
        value = NULL;
    }

    if (recoveryInfo) {
        json_decref(recoveryInfo);
        recoveryInfo = NULL;
    }

    if (outputDag) {
        status =
            DagLib::get()->destroyDag(outputDag,
                                      DagTypes::DestroyDeleteAndCleanNodes);
        outputDag = NULL;
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed recoverFromSnapshot for publish table %s %lu "
                        "user %s: %s",
                        publishedTableName,
                        hashTreeId,
                        userId->userIdName,
                        strGetFromStatus(status));
    }

    if (status != StatusOk) {
        if (hashTreeCreated) {
            Status status2;
            status2 = destroyHashTreeInt(hashTreeId, publishedTableName, true);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed destroyHashTreeInt in recoverFromSnapshot "
                        "for publish table %s %lu user %s: %s",
                        publishedTableName,
                        hashTreeId,
                        userId->userIdName,
                        strGetFromStatus(status2));
            }
        }
    } else {
        if (batchId) {
            *snapshotBatchIdOut = batchId + 1;
        }
    }

    return status;
}

Status
HashTreeMgr::executeRestoreRetina(const char *retName,
                                  unsigned numParams,
                                  XcalarApiParameter *params,
                                  Dag **outputDag)
{
    Status status = StatusOk;

    XcalarWorkItem *executeWorkItem = NULL;
    XcalarWorkItem *importWorkItem = NULL;
    XcalarApiExecuteRetinaInput *execInput = NULL;
    XcalarApiOutput *listRetinasOutput = NULL;
    XcalarApiOutput *listSessionOutput = NULL;
    XcalarApiOutput *importRetinaOutput = NULL;
    size_t outputSize = 0;
    XcalarApiUserId snapshotUser;

    char key[XcalarApiMaxKeyLen + 1];
    char *value = NULL;
    size_t valueSize = 0;

    strlcpy(snapshotUser.userIdName,
            SnapshotUserName,
            sizeof(snapshotUser.userIdName));
    snapshotUser.userIdUnique = 0;

    status =
        DagLib::get()->listRetinas(retName, &listRetinasOutput, &outputSize);
    BailIfFailed(status);

    if (listRetinasOutput->outputResult.listRetinasOutput.numRetinas == 0) {
        status = strSnprintf(key,
                             sizeof(key),
                             "%s/%s/%s",
                             KvStoreKeyPrefix,
                             retName,
                             SnapshotRestoreDFSuffix);
        BailIfFailed(status);

        status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                           key,
                                           &value,
                                           &valueSize);
        BailIfFailed(status);

        status = UserMgr::get()->list(&snapshotUser,
                                      "*",
                                      &listSessionOutput,
                                      &outputSize);
        BailIfFailed(status);

        XcalarApiSessionActivateInput sessionActivateInput;
        XcalarApiSessionGenericOutput sessionActivateOutput;
        strlcpy(sessionActivateInput.sessionName,
                SnapshotWorkbookName,
                sizeof(sessionActivateInput.sessionName));
        sessionActivateInput.sessionNameLength =
            strlen(sessionActivateInput.sessionName);
        sessionActivateInput.sessionId = 0;
        status = UserMgr::get()->activate(&snapshotUser,
                                          &sessionActivateInput,
                                          &sessionActivateOutput);
        BailIfFailed(status);

        importWorkItem = xcalarApiMakeImportRetinaWorkItem(retName,
                                                           false,
                                                           true,
                                                           SnapshotUserName,
                                                           SnapshotWorkbookName,
                                                           valueSize,
                                                           (uint8_t *) value);
        BailIfNull(importWorkItem);

        status = DagLib::get()
                     ->importRetina(&importWorkItem->input->importRetinaInput,
                                    &importRetinaOutput,
                                    &outputSize);
        BailIfFailed(status);
    }

    executeWorkItem = xcalarApiMakeExecuteRetinaWorkItem(retName,
                                                         retName,
                                                         true,
                                                         Runtime::NameSchedId0,
                                                         TmpTableName,
                                                         numParams,
                                                         params,
                                                         SnapshotUserName,
                                                         SnapshotWorkbookName);
    BailIfNull(executeWorkItem);

    execInput = &executeWorkItem->input->executeRetinaInput;

    status = DagLib::get()->executeRetina(&snapshotUser, execInput, outputDag);
    BailIfFailed(status);

CommonExit:
    if (executeWorkItem) {
        xcalarApiFreeWorkItem(executeWorkItem);
        executeWorkItem = NULL;
    }

    if (importWorkItem) {
        xcalarApiFreeWorkItem(importWorkItem);
        importWorkItem = NULL;
    }

    if (listRetinasOutput) {
        memFree(listRetinasOutput);
        listRetinasOutput = NULL;
    }

    if (listSessionOutput) {
        memFree(listSessionOutput);
        listSessionOutput = NULL;
    }

    if (importRetinaOutput) {
        memFree(importRetinaOutput);
        importRetinaOutput = NULL;
    }

    if (value) {
        memFree(value);
        value = NULL;
    }

    return status;
}

Status
HashTreeMgr::executeUpdateRetina(const char *retinaPath,
                                 const char *retinaName,
                                 const char *publishedTableName,
                                 const XcalarApiUserId *userId,
                                 bool avoidSelfSelects,
                                 Dag **outputDag,
                                 RestoreInfo *restoreInfo,
                                 Dag *sessionDag)
{
    Status status = StatusOk;
    bool retinaMade = false;
    size_t retinaSize, bytesRead;
    XcalarApiImportRetinaInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    XcalarApiExecuteRetinaInput *execInput;
    DagLib::DgRetina *retina = NULL;
    XcalarApiDagOutput *listDagsOut = NULL;
    size_t outputSize;
    XcalarApiOutput *tmpOutput = NULL;
    size_t tmpOutputSize = 0;
    char queryName[XcalarApiMaxTableNameLen + 1];
    int ret;

    // read retina from disk
    int fd = open(retinaPath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Unable to open update file %s: %s",
                retinaPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    retinaSize = lseek(fd, 0, SEEK_END);
    input =
        (XcalarApiImportRetinaInput *) memAlloc(sizeof(*input) + retinaSize);
    BailIfNull(input);
    memZero(input, sizeof(*input));

    status =
        strStrlcpy(input->retinaName, retinaName, sizeof(input->retinaName));
    BailIfFailed(status);

    input->retinaCount = retinaSize;
    input->loadRetinaJson = false;
    input->overwriteExistingUdf = false;

    lseek(fd, 0, SEEK_SET);
    status =
        FileUtils::convergentRead(fd, input->retina, retinaSize, &bytesRead);
    BailIfFailed(status);

    // import it as a dummy retina
    status = DagLib::get()->importRetina(input, &tmpOutput, &tmpOutputSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to import update retina from file %s:%s",
                    retinaPath,
                    strGetFromStatus(status));
    retinaMade = true;

    status = DagLib::get()->getRetinaObj(retinaName, &retina);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to get update retina from file %s:%s",
                    retinaPath,
                    strGetFromStatus(status));

    retina->sessionDag = sessionDag;

    // search for any self selects. These are a problem if we are restoring
    // updates in parallel.
    // keep track of any selects we find as dependent tables
    if (avoidSelfSelects) {
        XcalarApis api;
        api = XcalarApiSelect;

        status = retina->dag->listAvailableNodes("*",
                                                 &listDagsOut,
                                                 &outputSize,
                                                 1,
                                                 &api);
        BailIfFailed(status);

        for (unsigned ii = 0; ii < listDagsOut->numNodes; ii++) {
            XcalarApiSelectInput *selectInput =
                &listDagsOut->node[ii]->input->selectInput;

            if (strcmp(selectInput->srcTable.tableName, publishedTableName) ==
                0) {
                status = StatusSelfSelectRequired;
                goto CommonExit;
            } else {
                RestoreInfo::DependentTable *dep = NULL;
                restoreInfo->lock.lock();
                dep = restoreInfo->dependencies.find(
                    selectInput->srcTable.tableName);
                if (!dep) {
                    dep = new (std::nothrow) RestoreInfo::DependentTable(
                        selectInput->srcTable.tableName);
                }
                restoreInfo->lock.unlock();
                BailIfNull(dep);
            }
        }
    }

    // set queryName name to be unique, instead of reusing retinaName
    ret = snprintf(queryName,
                   sizeof(queryName),
                   "restore-publish-tab-%lu",
                   XidMgr::get()->xidGetNext());
    if (ret < 0 || ret >= (int) sizeof(queryName)) {
        status = StatusNoBufs;
        goto CommonExit;
    }
    workItem = xcalarApiMakeExecuteRetinaWorkItem(retinaName,
                                                  queryName,
                                                  true,
                                                  Runtime::NameSchedId0,
                                                  TmpTableName,
                                                  0,
                                                  NULL,
                                                  NULL,
                                                  NULL,
                                                  NULL);
    BailIfNull(workItem);

    execInput = &workItem->input->executeRetinaInput;

    // execute and return the output dag
    status =
        DagLib::get()->executeRetinaInt(retina, userId, execInput, outputDag);
    BailIfFailed(status);

CommonExit:
    if (input) {
        memFree(input);
        input = NULL;
    }

    if (listDagsOut) {
        memFree(listDagsOut);
        listDagsOut = NULL;
    }

    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }

    if (retinaMade) {
        Status status2 = DagLib::get()->deleteRetina(retinaName);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete retina %s: %s",
                    retinaName,
                    strGetFromStatus(status2));
        }
        retinaMade = false;
    }

    if (workItem) {
        // we attempted to execute retina as workItem is not NULL
        Status status2 = QueryManager::get()->requestQueryDelete(queryName);
        if (status2 != StatusOk && status2 != StatusStatsCollectionInProgress) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete query state '%s': %s",
                    queryName,
                    strGetFromStatus(status2));
        }
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (retina != NULL) {
        DagLib::get()->putRetinaInt(retina);
        retina = NULL;
    }

    if (tmpOutput != NULL) {
        memFree(tmpOutput);
        tmpOutput = NULL;
    }

    return status;
}

Status
HashTreeMgr::restorePublishedTable(const char *publishedTableName,
                                   const XcalarApiUserId *userId,
                                   RestoreInfo **restoreInfoOut,
                                   Dag *sessionDag)
{
    Status status = StatusOk;
    int64_t batchIdCurrent = 0;
    LibNsTypes::NsHandle nsHandle;
    HashTreeRefHandle hashTreeHandle;
    bool hashTreeHandleInit = false;
    bool activeSet = false;
    Xid hashTreeId = XidInvalid;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    XcalarApiUdfContainer emptySessionContainer;
    memZero(&emptySessionContainer, sizeof(XcalarApiUdfContainer));
    HashTreeRecord record(XidInvalid,
                          HashTreeRecord::State::Active,
                          HashTreeRecord::Restore::InProgress,
                          HashTree::InvalidBatchId,
                          HashTree::InvalidBatchId,
                          &emptySessionContainer);
    RestoreInfo *restoreInfo = NULL;
    int64_t coalesceBatchId = HashTree::InvalidBatchId;
    HashTreeEntry *htEntry = NULL;
    bool needsSchemaPersisted = false;

    status = getFQN(fullyQualName, sizeof(fullyQualName), publishedTableName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to restorePublishedTable hashtree %s: %s",
                publishedTableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = openHandleToHashTree(publishedTableName,
                                  LibNsTypes::ReadSharedWriteExclWriter,
                                  &hashTreeHandle,
                                  OpenRetry::False);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restorePublishedTable hashtree %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    hashTreeHandleInit = true;
    hashTreeId = hashTreeHandle.hashTreeId;
    record.hashTreeId_ = hashTreeHandle.hashTreeId;
    if (hashTreeHandle.active) {
        status = StatusOk;
        xSyslog(moduleName,
                XlogInfo,
                "Publish table %s %lu is already active",
                publishedTableName,
                hashTreeId);
        goto CommonExit;
    }

    // start restoring, first set up restore info
    restoreInfo = new (std::nothrow) RestoreInfo(publishedTableName);
    BailIfNull(restoreInfo);

    lock_.lock();
    status = restoresInProgress_.insert(restoreInfo);
    assert(status == StatusOk);
    lock_.unlock();

    // Update active = true, restoring = true.
    nsHandle =
        LibNs::get()->updateNsObject(hashTreeHandle.nsHandle, &record, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restorePublishedTable hashtree %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    activeSet = true;

    htEntry = getHashTreeEntryById(hashTreeId);
    needsSchemaPersisted = !htEntry->schemaAvailable;

    status = recoverFromSnapshot(publishedTableName,
                                 restoreInfo,
                                 hashTreeId,
                                 userId,
                                 &batchIdCurrent,
                                 sessionDag);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to restorePublishedTable %s from snapshot: %s",
                      publishedTableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    char filePath[XcalarApiMaxPathLen + 1];
    status = strSnprintf(filePath,
                         sizeof(filePath),
                         "%s/%s",
                         LogLib::get()->publishedTablePath_,
                         publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restorePublishedTable hashtree %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    status = restoreUpdatesFromDir(filePath,
                                   publishedTableName,
                                   restoreInfo,
                                   hashTreeId,
                                   userId,
                                   batchIdCurrent,
                                   &batchIdCurrent,
                                   sessionDag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restorePublishedTable hashtree %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    status = getPersistentCoalesceBatchId(publishedTableName,
                                          hashTreeId,
                                          &coalesceBatchId);
    if (status == StatusKvEntryNotFound) {
        // Not finding persistent coalesce batch Id info is not a
        // hard error.
        status = StatusOk;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restore coalesce publishedTableName %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    if (coalesceBatchId > 0) {
        status = coalesceHashTreeInternal(publishedTableName,
                                          hashTreeId,
                                          coalesceBatchId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restore coalesce publishedTableName %s: %s",
                        publishedTableName,
                        strGetFromStatus(status));
    }

    // Update active = true, restoring = false.
    new (&record) HashTreeRecord(hashTreeId,
                                 HashTreeRecord::State::Active,
                                 HashTreeRecord::Restore::NotInProgress,
                                 0,
                                 batchIdCurrent,
                                 &emptySessionContainer);
    nsHandle =
        LibNs::get()->updateNsObject(hashTreeHandle.nsHandle, &record, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restorePublishedTable hashtree %s: %s",
                    publishedTableName,
                    strGetFromStatus(status));

    assert(status == StatusOk);

    if (needsSchemaPersisted) {
        // Handle the upgrade case when the Publish tables did not have
        // schema persisted and in the newer bits, during restore, take this
        // as an opportinity to persist the schema.
        status = persistSchema(publishedTableName,
                               hashTreeId,
                               htEntry->hashTree->getXdbMeta());
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restorePublishedTable hashtree %s: %s",
                        publishedTableName,
                        strGetFromStatus(status));
    }

CommonExit:
    if (status != StatusOk) {
        if (hashTreeHandleInit) {
            Status status2;

            if (activeSet) {
                // Restore failed, set active = false, restoring = false.
                new (&record)
                    HashTreeRecord(hashTreeId,
                                   HashTreeRecord::State::Inactive,
                                   HashTreeRecord::Restore::NotInProgress,
                                   HashTree::InvalidBatchId,
                                   HashTree::InvalidBatchId,
                                   &emptySessionContainer);
                nsHandle = LibNs::get()->updateNsObject(hashTreeHandle.nsHandle,
                                                        &record,
                                                        &status2);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to reset state in restorePublishedTable "
                            "hashtree %s: %s",
                            publishedTableName,
                            strGetFromStatus(status2));
                }

                activeSet = false;
            }

            status2 = destroyHashTreeInt(hashTreeId,
                                         publishedTableName,
                                         true);  // inactivate
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to destroyHashTreeInt in restorePublishedTable "
                        "hashtree %s: %s",
                        publishedTableName,
                        strGetFromStatus(status2));
            }
        }
    }

    // clean up restore info
    if (restoreInfo) {
        lock_.lock();
        restoresInProgress_.remove(publishedTableName);
        lock_.unlock();
        *restoreInfoOut = restoreInfo;
    }

    if (hashTreeHandleInit) {
        closeHandleToHashTree(&hashTreeHandle, hashTreeId, publishedTableName);
        hashTreeHandleInit = false;
    }
    return status;
}

struct UpdateInfo {
    IntHashTableHook hook;
    int64_t batchId;
    time_t unixTS;

    UpdateInfo(int b, time_t t)
    {
        batchId = b;
        unixTS = t;
    }

    // During restores, we don't have to deal with negative batch IDs.
    uint64_t getBatchId() const { return (uint64_t) batchId; }
};

Status
HashTreeMgr::restoreUpdatesFromDir(const char *dirName,
                                   const char *publishedTableName,
                                   RestoreInfo *restoreInfo,
                                   Xid hashTreeId,
                                   const XcalarApiUserId *userId,
                                   int64_t batchIdStart,
                                   int64_t *restoreBatchId,
                                   Dag *sessionDag)
{
    Status status = StatusOk;
    DIR *publishedTableDir = NULL;
    char updatePattern[XcalarApiMaxPathLen + 1];
    int64_t maxBatch = HashTree::InvalidBatchId;
    Future<RestoreUpdateOutput> *updateFutures = NULL;
    unsigned maxOutstandingUpdates =
        XcalarConfig::get()->pubTableRestoreConcurrency_;

    IntHashTable<uint64_t,
                 UpdateInfo,
                 &UpdateInfo::hook,
                 &UpdateInfo::getBatchId,
                 11,
                 hashIdentity>
        updateInfos;

    snprintf(updatePattern, sizeof(updatePattern), "%s*", UpdateFilePrefix);

    publishedTableDir = opendir(dirName);
    if (publishedTableDir == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed restoreUpdatesFromDir published Table %s %lu folder %s "
                "read failed: %s",
                publishedTableName,
                hashTreeId,
                dirName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // find the max update batchId
    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(publishedTableDir);
        if (currentFile == NULL) {
            if (errno != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed restoreUpdatesFromDir published "
                        "Table %s %lu folder %s: %s",
                        publishedTableName,
                        hashTreeId,
                        dirName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            // End of directory.
            break;
        }

        if ((strcmp(currentFile->d_name, ".") == 0) ||
            (strcmp(currentFile->d_name, "..") == 0)) {
            // Ignore non-real directories
            continue;
        }

        if (!strMatch(updatePattern, currentFile->d_name)) {
            continue;
        }

        int64_t batchId;
        time_t unixTS;

        int ret =
            sscanf(currentFile->d_name, UpdateFilePattern, &batchId, &unixTS);
        assert(ret == 2);
        if (ret != 2) {
            // XXX This appears like a hard error to abort this.
            xSyslog(moduleName,
                    XlogErr,
                    "Error parsing update file name %s, publish table "
                    " %s %lu skipping",
                    currentFile->d_name,
                    publishedTableName,
                    hashTreeId);
            continue;
        }

        if (batchId >= batchIdStart) {
            UpdateInfo *info = new (std::nothrow) UpdateInfo(batchId, unixTS);
            BailIfNullMsg(info,
                          StatusNoMem,
                          moduleName,
                          "Failed restoreUpdatesFromDir published Table %s %lu "
                          " folder %s: %s",
                          publishedTableName,
                          hashTreeId,
                          dirName,
                          strGetFromStatus(status));

            verifyOk(updateInfos.insert(info));

            if (batchId > maxBatch) {
                maxBatch = batchId;
            }
        }
    }

    if (batchIdStart > maxBatch) {
        // no updates to restore
        goto CommonExit;
    }

    unsigned numUpdates;
    numUpdates = maxBatch - batchIdStart + 1;

    updateFutures = new (std::nothrow) Future<RestoreUpdateOutput>[numUpdates];
    BailIfNull(updateFutures);

    Atomic64 outstandingUpdates;
    atomicWrite64(&outstandingUpdates, 0);

    int64_t curFuture;
    curFuture = 0;

    // schedule batches of update retinas to run in parallel,
    // generating output tables that will be applied as updates
    for (int64_t ii = 0; ii < numUpdates; ii++) {
        int64_t curBatch = ii + batchIdStart;

        if (restoreInfo->checkCancelled()) {
            status = StatusCanceled;
            goto CommonExit;
        }

        if (atomicRead64(&outstandingUpdates) >= maxOutstandingUpdates) {
            // there are too many outstanding updates, let's wait for
            // one to complete before issuing more

            status = applyUpdateFuture(hashTreeId,
                                       dirName,
                                       publishedTableName,
                                       restoreInfo,
                                       userId,
                                       curFuture + batchIdStart,
                                       sessionDag,
                                       &updateFutures[curFuture]);
            BailIfFailed(status);

            curFuture++;
        }

        UpdateInfo *info = updateInfos.find(curBatch);
        assert(info);
        if (info == NULL) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to restore published Table %s %lu "
                          "folder %s, missing update %ld: %s",
                          publishedTableName,
                          hashTreeId,
                          dirName,
                          curBatch,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        atomicInc64(&outstandingUpdates);
        status = asyncObj(&updateFutures[curBatch - batchIdStart],
                          this,
                          &HashTreeMgr::restoreUpdate,
                          dirName,
                          publishedTableName,
                          userId,
                          hashTreeId,
                          curBatch,
                          info->unixTS,
                          true,
                          &outstandingUpdates,
                          restoreInfo,
                          sessionDag);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to schedule update %ld "
                        "published Table %s %lu "
                        "folder %s: %s",
                        curBatch,
                        publishedTableName,
                        hashTreeId,
                        dirName,
                        strGetFromStatus(status));

        updateInfos.remove(curBatch);
        delete info;
    }

    // wait for remaining futures to complete
    for (; curFuture < numUpdates; curFuture++) {
        status = applyUpdateFuture(hashTreeId,
                                   dirName,
                                   publishedTableName,
                                   restoreInfo,
                                   userId,
                                   curFuture + batchIdStart,
                                   sessionDag,
                                   &updateFutures[curFuture]);
        BailIfFailed(status);
    }
    *restoreBatchId = maxBatch;

CommonExit:
    if (publishedTableDir != NULL) {
        closedir(publishedTableDir);
        publishedTableDir = NULL;
    }

    if (updateFutures) {
        // clean out any unapplied futures
        for (; curFuture < numUpdates; curFuture++) {
            assert(status != StatusOk);

            if (updateFutures[curFuture].valid()) {
                updateFutures[curFuture].wait();
                RestoreUpdateOutput out = updateFutures[curFuture].get();
                if (out.outputDag) {
                    Status status2;
                    status2 =
                        DagLib::get()
                            ->destroyDag(out.outputDag,
                                         DagTypes::DestroyDeleteAndCleanNodes);
                    (void) status2;
                }
            }
        }

        delete[] updateFutures;
        updateFutures = NULL;
    }

    return status;
}

HashTreeMgr::RestoreUpdateOutput
HashTreeMgr::restoreUpdate(const char *dirName,
                           const char *publishedTableName,
                           const XcalarApiUserId *userId,
                           Xid hashTreeId,
                           int64_t batchId,
                           time_t unixTS,
                           bool avoidSelfSelects,
                           Atomic64 *outstandingUpdates,
                           RestoreInfo *restoreInfo,
                           Dag *sessionDag)
{
    RestoreUpdateOutput output;
    Status status;
    char filePath[XcalarApiMaxPathLen + 1];
    char updateName[XcalarApiMaxPathLen + 1];
    char tmpRetName[XcalarApiMaxPathLen + 1];
    DagNodeTypes::Node *node;

    status = strSnprintf(updateName,
                         sizeof(updateName),
                         UpdateFilePattern,
                         batchId,
                         unixTS);
    BailIfFailed(status);

    status =
        strSnprintf(filePath, sizeof(filePath), "%s/%s", dirName, updateName);
    BailIfFailed(status);

    status = strSnprintf(tmpRetName,
                         sizeof(tmpRetName),
                         "%s%s-%ld",
                         TmpUpdateRetinaNamePrefix,
                         publishedTableName,
                         batchId);
    BailIfFailed(status);

    status = executeUpdateRetina(filePath,
                                 tmpRetName,
                                 publishedTableName,
                                 userId,
                                 avoidSelfSelects,
                                 &output.outputDag,
                                 restoreInfo,
                                 sessionDag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed execute update %ld dirName %s publishedTableName "
                    "%s user %s: %s",
                    batchId,
                    dirName,
                    publishedTableName,
                    userId->userIdName,
                    strGetFromStatus(status));

    node = output.outputDag->getLastNode();
    output.srcTable.tableId = XidInvalid;
    output.srcTable.xdbId = node->xdbId;
    status = strStrlcpy(output.srcTable.tableName,
                        updateName,
                        sizeof(output.srcTable.tableName));
    BailIfFailed(status);

    output.size = node->dagNodeHdr.opDetails.sizeTotal;
    output.numRows = node->dagNodeHdr.opDetails.numRowsTotal;

CommonExit:
    output.status = status;
    output.unixTS = unixTS;
    output.batchId = batchId;

    if (outstandingUpdates) {
        atomicDec64(outstandingUpdates);
    }

    return output;
}

Status
HashTreeMgr::applyUpdateFuture(Xid hashTreeId,
                               const char *dirName,
                               const char *publishedTableName,
                               RestoreInfo *restoreInfo,
                               const XcalarApiUserId *userId,
                               int64_t batchId,
                               Dag *sessionDag,
                               Future<RestoreUpdateOutput> *future)
{
    Status status;
    RestoreUpdateOutput out;

    future->wait();
    out = future->get();
    status = out.status;

    if (restoreInfo->checkCancelled()) {
        status = StatusCanceled;
    }

    if (status == StatusSelfSelectRequired) {
        // we couldn't restore this batch in the future due to self dependencies
        // restore it in line here
        out = restoreUpdate(dirName,
                            publishedTableName,
                            userId,
                            hashTreeId,
                            batchId,
                            out.unixTS,
                            false,
                            NULL,
                            restoreInfo,
                            sessionDag);
        status = out.status;
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restored update %ld published Table %s %lu: %s",
                    batchId,
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));
    assert(out.batchId == batchId);

    if (batchId == 0) {
        status =
            createHashTreeInt(out.srcTable,
                              publishedTableName,
                              hashTreeId,
                              HashTreeMgr::CreateReason::RestoreFromXcRoot);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restore create publishedTableName %s: %s",
                        publishedTableName,
                        strGetFromStatus(status));
    }

    status = updateInMemHashTree(out.srcTable,
                                 hashTreeId,
                                 publishedTableName,
                                 out.unixTS,
                                 true,
                                 out.size,
                                 out.numRows,
                                 batchId,
                                 UpdateReason::RestoreFromXcRoot);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed restore update %ld publishedTableName %s: %s",
                    batchId,
                    publishedTableName,
                    strGetFromStatus(status));

    if (batchId == 0) {
        status = coalesceHashTreeInternal(publishedTableName, hashTreeId, 0);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restore coalesce publishedTableName %s: %s",
                        publishedTableName,
                        strGetFromStatus(status));
    }

CommonExit:
    if (out.outputDag) {
        Status status2;
        status2 =
            DagLib::get()->destroyDag(out.outputDag,
                                      DagTypes::DestroyDeleteAndCleanNodes);
        (void) status2;
    }

    return status;
}

Status
HashTreeMgr::getPersistedTables(const char *namePattern,
                                PersistedInfoHashTable *ht)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    LogLib *logLib = LogLib::get();
    PersistedInfo *info = NULL;

    dirIter = opendir(logLib->publishedTablePath_);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed getPersistedTables %s dir %s: %s",
                namePattern,
                logLib->publishedTablePath_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            if (errno != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed getPersistedTables %s dir %s: %s",
                        namePattern,
                        logLib->publishedTablePath_,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            // End of directory.
            break;
        }

        if ((strcmp(currentFile->d_name, ".") == 0) ||
            (strcmp(currentFile->d_name, "..") == 0)) {
            // Ignore non-real directories
            continue;
        }

        if (!strMatch(namePattern, currentFile->d_name)) {
            continue;
        }

        // count total number of updates
        int64_t numUpdates = 0;
        DIR *dirp;
        struct dirent *entry;
        char fullPath[XcalarApiMaxPathLen + 1];

        snprintf(fullPath,
                 sizeof(fullPath),
                 "%s/%s",
                 logLib->publishedTablePath_,
                 currentFile->d_name);

        dirp = opendir(fullPath);
        if (dirp == NULL) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Could not open published table dir %s name pattern %s: %s",
                    currentFile->d_name,
                    namePattern,
                    strGetFromStatus(status));
            continue;
        }

        while ((entry = readdir(dirp)) != NULL) {
            int64_t updateNum;
            time_t unixTS;
            int ret =
                sscanf(entry->d_name, UpdateFilePattern, &updateNum, &unixTS);
            if (ret == 2) {
                numUpdates++;
            }
        }
        closedir(dirp);

        info =
            new (std::nothrow) PersistedInfo(currentFile->d_name, numUpdates);
        BailIfNullMsg(info,
                      StatusNoMem,
                      moduleName,
                      "Failed getPersistedTables %s dir %s name %s: %s",
                      namePattern,
                      logLib->publishedTablePath_,
                      currentFile->d_name,
                      strGetFromStatus(status));

        status = ht->insert(info);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getPersistedTables %s dir %s name %s: %s",
                        namePattern,
                        logLib->publishedTablePath_,
                        currentFile->d_name,
                        strGetFromStatus(status));
        info = NULL;
    }

    status = StatusOk;
CommonExit:
    if (info != NULL) {
        delete info;
        info = NULL;
    }
    if (dirIter != NULL) {
        closedir(dirIter);
        dirIter = NULL;
    }

    return status;
}

Status
HashTreeMgr::unpersistUpdate(const char *publishedTableName,
                             Xid hashTreeId,
                             int64_t batchId,
                             time_t unixTS)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen + 1];
    int ret;

    status = strSnprintf(fullPath,
                         sizeof(fullPath),
                         "%s/%s/" UpdateFilePattern,
                         LogLib::get()->publishedTablePath_,
                         publishedTableName,
                         batchId,
                         unixTS);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistUpdate %s %lu batch %ld: %s",
                    publishedTableName,
                    hashTreeId,
                    batchId,
                    strGetFromStatus(status));

    ret = unlink(fullPath);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        if (status == StatusNoEnt) {
            // File not found is not an error.
            status = StatusOk;
            goto CommonExit;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed unpersistPublishedTable %s %lu: %s",
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    // XXX Any failed attempt to unpersist an update could lead to inconsistent
    // publish table after a restore.
    // XXX Xc-13063 Once we use KvStore for this, it should be possible to
    // revert an update atomically.
    return status;
}

Status
HashTreeMgr::unpersistPublishedTable(const char *publishedTableName,
                                     Xid hashTreeId)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen + 1];
    DIR *publishedTableDir = NULL;
    int ret;
    char ssInfoKey[XcalarApiMaxKeyLen + 1];
    char ssResultsKey[XcalarApiMaxKeyLen + 1];
    char coalesceKey[XcalarApiMaxKeyLen + 1];
    char schemaKey[XcalarApiMaxKeyLen + 1];

    status = strSnprintf(ssResultsKey,
                         sizeof(ssResultsKey),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishedTableName,
                         SnapshotResultsSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = strSnprintf(ssInfoKey,
                         sizeof(ssInfoKey),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishedTableName,
                         SnapshotInfoSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = strSnprintf(coalesceKey,
                         sizeof(coalesceKey),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishedTableName,
                         CoalesceSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = strSnprintf(schemaKey,
                         sizeof(schemaKey),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishedTableName,
                         SchemaSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = strSnprintf(fullPath,
                         sizeof(fullPath),
                         "%s/%s",
                         LogLib::get()->publishedTablePath_,
                         publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    publishedTableDir = opendir(fullPath);
    if (publishedTableDir == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed unpersistPublishedTable %s %lu, published Table folder "
                "%s read failed: %s",
                publishedTableName,
                hashTreeId,
                fullPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // delete all files in the directory
    while (true) {
        errno = 0;
        struct dirent *currentFile = readdir(publishedTableDir);
        if (currentFile == NULL) {
            if (errno != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed unpersistPublishedTable %s %lu: %s",
                        publishedTableName,
                        hashTreeId,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            break;
        }

        if ((strcmp(currentFile->d_name, ".") == 0) ||
            (strcmp(currentFile->d_name, "..") == 0)) {
            // Ignore non-real directories
            continue;
        }

        status = strSnprintf(fullPath,
                             sizeof(fullPath),
                             "%s/%s/%s",
                             LogLib::get()->publishedTablePath_,
                             publishedTableName,
                             currentFile->d_name);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed unpersistPublishedTable %s %lu: %s",
                        publishedTableName,
                        hashTreeId,
                        strGetFromStatus(status));

        ret = unlink(fullPath);
        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = strSnprintf(fullPath,
                         sizeof(fullPath),
                         "%s/%s",
                         LogLib::get()->publishedTablePath_,
                         publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed unpersistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    ret = rmdir(fullPath);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed unpersistPublishedTable %s %lu: %s",
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // remove any lingering state in the kvStore (ie. snapshots)
    status = KvStoreLib::get()->del(XidMgr::XidGlobalKvStore,
                                    ssResultsKey,
                                    KvStoreOptNone);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Failed key delete %s, unpersistPublishedTable %s %lu: %s",
                ssResultsKey,
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        // non-fatal
        status = StatusOk;
    }

    status = KvStoreLib::get()->del(XidMgr::XidGlobalKvStore,
                                    ssInfoKey,
                                    KvStoreOptNone);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Failed key delete %s, unpersistPublishedTable %s %lu: %s",
                ssInfoKey,
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        // non-fatal
        status = StatusOk;
    }

    status = KvStoreLib::get()->del(XidMgr::XidGlobalKvStore,
                                    coalesceKey,
                                    KvStoreOptNone);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Failed key delete %s, unpersistPublishedTable %s %lu: %s",
                coalesceKey,
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        // non-fatal
        status = StatusOk;
    }

    status = KvStoreLib::get()->del(XidMgr::XidGlobalKvStore,
                                    schemaKey,
                                    KvStoreOptNone);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Failed key delete %s, unpersistPublishedTable %s %lu: %s",
                schemaKey,
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        // non-fatal
        status = StatusOk;
    }

    {
        // remove from dependencies list
        PublishDependencyUpdateInput input;
        input.isDelete = true;
        input.numParents = 0;
        strlcpy(input.name, publishedTableName, sizeof(input.name));

        status =
            KvStoreLib::get()->updatePublishDependency(&input, sizeof(input));
        BailIfFailed(status);
    }

CommonExit:
    if (publishedTableDir != NULL) {
        closedir(publishedTableDir);
        publishedTableDir = NULL;
    }

    return status;
}

Status
HashTreeMgr::persistPublishedTable(XcalarApiTableInput firstUpdate,
                                   Dag *dag,
                                   time_t unixTS,
                                   const char *publishedTableName,
                                   Xid hashTreeId)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen + 1];
    int ret;

    status = strSnprintf(fullPath,
                         sizeof(fullPath),
                         "%s/%s",
                         LogLib::get()->publishedTablePath_,
                         publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    ret = mkdir(fullPath, S_IRWXU);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Cannot create '%s' directory in Xcalar root for "
                      "publish table %s(%lu): %s",
                      fullPath,
                      publishedTableName,
                      hashTreeId,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    // Check r/w permissions on folder
    ret = access(fullPath, R_OK | W_OK);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Read/write access not permitted for %s, "
                "publish table %s %lu: %s",
                fullPath,
                publishedTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = persistUpdate(firstUpdate,
                           dag,
                           unixTS,
                           0,
                           publishedTableName,
                           hashTreeId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persistPublishedTable %s %lu: %s",
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
HashTreeMgr::persistUpdate(XcalarApiTableInput srcTable,
                           Dag *dag,
                           time_t unixTS,
                           int64_t batchId,
                           const char *publishedTableName,
                           Xid hashTreeId)
{
    Status status = StatusOk;
    char fullPath[XcalarApiMaxPathLen + 1];
    char tmpRetName[XcalarApiMaxPathLen + 1];
    int fd = -1;
    XcalarWorkItem *workItem = NULL;
    XcalarApiMakeRetinaInput *makeInput;
    RetinaDst *table = NULL;
    XdbMeta *xdbMeta;
    unsigned numCols;
    bool retinaMade = false;
    XcalarApiExportRetinaInput *exportRetinaInput;
    XcalarApiOutput *output = NULL;
    size_t outputSize;

    status = strSnprintf(tmpRetName,
                         sizeof(tmpRetName),
                         "%s%s",
                         TmpUpdateRetinaNamePrefix,
                         publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persistUpdate src table %s publish table %s %lu: "
                    "%s",
                    srcTable.tableName,
                    publishedTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    // extract column info and convert the srcTable into a retina
    status = XdbMgr::get()->xdbGet(srcTable.xdbId, NULL, &xdbMeta);
    BailIfFailed(status);

    numCols = xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();

    table = (RetinaDst *) memAlloc(sizeof(*table) +
                                   numCols * sizeof(*table->columns));
    BailIfNull(table);

    table->numColumns = numCols;
    strlcpy(table->target.name, srcTable.tableName, sizeof(table->target.name));
    table->target.nodeId = srcTable.tableId;
    table->target.xid = srcTable.xdbId;
    table->target.isTable = true;

    for (unsigned ii = 0; ii < numCols; ii++) {
        strlcpy(table->columns[ii].name,
                xdbMeta->kvNamedMeta.valueNames_[ii],
                sizeof(table->columns[ii].name));

        strlcpy(table->columns[ii].headerAlias,
                xdbMeta->kvNamedMeta.valueNames_[ii],
                sizeof(table->columns[ii].headerAlias));
    }

    workItem = xcalarApiMakeMakeRetinaWorkItem(tmpRetName, 1, &table, 0, NULL);
    BailIfNull(workItem);

    makeInput = &workItem->input->makeRetinaInput;
    xcalarApiDeserializeRetinaInput(makeInput, workItem->inputSize);

    status =
        DagLib::get()->makeRetina(dag, makeInput, workItem->inputSize, true);
    BailIfFailed(status);

    retinaMade = true;

    // export the newly created retina to disk
    xcalarApiFreeWorkItem(workItem);
    workItem = xcalarApiMakeExportRetinaWorkItem(tmpRetName);
    BailIfNull(workItem);

    exportRetinaInput = &workItem->input->exportRetinaInput;

    status =
        DagLib::get()->exportRetina(exportRetinaInput, &output, &outputSize);
    BailIfFailed(status);

    status = strSnprintf(fullPath,
                         sizeof(fullPath),
                         "%s/%s/" UpdateFilePattern,
                         LogLib::get()->publishedTablePath_,
                         publishedTableName,
                         batchId,
                         unixTS);
    BailIfFailed(status);

    fd = open(fullPath,
              O_CREAT | O_CLOEXEC | O_WRONLY | O_TRUNC,
              S_IRWXU | S_IRWXG | S_IRWXO);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogErr, "Failed to create file %s", fullPath);
        goto CommonExit;
    }

    XcalarApiExportRetinaOutput *retinaOut;
    retinaOut = &output->outputResult.exportRetinaOutput;

    status = FileUtils::convergentWrite(fd,
                                        retinaOut->retina,
                                        retinaOut->retinaCount);
    BailIfFailed(status);

CommonExit:
    if (table) {
        memFree(table);
        table = NULL;
    }

    if (workItem) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (output) {
        memFree(output);
        output = NULL;
    }

    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }

    if (retinaMade) {
        Status status2 = DagLib::get()->deleteRetina(tmpRetName);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete retina %s: %s",
                    tmpRetName,
                    strGetFromStatus(status2));
        }
        retinaMade = false;
    }
    return status;
}

Status
HashTreeMgr::changeOwnership(const char *publishedTableName,
                             const char *userIdName,
                             const char *sessioName)
{
    Status status = StatusOk;
    bool hashTreeHandleInit = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    HashTreeRefHandle hashTreeHandle;
    XcalarApiUdfContainer sessionContainer;
    memZero(&sessionContainer, sizeof(XcalarApiUdfContainer));
    LibNs *libNs = LibNs::get();
    UserMgr *userMgr = UserMgr::get();
    HashTreeRecord htRecord;
    LibNsTypes::NsHandle nsHandle;

    status = getFQN(fullyQualName, sizeof(fullyQualName), publishedTableName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed hashtree %s ownership change user %s session %s: "
                    "%s",
                    publishedTableName,
                    userIdName,
                    sessioName,
                    strGetFromStatus(status));

    strlcpy(sessionContainer.userId.userIdName,
            userIdName,
            sizeof(sessionContainer.userId.userIdName));

    strlcpy(sessionContainer.sessionInfo.sessionName,
            sessioName,
            sizeof(sessionContainer.sessionInfo.sessionName));

    sessionContainer.sessionInfo.sessionNameLength =
        strnlen(sessionContainer.sessionInfo.sessionName,
                sizeof(sessionContainer.sessionInfo.sessionName));

    status = userMgr->getSessionId(&sessionContainer.userId,
                                   &sessionContainer.sessionInfo);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed hashtree %s ownership change user %s session %s: "
                    "%s",
                    publishedTableName,
                    userIdName,
                    sessioName,
                    strGetFromStatus(status));

    status = openHandleToHashTree(publishedTableName,
                                  LibNsTypes::WriterExcl,
                                  &hashTreeHandle,
                                  OpenRetry::False);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed hashtree %s ownership change user %s session %s: "
                    "%s",
                    publishedTableName,
                    userIdName,
                    sessioName,
                    strGetFromStatus(status));

    hashTreeHandleInit = true;

    new (&htRecord)
        HashTreeRecord(hashTreeHandle.hashTreeId,
                       hashTreeHandle.active ? HashTreeRecord::State::Active
                                             : HashTreeRecord::State::Inactive,
                       hashTreeHandle.restoring
                           ? HashTreeRecord::Restore::InProgress
                           : HashTreeRecord::Restore::NotInProgress,
                       hashTreeHandle.oldestBatchId,
                       hashTreeHandle.currentBatchId,
                       &sessionContainer);

    nsHandle =
        libNs->updateNsObject(hashTreeHandle.nsHandle, &htRecord, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed hashtree %s ownership change user %s session %s: "
                    "%s",
                    publishedTableName,
                    userIdName,
                    sessioName,
                    strGetFromStatus(status));

CommonExit:
    if (hashTreeHandleInit) {
        closeHandleToHashTree(&hashTreeHandle,
                              hashTreeHandle.hashTreeId,
                              publishedTableName);
        hashTreeHandleInit = false;
    }
    return status;
}

Status
HashTreeMgr::createHashTreeInt(XcalarApiTableInput srcTable,
                               const char *publishedTableName,
                               Xid hashTreeId,
                               HashTreeMgr::CreateReason createReason)
{
    Status status;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    LibHashTreeGvm::GvmInput *input = NULL;
    Gvm::Payload *gPayload = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed to create hashtree %s src table %s: %s",
                  publishedTableName,
                  srcTable.tableName,
                  strGetFromStatus(status));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed to create hashtree %s src table %s: %s",
                  publishedTableName,
                  srcTable.tableName,
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->srcTable = srcTable;
    input->hashTreeId = hashTreeId;
    input->createReason = (unsigned) createReason;
    strlcpy(input->pubTableName,
            publishedTableName,
            sizeof(input->pubTableName));

    // create the hash tree
    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::Create,
                   sizeof(LibHashTreeGvm::GvmInput));
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
                    "Failed to create hashtree %s src table %s: %s",
                    publishedTableName,
                    srcTable.tableName,
                    strGetFromStatus(status));

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
HashTreeMgr::getPersistedSchemaHelper(const char *publishTableName,
                                      Xid hashTreeId,
                                      json_t *jsonSchema,
                                      XcalarApiColumnInfo *colInfo,
                                      size_t numCols)
{
    json_t *jsonColumn = NULL, *jsonColumnName = NULL, *jsonColumnType = NULL;
    Status status = StatusOk;

    for (size_t ii = 0; ii < numCols; ii++) {
        jsonColumn = json_array_get(jsonSchema, ii);
        BailIfNullMsg(jsonColumn,
                      StatusNoMem,
                      moduleName,
                      "Failed get persisted schema for publish table %s %lu: "
                      "%s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        jsonColumnName = json_object_get(jsonColumn, SchemaColumnName);
        BailIfNullMsg(jsonColumnName,
                      StatusNoMem,
                      moduleName,
                      "Failed get persisted schema for publish table %s %lu: "
                      "%s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        if (strlcpy(colInfo[ii].name,
                    json_string_value(jsonColumnName),
                    sizeof(colInfo[ii].name)) >= sizeof(colInfo[ii].name)) {
            status = StatusOverflow;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed get persisted schema for publish table %s %lu: "
                    "%s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        jsonColumnType = json_object_get(jsonColumn, SchemaColumnType);
        BailIfNullMsg(jsonColumnType,
                      StatusNoMem,
                      moduleName,
                      "Failed get persisted schema for publish table %s %lu: "
                      "%s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        DfFieldType type;
        type = strToDfFieldType(json_string_value(jsonColumnType));
        if (!isValidDfFieldType(type)) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed get persisted schema for publish table %s %lu: "
                    "%s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        colInfo[ii].type = type;
    }
CommonExit:
    return status;
}

Status
HashTreeMgr::getPersistedSchema(const char *publishTableName,
                                Xid hashTreeId,
                                XcalarApiColumnInfo **retKeys,
                                size_t *retNumKeys,
                                XcalarApiColumnInfo **retValues,
                                size_t *retNumValues)
{
    Status status = StatusOk;
    XcalarApiColumnInfo *keys = NULL;
    size_t numKeys = 0;
    size_t numValues = 0;
    XcalarApiColumnInfo *values = NULL;
    char kvKey[XcalarApiMaxKeyLen + 1];
    char *kvValue = NULL;
    size_t kvValueSize = 0;
    json_error_t err;
    json_t *jsonSchema = NULL, *schemaInfo = NULL;

    status = strSnprintf(kvKey,
                         sizeof(kvKey),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishTableName,
                         SchemaSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persisted schema for publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       kvKey,
                                       &kvValue,
                                       &kvValueSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persisted schema for publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    schemaInfo = json_loads(kvValue, 0, &err);
    if (schemaInfo == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse schema info json, "
                "source %s line %d, column %d, position %d, '%s' "
                "get persisted schema for publish table %s %lu: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text,
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Extract keys
    jsonSchema = json_object_get(schemaInfo, SchemaKeys);
    BailIfNullMsg(jsonSchema,
                  StatusNoMem,
                  moduleName,
                  "Failed get persisted schema for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    if (!json_is_array(jsonSchema)) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Error parsing persisted schema for publish table %s %lu: %s",
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    numKeys = json_array_size(jsonSchema);
    assert(numKeys != 0);
    keys =
        (XcalarApiColumnInfo *) memAlloc(sizeof(XcalarApiColumnInfo) * numKeys);
    BailIfNullMsg(keys,
                  StatusNoMem,
                  moduleName,
                  "Failed get persisted schema for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = getPersistedSchemaHelper(publishTableName,
                                      hashTreeId,
                                      jsonSchema,
                                      keys,
                                      numKeys);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persisted schema for publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    // Extract values
    jsonSchema = json_object_get(schemaInfo, SchemaValues);
    BailIfNullMsg(jsonSchema,
                  StatusNoMem,
                  moduleName,
                  "Failed get persisted schema for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    if (!json_is_array(jsonSchema)) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Error parsing persisted schema for publish table %s %lu: %s",
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    numValues = json_array_size(jsonSchema);
    assert(numValues != 0);
    values = (XcalarApiColumnInfo *) memAlloc(sizeof(XcalarApiColumnInfo) *
                                              numValues);
    BailIfNullMsg(values,
                  StatusNoMem,
                  moduleName,
                  "Failed get persisted schema for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = getPersistedSchemaHelper(publishTableName,
                                      hashTreeId,
                                      jsonSchema,
                                      values,
                                      numValues);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persisted schema for publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    assert(status == StatusOk);
    *retKeys = keys;
    keys = NULL;
    *retNumKeys = numKeys;
    *retValues = values;
    values = NULL;
    *retNumValues = numValues;

CommonExit:
    if (keys != NULL) {
        memFree(keys);
        keys = NULL;
    }
    if (values != NULL) {
        memFree(values);
        values = NULL;
    }
    if (schemaInfo != NULL) {
        json_decref(schemaInfo);
        schemaInfo = NULL;
    }
    if (kvValue != NULL) {
        memFree(kvValue);
        kvValue = NULL;
    }
    return status;
}

// XXX: this is invoked from the publish code path meaning we only keep
// track of dependency through the publish api call
Status
HashTreeMgr::persistDependency(const char *publishTableName,
                               XcalarApiTableInput srcTable,
                               Dag *dag)
{
    // find all select nodes associated with this srcTable
    Status status;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    XcalarApiDagOutput *getDagOutput;
    PublishDependencyUpdateInput *input = NULL;
    const char **sources = NULL;
    size_t numSources = 0;
    size_t totalBufLen = 0;

    status = dag->getDagByName(srcTable.tableName,
                               Dag::TableScope::LocalOnly,
                               &output,
                               &outputSize);
    BailIfFailed(status);

    getDagOutput = &output->outputResult.dagOutput;

    sources =
        (const char **) memAlloc(sizeof(*sources) * getDagOutput->numNodes);
    BailIfNull(sources);

    for (uint64_t ii = 0; ii < getDagOutput->numNodes; ii++) {
        if (getDagOutput->node[ii]->hdr.api == XcalarApiSelect) {
            XcalarApiSelectInput *selectInput =
                &getDagOutput->node[ii]->input->selectInput;

            totalBufLen += strlen(selectInput->srcTable.tableName) + 1;
            sources[numSources++] = selectInput->srcTable.tableName;
        }
    }

    input =
        (PublishDependencyUpdateInput *) memAlloc(sizeof(*input) + totalBufLen);
    BailIfNull(input);

    input->isDelete = false;
    input->numParents = numSources;
    strlcpy(input->name, publishTableName, sizeof(input->name));

    {
        char *cur = input->parentNames;
        for (size_t ii = 0; ii < numSources; ii++) {
            const char *parentName = sources[ii];
            strcpy(cur, parentName);
            cur += strlen(parentName) + 1;
        }
    }

    status = KvStoreLib::get()->updatePublishDependency(input,
                                                        sizeof(*input) +
                                                            totalBufLen);
    BailIfFailed(status);

CommonExit:
    if (output) {
        memFree(output);
    }
    if (sources) {
        memFree(sources);
    }
    if (input) {
        memFree(input);
    }

    return status;
}

Status
HashTreeMgr::persistSchema(const char *publishTableName,
                           Xid hashTreeId,
                           XdbMeta *xdbMeta)
{
    Status status = StatusOk;
    char key[XcalarApiMaxKeyLen + 1];
    char *value = NULL;
    json_t *jsonSchema = NULL, *jsonColumn = NULL, *jsonColumnName = NULL,
           *jsonColumnType = NULL, *schemaInfo = NULL;
    NewKeyValueNamedMeta *kvNamedMeta = &xdbMeta->kvNamedMeta;
    const NewTupleMeta *tupMeta = kvNamedMeta->kvMeta_.tupMeta_;
    int ret;

    schemaInfo = json_object();
    BailIfNullMsg(schemaInfo,
                  StatusNoMem,
                  moduleName,
                  "Failed persist schema %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    jsonSchema = json_array();
    BailIfNullMsg(jsonSchema,
                  StatusNoMem,
                  moduleName,
                  "Failed persist schema %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = strSnprintf(key,
                         sizeof(key),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishTableName,
                         SchemaSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    for (size_t ii = 0; ii < tupMeta->getNumFields(); ii++) {
        jsonColumn = json_object();
        BailIfNullMsg(jsonColumn,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        jsonColumnName = json_string(kvNamedMeta->valueNames_[ii]);
        BailIfNullMsg(jsonColumnName,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonColumn, SchemaColumnName, jsonColumnName);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumnName = NULL;

        jsonColumnType =
            json_string(strGetFromDfFieldType(tupMeta->getFieldType(ii)));
        BailIfNullMsg(jsonColumnType,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonColumn, SchemaColumnType, jsonColumnType);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumnType = NULL;

        ret = json_array_append_new(jsonSchema, jsonColumn);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumn = NULL;
    }

    ret = json_object_set_new(schemaInfo, SchemaValues, jsonSchema);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed persist schema %s %lu: %s",
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    jsonSchema = NULL;

    jsonSchema = json_array();
    BailIfNullMsg(jsonSchema,
                  StatusNoMem,
                  moduleName,
                  "Failed persist schema %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    assert(xdbMeta->numKeys > 0);
    for (unsigned ii = 0; ii < xdbMeta->numKeys; ii++) {
        jsonColumn = json_object();
        BailIfNullMsg(jsonColumn,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        jsonColumnName = json_string(xdbMeta->keyAttr[ii].name);
        BailIfNullMsg(jsonColumnName,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonColumn, SchemaColumnName, jsonColumnName);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumnName = NULL;

        jsonColumnType =
            json_string(strGetFromDfFieldType(xdbMeta->keyAttr[ii].type));
        BailIfNullMsg(jsonColumnType,
                      StatusNoMem,
                      moduleName,
                      "Failed persist schema %s %lu: %s",
                      publishTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        ret = json_object_set_new(jsonColumn, SchemaColumnType, jsonColumnType);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumnType = NULL;

        ret = json_array_append_new(jsonSchema, jsonColumn);
        if (ret != 0) {
            status = StatusJsonError;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        jsonColumn = NULL;
    }

    ret = json_object_set_new(schemaInfo, SchemaKeys, jsonSchema);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed persist schema %s %lu: %s",
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    jsonSchema = NULL;

    value = json_dumps(schemaInfo, 0);
    BailIfNullMsg(value,
                  StatusNoMem,
                  moduleName,
                  "Failed persist schema %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                             key,
                                             strlen(key) + 1,
                                             value,
                                             strlen(value) + 1,
                                             true,
                                             KvStoreOptSync);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist schema %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
CommonExit:
    if (jsonColumnName != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnName);
        jsonColumnName = NULL;
    }

    if (jsonColumnType != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumnName);
        jsonColumnName = NULL;
    }

    if (jsonColumn != NULL) {
        assert(status != StatusOk);
        json_decref(jsonColumn);
        jsonColumn = NULL;
    }

    if (jsonSchema != NULL) {
        assert(status != StatusOk);
        json_decref(jsonSchema);
        jsonSchema = NULL;
    }

    if (schemaInfo != NULL) {
        json_decref(schemaInfo);
        schemaInfo = NULL;
    }

    if (value != NULL) {
        memFree(value);
        value = NULL;
    }

    return status;
}

Status
HashTreeMgr::persistCoalesceBatchId(const char *publishTableName,
                                    Xid hashTreeId,
                                    int64_t coalesceBatchId)
{
    char key[XcalarApiMaxKeyLen + 1];
    json_t *coalesceInfo = NULL;
    char *value = NULL;
    int64_t oldBatchId = HashTree::InvalidBatchId;
    Status status = StatusOk;

#ifdef DEBUG
    if (coalesceBatchId > 0) {
        status = getPersistentCoalesceBatchId(publishTableName,
                                              hashTreeId,
                                              &oldBatchId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed persist coalesce for publish table %s %lu: %s",
                        publishTableName,
                        hashTreeId,
                        strGetFromStatus(status));
        assert(oldBatchId <= coalesceBatchId);
    }
#endif  // DEBUG

    status = strSnprintf(key,
                         sizeof(key),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishTableName,
                         CoalesceSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist coalesce batchId on publish table "
                    "%s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    coalesceInfo = json_pack(CoalesceJsonPackFormatStr,
                             CoalesceBatchIdKey,
                             coalesceBatchId);
    BailIfNullMsg(coalesceInfo,
                  StatusNoMem,
                  moduleName,
                  "Failed persist coalesce for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    value = json_dumps(coalesceInfo, 0);
    BailIfNullMsg(value,
                  StatusNoMem,
                  moduleName,
                  "Failed persist coalesce for publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                             key,
                                             strlen(key) + 1,
                                             value,
                                             strlen(value) + 1,
                                             true,
                                             KvStoreOptSync);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist coalesce for publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
CommonExit:
    if (coalesceInfo) {
        json_decref(coalesceInfo);
        coalesceInfo = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
HashTreeMgr::getPersistentCoalesceBatchId(const char *publishTableName,
                                          Xid hashTreeId,
                                          int64_t *retCoalesceBatchId)
{
    Status status = StatusOk;
    char key[XcalarApiMaxKeyLen + 1];
    json_t *coalesceInfo = NULL;
    char *value = NULL;
    size_t valueSize = 0;
    json_error_t err;
    int ret = 0;
    int64_t coalesceBatchId = HashTree::InvalidBatchId;
    *retCoalesceBatchId = HashTree::InvalidBatchId;

    status = strSnprintf(key,
                         sizeof(key),
                         "%s/%s/%s",
                         KvStoreKeyPrefix,
                         publishTableName,
                         CoalesceSuffix);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persistent coalesce batchId on publish table "
                    "%s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = KvStoreLib::get()->lookup(XidMgr::XidGlobalKvStore,
                                       key,
                                       &value,
                                       &valueSize);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed get persistent coalesce batchId on publish table "
                    "%s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    coalesceInfo = json_loads(value, 0, &err);
    if (coalesceInfo == NULL) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse coalesce info json, "
                "source %s line %d, column %d, position %d, '%s' "
                "get persistent coalesce batchId on publish table %s %lu: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text,
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = json_unpack_ex(coalesceInfo,
                         &err,
                         0,
                         CoalesceJsonPackFormatStr,
                         CoalesceBatchIdKey,
                         &coalesceBatchId);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(moduleName,
                XlogErr,
                "Failed to parse recovery info json, "
                "source %s line %d, column %d, position %d, '%s' "
                "get persistent coalesce batchId on publish tablee %s %lu: %s",
                err.source,
                err.line,
                err.column,
                err.position,
                err.text,
                publishTableName,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    *retCoalesceBatchId = coalesceBatchId;

CommonExit:
    if (coalesceInfo) {
        json_decref(coalesceInfo);
        coalesceInfo = NULL;
    }
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    return status;
}

Status
HashTreeMgr::createHashTree(XcalarApiTableInput srcTable,
                            Xid hashTreeId,
                            const char *name,
                            time_t unixTS,
                            bool dropSrc,
                            size_t size,
                            size_t numRows,
                            Dag *dag)
{
    Status status;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    LibNsTypes::NsHandle nsHandle;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    HashTreeRecord record(hashTreeId,
                          HashTreeRecord::State::Inactive,
                          HashTreeRecord::Restore::NotInProgress,
                          HashTree::InvalidBatchId,
                          HashTree::InvalidBatchId,
                          dag->getSessionContainer());
    HashTreeRefHandle refHandle;
    bool hashTreeHandleValid = false;
    bool hashTreeCreated = false;
    bool hashTreePersisted = false;

    status = getFQN(fullyQualName, sizeof(fullyQualName), name);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed create hashtree %s %lu src table %s: %s",
                name,
                hashTreeId,
                srcTable.tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Publish active = false, restoring = false.
    nsId = libNs->publish(fullyQualName, &record, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusInvPubTableName;
        } else if (status == StatusPendingRemoval || status == StatusExist) {
            status = StatusExistsPubTableName;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed create hashtree %s %lu src table %s: %s",
                name,
                hashTreeId,
                srcTable.tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Allow reader writer exclusive access
    status = openHandleToHashTree(name,
                                  LibNsTypes::WriterExcl,
                                  &refHandle,
                                  OpenRetry::False);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed create hashtree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));
    hashTreeHandleValid = true;

    // initialize the hash tree structure on all nodes
    status =
        createHashTreeInt(srcTable, name, hashTreeId, CreateReason::Publish);
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed create hashTree %s %lu src table %s: %s",
                       name,
                       hashTreeId,
                       srcTable.tableName,
                       strGetFromStatus(status));
    hashTreeCreated = true;

    // create the persisted directory in xcalar root
    status = persistPublishedTable(srcTable, dag, unixTS, name, hashTreeId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist hashtree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));
    hashTreePersisted = true;

    // populate the hash tree with the first update
    status = updateInMemHashTree(srcTable,
                                 hashTreeId,
                                 name,
                                 unixTS,
                                 dropSrc,
                                 size,
                                 numRows,
                                 0,
                                 UpdateReason::RegularUpdate);
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed to update hashTree %s %lu src table %s: %s",
                       name,
                       hashTreeId,
                       srcTable.tableName,
                       strGetFromStatus(status));

    HashTree *htree;
    htree = getHashTreeById(hashTreeId);  // Should not fail

    status = persistDependency(name, srcTable, dag);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed persist dependency hashTree %s %lu src table %s: "
                    "%s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));

    status = persistSchema(name, hashTreeId, htree->getXdbMeta());
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed create hashTree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));

    status = persistCoalesceBatchId(name, hashTreeId, 0);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed create hashTree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));

    // coalesce the update to optimize selects
    status = coalesceHashTreeInternal(name, hashTreeId, 0);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed create hashTree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));

    // Update active = true, restoring = false.
    new (&record) HashTreeRecord(hashTreeId,
                                 HashTreeRecord::State::Active,
                                 HashTreeRecord::Restore::NotInProgress,
                                 0,
                                 0,
                                 dag->getSessionContainer());

    nsHandle = libNs->updateNsObject(refHandle.nsHandle, &record, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed create hashTree %s %lu src table %s: %s",
                    name,
                    hashTreeId,
                    srcTable.tableName,
                    strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        bool objDeleted = false;
        if (nsId != LibNsTypes::NsInvalidId) {
            Status status2 = libNs->remove(fullyQualName, &objDeleted);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed clean out for hash tree %s %lu src table %s on"
                        " remove: %s",
                        name,
                        hashTreeId,
                        srcTable.tableName,
                        strGetFromStatus(status2));
            }
        }
        if (objDeleted && hashTreeCreated) {
            Status status2 =
                destroyHashTreeInt(hashTreeId, name, !hashTreePersisted);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to destroy hashtree %s %lu src"
                        " table %s: %s",
                        name,
                        hashTreeId,
                        srcTable.tableName,
                        strGetFromStatus(status2));
            }
            hashTreeCreated = false;
        }
    }

    if (hashTreeHandleValid) {
        closeHandleToHashTree(&refHandle, hashTreeId, name);
    }
    return status;
}

Status
HashTreeMgr::updateInMemHashTree(XcalarApiTableInput srcTable,
                                 Xid hashTreeId,
                                 const char *name,
                                 time_t unixTS,
                                 bool dropSrc,
                                 size_t size,
                                 size_t numRows,
                                 int64_t newBatchId,
                                 HashTreeMgr::UpdateReason updateReason)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmInput *input = NULL;
    bool hashTreeUpdated = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    HashTree::UpdateStats *outputPerNode[MaxNodes];
    uint64_t sizePerNode[MaxNodes];
    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed updateInMemHashTree publish table %s %lu src %s: "
                  "%s",
                  name,
                  hashTreeId,
                  srcTable.tableName,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed updateInMemHashTree publish table %s %lu src %s: "
                  "%s",
                  name,
                  hashTreeId,
                  srcTable.tableName,
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->srcTable = srcTable;
    input->hashTreeId = hashTreeId;
    input->unixTS = unixTS;
    input->size = size;
    input->numRows = numRows;
    input->dropSrc = dropSrc;
    input->batchId = newBatchId;
    input->updateReason = (unsigned) updateReason;

    // update the hash tree
    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::Update,
                   sizeof(LibHashTreeGvm::GvmInput));
    status = Gvm::get()->invokeWithOutput(gPayload,
                                          sizeof(outputPerNode),
                                          (void **) &outputPerNode,
                                          sizePerNode,
                                          nodeStatus);
    if (status == StatusOk) {
        hashTreeUpdated = true;

        HashTree::UpdateStats total;
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }

            total.numInserts += outputPerNode[ii]->numInserts;
            total.numUpdates += outputPerNode[ii]->numUpdates;
            total.numDeletes += outputPerNode[ii]->numDeletes;
        }

        if (status == StatusOk) {
            // collect total stats and store in local ht copy
            HashTree *ht = getHashTreeById(hashTreeId);

            ht->updateHead_->stats = total;
        }
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed updateInMemHashTree publish table %s %lu src "
                       "%s: %s",
                       name,
                       hashTreeId,
                       srcTable.tableName,
                       strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        if (hashTreeUpdated) {
            Status status2 =
                revertHashTree(newBatchId, hashTreeId, name, unixTS);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed updateInMemHashTree publish table %s %lu "
                        "src %s revert: %s",
                        name,
                        hashTreeId,
                        srcTable.tableName,
                        strGetFromStatus(status2));
            }
            hashTreeUpdated = false;
        }
    }

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        MemFreeAndNullify(outputPerNode[ii]);
    }

    MemFreeAndNullify(gPayload);

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

Status
HashTreeMgr::selectHashTree(XcalarApiTableInput dstTable,
                            XcalarApiTableInput joinTable,
                            Xid hashTreeId,
                            const char *publishTableName,
                            int64_t minBatchId,
                            int64_t maxBatchId,
                            const char *filterString,
                            unsigned numColumns,
                            XcalarApiRenameMap *columns,
                            size_t evalInputLen,
                            const char *evalInput,
                            uint64_t limitRows,
                            DagTypes::DagId dagId)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmSelectInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    size_t selectInputSize =
        LibHashTreeGvm::GvmSelectInput::getSize(evalInputLen);

    gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + selectInputSize);
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed selectHashTree publish table %s %lu dst table %s "
                  "batch Id %ld %ld: %s",
                  publishTableName,
                  hashTreeId,
                  dstTable.tableName,
                  minBatchId,
                  maxBatchId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed selectHashTree publish table %s %lu dst table %s "
                  "batch Id %ld %ld: %s",
                  publishTableName,
                  hashTreeId,
                  dstTable.tableName,
                  minBatchId,
                  maxBatchId,
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmSelectInput *) gPayload->buf;
    input->dstTable = dstTable;
    input->joinTable = joinTable;
    input->hashTreeId = hashTreeId;
    input->minBatchId = minBatchId;
    input->maxBatchId = maxBatchId;
    input->dagId = dagId;
    input->limitRows = limitRows;
    input->numColumns = numColumns;
    memcpy(input->columns, columns, sizeof(*columns) * numColumns);
    strlcpy(input->filterString, filterString, sizeof(input->filterString));
    if (evalInputLen > 0) {
        strlcpy(input->evalInput, evalInput, evalInputLen);
    } else {
        // strlcpy doesn't null-terminate destination if size is zero
        // LibHashTreeGvm::GvmSelectInput::getSize adds extra byte for null term
        input->evalInput[0] = '\0';
    }

    // populate dstXdb
    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::Select,
                   selectInputSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed selectHashTree publish table %s %lu dst table "
                       "%s "
                       "batch Id %ld %ld: %s",
                       publishTableName,
                       hashTreeId,
                       dstTable.tableName,
                       minBatchId,
                       maxBatchId,
                       strGetFromStatus(status));

    status = XdbMgr::get()->xdbLoadDone(dstTable.xdbId);
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed selectHashTree publish table %s %lu dst table "
                       "%s "
                       "batch Id %ld %ld: %s",
                       publishTableName,
                       hashTreeId,
                       dstTable.tableName,
                       minBatchId,
                       maxBatchId,
                       strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        XdbMgr::get()->xdbDrop(dstTable.xdbId);
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
HashTreeMgr::coalesceHashTree(HashTreeRefHandle *refHandle,
                              const char *publishTableName,
                              Xid hashTreeId)
{
    Status status = StatusOk;
    HashTreeRecord htRecord;
    LibNsTypes::NsHandle nsHandle;
    LibNs *libNs = LibNs::get();

    status =
        HashTreeMgr::get()->persistCoalesceBatchId(publishTableName,
                                                   hashTreeId,
                                                   refHandle->currentBatchId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed coalesce on publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    new (&htRecord)
        HashTreeRecord(refHandle->hashTreeId,
                       refHandle->active ? HashTreeRecord::State::Active
                                         : HashTreeRecord::State::Inactive,
                       refHandle->restoring
                           ? HashTreeRecord::Restore::InProgress
                           : HashTreeRecord::Restore::NotInProgress,
                       refHandle->currentBatchId,
                       refHandle->currentBatchId,
                       &refHandle->sessionContainer);

    nsHandle = libNs->updateNsObject(refHandle->nsHandle, &htRecord, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed coalesce on publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status =
        HashTreeMgr::get()->coalesceHashTreeInternal(publishTableName,
                                                     hashTreeId,
                                                     refHandle->currentBatchId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed coalesce on publish table %s %lu: %s",
                    publishTableName,
                    hashTreeId,
                    strGetFromStatus(status));
CommonExit:
    return status;
}

Status
HashTreeMgr::coalesceHashTreeInternal(const char *publishTableName,
                                      Xid hashTreeId,
                                      int64_t coalesceBatchId)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed coalesceHashTreeInternal publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed coalesceHashTreeInternal publish table %s %lu: %s",
                  publishTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->hashTreeId = hashTreeId;
    input->batchId = coalesceBatchId;

    // populate dstXdb
    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::Coalesce,
                   sizeof(LibHashTreeGvm::GvmInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed coalesceHashTreeInternal publish table %s %lu: "
                       "%s",
                       publishTableName,
                       hashTreeId,
                       strGetFromStatus(status));

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
HashTreeMgr::initState(Xid hashTreeId, const char *name)
{
    Status status = StatusOk;

    HashTreeEntry *htEntry =
        new (std::nothrow) HashTreeEntry(name, hashTreeId, NULL);
    BailIfNullMsg(htEntry,
                  StatusNoMem,
                  moduleName,
                  "Failed initState %s %lu: %s",
                  name,
                  hashTreeId,
                  strGetFromStatus(status));

    // XXX May be source node can do this lookup and ship it with the GVM
    // payload. But then GVM needs to deal with variable sized buffer.
    // So for now, since this is not in performance path, it is fine do
    // look up KvStore for schema.
    status = getPersistedSchema(name,
                                hashTreeId,
                                &htEntry->keys,
                                &htEntry->numKeys,
                                &htEntry->values,
                                &htEntry->numValues);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failure to get persisted schema for %s %lu not a hard error.",
                htEntry->pubTableName,
                hashTreeId);
        status = StatusOk;
    } else {
        htEntry->schemaAvailable = true;
    }

    lock_.lock();
    verifyOk(hashTreeIdTable_.insert(htEntry));
    verifyOk(hashTreeNameTable_.insert(htEntry));
    lock_.unlock();
    htEntry = NULL;

CommonExit:
    if (htEntry != NULL) {
        htEntry->destroy();
        htEntry = NULL;
    }
    return status;
}

Status
HashTreeMgr::coalesceHashTreeLocal(Xid hashTreeId, int64_t coalesceBatchId)
{
    Status status;
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    assert(htEntry != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;
    int64_t curBatchId = hashTree->getCurrentBatchId();
    if (curBatchId > coalesceBatchId) {
        for (int64_t ii = curBatchId; ii > coalesceBatchId; ii--) {
            // Revert from new to old
            revertHashTreeLocal(ii, hashTreeId);
        }
    }

    status = hashTree->coalesce();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed coalesceHashTree publish table %s %lu: %s",
                    htEntry->pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
HashTreeMgr::destroyHashTreeInt(Xid hashTreeId,
                                const char *name,
                                bool inactivateOnly)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    uint32_t action;

    if (inactivateOnly) {
        action = (uint32_t) LibHashTreeGvm::Action::Inactivate;
    } else {
        action = (uint32_t) LibHashTreeGvm::Action::Destroy;
    }

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed destroyHashTreeInt %s %lu inactivateOnly %s: %s",
                  name,
                  hashTreeId,
                  inactivateOnly ? "True" : "False",
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed destroyHashTreeInt %s %lu inactivateOnly %s: %s",
                  name,
                  hashTreeId,
                  inactivateOnly ? "True" : "False",
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->hashTreeId = hashTreeId;
    strlcpy(input->pubTableName, name, sizeof(input->pubTableName));

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   action,
                   sizeof(LibHashTreeGvm::GvmInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    BailIfFailedTxnMsg(moduleName,
                       status,
                       "Failed destroyHashTreeInt %s %lu inactivateOnly %s: %s",
                       name,
                       hashTreeId,
                       inactivateOnly ? "True" : "False",
                       strGetFromStatus(status));

    if (!inactivateOnly) {
        status = unpersistPublishedTable(name, hashTreeId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed destroyHashTreeInt %s %lu inactivateOnly %s: "
                        "%s",
                        name,
                        hashTreeId,
                        inactivateOnly ? "True" : "False",
                        strGetFromStatus(status));
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
HashTreeMgr::destroyHashTree(const char *name, bool inactivateOnly)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    bool objDeleted = false;
    HashTreeRefHandle refHandle;
    Xid hashTreeId = XidInvalid;
    HashTreeEntry *htEntry = NULL;
    bool hashTreeHandleOpened = false;
    NsObject *obj = NULL;

    lock_.lock();
    htEntry = hashTreeNameTable_.find(name);
    if (htEntry != NULL) {
        hashTreeId = htEntry->hashTreeId;
    }
    lock_.unlock();

    BailIfNullMsg(htEntry,
                  StatusPubTableNameNotFound,
                  moduleName,
                  "Failed destroyHashTree hashtree %s: %s",
                  name,
                  strGetFromStatus(status));

    status = getFQN(fullyQualName, sizeof(fullyQualName), name);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed destroyHashTree hashtree %s %lu: %s",
                name,
                hashTreeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    {
        HashTreeRecord *record;
        obj = LibNs::get()->getNsObject(fullyQualName);
        if (obj == NULL) {
            status = StatusPubTableNameNotFound;
            goto CommonExit;
        }

        record = static_cast<HashTreeRecord *>(obj);
        if (record->restoreState_ == HashTreeRecord::Restore::InProgress) {
            // cancel any outstanding restores. we can bail out afterwards and
            // let the restore deactivate the table
            cancelRestore(name);
            status = StatusOk;
            goto CommonExit;
        }
    }

    // Get the hashTreeId from libNs. Also open LibNs handle with read write
    // exclusive access.
    status = openHandleToHashTree(name,
                                  LibNsTypes::WriterExcl,
                                  &refHandle,
                                  OpenRetry::False);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed destroyHashTreee hashtree %s %lu: %s",
                    name,
                    hashTreeId,
                    strGetFromStatus(status));
    hashTreeHandleOpened = true;

    assert(!refHandle.restoring &&
           "Restore of publish table prevents inactivation and deletion");

    if (!inactivateOnly) {
        if (!refHandle.active) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Hashtree %s %lu already inactive",
                    name,
                    hashTreeId);
        }
        status = libNs->remove(fullyQualName, &objDeleted);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed destroyHashTreee hashtree %s %lu: %s",
                        name,
                        hashTreeId,
                        strGetFromStatus(status));
    } else {
        XcalarApiUdfContainer emptySessionContainer;
        memZero(&emptySessionContainer, sizeof(XcalarApiUdfContainer));
        HashTreeRecord record(hashTreeId,
                              HashTreeRecord::State::Inactive,
                              HashTreeRecord::Restore::NotInProgress,
                              HashTree::InvalidBatchId,
                              HashTree::InvalidBatchId,
                              &emptySessionContainer);
        LibNsTypes::NsHandle nsHandle;

        if (!refHandle.active) {
            status = StatusOk;
            xSyslog(moduleName,
                    XlogInfo,
                    "Hashtree %s %lu already inactive",
                    name,
                    hashTreeId);
            goto CommonExit;
        }

        // Update active = false, restoring = false.
        nsHandle = libNs->updateNsObject(refHandle.nsHandle, &record, &status);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed destroyHashTreee hashtree %s %lu: %s",
                        name,
                        hashTreeId,
                        strGetFromStatus(status));
    }

    status = destroyHashTreeInt(hashTreeId, name, inactivateOnly);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed destroyHashTreee hashtree %s %lu: %s",
                    name,
                    hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    if (hashTreeHandleOpened) {
        closeHandleToHashTree(&refHandle, hashTreeId, name);
    }

    if (obj) {
        memFree(obj);
    }
    return status;
}

Status
HashTreeMgr::createHashTreeLocal(XcalarApiTableInput srcTable,
                                 Xid hashTreeId,
                                 HashTreeMgr::CreateReason createReason,
                                 const char *pubTableName)
{
    Status status;
    Xdb *srcXdb;
    XdbMeta *srcMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    HashTree *hashTree = NULL;
    HashTreeEntry *htEntry = NULL;
    TableMgr::ColumnInfoTable columns;

    status = xdbMgr->xdbGet(srcTable.xdbId, &srcXdb, &srcMeta);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed createHashTreeLocal reason %u src table %s "
                    "publish table %s %lu: %s",
                    (unsigned) createReason,
                    srcTable.tableName,
                    pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    hashTree = new (std::nothrow) HashTree();
    BailIfNullMsg(hashTree,
                  StatusNoMem,
                  moduleName,
                  "Failed createHashTreeLocal reason %u src table %s "
                  "publish table %s %lu: %s",
                  (unsigned) createReason,
                  srcTable.tableName,
                  pubTableName,
                  hashTreeId,
                  strGetFromStatus(status));

    status = TableMgr::createColumnInfoTable(&srcMeta->kvNamedMeta, &columns);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed createHashTreeLocal reason %u src table %s "
                    "publish table %s %lu: %s",
                    (unsigned) createReason,
                    srcTable.tableName,
                    pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    status = hashTree->init(pubTableName,
                            hashTreeId,
                            &columns,
                            srcMeta->numKeys,
                            srcMeta->keyAttr,
                            &srcTable);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed createHashTreeLocal reason %u src table %s "
                    "publish table %s %lu: %s",
                    (unsigned) createReason,
                    srcTable.tableName,
                    pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

    if (createReason == CreateReason::RestoreFromSnap ||
        createReason == CreateReason::RestoreFromXcRoot) {
        lock_.lock();
        verify((htEntry = hashTreeIdTable_.find(hashTreeId)) != NULL);
        lock_.unlock();
        htEntry->hashTree = hashTree;
    } else {
        assert(createReason == CreateReason::Publish);
        htEntry = new (std::nothrow)
            HashTreeEntry(pubTableName, hashTreeId, hashTree);
        BailIfNullMsg(htEntry,
                      StatusNoMem,
                      moduleName,
                      "Failed createHashTreeLocal reason %u src table %s "
                      "publish table %s %lu: %s",
                      (unsigned) createReason,
                      srcTable.tableName,
                      pubTableName,
                      hashTreeId,
                      strGetFromStatus(status));

        lock_.lock();
        verifyOk(hashTreeIdTable_.insert(htEntry));
        verifyOk(hashTreeNameTable_.insert(htEntry));
        lock_.unlock();
    }

    htEntry = NULL;
    hashTree = NULL;
    assert(status == StatusOk);

CommonExit:
    if (hashTree != NULL) {
        hashTree->destroy();
    }

    columns.removeAll(&TableMgr::ColumnInfo::destroy);

    return status;
}

Status
HashTreeMgr::updateInMemHashTreeLocal(XcalarApiTableInput srcTable,
                                      time_t unixTS,
                                      bool dropSrc,
                                      size_t size,
                                      size_t numRows,
                                      int64_t newBatchId,
                                      Xid hashTreeId,
                                      HashTreeMgr::UpdateReason updateReason,
                                      HashTree::UpdateStats *updateOutput)
{
    Status status;
    Xdb *srcXdb;
    XdbMeta *srcMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    HashTree *hashTree = NULL;
    HashTreeEntry *htEntry = NULL;
    int flags = HashTree::NoFlags;
    bool opCodeFound = false;
    bool rankOverFound = false;

    status = xdbMgr->xdbGet(srcTable.xdbId, &srcXdb, &srcMeta);
    assert(status == StatusOk);

    lock_.lock();
    verify((htEntry = hashTreeIdTable_.find(hashTreeId)) != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;
    int64_t curBatchId = hashTree->getCurrentBatchId();

    if (curBatchId >= newBatchId) {
        for (int64_t ii = curBatchId; ii >= newBatchId; ii--) {
            // Revert from new to old
            revertHashTreeLocal(ii, hashTreeId);
        }
    }

    for (unsigned ii = 0; ii < srcXdb->tupMeta.getNumFields(); ii++) {
        if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                   XcalarOpCodeColumnName) == 0) {
            if (srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii) !=
                DfInt64) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed updateInMemHashTreeLocal for publish table %s "
                        "%lu "
                        " %u, %s must have type DfInt64: %s",
                        htEntry->pubTableName,
                        hashTreeId,
                        (unsigned) updateReason,
                        XcalarOpCodeColumnName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            opCodeFound = true;
        }

        if (strcmp(srcMeta->kvNamedMeta.valueNames_[ii],
                   XcalarRankOverColumnName) == 0) {
            if (srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii) !=
                DfInt64) {
                status = StatusInval;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed updateInMemHashTreeLocal for publish table %s "
                        "%lu %u, %s must have type DfInt64: %s",
                        htEntry->pubTableName,
                        hashTreeId,
                        (unsigned) updateReason,
                        XcalarRankOverColumnName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            rankOverFound = true;
        }
    }

    if (!opCodeFound) {
        flags |= HashTree::AddOpCodeFlag;
    }

    if (!rankOverFound) {
        flags |= HashTree::AddRankOverFlag;
    }

    if (dropSrc) {
        flags |= HashTree::DropSrcSlotsFlag;
    }

    status = hashTree->insertXdb(srcXdb,
                                 &srcTable,
                                 unixTS,
                                 size,
                                 numRows,
                                 flags,
                                 newBatchId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed updateInMemHashTreeLocal for publish table %s "
                    "%lu %u: %s",
                    htEntry->pubTableName,
                    hashTreeId,
                    (unsigned) updateReason,
                    strGetFromStatus(status));

    *updateOutput = hashTree->updateHead_->stats;

CommonExit:
    return status;
}

void
HashTreeMgr::revertHashTreeLocal(int64_t batchIdToClean, Xid hashTreeId)
{
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    verify((htEntry = hashTreeIdTable_.find(hashTreeId)) != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;
    hashTree->cleanOutBatchId(batchIdToClean);

    if (hashTree->currentBatchId_ == batchIdToClean) {
        HashTree::UpdateMeta *tmp = hashTree->popUpdateMeta();
        assert(tmp->batchId == batchIdToClean);
        delete tmp;
    }
}

Status
HashTreeMgr::selectHashTreeLocal(LibHashTreeGvm::GvmSelectInput *input)
{
    Status status;
    Xdb *dstXdb;
    XdbMeta *dstMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    verify((htEntry = hashTreeIdTable_.find(input->hashTreeId)) != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;

    status = xdbMgr->xdbGet(input->dstTable.xdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);

    status = hashTree->select(dstXdb, input);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed selectHashTreeLocal for publish table %s %lu: "
                    "%s",
                    htEntry->pubTableName,
                    input->hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

void
HashTreeMgr::inactivateHashTreeLocal(Xid hashTreeId, const char *name)
{
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;
    bool getSchema = false;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    if (htEntry && htEntry->hashTree) {
        hashTree = htEntry->hashTree;
        htEntry->hashTree = NULL;
        getSchema = !htEntry->schemaAvailable;
    }
    lock_.unlock();

    if (hashTree) {
        hashTree->destroy();
    }

    if (!getSchema) {
        return;
    }

    XcalarApiColumnInfo *keys = NULL, *values = NULL;
    size_t numKeys = 0, numValues = 0;

    // XXX May be source node can do this lookup and ship it with the GVM
    // payload. But then GVM needs to deal with variable sized buffer.
    // So for now, since this is not in performance path, it is fine do
    // look up KvStore for schema.
    Status status = getPersistedSchema(name,
                                       hashTreeId,
                                       &keys,
                                       &numKeys,
                                       &values,
                                       &numValues);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed inactivateHashTreeLocal %s %lu: %s",
                name,
                hashTreeId,
                strGetFromStatus(status));
    } else {
        lock_.lock();
        htEntry = hashTreeIdTable_.find(hashTreeId);
        assert(htEntry != NULL);
        htEntry->numKeys = numKeys;
        htEntry->keys = keys;
        htEntry->numValues = numValues;
        htEntry->values = values;
        htEntry->schemaAvailable = true;
        lock_.unlock();
    }
}

void
HashTreeMgr::destroyHashTreeLocal(Xid hashTreeId, const char *name)
{
    HashTreeEntry *htEntry = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.remove(hashTreeId);
    if (htEntry) {
        verify(htEntry == hashTreeNameTable_.remove(htEntry->pubTableName));
    }
    lock_.unlock();

    if (htEntry != NULL) {
        htEntry->destroy();
    }
}

// This is not a refcounted object. Caller here is expected to honor LibNs
// rules.
HashTreeMgr::HashTreeEntry *
HashTreeMgr::getHashTreeEntryById(Xid hashTreeId)
{
    HashTreeEntry *htEntry = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    lock_.unlock();

    return htEntry;
}

// This is not a refcounted object. Caller here is expected to honor LibNs
// rules.
HashTree *
HashTreeMgr::getHashTreeById(Xid hashTreeId)
{
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    if (htEntry != NULL) {
        hashTree = htEntry->hashTree;
    }
    lock_.unlock();

    return hashTree;
}

Status
HashTreeMgr::getFQN(char *fqn, size_t fqnLen, const char *ptName)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    int ret = snprintf(fqn, fqnLen, "%s/%s", PublishTableNsPrefix, ptName);
    if (ret >= (int) fqnLen) {
        status = StatusNameTooLong;
        goto CommonExit;
    }

    status = libNs->setFQNForNodeId(DlmNodeId, fqn, fqnLen);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
HashTreeMgr::openHandleToHashTree(const char *name,
                                  LibNsTypes::NsOpenFlags openFlag,
                                  HashTreeRefHandle *handleOut,
                                  OpenRetry retry)
{
    Status status;
    LibNsTypes::NsHandle nsHandle;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    NsObject *obj = NULL;
    HashTreeRecord *record;
    unsigned retryCount = 0;
    unsigned maxRetries = XcalarConfig::get()->pubTableAccessRetries_;
    unsigned sleepUsec =
        XcalarConfig::get()->pubTableRetriesTimeoutMsec_ * USecsPerMSec;
    LibNs *libNs = LibNs::get();

    status = getFQN(fullyQualName, sizeof(fullyQualName), name);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open handle to Publish Table %s : %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    do {
        nsHandle = libNs->open(fullyQualName, openFlag, &obj, &status);
        if (status != StatusOk && status != StatusAccess) {
            if (status == StatusNsNotFound) {
                status = StatusPubTableNameNotFound;
            }
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to open handle to Publish Table %s : %s",
                          name,
                          strGetFromStatus(status));
            goto CommonExit;
        }
        if (status == StatusAccess) {
            if (retry == OpenRetry::False) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Failed to open handle to Publish Table %s, "
                              "table is busy",
                              name);
                goto CommonExit;
            } else if (retryCount >= maxRetries) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Failed to open handle to Publish Table %s"
                              " after %u retries, table is busy",
                              name,
                              retryCount);
                goto CommonExit;
            } else {
                sysUSleep(sleepUsec);
                retryCount++;
            }
        }
    } while (status == StatusAccess);

    record = static_cast<HashTreeRecord *>(obj);

    handleOut->nsHandle = nsHandle;
    handleOut->hashTreeId = record->hashTreeId_;
    if (record->activeState_ == HashTreeRecord::State::Active) {
        handleOut->active = true;
    } else {
        assert(record->activeState_ == HashTreeRecord::State::Inactive);
        handleOut->active = false;
    }
    if (record->restoreState_ == HashTreeRecord::Restore::InProgress) {
        handleOut->restoring = true;
    } else {
        assert(record->restoreState_ == HashTreeRecord::Restore::NotInProgress);
        handleOut->restoring = false;
    }
    strlcpy(handleOut->name, name, sizeof(handleOut->name));
    handleOut->currentBatchId = record->currentBatchId_;
    handleOut->oldestBatchId = record->oldestBatchId_;
    UserDefinedFunction::copyContainers(&handleOut->sessionContainer,
                                        &record->sessionContainer_);

CommonExit:
    if (obj) {
        memFree(obj);
        obj = NULL;
    }

    return status;
}

void
HashTreeMgr::closeHandleToHashTree(HashTreeRefHandle *handle,
                                   Xid hashTreeId,
                                   const char *name)
{
    Status status;
    LibNs *libNs = LibNs::get();
    bool isObjDeleted = false;

    status = libNs->close(handle->nsHandle, &isObjDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed closeHandleToHashTree %s %lu: %s",
                name,
                hashTreeId,
                strGetFromStatus(status));
        return;
    }

    if (isObjDeleted == true) {
        status = destroyHashTreeInt(handle->hashTreeId,
                                    handle->name,
                                    false);  // delete it's durable state
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed closeHandleToHashTree %s %lu: %s",
                    name,
                    hashTreeId,
                    strGetFromStatus(status));
        }
    }
}

Status
HashTreeMgr::openHandles(const char **publishedTableNames,
                         unsigned numTables,
                         LibNsTypes::NsOpenFlags openFlags,
                         HashTreeRefHandle *refHandles)
{
    Status status = StatusOk;
    unsigned ii = 0;

    for (ii = 0; ii < numTables; ii++) {
        // no deadlock possible because failure is returned if we fail to
        // acquire the handle
        status = openHandleToHashTree(publishedTableNames[ii],
                                      openFlags,
                                      &refHandles[ii],
                                      OpenRetry::True);
        BailIfFailed(status);
    }

CommonExit:
    if (status != StatusOk) {
        for (unsigned jj = 0; jj < ii; jj++) {
            closeHandleToHashTree(&refHandles[jj],
                                  refHandles[jj].hashTreeId,
                                  publishedTableNames[jj]);
        }
    }

    return status;
}

void
HashTreeMgr::closeHandles(const char **publishedTableNames,
                          unsigned numHandles,
                          HashTreeRefHandle *refHandles)
{
    for (unsigned ii = 0; ii < numHandles; ii++) {
        closeHandleToHashTree(&refHandles[ii],
                              refHandles[ii].hashTreeId,
                              publishedTableNames[ii]);
    }
}

using namespace xcalar::compute::localtypes;

Status
HashTreeMgr::listHashTrees(const char *namePattern,
                           uint32_t maxUpdateCount,
                           int64_t updateStartBatchId,
                           uint32_t maxSelectCount,
                           PublishedTable::ListTablesResponse *response)
{
    Status status = StatusOk;
    size_t numTables = 0;
    HashTreeRefHandle handle;
    bool handleInit = false;
    PersistedInfoHashTable persistedTables;
    unsigned ii = 0;
    UserMgr *userMgr = UserMgr::get();
    XcalarApiUdfContainer emptySessionContainer;
    memZero(&emptySessionContainer, sizeof(XcalarApiUdfContainer));

    // XXX This can be replaced with just LibNs lookups.
    status = getPersistedTables(namePattern, &persistedTables);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed listHashTrees %s: %s",
                    namePattern,
                    strGetFromStatus(status));

    numTables = persistedTables.getSize();

    PersistedInfo *info;
    char *tableName;

    for (auto iter = persistedTables.begin(); (info = iter.get()) != NULL;
         iter.next(), ii++) {
        tableName = info->name;
        PublishedTable::ListTablesResponse::TableInfo *table;
        try {
            table = response->add_tables();
            if (table == nullptr) {
                status = StatusNoMem;
                goto CommonExit;
            }
            table->set_name(tableName);
            table->set_numpersistedupdates(info->totalUpdates);
        } catch (std::exception &e) {
            status = StatusNoMem;
            goto CommonExit;
        }

        // Open as reader with read shared write exclusive access.
        status = openHandleToHashTree(tableName,
                                      LibNsTypes::ReadSharedWriteExclReader,
                                      &handle,
                                      OpenRetry::True);
        if (status != StatusOk) {
            // XXX This is inaccurate, but not fatal, since a retry will give an
            // accurate state for this publish table.
            table->set_active(false);
            table->set_restoring(false);
            continue;
        }
        handleInit = true;

        table->set_active(handle.active);
        table->set_restoring(handle.restoring);

        if (!UserDefinedFunction::containersMatch(&handle.sessionContainer,
                                                  &emptySessionContainer)) {
            XcalarApiUdfContainer sessionContainer;
            UserDefinedFunction::copyContainers(&sessionContainer,
                                                &handle.sessionContainer);

            // Handle the session rename case. Note the session could have been
            // renamed by the time we around to list PTs.
            sessionContainer.sessionInfo.sessionNameLength = 0;
            status = userMgr->getSessionId(&sessionContainer.userId,
                                           &sessionContainer.sessionInfo);
            if (status == StatusOk) {
                try {
                    table->set_useridname(sessionContainer.userId.userIdName);
                    table->set_sessionname(
                        sessionContainer.sessionInfo.sessionName);
                } catch (std::exception &e) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
            }
        }

        HashTreeEntry *htEntry = hashTreeIdTable_.find(handle.hashTreeId);
        if (!htEntry) {
            closeHandleToHashTree(&handle, handle.hashTreeId, tableName);
            handleInit = false;
            continue;
        }

        HashTree *hashTree = htEntry->hashTree;
        XdbMeta *xdbMeta = NULL;
        if (!htEntry->hashTree) {
            if (htEntry->schemaAvailable) {
                for (size_t jj = 0; jj < htEntry->numKeys; jj++) {
                    ColumnAttribute::ColumnAttributeProto *key =
                        table->add_keys();
                    if (key == nullptr) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                    try {
                        key->set_name(htEntry->keys[jj].name);
                        key->set_type(
                            strGetFromDfFieldType(htEntry->keys[jj].type));
                    } catch (std::exception &e) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                }

                for (size_t jj = 0; jj < htEntry->numValues; jj++) {
                    ColumnAttribute::ColumnAttributeProto *value =
                        table->add_values();
                    if (value == nullptr) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                    try {
                        value->set_name(htEntry->values[jj].name);
                        value->set_type(
                            strGetFromDfFieldType(htEntry->values[jj].type));
                    } catch (std::exception &e) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                }
            }

            closeHandleToHashTree(&handle, handle.hashTreeId, tableName);
            handleInit = false;
            continue;
        } else {
            xdbMeta = hashTree->xdbMeta_;
            for (unsigned jj = 0; jj < xdbMeta->numKeys; jj++) {
                ColumnAttribute::ColumnAttributeProto *key = table->add_keys();
                if (key == nullptr) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
                try {
                    key->set_name(xdbMeta->keyAttr[jj].name);
                    key->set_type(
                        strGetFromDfFieldType(xdbMeta->keyAttr[jj].type));
                } catch (std::exception &e) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
            }
            for (unsigned jj = 0;
                 jj < xdbMeta->numImmediates + xdbMeta->numFatptrs;
                 jj++) {
                ColumnAttribute::ColumnAttributeProto *value =
                    table->add_values();
                if (value == nullptr) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
                try {
                    value->set_name(xdbMeta->kvNamedMeta.valueNames_[jj]);
                    value->set_type(strGetFromDfFieldType(
                        xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(
                            jj)));
                } catch (std::exception &e) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
            }
        }

        try {
            table->set_sizetotal(hashTree->sizeTotal_);
            table->set_numrowstotal(hashTree->numRowsTotal_);
            table->set_srctablename(hashTree->sourceTable_.tableName);
            table->set_oldestbatchid(hashTree->mainBatchId_);
            table->set_nextbatchid(hashTree->currentBatchId_ + 1);
        } catch (std::exception &e) {
            status = StatusNoMem;
            goto CommonExit;
        }

        if (hashTree->needsSizeRefresh_) {
            status = refreshTotalSize(hashTree->hashTreeId_);
            if (status != StatusOk) {
                status = StatusOk;  // non fatal
            }
        }

        table->set_sizetotal(hashTree->sizeTotal_);
        table->set_numrowstotal(hashTree->numRowsTotal_);

        // XXX TODO Updates meta can be inaccurate except on the
        // session owner node.
        if (maxUpdateCount > 0) {
            HashTree::UpdateMeta *update = hashTree->updateHead_;
            int64_t updateEndBatchId;

            if (updateStartBatchId == HashTree::InvalidBatchId) {
                // XXX getCurrentBatchId() API need not be consistent on
                // non-session owner nodes.
                updateEndBatchId = hashTree->getCurrentBatchId() + 1;
            } else {
                updateEndBatchId = updateStartBatchId + maxUpdateCount;
            }
            while (update != nullptr) {
                if (update->batchId < updateEndBatchId) {
                    try {
                        PublishedTable::ListTablesResponse::UpdateInfo
                            *updateProto = table->add_updates();
                        if (updateProto == nullptr) {
                            status = StatusNoMem;
                            goto CommonExit;
                        }
                        updateProto->set_srctablename(
                            update->srcTable.tableName);
                        updateProto->set_startts(update->unixTS);
                        updateProto->set_batchid(update->batchId);
                        updateProto->set_size(update->size);
                        updateProto->set_numrows(update->numRows);
                        updateProto->set_numinserts(update->stats.numInserts);
                        updateProto->set_numupdates(update->stats.numUpdates);
                        updateProto->set_numdeletes(update->stats.numDeletes);
                    } catch (std::exception &e) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                }

                update = update->next;
            }
        }

        if (maxSelectCount > 0) {
            HashTree::SelectMeta *select = hashTree->selectHead_;
            while (select != nullptr &&
                   (uint32_t) table->selects_size() < maxSelectCount) {
                try {
                    PublishedTable::ListTablesResponse::SelectInfo
                        *selectProto = table->add_selects();
                    if (selectProto == nullptr) {
                        status = StatusNoMem;
                        goto CommonExit;
                    }
                    selectProto->set_dsttablename(select->dstTable.tableName);
                    selectProto->set_minbatchid(select->minBatchId);
                    selectProto->set_maxbatchid(select->maxBatchId);
                    select = select->next;
                } catch (std::exception &e) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
            }
        }

        HashTree::Index *index;
        for (auto indexIter = hashTree->indexHashTable_.begin();
             (index = indexIter.get()) != NULL;
             indexIter.next()) {
            try {
                PublishedTable::ListTablesResponse::IndexInfo *indexProto =
                    table->add_indexes();
                if (indexProto == nullptr) {
                    status = StatusNoMem;
                    goto CommonExit;
                }
                indexProto->mutable_key()->set_name(
                    table->values(index->getKeyIdx()).name());
                indexProto->mutable_key()->set_type(
                    table->values(index->getKeyIdx()).type());
                // XXX: this just estimates total size based on index size on
                // this node.
                indexProto->set_sizeestimate(
                    XdbMgr::get()->xdbGetSize(index->xdbId) *
                    Config::get()->getActiveNodes());
                indexProto->set_uptimems(index->timer.getCurElapsedMSecs());
            } catch (std::exception &e) {
                status = StatusNoMem;
                goto CommonExit;
            }
        }

        closeHandleToHashTree(&handle, handle.hashTreeId, tableName);
        handleInit = false;
    }

    status = StatusOk;

CommonExit:

    if (handleInit) {
        closeHandleToHashTree(&handle, handle.hashTreeId, tableName);
        handleInit = false;
    }

    persistedTables.removeAll(&PersistedInfo::destroy);

    return status;
}

Status
HashTreeMgr::revertHashTree(int64_t batchId,
                            Xid hashTreeId,
                            const char *name,
                            time_t unixTS)
{
    LibHashTreeGvm::GvmInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;

    status = unpersistUpdate(name, hashTreeId, batchId, unixTS);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed revertHashTree %s %lu batch %ld: %s",
                    name,
                    hashTreeId,
                    batchId,
                    strGetFromStatus(status));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed revertHashTree %s %lu batch %ld: %s",
                  name,
                  hashTreeId,
                  batchId,
                  strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed revertHashTree %s %lu batch %ld: %s",
                  name,
                  hashTreeId,
                  batchId,
                  strGetFromStatus(status));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->batchId = batchId;
    input->hashTreeId = hashTreeId;

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::Revert,
                   sizeof(LibHashTreeGvm::GvmInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed revertHashTree %s %lu batch %ld: %s",
                name,
                hashTreeId,
                batchId,
                strGetFromStatus(status));
    }
CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
HashTreeMgr::refreshTotalSize(Xid hashTreeId)
{
    LibHashTreeGvm::GvmInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    void *outputPerNode[MaxNodes];
    uint64_t sizePerNode[MaxNodes];
    Status *nodeStatus = NULL;

    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;
    size_t totalSize = 0;
    size_t totalNumRows = 0;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNull(gPayload);

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::GetSize,
                   sizeof(LibHashTreeGvm::GvmInput));

    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;
    input->hashTreeId = hashTreeId;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed refreshTotalSize for hashTree %lu: %s",
                  hashTreeId,
                  strGetFromStatus(status));

    status = Gvm::get()->invokeWithOutput(gPayload,
                                          sizeof(SizeAndRows),
                                          (void **) &outputPerNode,
                                          sizePerNode,
                                          nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get size for hashTree %lu",
                hashTreeId);
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        SizeAndRows *output = (SizeAndRows *) outputPerNode[ii];
        totalSize += output->size;
        totalNumRows += output->rows;
    }

    input->size = totalSize;
    input->numRows = totalNumRows;

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::RefreshSize,
                   sizeof(LibHashTreeGvm::GvmInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to refresh size for hash tree %lu: %s",
                hashTreeId,
                strGetFromStatus(status));
    }
    for (unsigned ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
            outputPerNode[ii] = NULL;
        }
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

void
HashTreeMgr::refreshTotalSizeLocal(Xid hashTreeId, size_t size, size_t numRows)
{
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    assert(htEntry != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;
    hashTree->sizeTotal_ = size;
    hashTree->numRowsTotal_ = numRows;
    hashTree->needsSizeRefresh_ = false;
}

void
HashTreeMgr::getSizeLocal(Xid hashTreeId,
                          void *outPayload,
                          size_t *outputSizeOut)
{
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;
    SizeAndRows *output = (SizeAndRows *) outPayload;
    *outputSizeOut = sizeof(*output);

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    assert(htEntry != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;
    hashTree->computeLocalSize(&output->size, &output->rows);
}

uint64_t
HashTreeMgr::getTotalUsedBytes()
{
    uint64_t total = 0;
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;
    lock_.lock();

    for (HashTreeIdTable::iterator it = hashTreeIdTable_.begin();
         (htEntry = it.get()) != NULL;
         it.next()) {
        uint64_t size, rows;
        hashTree = htEntry->hashTree;
        if (hashTree) {
            hashTree->computeLocalSize(&size, &rows);
            total += size;
        }
    }
    lock_.unlock();

    return total;
}

// Invoked during bootstrap time
Status
HashTreeMgr::publishNamesToNs()
{
    Status status = StatusOk;
    PersistedInfoHashTable persistedTables;
    PersistedInfo *info = NULL;
    unsigned ii = 0;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    LibHashTreeGvm::GvmInput *input = NULL;
    Gvm::Payload *gPayload = NULL;
    XcalarApiUdfContainer emptySessionContainer;
    memZero(&emptySessionContainer, sizeof(XcalarApiUdfContainer));
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;

    status = getPersistedTables("*", &persistedTables);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed publishNamesToNs on getPersistedTables: %s",
                    strGetFromStatus(status));

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNullMsg(nodeStatus,
                  StatusNoMem,
                  moduleName,
                  "Failed publishNamesToNs: %s",
                  strGetFromStatus(status));

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmInput));
    BailIfNullMsg(gPayload,
                  StatusNoMem,
                  moduleName,
                  "Failed publishNamesToNs: %s",
                  strGetFromStatus(status));

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::InitState,
                   sizeof(LibHashTreeGvm::GvmInput));
    input = (LibHashTreeGvm::GvmInput *) gPayload->buf;

    for (auto iter = persistedTables.begin(); (info = iter.get()) != NULL;
         iter.next(), ii++) {
        HashTreeRecord record(XidMgr::get()->xidGetNext(),
                              HashTreeRecord::State::Inactive,
                              HashTreeRecord::Restore::NotInProgress,
                              HashTree::InvalidBatchId,
                              HashTree::InvalidBatchId,
                              &emptySessionContainer);

        char fullyQualName[LibNsTypes::MaxPathNameLen];

        xSyslog(moduleName,
                XlogInfo,
                "Found publish table %s %lu",
                info->getName(),
                record.hashTreeId_);

        status = getFQN(fullyQualName, sizeof(fullyQualName), info->getName());
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed publishNamesToNs for %s: %s",
                    info->getName(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Publish active = false, restoring = false.
        nsId = LibNs::get()->publish(fullyQualName, &record, &status);
        if (status != StatusOk) {
            if (status == StatusNsInvalidObjName) {
                status = StatusInvPubTableName;
            } else if (status == StatusPendingRemoval ||
                       status == StatusExist) {
                status = StatusExistsPubTableName;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "Failed publishNamesToNs for %s: %s",
                    info->getName(),
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // XXX Do a batch GVM instead of per Publish table GVM.
        strlcpy(input->pubTableName,
                info->getName(),
                sizeof(input->pubTableName));
        input->hashTreeId = record.hashTreeId_;
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
                        "Failed publishNamesToNs for %s: %s",
                        info->getName(),
                        strGetFromStatus(status));
    }

    nsId = LibNs::get()->publish(PubTableSnapshotNs, &status);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed publishNamesToNs: %s",
                    strGetFromStatus(status));

    assert(status == StatusOk);
    xSyslog(moduleName, XlogInfo, "Published %u tables in namespace", ii);

CommonExit:
    persistedTables.removeAll(&PersistedInfo::destroy);
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

// Invoked during teardown
void
HashTreeMgr::unpublishNamesFromNs()
{
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    Status status = StatusOk;

    status = LibNs::get()->removeMatching(PubTableSnapshotNs);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed unpublishNamesFromNs: %s",
                strGetFromStatus(status));
        // Pass through
        status = StatusOk;
    }

    status = getFQN(fullyQualName, sizeof(fullyQualName), "*");
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed unpublishNamesFromNs for '*': %s",
                strGetFromStatus(status));
        return;
    }

    status = LibNs::get()->removeMatching(fullyQualName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed unpublishNamesFromNs '%s': %s",
                fullyQualName,
                strGetFromStatus(status));
        return;
    }
}

void
HashTreeMgr::cancelRestore(const char *name)
{
    char snapshotRetinaName[XcalarApiMaxTableNameLen + 1];

    {
        // mark the restore as cancelled, the restoring thread should eventually
        // notice this and stop restoring.
        lock_.lock();
        RestoreInfo *info = restoresInProgress_.find(name);
        if (info == NULL) {
            lock_.unlock();
            return;
        }

        info->markCancelled();
        strcpy(snapshotRetinaName, info->snapshotRetinaName);
        lock_.unlock();
    }

    // try to cancel any ongoing retinas that the restore thread is running
    Status status;
    if (strlen(snapshotRetinaName) > 0) {
        status = QueryManager::get()->requestQueryCancel(snapshotRetinaName);
        if (status == StatusOk) {
            // sucessfully cancelled snapshot retina, no more needs to be done
            return;
        }
    }

    // try to cancel any ongoing update retinas
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    char retPrefix[XcalarApiMaxPathLen + 1];

    snprintf(retPrefix,
             sizeof(retPrefix),
             "%s%s*",
             TmpUpdateRetinaNamePrefix,
             name);

    status = DagLib::get()->listRetinas(retPrefix, &output, &outputSize);
    if (status == StatusOk) {
        XcalarApiListRetinasOutput *listOutput =
            &output->outputResult.listRetinasOutput;

        for (unsigned ii = 0; ii < listOutput->numRetinas; ii++) {
            status = QueryManager::get()->requestQueryCancel(
                listOutput->retinaDescs[ii].retinaName);

            // ignore any cancellation failures
            (void) status;
        }
    }
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
}

HashTreeMgr::HashTreeEntry::HashTreeEntry(const char *pubTableNameIn,
                                          Xid hashTreeIdIn,
                                          HashTree *hashTreeIn)
{
    strlcpy(pubTableName, pubTableNameIn, sizeof(pubTableName));
    hashTreeId = hashTreeIdIn;
    hashTree = hashTreeIn;
}

HashTreeMgr::HashTreeEntry::~HashTreeEntry()
{
    if (numKeys) {
        assert(keys != NULL);
        memFree(keys);
        keys = NULL;
        numKeys = 0;
    }
    if (numValues != 0) {
        assert(values != NULL);
        memFree(values);
        values = NULL;
        numValues = 0;
    }
}

bool
HashTreeMgr::runSnapshotApp(LibNsTypes::NsHandle *retNsHandle)
{
    LibNsTypes::NsHandle nsHandle;
    LibNs *libNs = LibNs::get();
    Status status = StatusOk;
    bool runSs = false;

    nsHandle = libNs->open(PubTableSnapshotNs, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to open handle to snapshot: %s",
                      strGetFromStatus(status));
        goto CommonExit;
    }
    *retNsHandle = nsHandle;
    runSs = true;

CommonExit:
    return runSs;
}

void
HashTreeMgr::snapshotAppDone(LibNsTypes::NsHandle nsHandle)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();

    status = libNs->close(nsHandle, NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close handle to snapshot: %s",
                strGetFromStatus(status));
        // pass through
        status = StatusOk;
    }
}

Status
HashTreeMgr::addIndexToHashTree(const char *publishTableName,
                                Xid hashTreeId,
                                const char *keyName)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmIndexInput *input = NULL;
    Gvm::Payload *gPayload = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmIndexInput));
    BailIfNull(gPayload);

    input = (LibHashTreeGvm::GvmIndexInput *) gPayload->buf;
    input->hashTreeId = hashTreeId;
    strlcpy(input->key, keyName, sizeof(input->key));

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::AddIndex,
                   sizeof(*input));

    status = Gvm::get()->invoke(gPayload);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        Status status2 =
            removeIndexFromHashTree(publishTableName, hashTreeId, keyName);
        xSyslog(moduleName,
                XlogErr,
                "Failed removeIndexFromHashTree for %s: %s",
                publishTableName,
                strGetFromStatus(status2));
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    return status;
}

Status
HashTreeMgr::addIndexToHashTreeLocal(Xid hashTreeId, const char *keyName)
{
    Status status;
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    assert(htEntry != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;

    status = hashTree->addIndex(keyName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed addIndex publish table %s %lu: %s",
                    htEntry->pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
HashTreeMgr::removeIndexFromHashTree(const char *publishTableName,
                                     Xid hashTreeId,
                                     const char *keyName)
{
    Status status = StatusOk;
    LibHashTreeGvm::GvmIndexInput *input = NULL;
    Gvm::Payload *gPayload = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(LibHashTreeGvm::GvmIndexInput));
    BailIfNull(gPayload);

    input = (LibHashTreeGvm::GvmIndexInput *) gPayload->buf;
    input->hashTreeId = hashTreeId;
    strlcpy(input->key, keyName, sizeof(input->key));

    gPayload->init(LibHashTreeGvm::get()->getGvmIndex(),
                   (uint32_t) LibHashTreeGvm::Action::RemoveIndex,
                   sizeof(*input));

    status = Gvm::get()->invoke(gPayload);
    BailIfFailed(status);

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    return status;
}

void
HashTreeMgr::removeIndexFromHashTreeLocal(Xid hashTreeId, const char *keyName)
{
    Status status;
    HashTreeEntry *htEntry = NULL;
    HashTree *hashTree = NULL;

    lock_.lock();
    htEntry = hashTreeIdTable_.find(hashTreeId);
    assert(htEntry != NULL);
    lock_.unlock();

    hashTree = htEntry->hashTree;

    hashTree->removeIndex(keyName);
}

Status
HashTreeMgr::dispatchToDlmNode(void *payload,
                               size_t payloadSize,
                               void *ephemeral,
                               size_t ackSize)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    MsgSendRecvFlags srFlags;
    TwoPcBufLife bLife;

    assert(payload != NULL && payloadSize != 0);

    if (ackSize != 0) {
        srFlags =
            (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrPlusPayload);
        bLife = (TwoPcBufLife)(TwoPcMemCopyInput | TwoPcMemCopyOutput);
    } else {
        srFlags = (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly);
        bLife = TwoPcMemCopyInput;
    }

    msgMgr->twoPcEphemeralInit(&eph,
                               payload,
                               payloadSize,
                               payloadSize > ackSize ? payloadSize : ackSize,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcPubTableDlm1,
                               ephemeral,
                               bLife);

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcPubTableDlm,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           srFlags,
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           DlmNodeId,
                           TwoPcClassNonNested);
    if (status == StatusOk) {
        assert(!twoPcHandle.twoPcHandle);
    }

    return status;
}

Status
HashTreeMgr::updateCommit(unsigned numUpdates,
                          HashTreeRefHandle **refHandles,
                          int64_t *commitBatchIds)
{
    Status status = StatusOk;
    size_t msgSize =
        sizeof(UpdateDlmMsg) + sizeof(UpdateDlmMsg::Info) * numUpdates;
    UpdateDlmMsg *msg = (UpdateDlmMsg *) memAlloc(msgSize);
    BailIfNullMsg(msg,
                  StatusNoMem,
                  moduleName,
                  "Failed updateCommit: %s",
                  strGetFromStatus(status));

    new (&msg->hdr) DlmMsg(DlmMsg::Type::Update, msgSize);
    msg->numUpdates = numUpdates;
    for (unsigned ii = 0; ii < numUpdates; ii++) {
        msg->info[ii].commitBatchId = commitBatchIds[ii];
        msg->info[ii].htreeHandle = *refHandles[ii];
    }

    status = dispatchToDlmNode((void *) msg, msgSize, msg, 0);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed updateCommit: %s",
                    strGetFromStatus(status));

    status = msg->hdr.status;
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed updateCommit: %s",
                    strGetFromStatus(status));

CommonExit:
    if (msg) {
        memFree(msg);
        msg = NULL;
    }
    return status;
}

void
TwoPcMsg2pcPubTableDlm1::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    DlmMsg *dlmMsg = (DlmMsg *) payload;
    Status status = StatusOk;
    size_t ackSize = 0;
    HashTreeRecord **htRecord = NULL;
    LibNs *libNs = LibNs::get();

    switch (dlmMsg->type) {
    case DlmMsg::Type::Update: {
        UpdateDlmMsg *uMsg = (UpdateDlmMsg *) dlmMsg;
        assert(uMsg->hdr.msgSize ==
               sizeof(UpdateDlmMsg) +
                   sizeof(UpdateDlmMsg::Info) * uMsg->numUpdates);

        htRecord = (HashTreeRecord **) memAlloc(sizeof(HashTreeRecord *) *
                                                uMsg->numUpdates);
        BailIfNullMsg(htRecord,
                      StatusNoMem,
                      moduleName,
                      "Failed updateCommit: %s",
                      strGetFromStatus(status));

        for (unsigned ii = 0; ii < uMsg->numUpdates; ii++) {
            htRecord[ii] =
                (HashTreeRecord *)
                    libNs->getRefToObject(uMsg->info[ii].htreeHandle.nsHandle,
                                          &status);
            if (status != StatusOk) {
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed to getRefToObject for publish table %s "
                                "%lu: %s",
                                uMsg->info[ii].htreeHandle.name,
                                uMsg->info[ii].htreeHandle.hashTreeId,
                                strGetFromStatus(status));
            }
        }

        for (unsigned ii = 0; ii < uMsg->numUpdates; ii++) {
            htRecord[ii]->currentBatchId_ = uMsg->info[ii].commitBatchId;
        }
        break;
    }
    default:
        assert(0 && "Not implemented");
        break;
    }
CommonExit:
    if (htRecord != NULL) {
        memFree(htRecord);
        htRecord = NULL;
    }
    eph->setAckInfo(status, ackSize);
}

void
TwoPcMsg2pcPubTableDlm1::schedLocalCompletion(MsgEphemeral *eph, void *payload)
{
    DlmMsg *dlmMsg = (DlmMsg *) eph->ephemeral;

    switch (dlmMsg->type) {
    case DlmMsg::Type::Update: {
        dlmMsg->status = eph->status;
        break;
    }
    default:
        assert(0 && "Not implemented");
        break;
    }
}

void
TwoPcMsg2pcPubTableDlm1::recvDataCompletion(MsgEphemeral *eph, void *payload)
{
    schedLocalCompletion(eph, payload);
}
