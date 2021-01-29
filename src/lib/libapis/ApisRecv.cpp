// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <string.h>
#include <semaphore.h>
#include <cstdlib>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <pwd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/ApisRecvObject.h"
#include "libapis/ApisRecvClient.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerTop.h"
#include "libapis/ApiHandlerGetNumNodes.h"
#include "libapis/ApiHandlerGetStatGroupIdMap.h"
#include "libapis/ApiHandlerSessionList.h"
#include "libapis/ApiHandlerSessionNew.h"
#include "libapis/ApiHandlerSessionInact.h"
#include "libapis/ApiHandlerSessionDelete.h"
#include "libapis/ApiHandlerSessionActivate.h"
#include "libapis/ApiHandlerSessionDownload.h"
#include "libapis/ApiHandlerSessionUpload.h"
#include "libapis/ApiHandlerListDagNodeInfo.h"
#include "libapis/ApiHandlerQueryState.h"
#include "libapis/ApiHandlerGetTableMeta.h"
#include "libapis/ApiHandlerGetDag.h"
#include "libapis/ApiHandlerListDatasets.h"
#include "libapis/ApiHandlerGetDatasetsInfo.h"
#include "libapis/ApiHandlerListDatasetUsers.h"
#include "libapis/ApiHandlerListUserDatasets.h"
#include "libapis/ApiHandlerDatasetCreate.h"
#include "libapis/ApiHandlerDatasetGetMeta.h"
#include "libapis/ApiHandlerDatasetDelete.h"
#include "libapis/ApiHandlerDatasetUnload.h"
#include "libapis/ApiHandlerListExportTargets.h"
#include "libapis/ApiHandlerMakeRetina.h"
#include "libapis/ApiHandlerGetRetina.h"
#include "libapis/ApiHandlerGetRetinaJson.h"
#include "libapis/ApiHandlerUpdateRetina.h"
#include "libapis/ApiHandlerDeleteRetina.h"
#include "libapis/ApiHandlerCreateDht.h"
#include "libapis/ApiHandlerDeleteDht.h"
#include "libapis/ApiHandlerListFuncTests.h"
#include "libapis/ApiHandlerStartFuncTests.h"
#include "libapis/ApiHandlerKeyAddOrReplace.h"
#include "libapis/ApiHandlerKeyList.h"
#include "libapis/ApiHandlerTarget.h"
#include "libapis/ApiHandlerPreview.h"
#include "libapis/ApiHandlerGetQuery.h"
#include "libapis/ApiHandlerQueryCancel.h"
#include "libapis/ApiHandlerQueryDelete.h"
#include "libapis/ApiHandlerGetTableRefCount.h"
#include "libapis/ApiHandlerRenameNode.h"
#include "libapis/ApiHandlerTagDagNodes.h"
#include "libapis/ApiHandlerCommentDagNodes.h"
#include "libapis/ApiHandlerGetStat.h"
#include "libapis/ApiHandlerResetStat.h"
#include "libapis/ApiHandlerGetOpStats.h"
#include "libapis/ApiHandlerCancelOp.h"
#include "libapis/ApiHandlerListRetinas.h"
#include "libapis/ApiHandlerListParametersInRetina.h"
#include "libapis/ApiHandlerImportRetina.h"
#include "libapis/ApiHandlerExportRetina.h"
#include "libapis/ApiHandlerUdfAdd.h"
#include "libapis/ApiHandlerUdfGet.h"
#include "libapis/ApiHandlerUdfUpdate.h"
#include "libapis/ApiHandlerUdfDelete.h"
#include "libapis/ApiHandlerKeyDelete.h"
#include "libapis/ApiHandlerKeyAppend.h"
#include "libapis/ApiHandlerKeySetIfEqual.h"
#include "libapis/ApiHandlerListXdfs.h"
#include "libapis/ApiHandlerSessionRename.h"
#include "libapis/ApiHandlerSessionPersist.h"
#include "libapis/ApiHandlerSupportGenerate.h"
#include "libapis/ApiHandlerGetPerNodeOpStats.h"
#include "libapis/ApiHandlerGetConfigParams.h"
#include "libapis/ApiHandlerSetConfigParam.h"
#include "libapis/ApiHandlerAppSet.h"
#include "libapis/ApiHandlerAppRun.h"
#include "libapis/ApiHandlerAppReap.h"
#include "libapis/ApiHandlerPerNodeTop.h"
#include "libapis/ApiHandlerGetIpAddr.h"
#include "libapis/ApiHandlerRestoreTable.h"
#include "libapis/ApiHandlerPublish.h"
#include "libapis/ApiHandlerUnpublish.h"
#include "libapis/ApiHandlerCoalesce.h"
#include "libapis/ApiHandlerListTables.h"
#include "libapis/ApiHandlerUpdate.h"
#include "libapis/ApiHandlerRuntimeSetParam.h"
#include "libapis/ApiHandlerRuntimeGetParam.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerBulkLoad.h"
#include "libapis/OperatorHandlerIndex.h"
#include "libapis/OperatorHandlerJoin.h"
#include "libapis/OperatorHandlerUnion.h"
#include "libapis/OperatorHandlerFilter.h"
#include "libapis/OperatorHandlerMap.h"
#include "libapis/OperatorHandlerSynthesize.h"
#include "libapis/OperatorHandlerGroupBy.h"
#include "libapis/OperatorHandlerAggregate.h"
#include "libapis/OperatorHandlerExport.h"
#include "libapis/OperatorHandlerProject.h"
#include "libapis/OperatorHandlerGetRowNum.h"
#include "libapis/OperatorHandlerDeleteObjects.h"
#include "libapis/OperatorHandlerArchiveTables.h"
#include "libapis/OperatorHandlerExecuteRetina.h"
#include "libapis/OperatorHandlerSelect.h"
#include "libapis/LibApisSend.h"
#include "libapis/LibApisRecv.h"
#include "sys/Socket.h"
#include "config/Config.h"
#include "usrnode/UsrNode.h"
#include "msg/Message.h"
#include "df/DataFormat.h"
#include "dataset/Dataset.h"
#include "operators/Operators.h"
#include "scalars/Scalars.h"
#include "querymanager/QueryManager.h"
#include "xdb/Xdb.h"
#include "stat/Statistics.h"
#include "queryparser/QueryParser.h"
#include "dag/DagLib.h"
#include "kvstore/KvStore.h"
#include "operators/XcalarEval.h"
#include "util/MemTrack.h"
#include "usr/Users.h"
#include "sys/XLog.h"
#include "operators/DhtTypes.h"
#include "support/SupportBundle.h"
#include "util/License.h"
#include "util/System.h"
#include "udf/UserDefinedFunction.h"
#include "common/Version.h"
#include "util/Stopwatch.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "msg/Xid.h"
#include "strings/String.h"
#include "util/CronMgr.h"
#include "util/IntHashTable.h"
#include "util/XcalarProcFsHelper.h"
#include "app/AppMgr.h"

static constexpr const char *moduleName = "libapis";

// Outstanding APIs received.
Atomic64 apisOutstanding;

// Stat APIs received.
static StatGroupId apisStatGroupId = 0;
static StatHandle apisStatInComingCumulative[XcalarApisLen];
StatHandle apisStatDoneCumulative[XcalarApisLen];
StatHandle apisStatOutstanding;
StatHandle apisStatImmedOutstanding;
static StatHandle apisStatInsufficientMem;
static StatHandle apisStatTooManyOutstanding;
static StatHandle apisStatScheduledOnRuntime;
static StatHandle apiStatTotalRunInline;
static constexpr size_t apisStatCount = XcalarApisLen + XcalarApisLen + 6;
static bool statsInited = false;
static bool usrNodeShutdown = false;
static bool usrNodeShutdownForce = false;
static bool apisRecvInited = false;

static time_t timeStarted;

void
setApisRecvInitialized()
{
    apisRecvInited = true;
    memBarrier();
    time(&timeStarted);
}

bool
apisRecvInitialized()
{
    return apisRecvInited;
}

// There are 3 phases in shutdown: 1) Session persistence (so each node goes
// ahead and persists their sessions), 2) Global objects deletion (done by
// nodeId 0), and 3) Local modules teardown. There's a 2pc barrier between
// each phase
static void
xcApiObjectCleanup()
{
    Status status;

    Txn newTxn = Txn::newTxn(Txn::Mode::NonLRQ);
    Txn::setTxn(newTxn);

    // Phase 1: Session persistence
    UserMgr::get()->shutdown();

    // We need all sessions to have persisted their workspace graphs
    // before we can destroy the dataset. Otherwise there is a race with
    // getActiveNodes

    status = MsgMgr::get()->twoPcBarrier(
        MsgMgr::BarrierType::BarrierWorkbookPersistence);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "2pc barrier failed during session shutdown: %s",
                strGetFromStatus(status));
        return;
    }

    // Phase 2: Global objects deletion
    if (Config::get()->getMyNodeId() == 0) {
        XcalarApiOutput *output = NULL;
        size_t outputSize = 0;
        Status status;

        status = DagLib::get()->listRetinas("*", &output, &outputSize);
        if (status == StatusOk) {
            XcalarApiListRetinasOutput *listRetinasOutput = NULL;
            listRetinasOutput = &output->outputResult.listRetinasOutput;
            for (uint64_t ii = 0; ii < listRetinasOutput->numRetinas; ii++) {
                status = DagLib::get()->deleteRetina(
                    listRetinasOutput->retinaDescs[ii].retinaName);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to delete batch dataflow %s: %s",
                            listRetinasOutput->retinaDescs[ii].retinaName,
                            strGetFromStatus(status));
                }
            }
            memFree(output);
            output = NULL;
            listRetinasOutput = NULL;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to list batch dataflows during shutdown: %s",
                    strGetFromStatus(status));
        }
        assert(output == NULL);

        status = Dataset::get()->unloadDatasets("*", &output, &outputSize);
        if (status == StatusOk) {
            memFree(output);
            output = NULL;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete datasets during shutdown: %s",
                    strGetFromStatus(status));
        }
        assert(output == NULL);
    }

    status = MsgMgr::get()->twoPcBarrier(
        MsgMgr::BarrierType::BarrierGlobalObjectsDeleted);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "2pc barrier failed during global objects deletion: %s",
                strGetFromStatus(status));
        return;
    }

    // App Msg stream teardown
    AppMgr::get()->teardownMsgStreams();
}

uint64_t
getBcXcalarApisBufSize()
{
    return sizeof(ApisRecvObject);
}

static bool
isTrialPeriodOver()
{
    // XXX Can add more custom logic here in future
    return true;
}

// You want to be able to get your data into and out of Xcalar
// even if your license expires. You just don't allow the users
// to do any heavy duty operations
static bool
needsLicense(ApiHandler *apiHandler)
{
    return apiHandler->isOperator() && apiHandler->getApi() != XcalarApiIndex &&
           apiHandler->getApi() != XcalarApiExport;
}

static Status
authenticate(XcalarApiUserId *userId, XcalarApis api)
{
    Status status = StatusOk;
    size_t userNameLen = 0;
    uid_t uid;
    int64_t returnedPwdSize;
    char *returnedPwd = NULL;
    int getloginRc = 0;
    struct passwd pwd;
    struct passwd *returnedPwdStruct;
    char userName[LOGIN_NAME_MAX + 1];

    char *xceUser = NULL;

    // Verify that the user name is within the allowed size range
    userNameLen = strnlen(userId->userIdName, sizeof(userId->userIdName));
    if (userNameLen < 1 || userNameLen == sizeof(userId->userIdName)) {
        status = StatusInvalidUserNameLen;
        goto CommonExit;
    }

    // XXX Do some actual authenticating

    // XXX Make sure you allow sending APIs to self like say shutdown
    // from a signal handler.
    if (api == XcalarApiShutdown) {
        uid = getuid();

        returnedPwdSize = sysconf(_SC_GETPW_R_SIZE_MAX);
        if (returnedPwdSize < 0) {
            // Value was indeterminate.  Pick something reasonable.
            returnedPwdSize = 16384;
        }

        returnedPwd = (char *) memAllocExt(returnedPwdSize, moduleName);
        if (returnedPwd == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to allocate %ld bytes for pwd info",
                    returnedPwdSize);
            goto CommonExit;
        }

        getloginRc = getpwuid_r(uid,
                                &pwd,
                                returnedPwd,
                                returnedPwdSize,
                                &returnedPwdStruct);
        if (getloginRc != 0 || returnedPwdStruct == NULL) {
            status = sysErrnoToStatus(getloginRc);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get passwd file info: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        strlcpy(userName, pwd.pw_name, sizeof(userName) - 1);

        xceUser = getenv("XCE_USER");
        // Ensure the user making the shutdown request is allowed to issue
        // the shutdown.
        if ((strncmp(userId->userIdName, userName, userNameLen) != 0) &&
            (strncmp(userId->userIdName, XcalarUsrName, userNameLen) != 0) &&
            (strncmp(userId->userIdName, JenkinsUsrName, userNameLen) != 0) &&
            (strncmp(userId->userIdName, RootUsrName, userNameLen) != 0) &&
            (strncmp(userId->userIdName,
                     UsrNodeShutdownTrigUsrName,
                     userNameLen) != 0) &&
            (xceUser == NULL ||
             (strncmp(userId->userIdName, xceUser, userNameLen) != 0))) {
            status = StatusNoShutdownPrivilege;
            xSyslog(moduleName,
                    XlogErr,
                    "User '%s' cannot shutdown cluster: %s",
                    userId->userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:

    if (returnedPwd != NULL) {
        memFree(returnedPwd);
        returnedPwd = NULL;
    }

    return status;
}

// 'retPrevTxn' semantics:
//
//  1: if non-NULL, caller is requesting a new transaction and log to be
//     created.  If this routine returns successfully, this implies a new txn
//     is in force and the prev transaction is returned in 'retPrevTxn' for
//     the caller to restore later. The caller must not restore 'retPrevTxn' if
//     this routine returns failure
//
//  2. if NULL, caller is requesting the current txn must be maintained, if any
//     (no new txn should be created and set)
//
Status
xcApiGetOperatorHandlerInited(OperatorHandler **operatorHandlerOut,
                              XcalarWorkItem *workItem,
                              XcalarApiUserId *userId,
                              Dag *dstGraph,
                              bool preserveDatasetUuid,
                              Txn *retPrevTxn)
{
    Status status = StatusUnknown;
    OperatorHandler *operatorHandler = NULL;
    Txn::Mode mode = Txn::Mode::Invalid;
    bool newTxnStarted = false;
    Txn prevTxn;

    status = xcApiGetOperatorHandlerUninited(&operatorHandler,
                                             workItem->api,
                                             preserveDatasetUuid);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get operatorHandler for %s: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->init(userId, workItem->sessionInfo, dstGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed initialization in operatorHandler for %s: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (retPrevTxn != NULL) {
        // caller wants new transaction to be set, and if this is successful,
        // the previous txn in force must be returned in 'retPrevTxn'
        if (UserDefinedFunction::get()->containerForDataflows(
                dstGraph->getUdfContainer())) {
            // if the destination qgraph is a retina dag, the mode should be LRQ
            // so that any validation of fields during operator setArg
            // processing, is cognizant of this (e.g. the '--' delimiter is
            // acceptable in a retina dag but not otherwise - see
            // XcalarEval::isValidFieldName and its check for NonLRQ mode to
            // check if DfFatptrPrefixDelimiterReplaced is allowed) - the mode
            // is probably not the best way to do this check, it should really
            // be the graph-type but the transaction is the only entity
            // available deep down in XcalarEval (and there are probably other
            // reasons for this - so we must adhere to it for now).

            mode = Txn::Mode::LRQ;
        } else {
            mode = workItem->api == XcalarApiExecuteRetina ? Txn::Mode::LRQ
                                                           : Txn::Mode::NonLRQ;
        }
        prevTxn = Txn::currentTxn();
        status =
            operatorHandler->setTxnAndTxnLog(mode, Runtime::SchedId::Sched0);
        if (!status.ok()) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set txn for API %s: %s",
                    strGetFromXcalarApis(workItem->api),
                    status.message());
            goto CommonExit;
        }
        newTxnStarted = true;
    }

    status = operatorHandler->setArg(workItem->input, workItem->inputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set argument for operatorHandler for %s: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    // Not deleting operatorHandler here even if (status != StatusOk). If
    // operatorHandler is deleted, the TxnLog will be deleted by destructor, and
    // won't be able to be copied to the parent Txn. The operatorHandler will be
    // deleted in caller function anyway.

    *operatorHandlerOut = operatorHandler;

    if (newTxnStarted) {
        if (status != StatusOk) {
            // failure could occur (e.g. in setArg) after a new txn is set.
            // If so, restore the previous transaction before returning
            // failure, as expected by caller
            MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
        } else if (retPrevTxn) {
            // return previous transaction only when returning success
            *retPrevTxn = prevTxn;
        }
    }
    return status;
}

Status
xcApiGetOperatorHandlerUninited(OperatorHandler **operatorHandlerOut,
                                XcalarApis api,
                                bool preserveDatasetUuid)
{
    OperatorHandler *operatorHandler = NULL;
    Status status = StatusUnknown;

    switch (api) {
    case XcalarApiBulkLoad: {
        OperatorHandlerBulkLoad *operatorHandlerBulkLoad;
        operatorHandlerBulkLoad =
            new (std::nothrow) OperatorHandlerBulkLoad(api);
        operatorHandler = operatorHandlerBulkLoad;
        if (operatorHandler == NULL) {
            break;
        }

        if (preserveDatasetUuid) {
            status = operatorHandlerBulkLoad->setPreserveDatasetUuidMode();
            if (status != StatusOk) {
                goto CommonExit;
            }
        }
        break;
    }
    case XcalarApiIndex:
        operatorHandler = new (std::nothrow) OperatorHandlerIndex(api);
        break;
    case XcalarApiJoin:
        operatorHandler = new (std::nothrow) OperatorHandlerJoin(api);
        break;
    case XcalarApiUnion:
        operatorHandler = new (std::nothrow) OperatorHandlerUnion(api);
        break;
    case XcalarApiFilter:
        operatorHandler = new (std::nothrow) OperatorHandlerFilter(api);
        break;
    case XcalarApiGroupBy:
        operatorHandler = new (std::nothrow) OperatorHandlerGroupBy(api);
        break;
    case XcalarApiMap:
        operatorHandler = new (std::nothrow) OperatorHandlerMap(api);
        break;
    case XcalarApiSynthesize:
        operatorHandler = new (std::nothrow) OperatorHandlerSynthesize(api);
        break;
    case XcalarApiAggregate:
        operatorHandler = new (std::nothrow) OperatorHandlerAggregate(api);
        break;
    case XcalarApiExport:
        operatorHandler = new (std::nothrow) OperatorHandlerExport(api);
        break;
    case XcalarApiProject:
        operatorHandler = new (std::nothrow) OperatorHandlerProject(api);
        break;
    case XcalarApiGetRowNum:
        operatorHandler = new (std::nothrow) OperatorHandlerGetRowNum(api);
        break;
    case XcalarApiDeleteObjects:
        operatorHandler = new (std::nothrow) OperatorHandlerDeleteObjects(api);
        break;
    case XcalarApiArchiveTables:
        operatorHandler = new (std::nothrow) OperatorHandlerArchiveTables(api);
        break;
    case XcalarApiExecuteRetina:
        operatorHandler = new (std::nothrow) OperatorHandlerExecuteRetina(api);
        break;
    case XcalarApiSelect:
        operatorHandler = new (std::nothrow) OperatorHandlerSelect(api);
        break;
    default:
        status = StatusApiFunctionInvalid;
        goto CommonExit;
    }

    if (operatorHandler == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate operatorHandler for %s",
                strGetFromXcalarApis(api));
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (operatorHandler != NULL) {
            delete operatorHandler;
            operatorHandler = NULL;
        }
    }
    *operatorHandlerOut = operatorHandler;
    return status;
}

Status
xcApiGetApiHandler(ApiHandler **apiHandlerOut, XcalarApis api)
{
    ApiHandler *apiHandler = NULL;
    OperatorHandler *operatorHandler = NULL;
    Status status = StatusUnknown;

    switch (api) {
    case XcalarApiBulkLoad:
    case XcalarApiIndex:
    case XcalarApiJoin:
    case XcalarApiUnion:
    case XcalarApiFilter:
    case XcalarApiGroupBy:
    case XcalarApiAggregate:
    case XcalarApiMap:
    case XcalarApiSynthesize:
    case XcalarApiExport:
    case XcalarApiProject:
    case XcalarApiGetRowNum:
    case XcalarApiDeleteObjects:
    case XcalarApiArchiveTables:
    case XcalarApiExecuteRetina:
    case XcalarApiSelect:
        status = xcApiGetOperatorHandlerUninited(&operatorHandler, api, false);
        if (status != StatusOk) {
            goto CommonExit;
        }
        apiHandler = operatorHandler;
        break;
    case XcalarApiTop:
        apiHandler = new (std::nothrow) ApiHandlerTop(api);
        break;
    case XcalarApiGetNumNodes:
        apiHandler = new (std::nothrow) ApiHandlerGetNumNodes(api);
        break;
    case XcalarApiGetStatGroupIdMap:
        apiHandler = new (std::nothrow) ApiHandlerGetStatGroupIdMap(api);
        break;
    case XcalarApiGetStatByGroupId:
        // this api has been deprecated
        status = StatusApiFunctionInvalid;
        goto CommonExit;
        break;
    case XcalarApiSessionList:
        apiHandler = new (std::nothrow) ApiHandlerSessionList(api);
        break;
    case XcalarApiSessionNew:
        apiHandler = new (std::nothrow) ApiHandlerSessionNew(api);
        break;
    case XcalarApiSessionInact:
        apiHandler = new (std::nothrow) ApiHandlerSessionInact(api);
        break;
    case XcalarApiSessionDelete:
        apiHandler = new (std::nothrow) ApiHandlerSessionDelete(api);
        break;
    case XcalarApiSessionActivate:
        apiHandler = new (std::nothrow) ApiHandlerSessionActivate(api);
        break;
    case XcalarApiSessionDownload:
        apiHandler = new (std::nothrow) ApiHandlerSessionDownload(api);
        break;
    case XcalarApiSessionUpload:
        apiHandler = new (std::nothrow) ApiHandlerSessionUpload(api);
        break;
    case XcalarApiRuntimeSetParam:
        apiHandler = new (std::nothrow) ApiHandlerRuntimeSetParam(api);
        break;
    case XcalarApiRuntimeGetParam:
        apiHandler = new (std::nothrow) ApiHandlerRuntimeGetParam(api);
        break;
    case XcalarApiListDagNodeInfo:
        apiHandler = new (std::nothrow) ApiHandlerListDagNodeInfo(api);
        break;
    case XcalarApiQueryState:
        apiHandler = new (std::nothrow) ApiHandlerQueryState(api);
        break;
    case XcalarApiGetTableMeta:
        apiHandler = new (std::nothrow) ApiHandlerGetTableMeta(api);
        break;
    case XcalarApiGetDag:
        apiHandler = new (std::nothrow) ApiHandlerGetDag(api);
        break;
    case XcalarApiListDatasets:
        apiHandler = new (std::nothrow) ApiHandlerListDatasets(api);
        break;
    case XcalarApiGetDatasetsInfo:
        apiHandler = new (std::nothrow) ApiHandlerGetDatasetsInfo(api);
        break;
    case XcalarApiListExportTargets:
        apiHandler = new (std::nothrow) ApiHandlerListExportTargets(api);
        break;
    case XcalarApiMakeRetina:
        apiHandler = new (std::nothrow) ApiHandlerMakeRetina(api);
        break;
    case XcalarApiGetRetina:
        apiHandler = new (std::nothrow) ApiHandlerGetRetina(api);
        break;
    case XcalarApiGetRetinaJson:
        apiHandler = new (std::nothrow) ApiHandlerGetRetinaJson(api);
        break;
    case XcalarApiUpdateRetina:
        apiHandler = new (std::nothrow) ApiHandlerUpdateRetina(api);
        break;
    case XcalarApiDeleteRetina:
        apiHandler = new (std::nothrow) ApiHandlerDeleteRetina(api);
        break;
    case XcalarApiCreateDht:
        apiHandler = new (std::nothrow) ApiHandlerCreateDht(api);
        break;
    case XcalarApiDeleteDht:
        apiHandler = new (std::nothrow) ApiHandlerDeleteDht(api);
        break;
    case XcalarApiListFuncTests:
        apiHandler = new (std::nothrow) ApiHandlerListFuncTests(api);
        break;
    case XcalarApiStartFuncTests:
        apiHandler = new (std::nothrow) ApiHandlerStartFuncTests(api);
        break;
    case XcalarApiKeyAddOrReplace:
        apiHandler = new (std::nothrow) ApiHandlerKeyAddOrReplace(api);
        break;
    case XcalarApiKeyList:
        apiHandler = new (std::nothrow) ApiHandlerKeyList(api);
        break;
    case XcalarApiTarget:
        apiHandler = new (std::nothrow) ApiHandlerTarget(api);
        break;
    case XcalarApiPreview:
        apiHandler = new (std::nothrow) ApiHandlerPreview(api);
        break;
    case XcalarApiGetQuery:
        apiHandler = new (std::nothrow) ApiHandlerGetQuery(api);
        break;
    case XcalarApiQueryCancel:
        apiHandler = new (std::nothrow) ApiHandlerQueryCancel(api);
        break;
    case XcalarApiQueryDelete:
        apiHandler = new (std::nothrow) ApiHandlerQueryDelete(api);
        break;
    case XcalarApiGetTableRefCount:
        apiHandler = new (std::nothrow) ApiHandlerGetTableRefCount(api);
        break;
    case XcalarApiRenameNode:
        apiHandler = new (std::nothrow) ApiHandlerRenameNode(api);
        break;
    case XcalarApiTagDagNodes:
        apiHandler = new (std::nothrow) ApiHandlerTagDagNodes(api);
        break;
    case XcalarApiCommentDagNodes:
        apiHandler = new (std::nothrow) ApiHandlerCommentDagNodes(api);
        break;
    case XcalarApiGetStat:
        apiHandler = new (std::nothrow) ApiHandlerGetStat(api);
        break;
    case XcalarApiResetStat:
        apiHandler = new (std::nothrow) ApiHandlerResetStat(api);
        break;
    case XcalarApiGetOpStats:
        apiHandler = new (std::nothrow) ApiHandlerGetOpStats(api);
        break;
    case XcalarApiCancelOp:
        apiHandler = new (std::nothrow) ApiHandlerCancelOp(api);
        break;
    case XcalarApiListRetinas:
        apiHandler = new (std::nothrow) ApiHandlerListRetinas(api);
        break;
    case XcalarApiListParametersInRetina:
        apiHandler = new (std::nothrow) ApiHandlerListParametersInRetina(api);
        break;
    case XcalarApiImportRetina:
        apiHandler = new (std::nothrow) ApiHandlerImportRetina(api);
        break;
    case XcalarApiExportRetina:
        apiHandler = new (std::nothrow) ApiHandlerExportRetina(api);
        break;
    case XcalarApiUdfAdd:
        apiHandler = new (std::nothrow) ApiHandlerUdfAdd(api);
        break;
    case XcalarApiUdfGet:
        apiHandler = new (std::nothrow) ApiHandlerUdfGet(api);
        break;
    case XcalarApiUdfUpdate:
        apiHandler = new (std::nothrow) ApiHandlerUdfUpdate(api);
        break;
    case XcalarApiUdfDelete:
        apiHandler = new (std::nothrow) ApiHandlerUdfDelete(api);
        break;
    case XcalarApiKeyDelete:
        apiHandler = new (std::nothrow) ApiHandlerKeyDelete(api);
        break;
    case XcalarApiKeyAppend:
        apiHandler = new (std::nothrow) ApiHandlerKeyAppend(api);
        break;
    case XcalarApiKeySetIfEqual:
        apiHandler = new (std::nothrow) ApiHandlerKeySetIfEqual(api);
        break;
    case XcalarApiListXdfs:
        apiHandler = new (std::nothrow) ApiHandlerListXdfs(api);
        break;
    case XcalarApiSessionRename:
        apiHandler = new (std::nothrow) ApiHandlerSessionRename(api);
        break;
    case XcalarApiSessionPersist:
        apiHandler = new (std::nothrow) ApiHandlerSessionPersist(api);
        break;
    case XcalarApiSupportGenerate:
        apiHandler = new (std::nothrow) ApiHandlerSupportGenerate(api);
        break;
    case XcalarApiGetPerNodeOpStats:
        apiHandler = new (std::nothrow) ApiHandlerGetPerNodeOpStats(api);
        break;
    case XcalarApiGetConfigParams:
        apiHandler = new (std::nothrow) ApiHandlerGetConfigParams(api);
        break;
    case XcalarApiSetConfigParam:
        apiHandler = new (std::nothrow) ApiHandlerSetConfigParam(api);
        break;
    case XcalarApiAppSet:
        apiHandler = new (std::nothrow) ApiHandlerAppSet(api);
        break;
    case XcalarApiAppRun:
        apiHandler = new (std::nothrow) ApiHandlerAppRun(api);
        break;
    case XcalarApiAppReap:
        apiHandler = new (std::nothrow) ApiHandlerAppReap(api);
        break;
    case XcalarApiGetIpAddr:
        apiHandler = new (std::nothrow) ApiHandlerGetIpAddr(api);
        break;
    case XcalarApiListDatasetUsers:
        apiHandler = new (std::nothrow) ApiHandlerListDatasetUsers(api);
        break;
    case XcalarApiListUserDatasets:
        apiHandler = new (std::nothrow) ApiHandlerListUserDatasets(api);
        break;
    case XcalarApiDatasetCreate:
        apiHandler = new (std::nothrow) ApiHandlerDatasetCreate(api);
        break;
    case XcalarApiDatasetGetMeta:
        apiHandler = new (std::nothrow) ApiHandlerDatasetGetMeta(api);
        break;
    case XcalarApiDatasetDelete:
        apiHandler = new (std::nothrow) ApiHandlerDatasetDelete(api);
        break;
    case XcalarApiDatasetUnload:
        apiHandler = new (std::nothrow) ApiHandlerDatasetUnload(api);
        break;
    case XcalarApiPerNodeTop:
        apiHandler = new (std::nothrow) ApiHandlerPerNodeTop(api);
        break;
    case XcalarApiRestoreTable:
        apiHandler = new (std::nothrow) ApiHandlerRestoreTable(api);
        break;
    case XcalarApiPublish:
        apiHandler = new (std::nothrow) ApiHandlerPublish(api);
        break;
    case XcalarApiUpdate:
        apiHandler = new (std::nothrow) ApiHandlerUpdate(api);
        break;
    case XcalarApiUnpublish:
        apiHandler = new (std::nothrow) ApiHandlerUnpublish(api);
        break;
    case XcalarApiCoalesce:
        apiHandler = new (std::nothrow) ApiHandlerCoalesce(api);
        break;
    default:
        status = StatusApiFunctionInvalid;
        goto CommonExit;
    }

    if (apiHandler == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate apiHandler for %s",
                strGetFromXcalarApis(api));
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (apiHandler != NULL) {
            delete apiHandler;
            apiHandler = NULL;
        }
    }

    *apiHandlerOut = apiHandler;
    return status;
}

Status
xcApiCheckIfSufficientMem(XcalarApis api)
{
    switch (api) {
    // The following apis have free passes (some of them
    // is to prevent the situation where we tell the user to free more
    // memory because we're low on memory while the user is trying to
    // free memory)
    case XcalarApiGetVersion:
    case XcalarApiShutdown:
    case XcalarApiShutdownLocal:
    case XcalarApiGetStat:
    case XcalarApiGetStatGroupIdMap:
    case XcalarApiTop:
    case XcalarApiCancelOp:
    case XcalarApiDeleteObjects:
    case XcalarApiArchiveTables:
    case XcalarApiQueryCancel:
    case XcalarApiQueryDelete:
    case XcalarApiQueryState:
    case XcalarApiKeyList:
    case XcalarApiKeyAddOrReplace:
    case XcalarApiKeyDelete:
    case XcalarApiGetNumNodes:
    case XcalarApiSessionInact:
    case XcalarApiSessionDelete:
    case XcalarApiSessionPersist:
    case XcalarApiUdfDelete:
    case XcalarApiGetConfigParams:
    case XcalarApiSetConfigParam:
    case XcalarApiAppSet:
    case XcalarApiAppRun:
    case XcalarApiAppReap:
        return StatusOk;
    default:
        break;
    }

    Status status = StatusOk;
    Config *config = Config::get();
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    size_t nodeCount = config->getActiveNodesOnMyPhysicalNode();
    size_t memRequired =
        xcalarConfig->maxOutstandApisWork_ * xcalarConfig->thrStackSize_;
    size_t resourceAvail = 0;
    status = XcSysHelper::get()->getProcFsHelper()->getSysFreeResourceSize(
        &resourceAvail);
    BailIfFailed(status);

    if ((resourceAvail / nodeCount) < memRequired) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    return status;
}

static void
initiateClusterShutdown(ApisRecvClient *apisRecvClient)
{
    // This is static because it is large and thus function will only ever be
    // called once, so we can just store it in data rather than on the heap
    static XcalarWorkItem workItem;
    static XcalarApiOutput output;
    XcalarWorkItem *workItemIn;
    size_t outputSize = 0;
    bool forceShutdown;
    unsigned ii, numNodes;
    NodeId myNodeId;
    Config *config = Config::get();
    Status status = StatusUnknown;
    Status tmpStatus = StatusUnknown;

    assertStatic(XcalarApiSizeOfOutput(output.outputResult.noOutput) <=
                 sizeof(output));

    workItemIn = &apisRecvClient->workItem_;
    workItem = *workItemIn;
    forceShutdown = workItemIn->input->shutdownInput.doShutdown;
    workItem.api = XcalarApiShutdownLocal;
    workItem.input = (XcalarApiInput *) &forceShutdown;
    workItem.inputSize = sizeof(forceShutdown);
    workItem.userId = workItemIn->userId;

    numNodes = config->getActiveNodes();
    myNodeId = config->getMyNodeId();
    status = StatusOk;
    for (ii = 0; ii < numNodes; ii++) {
        if (ii != myNodeId) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Sending shutdown request to node %u (%s:%d)",
                    ii,
                    config->getIpAddr(ii),
                    config->getApiPort(ii));

            tmpStatus = xcalarApiQueueWork(&workItem,
                                           config->getIpAddr(ii),
                                           config->getApiPort(ii),
                                           workItemIn->userId->userIdName,
                                           workItemIn->userId->userIdUnique);
            if (tmpStatus != StatusOk) {
                // We'll just have to keep going despite errors
                status = tmpStatus;
                xSyslog(moduleName,
                        XlogErr,
                        "Error sending shutdown request to node %u (%s:%d): %s",
                        ii,
                        config->getIpAddr(ii),
                        config->getApiPort(ii),
                        strGetFromStatus(status));
            }
        }
    }

    outputSize = XcalarApiSizeOfOutput(output.outputResult.noOutput);
    output.hdr.status = status.code();

    status = apisRecvClient->sendOutput(&output, outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to send response \"%s\" back to client: %s",
                strGetFromStatusCode(output.hdr.status),
                strGetFromStatus(status));
    }
}

static Status
processIncomingApiRequest(ApisRecvClient *apisRecvClient,
                          BcHandle *apisRecvObjectBc,
                          Semaphore *sem)
{
    Status status = StatusUnknown;
    ApiHandler *apiHandler = NULL;
    ApisRecvObject *apisRecvObject = NULL;
    bool workItemTracked = false;
    bool isImmediate = false;
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    XcalarWorkItem *workItem = &apisRecvClient->workItem_;
    assert(isValidXcalarApis(workItem->api));
    LicenseMgr *licenseMgr = LicenseMgr::get();

    status = xcApiCheckIfSufficientMem(workItem->api);
    if (status != StatusOk) {
        StatsLib::statAtomicIncr64(apisStatInsufficientMem);
        goto CommonExit;
    }

    status = xcApiGetApiHandler(&apiHandler, workItem->api);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (needsLicense(apiHandler) && xcalarConfig->enableLicenseEnforcement_ &&
        isTrialPeriodOver()) {
        if (!licenseMgr->licIsLoaded() ||
            (LicenseMgr::get()->getLicenseData()->getLicenseExpiryBehavior() ==
                 LicExpiryBehaviorDisable &&
             licenseMgr->licIsExpired())) {
            status = StatusLicOpDisabledUnlicensed;
            goto CommonExit;
        }
    }

    if (apiHandler->needsToRunImmediately()) {
        status = apiHandler->setImmediateTxnAndTxnLog();
    } else {
        status = apiHandler->setTxnAndTxnLog(Txn::Mode::NonLRQ,
                                             Runtime::SchedId::Sched0);
    }
    BailIfFailed(status);

    apisRecvObject = (ApisRecvObject *) apisRecvObjectBc->allocBuf(
        XidInvalid, &status);
    if (apisRecvObject == NULL) {
        status = StatusApisWorkTooManyOutstanding;
        StatsLib::statAtomicIncr64(apisStatTooManyOutstanding);
        goto CommonExit;
    }
    new (apisRecvObject)
        ApisRecvObject(apiHandler, apisRecvClient, apisRecvObjectBc, sem);

    if (apiHandler->needsToRunInline()) {
        // Processes the API in the context of listener thread. Need to be
        // careful about the kind of APIs that get processed here.
        atomicInc64(&apisOutstanding);
        StatsLib::statAtomicIncr64(apisStatOutstanding);
        StatsLib::statAtomicIncr64(
            apisStatInComingCumulative[(int) workItem->api]);
        StatsLib::statAtomicIncr64(apiStatTotalRunInline);
        apisRecvObject->run();
        apisRecvObject->done();
    } else {
        if (apiHandler->needsToRunImmediately()) {
            // Track immediate workItem
            StatsLib::statAtomicIncr64(apisStatImmedOutstanding);
            isImmediate = true;
        }

        // Track scheduled workItem
        atomicInc64(&apisOutstanding);
        StatsLib::statAtomicIncr64(apisStatOutstanding);
        StatsLib::statAtomicIncr64(
            apisStatInComingCumulative[(int) workItem->api]);
        workItemTracked = true;

        status = Runtime::get()->schedule(
            static_cast<Schedulable *>(apisRecvObject));
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not enqueue apisRecvObject onto runtime: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        StatsLib::statAtomicIncr64(apisStatScheduledOnRuntime);
    }

    // At this point, it's up to apisRecvObject->done() to free the resources
    apiHandler = NULL;
    apisRecvObject = NULL;
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (apiHandler != NULL) {
            if (apisRecvObject != NULL) {
                apisRecvObject->setApiStatus(status);
                apisRecvObject->sendOutputToClient();
            } else {
                delete apiHandler;
            }
            apiHandler = NULL;
        }

        if (apisRecvObject != NULL) {
            apisRecvObject->~ApisRecvObject();
            apisRecvObjectBc->freeBuf(apisRecvObject);
            apisRecvObject = NULL;
        }

        if (workItemTracked) {
            if (isImmediate) {
                StatsLib::statAtomicDecr64(apisStatImmedOutstanding);
            }
            assert(atomicRead64(&apisOutstanding) > 0);
            atomicDec64(&apisOutstanding);
            StatsLib::statAtomicDecr64(apisStatOutstanding);
            StatsLib::statAtomicDecr64(
                apisStatInComingCumulative[(int) workItem->api]);
        }
    }

    return status;
}

typedef IntHashTable<uint64_t,
                     ApisRecvClient,
                     &ApisRecvClient::hook,
                     &ApisRecvClient::getClientFd,
                     17,
                     hashIdentity>
    ApisRecvClientTable;

static void
xcApiRemoveClient(SocketHandle *clientFd,
                  ApisRecvClient *apisRecvClient,
                  ApisRecvClientTable *apisRecvClientTable,
                  Status rejectionStatus)
{
    Status status = StatusUnknown;
    assert(*clientFd == (SocketHandle) apisRecvClient->getClientFd());

    status = apisRecvClient->drainIncomingTcp();
    if (status == StatusOk && rejectionStatus != StatusOk) {
        apisRecvClient->sendRejection(rejectionStatus);
    }

    apisRecvClientTable->remove(*clientFd);
    // following destructor will close clientFd_ which was earlier asserted
    // to be the same as *clientFd. So no need to close *clientFd explicitly
    // here, but the param must be marked invalid to reflect its closure inside
    // the destructor.
    delete apisRecvClient;
    *clientFd = SocketHandleInvalid;
    apisRecvClient = NULL;
}

void *
xcApiStartListener(void *unused)
{
    SocketAddr socketAddr, clientAddr;
    XcalarApiInput *input = NULL;
    XcalarApiUserId *userId = NULL;
    SocketHandle listenFd = SocketHandleInvalid;
    SocketHandle clientFd = SocketHandleInvalid;
    Status status;
    Semaphore sem(0);
    StatsLib *statsLib = StatsLib::get();
    Config *config = Config::get();
    Runtime *runtime = Runtime::get();
    BcHandle *apisRecvObjectBc = NULL;
    ApisRecvClientTable *apisRecvClientTable = NULL;
    struct pollfd *pollFds = NULL;
    int descReady = 0, numFds = 0;
    static constexpr short PollFdEvents = POLLIN | POLLPRI | POLLRDHUP;
    Txn curTxn;
    char statName[StatStringLen];
    const char *incomingStatStr = "Num%sStarted";
    const char *doneStatStr = "Num%sFinished";

    pollFds =
        (struct pollfd *) memAlloc(MaxNumApisConnections * sizeof(pollFds[0]));
    if (pollFds == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "Insufficient memory to allocate pollFds");
        goto CommonExit;
    }

    apisRecvClientTable = new (std::nothrow) ApisRecvClientTable();
    if (apisRecvClientTable == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate apisRecvClientTable");
        goto CommonExit;
    }

    // Stats
    status = statsLib->initNewStatGroup("apis",
                                        &apisStatGroupId,
                                        (uint64_t) apisStatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apisStatOutstanding);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApisWorkOutstanding",
                                         apisStatOutstanding,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apisStatImmedOutstanding);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApisWorkImmedOutstanding",
                                         apisStatImmedOutstanding,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apisStatInsufficientMem);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApisWorkFailedInsufficientMem",
                                         apisStatInsufficientMem,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apisStatTooManyOutstanding);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApisWorkFailedTooManyOutstanding",
                                         apisStatTooManyOutstanding,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apisStatScheduledOnRuntime);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApisStatScheduledOnRuntime",
                                         apisStatScheduledOnRuntime,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&apiStatTotalRunInline);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                         "ApiStatTotalRunInline",
                                         apiStatTotalRunInline,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    for (int ii = (int) XcalarApiUnknown; ii < (int) XcalarApisLen; ii++) {
        status = statsLib->initStatHandle(&apisStatInComingCumulative[ii]);
        BailIfFailed(status);
        status = strSnprintf(statName,
                             sizeof(statName),
                             incomingStatStr,
                             strGetFromXcalarApis((XcalarApis) ii));
        BailIfFailed(status);
        status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                             statName,
                                             apisStatInComingCumulative[ii],
                                             StatUint64,
                                             StatAbsoluteWithNoRefVal,
                                             StatRefValueNotApplicable);
        BailIfFailed(status);

        status = statsLib->initStatHandle(&apisStatDoneCumulative[ii]);
        BailIfFailed(status);
        status = strSnprintf(statName,
                             sizeof(statName),
                             doneStatStr,
                             strGetFromXcalarApis((XcalarApis) ii));
        BailIfFailed(status);
        status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                             statName,
                                             apisStatDoneCumulative[ii],
                                             StatUint64,
                                             StatAbsoluteWithNoRefVal,
                                             StatRefValueNotApplicable);
        BailIfFailed(status);
    }

    // 1 ref for myself which I'll decrement on teardown
    atomicWrite64(&apisOutstanding, 1);
    statsInited = true;

    apisRecvObjectBc = BcHandle::create(BufferCacheObjects::XcalarApis);
    if (apisRecvObjectBc == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    assert(listenFd == SocketHandleInvalid);
    status = sockCreate(SockIPAddrAny,
                        config->getApiPort(config->getMyNodeId()),
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &listenFd,
                        &socketAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = sockSetOption(listenFd, SocketOptionReuseAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = sockSetOption(listenFd, SocketOptionCloseOnExec);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = sockBind(listenFd, &socketAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = sockListen(listenFd, 32);
    if (status != StatusOk) {
        goto CommonExit;
    }

    curTxn = Txn::newTxn(Txn::Mode::NonLRQ);
    Txn::setTxn(curTxn);
    struct timespec timeout;
    status = MsgMgr::get()->twoPcBarrier(
        MsgMgr::BarrierType::BarrierUsrnodeApisRecvReady);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "2pc barrier failed during API listener bring up: %s",
                strGetFromStatus(status));
        Txn::setTxn(Txn());
        goto CommonExit;
    }
    Txn::setTxn(Txn());

    while (!usrNodeNormalShutdown()) {
        ApisRecvClient *apisRecvClient = NULL;
        ApisRecvClient *rmApisRecvClient = NULL;

        numFds = 0;
        pollFds[0].fd = listenFd;
        pollFds[0].events = PollFdEvents;
        pollFds[0].revents = 0;
        numFds++;

        for (ApisRecvClientTable::iterator it = apisRecvClientTable->begin();
             numFds < MaxNumApisConnections &&
             (apisRecvClient = it.get()) != NULL;
             it.next()) {
            pollFds[numFds].fd = apisRecvClient->getClientFd();
            pollFds[numFds].events = PollFdEvents;
            pollFds[numFds].revents = 0;
            numFds++;
            apisRecvClient = NULL;
        }

        while (descReady == 0) {
            timeout.tv_sec = 1 * 60;  // 1 minute
            timeout.tv_nsec = 0;
            errno = 0;
            descReady = ppoll(pollFds, numFds, &timeout, NULL);
            if (descReady < 0) {
                if (errno == EINTR) {
                    if (usrNodeNormalShutdown()) {
                        break;
                    } else {
                        descReady = 0;
                        continue;
                    }
                }

                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "ppoll() failed: %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            if (descReady == 0) {
                // 1 minute of inactivity!
                while ((apisRecvClient = apisRecvClientTable->begin().get()) !=
                       NULL) {
                    // Xcalar is not a place for slowpokes.
                    // Move fast or you're out
                    clientFd = (SocketHandle) apisRecvClient->getClientFd();
                    xcApiRemoveClient(&clientFd,
                                      apisRecvClient,
                                      apisRecvClientTable,
                                      StatusApisRecvTimeout);
                }

                numFds = 0;
                pollFds[0].fd = listenFd;
                pollFds[0].events = PollFdEvents;
                numFds++;
            }
        }

        for (int jj = 0; jj < MaxNumApisConnections && descReady > 0; jj++) {
            assert(apisRecvClient == NULL);
            if (pollFds[jj].revents == 0) {
                continue;
            }

            descReady--;
            if (pollFds[jj].fd == listenFd) {
                status = sockAccept(listenFd, &clientAddr, &clientFd);
                if (status == StatusConnAborted) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Client aborted connection during tcp handshake.");
                    continue;
                } else if (status == StatusConnectionWrongHandshake) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Client attempted a handshake we didn't "
                            "understand");
                    continue;
                } else if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Aborting due to un-recoverable error during tcp "
                            "handshake: %s",
                            strGetFromStatus(status));
                    break;
                }

                // We don't want an xcApi to hold XCE hostage
                status = sockSetOption(clientFd, SocketOptionNonBlock);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to set nonblocking flag on socket %d: %s",
                            clientFd,
                            strGetFromStatus(status));
                    sockDestroy(&clientFd);
                }

                status = sockSetOption(clientFd, SocketOptionKeepAlive);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to set keepalive flag on socket %d: %s",
                            clientFd,
                            strGetFromStatus(status));
                    sockDestroy(&clientFd);
                    continue;
                }

                status = sockSetOption(clientFd, SocketOptionCloseOnExec);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to set FD_CLOEXEC flag on socket %d: %s",
                            clientFd,
                            strGetFromStatus(status));
                    sockDestroy(&clientFd);
                    continue;
                }

                apisRecvClient = new (std::nothrow) ApisRecvClient(clientFd);
                if (apisRecvClient == NULL) {
                    status = StatusNoMem;
                    xSyslog(moduleName,
                            XlogErr,
                            "Insufficient memory to allocate apisRecvClient");
                    sockDestroy(&clientFd);
                    continue;
                }

                status = apisRecvClientTable->insert(apisRecvClient);
                assert(status == StatusOk);
            } else {
                clientFd = pollFds[jj].fd;
                apisRecvClient = apisRecvClientTable->find(clientFd);
                assert(apisRecvClient != NULL);
                assert((SocketHandle) apisRecvClient->getClientFd() ==
                       clientFd);

                status = apisRecvClient->processIncoming();
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "processIncoming fail on clientFd %d:%s",
                            clientFd,
                            strGetFromStatus(status));
                    xcApiRemoveClient(&clientFd,
                                      apisRecvClient,
                                      apisRecvClientTable,
                                      status);
                    apisRecvClient = NULL;
                    continue;
                }
            }

            if (!apisRecvClient->isReady()) {
                apisRecvClient = NULL;
                continue;
            }

            XcalarWorkItem *workItem;
            workItem = &apisRecvClient->workItem_;
            status = authenticate(workItem->userId, workItem->api);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "authenticate fail on clientFd %d:%s",
                        clientFd,
                        strGetFromStatus(status));
                xcApiRemoveClient(&clientFd,
                                  apisRecvClient,
                                  apisRecvClientTable,
                                  status);
                apisRecvClient = NULL;
                continue;
            }

            if (workItem->api == XcalarApiShutdown ||
                workItem->api == XcalarApiShutdownLocal) {
                if (workItem->api == XcalarApiShutdown) {
                    initiateClusterShutdown(apisRecvClient);
                } else {
                    xSyslog(moduleName, XlogInfo, "Shutdown request received");
                }

                usrNodeShutdown = true;
                if (workItem->input->shutdownInput.doShutdown) {
                    usrNodeShutdownForce = true;
                }
                memBarrier();

                xcApiRemoveClient(&clientFd,
                                  apisRecvClient,
                                  apisRecvClientTable,
                                  StatusOk);
                apisRecvClient = NULL;
                break;
            }

            // To be handed off to ApisRecvObject. Note that
            // processIncomingApiRequest could delete apisRecvClient,
            // and so we need to remove it from the hashTable beforehand
            rmApisRecvClient = apisRecvClientTable->remove(clientFd);
            assert(rmApisRecvClient == apisRecvClient);

            status = processIncomingApiRequest(apisRecvClient,
                                               apisRecvObjectBc,
                                               &sem);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "processIncomingApiRequest (%s) failed clientFd %d %s",
                        strGetFromXcalarApis(workItem->api),
                        clientFd,
                        strGetFromStatus(status));
                xcApiRemoveClient(&clientFd,
                                  apisRecvClient,
                                  apisRecvClientTable,
                                  status);
                apisRecvClient = NULL;
                continue;
            }

            // Handed off to ApisRecvObject
            apisRecvClient = NULL;
            clientFd = SocketHandleInvalid;
            workItem = NULL;
        }
    }

    // Let all activities die down
    runtime->drainAllRunnables();
    assert(statsInited);
    assert(atomicRead64(&apisOutstanding) > 0);
    if (atomicDec64(&apisOutstanding) > 0) {
        sem.semWait();
    }
    assert(atomicRead64(&apisOutstanding) == 0);

    // Attempt to drop all the objects owned by sessions on this node
    xcApiObjectCleanup();

    assert(atomicRead64(&apisOutstanding) == 0);
    statsInited = false;

    status = StatusOk;

CommonExit:
    assert(clientFd == SocketHandleInvalid);
    if (apisRecvClientTable != NULL) {
        ApisRecvClient *apisRecvClient;
        while ((apisRecvClient = apisRecvClientTable->begin().get()) != NULL) {
            clientFd = (SocketHandle) apisRecvClient->getClientFd();
            xcApiRemoveClient(&clientFd,
                              apisRecvClient,
                              apisRecvClientTable,
                              StatusShutdownInProgress);
        }
        delete apisRecvClientTable;
        apisRecvClientTable = NULL;
    }

    if (pollFds != NULL) {
        memFree(pollFds);
        pollFds = NULL;
    }

    if (listenFd != SocketHandleInvalid) {
        sockDestroy(&listenFd);
        listenFd = SocketHandleInvalid;
    }

    if (clientFd != SocketHandleInvalid) {
        sockDestroy(&clientFd);
        clientFd = SocketHandleInvalid;
    }

    if (input != NULL) {
        memFree(input);
        input = NULL;
    }

    if (userId != NULL) {
        memFree(userId);
        userId = NULL;
    }

    runtime->drainAllRunnables();

    if (statsInited) {
        assert(atomicRead64(&apisOutstanding) > 0);
        if (atomicDec64(&apisOutstanding) > 0) {
            // There's outstanding APIs!
            sem.semWait();
        }
        assert(atomicRead64(&apisOutstanding) == 0);
        statsInited = false;
    }

    if (apisRecvObjectBc != NULL) {
        BcHandle::destroy(&apisRecvObjectBc);
        apisRecvObjectBc = NULL;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to setup API listener socket: %s",
                strGetFromStatus(status));
    }

    return NULL;
}

bool
usrNodeNormalShutdown()
{
    return usrNodeShutdown;
}

void
setUsrNodeForceShutdown()
{
    usrNodeShutdownForce = true;
}

void
unsetUsrNodeForceShutdown()
{
    usrNodeShutdownForce = false;
}

bool
usrNodeForceShutdown()
{
    return usrNodeShutdownForce;
}

bool
isShutdownInProgress()
{
    return usrNodeShutdownForce || usrNodeShutdown;
}
