// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <string.h>

#include "primitives/Primitives.h"
#include "msg/Xid.h"
#include "operators/GenericTypes.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "querymanager/QueryManager.h"
#include "queryparser/QueryParser.h"
#include "queryeval/QueryEvaluate.h"
#include "util/CmdParser.h"
#include "libapis/LibApisRecv.h"
#include "QueryParserEnums.h"
#include "bc/BufferCache.h"
#include "libapis/LibApisCommon.h"
#include "libapis/WorkItem.h"
#include "dag/DagLib.h"
#include "QueryStateEnums.h"
#include "util/MemTrack.h"
#include "usr/Users.h"
#include "optimizer/Optimizer.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "udf/UserDefinedFunction.h"
#include "app/AppMgr.h"
#include "util/FileUtils.h"
#include "stat/Statistics.h"

QueryManager *QueryManager::instance = NULL;
static constexpr const char *moduleName = "libquery";

QueryManager *
QueryManager::get()
{
    return QueryManager::instance;
}

Status
QueryManager::init()
{
    Status status;
    assert(instance == NULL);
    instance = new (std::nothrow) QueryManager;
    if (instance == NULL) {
        return StatusNoMem;
    }

    status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }

    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("jobStats",
                                        &instance->jobStatGroupId_,
                                        StatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.countJobsInQueue);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->jobStatGroupId_,
                                         "jobs.InQueue",
                                         instance->stats.countJobsInQueue,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->stats.countJobsRunning);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(instance->jobStatGroupId_,
                                         "jobs.Running",
                                         instance->stats.countJobsRunning,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
QueryManager::initInternal()
{
    Status status = StatusOk;
    unsigned shippers = XcalarConfig::get()->dfStatsShippersCount_;

    // Used to ensure session "owner" does not conflict with a real user
    atomicWrite32(&qpUnique_, QpUniquifier);

    // Launch dataflow stats app mgr thread
    auto trxn = Txn::currentTxn();
    Txn::setTxn(Txn::newImmediateTxn());
    dataflowStatsAppMgr_ = new (std::nothrow) DataflowStatsAppMgr();
    BailIfNull(dataflowStatsAppMgr_);

    for (unsigned ii = 0; ii < shippers; ii++) {
        status = Runtime::get()->schedule(dataflowStatsAppMgr_);
        BailIfFailed(status);
        dataflowStatsAppMgr_->incActiveShippers();
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to schedule dataflow stats app manager with error: %s",
                strGetFromStatus(status));
    }
    Txn::setTxn(trxn);
    return status;
}

void
QueryManager::destroy()
{
    if (instance == NULL) {
        return;
    }

    instance->destroyInternal();
    delete instance;
    instance = NULL;
}

void
QueryManager::destroyInternal()
{
    if (dataflowStatsAppMgr_) {
        if (dataflowStatsAppMgr_->getActiveShippers()) {
            dataflowStatsAppMgr_->waitUntilDone();
            xSyslog(moduleName, XlogInfo, "Exited dataflow stats app manager");
        }

        delete dataflowStatsAppMgr_;
        dataflowStatsAppMgr_ = nullptr;
    }

    queryJobHTLock_.lock();
    queryJobHashTable_.removeAll(&QueryJob::decRef);
    queryJobHTLock_.unlock();
}

void
QueryManager::QueryJob::incRef()
{
    assert(atomicRead64(&ref) > 0);
    atomicInc64(&ref);
}

void
QueryManager::QueryJob::decRef()
{
    assert(atomicRead64(&ref) > 0);
    if (atomicDec64(&ref) != 0) {
        return;
    }

    if (this->calloutIssued) {
        // Even without this call, when callout kicks in, we should be able
        // to handle the queryJob deletion gracefully.
        CalloutQueue::get()->cancel(&this->calloutHandle);
    }

    if (this->deleteCalloutIssued) {
        CalloutQueue::get()->cancel(this->deleteQueryStateCalloutHandle);
    }

    delete this;
}

QueryManager::QueryJob::QueryJob() {}

QueryManager::QueryJob::~QueryJob()
{
    delete this->deleteQueryStateCalloutHandle;
    this->deleteQueryStateCalloutHandle = NULL;

    if (this->queryGraph != NULL) {
        Status status;
        status = DagLib::get()->destroyDag(this->queryGraph,
                                           DagTypes::DestroyDeleteNodes);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Query %s destroy DAG failed:%s",
                    this->queryName,
                    strGetFromStatus(status));
        }
    }

    if (this->query) {
        memFree(this->query);
        this->query = NULL;
    }
}

void
QueryManager::queryJobTimeout(void *arg)
{
    LibNsTypes::NsId queryId = (uintptr_t)(LibNsTypes::NsId)((uintptr_t) arg);
    QueryManager *qm = QueryManager::get();
    Status status;
    char queryName[QueryMaxNameLen + 1];
    QueryJob *queryJob = NULL;

    qm->queryJobHTLock_.lock();
    queryJob = qm->queryJobHashTable_.find(queryId);
    if (queryJob == NULL) {
        status = StatusQrJobNonExist;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %ld delete on query job: %s",
                queryId,
                strGetFromStatus(status));
        qm->queryJobHTLock_.unlock();
        return;
    }
    status = strStrlcpy(queryName, queryJob->queryName, sizeof(queryName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query %ld delete on query job: %s",
                queryId,
                strGetFromStatus(status));
        qm->queryJobHTLock_.unlock();
        return;
    }
    qm->queryJobHTLock_.unlock();

    status = QueryManager::get()->requestQueryDelete(queryName);
    if (status == StatusQrJobRunning) {
        xSyslog(moduleName,
                XlogWarn,
                "Timed delete of query %s failed, please delete manually",
                queryName);
    }
    xSyslog(moduleName,
            XlogDebug,
            "Timed delete of query %s finished: %s",
            queryName,
            strGetFromStatus(status));
}

void
QueryManager::DataflowStatsAppMgr::run()
{
    // We want to rate limit the total number of dataflow stats apps
    // to 1. Any time a job completes, it is added to dataflowStatsQueue_.
    // This routine periodically checks to see if there is anything in the
    // queue.
    Semaphore sema(0);
    QueryJob *queryJob;
    QueryManager *qm = QueryManager::get();
    QueryJobPendingStatsCollection *curQueryJobPendingStatsCollection;
    while (true) {
        if (isShutdownInProgress()) {
            return;
        }
        // Empty out the dataflowStatsQueue linked list. Will break from
        // the loop when qm->dataflowStatsQueueFront_ is NULL
        while (true) {
            qm->dataflowStatsQueueLock_.lock();
            if (qm->dataflowStatsQueueFront_ == NULL) {
                qm->dataflowStatsQueueLock_.unlock();
                break;
            }
            curQueryJobPendingStatsCollection = qm->dataflowStatsQueueFront_;
            queryJob = curQueryJobPendingStatsCollection->queryJob;
            qm->dataflowStatsQueueFront_ =
                curQueryJobPendingStatsCollection->nextJob;
            if (qm->dataflowStatsQueueFront_ == NULL) {
                qm->dataflowStatsQueueRear_ = NULL;
            }
            delete curQueryJobPendingStatsCollection;
            qm->dataflowStatsQueueLock_.unlock();
            qm->runDataflowStatsApp(queryJob);
        }
        sema.timedWait(USecsPerSec);
    }
}

void
QueryManager::DataflowStatsAppMgr::done()
{
    QueryManager *qm = QueryManager::get();
    QueryJobPendingStatsCollection *curQueryJobPendingStatsCollection;
    // Clear out the dataflowStatsQueue and decRef all jobs in the queue
    qm->dataflowStatsQueueLock_.lock();
    while (qm->dataflowStatsQueueFront_) {
        curQueryJobPendingStatsCollection = qm->dataflowStatsQueueFront_;
        curQueryJobPendingStatsCollection->queryJob->decRef();
        qm->dataflowStatsQueueFront_ =
            curQueryJobPendingStatsCollection->nextJob;
        delete curQueryJobPendingStatsCollection;
    }
    qm->dataflowStatsQueueLock_.unlock();
    if ((unsigned) atomicInc32(&shippersDone_) == getActiveShippers()) {
        doneSem_.post();
    }
}

void
QueryManager::scheduleDataflowStatsApp(QueryJob *queryJob)
{
    Status status = StatusOk;
    QueryJobPendingStatsCollection *queryJobPendingStatsCollection;
    XcalarConfig *xc = XcalarConfig::get();
    if (!xc->collectStats_ || !xc->collectDataflowStats_) {
        goto CommonExit;
    }
    queryJob->incRef();
    queryJobPendingStatsCollection =
        new (std::nothrow) QueryJobPendingStatsCollection;
    BailIfNull(queryJobPendingStatsCollection);

    queryJobPendingStatsCollection->queryJob = queryJob;
    queryJobPendingStatsCollection->nextJob = NULL;

    // We don't want the query to be deleted until the stats have been
    // written out to disk, so we set statsCollectionInProgress to true.
    // We will set it to false again once the dataflow stats app completes.
    queryJob->statsCollectionInProgress = true;
    dataflowStatsQueueLock_.lock();
    if (dataflowStatsQueueRear_ == NULL) {
        dataflowStatsQueueFront_ = dataflowStatsQueueRear_ =
            queryJobPendingStatsCollection;
    } else {
        dataflowStatsQueueRear_->nextJob = queryJobPendingStatsCollection;
        dataflowStatsQueueRear_ = queryJobPendingStatsCollection;
    }
    dataflowStatsQueueLock_.unlock();

CommonExit:
    return;
}

// this app runs at the end of a query to call queryState API and process
// results of this API and store them persistently in json layout
void
QueryManager::runDataflowStatsApp(QueryJob *queryJob)
{
    QueryManager *qm = QueryManager::get();
    App *writeDataflowStats = NULL;
    char *inObj = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    json_t *json = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    Status status = StatusUnknown;
    unsigned long long startTime;
    unsigned long long endTime;
    QueryState state;
    XcalarConfig *xc = XcalarConfig::get();

    if (queryJob->status == StatusOk) {
        state = qrFinished;
    } else if (queryJob->status == StatusCanceled) {
        state = qrCancelled;
    } else {
        state = qrError;
    }

    status = qm->createStatsHistDirForQuery(queryJob);
    BailIfFailed(status);

    startTime = queryJob->stopwatch.getStartRealTime();
    endTime = queryJob->stopwatch.getEndRealTime();

    writeDataflowStats =
        AppMgr::get()->openAppHandle(AppMgr::WriteToFileDataflowStatsAppName,
                                     &handle);
    if (writeDataflowStats == NULL) {
        status = StatusAppDoesNotExist;
        xSyslog(moduleName,
                XlogErr,
                "Write Dataflow Stats app %s does not exist",
                AppMgr::WriteToFileDataflowStatsAppName);
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Starting write dataflow Stats app '%s' for job: %s",
            writeDataflowStats->getName(),
            queryJob->queryName);
    json = json_pack(
        "{"
        "s:s,"  // jobName
        "s:s,"  // sessionName
        "s:s,"  // userIdName
        "s:I,"  // startTime
        "s:I,"  // endTime
        "s:s,"  // stats_dir_path
        "s:s,"  // jobState
        "s:s,"  // jobStatus
        "s:s,"  // logPath
        "}",
        "jobName",
        queryJob->queryName,
        "sessionName",
        queryJob->sessionName,
        "userIdName",
        queryJob->userId.userIdName,
        "startTime",
        startTime,
        "endTime",
        endTime,
        "statsDirPath",
        queryJob->queryStatsHistoryDir,
        "jobState",
        strGetFromQueryState(state),
        "jobStatus",
        strGetFromStatus(queryJob->status),
        "logPath",
        xc->xcalarLogCompletePath_);
    if (json == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Unknown error json formatting dataflow stats input");
        status = StatusInval;
        goto CommonExit;
    }
    inObj = json_dumps(json, 0);
    BailIfNull(inObj);

    status = AppMgr::get()->runMyApp(writeDataflowStats,
                                     AppGroup::Scope::Local,
                                     queryJob->userId.userIdName,
                                     0,
                                     inObj,
                                     0 /* cookie */,
                                     &outStr,
                                     &errStr,
                                     &appInternalError);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Write dataflow stats app '%s' failed with status : '%s', "
                "error: '%s', output: '%s'",
                writeDataflowStats->getName(),
                strGetFromStatus(status),
                errStr ? errStr : "",
                outStr ? outStr : "");
        goto CommonExit;
    }
    xSyslog(moduleName,
            XlogInfo,
            "Completed write dataflow stats app '%s'",
            writeDataflowStats->getName());

CommonExit:
    if (writeDataflowStats != NULL) {
        AppMgr::get()->closeAppHandle(writeDataflowStats, handle);
    }
    if (inObj != NULL) {
        memFree(inObj);
        inObj = NULL;
    }
    if (json != NULL) {
        json_decref(json);
        json = NULL;
    }
    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }
    bool deleteQueryState = false;
    // using this lock to manage state of markForDeletion
    queryJob->queryLock_.lock();
    queryJob->statsCollectionInProgress = false;
    if (queryJob->markForDeletion == true) {
        deleteQueryState = true;
        queryJob->markForDeletion = false;
    }
    queryJob->queryLock_.unlock();
    if (deleteQueryState == true) {
        status = qm->requestQueryDelete(queryJob->queryName);
    }
    queryJob->decRef();
}

Status
QueryManager::createStatsHistDirForQuery(QueryJob *queryJob)
{
    Status status = StatusOk;
    char *statsHistDir = queryJob->queryStatsHistoryDir;
    size_t statsHistDirNameSize = sizeof(queryJob->queryStatsHistoryDir);
    LogLib *logLib = LogLib::get();
    char start_date[15];       // XXX: fix later to remove constant
    char start_timestamp[15];  // XXX: fix later to remove constant
    char start_hour[5];        // XXX: fix later to remove constant

    // create stats dir and record dir path in query job

    time_t ts = time(NULL);
    struct tm tm = *localtime(&ts);
    // XXX: is there a time zone issue here? with XD and XCE? think list-stats
    // can avoid having to pass-in dates in which case this should be immune
    // from time-zone issue (think also of SDK which may call list-stats)
    status = strSnprintf(start_date,
                         sizeof(start_date),
                         "%d-%02d-%02d",
                         tm.tm_year + 1900,
                         tm.tm_mon + 1,
                         tm.tm_mday);
    BailIfFailed(status);

    status = strSnprintf(start_timestamp, sizeof(start_timestamp), "%ld", ts);
    BailIfFailed(status);

    status = strSnprintf(start_hour, sizeof(start_hour), "%d", tm.tm_hour);
    BailIfFailed(status);

    // <dfStatsHistoryDir>/jobStats/<date>/<ts>-queryName

    status = strSnprintf(statsHistDir,
                         statsHistDirNameSize,
                         "%s/%s/%s/%s/%s-%s",
                         logLib->dfStatsDirPath_,
                         "jobStats",
                         start_date,
                         start_hour,
                         start_timestamp,
                         queryJob->queryName);
    BailIfFailed(status);

    // 0740 is rwx r--, ---, respectively for u,g,o
    status = FileUtils::recursiveMkdir(statsHistDir, 0740);
    BailIfFailed(status);
CommonExit:
    return status;
}

Status
QueryManager::QueryWork::optimizeRetina(DagLib::DgRetina *retina)
{
    Status status = StatusUnknown;
    DagLib *dagLib = DagLib::get();
    Dag *sessionGraph = NULL;
    Dag *instanceDag = NULL;

    status = UserMgr::get()->getDag(&userId_, sessionName_, &sessionGraph);
    BailIfFailed(status);

    retina->sessionDag = sessionGraph;
    // Optimize the dag
    status =
        dagLib
            ->instantiateRetina(retina,
                                retinaInput_->executeRetinaInput.numParameters,
                                retinaInput_->executeRetinaInput.parameters,
                                udfContainer_ == NULL
                                    ? retina->dag->getUdfContainer()
                                    : udfContainer_,
                                &instanceDag);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "instantiateBatchDataflow failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    // Fix up the column names
    status = instanceDag->convertNamesToImmediate();
    BailIfFailed(status);
    if (exportToActiveSession_) {
        if (retina->numTargets > 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Dataflow %s cannot export more than "
                    "one table to active session",
                    queryJob_->queryName);
            status = StatusExportMultipleTables;
            goto CommonExit;
        }
        status = instanceDag->convertLastExportNode();
        BailIfFailed(status);

        retina->targetNodeId[0] = instanceDag->hdr_.lastNode;
    }
    queryJob_->queryGraph = instanceDag;
    instanceDag = NULL;

CommonExit:
    sessionGraph = NULL;
    if (instanceDag != NULL) {
        Status status2 =
            dagLib->destroyDag(instanceDag, DagTypes::DestroyDeleteNodes);
        assert(status2 == StatusOk);
        instanceDag = NULL;
    }
    return (status);
}

Status
QueryManager::QueryWork::getRetinaFromTemplate(DagLib::DgRetina **retinaOut)
{
    Status status = StatusUnknown;
    DagLib *dagLib = DagLib::get();
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle nsHandle;
    RetinaNsObject *retinaNsObject = NULL;
    RetinaId retinaId;
    char retinaPathName[LibNsTypes::MaxPathNameLen];
    bool pathOpened = false;
    DagLib::DgRetina *retina = NULL;
    int ret;

    assert(retinaOut != NULL);
    // This open protects our access to the Retina.
    ret = snprintf(retinaPathName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   RetinaNsObject::PathPrefix,
                   retinaName_);
    if (ret < 0 || ret >= (int) LibNsTypes::MaxPathNameLen) {
        status = StatusOverflow;
        goto CommonExit;
    }
    nsHandle = libNs->open(retinaPathName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &retinaNsObject,
                           &status);
    if (status != StatusOk) {
        if (status == StatusAccess) {
            status = StatusRetinaInUse;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to open '%s' (readonly): %s",
                retinaPathName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pathOpened = true;
    retinaId = retinaNsObject->getRetinaId();
    memFree(retinaNsObject);
    retinaNsObject = NULL;

    // get the retina from the uploaded template
    status = dagLib->getRetinaInt(retinaId, udfContainer_, &retina);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "getDataflowInt failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    *retinaOut = retina;

CommonExit:
    if (pathOpened) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close '%s': %s",
                    retinaName_,
                    strGetFromStatus(status2));
        }
    }
    return (status);
}

// XXX TODO ENG-9062
// This API needs to be fixed. It's Synthesize specific and does not fit
// well with Shared Tables.
//
Status
QueryManager::QueryWork::grabSourceRefs(
    DagLib::DgRetina *retina,
    Xid **srcNodeIdsOut,
    Dag ***srcGraphsOut,
    DagTypes::DagId **srcGraphIdsOut,
    XcalarApiUdfContainer **srcUdfContainersOut,
    TableNsMgr::TableHandleTrack **srcHandleTrackOut,
    unsigned *numSrcNodesOut)
{
    Status status;
    bool *srcRefsAcquired = NULL;
    Xid *srcNodeIds = NULL;
    DagTypes::DagId *srcGraphIds = NULL;
    XcalarApiUdfContainer *srcUdfContainers = NULL;
    Dag **srcGraphs = NULL;
    TableNsMgr::TableHandleTrack *srcHandleTrack = NULL;
    XcalarApiDagOutput *listDagsOut = NULL;
    size_t outputSize;
    XcalarApis synthesizeApi = XcalarApiSynthesize;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    char **srcNames = NULL;
    unsigned numSrcNodes = 0;
    UserMgr *usrMgr = UserMgr::get();
    DagLib *dlib = DagLib::get();
    unsigned srcSessionOpsIdx = 0;

    // grab source refs
    status = retina->dag->listAvailableNodes("*",
                                             &listDagsOut,
                                             &outputSize,
                                             1,
                                             &synthesizeApi);
    BailIfFailed(status);

    // extract only the synthesize nodes that want tables from our session
    srcNames = new (std::nothrow) char *[listDagsOut->numNodes];
    BailIfNull(srcNames);

    for (unsigned ii = 0; ii < listDagsOut->numNodes; ii++) {
        if (listDagsOut->node[ii]->input->synthesizeInput.sameSession) {
            // the source for this synthesize is part of the retina itself
            // and does not exist in the current session
            continue;
        }
        srcNames[numSrcNodes++] =
            listDagsOut->node[ii]->input->synthesizeInput.source.name;
    }

    srcRefsAcquired = new (std::nothrow) bool[numSrcNodes];
    BailIfNull(srcRefsAcquired);

    srcNodeIds = new (std::nothrow) Xid[numSrcNodes];
    BailIfNull(srcNodeIds);

    srcGraphIds = new (std::nothrow) DagTypes::DagId[numSrcNodes];
    BailIfNull(srcGraphIds);

    srcUdfContainers = new (std::nothrow) XcalarApiUdfContainer[numSrcNodes];
    BailIfNull(srcGraphIds);

    srcGraphs = new (std::nothrow) Dag *[numSrcNodes];
    BailIfNull(srcGraphs);

    srcHandleTrack =
        new (std::nothrow) TableNsMgr::TableHandleTrack[numSrcNodes];
    BailIfNull(srcHandleTrack);

    for (unsigned ii = 0; ii < numSrcNodes; ii++) {
        status = OperatorHandler::getSourceDagNode(srcNames[ii],
                                                   retina->sessionDag,
                                                   &srcUdfContainers[ii],
                                                   &srcGraphIds[ii],
                                                   &srcNodeIds[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to get source %s: %s",
                        srcNames[ii],
                        strGetFromStatus(status));
    }

    for (unsigned ii = 0; ii < numSrcNodes; ii++) {
        // XXX TODO
        // This entire API needs to be cleaned up now.
        // Session ref is tracked in QueryEvaluate.
        if (false && retina->sessionDag->getId() != srcGraphIds[ii]) {
            status = usrMgr->trackOutstandOps(&srcUdfContainers[ii],
                                              UserMgr::OutstandOps::Inc);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', Id %ld: %s",
                            srcUdfContainers[ii].userId.userIdName,
                            srcUdfContainers[ii].sessionInfo.sessionName,
                            srcUdfContainers[ii].sessionInfo.sessionId,
                            strGetFromStatus(status));
        }
        srcSessionOpsIdx++;

        // Now the src DAG is guaranteed to be live.
        verify((srcGraphs[ii] = dlib->getDagLocal(srcGraphIds[ii])) != NULL);
    }

    for (unsigned ii = 0; ii < numSrcNodes; ii++) {
        status = srcGraphs[ii]->getDagNodeRefById(srcNodeIds[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to grab ref for source %s: %s",
                        srcNames[ii],
                        strGetFromStatus(status));
        srcRefsAcquired[ii] = true;

        status =
            srcGraphs[ii]->getTableIdFromNodeId(srcNodeIds[ii],
                                                &srcHandleTrack[ii].tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        srcNodeIds[ii],
                        strGetFromStatus(status));

        // XXX TODO ENG-9062
        // This entire API needs to be cleaned up now.
        // Disable DF level source tables locking. Needs to be revisited.
        if (false && tnsMgr->isTableIdValid(srcHandleTrack[ii].tableId)) {
            status =
                tnsMgr->openHandleToNs(srcGraphs[ii]->getSessionContainer(),
                                       srcHandleTrack[ii].tableId,
                                       LibNsTypes::ReaderShared,
                                       &srcHandleTrack[ii].tableHandle,
                                       TableNsMgr::OpenSleepInUsecs);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed to open handle to table %ld: %s",
                            srcHandleTrack[ii].tableId,
                            strGetFromStatus(status));
            srcHandleTrack[ii].tableHandleValid = true;
        }

        DagNodeTypes::Node *dagNode;
        status = srcGraphs[ii]->lookupNodeById(srcNodeIds[ii], &dagNode);
        assert(status == StatusOk);

        if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate) {
            status = StatusInval;
            BailIfFailedMsg(moduleName,
                            status,
                            "%s is not a valid source, "
                            "api not supported",
                            srcNames[ii]);
        }
    }
    assert(status == StatusOk);
    *srcNodeIdsOut = srcNodeIds;
    srcNodeIds = NULL;
    *srcHandleTrackOut = srcHandleTrack;
    srcHandleTrack = NULL;
    *srcGraphsOut = srcGraphs;
    srcGraphs = NULL;
    *srcGraphIdsOut = srcGraphIds;
    srcGraphIds = NULL;
    *srcUdfContainersOut = srcUdfContainers;
    srcUdfContainers = NULL;
    *numSrcNodesOut = numSrcNodes;

CommonExit:
    if (srcNames) {
        delete[] srcNames;
        srcNames = NULL;
    }
    if (status != StatusOk) {
        // XXX TODO ENG-9062
        // This API needs to be cleaned up.
        for (unsigned ii = 0; false && ii < srcSessionOpsIdx; ii++) {
            // Source session ops is tracked only if it's not already a local
            // session.
            if (retina->sessionDag->getId() != srcGraphIds[ii]) {
                Status status2 =
                    usrMgr->trackOutstandOps(&srcUdfContainers[ii],
                                             UserMgr::OutstandOps::Dec);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', Id %ld: %s",
                            srcUdfContainers[ii].userId.userIdName,
                            srcUdfContainers[ii].sessionInfo.sessionName,
                            srcUdfContainers[ii].sessionInfo.sessionId,
                            strGetFromStatus(status2));
                }
            }
        }

        if (srcRefsAcquired) {
            for (unsigned ii = 0; ii < numSrcNodes; ii++) {
                if (srcRefsAcquired[ii]) {
                    srcGraphs[ii]->putDagNodeRefById(srcNodeIds[ii]);
                    srcRefsAcquired[ii] = false;
                }
            }
            delete[] srcRefsAcquired;
            srcRefsAcquired = NULL;
        }

        if (srcHandleTrack) {
            for (unsigned ii = 0; ii < numSrcNodes; ii++) {
                if (srcHandleTrack[ii].tableHandleValid) {
                    tnsMgr->closeHandleToNs(&srcHandleTrack[ii].tableHandle);
                    srcHandleTrack[ii].tableHandleValid = false;
                }
            }
            delete[] srcHandleTrack;
            srcHandleTrack = NULL;
        }

        if (srcNodeIds) {
            delete[] srcNodeIds;
            srcNodeIds = NULL;
        }

        if (srcGraphIds) {
            delete[] srcGraphIds;
            srcGraphIds = NULL;
        }

        if (srcUdfContainers) {
            delete[] srcUdfContainers;
            srcUdfContainers = NULL;
        }

        if (srcGraphs) {
            delete[] srcGraphs;
            srcGraphs = NULL;
        }
    }
    if (srcRefsAcquired) {
        delete[] srcRefsAcquired;
        srcRefsAcquired = NULL;
    }
    if (listDagsOut) {
        memFree(listDagsOut);
        listDagsOut = NULL;
    }
    return status;
}

void
QueryManager::QueryWork::runOptimized()
{
    Status status = StatusUnknown;
    DagLib::DgRetina *retina = NULL;
    Dag *workspaceGraphOut = NULL;
    DagLib *dagLib = DagLib::get();

    DagTypes::NodeId dstNodeId = DagTypes::InvalidDagNodeId;
    DagNodeTypes::Node *dagNode = NULL;
    XdbId dstXdbId = XidInvalid;
    OpDetails *savedOpDetails = NULL;
    DagTypes::NodeId *parentNodeIds = NULL;
    uint64_t numParents = 0;
    bool dstNodeRefAcquired = false;
    XcalarApiExportRetinaInput exportInput;
    XcalarApiOutput *exportOutput = NULL;
    size_t exportOutputSize;
    size_t inputWithRetinaSize;
    XcalarApiInput *inputWithRetina = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    TableNsMgr::TableHandleTrack handleTrack;
    Xid *srcNodeIds = NULL;
    Dag **srcGraphs = NULL;
    DagTypes::DagId *srcGraphIds = NULL;
    XcalarApiUdfContainer *srcUdfContainers = NULL;
    XcalarApiUdfContainer sessionContainer;

    TableNsMgr::TableHandleTrack *srcHandleTrack = NULL;
    unsigned numSrcNodes = 0;
    bool addedToNs = false;
    char *nodeName = retinaInput_->executeRetinaInput.dstTable.tableName;
    size_t nodeNameSize =
        sizeof(retinaInput_->executeRetinaInput.dstTable.tableName);

    status = UserDefinedFunction::initUdfContainer(&sessionContainer,
                                                   NULL,
                                                   NULL,
                                                   queryJob_->queryName);
    BailIfFailed(status);

    if (retinaName_[0] == '\0') {
        // User passed the json query string
        // Build the retina from the queryString
        status =
            dagLib->getRetinaIntFromSerializedDag((const char *) queryInput_,
                                                  querySize_,
                                                  queryJob_->queryName,
                                                  udfContainer_,
                                                  &retina);
    } else {
        // get the retina from the template loaded into the system already
        status = getRetinaFromTemplate(&retina);
    }
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed dataflow '%s' parsing: %s",
                    queryJob_->queryName,
                    strGetFromStatus(status));

    status = optimizeRetina(retina);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to instantiate dataflow with job name '%s': %s",
                    queryJob_->queryName,
                    strGetFromStatus(status));

    status = grabSourceRefs(retina,
                            &srcNodeIds,
                            &srcGraphs,
                            &srcGraphIds,
                            &srcUdfContainers,
                            &srcHandleTrack,
                            &numSrcNodes);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed grab ref of sources in dataflow '%s': %s",
                    queryJob_->queryName,
                    strGetFromStatus(status));

    StatsLib::statAtomicDecr64(QueryManager::get()->stats.countJobsInQueue);
    StatsLib::statAtomicIncr64(QueryManager::get()->stats.countJobsRunning);

    xSyslog(moduleName,
            XlogInfo,
            "Dataflow with name %s running in mode %s",
            queryJob_->queryName,
            strGetFromExecutionMode(dataflowExecMode_));

    status = QueryEvaluate::get()->evaluate(queryJob_->queryGraph,
                                            retina->numTargets,
                                            retina->targetNodeId,
                                            queryJob_,
                                            &workspaceGraphOut,
                                            schedId_,
                                            dataflowExecMode_,
                                            &sessionContainer);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Failed dataflow %s processing request on evaluate: %s",
                queryJob_->queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (workspaceGraphOut != NULL) {
        status = workspaceGraphOut->copyOpDetails(retina->dag);
        assert(status == StatusOk);
    }

    // currently only support export to one table
    if (exportToActiveSession_) {
        // using XcalarApiExecuteRetinaInput input_ without exportRetinaBuf as
        // the node requires information only about the destTable
        // for column lineage and we don't need to replay the node anymore
        int ret;
        if (nodeName[0] == '\0') {
            ret = snprintf(nodeName,
                           nodeNameSize,
                           "%s-%lu",
                           queryJob_->queryName,
                           dstNodeId);
            if (ret < 0 || ret >= (int) nodeNameSize) {
                status = StatusNoBufs;
                goto CommonExit;
            }
        }
        ret = snprintf(exportInput.retinaName,
                       sizeof(exportInput.retinaName),
                       "%s%s%s",
                       queryJob_->queryName,
                       DagLib::SyntheticRetinaNameDelim,
                       nodeName);
        if (ret < 0 || ret >= (int) sizeof(exportInput.retinaName)) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        status = DagLib::get()->exportRetinaInt(retina,
                                                &exportInput,
                                                &exportOutput,
                                                &exportOutputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to export table to session when executing \"%s\": "
                    "%s",
                    queryJob_->queryName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // we will be replacing any existing exportBufs
        size_t inputSizeNoExportBuf =
            retinaInputSize_ -
            retinaInput_->executeRetinaInput.exportRetinaBufSize;
        inputWithRetinaSize =
            inputSizeNoExportBuf +
            exportOutput->outputResult.exportRetinaOutput.retinaCount;
        inputWithRetina =
            (XcalarApiInput *) memAllocExt(inputWithRetinaSize, moduleName);
        BailIfNullWith(inputWithRetina, StatusNoMem);

        memcpy(inputWithRetina, retinaInput_, inputSizeNoExportBuf);

        status =
            strStrlcpy(inputWithRetina->executeRetinaInput.retinaName,
                       queryJob_->queryName,
                       sizeof(inputWithRetina->executeRetinaInput.retinaName));
        BailIfFailed(status);
        status =
            strStrlcpy(inputWithRetina->executeRetinaInput.queryName,
                       queryJob_->queryName,
                       sizeof(inputWithRetina->executeRetinaInput.queryName));
        BailIfFailed(status);

        // set the pointer for the exported retina buffer to be at the
        // end of the input struct
        inputWithRetina->executeRetinaInput.exportRetinaBuf =
            (char *) ((uint64_t) inputWithRetina + inputSizeNoExportBuf);
        inputWithRetina->executeRetinaInput.exportRetinaBufSize =
            exportOutput->outputResult.exportRetinaOutput.retinaCount;
        memcpy(inputWithRetina->executeRetinaInput.exportRetinaBuf,
               exportOutput->outputResult.exportRetinaOutput.retina,
               exportOutput->outputResult.exportRetinaOutput.retinaCount);

        savedOpDetails =
            (OpDetails *) memAllocExt(sizeof(OpDetails), moduleName);
        BailIfNull(savedOpDetails);

        status = workspaceGraphOut->dropLastNodeAndReturnInfo(&dstNodeId,
                                                              &dstXdbId,
                                                              savedOpDetails);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to export table to session when executing \"%s\": "
                    "%s",
                    queryJob_->queryName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // DagNodeId needs to be always be unique cluster-wide
        inputWithRetina->executeRetinaInput.dstTable.tableId = dstNodeId =
            XidMgr::get()->xidGetNext();
        inputWithRetina->executeRetinaInput.dstTable.xdbId = dstXdbId;

        status =
            strStrlcpy(inputWithRetina->executeRetinaInput.dstTable.tableName,
                       nodeName,
                       sizeof(inputWithRetina->executeRetinaInput.dstTable
                                  .tableName));
        BailIfFailed(status);

        // get parent nodes information
        parentNodeIds = new (std::nothrow) DagTypes::NodeId[numSrcNodes];
        BailIfNull(parentNodeIds);
        for (unsigned ii = 0; ii < numSrcNodes; ii++) {
            parentNodeIds[numParents++] = srcNodeIds[ii];
        }

        // create new dag node, grab ref and change state of node
        handleTrack.tableId = tnsMgr->genTableId();
        status = retina->sessionDag->createNewDagNode(XcalarApiExecuteRetina,
                                                      inputWithRetina,
                                                      inputWithRetinaSize,
                                                      dstXdbId,
                                                      handleTrack.tableId,
                                                      nodeName,
                                                      numParents,
                                                      srcGraphs,
                                                      parentNodeIds,
                                                      &dstNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create graphNode for table \"%s\": %s",
                    nodeName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // As soon as we create the node, it is globally visible, and can
        // be dropped. This is why we need to grab a ref to it before it's
        // safe to udpate the node
        status = retina->sessionDag->getDagNodeRefById(dstNodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not get refCount to dagNode: %s (%lu) (status: "
                    "%s)",
                    nodeName,
                    dstNodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        dstNodeRefAcquired = true;

        status = tnsMgr->addToNs(retina->sessionDag->getSessionContainer(),
                                 handleTrack.tableId,
                                 nodeName,
                                 retina->sessionDag->getId(),
                                 dstNodeId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to publish table %s %ld to table "
                        "namespace: %s",
                        nodeName,
                        handleTrack.tableId,
                        strGetFromStatus(status));
        addedToNs = true;

        status =
            tnsMgr->openHandleToNs(retina->sessionDag->getSessionContainer(),
                                   handleTrack.tableId,
                                   LibNsTypes::ReaderShared,
                                   &handleTrack.tableHandle,
                                   TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        handleTrack.tableId,
                        strGetFromStatus(status));
        handleTrack.tableHandleValid = true;

        status = retina->sessionDag->setOpDetails(dstNodeId, savedOpDetails);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set opDetails for dagNode \"%s\" (%lu): %s",
                    nodeName,
                    dstNodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status =
            retina->sessionDag->changeDagNodeState(dstNodeId, DgDagStateReady);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not change dagNode %s (%lu) state to Ready. "
                    "Status: %s",
                    nodeName,
                    dstNodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = retina->sessionDag->lookupNodeById(dstNodeId, &dagNode);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to lookup dag node %lu: %s",
                    dstNodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        dagNode->dagNodeHdr.apiDagNodeHdr.pinned = pinResults_;

        retina->sessionDag->putDagNodeRefById(dstNodeId);
        dstNodeRefAcquired = false;

        if (handleTrack.tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
            handleTrack.tableHandleValid = false;
        }

        dagNode = NULL;
        dstNodeId = DagTypes::InvalidDagNodeId;
        dstXdbId = XidInvalid;
        addedToNs = false;
    }

CommonExit:
    queryStatus_ = status;

    // free src ref and handles
    if (srcGraphIds) {
        UserMgr *usrMgr = UserMgr::get();

        // XXX TODO ENG-9062
        // Does not fit with Shared table abstraction.
        for (unsigned ii = 0; false && ii < numSrcNodes; ii++) {
            // Source session ops is tracked only if it's not already a local
            // session.
            if (retina->sessionDag->getId() != srcGraphIds[ii]) {
                Status status2 =
                    usrMgr->trackOutstandOps(&srcUdfContainers[ii],
                                             UserMgr::OutstandOps::Dec);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', Id %ld: %s",
                            srcUdfContainers[ii].userId.userIdName,
                            srcUdfContainers[ii].sessionInfo.sessionName,
                            srcUdfContainers[ii].sessionInfo.sessionId,
                            strGetFromStatus(status2));
                }
            }
        }
    }

    if (srcNodeIds != NULL) {
        for (unsigned ii = 0; ii < numSrcNodes; ii++) {
            srcGraphs[ii]->putDagNodeRefById(srcNodeIds[ii]);
        }
        delete[] srcNodeIds;
        srcNodeIds = NULL;
    }

    if (srcGraphIds) {
        delete[] srcGraphIds;
        srcGraphIds = NULL;
    }

    if (srcUdfContainers) {
        delete[] srcUdfContainers;
        srcUdfContainers = NULL;
    }

    if (srcGraphs) {
        delete[] srcGraphs;
        srcGraphs = NULL;
    }

    if (srcHandleTrack != NULL) {
        for (unsigned ii = 0; ii < numSrcNodes; ii++) {
            if (srcHandleTrack[ii].tableHandleValid) {
                tnsMgr->closeHandleToNs(&srcHandleTrack[ii].tableHandle);
                srcHandleTrack[ii].tableHandleValid = false;
            }
        }
        delete[] srcHandleTrack;
        srcHandleTrack = NULL;
    }
    if (parentNodeIds != NULL) {
        delete[] parentNodeIds;
        parentNodeIds = NULL;
        numParents = 0;
    }
    if (savedOpDetails != NULL) {
        memFree(savedOpDetails);
        savedOpDetails = NULL;
    }
    if (dstNodeId != DagTypes::InvalidDagNodeId) {
        dagNode = NULL;
        if (dstNodeRefAcquired) {
            retina->sessionDag->putDagNodeRefById(dstNodeId);
            dstNodeRefAcquired = false;
        }

        if (addedToNs && status != StatusTableAlreadyExists) {
            tnsMgr->removeFromNs(retina->sessionDag->getSessionContainer(),
                                 handleTrack.tableId,
                                 nodeName);
            addedToNs = false;
        }

        if (handleTrack.tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
            handleTrack.tableHandleValid = false;
        }

        // This doesn't actually drop the xdb
        Status status2 =
            retina->sessionDag->dropAndChangeState(dstNodeId, DgDagStateError);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Failed to drop and change dagNode (%lu) state to "
                    "Error: %s",
                    dstNodeId,
                    strGetFromStatus(status2));
        }
        dstNodeId = DagTypes::InvalidDagNodeId;
    }
    if (dstXdbId != XidInvalid) {
        XdbMgr::get()->xdbDrop(dstXdbId);
        dstXdbId = XidInvalid;
    }

    assert(!dstNodeRefAcquired);
    assert(dstNodeId == DagTypes::InvalidDagNodeId);

    if (workspaceGraphOut != NULL) {
        Status status2 =
            DagLib::get()->destroyDag(workspaceGraphOut,
                                      DagTypes::DestroyDeleteAndCleanNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy workspace graph for dataflow '%s': %s",
                    queryJob_->queryName,
                    strGetFromStatus(status2));
        }
        workspaceGraphOut = NULL;
    }

    if (exportOutput != NULL) {
        memFree(exportOutput);
        exportOutput = NULL;
    }

    if (inputWithRetina != NULL) {
        memFree(inputWithRetina);
        inputWithRetina = NULL;
    }

    if (retina != NULL) {
        DagLib::get()->putRetinaInt(retina);
        retina = NULL;
    }
}

void
QueryManager::QueryWork::runNonOptimized()
{
    Dag *queryGraph = NULL, *workspaceGraph = NULL;
    Status status = StatusUnknown;
    uint64_t numDagNodes = 0;
    UserMgr *userMgr = UserMgr::get();
    DagLib *dagLib = DagLib::get();

    xSyslog(moduleName,
            XlogDebug,
            "Input dataflow is: %s, size %llu",
            queryInput_,
            (unsigned long long) querySize_);

    // check if the query can be run optimized
    // Get the DAG handle that was just created for the session or the
    // supplied session name if we did not create one.
    status = userMgr->getDag(&userId_, sessionName_, &workspaceGraph);
    BailIfFailed(status);
    assert(workspaceGraph != NULL);

    if (udfContainer_ == NULL) {
        // if the query didn't come in with a udfContainer,
        // use the workbook default
        status = QueryParser::get()->parse(queryInput_,
                                           workspaceGraph->getUdfContainer(),
                                           &queryGraph,
                                           &numDagNodes);
        if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        status = QueryParser::get()->parse(queryInput_,
                                           udfContainer_,
                                           &queryGraph,
                                           &numDagNodes);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    status = QueryEvaluate::get()->stepThrough(workspaceGraph,
                                               &queryGraph,
                                               numDagNodes,
                                               true,
                                               schedId_,
                                               pinResults_,
                                               queryJob_);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    queryStatus_ = status;
    if (queryGraph != NULL) {
        Status status2 =
            dagLib->destroyDag(queryGraph, DagTypes::DestroyDeleteNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Dataflow %s destroy DAG failed:%s",
                    queryJob_->queryName,
                    strGetFromStatus(status2));
        }
    }
}

void
QueryManager::QueryWork::done()
{
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    // Drop the session refcount now.
    Status status = UserMgr::get()->trackOutstandOps(&userId_,
                                                     sessionName_,
                                                     UserMgr::OutstandOps::Dec);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed dec on session %s outstanding ops for dataflow '%s': "
                "%s",
                sessionName_,
                queryJob_->queryName,
                strGetFromStatus(status));
    }

    if (usrNodeNormalShutdown() || usrNodeForceShutdown()) {
        // For now, just bail here since shutdown is in-progress.
        return;
    }

    if (isOptimized_) {
        StatsLib::statAtomicDecr64(QueryManager::get()->stats.countJobsRunning);
    }

    queryJob_->status = queryStatus_;
    // We need to run the dataflow stats app before updating the
    // queryState value. This is because XD polls the system and checks
    // queryState to see if the dataflow has come to completion. Thus,
    // we want to make sure we set statsCollectionInProgress=True before XD sees
    // the change in queryState (just in case the query is immediately
    // deleted by XD before we have a chance to set statsCollectionInProgress)
    if (collectStats_ || isOptimized_) {
        QueryManager::get()->scheduleDataflowStatsApp(queryJob_);
    }

    if (queryStatus_ == StatusOk) {
        atomicWrite32(&queryJob_->queryState, qrFinished);
    } else if (queryStatus_ == StatusCanceled) {
        atomicWrite32(&queryJob_->queryState, qrCancelled);
    } else {
        /*
         * There are situations (e.g. errors during param parsing), when
         * queryState hasn't yet gone beyond qrNotStarted, but there's a
         * failure, as indicated by status != StatusOk here. So, the query
         * state must be fixed to be qrError. Otherwise, some other
         * sub-system (e.g. waitForQuery() in libapisSanity) which has
         * submitted a query and waiting for a terminal state (qrError or
         * qrFinished) will wait indefinitely, leading to hangs, which are
         * hard to debug.
         */
        atomicWrite32(&queryJob_->queryState, qrError);
    }

    queryJob_->stopwatch.stop();
    queryJob_->stopwatch.getPrintableTime(hours,
                                          minutesLeftOver,
                                          secondsLeftOver,
                                          millisecondsLeftOver);
    xSyslog(moduleName,
            queryStatus_ == StatusOk ? XlogInfo : XlogErr,
            "Dataflow execution with query name '%s' finished in "
            "%lu:%02lu:%02lu.%03lu: %s",
            queryJob_->queryName,
            hours,
            minutesLeftOver,
            secondsLeftOver,
            millisecondsLeftOver,
            strGetFromStatus(queryStatus_));

    if (queryStatus_ == StatusOk && cleanUpState_) {
        status = QueryManager::get()->requestQueryDelete(queryJob_->queryName);
        if (status != StatusOk && status != StatusStatsCollectionInProgress) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete job state with name '%s': %s",
                    queryJob_->queryName,
                    strGetFromStatus(status));
        }
    } else {
        // let's create a Callout to delete the job state
        queryJob_->deleteQueryStateCalloutHandle =
            new (std::nothrow) CalloutQueue::Handle();
        if (queryJob_->deleteQueryStateCalloutHandle != NULL) {
            status = CalloutQueue::get()
                         ->insert(queryJob_->deleteQueryStateCalloutHandle,
                                  XcalarConfig::get()
                                      ->dataflowStateDeleteTimeoutinSecs_,
                                  0,
                                  0,
                                  QueryManager::queryJobTimeout,
                                  (void *) (uintptr_t)(queryJob_->queryId),
                                  (char *) "dataflow state delete timeout",
                                  CalloutQueue::FlagsNone);
            queryJob_->deleteCalloutIssued = status == StatusOk;
        } else {
            // failed to allocate memory for deleteQueryStateCalloutHandle
            status = StatusNoMem;
        }
        if (status != StatusOk) {
            // XXX may be set queryJob_->status with status failed?
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create callout to delete job state with name "
                    "'%s': %s",
                    queryJob_->queryName,
                    strGetFromStatus(status));
        }
    }

    queryJob_->decRef();
    delete this;
}

Status
QueryManager::validateExecuteRequest(
    const xcalar::compute::localtypes::Dataflow::ExecuteRequest *executeReq)
{
    if (executeReq->dataflow_str().empty() &&
        executeReq->dataflow_name().empty()) {
        xSyslog(moduleName,
                XlogErr,
                "Dataflow json string or name should be provided.");
        return (StatusInval);
    }

    if (!executeReq->optimized() && executeReq->dataflow_str().empty()) {
        xSyslog(moduleName,
                XlogErr,
                "Dataflow json string should be provided.");
        return (StatusInval);
    }

    if (!executeReq->scope().has_workbook()) {
        xSyslog(moduleName, XlogErr, "session information not provided!");
        return (StatusInval);
    }

    return (StatusOk);
}

Status
QueryManager::QueryWork::initWork(
    const xcalar::compute::localtypes::Dataflow::ExecuteRequest *executeReq,
    QueryJob *queryJob,
    XcalarApiUserId *user)
{
    Status status;
    UserMgr *usrMgr = UserMgr::get();
    XcalarApiUdfContainer *udfContainer = NULL;
    const char *sessionName =
        executeReq->scope().workbook().name().workbookname().c_str();
    const char *queryStr = executeReq->dataflow_str().c_str();

    querySize_ = executeReq->dataflow_str().size() + 1;
    pinResults_ = executeReq->pin_results();
    exportToActiveSession_ = executeReq->export_to_active_session();
    isOptimized_ = executeReq->optimized();
    collectStats_ = executeReq->collect_stats();
    cleanUpState_ = executeReq->clean_job_state();
    dataflowExecMode_ = (ExecutionMode) executeReq->execution_mode();

    status = strStrlcpy(sessionName_, sessionName, sizeof(sessionName_));
    BailIfFailed(status);

    if (!executeReq->udf_user_name().empty() &&
        !executeReq->udf_session_name().empty()) {
        status = usrMgr->getUdfContainer(executeReq->udf_user_name().c_str(),
                                         executeReq->udf_session_name().c_str(),
                                         &udfContainer);
        assert(status == StatusOk || udfContainer == NULL);
        BailIfFailedMsg(moduleName,
                        status,
                        "could not find container for user %s session %s",
                        executeReq->udf_user_name().c_str(),
                        executeReq->udf_session_name().c_str());
    }
    if (executeReq->sched_name().empty()) {
        schedId_ = Runtime::SchedId::MaxSched;  // sched will be decided later
    } else {
        schedId_ =
            Runtime::getSchedIdFromName(executeReq->sched_name().c_str(),
                                        executeReq->sched_name().size() + 1);
        if (static_cast<uint8_t>(schedId_) >= Runtime::TotalFastPathScheds) {
            status = StatusInval;
            BailIfFailedMsg(moduleName,
                            status,
                            "Scheduler name %s is invalid: %s",
                            executeReq->sched_name().c_str(),
                            strGetFromStatus(status));
        }
    }
    if (udfContainer != NULL) {
        udfContainer_ =
            (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                                  moduleName);
        BailIfNull(udfContainer_);
        memcpy(udfContainer_, udfContainer, sizeof(*udfContainer));
    }

    memcpy(&userId_, user, sizeof(*user));
    queryInput_ = (char *) memAlloc(querySize_);
    BailIfNull(queryInput_);
    memcpy(queryInput_, queryStr, querySize_);

    status = strStrlcpy(retinaName_,
                        executeReq->dataflow_name().c_str(),
                        sizeof(retinaName_));
    BailIfFailed(status);

    // creating XcalarApiExecuteRetinaInput without exportBuf here
    retinaInputSize_ =
        sizeof(XcalarApiExecuteRetinaInput) +
        sizeof(XcalarApiParameter) * executeReq->parameters_size();
    retinaInput_ = (XcalarApiInput *) memAllocExt(retinaInputSize_, moduleName);
    BailIfNull(retinaInput_);
    memZero(retinaInput_, retinaInputSize_);
    convertProtoInputToApiInput(executeReq, retinaInput_);

    queryJob_ = queryJob;
CommonExit:
    return status;
}

Status
QueryManager::initQueryJob(QueryJob *queryJob,
                           LibNsTypes::NsId queryId,
                           const char *queryName,
                           const char *sessionName,
                           const XcalarApiUserId *userId)
{
    new (queryJob) QueryJob();

    atomicWrite32(&queryJob->queryState, qrNotStarted);
    assertStatic(sizeof(queryJob->userId) == sizeof(*userId));
    memcpy(&queryJob->userId, userId, sizeof(queryJob->userId));
    queryJob->status = StatusOk;
    int ret = snprintf(queryJob->queryName,
                       sizeof(queryJob->queryName),
                       "%s",
                       queryName);
    if (ret < 0 || ret >= (int) sizeof(queryJob->queryName)) {
        return StatusNameTooLong;
    }
    if (sessionName != NULL) {
        ret = snprintf(queryJob->sessionName,
                       sizeof(queryJob->sessionName),
                       "%s",
                       sessionName);
        if (ret >= (int) sizeof(queryJob->sessionName)) {
            return StatusNameTooLong;
        }
    } else {
        const char *tempSessionName = "NoSessionNameProvided";
        ret = snprintf(queryJob->sessionName,
                       sizeof(queryJob->sessionName),
                       "%s",
                       tempSessionName);
        if (ret < 0 || ret >= (int) sizeof(queryJob->sessionName)) {
            return StatusNameTooLong;
        }
    }
    queryJob->queryId = queryId;
    queryJob->workspaceGraph = NULL;
    queryJob->queryGraph = NULL;
    queryJob->currentRunningNodeName = NULL;
    queryJob->currentRunningNodeId = XidInvalid;
    queryJob->query = NULL;
    queryJob->calloutIssued = false;
    queryJob->markedForCancelling = false;
    queryJob->systemStatsAppLaunched = false;
    atomicWrite64(&queryJob->ref, 1);
    queryJob->markForDeletion = false;
    queryJob->statsCollectionInProgress = false;
    return StatusOk;
}

Status
QueryManager::initQueryNS(const char *queryName,
                          LibNsTypes::NsHandle *nsHandle,
                          LibNsTypes::NsId *queryId,
                          bool deleteIfExists)
{
    Status status = StatusOk;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    const NodeId nodeId = Config::get()->getMyNodeId();
    QueryRecord queryRecord(nodeId);
    bool nsHandleValid = false;
    bool queryIdValid = false;
    int ret = snprintf(fullyQualName,
                       LibNsTypes::MaxPathNameLen,
                       "%s%s",
                       QueryManager::NsPrefix,
                       queryName);
    if (ret < 0 || ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s processing request on queryName: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // setup query, check if we've run this before
    if (deleteIfExists) {
        *nsHandle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
        if (status == StatusOk) {
            status = libNs->close(*nsHandle, NULL);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request on NS "
                        "close: "
                        "%s",
                        queryName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            status = requestQueryDelete(queryName);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request Query "
                        "delete: %s",
                        queryName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else if (status == StatusNsNotFound) {
            status = StatusOk;
        } else {
            if (status == StatusAccess) {
                status = StatusQrQueryInUse;
            } else if (status == StatusNsInvalidObjName) {
                status = StatusQrQueryNameInvalid;
            } else if (status == StatusPendingRemoval) {
                status = StatusQrQueryAlreadyDeleted;
            }
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query graph %s processing request on NS open: "
                    "%s",
                    queryName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    *queryId = libNs->publish(fullyQualName, &queryRecord, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusQrQueryNameInvalid;
        } else if (status == StatusPendingRemoval || status == StatusExist) {
            status = StatusQrQueryAlreadyExists;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s processing request on NS publish: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    queryIdValid = true;

    *nsHandle = libNs->open(fullyQualName, LibNsTypes::ReaderShared, &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusQrQueryNameInvalid;
        } else if (status == StatusAccess) {
            status = StatusQrQueryInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusQrQueryAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusQrQueryNotExist;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s processing request on NS open: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;

CommonExit:
    if (status != StatusOk) {
        if (nsHandleValid) {
            Status status2 = libNs->close(*nsHandle, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query %s processing request on NS close: "
                        "%s",
                        queryName,
                        strGetFromStatus(status2));
            }
        }
        if (queryIdValid) {
            Status status2 = libNs->remove(*queryId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request on"
                        " NS remove: %s",
                        queryName,
                        strGetFromStatus(status2));
                if (status == StatusOk) {
                    status = status2;
                }
            }
        }
    }
    return status;
}

void
QueryManager::convertProtoInputToApiInput(
    const xcalar::compute::localtypes::Dataflow::ExecuteRequest
        *executeDFProtoInput,
    XcalarApiInput *executeRetinaApiInput)
{
    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.retinaName,
                   executeDFProtoInput->dataflow_name().c_str(),
                   sizeof(
                       executeRetinaApiInput->executeRetinaInput.retinaName)));
    // if no query name specified, use retinaName
    if (executeDFProtoInput->job_name().empty()) {
        verifyOk(strStrlcpy(executeRetinaApiInput->executeRetinaInput.queryName,
                            executeDFProtoInput->dataflow_name().c_str(),
                            sizeof(executeRetinaApiInput->executeRetinaInput
                                       .retinaName)));
    } else {
        verifyOk(strStrlcpy(executeRetinaApiInput->executeRetinaInput.queryName,
                            executeDFProtoInput->job_name().c_str(),
                            sizeof(executeRetinaApiInput->executeRetinaInput
                                       .queryName)));
    }

    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.udfUserName,
                   executeDFProtoInput->udf_user_name().c_str(),
                   sizeof(
                       executeRetinaApiInput->executeRetinaInput.udfUserName)));
    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.udfSessionName,
                   executeDFProtoInput->udf_session_name().c_str(),
                   sizeof(executeRetinaApiInput->executeRetinaInput
                              .udfSessionName)));
    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.schedName,
                   executeDFProtoInput->sched_name().c_str(),
                   sizeof(
                       executeRetinaApiInput->executeRetinaInput.schedName)));
    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.dstTable.tableName,
                   executeDFProtoInput->dest_table().c_str(),
                   sizeof(executeRetinaApiInput->executeRetinaInput.dstTable
                              .tableName)));

    verifyOk(
        strStrlcpy(executeRetinaApiInput->executeRetinaInput.userId.userIdName,
                   executeDFProtoInput->scope()
                       .workbook()
                       .name()
                       .username()
                       .c_str(),
                   sizeof(executeRetinaApiInput->executeRetinaInput.userId
                              .userIdName)));
    executeRetinaApiInput->executeRetinaInput.userId.userIdUnique = NULL;

    executeRetinaApiInput->executeRetinaInput.exportToActiveSession =
        executeDFProtoInput->export_to_active_session();
    executeRetinaApiInput->executeRetinaInput.pinResults =
        executeDFProtoInput->pin_results();
    // parameters
    executeRetinaApiInput->executeRetinaInput.numParameters =
        executeDFProtoInput->parameters_size();
    for (int ii = 0; ii < executeDFProtoInput->parameters_size(); ii++) {
        xcalar::compute::localtypes::Dataflow::Parameter protoParam =
            executeDFProtoInput->parameters(ii);
        verifyOk(
            strStrlcpy(executeRetinaApiInput->executeRetinaInput.parameters[ii]
                           .parameterName,
                       protoParam.name().c_str(),
                       sizeof(executeRetinaApiInput->executeRetinaInput
                                  .parameters[ii]
                                  .parameterName)));
        verifyOk(
            strStrlcpy(executeRetinaApiInput->executeRetinaInput.parameters[ii]
                           .parameterValue,
                       protoParam.value().c_str(),
                       sizeof(executeRetinaApiInput->executeRetinaInput
                                  .parameters[ii]
                                  .parameterValue)));
    }
}

// Executes query graph. Input queryGraph will be handed off to the queryJob
// which will be responsible for destroying it
Status
QueryManager::processQueryGraph(const XcalarApiUserId *userId,
                                Dag **queryGraphIn,
                                uint64_t numTarget,
                                DagTypes::NodeId targetNodeArray[],
                                Dag **workspaceGraphOut,
                                const char *queryName,
                                const char *sessionName,
                                Runtime::SchedId schedId)
{
    Status status;
    LibNs *libNs = LibNs::get();
    const NodeId nodeId = Config::get()->getMyNodeId();
    QueryJob *queryJob = NULL;
    LibNsTypes::NsHandle nsHandle;
    QueryRecord queryRecord(nodeId);
    bool nsPublished = false;
    bool queryEvalDone = false;
    LibNsTypes::NsId queryId;
    bool queryJobInited = false;
    XcalarApiUdfContainer sessionContainer;

    status = UserDefinedFunction::initUdfContainer(&sessionContainer,
                                                   NULL,
                                                   NULL,
                                                   queryName);
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogInfo,
            "Query graph %s: processing request",
            queryName);

    status = initQueryNS(queryName, &nsHandle, &queryId, true);
    BailIfFailed(status);
    nsPublished = true;

    queryJob = new (std::nothrow) QueryJob();
    BailIfNullWith(queryJob, StatusNoMem);

    status = initQueryJob(queryJob, queryId, queryName, sessionName, userId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query graph %s processing request on Query Job"
                " init: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    queryJobInited = true;

    queryJob->queryGraph = *queryGraphIn;
    *queryGraphIn = NULL;

    queryJobHTLock_.lock();
    // The moment it's inserted, it's globally visible. This cannot fail
    // since LibNs insert guarantees that this globally unique.
    queryJob->incRef();
    verifyOk(queryJobHashTable_.insert(queryJob));
    queryJobHTLock_.unlock();

    // Set a flag indicating that the evaluation of the query was attempted
    // and if it errors out (e.g. gets cancelled) or completes the query
    // name remains in libNs so that the user can requestQueryState.  This
    // remains so until a requestQueryDelete is done.
    queryEvalDone = true;
    status = QueryEvaluate::get()->evaluate(queryJob->queryGraph,
                                            numTarget,
                                            targetNodeArray,
                                            queryJob,
                                            workspaceGraphOut,
                                            schedId,
                                            DFS,
                                            &sessionContainer);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query graph %s processing request on evaluate: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (queryJob != NULL) {
        queryJob->stopwatch.stop();

        queryJob->status = status;
        // We need to run the dataflow stats app before updating the
        // queryState value. This is because XD polls the system and checks
        // queryState to see if the dataflow has come to completion. Thus,
        // we want to make sure we set statsCollectionInProgress=True before XD
        // sees the change in queryState (just in case the query is immediately
        // deleted by XD before we have a chance to set
        // statsCollectionInProgress
        if (queryJobInited) {
            scheduleDataflowStatsApp(queryJob);
        }

        if (status == StatusOk) {
            atomicWrite32(&queryJob->queryState, qrFinished);
        } else if (status == StatusCanceled) {
            atomicWrite32(&queryJob->queryState, qrCancelled);
        } else {
            atomicWrite32(&queryJob->queryState, qrError);
        }
    }

    if (nsPublished) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query graph %s processing request on NS close: "
                    "%s",
                    queryName,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
        if (!queryEvalDone) {
            Status status2 = libNs->remove(nsHandle.nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request on"
                        " NS remove: %s",
                        queryName,
                        strGetFromStatus(status2));
                if (status == StatusOk) {
                    status = status2;
                }
            }
        }
    }

    if (status != StatusOk) {
        if (queryJob != NULL) {
            if (queryJobInited == true) {
                queryJob->decRef();
                queryJob = NULL;
            } else {
                delete queryJob;
                queryJob = NULL;
            }
        }

        if (workspaceGraphOut != NULL && *workspaceGraphOut != NULL) {
            // An error is being returned so don't return the workspace
            // graph to the caller.
            Status status2 =
                DagLib::get()->destroyDag(*workspaceGraphOut,
                                          DagTypes::DestroyDeleteAndCleanNodes);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to destroy workspace graph for query '%s': "
                        "%s",
                        queryName,
                        strGetFromStatus(status2));
                // Continue...
            }
            *workspaceGraphOut = NULL;
        }
    } else {
        if (queryJob != NULL) {
            queryJob->decRef();
            queryJob = NULL;
        }
    }

    return status;
}

Status
QueryManager::processDataflow(
    const xcalar::compute::localtypes::Dataflow::ExecuteRequest
        *executeDFProtoInput,
    xcalar::compute::localtypes::Dataflow::ExecuteResponse
        *executeRetinaProtoOutput)
{
    Status status = StatusUnknown;
    LibNs *libNs = LibNs::get();
    QueryWork *queryWork = NULL;
    QueryJob *queryJob = NULL;
    LibNsTypes::NsHandle nsHandle;
    bool nsPublished = false;
    bool queryEvalDone = false;
    LibNsTypes::NsId queryId;
    bool queryJobInited = false;
    bool cleanupState = true;
    bool insertHt = false;
    bool trackOpsToSession = false;
    UserMgr *usrMgr = UserMgr::get();
    auto workbookSpec = executeDFProtoInput->scope().workbook();
    char queryName[XcalarApiMaxTableNameLen + 1];
    const char *sessionName =
        executeDFProtoInput->scope().workbook().name().workbookname().c_str();
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;

    status = validateExecuteRequest(executeDFProtoInput);
    BailIfFailed(status);

    if (executeDFProtoInput->job_name().empty()) {
        if (!executeDFProtoInput->dataflow_name().empty()) {
            status = strStrlcpy(queryName,
                                executeDFProtoInput->dataflow_name().c_str(),
                                sizeof(queryName));
            BailIfFailed(status);
        } else {
            int ret = snprintf(queryName,
                               sizeof(queryName),
                               "XcJob-%lu",
                               XidMgr::get()->xidGetNext());
            if (ret < 0 || ret >= (int) sizeof(queryName)) {
                status = StatusNoBufs;
                goto CommonExit;
            }
        }
    } else {
        status = strStrlcpy(queryName,
                            executeDFProtoInput->job_name().c_str(),
                            sizeof(queryName));
        BailIfFailed(status);
    }

    xSyslog(moduleName,
            XlogInfo,
            "Dataflow with query name %s processing as %soptimized",
            queryName,
            executeDFProtoInput->optimized() ? "" : "non-");

    if (strlen(sessionName) == 0) {
        status = StatusInval;
        BailIfFailedMsg(moduleName,
                        status,
                        "Session information is required to process dataflow "
                        "with query name '%s': %s",
                        queryName,
                        strGetFromStatus(status));
    }

    status = usrMgr->getUserAndSessInput(&workbookSpec, &user, &sessInput);
    BailIfFailed(status);
    status = usrMgr->trackOutstandOps(&user,
                                      sessInput.sessionName,
                                      UserMgr::OutstandOps::Inc);
    BailIfFailed(status);
    trackOpsToSession = true;

    status = initQueryNS(queryName, &nsHandle, &queryId, true);
    BailIfFailed(status);
    nsPublished = true;

    queryJob = new (std::nothrow) QueryJob();
    BailIfNullWith(queryJob, StatusNoMem);

    status = initQueryJob(queryJob, queryId, queryName, sessionName, &user);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query graph %s processing request on Query Job"
                " init: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    queryJobInited = true;

    queryWork = new (std::nothrow) QueryWork();
    BailIfNullMsg(queryWork,
                  StatusNoMem,
                  moduleName,
                  "Failed dataflow work memory allocation for query name "
                  "'%s'",
                  queryName);
    status = queryWork->initWork(executeDFProtoInput, queryJob, &user);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed dataflow work init for query name '%s': %s",
                    queryName,
                    strGetFromStatus(status));

    queryJobHTLock_.lock();
    // The moment it's inserted, it's globally visible. This cannot fail
    // since LibNs insert guarantees that this globally unique.
    queryJob->incRef();
    verifyOk(queryJobHashTable_.insert(queryJob));
    queryJobHTLock_.unlock();
    insertHt = true;
    if (!executeDFProtoInput->optimized()) {
        status = CalloutQueue::get()
                     ->insert(&queryJob->calloutHandle,
                              XcalarConfig::get()->queryJobTimeoutinSecs_,
                              0,
                              0,
                              QueryManager::queryJobTimeout,
                              (void *) (uintptr_t)(queryId),
                              (char *) "query job timeout",
                              CalloutQueue::FlagsNone);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query %s processing request on callout queue"
                    " insert: %s",
                    queryName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        queryJob->calloutIssued = true;
    }
    if (executeDFProtoInput->optimized()) {
        StatsLib::statAtomicIncr64(QueryManager::get()->stats.countJobsInQueue);
    }
    // Set a flag indicating that the evaluation of the query was attempted
    // and if it errors out (e.g. gets cancelled) or completes the query
    // name remains in libNs so that the user can requestQueryState.  This
    // remains so until a requestQueryDelete is done.
    queryEvalDone = true;
    executeRetinaProtoOutput->set_job_name(queryName, strlen(queryName));

    if (!executeDFProtoInput->is_async()) {
        queryWork->run();
        queryWork->done();

        status = queryJob->status;
        cleanupState = false;
    } else {
        status = Runtime::get()->schedule(queryWork);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to schedule query %s: %s",
                    queryName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (nsPublished) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query graph %s processing request on NS close: "
                    "%s",
                    queryName,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
        if (!queryEvalDone) {
            Status status2 = libNs->remove(nsHandle.nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request on"
                        " NS remove: %s",
                        queryName,
                        strGetFromStatus(status2));
                if (status == StatusOk) {
                    status = status2;
                }
            }
        }
    }

    if (status != StatusOk && cleanupState) {
        if (queryWork != NULL) {
            delete queryWork;
            queryWork = NULL;
        }
        if (queryJob != NULL) {
            if (queryJobInited == true) {
                if (insertHt == true) {
                    queryJobHTLock_.lock();
                    QueryJob *tmpQueryJob = queryJobHashTable_.remove(queryId);
                    queryJobHTLock_.unlock();
                    assert(tmpQueryJob == queryJob);
                    queryJob->decRef();
                }
                queryJob->decRef();
                queryJob = NULL;
            } else {
                delete queryJob;
                queryJob = NULL;
            }
        }
        if (executeDFProtoInput->optimized()) {
            StatsLib::statAtomicDecr64(
                QueryManager::get()->stats.countJobsInQueue);
        }
        // In success case, the decrement happens in
        // QueryManager::QueryWork::done()
        if (trackOpsToSession) {
            Status status2 =
                usrMgr->trackOutstandOps(&workbookSpec,
                                         UserMgr::OutstandOps::Dec);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed query graph %s processing request: %s",
                        queryName,
                        strGetFromStatus(status2));
                // Not a hard error, but this is a leaked Session
                // outstanding operations refcount.
            }
        }
    }

    return status;
}

Status
QueryManager::requestQueryState(XcalarApiOutput **outputOut,
                                size_t *outputSizeOut,
                                const char *queryName,
                                bool detailedStats)
{
    LibNs *libNs = LibNs::get();
    QueryRecord *queryRecord = NULL;
    Status status = StatusOk;
    LibNsTypes::NsHandle nsHandle;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    bool nsHandleValid = false;
    QueryStateMsg queryStateMsg;

    int ret = snprintf(fullyQualName,
                       LibNsTypes::MaxPathNameLen,
                       "%s%s",
                       QueryManager::NsPrefix,
                       queryName);
    if (ret < 0 || ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Failed request query %s state on queryName: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsHandle = libNs->open(fullyQualName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &queryRecord,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusQrQueryNameInvalid;
        } else if (status == StatusAccess) {
            status = StatusQrQueryInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusQrQueryAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusQrQueryNotExist;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed request query %s state on NS open: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;
    queryStateMsg.queryId = nsHandle.nsId;
    queryStateMsg.detailedStats = detailedStats;

    status = requestQueryStateInt(&queryStateMsg,
                                  queryRecord->nodeId_,
                                  outputOut,
                                  outputSizeOut);
    if (status == StatusQrJobNonExist) {
        // query got deleted by another thread,
        // converting status of what user expects in that case.
        status = StatusQrQueryNotExist;
    }

CommonExit:
    if (nsHandleValid) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed request query %s state on NS close: %s",
                    queryName,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
    }
    if (queryRecord != NULL) {
        memFree(queryRecord);
    }

    return status;
}

Status
QueryManager::getQueryState(QueryStateMsg *queryStateMsg,
                            void **outputOut,
                            size_t *outputSizeOut)
{
    Status status = StatusOk;
    XcalarApiQueryStateOutput *queryStateOutput;
    QueryJob *queryJob;
    void *output = NULL;
    XcalarApiDagOutput *listDagOutput = NULL;
    XcalarApis api = XcalarApiUnknown;
    size_t dagSize;
    size_t outputSize = XcalarApiSizeOfOutput(*queryStateOutput);
    uint64_t queued = 0, completed = 0, failed = 0;
    bool queryJobIncremented = false;

    *outputOut = NULL;
    *outputSizeOut = 0;

    // find the queryJob from local hashTable
    queryJobHTLock_.lock();
    queryJob = queryJobHashTable_.find(queryStateMsg->queryId);
    if (queryJob == NULL) {
        status = StatusQrJobNonExist;
        xSyslog(moduleName,
                XlogErr,
                "Failed request query %ld state on query job: %s",
                queryStateMsg->queryId,
                strGetFromStatus(status));
        queryJobHTLock_.unlock();
        goto CommonExit;
    }

    // Protect our access to the queryGraph Dag otherwise it can be deleted
    // via requestQueryDelete.
    queryJob->incRef();
    queryJobIncremented = true;
    queryJobHTLock_.unlock();

    if (queryJob->queryGraph != NULL && queryStateMsg->detailedStats) {
        status = queryJob->queryGraph->listAvailableNodes("*",
                                                          &listDagOutput,
                                                          &dagSize,
                                                          1,
                                                          &api);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed request query %s state on "
                        "listAvailableNodes : "
                        "%s",
                        queryJob->queryName,
                        strGetFromStatus(status));
        outputSize += dagSize;
    } else {
        dagSize = 0;
    }

    outputSize = outputSize + sizeof(size_t);
    output = memAllocExt(outputSize, moduleName);
    BailIfNull(output);

    *((size_t *) output) = outputSize - sizeof(size_t);
    queryStateOutput =
        &((XcalarApiOutput *) ((uintptr_t) output + sizeof(size_t)))
             ->outputResult.queryStateOutput;

    queryJob->queryLock_.lock();
    queryStateOutput->queryState =
        (QueryState) atomicRead32(&queryJob->queryState);
    queryStateOutput->queryStatus = queryJob->status.code();
    if (dagSize > 0) {
        for (uint64_t ii = 0; ii < listDagOutput->numNodes; ii++) {
            if (listDagOutput->node[ii]->hdr.state == DgDagStateReady ||
                listDagOutput->node[ii]->hdr.state == DgDagStateDropped ||
                listDagOutput->node[ii]->hdr.state == DgDagStateCleaned) {
                completed++;
            } else if (listDagOutput->node[ii]->hdr.state == DgDagStateError) {
                failed++;
            } else if (queryJob->workspaceGraph != NULL &&
                       queryJob->currentRunningNodeName != NULL &&
                       strncmp(listDagOutput->node[ii]->hdr.name,
                               queryJob->currentRunningNodeName,
                               sizeof(listDagOutput->node[ii]->hdr.name)) ==
                           0) {
                status = queryJob->workspaceGraph
                             ->getOpProgress(queryJob->currentRunningNodeId,
                                             listDagOutput->node[ii]
                                                 ->localData.numWorkCompleted,
                                             listDagOutput->node[ii]
                                                 ->localData.numWorkTotal);
                if (status == StatusDagNodeNotFound) {
                    // There is a lack of atomicity between queryGraph and
                    // workspaceGraph updates, and also between the call to
                    // listAvailableNodes and getOpProgress.  Especailly
                    // with the frequent drops in SQL DAYG, this can cause
                    // us to attempt lookup of a DAG node that has since
                    // been dropped.  It's safe to ignore this and continue;
                    // we'll pick it up the drop on the next getQueryState
                    // call.
                    // TODO: Fix libdag locking
                    listDagOutput->node[ii]->localData.numWorkCompleted = 0;
                    listDagOutput->node[ii]->localData.numWorkTotal = 0;
                    status = StatusOk;
                }
            }
        }

        queryJob->queryLock_.unlock();
        queued = listDagOutput->numNodes - completed - failed;
        memcpy(&queryStateOutput->queryGraph, listDagOutput, dagSize);
    } else {
        // no dag yet
        queryJob->queryLock_.unlock();
        queryStateOutput->queryGraph.numNodes = 0;
        queryStateOutput->queryGraph.bufSize = 0;
    }
    queryStateOutput->numQueuedWorkItem = queued;
    queryStateOutput->numCompletedWorkItem = completed;
    queryStateOutput->numFailedWorkItem = failed;
    queryStateOutput->elapsed.milliseconds =
        queryJob->stopwatch.getCurElapsedMSecs();
    queryStateOutput->queryNodeId = Config::get()->getMyNodeId();

CommonExit:

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        outputSize = 0;
    }

    if (listDagOutput) {
        memFree(listDagOutput);
    }

    if (queryJobIncremented) {
        queryJob->decRef();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
QueryManager::requestQueryStateInt(QueryStateMsg *queryStateMsg,
                                   NodeId dstNodeId,
                                   XcalarApiOutput **outputOut,
                                   size_t *outputSizeOut)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    QueryStateResponse qsResponse;
    TwoPcHandle twoPcHandle;

    *outputOut = NULL;
    *outputSizeOut = 0;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      (void *) queryStateMsg,
                                      sizeof(*queryStateMsg),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcQueryState1,
                                      &qsResponse,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));
    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcQueryState,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrPlusPayload),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  dstNodeId,
                                  TwoPcClassNested);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed request query %lu state: %s",
                queryStateMsg->queryId,
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status = qsResponse.status;
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed request query %lu state: %s",
                queryStateMsg->queryId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    *outputOut = qsResponse.output;
    *outputSizeOut = qsResponse.outputSize;

CommonExit:
    return status;
}

void
QueryManager::requestQueryStateLocal(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    size_t outputSize = 0;
    QueryStateMsg *queryStateMsg = (QueryStateMsg *) payload;

    // XXX Needed because of hack in Message.cpp to free
    // msg->payload and assign msg->payload to eph->payloadToDistribute.
    eph->payloadToDistribute = NULL;

    status =
        getQueryState(queryStateMsg, &eph->payloadToDistribute, &outputSize);
    BailIfFailed(status);

CommonExit:
    eph->setAckInfo(status, outputSize);
}

Status
QueryManager::requestQueryCancel(const char *queryName)
{
    LibNsTypes::NsHandle nsHandle;
    QueryRecord *queryRecord = NULL;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    Status status = StatusUnknown;
    bool nsHandleValid = false;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsId queryId;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    Status twoPcStatus;

    xSyslog(moduleName, XlogInfo, "Query %s cancel", queryName);

    int ret = snprintf(fullyQualName,
                       LibNsTypes::MaxPathNameLen,
                       "%s%s",
                       QueryManager::NsPrefix,
                       queryName);
    if (ret < 0 || ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s cancel request on queryName: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsHandle = libNs->open(fullyQualName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &queryRecord,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusQrQueryNameInvalid;
        } else if (status == StatusAccess) {
            status = StatusQrQueryInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusQrQueryAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusQrQueryNotExist;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s cancel request on NS open: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;
    queryId = nsHandle.nsId;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      (void *) &queryId,
                                      sizeof(queryId),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcQueryCancel1,
                                      &status,
                                      (TwoPcBufLife)(TwoPcMemCopyInput));

    // Status set directly via the 2PC completion routine
    twoPcStatus =
        MsgMgr::get()->twoPc(&twoPcHandle,
                             MsgTypeId::Msg2pcQueryCancel,
                             TwoPcDoNotReturnHandle,
                             &eph,
                             (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                MsgRecvHdrOnly),
                             TwoPcSyncCmd,
                             TwoPcSingleNode,
                             queryRecord->nodeId_,
                             TwoPcClassNonNested);
    if (twoPcStatus != StatusOk) {
        status = twoPcStatus;
    } else {
        assert(!twoPcHandle.twoPcHandle);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s cancel request: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (nsHandleValid) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query %s cancel request on NS close: %s",
                    queryName,
                    strGetFromStatus(status2));
            if (status != StatusOk) {
                status = status2;
            }
        }
    }

    if (queryRecord != NULL) {
        memFree(queryRecord);
    }

    return status;
}

void
QueryManager::requestQueryCancelLocal(MsgEphemeral *eph, void *payload)
{
    LibNsTypes::NsId queryId;
    QueryJob *queryJob;
    Status status = StatusOk;
    QueryState state;
    bool queryJobIncremented = false;

    queryId = *((LibNsTypes::NsId *) payload);

    queryJobHTLock_.lock();
    queryJob = queryJobHashTable_.find(queryId);
    if (queryJob == NULL) {
        status = StatusQrJobNonExist;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %ld cancel on query job: %s",
                queryId,
                strGetFromStatus(status));
        queryJobHTLock_.unlock();
        goto CommonExit;
    }

    // Protect our access to the queryGraph Dag otherwise it can be deleted
    // via requestQueryDelete.
    queryJob->incRef();
    queryJobIncremented = true;
    queryJobHTLock_.unlock();

    state = (QueryState) atomicRead32(&queryJob->queryState);

    if (state == qrFinished) {
        status = StatusOperationHasFinished;
    } else if (state == qrError) {
        status = StatusOperationInError;
    } else if (state == qrCancelled) {
        status = StatusOperationCancelled;
    } else if (queryJob->markedForCancelling) {
        status = StatusOk;
    } else {
        queryJob->queryLock_.lock();
        // Mark the intension to cancel here. Once QueryWork get's picked up
        // by runtime from the runnable list, it would look at the
        // markedForCancelling flag and begin cancelling the query.
        queryJob->markedForCancelling = true;
        if (queryJob->workspaceGraph != NULL &&
            queryJob->currentRunningNodeName != NULL) {
            status = queryJob->workspaceGraph->cancelOp(
                queryJob->currentRunningNodeName);
            (void) status;
        }
        queryJob->queryLock_.unlock();
        status = StatusOk;
    }

CommonExit:
    if (queryJobIncremented) {
        queryJob->decRef();
    }

    eph->setAckInfo(status, 0);
}

Status
QueryManager::requestQueryDelete(const char *queryName)
{
    QueryRecord *queryRecord = NULL;
    Status status = StatusUnknown;
    LibNs *libNs = LibNs::get();
    bool nsHandleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle nsHandle;
    LibNsTypes::NsId queryId;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    Status twoPcStatus;

    xSyslog(moduleName, XlogInfo, "Query %s delete", queryName);

    int ret = snprintf(fullyQualName,
                       LibNsTypes::MaxPathNameLen,
                       "%s%s",
                       QueryManager::NsPrefix,
                       queryName);
    if (ret < 0 || ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s delete request on queryName: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsHandle = libNs->open(fullyQualName,
                           LibNsTypes::ReaderShared,
                           (NsObject **) &queryRecord,
                           &status);
    if (status != StatusOk) {
        if (status == StatusNsInvalidObjName) {
            status = StatusQrQueryNameInvalid;
        } else if (status == StatusAccess) {
            status = StatusQrQueryInUse;
        } else if (status == StatusPendingRemoval) {
            status = StatusQrQueryAlreadyDeleted;
        } else if (status == StatusNsNotFound) {
            status = StatusQrQueryNotExist;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s delete request on NS open: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsHandleValid = true;
    queryId = nsHandle.nsId;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      (void *) &queryId,
                                      sizeof(queryId),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcQueryDelete1,
                                      &status,
                                      (TwoPcBufLife)(TwoPcMemCopyInput));

    // Status set directly via the 2PC completion routine
    twoPcStatus =
        MsgMgr::get()->twoPc(&twoPcHandle,
                             MsgTypeId::Msg2pcQueryDelete,
                             TwoPcDoNotReturnHandle,
                             &eph,
                             (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                MsgRecvHdrOnly),
                             TwoPcSyncCmd,
                             TwoPcSingleNode,
                             queryRecord->nodeId_,
                             TwoPcClassNonNested);
    if (twoPcStatus != StatusOk) {
        status = twoPcStatus;
    } else {
        assert(!twoPcHandle.twoPcHandle);
    }

    // Delete could fail with JobNonExist as another delete request could
    // have deleted it already
    if (status != StatusOk && status != StatusQrJobNonExist) {
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s delete request: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(nsHandleValid);
    status = libNs->remove(nsHandle.nsId, NULL);
    if (status != StatusOk) {
        if (status == StatusPendingRemoval) {
            status = StatusQrQueryAlreadyDeleted;
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed query %s delete request on NS remove: %s",
                queryName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (nsHandleValid == true) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed query %s delete request on NS close: %s",
                    queryName,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
    }
    if (queryRecord != NULL) {
        memFree(queryRecord);
    }
    return status;
}

void
QueryManager::requestQueryDeleteLocal(MsgEphemeral *eph, void *payload)
{
    LibNsTypes::NsId queryId;
    QueryJob *queryJob;
    Status status = StatusOk;
    bool lockAcquired = false;
    QueryState state;

    queryId = *((LibNsTypes::NsId *) payload);

    queryJobHTLock_.lock();
    lockAcquired = true;

    queryJob = queryJobHashTable_.find(queryId);
    if (queryJob == NULL) {
        status = StatusQrJobNonExist;
        xSyslog(moduleName,
                XlogErr,
                "Failed query %ld delete request on query job: %s",
                queryId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    state = (QueryState) atomicRead32(&queryJob->queryState);
    if (state == qrNotStarted || state == qrProcessing) {
        status = StatusQrJobRunning;
        goto CommonExit;
    }
    queryJob->queryLock_.lock();
    if (queryJob->statsCollectionInProgress) {
        // stats collection is still in progress so we wait to remove hash
        // table entry after stats collection completes
        queryJob->markForDeletion = true;
        status = StatusStatsCollectionInProgress;
        queryJob->queryLock_.unlock();
        goto CommonExit;
    } else {
        queryJob->queryLock_.unlock();
        queryJobHashTable_.remove(queryId);
    }

    queryJobHTLock_.unlock();
    lockAcquired = false;

    queryJob->decRef();

CommonExit:
    if (lockAcquired) {
        queryJobHTLock_.unlock();
    }

    eph->setAckInfo(status, 0);
}

uint32_t
QueryManager::getUniqueUserId()
{
    return (uint32_t)(rand() * Config::get()->getMyNodeId()) +
           atomicDec32(&qpUnique_);
}

void
QueryManager::requestQueryStateCompletion(MsgEphemeral *eph, void *payload)
{
    QueryStateResponse *rqResponse = (QueryStateResponse *) eph->ephemeral;

    rqResponse->status = eph->status;
    if (rqResponse->status != StatusOk) {
        return;
    }

    rqResponse->outputSize = *((size_t *) payload);
    rqResponse->output = (XcalarApiOutput *) memAlloc(rqResponse->outputSize);
    if (rqResponse->output == NULL) {
        rqResponse->status = StatusNoMem;
        return;
    }

    memcpy(rqResponse->output,
           (XcalarApiInput *) ((uintptr_t) payload + sizeof(size_t)),
           rqResponse->outputSize);
}
