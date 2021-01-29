// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "SysStatsHelper.h"
#include "libapis/LibApisSend.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "xccompress/XcSnappy.h"
#include "strings/String.h"

static const char *moduleName = "SysStatsHelper";

static constexpr const char *topApiMemberNames[] = {
    "nodeId",
    "cpuUsageInPercent",
    "memUsageInPercent",
    "memUsedInBytes",
    "totalAvailableMemInBytes",
    "networkRecvInBytesPerSec",
    "networkSendInBytesPerSec",
    "xdbUsedBytes",
    "xdbTotalBytes",
    "parentCpuUsageInPercent",
    "childrenCpuUsageInPercent",
    "numCores",
    "sysSwapUsedInBytes",
    "sysSwapTotalInBytes",
    "uptimeInSeconds",
    "datasetUsedBytes",
    "sysMemUsedInBytes",
    "publishedTableUsedBytes",
};

SysStatsHelper::SysStatsHelper()
{
    workerAwake_ = false;
    inited_ = false;
    topCacheSize_ = 0;
    topResultCached_ = NULL;
    sendingSocketInfo_ = NULL;
    tearingDown_ = false;
    numNodesInXdpCluster_ = 0;
}

SysStatsHelper::~SysStatsHelper()
{
    assert(inited_ == false);
    assert(sendingSocketInfo_ == NULL);
    assert(topResultCached_ == NULL);
}

void
SysStatsHelper::tearDownStatsHelper()
{
    if (inited_ == false) {
        // nothing to do
        return;
    }

    assert(tearingDown_ == false);
    tearingDown_ = true;

    sendingSocketInfo_->tearDownSocketInfo();
    delete sendingSocketInfo_;
    sendingSocketInfo_ = NULL;

    if (topResultCached_ != NULL) {
        memFree(topResultCached_);
        topResultCached_ = NULL;
    }

    // ideally we would like to have a remove lock here before
    // destroying the below members in case if there is an
    // in flight request. However the consumer of this only
    // mgmt daemon and he can get here only during shutdown
    // before which he "ensures (I think)" that all api handle
    // threads have joined back

    verify(pthread_cond_destroy(&workerWaitingCondVar_) == 0);
    verify(pthread_mutex_destroy(&workerWaitingMutex_) == 0);

    inited_ = false;
}

Status
SysStatsHelper::initStatsHelper()
{
    Status status = StatusOk;
    int ret;
    bool mutexInited = false;
    bool condVarInited = false;

    assert(inited_ == false);

    topResultCached_ = NULL;

    localNodeId_ = Config::get()->getLowestNodeIdOnThisHost();

    // @SymbolCheckIgnore
    ret = pthread_mutex_init(&workerWaitingMutex_, NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    mutexInited = true;

    // @SymbolCheckIgnore
    ret = pthread_cond_init(&workerWaitingCondVar_, NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    condVarInited = true;

    // this is just initing cachedTime_ so that it doesn't have garbage
    ret = clock_gettime(CLOCK_REALTIME, &cachedTime_);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // init lastShipTime for stats sender thread to know if it should
    // ship stats to XEM based on timeout specified in config
    ret = clock_gettime(CLOCK_REALTIME, &lastStatsShipTime_);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    sendingSocketInfo_ = new (std::nothrow) XcalarConnectedSocketInfo;
    BailIfNull(sendingSocketInfo_);

    inited_ = true;

CommonExit:

    if (status != StatusOk) {
        assert(inited_ == false);
        if (condVarInited == true) {
            pthread_cond_destroy(&workerWaitingCondVar_);
        }

        if (mutexInited == true) {
            pthread_mutex_destroy(&workerWaitingMutex_);
        }

        if (sendingSocketInfo_ != NULL) {
            sendingSocketInfo_->tearDownSocketInfo();
            delete sendingSocketInfo_;
            sendingSocketInfo_ = NULL;
        }

        xSyslog(moduleName,
                XlogErr,
                "%s() failed since %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

Status
SysStatsHelper::updateTopCachedTime()
{
    Status status = StatusOk;
    int ret;

    pthread_mutex_lock(&workerWaitingMutex_);

    ret = clock_gettime(CLOCK_REALTIME, &cachedTime_);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
    }

    pthread_mutex_unlock(&workerWaitingMutex_);

    return status;
}

Status
SysStatsHelper::updateTopResultCacheIfInvalid(const char *userIdName,
                                              unsigned int userIdUnique,
                                              const char *destIp,
                                              int destPort,
                                              int64_t measureIntervalInMs,
                                              int64_t cacheValidityInMs)
{
    Status status = StatusOk;
    bool lockHeld = false;
    bool isThisThreadWorker = false;
    int ret;
    struct timespec currentTime;

    uint64_t nSecsBetweenSnapshots =
        xcMin(measureIntervalInMs, ((int64_t) XcalarApiMaxTopIntervalInMs)) *
        NSecsPerMSec;

    // if XD passes 0 Ms, we serve Top results from cache in the same second
    int64_t cacheValidityInSeconds =
        xcMin(cacheValidityInMs, ((int64_t) XcalarApiMaxCacheValidityInMs)) /
        MSecsPerSec;

    verify(pthread_mutex_lock(&workerWaitingMutex_) == 0);
    lockHeld = true;

    ret = clock_gettime(CLOCK_REALTIME, &currentTime);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if ((currentTime.tv_sec - cachedTime_.tv_sec) <=
        (time_t)(cacheValidityInSeconds)) {
        // cache valid
        if (topResultCached_ != NULL) {
            goto CommonExit;
        }
        // if the first top call came in within the number of
        // seconds specified above and this object was created
        // in which case current thread (or some other thread)
        // who already was here) will initialize the cache
    }

    if (workerAwake_ == false) {
        // the thread which gets here is held hostage
        // to get top cache from usrnode[s]
        workerAwake_ = isThisThreadWorker = true;
        verify(pthread_mutex_unlock(&workerWaitingMutex_) == 0);
        lockHeld = false;

        status = getTopFromUsrNodesAndUpdateCache(userIdName,
                                                  userIdUnique,
                                                  destIp,
                                                  destPort,
                                                  nSecsBetweenSnapshots);
        if (status != StatusOk) {
            goto CommonExit;
        }

        status = updateTopCachedTime();
        if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        status = sysCondTimedWait(&workerWaitingCondVar_,
                                  &workerWaitingMutex_,
                                  (60 * USecsPerSec));
        if (status != StatusOk) {
            goto CommonExit;
        }

        ret = clock_gettime(CLOCK_REALTIME, &currentTime);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }

        if ((currentTime.tv_sec - cachedTime_.tv_sec) >
            (time_t)(cacheValidityInSeconds)) {
            // we get here if the worker tried to update cache but he failed
            // or he is still working on it. Lets not retry sleeping
            status = StatusAgain;
            goto CommonExit;
        }
    }

CommonExit:

    if (isThisThreadWorker == true) {
        assert(lockHeld == false);
        if (wakeWaiters() != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "%s() unable to wake waiters",
                    __func__);
        }
    }

    if (lockHeld == true) {
        verify(pthread_mutex_unlock(&workerWaitingMutex_) == 0);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s() failed since %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

Status
SysStatsHelper::wakeWaiters()
{
    Status status = StatusOk;
    int ret;

    verify(pthread_mutex_lock(&workerWaitingMutex_) == 0);
    ret = pthread_cond_broadcast(&workerWaitingCondVar_);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
    }
    workerAwake_ = false;
    verify(pthread_mutex_unlock(&workerWaitingMutex_) == 0);

    return status;
}

/*
 * Mgmtd gets two snapshots of total number of jiffies of child node[s]
 * There can be state change between these two snapshots or even
 * while usrnode populates the total, reading from each child node's
 * /proc/<pid>/stat file
 *
 * Because of these issues, child node CPU usage can give stupid values
 * and confuse someone who is looking at the XD while child nodes are
 * born and killed for several operations. Hence we need to manipulate
 * these so that the values can be "meaningful"
 */
void
SysStatsHelper::manipulateCpuUsage(double *cpuUsage)
{
    if (*cpuUsage < (double) 0) {
        // child nodes disappeared between the calls, so return 0
        *cpuUsage = 0;
    }

    if (*cpuUsage > (double) 100) {
        *cpuUsage = (double) 100;
    }
}

void
SysStatsHelper::updateCpu(double *cachedCpuValue,
                          double firstSnapshotCpuValue,
                          double secondSnapshotCpuValue,
                          uint64_t numCores,
                          uint64_t nSecsBetweenSnapshots)
{
    double cpuPercent =
        (double) ((((secondSnapshotCpuValue - firstSnapshotCpuValue) * 100) /
                   (nSecsBetweenSnapshots / NSecsPerUSec)) /
                  numCores);

    manipulateCpuUsage(&cpuPercent);

    *cachedCpuValue = cpuPercent;
}

Status
SysStatsHelper::populateTopOutput(XcalarApiTopOutput *firstSnapshot,
                                  XcalarApiTopOutput *secondSnapshot,
                                  uint64_t nSecsBetweenSnapshots,
                                  XcalarApiTopOutput **cache,
                                  size_t *cacheSize)
{
    Status status = StatusOk;
    XcalarApiTopOutputPerNode *cacheCursor = NULL;
    XcalarApiTopOutputPerNode *firstSnapshotCursor = NULL;
    XcalarApiTopOutputPerNode *secondSnapshotCursor = NULL;

    assert(cache != NULL);
    assert(cacheSize != NULL);
    if (*cache == NULL) {
        assert(*cacheSize == 0);
        // only one thread can be here at any point hence no lock needed
        assert(firstSnapshot->numNodes == secondSnapshot->numNodes);

        assert(firstSnapshot->numNodes > 0);
        *cacheSize =
            (sizeof(XcalarApiTopOutput) +
             (sizeof(XcalarApiTopOutputPerNode) * firstSnapshot->numNodes));
        *cache = (XcalarApiTopOutput *) memAllocExt(*cacheSize, moduleName);
        if (*cache == NULL) {
            status = StatusNoMem;
            *cacheSize = 0;
            goto CommonExit;
        }
        (*cache)->numNodes = firstSnapshot->numNodes;
    }

    (*cache)->status = StatusOk.code();

    assert(firstSnapshot->numNodes == secondSnapshot->numNodes);
    assert((*cache)->numNodes == firstSnapshot->numNodes);

    for (unsigned ii = 0; ii < firstSnapshot->numNodes; ii++) {
        cacheCursor = &((*cache)->topOutputPerNode[ii]);
        firstSnapshotCursor = &(firstSnapshot->topOutputPerNode[ii]);
        secondSnapshotCursor = &(secondSnapshot->topOutputPerNode[ii]);

        assert(firstSnapshotCursor->nodeId == secondSnapshotCursor->nodeId);
        cacheCursor->nodeId = firstSnapshotCursor->nodeId;

        // parent and child CPU usage will be in uSeconds
        updateCpu(&cacheCursor->parentCpuUsageInPercent,
                  firstSnapshotCursor->parentCpuUsageInPercent,
                  secondSnapshotCursor->parentCpuUsageInPercent,
                  secondSnapshotCursor->numCores,
                  nSecsBetweenSnapshots);

        updateCpu(&cacheCursor->childrenCpuUsageInPercent,
                  firstSnapshotCursor->childrenCpuUsageInPercent,
                  secondSnapshotCursor->childrenCpuUsageInPercent,
                  secondSnapshotCursor->numCores,
                  nSecsBetweenSnapshots);

        cacheCursor->cpuUsageInPercent =
            (cacheCursor->parentCpuUsageInPercent +
             cacheCursor->childrenCpuUsageInPercent);

        // ensure this does not go above 100 percent
        manipulateCpuUsage(&cacheCursor->cpuUsageInPercent);

        cacheCursor->memUsageInPercent =
            secondSnapshotCursor->memUsageInPercent;

        cacheCursor->memUsedInBytes = secondSnapshotCursor->memUsedInBytes;

        cacheCursor->totalAvailableMemInBytes =
            secondSnapshotCursor->totalAvailableMemInBytes;

        cacheCursor->networkRecvInBytesPerSec =
            (((secondSnapshotCursor->networkRecvInBytesPerSec -
               firstSnapshotCursor->networkRecvInBytesPerSec) *
              NSecsPerSec) /
             nSecsBetweenSnapshots);

        cacheCursor->networkSendInBytesPerSec =
            (((secondSnapshotCursor->networkSendInBytesPerSec -
               firstSnapshotCursor->networkSendInBytesPerSec) *
              NSecsPerSec) /
             nSecsBetweenSnapshots);

        cacheCursor->xdbUsedBytes = secondSnapshotCursor->xdbUsedBytes;

        cacheCursor->xdbTotalBytes = secondSnapshotCursor->xdbTotalBytes;

        cacheCursor->numCores = secondSnapshotCursor->numCores;

        cacheCursor->sysSwapUsedInBytes =
            secondSnapshotCursor->sysSwapUsedInBytes;

        cacheCursor->sysSwapTotalInBytes =
            secondSnapshotCursor->sysSwapTotalInBytes;

        cacheCursor->uptimeInSeconds = secondSnapshotCursor->uptimeInSeconds;

        cacheCursor->datasetUsedBytes = secondSnapshotCursor->datasetUsedBytes;
        cacheCursor->publishedTableUsedBytes =
            secondSnapshotCursor->publishedTableUsedBytes;

        cacheCursor->sysMemUsedInBytes =
            secondSnapshotCursor->sysMemUsedInBytes;
    }

CommonExit:

    return status;
}

Status
SysStatsHelper::getCurrentTimeInNSec(uint64_t *currentTimeInNSec)
{
    Status status = StatusOk;
    struct timespec currentTime;
    int ret;

    assert(currentTimeInNSec != NULL);
    *currentTimeInNSec = 0;

    ret = clock_gettime(CLOCK_MONOTONIC_RAW, &currentTime);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    *currentTimeInNSec =
        ((currentTime.tv_sec * NSecsPerSec) + currentTime.tv_nsec);

CommonExit:

    xcAssertIf((status != StatusOk), (*currentTimeInNSec == 0));

    return status;
}

Status
SysStatsHelper::getTopResultFromUsrNodes(
    XcalarWorkItem **workItemOut,
    const char *destIp,
    int destPort,
    const char *userIdName,
    unsigned int userIdUnique,
    XcalarApiTopRequestType topStatsRequestType,
    uint64_t *timeElapsedToGetResultInNSec,
    XcalarApiTopType topApiType)
{
    Status status = StatusOk;
    XcalarWorkItem *topWorkItem = NULL;
    uint64_t preTimeInNSec = 0;
    uint64_t postTimeInNSec = 0;

    assert(workItemOut != NULL);
    assert(*workItemOut == NULL);
    assert(destIp != NULL);
    assert(userIdName != NULL);
    assert(timeElapsedToGetResultInNSec);
    *timeElapsedToGetResultInNSec = 0;

    topWorkItem = xcalarApiMakeTopWorkItem(topStatsRequestType, topApiType);
    if (topWorkItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = getCurrentTimeInNSec(&preTimeInNSec);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = xcalarApiQueueWork(topWorkItem,
                                destIp,
                                destPort,
                                userIdName,
                                userIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = getCurrentTimeInNSec(&postTimeInNSec);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(topWorkItem->status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (!(topWorkItem->outputSize > 0)) {
        // IMO it is not possible to get here, however handling
        // it in run time since this is most used code path
        // XXX: change this code to something meaningful
        status = StatusNoMem;
        goto CommonExit;
    }

    status.fromStatusCode(topWorkItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(topWorkItem->output->outputResult.topOutput.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *workItemOut = topWorkItem;
    *timeElapsedToGetResultInNSec = (postTimeInNSec - preTimeInNSec);

CommonExit:

    xcAssertIf((status != StatusOk), (*timeElapsedToGetResultInNSec == 0));
    xcAssertIf((status != StatusOk), (*workItemOut == NULL));
    if (status != StatusOk) {
        if (topWorkItem != NULL) {
            xcalarApiFreeWorkItem(topWorkItem);
        }
    }

    return status;
}

Status
SysStatsHelper::getTopFromUsrNodesAndUpdateCache(const char *userIdName,
                                                 unsigned int userIdUnique,
                                                 const char *destIp,
                                                 int destPort,
                                                 uint64_t nSecsBetweenSnapshots)
{
    Status status = StatusOk;

    XcalarWorkItem *workItemFirstSnapshot = NULL;
    XcalarWorkItem *workItemSecondSnapshot = NULL;

    uint64_t timeInNSecFirstSnapshot = 0;
    uint64_t timeInNSecSecondSnapshot = 0;

    status = getTopResultFromUsrNodes(&workItemFirstSnapshot,
                                      destIp,
                                      destPort,
                                      userIdName,
                                      userIdUnique,
                                      GetCpuAndNetworkTopStats,
                                      &timeInNSecFirstSnapshot,
                                      XcalarApiTopAllNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    sysNSleep(nSecsBetweenSnapshots);

    status = getTopResultFromUsrNodes(&workItemSecondSnapshot,
                                      destIp,
                                      destPort,
                                      userIdName,
                                      userIdUnique,
                                      GetAllTopStats,
                                      &timeInNSecSecondSnapshot,
                                      XcalarApiTopAllNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = populateTopOutput(&(workItemFirstSnapshot->output->outputResult
                                     .topOutput),
                               &(workItemSecondSnapshot->output->outputResult
                                     .topOutput),
                               (nSecsBetweenSnapshots +
                                (timeInNSecFirstSnapshot / 2) +
                                (timeInNSecSecondSnapshot / 2)),
                               &topResultCached_,
                               &topCacheSize_);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:

    if (workItemFirstSnapshot != NULL) {
        xcalarApiFreeWorkItem(workItemFirstSnapshot);
    }

    if (workItemSecondSnapshot != NULL) {
        xcalarApiFreeWorkItem(workItemSecondSnapshot);
    }

    return status;
}

Status
SysStatsHelper::getTopResults(const XcalarApiWorkItemT *workItem,
                              XcalarApiWorkItemResult *_return,
                              const char *destIp,
                              int destPort)
{
    if (workItem->api == XcalarApisT::XcalarApiTop) {
        return getTopResultsFromAllNodes(workItem, _return, destIp, destPort);
    }

    assert(workItem->api == XcalarApisT::XcalarApiPerNodeTop);
    return getTopResultsFromConnectedNode(workItem, _return, destIp, destPort);
}

// XXXTODO: dedupe this with getTopFromUsrNodesAndUpdateCache()
Status
SysStatsHelper::getLocalNodeTopOutput(const char *userIdName,
                                      unsigned int userIdUnique,
                                      const char *destIp,
                                      int destPort,
                                      uint64_t nSecsBetweenSnapshots,
                                      XcalarApiTopOutput **perNodeTopOutput,
                                      size_t *perNodeTopOutputSize)
{
    Status status = StatusOk;

    XcalarWorkItem *workItemFirstSnapshot = NULL;
    XcalarWorkItem *workItemSecondSnapshot = NULL;

    uint64_t timeInNSecFirstSnapshot = 0;
    uint64_t timeInNSecSecondSnapshot = 0;

    assert(perNodeTopOutput != NULL);
    assert(perNodeTopOutputSize != NULL);

    status = getTopResultFromUsrNodes(&workItemFirstSnapshot,
                                      destIp,
                                      destPort,
                                      userIdName,
                                      userIdUnique,
                                      GetCpuAndNetworkTopStats,
                                      &timeInNSecFirstSnapshot,
                                      XcalarApiTopLocalNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    sysNSleep(nSecsBetweenSnapshots);

    status = getTopResultFromUsrNodes(&workItemSecondSnapshot,
                                      destIp,
                                      destPort,
                                      userIdName,
                                      userIdUnique,
                                      GetAllTopStats,
                                      &timeInNSecSecondSnapshot,
                                      XcalarApiTopLocalNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = populateTopOutput(&(workItemFirstSnapshot->output->outputResult
                                     .topOutput),
                               &(workItemSecondSnapshot->output->outputResult
                                     .topOutput),
                               (nSecsBetweenSnapshots +
                                (timeInNSecFirstSnapshot / 2) +
                                (timeInNSecSecondSnapshot / 2)),
                               perNodeTopOutput,
                               perNodeTopOutputSize);
    BailIfFailed(status);

CommonExit:

    if (workItemFirstSnapshot != NULL) {
        xcalarApiFreeWorkItem(workItemFirstSnapshot);
    }

    if (workItemSecondSnapshot != NULL) {
        xcalarApiFreeWorkItem(workItemSecondSnapshot);
    }

    return status;
}

// FIXME: during writing api::local node top tests
// populate this from cache if valid
Status
SysStatsHelper::getTopResultsFromConnectedNode(
    const XcalarApiWorkItemT *workItem,
    XcalarApiWorkItemResult *_return,
    const char *destIp,
    int destPort)
{
    Status status = StatusOk;

    const char *userIdName = NULL;
    unsigned int userIdUnique = 0;

    XcalarApiTopOutput *perNodeTopOutput = NULL;
    size_t perNodeTopOutputSize = 0;

    XcalarApiTopOutputPerNode *topOutputPerNodeSrc = NULL;

    XcalarApiTopOutputPerNodeT topOutputPerNodeDst;

    assert(destIp != NULL);

    userIdName = workItem->userId.c_str();
    userIdUnique = (unsigned int) workItem->userIdUnique;

    status = getLocalNodeTopOutput(userIdName,
                                   userIdUnique,
                                   destIp,
                                   destPort,
                                   workItem->input.topInput.measureIntervalInMs,
                                   &perNodeTopOutput,
                                   &perNodeTopOutputSize);
    BailIfFailed(status);

    topOutputPerNodeSrc = &(perNodeTopOutput->topOutputPerNode[0]);

    _return->output.outputResult.topOutput.status =
        (StatusT::type) status.code();

    topOutputPerNodeDst.nodeId = topOutputPerNodeSrc->nodeId;
    topOutputPerNodeDst.cpuUsageInPercent =
        topOutputPerNodeSrc->cpuUsageInPercent;

    topOutputPerNodeDst.memUsageInPercent =
        topOutputPerNodeSrc->memUsageInPercent;

    topOutputPerNodeDst.memUsedInBytes = topOutputPerNodeSrc->memUsedInBytes;

    topOutputPerNodeDst.totalAvailableMemInBytes =
        topOutputPerNodeSrc->totalAvailableMemInBytes;

    topOutputPerNodeDst.networkRecvInBytesPerSec =
        topOutputPerNodeSrc->networkRecvInBytesPerSec;

    topOutputPerNodeDst.networkSendInBytesPerSec =
        topOutputPerNodeSrc->networkSendInBytesPerSec;

    topOutputPerNodeDst.xdbUsedBytes = topOutputPerNodeSrc->xdbUsedBytes;

    topOutputPerNodeDst.xdbTotalBytes = topOutputPerNodeSrc->xdbTotalBytes;

    topOutputPerNodeDst.parentCpuUsageInPercent =
        topOutputPerNodeSrc->parentCpuUsageInPercent;

    topOutputPerNodeDst.childrenCpuUsageInPercent =
        topOutputPerNodeSrc->childrenCpuUsageInPercent;

    topOutputPerNodeDst.numCores = topOutputPerNodeSrc->numCores;

    topOutputPerNodeDst.sysSwapUsedInBytes =
        topOutputPerNodeSrc->sysSwapUsedInBytes;

    topOutputPerNodeDst.sysSwapTotalInBytes =
        topOutputPerNodeSrc->sysSwapTotalInBytes;

    topOutputPerNodeDst.uptimeInSeconds = topOutputPerNodeSrc->uptimeInSeconds;

    _return->output.outputResult.topOutput.topOutputPerNode.push_back(
        topOutputPerNodeDst);
    _return->output.outputResult.__isset.topOutput = true;
CommonExit:

    if (perNodeTopOutput != NULL) {
        memFree(perNodeTopOutput);
    }

    return status;
}

Status
SysStatsHelper::getTopResultsFromAllNodes(const XcalarApiWorkItemT *workItem,
                                          XcalarApiWorkItemResult *_return,
                                          const char *destIp,
                                          int destPort)
{
    Status status = StatusOk;
    XcalarApiTopOutputPerNode *topOutputPerNodeSrc = NULL;
    XcalarApiTopOutputPerNodeT topOutputPerNodeDst;

    const char *userIdName = NULL;
    unsigned int userIdUnique = 0;

    assert(destIp != NULL);

    userIdName = workItem->userId.c_str();
    userIdUnique = (unsigned int) workItem->userIdUnique;

    status = updateTopResultCacheIfInvalid(userIdName,
                                           userIdUnique,
                                           destIp,
                                           destPort,
                                           workItem->input.topInput
                                               .measureIntervalInMs,
                                           workItem->input.topInput
                                               .cacheValidityInMs);
    if (status != StatusOk) {
        _return->output.outputResult.topOutput.status =
            (StatusT::type) status.code();
        return status;
    }

    if (topResultCached_ == NULL) {
        _return->output.outputResult.topOutput.status =
            (StatusT::type) StatusNoMem.code();
        return StatusNoMem;
    }

    status.fromStatusCode(topResultCached_->status);
    _return->output.outputResult.topOutput.status =
        (StatusT::type) status.code();
    _return->output.outputResult.topOutput.numNodes =
        topResultCached_->numNodes;

    for (unsigned ii = 0; ii < topResultCached_->numNodes; ii++) {
        topOutputPerNodeSrc = &(topResultCached_->topOutputPerNode[ii]);
        topOutputPerNodeDst.nodeId = topOutputPerNodeSrc->nodeId;
        topOutputPerNodeDst.cpuUsageInPercent =
            (topOutputPerNodeSrc->cpuUsageInPercent);
        topOutputPerNodeDst.memUsageInPercent =
            topOutputPerNodeSrc->memUsageInPercent;
        topOutputPerNodeDst.memUsedInBytes =
            topOutputPerNodeSrc->memUsedInBytes;
        topOutputPerNodeDst.totalAvailableMemInBytes =
            topOutputPerNodeSrc->totalAvailableMemInBytes;
        topOutputPerNodeDst.networkRecvInBytesPerSec =
            topOutputPerNodeSrc->networkRecvInBytesPerSec;
        topOutputPerNodeDst.networkSendInBytesPerSec =
            topOutputPerNodeSrc->networkSendInBytesPerSec;
        topOutputPerNodeDst.xdbUsedBytes = topOutputPerNodeSrc->xdbUsedBytes;
        topOutputPerNodeDst.xdbTotalBytes = topOutputPerNodeSrc->xdbTotalBytes;
        topOutputPerNodeDst.parentCpuUsageInPercent =
            topOutputPerNodeSrc->parentCpuUsageInPercent;
        topOutputPerNodeDst.childrenCpuUsageInPercent =
            topOutputPerNodeSrc->childrenCpuUsageInPercent;
        topOutputPerNodeDst.numCores = topOutputPerNodeSrc->numCores;
        topOutputPerNodeDst.sysSwapUsedInBytes =
            topOutputPerNodeSrc->sysSwapUsedInBytes;

        topOutputPerNodeDst.sysSwapTotalInBytes =
            topOutputPerNodeSrc->sysSwapTotalInBytes;

        topOutputPerNodeDst.uptimeInSeconds =
            topOutputPerNodeSrc->uptimeInSeconds;

        topOutputPerNodeDst.datasetUsedBytes =
            topOutputPerNodeSrc->datasetUsedBytes;

        topOutputPerNodeDst.publishedTableUsedBytes =
            topOutputPerNodeSrc->publishedTableUsedBytes;

        topOutputPerNodeDst.sysMemUsedInBytes =
            topOutputPerNodeSrc->sysMemUsedInBytes;

        _return->output.outputResult.topOutput.topOutputPerNode.push_back(
            topOutputPerNodeDst);
    }

    _return->output.outputResult.__isset.topOutput = true;

    return status;
}

Status
SysStatsHelper::getNumNodes(const char *userIdName,
                            const unsigned int userIdUnique,
                            const char *destIp,
                            int destPort,
                            unsigned *numNodes)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem;

    assert(numNodes != NULL);
    *numNodes = 0;

    workItem = xcalarApiMakeGetNumNodesWorkItem();
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Unable to allocate workItem for (%s) failed: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                destIp,
                                destPort,
                                userIdName,
                                userIdUnique);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "queueWork(%s) failed: %s",
                strGetFromXcalarApis(workItem->api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status.fromStatusCode(workItem->status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->outputSize ==
           XcalarApiSizeOfOutput(XcalarApiGetNumNodesOutput));
    *numNodes =
        (unsigned) workItem->output->outputResult.getNumNodesOutput.numNodes;
CommonExit:

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s() failed since %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

// we need to update this once XDP cluster comes up since
// when mgmtd comes up there is no guarantee that XDP cluster is up
Status
SysStatsHelper::updateNumNodesIf()
{
    Status status = StatusOk;
    Config *config = Config::get();

    if (numNodesInXdpCluster_ > 0) {
        // already fetched, nothing  more to do:
        assert(status == StatusOk);
        goto CommonExit;
    }

    status = getNumNodes(userIdName_,
                         userIdUnique_,
                         config->getIpAddr(localNodeId_),
                         config->getApiPort(localNodeId_),
                         &numNodesInXdpCluster_);
    BailIfFailed(status);

    assert(numNodesInXdpCluster_ > 0);

CommonExit:

    return status;
}
