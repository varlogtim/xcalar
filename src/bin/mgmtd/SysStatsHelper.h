// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SYS_STATS_HELPER_H
#define _SYS_STATS_HELPER_H

#include "primitives/Macros.h"
#include "primitives/Status.h"
#include "LibApisCommon_constants.h"
#include "LibApisCommon.h"

class SysStatsHelper {
public:
    SysStatsHelper();
    virtual ~SysStatsHelper();

    MustCheck Status initStatsHelper();

    void tearDownStatsHelper();

    MustCheck Status
        getTopResults(const XcalarApiWorkItemT *workItem,
            XcalarApiWorkItemResult *_return,
            const char *destIp, int destPort);

private:

    enum class TopCacherStatIndex : uint32_t {
        numTotalTopCommandsReceived = 0,
        numTopCommandsIssuedToXce = 1,
        TopCacherStatIndexMax = 2,
    };

    static constexpr const char *userIdName_ = "XcMgmtd";
    static constexpr const unsigned int userIdUnique_ = 0xbaada555;

    bool inited_ = false;
    bool workerAwake_ = false;

    // these will be attempted to be allocated and updated
    // during first top api call
    struct timespec cachedTime_;
    size_t topCacheSize_ = 0;
    XcalarApiTopOutput *topResultCached_ = NULL;

    pthread_cond_t workerWaitingCondVar_;
    pthread_mutex_t workerWaitingMutex_;

    MustCheck Status
        updateTopResultCacheIfInvalid(
                    const char *userIdName, unsigned int userIdUnique,
                    const char *destIp, int destPort,
                    int64_t measureIntervalInMs, int64_t cacheValidityInMs);

    MustCheck Status wakeWaiters();

    MustCheck Status
        getTopFromUsrNodesAndUpdateCache(const char *userIdName,
                                        unsigned int userIdUnique,
                                        const char *destIp, int destPort,
                                        uint64_t nSecsBetweenSnapshots);

    MustCheck Status
        populateTopOutput(XcalarApiTopOutput *firstSnapshot,
                    XcalarApiTopOutput *secondSnapshot,
                    uint64_t nSecsBetweenSnapshots,
                    XcalarApiTopOutput **cache,
                    size_t *cacheSize);

    void manipulateCpuUsage(double *cpuUsage);

    void updateCpu(double *cachedCpuValue,
                   double firstSnapshotCpuValue,
                   double secondSnapshotCpuValue,
                   uint64_t numCores,
                   uint64_t nSecsBetweenSnapshots);

    MustCheck Status updateTopCachedTime();

    MustCheck Status getTopResultFromUsrNodes(
            XcalarWorkItem **workItemOut,
            const char *destIp, int destPort, const char *userIdName,
            unsigned int userIdUnique,
            XcalarApiTopRequestType topStatsRequestType,
            uint64_t *timeElapsedToGetResultInNSec,
            XcalarApiTopType topApiType);

    MustCheck Status getCurrentTimeInNSec(uint64_t *currentTimeInNSec);

    MustCheck Status getTopResultsFromConnectedNode(
                        const XcalarApiWorkItemT *workItem,
                        XcalarApiWorkItemResult *_return,
                        const char *destIp, int destPort);

    MustCheck Status getTopResultsFromAllNodes(
                            const XcalarApiWorkItemT *workItem,
                            XcalarApiWorkItemResult *_return,
                            const char *destIp, int destPort);

    MustCheck Status getLocalNodeTopOutput(
                        const char *userIdName, unsigned int userIdUnique,
                        const char *destIp, int destPort,
                        uint64_t nSecsBetweenSnapshots,
                        XcalarApiTopOutput **perNodeTopOutput,
                        size_t *perNodeTopOutputSize);

    NodeId localNodeId_;

    // this will contain when did this module ship stats to XEM
    struct timespec lastStatsShipTime_;

    XcalarConnectedSocketInfo *sendingSocketInfo_ = NULL;

    bool tearingDown_ = false;

    MustCheck Status getNumNodes(const char *userIdName,
                    const unsigned int userIdUnique,
                    const char *destIp, int destPort,
                    unsigned *numNodes);

    unsigned numNodesInXdpCluster_ = -1;
    MustCheck Status updateNumNodesIf();

    static constexpr const char *sysPerformanceStatsGroupName =
                            "XcalarSystemPerformanceStats";
};

#endif // _SYS_STATS_HELPER_H
