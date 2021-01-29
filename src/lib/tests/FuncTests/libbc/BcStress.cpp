// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <getopt.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <libgen.h>

#include "primitives/Primitives.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "bc/BufferCache.h"
#include "bcquota/BufferCacheQuota.h"
#include "stat/Statistics.h"
#include "usrnode/UsrNode.h"
#include "sys/Socket.h"
#include "libapis/LibApisRecv.h"
#include "dataset/Dataset.h"
#include "table/ResultSet.h"
#include "util/Math.h"
#include "util/WorkQueue.h"
#include "util/Lookaside.h"
#include "util/Random.h"
#include "util/MemTrack.h"
#include "bc/BufCacheObjectMgr.h"
#include "runtime/Runtime.h"
#include "sys/XLog.h"
#include "LibBcFuncTestConfig.h"
#include "stat/Statistics.h"
#include "util/System.h"

#include "test/QA.h"

static uint64_t bufferCacheElementsInBcStress = 16384;
static uint64_t loopIterations = 32;
static bool useSleep = false;
static bool delayInUs = 10;
static bool bcTest = true;
static bool lookasideTest = true;
static bool mallocTest = true;
static bool bcQuotaTest = true;
static uint64_t elementSize = 256;
static uint64_t bcStressThreads = 8;

static pthread_t *bcStressThreadIds;
static void *bcStress(void *unused);
static void *laStress(void *unused);
static Status doBcWork();
static Status doBcQuotaWork();
static Status doLaWork();
static Status doMallocWork();
static const char *moduleName = "bcstress";

static BcQuota *bcQuotaGlobal;
static constexpr uint64_t BcQuotaObjectCount = 2;
static uint64_t bcQuotaObject[BcQuotaObjectCount];
static BcHandle *bcGlobal;
static BcObject *bcObjectGlobal;
static LookasideList bufLookaside;

static StatGroupId groupId;
static StatHandle bcTestCurIter;
static StatHandle bcQuotaTestCurIter;
static StatHandle mallocTestCurIter;
static StatHandle lookasideTestCurIter;

Status
bcStressStatsInit()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(moduleName, &groupId, 4);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&bcTestCurIter);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId,
                                         "bcStress.bcTestCurIter",
                                         bcTestCurIter,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&bcQuotaTestCurIter);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId,
                                         "bcStress.bcQuotaTestCurIter",
                                         bcQuotaTestCurIter,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&mallocTestCurIter);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId,
                                         "bcStress.mallocTestCurIter",
                                         mallocTestCurIter,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&lookasideTestCurIter);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId,
                                         "bcStress.lookasideTestCurIter",
                                         lookasideTestCurIter,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}

static Status
doBcTest()
{
    Status status = StatusOk;
    struct timeval tv1, tv2;
    StatsLib *statsLib = StatsLib::get();

    xSyslog(moduleName, XlogInfo, "bcStress::Starting buf$ stress test");

    bcObjectGlobal = (BcObject *) memAllocExt(sizeof(BcObject), moduleName);
    if (bcObjectGlobal == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "doBcTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = bcObjectGlobal->setUp("bc.bcStress",
                                   bufferCacheElementsInBcStress,
                                   1,
                                   elementSize,
                                   BcObject::InitFastAllocsOnly,
                                   BufferCacheObjects::ObjectIdUnknown);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doBcTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    statsLib->removeGroupIdFromHashTable("bc.bcStress");
    bcGlobal = BcHandle::create(bcObjectGlobal);
    if (bcGlobal == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "doBcTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    status = doBcWork();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doBcTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);

    xSyslog(moduleName,
            XlogInfo,
            "bcStress::Done buf$ stress test, Elapsed time = %f seconds",
            (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                (double) (tv2.tv_sec - tv1.tv_sec));

CommonExit:
    if (bcGlobal != NULL) {
        BcHandle::destroy(&bcGlobal);
        bcGlobal = NULL;
    }

    if (bcObjectGlobal != NULL) {
        bcObjectGlobal->tearDown();
        memFree(bcObjectGlobal);
        bcObjectGlobal = NULL;
    }
    return status;
}

static Status
doBcQuotaTest()
{
    Status status = StatusOk;
    struct timeval tv1, tv2;
    StatsLib *statsLib = StatsLib::get();

    xSyslog(moduleName,
            XlogInfo,
            "doBcQuotaTest::Starting buf$ quota stress test");

    bcObjectGlobal = (BcObject *) memAllocExt(sizeof(BcObject), moduleName);
    if (bcObjectGlobal == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "doBcQuotaTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = bcObjectGlobal->setUp("bc.bcStress",
                                   bufferCacheElementsInBcStress,
                                   1,
                                   elementSize,
                                   BcObject::InitFastAllocsOnly,
                                   BufferCacheObjects::ObjectIdUnknown);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doBcQuotaTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    statsLib->removeGroupIdFromHashTable("bc.bcStress");
    bcGlobal = BcHandle::create(bcObjectGlobal);
    if (bcGlobal == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "doBcQuotaTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    uint64_t ii;
    for (ii = 0; ii < BcQuotaObjectCount; ii++) {
        bcQuotaObject[ii] = ii + 1;
    }
    bcQuotaGlobal =
        BcQuota::quotaCreate(bcGlobal, bcQuotaObject, BcQuotaObjectCount);
    if (bcQuotaGlobal == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "doBcQuotaTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    status = doBcQuotaWork();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doBcQuotaTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);

    xSyslog(moduleName,
            XlogInfo,
            "doBcQuotaTest::Buf$ quota test done, Elapsed time = %f seconds",
            (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                (double) (tv2.tv_sec - tv1.tv_sec));

CommonExit:

    if (bcQuotaGlobal != NULL) {
        BcQuota::quotaDestroy(&bcQuotaGlobal);
        bcQuotaGlobal = NULL;
    }

    if (bcGlobal != NULL) {
        BcHandle::destroy(&bcGlobal);
        bcGlobal = NULL;
    }

    if (bcObjectGlobal != NULL) {
        bcObjectGlobal->tearDown();
        memFree(bcObjectGlobal);
        bcObjectGlobal = NULL;
    }

    return status;
}

static Status
doLookAsideTest()
{
    Status status = StatusOk;
    struct timeval tv1, tv2;

    xSyslog(moduleName, XlogInfo, "doLookAsideTest::Starting look aside test");

    // Setup Lookaside
    status = lookasideInit(elementSize,
                           bufferCacheElementsInBcStress,
                           LookasideOptsNone,
                           &bufLookaside);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doLookAsideTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    status = doLaWork();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doLookAsideTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);

    xSyslog(moduleName,
            XlogInfo,
            "doLookAsideTest:: done, Elapsed time = %f seconds\n",
            (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                (double) (tv2.tv_sec - tv1.tv_sec));

    lookasideDestroy(&bufLookaside);

CommonExit:
    return status;
}

static Status
doMallocTest()
{
    Status status = StatusOk;
    struct timeval tv1, tv2;

    xSyslog(moduleName, XlogInfo, "doMallocTest::Starting malloc test");

    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    status = doMallocWork();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "doMallocTest: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);

    xSyslog(moduleName,
            XlogInfo,
            "doMallocTest:: done, Elapsed time = %f seconds",
            (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                (double) (tv2.tv_sec - tv1.tv_sec));

CommonExit:
    return status;
}

Status
bcStressMain()
{
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "bcStressMain: Start tests");

    // malloc memory for thread ids
    bcStressThreadIds =
        (pthread_t *) memAllocExt(sizeof(*bcStressThreadIds) * bcStressThreads,
                                  moduleName);
    if (bcStressThreadIds == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "bcStressMain: Failed %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (bcTest == true) {
        status = doBcTest();
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (bcQuotaTest == true) {
        status = doBcQuotaTest();
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (lookasideTest == true) {
        status = doLookAsideTest();
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (mallocTest) {
        status = doMallocTest();
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

CommonExit:
    if (bcStressThreadIds != NULL) {
        memFree(bcStressThreadIds);
        bcStressThreadIds = NULL;
    }

    xSyslog(moduleName,
            XlogInfo,
            "bcStressMain: End tests with %s",
            strGetFromStatus(status));

    // Out of resource is a valid/Success case for Functional tests.
    if (status == StatusNoMem) {
        status = StatusOk;
    }
    return status;
}

// Multiple threads beat up on the same buf$ Quota
static void *
bcQuotaStress(void *threadIndexIn)
{
    uint32_t threadIndex = *((uint32_t *) threadIndexIn);
    uintptr_t ret = 0;
    StatsLib *statsLib = StatsLib::get();

    // find elapsed time for each work element to compute average elapsed
    // time.
    struct timespec startTime, endTime;
    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    verify(ret == 0);

    xSyslog(moduleName,
            XlogDebug,
            "bcQuotaStress: start tid %u, iter %llu\n",
            threadIndex,
            (unsigned long long) loopIterations);

    for (uint32_t ii = 0; ii < loopIterations; ++ii) {
        for (uint64_t jj = 0; jj < BcQuotaObjectCount; jj++) {
            char **buf =
                (char **) memAlloc(sizeof(uintptr_t) * bcQuotaObject[jj]);
            verify(buf != NULL);
        reStart:
            for (uint64_t kk = 0; kk < bcQuotaObject[jj]; kk++) {
                uint64_t loopCount = 0, maxLoopCount = 16;
                while ((buf[kk] = (char *) bcQuotaGlobal->quotaAllocBuf(jj)) ==
                       NULL) {
                    // If someone is releasing all the Quota, let it do it.
                    if (useSleep) {
                        sysUSleep(delayInUs);
                    }
                    loopCount++;
                    if (loopCount == maxLoopCount) {
                        break;
                    }
                }
                if (buf[kk] == NULL) {
                    // back off and release all the Quota
                    for (uint64_t mm = 0; mm < kk; mm++) {
                        bcQuotaGlobal->quotaFreeBuf(buf[mm], jj);
                        buf[mm] = NULL;
                    }
                    goto reStart;
                }
            }

            for (uint64_t kk = 0; kk < bcQuotaObject[jj]; kk++) {
                bcQuotaGlobal->quotaFreeBuf(buf[kk], jj);
                buf[kk] = NULL;
            }

            memFree(buf);
        }
        if (threadIndex == 0) {
            statsLib->statNonAtomicIncr(bcQuotaTestCurIter);
        }
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);
    uint64_t elapTimeUsec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerUSec;

    xSyslog(moduleName,
            XlogDebug,
            "bcQuotaStress: End tid %u, ret %lu, elapsed time Usec %ld\n",
            threadIndex,
            ret,
            elapTimeUsec);

    return (void *) ret;
}

// Multiple threads beat up on the same buf$
void *
bcStress(void *threadIndexIn)
{
    uint32_t threadIndex = *((uint32_t *) threadIndexIn);
    uintptr_t ret = 0;
    uint64_t *buf;
    uint64_t *prev;
    uint64_t *next;
    uint64_t *head = NULL;
    StatsLib *statsLib = StatsLib::get();

    // find elapsed time for each work element to compute average elapsed
    // time.
    struct timespec startTime, endTime;
    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    verify(ret == 0);

    xSyslog(moduleName,
            XlogDebug,
            "bcStress: start tid %u, iter %llu\n",
            threadIndex,
            (unsigned long long) loopIterations);

    verify(bufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < loopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            buf = (uint64_t *) bcGlobal->allocBuf(XidInvalid);
            verify(buf);

            if (useSleep) {
                sysUSleep(delayInUs);
            }

            if (prev) {
                *prev = (uint64_t) buf;
            } else {
                head = buf;
            }

            // parity with mallocStress()
            ret += (uintptr_t) buf;

            prev = buf;
        }

        buf = head;
        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            bcGlobal->freeBuf(buf);
            buf = next;
        }
        if (threadIndex == 0) {
            statsLib->statNonAtomicIncr(bcTestCurIter);
        }
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    verify(ret == 0);
    uint64_t elapTimeUsec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerUSec;

    xSyslog(moduleName,
            XlogDebug,
            "bcStress: End tid %u, ret %lu, elapsed time Usec %ld\n",
            threadIndex,
            ret,
            elapTimeUsec);

    return (void *) ret;
}

void *
laStress(void *threadIndexIn)
{
    uint32_t threadIndex = *((uint32_t *) threadIndexIn);
    uintptr_t ret = 0;
    uint64_t *buf;
    uint64_t *prev;
    uint64_t *next;
    uint64_t *head;
    StatsLib *statsLib = StatsLib::get();

    verify(bufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < loopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            buf = (uint64_t *) lookasideAlloc(&bufLookaside);
            verify(buf);

            if (useSleep) {
                sysUSleep(delayInUs);
            }

            if (prev) {
                *prev = (uint64_t) buf;
            } else {
                head = buf;
            }

            // parity with mallocStress()
            ret += (uintptr_t) buf;

            prev = buf;
        }

        buf = head;
        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            lookasideFree(&bufLookaside, buf);
            buf = next;
        }
        if (threadIndex == 0) {
            statsLib->statAtomicIncr64(lookasideTestCurIter);
        }
    }

    return (void *) ret;
}

static void *
mallocStress(void *threadIndexIn)
{
    uint32_t threadIndex = *((uint32_t *) threadIndexIn);
    uintptr_t ret = 0;
    uint64_t *buf;
    uint64_t *prev;
    uint64_t *next;
    uint64_t *head;
    StatsLib *statsLib = StatsLib::get();

    verify(bufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < loopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            buf = (uint64_t *) memAlloc(elementSize);
            verify(buf);

            if (useSleep) {
                sysUSleep(delayInUs);
            }

            if (prev) {
                *prev = (uint64_t) buf;
            } else {
                head = buf;
            }

            // prevent compiler from optimizing away the loop
            ret += (uintptr_t) buf;

            prev = buf;
        }

        buf = head;
        for (uint32_t jj = 0;
             jj < bufferCacheElementsInBcStress / bcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            memFree(buf);
            buf = next;
        }
        if (threadIndex == 0) {
            statsLib->statAtomicIncr64(mallocTestCurIter);
        }
    }

    return (void *) ret;
}

static Status
doBcWork()
{
    Status status;
    int ret;
    uint32_t threadIndex[bcStressThreads];

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       bcStress,
                                                       &threadIndex[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
    }

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        verify(!ret);
    }
    return StatusOk;
}

static Status
doBcQuotaWork()
{
    Status status;
    int ret;
    uint32_t threadIndex[bcStressThreads];

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       bcQuotaStress,
                                                       &threadIndex[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
    }

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        verify(!ret);
    }
    return StatusOk;
}

static Status
doLaWork()
{
    Status status;
    int ret;
    uint32_t threadIndex[bcStressThreads];
    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       laStress,
                                                       &threadIndex[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
    }

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        verify(!ret);
    }
    return StatusOk;
}

static Status
doMallocWork()
{
    Status status;
    int ret;
    uint32_t threadIndex[bcStressThreads];
    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       mallocStress,
                                                       &threadIndex[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
    }

    for (uint32_t ii = 0; ii < bcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        verify(!ret);
    }
    return StatusOk;
}

Status
bcStressParseConfig(Config::Configuration *config,
                    char *key,
                    char *value,
                    bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibBcFuncTestConfig(
                       LibBcNumBufferCacheElements)) == 0) {
        bufferCacheElementsInBcStress = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibBcFuncTestConfig(LibBcLoopIterations)) ==
               0) {
        loopIterations = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibBcFuncTestConfig(LibBcElementSize)) ==
               0) {
        elementSize = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibBcFuncTestConfig(LibBcStressThreads)) ==
               0) {
        bcStressThreads = strtoll(value, NULL, 0);
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
