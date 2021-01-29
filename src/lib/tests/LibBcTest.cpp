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

#include "StrlFunc.h"  // strlcpy
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
#include "common/InitTeardown.h"

#include "test/QA.h"  // Must be last

enum {
    MaxBufferCacheElementsInBcStress = (4096),
    LoopIterations = 64,
    UseSleep = false,
    DelayInUs = 10,
    elementSize = 256
};

Status bcTest();
Status lookasideTest();
Status mallocTest();
Status bcQuotaTest();

pthread_t *bcStressThreadIds;
void *bcStress(void *unused);
void *laStress(void *unused);
void doBcWork();
void doBcQuotaWork();
void doLaWork();
void doMallocWork();
const char *moduleName = "bcstress";
const unsigned Iterations = 3;

BcQuota *bcQuotaGlobal;
constexpr uint64_t BcQuotaObjectCount = 8;
static uint64_t bcQuotaObject[BcQuotaObjectCount];
BcHandle *bcGlobal;
BcObject *bcObjectGlobal;
LookasideList bufLookaside;
unsigned MaxBcStressThreads;

// The disabled tests below are taken straight from
// BcStress.cpp and don't have a pass/fail criteria.
// Xc-4446 tracks how to proceed on this.
TestCase testCases[] = {
    {"Buffercache test", bcTest, TestCaseEnable, ""},
    {"Lookaside test", lookasideTest, TestCaseDisable, ""},
    {"Malloc test", mallocTest, TestCaseDisable, ""},
    // XXX: Disabling below test until ENG-9433 is fixed
    {"Buffercache quota test", bcQuotaTest, TestCaseDisable, ""},
};

TestCaseOptionMask testCaseOptionMask = TestCaseOptDisableIsPass;

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);

    MaxBcStressThreads = (unsigned) sysconf(_SC_NPROCESSORS_ONLN);
    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    // malloc memory for thread ids
    bcStressThreadIds = (pthread_t *) memAllocExt(sizeof(*bcStressThreadIds) *
                                                      MaxBcStressThreads,
                                                  moduleName);
    assert(bcStressThreadIds);

    int numFailedTests;
    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    memFree(bcStressThreadIds);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}

Status
bcTest()
{
    struct timeval tv1, tv2;
    bcObjectGlobal = (BcObject *) memAllocExt(sizeof(BcObject), moduleName);
    verifyOk(bcObjectGlobal->setUp("bc.bcStress",
                                   MaxBufferCacheElementsInBcStress,
                                   1,
                                   elementSize,
                                   BcObject::InitFastAllocsOnly,
                                   BufferCacheObjects::ObjectIdUnknown));
    bcGlobal = BcHandle::create(bcObjectGlobal);
    assert(bcGlobal != NULL);

    // Do buf$ work
    printf("------------------------------------------------\n");
    printf("bcStress::Starting buf$ stress test...\n");
    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    doBcWork();

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);
    printf("Buf$ test done. All buffer cache threads have exited\n");
    printf("Elapsed time = %f seconds\n",
           (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
               (double) (tv2.tv_sec - tv1.tv_sec));

    BcHandle::destroy(&bcGlobal);
    bcGlobal = NULL;
    bcObjectGlobal->tearDown();
    memFree(bcObjectGlobal);
    bcObjectGlobal = NULL;

    printf("------------------------------------------------\n");

    return StatusOk;
}

Status
bcQuotaTest()
{
    StatsLib *statsLib = StatsLib::get();
    struct timeval tv1, tv2;
    bcObjectGlobal = (BcObject *) memAllocExt(sizeof(BcObject), moduleName);
    verifyOk(bcObjectGlobal->setUp("bc.bcStress",
                                   MaxBufferCacheElementsInBcStress,
                                   1,
                                   elementSize,
                                   BcObject::InitFastAllocsOnly,
                                   BufferCacheObjects::ObjectIdUnknown));
    statsLib->removeGroupIdFromHashTable("bc.bcStress");
    bcGlobal = BcHandle::create(bcObjectGlobal);
    assert(bcGlobal != NULL);

    // Do buf$ work
    uint64_t ii;
    for (ii = 0; ii < BcQuotaObjectCount; ii++) {
        bcQuotaObject[ii] = ii + 1;
    }
    bcQuotaGlobal =
        BcQuota::quotaCreate(bcGlobal, bcQuotaObject, BcQuotaObjectCount);
    assert(bcQuotaGlobal != NULL);

    printf("------------------------------------------------\n");
    printf("bcQuotaStress::Starting buf$ quota stress test...\n");
    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);

    doBcQuotaWork();

    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);
    printf("Buf$ quota test done. All buffer cache threads have exited\n");
    printf("Elapsed time = %f seconds\n",
           (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
               (double) (tv2.tv_sec - tv1.tv_sec));

    BcQuota::quotaDestroy(&bcQuotaGlobal);
    bcQuotaGlobal = NULL;
    BcHandle::destroy(&bcGlobal);
    bcGlobal = NULL;
    bcObjectGlobal->tearDown();
    memFree(bcObjectGlobal);
    bcObjectGlobal = NULL;

    printf("------------------------------------------------\n");

    return StatusOk;
}

Status
lookasideTest()
{
    Status status;
    struct timeval tv1, tv2;

    // Setup Lookaside
    status = lookasideInit(elementSize,
                           MaxBufferCacheElementsInBcStress,
                           LookasideOptsNone,
                           &bufLookaside);
    assert(status == StatusOk);

    // Do lookaside work
    printf("bcstress::Starting lookaside stress test...\n");
    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);
    doLaWork();
    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);
    printf("Lookaside test done All lookaside threads have exited\n");
    printf("Elapsed time = %f seconds\n",
           (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
               (double) (tv2.tv_sec - tv1.tv_sec));

    return status;
}

Status
mallocTest()
{
    struct timeval tv1, tv2;
    // Do malloc work
    printf("bcstress::Starting malloc stress test...\n");
    // @SymbolCheckIgnore
    gettimeofday(&tv1, NULL);
    doMallocWork();
    // @SymbolCheckIgnore
    gettimeofday(&tv2, NULL);
    printf("Malloc test done All malloc threads have exited\n");
    printf("Elapsed time = %f seconds\n",
           (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
               (double) (tv2.tv_sec - tv1.tv_sec));

    return StatusOk;
}

// Multiple threads beat up on the same buf$ Quota
static void *
bcQuotaStress(void *threadIndexIn)
{
    uint32_t threadIndex = *((uint32_t *) threadIndexIn);
    uintptr_t ret = 0;

    // find elapsed time for each work element to compute average elapsed
    // time.
    struct timespec startTime, endTime;
    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    assert(ret == 0);

    printf("bcQuotaStress: start tid %u, iter %u\n",
           threadIndex,
           LoopIterations);

    for (uint32_t ii = 0; ii < LoopIterations; ++ii) {
        for (uint64_t jj = 0; jj < BcQuotaObjectCount; jj++) {
            char **buf =
                (char **) memAlloc(sizeof(uintptr_t) * bcQuotaObject[jj]);
        reStart:
            for (uint64_t kk = 0; kk < bcQuotaObject[jj]; kk++) {
                uint64_t loopCount = 0, maxLoopCount = 1 * KB;
                while ((buf[kk] = (char *) bcQuotaGlobal->quotaAllocBuf(jj)) ==
                       NULL) {
                    // If someone is releasing all the Quota, let it do it.
                    if (UseSleep) {
                        sysUSleep(DelayInUs);
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
        // printf("bcQuotaStress: tid %u, iter %u done\n", threadIndex, ii);
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    assert(ret == 0);
    uint64_t elapTimeUsec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerUSec;

    printf("bcStress: End tid %u, ret %lu, elapsed time Usec %ld\n",
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

    // find elapsed time for each work element to compute average elapsed
    // time.
    struct timespec startTime, endTime;
    ret = clock_gettime(CLOCK_REALTIME, &startTime);
    assert(ret == 0);

    printf("bcStress: start tid %u, iter %u\n", threadIndex, LoopIterations);

    assert(MaxBufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < LoopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            buf = (uint64_t *) bcGlobal->allocBuf(XidInvalid);
            assert(buf);

            if (UseSleep) {
                sysUSleep(DelayInUs);
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
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            bcGlobal->freeBuf(buf);
            buf = next;
        }
    }

    ret = clock_gettime(CLOCK_REALTIME, &endTime);
    assert(ret == 0);
    uint64_t elapTimeUsec =
        clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerUSec;

    printf("bcStress: End tid %u, ret %lu, elapsed time Usec %ld\n",
           threadIndex,
           ret,
           elapTimeUsec);

    return (void *) ret;
}

void *
laStress(void *unused)
{
    uintptr_t ret = 0;
    uint64_t *buf;
    uint64_t *prev;
    uint64_t *next;
    uint64_t *head = NULL;

    assert(MaxBufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < LoopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            buf = (uint64_t *) lookasideAlloc(&bufLookaside);
            assert(buf);

            if (UseSleep) {
                sysUSleep(DelayInUs);
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
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            lookasideFree(&bufLookaside, buf);
            buf = next;
        }
    }

    return (void *) ret;
}

static void *
mallocStress(void *unused)
{
    uintptr_t ret = 0;
    uint64_t *buf;
    uint64_t *prev;
    uint64_t *next;
    uint64_t *head = NULL;

    assert(MaxBufferCacheElementsInBcStress > 0);

    for (uint32_t ii = 0; ii < LoopIterations; ++ii) {
        prev = NULL;

        for (uint32_t jj = 0;
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            buf = (uint64_t *) memAlloc(elementSize);
            assert(buf);

            if (UseSleep) {
                sysUSleep(DelayInUs);
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
             jj < MaxBufferCacheElementsInBcStress / MaxBcStressThreads;
             jj++) {
            next = (uint64_t *) (*buf);
            memFree(buf);
            buf = next;
        }
    }

    return (void *) ret;
}

void
doBcWork()
{
    int ret;
    uint32_t threadIndex[MaxBcStressThreads];

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        verifyOk(Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       bcStress,
                                                       &threadIndex[ii]));
    }

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        assert(!ret);
    }
}

void
doBcQuotaWork()
{
    int ret;
    uint32_t threadIndex[MaxBcStressThreads];

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        threadIndex[ii] = ii;
        verifyOk(Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       bcQuotaStress,
                                                       &threadIndex[ii]));
    }

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        assert(!ret);
    }
}

void
doLaWork()
{
    int ret;
    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        verifyOk(Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       laStress,
                                                       NULL));
    }

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        assert(!ret);
    }
}

void
doMallocWork()
{
    int ret;
    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        verifyOk(Runtime::get()->createBlockableThread(&bcStressThreadIds[ii],
                                                       NULL,
                                                       mallocStress,
                                                       NULL));
    }

    for (uint32_t ii = 0; ii < MaxBcStressThreads; ++ii) {
        ret = sysThreadJoin(bcStressThreadIds[ii], NULL);
        assert(!ret);
    }
}
