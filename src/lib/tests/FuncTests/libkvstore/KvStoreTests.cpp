// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "msg/Xid.h"

#include "kvstore/KvStore.h"
#include "KvStoreTestsCommon.h"
#include "LibKvStoreFuncTestConfig.h"

#include "test/QA.h"

static constexpr const char *moduleName = "kvStoreFuncTest";

static uint64_t runsPerThread = RunsPerThreadDefault;
static uint64_t threadsPerCore = ThreadsPerCoreDefault;

static uint64_t kvsMaxThreads;
// This test defaults to disabled as it uses different Xids to access the
// same common kvstore.  The test should use the same Xid across the cluster
// for the common kvstore.
static bool commonStoreTestEnabled = false;

static void
kvStoreReportProgress(const char *testName,
                      const uint32_t threadNum,
                      const unsigned iter,
                      const uint64_t totalRuns)
{
    // Some of the tests run for a longer duration and it's nice to
    // report progress to the user.
    xSyslog(moduleName,
            XlogDebug,
            "testThread %u starting %s (iteration %d of %lu)",
            threadNum,
            testName,
            iter + 1,
            totalRuns);
}

static void *
kvStoreTestPerThread(void *arg)
{
    uint32_t threadNum = *((uint32_t *) arg);
    Config *config = Config::get();
    assert(config != NULL);
    uint32_t myNodeId = config->getMyNodeId();

    // Odd numbered threads do random ops to a common library which means
    // verifications cannot be done.
    // Even numbered threads do orderly ops to their own library which
    // allows verification checks.

    char kvStoreName[XcalarApiMaxPathLen];
    bool doVerification;

    if (commonStoreTestEnabled && (threadNum % 2)) {
        // Threads use the same KvStore which effectively randomizes
        // the ops and makes verification not possible.
        snprintf(kvStoreName, XcalarApiMaxPathLen, "%s", commonKvStoreName);
        doVerification = NoVerification;
    } else {
        // Each thread must have it's own unique KvStore so that
        // verifications can be done after each op.  The node number is
        // needed to allow running multiple usrnodes on the same system
        // (e.g. desktop for testing).
        snprintf(kvStoreName,
                 XcalarApiMaxPathLen,
                 "%sNode%uThread%u",
                 commonKvStoreName,
                 myNodeId,
                 threadNum);

        // Delete any prior kvstore with the same name.
        Xid kvs_id = XidMgr::get()->xidGetNext();
        KvStoreLib *kvs = KvStoreLib::get();

        Status status = kvs->open(kvs_id, kvStoreName, KvStoreSession);
        if (status == StatusOk) {
            kvs->close(kvs_id, KvStoreCloseDeletePersisted);
        }
        // Errors from open will be caught downstream.

        doVerification = DoVerification;
    }

    for (unsigned ii = 0; ii < runsPerThread; ii++) {
        kvStoreReportProgress("kvStoreTests", threadNum, ii, runsPerThread);
        kvStoreTests(kvStoreName, doVerification);

        kvStoreReportProgress("kvStoreBigMessageTest",
                              threadNum,
                              ii,
                              runsPerThread);
        kvStoreBigMessageTest(kvStoreName, doVerification);

        kvStoreReportProgress("kvStoreBadUserTest",
                              threadNum,
                              ii,
                              runsPerThread);
        kvStoreBadUserTest(kvStoreName, doVerification);

        kvStoreReportProgress("kvStoreRandomTest",
                              threadNum,
                              ii,
                              runsPerThread);
        kvStoreRandomTest(kvStoreName, doVerification);
    }
    return NULL;
}

Status
kvStoreStress()
{
    Status status;
    pthread_t *threadHandle;

    kvsMaxThreads =
        (uint64_t) XcSysHelper::get()->getNumOnlineCores() * threadsPerCore;

    // malloc memory for thread ids
    threadHandle =
        (pthread_t *) memAllocExt(sizeof(*threadHandle) * kvsMaxThreads,
                                  moduleName);
    assert(threadHandle != NULL);
    uint32_t threadIndex[kvsMaxThreads];

    for (unsigned ii = 0; ii < kvsMaxThreads; ii++) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       kvStoreTestPerThread,
                                                       &threadIndex[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    xSyslog(moduleName,
            XlogDebug,
            "%lu threads have been created.\n",
            kvsMaxThreads);

    for (unsigned ii = 0; ii < kvsMaxThreads; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }

    xSyslog(moduleName,
            XlogDebug,
            "%lu threads have been joined.\n",
            kvsMaxThreads);

    memFree(threadHandle);

    return StatusOk;
}

Status
kvStoreStressParseConfig(Config::Configuration *config,
                         char *key,
                         char *value,
                         bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibKvStoreFuncTestConfig(
                       LibKvStoreThreadsPerCore)) == 0) {
        threadsPerCore = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibKvStoreFuncTestConfig(
                              LibKvStoreRunsPerThread)) == 0) {
        runsPerThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibKvStoreFuncTestConfig(
                              LibKvStoreRandomLoopIterations)) == 0) {
        randomLoopIterations = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibKvStoreFuncTestConfig(
                              LibKvStoreCommonStoreTestEnabled)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            commonStoreTestEnabled = true;
        } else if (strcasecmp(value, "false") == 0) {
            commonStoreTestEnabled = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else {
        status = StatusUsrNodeIncorrectParams;
    }

    xSyslog(moduleName,
            XlogDebug,
            "%s changed %s to %s",
            (status == StatusOk ? "Successfully" : "Unsuccessfully"),
            key,
            value);

    return status;
}
