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

#include "optimizer/Optimizer.h"
#include "OptimizerTestsCommon.h"
#include "LibOptimizerFuncTestConfig.h"

#include "test/QA.h"

static constexpr const char *moduleName = "optimizerFuncTest";

static uint64_t numberOfCores = 0;  // 0 == use physical count
static uint64_t runsPerThread = RunsPerThreadDefault;
static uint64_t threadsPerCore = ThreadsPerCoreDefault;
static uint64_t optMaxThreads;

static void *
optTestPerThread(void *arg)
{
    uint32_t threadNum = *((uint32_t *) arg);

    for (unsigned ii = 0; ii < runsPerThread; ii++) {
        optDoOptimizerTest(threadNum);
    }
    return NULL;
}

Status
optimizerStress()
{
    Status status;
    pthread_t *threadHandle;

    if (numberOfCores == 0) {
        // Wasn't specified as config parameter so use all the cores.
        numberOfCores = (uint64_t) XcSysHelper::get()->getNumOnlineCores();
    }

    optMaxThreads = numberOfCores * threadsPerCore;

    // malloc memory for thread ids
    threadHandle =
        (pthread_t *) memAllocExt(sizeof(*threadHandle) * optMaxThreads,
                                  moduleName);
    assert(threadHandle != NULL);
    uint32_t threadIndex[optMaxThreads];

    for (unsigned ii = 0; ii < optMaxThreads; ii++) {
        threadIndex[ii] = ii;
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       optTestPerThread,
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
            optMaxThreads);

    for (unsigned ii = 0; ii < optMaxThreads; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }

    xSyslog(moduleName,
            XlogDebug,
            "%lu threads have been joined.\n",
            optMaxThreads);

    memFree(threadHandle);

    return StatusOk;
}

Status
optimizerStressParseConfig(Config::Configuration *config,
                           char *key,
                           char *value,
                           bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibOptimizerFuncTestConfig(
                       LibOptimizerThreadsPerCore)) == 0) {
        threadsPerCore = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOptimizerFuncTestConfig(
                              LibOptimizerRunsPerThread)) == 0) {
        runsPerThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibOptimizerFuncTestConfig(
                              LibOptimizerNumberOfCores)) == 0) {
        numberOfCores = strtoll(value, NULL, 0);
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
