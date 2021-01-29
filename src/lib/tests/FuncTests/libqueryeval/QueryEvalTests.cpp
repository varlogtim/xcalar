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
#include "TestQueries.h"
#include "dataset/Dataset.h"

#include "queryeval/QueryEvaluate.h"
#include "QueryEvalTestsCommon.h"
#include "LibQueryEvalFuncTestConfig.h"

#include "test/QA.h"  // Must be last

static constexpr const char *moduleName = "queryEvalFuncTest";

static uint64_t numberOfCores = 0;  // 0 == use physical count
static uint64_t runsPerThread = RunsPerThreadDefault;
static uint64_t threadsPerCore = ThreadsPerCoreDefault;
static bool stepThroughTestEnabled = true;
static bool evaluateTestEnabled = true;

static uint64_t qeMaxThreads;

static constexpr const char *simpleQuery =
    "load --url nfs://%s/jsonRandom"
    " --format json --name randDs%s;"
    "index --key JgPrimaryKey --dataset .XcalarDS.randDs%s --dsttable indexT "
    "--prefix pf;"
    "join --leftTable indexT --rightTable indexT --joinType fullOuterJoin;"
    "drop table *; drop constant *; drop dataset *;";

struct ArgQueryEvalTest {
    Runtime::SchedId schedId;
    uint32_t threadNum;
};

static void *
qeTestPerThread(void *arg)
{
    Status status = StatusOk;
    uint32_t threadNum = ((ArgQueryEvalTest *) arg)->threadNum;
    Runtime::SchedId schedId = ((ArgQueryEvalTest *) arg)->schedId;
    char buffer[MaxTestQueryBufSize];
    static const size_t ThreadUniqueBufSize = 256;
    char threadUniqueBuf[ThreadUniqueBufSize];
    // Two extra for  "*<threadUniquebuf>*"
    char datasetDelistPattern[ThreadUniqueBufSize + 2];
    Config *config = Config::get();
    XcalarApiOutput *apiOutput = NULL;
    size_t apiOutputSize = 0;

    for (unsigned ii = 0; ii < runsPerThread; ii++) {
        // Each thread does half and half of the tests with a different
        // dataset for each half.
        // Each thread must uniqueify its dataset names to avoid stomping
        // on each other.
        snprintf(threadUniqueBuf,
                 ThreadUniqueBufSize,
                 "%s-Node%dThread%dPart%d",
                 moduleName,
                 config->getMyNodeId(),
                 threadNum,
                 ii % 2);

        if (ii % 2) {
            if (!evaluateTestEnabled) {
                // User doesn't want this test to be run
                continue;
            }

            // Simple Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     simpleQuery,
                     qaGetQaDir(),
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoEvaluateTest(buffer);
            // Customer 1 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust1Query,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     cust1QueryFormat1);
            qeDoEvaluateTest(buffer);
	    // Customer 2 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust2Query,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoEvaluateTest(buffer);
            // Flight Demo Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     fdQuery,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoEvaluateTest(buffer);
            // Another Customer 1 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust1_2Query,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoEvaluateTest(buffer);
        } else {
            if (!stepThroughTestEnabled) {
                // User doesn't want this test to be run
                continue;
            }

            // Simple Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     simpleQuery,
                     qaGetQaDir(),
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoStepThroughTest(buffer, threadNum, schedId);
            // Customer 1 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust1Query,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     cust1QueryFormat1);
            qeDoStepThroughTest(buffer, threadNum, schedId);
            // Customer 2 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust2Query,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoStepThroughTest(buffer, threadNum, schedId);
            // Flight Demo Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     fdQuery,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoStepThroughTest(buffer, threadNum, schedId);
            // Another Customer 1 Query
            snprintf(buffer,
                     MaxTestQueryBufSize,
                     cust1_2Query,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf,
                     threadUniqueBuf);
            qeDoStepThroughTest(buffer, threadNum, schedId);
        }
        // Clean up the datasets so they don't clash with the next iteration.
        // Only delete those that this test created.
        snprintf(datasetDelistPattern,
                 sizeof(datasetDelistPattern),
                 "*%s*",
                 threadUniqueBuf);
        status = Dataset::get()->unloadDatasets(datasetDelistPattern,
                                                &apiOutput,
                                                &apiOutputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete dataset '%s': %s",
                    datasetDelistPattern,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        if (apiOutput != NULL) {
            memFree(apiOutput);
            apiOutput = NULL;
        }
    }

CommonExit:
    return NULL;
}

Status
queryEvalStress()
{
    Status status;
    pthread_t *threadHandle;

    if (numberOfCores == 0) {
        // Wasn't specified as config parameter so use all the cores.
        numberOfCores = (uint64_t) XcSysHelper::get()->getNumOnlineCores();
    }

    qeMaxThreads = numberOfCores * threadsPerCore;

    // malloc memory for thread ids
    threadHandle =
        (pthread_t *) memAllocExt(sizeof(*threadHandle) * qeMaxThreads,
                                  moduleName);
    assert(threadHandle != NULL);
    ArgQueryEvalTest args[qeMaxThreads];

    for (unsigned ii = 0; ii < qeMaxThreads; ii++) {
        args[ii].threadNum = ii;
        args[ii].schedId = static_cast<Runtime::SchedId>(
            rand() % Runtime::TotalFastPathScheds);
        status = Runtime::get()->createBlockableThread(&threadHandle[ii],
                                                       NULL,
                                                       qeTestPerThread,
                                                       &args[ii]);
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
            qeMaxThreads);

    for (unsigned ii = 0; ii < qeMaxThreads; ii++) {
        sysThreadJoin(threadHandle[ii], NULL);
    }

    xSyslog(moduleName,
            XlogDebug,
            "%lu threads have been joined.\n",
            qeMaxThreads);

    memFree(threadHandle);

    return StatusOk;
}

Status
queryEvalStressParseConfig(Config::Configuration *config,
                           char *key,
                           char *value,
                           bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibQueryEvalFuncTestConfig(
                       LibQueryEvalThreadsPerCore)) == 0) {
        threadsPerCore = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQueryEvalFuncTestConfig(
                              LibQueryEvalRunsPerThread)) == 0) {
        runsPerThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQueryEvalFuncTestConfig(
                              LibQueryEvalNumberOfCores)) == 0) {
        numberOfCores = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibQueryEvalFuncTestConfig(
                              LibQueryEvalEvaluateTestEnabled)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            evaluateTestEnabled = true;
        } else if (strcasecmp(value, "false") == 0) {
            evaluateTestEnabled = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibQueryEvalFuncTestConfig(
                              LibQueryEvalStepThroughTestEnabled)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            stepThroughTestEnabled = true;
        } else if (strcasecmp(value, "false") == 0) {
            stepThroughTestEnabled = false;
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
