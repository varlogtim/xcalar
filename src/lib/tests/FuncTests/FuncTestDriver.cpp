// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>
#include <unistd.h>
#include <new>

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "util/System.h"

#include "test/FuncTests/FuncTestDriver.h"
#include "test/FuncTests/Hello.h"
#include "test/FuncTests/BcStressTest.h"
#include "test/FuncTests/TableTests.h"
#include "test/FuncTests/RuntimeTests.h"
#include "test/FuncTests/SessionTest.h"
#include "test/FuncTests/LogTest.h"
#include "test/FuncTests/OperatorsFuncTest.h"
#include "test/FuncTests/CalloutTests.h"
#include "test/FuncTests/DagTests.h"
#include "test/FuncTests/KvStoreTests.h"
#include "test/FuncTests/XdbTests.h"
#include "test/FuncTests/DataSourceFuncTests.h"
#include "test/FuncTests/MsgTests.h"
#include "test/FuncTests/AppFuncTests.h"
#include "test/FuncTests/LibNsTests.h"
#include "test/FuncTests/QueryManagerTests.h"
#include "test/FuncTests/QueryEvalTests.h"
#include "test/FuncTests/OptimizerTests.h"
#include "test/FuncTests/LocalMsgTests.h"

#include "test/QA.h"

struct FuncTest {
    const char *testName;

    // This is run on all nodes if --allNodes are specified.
    Status (*testFn)(void);

    // This is run on 1 node even if --allNodes are specified.
    // This is run before any tests have begun. Use this to do
    // the init required in your tests.
    Status (*initFn)(void);

    // Same as above, but for cleanouts
    void (*cleanoutFn)(void);
};

static const char *moduleName = "FuncTestDriver";
FuncTestDriver *FuncTestDriver::funcTestDriver = NULL;
bool FuncTestDriver::funcTestDriverInited = false;
static FuncTest funcTests[] = {
    {"libhello::hello", helloMain, NULL, NULL},
    {"libbc::bcStress", bcStressMain, NULL, NULL},
    {"libruntime::stress", RuntimeTests::testStress, NULL, NULL},
    {"libruntime::custom", RuntimeTests::testCustom, NULL, NULL},
    {"libsession::sessionStress", sessionStress, NULL, NULL},
    {"liblog::logStress", logStress, logLoadDataFiles, NULL},
    {"liboperators::basicFunc", operatorsFuncTestBasicMain, NULL, NULL},
    {"liboperators::publishedTable", publishedTableTest, NULL, NULL},
    {"libcallout::threads", calloutTestThreads, NULL, NULL},
    {"libcallout::cancelStarvation", calloutTestCancelStarvation, NULL, NULL},
    {"libdag::sanityTest", dgSanityTest, NULL, NULL},
    {"libdag::randomTest", dgRandomTest, NULL, NULL},
    {"libkvstore::kvStoreStress", kvStoreStress, NULL, NULL},
    {"libxdb::xdbStress", xdbStress, NULL, NULL},
    {"libxdb::newCursorBenchmark", newCursorBenchmark, NULL, NULL},
    {"libmsg::msgStress", msgStress, NULL, NULL},
    {"liboptimizer::optimizerStress", optimizerStress, NULL, NULL},
    {"libapp::sanity", AppFuncTests::testSanity, NULL, NULL},
    {"libapp::stress", AppFuncTests::testStress, NULL, NULL},
    {"libns::nsTest", nsTest, NULL, NULL},
    {"libqm::qmStringQueryTest",
     QueryManagerTests::stringQueryTest,
     QueryManagerTests::initTest,
     QueryManagerTests::cleanoutTest},
    {"libqm::qmRetinaQueryTest",
     QueryManagerTests::retinaQueryTest,
     QueryManagerTests::initTest,
     QueryManagerTests::cleanoutTest},
    {"libqueryeval::queryEvalStress", queryEvalStress, NULL, NULL},
    {"liblocalmsg::sanity", LocalMsgTests::testSanity, NULL, NULL},
    {"liblocalmsg::stress", LocalMsgTests::testStress, NULL, NULL},
    {"libtable::sanity", TableTestsSanity, NULL, NULL},
    {"libtable::stress", TableTestsStress, NULL, NULL},
};

FuncTestDriver *
FuncTestDriver::get()
{
    return funcTestDriver;
}

Status
FuncTestDriver::createSingleton()
{
    Status status = StatusOk;

    FuncTestDriver::funcTestDriver = new (std::nothrow) FuncTestDriver;
    if (funcTestDriver == NULL) {
        status = StatusNoMem;
        return StatusNoMem;
    }

    // Add all the Stats for functional tests here.
    status = bcStressStatsInit();
    if (status != StatusOk) {
        goto CommonExit;
    }

    FuncTestDriver::funcTestDriverInited = true;

CommonExit:
    if (status != StatusOk) {
        deleteSingleton();
    }

    return status;
}

void
FuncTestDriver::deleteSingleton()
{
    if (funcTestDriver == NULL) {
        return;
    }
    delete funcTestDriver;
    funcTestDriver = NULL;
}

Status
FuncTestDriver::listTests(
    const char *testNamePattern,
    char (**testNamesOut)[FuncTestTypes::MaxTestNameLen + 1],
    unsigned *numTestsOut)
{
    unsigned totalNumTests = ArrayLen(funcTests);
    unsigned numTests = 0;
    unsigned ii;
    Status status = StatusUnknown;
    char(*testNames)[FuncTestTypes::MaxTestNameLen + 1] = NULL;

    testNames = (char(*)[sizeof(*testNames)]) memAlloc(sizeof(*testNames) *
                                                       totalNumTests);
    if (testNames == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate testNames "
                "(numTests: %u)",
                totalNumTests);
        status = StatusNoMem;
        goto CommonExit;
    }

    for (ii = 0; ii < totalNumTests; ii++) {
        if (strMatch(testNamePattern, funcTests[ii].testName)) {
            strlcpy(testNames[ii],
                    funcTests[ii].testName,
                    sizeof(testNames[ii]));
            numTests++;
        }
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (testNames != NULL) {
            memFree(testNames);
            testNames = NULL;
            numTests = 0;
        }
    }

    *testNamesOut = testNames;
    *numTestsOut = numTests;

    return status;
}

bool
FuncTestDriver::matchTest(
    const char *testName,
    unsigned numTests,
    const char testNamePatterns[][FuncTestTypes::MaxTestNameLen + 1])
{
    unsigned ii;
    for (ii = 0; ii < numTests; ii++) {
        if (strMatch(testNamePatterns[ii], testName)) {
            return true;
        }
    }
    return false;
}

struct ThreadArgs {
    StatusCode *status;
    FuncTest *funcTest;
};

void *
FuncTestDriver::startTestFn(void *arg)
{
    ThreadArgs *threadArgs;
    StatusCode *status;
    threadArgs = (ThreadArgs *) arg;
    status = threadArgs->status;
    xSyslog(moduleName,
            XlogInfo,
            "Functional Test '%s' is starting",
            threadArgs->funcTest->testName);
    *status = fixupStatus(threadArgs->funcTest->testFn().code());
    xSyslog(moduleName,
            XlogInfo,
            "Functional Test '%s' has completed: %s",
            threadArgs->funcTest->testName,
            strGetFromStatusCode(*status));
    memFree(threadArgs);
    return NULL;
}

Status
FuncTestDriver::runFuncTestsInit(
    bool runAllTest,
    unsigned numTests,
    const char testNames[][FuncTestTypes::MaxTestNameLen + 1])
{
    unsigned totalNumTests = ArrayLen(funcTests);
    Status status = StatusOk;
    unsigned ii;
    for (ii = 0; ii < totalNumTests; ii++) {
        if (runAllTest ||
            matchTest(funcTests[ii].testName, numTests, testNames)) {
            if (funcTests[ii].initFn != NULL) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' starting initialization",
                        funcTests[ii].testName);
                status = funcTests[ii].initFn();
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' completed initialization: %s",
                        funcTests[ii].testName,
                        strGetFromStatus(status));
                if (status != StatusOk) {
                    goto CommonExit;
                }
            }
        }
    }

CommonExit:
    return status;
}

void
FuncTestDriver::runFuncTestsCleanOut(
    bool runAllTest,
    unsigned numTests,
    const char testNames[][FuncTestTypes::MaxTestNameLen + 1])
{
    unsigned totalNumTests = ArrayLen(funcTests);
    Status status = StatusOk;
    unsigned ii;
    for (ii = 0; ii < totalNumTests; ii++) {
        if (runAllTest ||
            matchTest(funcTests[ii].testName, numTests, testNames)) {
            if (funcTests[ii].cleanoutFn != NULL) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' starting clean out",
                        funcTests[ii].testName);
                funcTests[ii].cleanoutFn();
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' completed clean out",
                        funcTests[ii].testName);
            }
        }
    }
}

Status
FuncTestDriver::runFuncTests(
    bool parallel,
    bool runAllTest,
    bool runOnAllNodes,
    unsigned numTestsIn,
    const char testNames[][FuncTestTypes::MaxTestNameLen + 1],
    XcalarApiOutput **outputOut,
    uint64_t *outputSizeOut)
{
    Status status = StatusOk;
    size_t ret;
    uint64_t numTestReturned = 0;
    XcalarApiStartFuncTestOutput *startFuncTestOutput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    uint64_t numNode = runOnAllNodes ? Config::get()->getActiveNodes() : 1;
    FuncTestOutputPerNode outputArray[numNode];
    TwoPcPayload *twoPcPayload = NULL;
    size_t twoPcPayloadSize;
    MsgMgr *msgMgr = MsgMgr::get();

    if (!XcalarConfig::get()->testMode_) {
        return StatusFunctionalTestDisabled;
    }

    for (uint64_t ii = 0; ii < numNode; ++ii) {
        outputArray[ii].numTestToRun = XcalarApiMaxNumFuncTests;
        outputArray[ii].testOutputs = (FuncTestTypes::FuncTestOutput *)
            memAllocExt(sizeof(FuncTestTypes::FuncTestOutput) *
                            XcalarApiMaxNumFuncTests,
                        moduleName);
        if (outputArray[ii].testOutputs == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        for (uint64_t jj = 0; jj < XcalarApiMaxNumFuncTests; ++jj) {
            outputArray[ii].testOutputs[jj].status = StatusInval.code();
            outputArray[ii].testOutputs[jj].testName[0] = '\0';
        }
    }

    status = this->runFuncTestsInit(runAllTest, numTestsIn, testNames);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (!runOnAllNodes) {
        status = this->runFuncTestsLocal(parallel,
                                         runAllTest,
                                         numTestsIn,
                                         testNames,
                                         outputArray[0].testOutputs,
                                         &outputArray[0].numTestReturned);
        outputArray[0].status = status;
    } else {
        twoPcPayloadSize = sizeof(*twoPcPayload) +
                           numTestsIn * sizeof(*twoPcPayload->testNamePatterns);

        twoPcPayload =
            (TwoPcPayload *) memAllocExt(twoPcPayloadSize, moduleName);

        twoPcPayload->parallel = parallel;
        twoPcPayload->runAllTests = runAllTest;
        twoPcPayload->numTestPatterns = numTestsIn;
        memcpy(twoPcPayload->testNamePatterns,
               testNames,
               sizeof(*twoPcPayload->testNamePatterns) * numTestsIn);

        MsgEphemeral eph;
        msgMgr->twoPcEphemeralInit(&eph,
                                   twoPcPayload,
                                   twoPcPayloadSize,
                                   0,
                                   TwoPcFastPath,
                                   TwoPcCallId::Msg2pcFuncTest1,
                                   outputArray,
                                   (TwoPcBufLife)(TwoPcMemCopyInput |
                                                  TwoPcMemCopyOutput));

        TwoPcHandle twoPcHandle;
        status = msgMgr->twoPc(&twoPcHandle,
                               MsgTypeId::Msg2pcFuncTest,
                               TwoPcDoNotReturnHandle,
                               &eph,
                               (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                  MsgRecvHdrPlusPayload),
                               TwoPcSyncCmd,
                               TwoPcAllNodes,
                               TwoPcIgnoreNodeId,
                               TwoPcClassNonNested);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(!twoPcHandle.twoPcHandle);
    }

    for (uint64_t ii = 0; ii < numNode; ++ii) {
        if (outputArray[ii].status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "TwoPc failed on node %lu with Status:%s",
                    ii,
                    strGetFromStatus(outputArray[ii].status));

            if (status == StatusOk) {
                status = outputArray[ii].status;
            }
        }
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    xSyslog(moduleName, XlogDebug, "All TwoPc issued and returned");

    numTestReturned = outputArray[0].numTestReturned;
    for (uint64_t ii = 0; ii < numNode; ++ii) {
        assert(numTestReturned == outputArray[ii].numTestReturned);
    }

    assert(numTestReturned <= XcalarApiMaxNumFuncTests);

    outputSize =
        XcalarApiSizeOfOutput(output->outputResult.startFuncTestOutput) +
        sizeof(FuncTestTypes::FuncTestOutput) * numTestReturned;

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    startFuncTestOutput = &output->outputResult.startFuncTestOutput;

    startFuncTestOutput->numTests = numTestReturned;

    for (uint64_t ii = 0; ii < numTestReturned; ++ii) {
        ret = strlcpy(startFuncTestOutput->testOutputs[ii].testName,
                      outputArray[0].testOutputs[ii].testName,
                      sizeof(startFuncTestOutput->testOutputs[ii].testName));

        assert(ret < sizeof(startFuncTestOutput->testOutputs[ii].testName));

        startFuncTestOutput->testOutputs[ii].status = StatusOk.code();
        for (uint64_t jj = 0; jj < numNode; ++jj) {
            if (outputArray[jj].testOutputs[ii].status != StatusOk.code()) {
                startFuncTestOutput->testOutputs[ii].status =
                    outputArray[jj].testOutputs[ii].status;
                break;
            }
        }
    }

    this->runFuncTestsCleanOut(runAllTest, numTestsIn, testNames);
CommonExit:
    for (uint64_t ii = 0; ii < numNode; ++ii) {
        if (outputArray[ii].testOutputs != NULL) {
            memFree(outputArray[ii].testOutputs);
        } else {
            // Allocation quits at first failure...rest of
            // outputArray is uninit'd.
            assert(status == StatusNoMem);
            break;
        }
    }

    if (status == StatusOk) {
        *outputOut = output;
        *outputSizeOut = outputSize;
    }
    if (twoPcPayload != NULL) {
        memFree(twoPcPayload);
        twoPcPayload = NULL;
    }

    return status;
}

void
FuncTestDriver::processTwoPcRequest(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    TwoPcPayload *twoPcPayload = (TwoPcPayload *) payload;
    size_t payloadLength = 0;
    unsigned numTestsOut;
    FuncTestTypes::FuncTestOutput *funcTestsOutputArray =
        (FuncTestTypes::FuncTestOutput *)
            memAllocExt(sizeof(FuncTestTypes::FuncTestOutput) *
                            XcalarApiMaxNumFuncTests,
                        moduleName);
    if (funcTestsOutputArray == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    } else {
        status = this->runFuncTestsLocal(twoPcPayload->parallel,
                                         twoPcPayload->runAllTests,
                                         twoPcPayload->numTestPatterns,
                                         twoPcPayload->testNamePatterns,
                                         funcTestsOutputArray,
                                         &numTestsOut);
        if (status == StatusOk) {
            // Repurpose payload for our output
            TwoPcCompletionPayload *compPayLoad =
                (TwoPcCompletionPayload *) payload;
            compPayLoad->numTestReturned = numTestsOut;
            memcpy(compPayLoad->funcTestsOutputArray,
                   funcTestsOutputArray,
                   sizeof(*funcTestsOutputArray) * numTestsOut);

            payloadLength = sizeof(*compPayLoad) +
                            sizeof(*funcTestsOutputArray) * numTestsOut;
        }
        memFree(funcTestsOutputArray);
    }

CommonExit:

    eph->setAckInfo(status, payloadLength);
}

void *
FuncTestDriver::runTestFn(void *arg)
{
    FuncTestDriver *ftDriver = FuncTestDriver::get();
    RunFtLocalArgs *targs = (RunFtLocalArgs *) arg;
    bool parallel = targs->parallel;
    bool runAllTest = targs->runAllTest;
    unsigned numTestsIn = targs->numTestsIn;
    const char(*testNames)[FuncTestTypes::MaxTestNameLen + 1] =
        targs->testNames;
    FuncTestTypes::FuncTestOutput *funcTestsOutputArray =
        targs->funcTestOutputArray;
    unsigned *numTestsOut = targs->numTestsOut;
    Semaphore *doneSem = targs->doneSem;
    Status *retStatus = targs->returnStatus;

    Status status = StatusOk;
    int ret;
    unsigned ii;
    unsigned totalNumTests = ArrayLen(funcTests);
    pthread_t *testThreads = NULL;
    unsigned numTests = 0, numThreadsSpawned = 0;
    pthread_attr_t attr;
    ThreadArgs *threadArgs = NULL;
    Runtime *rt = Runtime::get();

    if (parallel) {
        testThreads =
            (pthread_t *) memAlloc(sizeof(*testThreads) * totalNumTests);
        if (testThreads == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate testThreads "
                    "(numTests: %u)",
                    numTests);
            status = StatusNoMem;
            goto CommonExit;
        }
    }

    numThreadsSpawned = 0;
    status = StatusOk;
    for (ii = 0; ii < totalNumTests; ii++) {
        if (runAllTest || ftDriver->matchTest(funcTests[ii].testName,
                                              numTestsIn,
                                              testNames)) {
            if (parallel) {
                threadArgs = (ThreadArgs *) memAlloc(sizeof(*threadArgs));
                if (threadArgs == NULL) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Insufficient memory to allocate threadArgs");
                    status = StatusNoMem;
                    break;
                }

                errno = 0;
                ret = pthread_attr_init(&attr);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                    break;
                }

                ret =
                    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                    break;
                }

                strlcpy(funcTestsOutputArray[numTests].testName,
                        funcTests[ii].testName,
                        sizeof(funcTestsOutputArray[numTests].testName));
                threadArgs->status = &funcTestsOutputArray[numTests].status;
                threadArgs->funcTest = &funcTests[ii];

                status =
                    rt->createBlockableThread(&testThreads[numThreadsSpawned],
                                              &attr,
                                              FuncTestDriver::startTestFn,
                                              threadArgs);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                    break;
                }

                numThreadsSpawned++;
                threadArgs = NULL;  // Ref given to thread
            } else {
                StatusCode tmpStatus;
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' is starting",
                        funcTests[ii].testName);
                tmpStatus = funcTests[ii].testFn().code();
                tmpStatus = fixupStatus(tmpStatus);
                xSyslog(moduleName,
                        XlogInfo,
                        "Functional Test '%s' has completed : %s",
                        funcTests[ii].testName,
                        strGetFromStatusCode(tmpStatus));
                funcTestsOutputArray[numTests].status = tmpStatus;
                strlcpy(funcTestsOutputArray[numTests].testName,
                        funcTests[ii].testName,
                        sizeof(funcTestsOutputArray[numTests].testName));
            }
            numTests++;
        }
    }

    for (ii = 0; ii < numThreadsSpawned; ii++) {
        sysThreadJoin(testThreads[ii], NULL);
    }

CommonExit:
    if (threadArgs != NULL) {
        memFree(threadArgs);
        threadArgs = NULL;
    }

    if (testThreads != NULL) {
        memFree(testThreads);
        testThreads = NULL;
    }

    *numTestsOut = numTests;
    *retStatus = status;
    doneSem->post();

    return NULL;
}

Status
FuncTestDriver::runFuncTestsLocal(
    bool parallel,
    bool runAllTest,
    unsigned numTestsIn,
    const char testNames[][FuncTestTypes::MaxTestNameLen + 1],
    FuncTestTypes::FuncTestOutput *funcTestsOutputArray,
    unsigned *numTestsOut)
{
    Status status = StatusOk;
    int ret;
    pthread_t testThread;
    unsigned numTests = 0;
    RunFtLocalArgs ftLocalArgs;
    Semaphore doneSem(0);
    pthread_attr_t attr;
    Runtime *rt = Runtime::get();

    if (!funcTestDriverInited) {
        // Someone must have disabled functional testing. Probably
        // customer build
        xSyslog(moduleName,
                XlogErr,
                "Request to start functional test in production build denied. "
                "Please contact your local Xcalar sales representative.");
        status = StatusFunctionalTestDisabled;
        goto CommonExit;
    }

    if (numTestsIn > XcalarApiMaxNumFuncTests) {
        xSyslog(moduleName,
                XlogErr,
                "Too many functional test requested (%u). Max is %u",
                numTestsIn,
                XcalarApiMaxNumFuncTests);
        status = StatusFunctionalTestNumFuncTestExceeded;
        goto CommonExit;
    }

    ftLocalArgs.parallel = parallel;
    ftLocalArgs.runAllTest = runAllTest;
    ftLocalArgs.numTestsIn = numTestsIn;
    ftLocalArgs.testNames = testNames;
    ftLocalArgs.funcTestOutputArray = funcTestsOutputArray;
    ftLocalArgs.numTestsOut = &numTests;
    ftLocalArgs.doneSem = &doneSem;
    ftLocalArgs.returnStatus = &status;

    ret = pthread_attr_init(&attr);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = rt->createBlockableThread(&testThread,
                                       &attr,
                                       FuncTestDriver::runTestFn,
                                       &ftLocalArgs);
    BailIfFailed(status);

    doneSem.semWait();

    sysThreadJoin(testThread, NULL);

CommonExit:
    *numTestsOut = numTests;

    return status;
}

void
FuncTestDriver::processTwoPcComplete(MsgEphemeral *eph, void *payload)
{
    TwoPcCompletionPayload *compPayload = (TwoPcCompletionPayload *) payload;

    FuncTestOutputPerNode *outputArray =
        (FuncTestOutputPerNode *) eph->ephemeral;

    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);

    outputArray[dstNodeId].status = eph->status;
    if (eph->status == StatusOk) {
        outputArray[dstNodeId].numTestReturned = compPayload->numTestReturned;
        memcpy(outputArray[dstNodeId].testOutputs,
               compPayload->funcTestsOutputArray,
               sizeof(FuncTestTypes::FuncTestOutput) *
                   compPayload->numTestReturned);
    }
}

StatusCode
FuncTestDriver::fixupStatus(StatusCode status)
{
    // these are valid statuses and do not indicate failure
    if (status == StatusCodeNoMem || status == StatusCodeNoXdbPageBcMem ||
        status == StatusCodeApiWouldBlock ||
        status == StatusCodeMaxStatsGroupExceeded ||
        status == StatusCodeMaxStatsExceeded ||
        status == StatusCodeStreamPartialFailure ||
        // StatusCodeDgNodeInUse is expected if BulkLoad failed due to AppLoader
        // internal error and we deliberately leak the corresponding DagNode.
        status == StatusCodeDgNodeInUse) {
        return StatusCodeOk;
    }

    return status;
}
