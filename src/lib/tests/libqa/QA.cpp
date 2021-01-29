// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "test/QA.h"
#include "operators/GenericTypes.h"
#include "msg/Message.h"

void
SchedTestCase::run()
{
    *this->status = this->testCase->func();
}

void
SchedTestCase::done()
{
    verify(sem_post(this->testCaseSem) == 0);
    this->~SchedTestCase();
    memFree(this);
}

void *
runTestCase(void *arg)
{
    Status (*testFn)(void) = (Status(*)(void)) arg;
    Status status = StatusUnknown;

    status = testFn();
    return (void *) status.code();
}

int
qaRunTestSuite(TestCase testCases[],
               unsigned numTestCases,
               TestCaseOptionMask optionMask)
{
    unsigned ii;
    Status status = StatusOk;
    int numTestsFailed = 0;
    sem_t testCaseSem;

    printf("\n1..%d\n", numTestCases);
    if (numTestCases > 0) {
        printf("# \"%s\"\n", testCases[0].testName);
    }

    verify(sem_init(&testCaseSem, 0, 0) == 0);

    for (ii = 0; ii < numTestCases; ii++) {
        struct timespec begin, end;
        double duration;
        if (testCases[ii].testEnabled) {
            if (optionMask & TestCaseSetTxn) {
                Txn txn = Txn::newTxn(Txn::Mode::NonLRQ);
                Txn::setTxn(txn);
            }
            printf("Commencing test \"%s\"\n", testCases[ii].testName);
            clock_gettime(CLOCK_REALTIME, &begin);
            if (optionMask & TestCaseScheduleOnRuntime) {
                void *schedTestCase = memAlloc(sizeof(SchedTestCase));
                new (schedTestCase)
                    SchedTestCase(&testCases[ii], &testCaseSem, &status);
                verifyOk(Runtime::get()->schedule(
                    static_cast<Schedulable *>(schedTestCase)));
                verify(sem_wait(&testCaseSem) == 0);
            } else if (optionMask & TestCaseScheduleOnDedicatedThread) {
                pthread_t thread;
                status =
                    Runtime::get()
                        ->createBlockableThread(&thread,
                                                NULL,
                                                runTestCase,
                                                (void *) testCases[ii].func);

                if (status == StatusOk) {
                    void
                        *tmpStatus;  // Because sizeof(void *) > sizeof(Status);
                    sysThreadJoin(thread, &tmpStatus);
                    status.fromStatusCode((StatusCode)(uintptr_t) tmpStatus);
                }
            } else {
                status = testCases[ii].func();
            }
            clock_gettime(CLOCK_REALTIME, &end);
            duration = (double) clkGetElapsedTimeInNanosSafe(&begin, &end) /
                       (double) NSecsPerSec;

            if (status != StatusOk) {
                printf("not ok %d - Test \"%s\" failed in %.3fs\n",
                       ii + 1,
                       testCases[ii].testName,
                       duration);
                numTestsFailed++;
            } else {
                printf("ok %d - Test \"%s\" passed in %.3fs\n",
                       ii + 1,
                       testCases[ii].testName,
                       duration);
            }
        } else {
            printf("ok %d - Test \"%s\" disabled # SKIP\n",
                   ii + 1,
                   testCases[ii].testName);
            if (!(optionMask & TestCaseOptDisableIsPass)) {
                numTestsFailed++;
            }
        }
    }

    return numTestsFailed;
}

void
qaPrintStackTraceInt(int numStackFramesIn,
                     const char *moduleName,
                     const char *fileName,
                     const char *fnName,
                     int lineNumber)
{
    void *stackFramePtrs[numStackFramesIn];
    int numStackFrames, ii;
    char **stackFrames;

    numStackFrames = backtrace(stackFramePtrs, numStackFramesIn);
    stackFrames = backtrace_symbols(stackFramePtrs, numStackFrames);

    xSyslog(moduleName,
            XlogDebug,
            "Stack trace of %s:%s:%d",
            fileName,
            fnName,
            lineNumber);

    xSyslog(moduleName, XlogDebug, "========================================");
    for (ii = 0; ii < numStackFrames; ii++) {
        xSyslog(moduleName, XlogDebug, "%s", stackFrames[ii]);
    }

    // @SymbolCheckIgnore
    free(stackFrames);
}
