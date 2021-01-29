// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>
#include <sys/timerfd.h>
#include "callout/Callout.h"
#include "common/InitTeardown.h"
#include "runtime/CondVar.h"

#include "test/QA.h"  // Must be last

static Status calloutTestSanity();
static Status calloutTestEdge();
static Status calloutTestRecur();
static Status calloutTestParallelSequential();
static Status calloutTestDestroy();

static TestCase testCases[] = {
    {"libcallout: sanity", calloutTestSanity, TestCaseEnable, ""},
    {"libcallout: edge cases", calloutTestEdge, TestCaseEnable, ""},
    {"libcallout: recurring callouts", calloutTestRecur, TestCaseEnable, ""},
    {"libcallout: parallel vs. sequential",
     calloutTestParallelSequential,
     TestCaseEnable,
     ""},
    {"libcallout: calloutDestroy", calloutTestDestroy, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime);

static volatile int progress;
static int callbackArg = 0;

static void
workCallback(void *value)
{
    assert(value == &callbackArg);
    __atomic_add_fetch(&progress, 1, __ATOMIC_RELAXED);
}

static Status
calloutTestSanity()
{
    CalloutQueue::Handle handles[5];
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    // Seed with same number for more deterministic results.
    srand(7);

    progress = 0;

    status = calloutQueue->insert(&handles[0],
                                  1,
                                  0,
                                  0,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    sysSleep(2);
    assert(progress == 1);

    assert(calloutQueue->cancel(&handles[0]) == 1);

    status = calloutQueue->insert(&handles[1],
                                  6,
                                  0,
                                  0,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[2],
                                  1,
                                  0,
                                  0,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[3],
                                  6,
                                  0,
                                  0,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[4],
                                  6,
                                  0,
                                  0,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    calloutQueue->cancel(&handles[4]);

    sysSleep(2);
    assert(progress == 2);

    sysSleep(5);
    assert(progress == 4);

    return StatusOk;
}

static CalloutQueue::Handle *freesSelfHandle = NULL;
static CalloutQueue::Handle *reschedHandle = NULL;
static Mutex reschedMutex;
static CondVar reschedCond;
static int reschedCounter = 0;

static void
freesSelf(void *arg)
{
    // This is expected to be a common usage pattern: a callout that's scheduled
    // to run once and clean up after itself.

    memset(freesSelfHandle, 0xFF, sizeof(*freesSelfHandle));
    memFree(freesSelfHandle);
}

static void
reschedulesSelf(void *arg)
{
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    // This is a more contrived usage pattern where a callout may schedule
    // itself to run again. In particular, this looks at what happens if
    // they overlap.

    reschedMutex.lock();

    // Increment counter.
    if (++*(int *) arg > 3) {
        reschedCond.broadcast();
    } else {
        status = calloutQueue->insert(reschedHandle,
                                      1,
                                      0,
                                      0,
                                      &reschedulesSelf,
                                      arg,
                                      "reschedules self",
                                      CalloutQueue::FlagsNone);
        assert(status == StatusOk);
        reschedCond.wait(&reschedMutex);
    }

    reschedMutex.unlock();
}

static Status
calloutTestEdge()
{
    CalloutQueue::Handle handle;
    Status status;
    int ret;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    status = calloutQueue->insert(&handle,
                                  1,
                                  0,
                                  1,
                                  &workCallback,
                                  &callbackArg,
                                  "bad params",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusInval);
    status = calloutQueue->insert(&handle,
                                  1,
                                  -1,
                                  1,
                                  &workCallback,
                                  &callbackArg,
                                  "bad params",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusInval);

    // Hack up an illegally frequent callout handle to test edge cases.
    status = calloutQueue->insert(&handle,
                                  1,
                                  1,
                                  100000,
                                  &workCallback,
                                  &callbackArg,
                                  "frequent",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    struct itimerspec frequent;
    frequent.it_interval.tv_sec = 0;
    frequent.it_interval.tv_nsec = NSecsPerMSec;  // Fires every millisecond.
    frequent.it_value.tv_sec = 0;
    frequent.it_value.tv_nsec = NSecsPerMSec;
    ret = timerfd_settime(handle.fd, TFD_TIMER_ABSTIME, &frequent, NULL);
    assert(ret == 0);
    sysSleep(1);
    calloutQueue->cancel(&handle);

    freesSelfHandle =
        (CalloutQueue::Handle *) memAlloc(sizeof(*freesSelfHandle));
    assert(freesSelfHandle != NULL);
    reschedHandle = (CalloutQueue::Handle *) memAlloc(sizeof(*reschedHandle));
    assert(reschedHandle != NULL);

    status = calloutQueue->insert(freesSelfHandle,
                                  1,
                                  0,
                                  0,
                                  &freesSelf,
                                  NULL,
                                  "frees self",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusOk);

    status = calloutQueue->insert(reschedHandle,
                                  1,
                                  0,
                                  0,
                                  &reschedulesSelf,
                                  &reschedCounter,
                                  "reschedules self",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusOk);

    reschedMutex.lock();
    reschedCond.wait(&reschedMutex);
    reschedMutex.unlock();

    return StatusOk;
}

static Status
calloutTestRecur()
{
    CalloutQueue::Handle handles[4];
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    progress = 0;

    status = calloutQueue->insert(&handles[0],
                                  1,
                                  1,
                                  2,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    sysSleep(3);
    assert(progress == 2);

    progress = 0;

    status = calloutQueue->insert(&handles[1],
                                  1,
                                  1,
                                  30,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[2],
                                  1,
                                  1,
                                  30,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[3],
                                  1,
                                  1,
                                  30,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    sysSleep(5);

    calloutQueue->cancel(&handles[1]);
    calloutQueue->cancel(&handles[2]);
    calloutQueue->cancel(&handles[3]);

    sysSleep(1);

    memBarrier();
    int progressVal = progress;
    assert(progress >= 3 * 4);
    assert(progress < 3 * 6);

    sysSleep(1);
    memBarrier();
    assert(progress == progressVal);

    return StatusOk;
}

static void
longRunningCallback(void *arg)
{
    sysSleep(3);
}

static Status
calloutTestParallelSequential()
{
    CalloutQueue::Handle handles[2];
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    status = calloutQueue->insert(&handles[0],
                                  1,
                                  1,
                                  10,
                                  &longRunningCallback,
                                  NULL,
                                  "Parallel",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);
    status = calloutQueue->insert(&handles[1],
                                  1,
                                  1,
                                  10,
                                  &longRunningCallback,
                                  NULL,
                                  "Sequential",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusOk);

    sysSleep(4);

    uint64_t execCountParallel = calloutQueue->cancel(&handles[0]);
    uint64_t execCountSequential = calloutQueue->cancel(&handles[1]);

    assert(execCountParallel >= execCountSequential);

    return StatusOk;
}

static CalloutQueue::Handle destroyHandles[2];
static Status
calloutTestDestroy()
{
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    // Should be able to call calloutQueueDestroy with handles still queued.
    status = calloutQueue->insert(&destroyHandles[0],
                                  1000,
                                  100,
                                  10,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    status = calloutQueue->insert(&destroyHandles[1],
                                  1000,
                                  100,
                                  10,
                                  &workCallback,
                                  &callbackArg,
                                  "",
                                  CalloutQueue::ParallelExecute);
    assert(status == StatusOk);

    // Delete same handle twice. Leave one hanging.
    assert(calloutQueue->cancel(&destroyHandles[0]) == 0);
    assert(calloutQueue->cancel(&destroyHandles[0]) == 0);

    return StatusOk;
}

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    Status status;

    status = InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode);
    assert(status == StatusOk);

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
