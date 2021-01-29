// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <pthread.h>
#include "callout/Callout.h"
#include "runtime/Runtime.h"
#include "util/MemTrack.h"

#include "test/QA.h"

static constexpr const char *moduleName = "libcallout";

enum {
    NumThreads = 7,
    MaxWaitSeconds = 11,
    CalloutHandlesPerThread = 100,
};

//
// calloutTestThreads.
//

static volatile int progress;
static int callbackArg = 0;
static bool cancelThreadAlive;

static CalloutQueue::Handle
    calloutHandles[NumThreads * CalloutHandlesPerThread];
static bool calloutHandleAlive[NumThreads * CalloutHandlesPerThread];

static void
workCallback(void *value)
{
    assert(value == &callbackArg);
    __atomic_add_fetch(&progress, 1, __ATOMIC_RELAXED);
}

static void *
workProducer(void *threadIndexAsPtr)
{
    uint64_t threadIndex;
    unsigned i;
    unsigned indexToCancel;
    CalloutQueue::Handle handleToCancel;
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    threadIndex = (uint64_t) threadIndexAsPtr;
    indexToCancel = rand() % CalloutHandlesPerThread;

    for (i = 0; i < CalloutHandlesPerThread; i++) {
        status =
            calloutQueue
                ->insert(&calloutHandles[threadIndex * CalloutHandlesPerThread +
                                         i],
                         (rand() % MaxWaitSeconds) + 1,
                         0,
                         0,
                         &workCallback,
                         &callbackArg,
                         "",
                         CalloutQueue::ParallelExecute);
        assert(status == StatusOk);

        // Visible to cancel thread.
        calloutHandleAlive[threadIndex * CalloutHandlesPerThread + i] = true;
        memBarrier();

        if (i == indexToCancel) {
            // Add a "special" handle to cancel later. Cancel fails if this loop
            // takes longer than 10 seconds (it shouldn't).
            status = calloutQueue->insert(&handleToCancel,
                                          (rand() % MaxWaitSeconds) + 10,
                                          0,
                                          0,
                                          &workCallback,
                                          &callbackArg,
                                          "",
                                          CalloutQueue::ParallelExecute);
            assert(status == StatusOk);
        }
    }

    calloutQueue->cancel(&handleToCancel);

    return NULL;
}

// Stops cancelThread from executing.
static void
cancelThreadCanceller(void *cancelSwitch)
{
    *((bool *) cancelSwitch) = false;
}

static void *
cancelThread(void *ignored)
{
    CalloutQueue::Handle canceller;
    unsigned i;
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    // Create callout handle to cancel self.
    status = calloutQueue->insert(&canceller,
                                  MaxWaitSeconds,
                                  0,
                                  0,
                                  &cancelThreadCanceller,
                                  &cancelThreadAlive,
                                  "",
                                  CalloutQueue::FlagsNone);
    assert(status == StatusOk);

    while (cancelThreadAlive) {
        for (i = 0; i < NumThreads * CalloutHandlesPerThread; i++) {
            // Is this callout handle active?
            memBarrier();
            if (calloutHandleAlive[i]) {
                calloutHandleAlive[i] = false;
                calloutQueue->cancel(&calloutHandles[i]);
            }
        }

        sysSleep(1);
        memBarrier();
    }

    return NULL;
}

Status
calloutTestThreads()
{
    Status status;

    uint64_t i;
    pthread_t threads[NumThreads];

    progress = 0;
    memZero(calloutHandleAlive, sizeof(calloutHandleAlive));

    for (i = 0; i < NumThreads; i++) {
        status = Runtime::get()->createBlockableThread(&threads[i],
                                                       NULL,
                                                       workProducer,
                                                       (void *) i);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    sysSleep(MaxWaitSeconds + 1);

    for (i = 0; i < NumThreads; i++) {
        sysThreadJoin(threads[i], NULL);
    }

    assert(progress == NumThreads * CalloutHandlesPerThread);

    // Interleave cancels with inserts.
    memZero(calloutHandles, sizeof(calloutHandles));
    memZero(calloutHandleAlive, sizeof(calloutHandleAlive));

    pthread_t cancel;
    cancelThreadAlive = true;
    memBarrier();
    status = Runtime::get()->createBlockableThread(&cancel,
                                                   NULL,
                                                   &cancelThread,
                                                   NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "createBlockableThread failed: %s",
                strGetFromStatus(status));
    }
    assert(status == StatusOk);

    for (i = 0; i < NumThreads; i++) {
        status = Runtime::get()->createBlockableThread(&threads[i],
                                                       NULL,
                                                       workProducer,
                                                       (void *) i);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    sysSleep(MaxWaitSeconds + 1);

    for (i = 0; i < NumThreads; i++) {
        sysThreadJoin(threads[i], NULL);
    }
    sysThreadJoin(cancel, NULL);

    return StatusOk;
}

////////////////////////////////////////////////////////////////////////////////

//
// calloutTestCancelStarvation.
//

static const int argValue = 77;
// Dereferences the arg it's passed. Should segfault if arg has been freed.
static void
argDereferencer(void *arg)
{
    volatile int **asInt = (volatile int **) arg;
    **asInt = argValue;
}

Status
calloutTestCancelStarvation()
{
    static const size_t CountStarveHandles = 128;
    static const unsigned StarveIterations = 4;
    Status status;
    CalloutQueue *calloutQueue = CalloutQueue::get();

    srand(44);  // Use static seed for determinism.

    for (unsigned iter = 0; iter < StarveIterations; iter++) {
        CalloutQueue::Handle *handles[CountStarveHandles];
        int *args[CountStarveHandles];
        size_t i;

        for (i = 0; i < CountStarveHandles; i++) {
            handles[i] = (CalloutQueue::Handle *) memAlloc(sizeof(*handles[0]));
            assert(handles[i] != NULL);

            args[i] = (int *) memAlloc(sizeof(*args[i]));
            assert(args[i] != NULL);
        }

        for (i = 0; i < CountStarveHandles; i++) {
            status = calloutQueue->insert(handles[i],
                                          1,
                                          1,
                                          60 * 60 * 24 * 7,
                                          &argDereferencer,
                                          &args[i],
                                          "",
                                          CalloutQueue::ParallelExecute);
            assert(status == StatusOk);
        }

        sysSleep(rand() % 3 + 2);

        clock_t cancelStart = clock();
        for (i = 0; i < CountStarveHandles; i++) {
            uint64_t numExecuted = calloutQueue->cancel(handles[i]);
            assert(numExecuted > 0);
            assert(*args[i] == argValue);  // Run at least once.

            // Try to cause fault if args or callout structs are accessed after
            // cancel.
            memZero(handles[i], sizeof(*handles[i]));
            memZero(args[i], sizeof(*args[i]));

            memFree(handles[i]);
            memFree(args[i]);
        }
        double duration = (double) (clock() - cancelStart) / CLOCKS_PER_SEC;

        printf("Cancel callout handles duration: %f\n", (float) duration);
    }

    return StatusOk;
}

////////////////////////////////////////////////////////////////////////////////
