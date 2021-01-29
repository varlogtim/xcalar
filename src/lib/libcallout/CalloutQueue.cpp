// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>

#include "callout/Callout.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "bc/BufferCache.h"
#include "util/Atomics.h"
#include "stat/Statistics.h"
#include "util/IntHashTable.h"
#include "runtime/Runtime.h"

CalloutQueue *CalloutQueue::calloutQueue = NULL;
const char *CalloutQueue::moduleName = "libcallout";
bool CalloutQueue::calloutIsInit = false;

Status
CalloutQueue::createSingleton()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(CalloutQueue), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    CalloutQueue::calloutQueue = new (ptr) CalloutQueue();

    status = CalloutQueue::calloutQueue->createInternal();

    if (status != StatusOk) {
        if (ptr != NULL) {
            CalloutQueue::calloutQueue->~CalloutQueue();
            memFree(ptr);
            CalloutQueue::calloutQueue = NULL;
        }
    }

    return status;
}

CalloutQueue *
CalloutQueue::get()
{
    assert(calloutQueue);
    return calloutQueue;
}

// Removes handle from hashtable and clears related entries. Caller must be
// holding lock for that handle. May be called multiple times.
void
CalloutQueue::calloutQueueDestroyHandle(CalloutQueue::Handle *handle)
{
    if (handle->state == CalloutQueue::StateDestroyed) {
        return;
    }

    calloutHandles.remove(handle->calloutId);

    if (handle->fd != -1) {
        struct epoll_event bogusEvent;
        int ret;

        errno = 0;
        ret = epoll_ctl(epollFd, EPOLL_CTL_DEL, handle->fd, &bogusEvent);
        if (ret != 0) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete fd %d from epoll while destroying "
                    "handle: %s",
                    handle->fd,
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }

        errno = 0;
        ret = close(handle->fd);
        if (ret != 0) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close fd %d while destroying handle: %s",
                    handle->fd,
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }
        handle->fd = -1;
    }

    handle->state = CalloutQueue::StateDestroyed;

    pthread_cond_destroy(&handle->cancellable);

    StatsLib::statNonAtomicDecr(statCalloutHandleActiveCount);
}

// Performs a single callback execution on a worker thread and performs any
// state transitions that must happen after execution is complete.
void
CalloutQueue::CalloutDispatch::run()
{
    CalloutQueue::Handle *handle = this->handle;
    CalloutQueue *calloutQueue = CalloutQueue::get();
    bool earlyCleanup = false;

    //
    // Attempt early cleanup. If this is the last execution of this callout,
    // destroy the callout handle before executing so the invoked callback can
    // safely free the callout handle memory.
    //

    calloutQueue->calloutHandleLock();

    if (handle->refCountExecute == 1 &&  // We're the only executer.
        handle->executeCount + 1 >= handle->recurCountOriginal &&  // Last time.
        handle->state != CalloutQueue::StateSoftCancel) {
        calloutQueue->calloutQueueDestroyHandle(handle);
        earlyCleanup = true;

        // Do this prematurely to avoid accessing handle after invoking
        // callback.
        handle->refCountExecute--;
        handle->executeCount++;

        // Drop any deferred executions since recurCount was reached.
        if (handle->refCountDefer > 0) {
            StatsLib::statNonAtomicSubtr(calloutQueue->statCalloutsDeferred,
                                         handle->refCountDefer);
            handle->refCountDefer = 0;
        }
    }

    calloutQueue->calloutHandleUnlock();

    //
    // Execute callback.
    //

    handle->callback(handle->args);

    if (earlyCleanup) {
        return;
    }

    calloutQueue->calloutHandleLock();

    // Mark this execution as complete.
    handle->refCountExecute--;
    handle->executeCount++;

    //
    // Figure out if any further action must be initiated on this callout.
    //

    // Save callout ID on stack in case callout handle goes away before it can
    // be dispatched again.
    uint64_t calloutId = handle->calloutId;

    // If there's deferred dispatches, this function should perform the next.
    bool dispatchAgain = false;
    bool expired = false;

    if (handle->executeCount >= handle->recurCountOriginal) {
        // Callout handle has expired.
        if (handle->refCountDefer > 0) {
            StatsLib::statNonAtomicSubtr(calloutQueue->statCalloutsDeferred,
                                         handle->refCountDefer);
            handle->refCountDefer = 0;
        }
        expired = true;
    }

    if (handle->refCountExecute == 0) {
        if (handle->state == CalloutQueue::StateSoftCancel) {
            // Wake cancel thread(s) to destroy handle. Expire + cancel is a
            // special case in which the cancel threads must be allowed to carry
            // out the destroy (instead of this thread) to avoid destroying the
            // condition variable their sitting on (hence the "else if" below).
            pthread_cond_broadcast(&handle->cancellable);
        } else if (expired) {
            calloutQueue->calloutQueueDestroyHandle(handle);
        } else if (handle->refCountDefer > 0) {
            // Queue next execution.
            handle->refCountDefer--;
            StatsLib::statNonAtomicDecr(calloutQueue->statCalloutsDeferred);
            dispatchAgain = true;
        }
    }

    calloutQueue->calloutHandleUnlock();

    if (dispatchAgain) {
        calloutQueue->calloutDispatchById(calloutId);
    }
}

void
CalloutQueue::CalloutDispatch::done()
{
    this->~CalloutDispatch();
    calloutQueue->bcCalloutDispatch->freeBuf(this);
}

void
CalloutQueue::calloutDispatchById(uint64_t calloutId)
{
    calloutHandleLock();

    // First, check if callout still exists in the hashtable or has been freed
    // from the system.
    CalloutQueue::Handle *handle = calloutHandles.find(calloutId);

    if (handle == NULL) {
        // Callout no longer in system.
        calloutHandleUnlock();
        return;
    }

    // Next, check the current execution state of the callout and decide what
    // should happen on this timer fire.
    if (handle->state == CalloutQueue::StateSoftCancel) {
        // If in process of cancelling this handle, do not dispatch it.
        calloutHandleUnlock();

    } else if (handle->recurCountOriginal != 0 &&
               handle->executeCount + handle->refCountExecute +
                       handle->refCountDefer >=
                   handle->recurCountOriginal) {
        // Already have enough executions. Discard this one (but don't soft
        // cancel because there's no cancel thread available).
        calloutHandleUnlock();

    } else if (!(handle->flags & CalloutQueue::ParallelExecute) &&
               handle->refCountExecute > 0) {
        // Cannot execute callout multiple times in parallel. Defer this call
        // until the in-progress one completes.
        handle->refCountDefer++;
        StatsLib::statNonAtomicIncr(statCalloutsDeferred);
        calloutHandleUnlock();

    } else {
        // Execute callback on work queue.
        handle->refCountExecute++;
        calloutHandleUnlock();

        CalloutDispatch *dispatch;

        dispatch = (CalloutDispatch *) bcCalloutDispatch->allocBuf(XidInvalid);
        if (dispatch == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to dispatch %s.",
                    handle->description);
        } else {
            new (dispatch) CalloutDispatch();
            dispatch->handle = handle;
            Txn newTxn = Txn::newTxn(Txn::Mode::NonLRQ);
            Txn::setTxn(newTxn);
            Status status = Runtime::get()->schedule(dispatch);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to dispatch %s. status %s",
                        handle->description,
                        strGetFromStatus(status));
            }
            Txn::setTxn(Txn());
        }
    }
}

// Entry point of processing thread. calloutQueue::insert registers each callout
// handle with epollFd. epoll_wait, used here, will wait for one of their fds to
// become readable, meaning the timer has expired. epoll_event data stores the
// calloutId of the expired handle which is used to identify the callout handle.
// Function will return immediately if deleteSingleton has been invoked
// after any "Running" dispatches have been processed.
void *
CalloutQueue::calloutQueueProcess(void *ignored)
{
    CalloutQueue *calloutQueue = CalloutQueue::get();

    while (calloutQueue->processingThreadAlive) {
        int ret;
        size_t i;
        struct epoll_event events[MaxEpollEvents];

        //
        // First, build up list of handles ready for dispatch.
        //

        // Dispatch list stores the callout IDs and execution counts of each
        // callout handle to be dispatched.
        uint64_t dispatchListCalloutId[MaxEpollEvents];
        unsigned dispatchListExecCount[MaxEpollEvents];
        size_t dispatchListCount = 0;

        ret = epoll_wait(calloutQueue->epollFd, events, MaxEpollEvents, -1);
        if (ret < 0) {
            if (errno != EINTR) {
                xSyslog(moduleName,
                        XlogErr,
                        "epoll_wait failed: %s",
                        strerror(errno));
            }

            continue;
        }

        memBarrier();
        if (!calloutQueue->processingThreadAlive) {
            break;
        }

        calloutQueue->calloutHandleLock();

        for (i = 0; i < (size_t) ret; i++) {
            // This lookup is necessary because handles can be removed between
            // epoll_wait and listLock.

            // Get callout handle and requested execute count (timer may have
            // fired multiple times).
            CalloutQueue::Handle *handle =
                calloutQueue->calloutHandles.find(events[i].data.u64);

            if (handle == NULL) {
                continue;
            }

            uint64_t executeCount = 0;
            ssize_t readResult =
                read(handle->fd, &executeCount, sizeof(executeCount));
            if (readResult < 0) {
                // Failed to read from fd. This should be very rare, since
                // this isn't an actual file, but can happen under resource
                // constraints.
                assert(executeCount == 0);
                xSyslog(moduleName,
                        XlogCrit,
                        "Failed to dispatch callout handle: %s",
                        strerror(errno));
                continue;
            }

            dispatchListCalloutId[dispatchListCount] = handle->calloutId;
            dispatchListExecCount[dispatchListCount] = (unsigned) executeCount;
            dispatchListCount++;
        }

        calloutQueue->calloutHandleUnlock();

        //
        // Actually perform each dispatch.
        //

        for (i = 0; i < dispatchListCount; i++) {
            for (unsigned execCount = 0; execCount < dispatchListExecCount[i];
                 execCount++) {
                calloutQueue->calloutDispatchById(dispatchListCalloutId[i]);
            }
        }

        memBarrier();
    }

    return NULL;
}

Status
CalloutQueue::calloutInitStats()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("callout", &statGroupId, 5);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&statCalloutHandleActiveCount);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&statCalloutHandleActiveCountMaxReached);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&statCalloutsDeferred);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&statCancelWaitTimeMsAverageNumerator);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&statCancelWaitTimeMsAverageDenominator);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId,
                                         "handles.active",
                                         statCalloutHandleActiveCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId,
                                         "handles.maxActiveReached",
                                         statCalloutHandleActiveCountMaxReached,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId,
                                         "deferred",
                                         statCalloutsDeferred,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId,
                                         "cancelWaitTime.average.numeratorMs",
                                         statCancelWaitTimeMsAverageNumerator,
                                         StatDouble,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId,
                                         "cancelWaitTime.average.denominatorMs",
                                         statCancelWaitTimeMsAverageDenominator,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    // XXX Cleanup on failure.
    return status;
}

// Initialize and begin processing thread.
Status
CalloutQueue::createInternal()
{
    Status status;
    struct epoll_event stopProcessingEvent;
    bool calloutLockInit = false;

    // Used to cleanup after failure.
    epollFd = -1;
    stopProcessingFd = -1;
    assert(bcCalloutDispatch == NULL);

    assert(!calloutIsInit);

    status = calloutInitStats();
    BailIfFailed(status);

    bcCalloutDispatch = BcHandle::create(BufferCacheObjects::CalloutDispatch);
    BailIfNull(bcCalloutDispatch);

    // @SymbolCheckIgnore
    verify(pthread_mutex_init(&calloutHandleHTLock, NULL) == 0);
    calloutLockInit = true;

    epollFd = epoll_create1(0);
    if (epollFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    stopProcessingFd = eventfd(0, 0);
    if (stopProcessingFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    stopProcessingEvent.events = EPOLLIN;
    stopProcessingEvent.data.u64 = 0;

    if (epoll_ctl(epollFd,
                  EPOLL_CTL_ADD,
                  stopProcessingFd,
                  &stopProcessingEvent) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    processingThreadAlive = true;
    status = Runtime::get()->createBlockableThread(&processingThread,
                                                   NULL,
                                                   calloutQueueProcess,
                                                   NULL);

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize callout queue: %s.",
                strGetFromStatus(status));

        if (stopProcessingFd != -1) {
            close(stopProcessingFd);
        }
        if (epollFd != -1) {
            close(epollFd);
        }
        if (calloutLockInit) {
            verify(pthread_mutex_destroy(&calloutHandleHTLock) == 0);
        }
        if (bcCalloutDispatch != NULL) {
            BcHandle::destroy(&bcCalloutDispatch);
        }
    } else {
        calloutIsInit = true;
    }

    return status;
}

void
CalloutQueue::Handle::close()
{
    ::close(this->fd);
}

// Stop procressing thread and destroy any unprocessed callout handles. Returns
// number of cancelled callout handles.
void
CalloutQueue::deleteSingleton()
{
    ssize_t ret;
    uint64_t dummyValue;

    if (!calloutIsInit) {
        return;
    }

    CalloutQueue *calloutQueue = CalloutQueue::get();

    calloutIsInit = false;

    calloutQueue->processingThreadAlive = false;

    // Stop the processing thread.
    dummyValue = 1;  // stopProcessingFd is readable if dummyValue is > 0.
    // @SymbolCheckIgnore
    ret =
        write(calloutQueue->stopProcessingFd, &dummyValue, sizeof(dummyValue));
    assert(ret != -1);
    ret = sysThreadJoin(calloutQueue->processingThread, NULL);
    assert(ret == 0);
    verify(pthread_mutex_destroy(&calloutQueue->calloutHandleHTLock) == 0);
    calloutQueue->calloutHandles.removeAll(&Handle::close);

    close(calloutQueue->epollFd);

    BcHandle::destroy(&calloutQueue->bcCalloutDispatch);
    calloutQueue->bcCalloutDispatch = NULL;

    calloutQueue->~CalloutQueue();
    memFree(calloutQueue);
}

// Enqueue a callout to be executed seconds relative to the current time.
// Callout will recur every recurSeconds seconds (after the first execution)
// if recurSeconds is non-zero. Callout will not be executed more than
// recurCount times. Callout will execute in arbitrary thread context and be
// passed given args. By default, only one instance of the callout can be
// executing at once. CalloutQueue::ParallelExecute flag allows parallel
// execution.
Status
CalloutQueue::insert(CalloutQueue::Handle *handle,
                     long int seconds,
                     long int recurSeconds,
                     uint64_t recurCount,
                     void (*callback)(void *),
                     void *args,
                     const char *description,
                     CalloutQueue::Flags flags)
{
    Status status;

    // Used for cleanup.
    bool initCancellable = false;
    bool inHashTable = false;
    bool inEpoll = false;
    uint64_t currentCount;
    uint64_t maxCountReached;

    assert(calloutIsInit);

    //
    // Validation.
    //

    if (seconds < 0) {
        return StatusInval;
    }

    if (recurSeconds < 0) {
        return StatusInval;
    }

    if (recurCount > 0 && recurSeconds == 0) {
        return StatusInval;
    }

    //
    // Initialize members of CalloutQueue::Handle struct.
    //

    memZero(handle, sizeof(*handle));
    handle->callback = callback;
    handle->args = args;
    handle->description = description;
    handle->flags = flags;
    handle->recurCountOriginal = recurCount;
    handle->refCountExecute = 0;
    handle->refCountDefer = 0;
    handle->executeCount = 0;
    handle->state = CalloutQueue::StateNormal;

    handle->fd = -1;  // Used for cleanup.
    handle->fd = timerfd_create(clockId, 0);
    if (handle->fd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    int ret;
    ret = pthread_cond_init(&handle->cancellable, NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(ret);
        goto CommonExit;
    }
    initCancellable = true;

    // Get the absolute time at which this handle should be processed.
    if (clock_gettime(clockId, &handle->time.it_value) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    handle->time.it_value.tv_sec += seconds;
    handle->time.it_interval.tv_sec = recurSeconds;

    //
    // Insert into global data structures.
    //

    handle->calloutId = (uint64_t) atomicInc64(&calloutIdNext);
    calloutHandleLock();
    calloutHandles.insert(handle);
    calloutHandleUnlock();
    inHashTable = true;

    struct epoll_event event;
    event.events = EPOLLIN;
    event.data.u64 = handle->calloutId;

    if (epoll_ctl(epollFd, EPOLL_CTL_ADD, handle->fd, &event) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    inEpoll = true;

    //
    // Arm timer. After this step, the timer can fire.
    //

    if (timerfd_settime(handle->fd, TFD_TIMER_ABSTIME, &handle->time, NULL) !=
        0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    StatsLib::statNonAtomicIncr(statCalloutHandleActiveCount);

    // XXX Because this isn't atomic, it is only an approximation.
    currentCount = StatsLib::statReadUint64(statCalloutHandleActiveCount);
    maxCountReached =
        StatsLib::statReadUint64(statCalloutHandleActiveCountMaxReached);
    if (currentCount > maxCountReached) {
        StatsLib::statNonAtomicSet64(statCalloutHandleActiveCountMaxReached,
                                     currentCount);
    }

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to insert into callout queue: %s",
                strGetFromStatus(status));

        if (inHashTable) {
            calloutHandleLock();
            calloutHandles.remove(handle->calloutId);
            calloutHandleUnlock();
        }
        if (initCancellable) {
            pthread_cond_destroy(&handle->cancellable);
        }
        if (handle->fd != -1) {
            int ret;
            if (inEpoll) {
                // See BUGS in
                // http://man7.org/linux/man-pages/man2/epoll_ctl.2.html
                struct epoll_event bogusEvent;
                errno = 0;
                ret =
                    epoll_ctl(epollFd, EPOLL_CTL_DEL, handle->fd, &bogusEvent);
                if (ret != 0) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to delete fd %d from epoll during insert "
                            "(%s) while handling insert failure (%s)",
                            handle->fd,
                            strGetFromStatus(sysErrnoToStatus(errno)),
                            strGetFromStatus(status));
                }
                inEpoll = false;
            }

            errno = 0;
            ret = close(handle->fd);
            if (ret != 0) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to close fd %d during insert: %s",
                        handle->fd,
                        strGetFromStatus(sysErrnoToStatus(errno)));
            }
            handle->fd = -1;
        }

        assert(!inEpoll);
    }

    return status;
}

// Initiates cancel of callout by preventing any further callout instances from
// being scheduled. Must be called while holding lock.
void
CalloutQueue::calloutHandleSoftCancel(CalloutQueue::Handle *handle)
{
    struct epoll_event bogusEvent;
    int ret;

    assert(handle->state == CalloutQueue::StateNormal);
    assert(handle->fd != -1);

    errno = 0;
    ret = epoll_ctl(epollFd, EPOLL_CTL_DEL, handle->fd, &bogusEvent);
    if (ret != 0) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete fd %d from epoll during soft cancel: "
                "%s",
                handle->fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
    }

    errno = 0;
    ret = close(handle->fd);
    if (ret != 0) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to close fd %d during soft cancel: %s",
                handle->fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
    }

    handle->fd = -1;

    handle->state = CalloutQueue::StateSoftCancel;
}

// Cancels callout handle. Returns the number of times the callout has been
// invoked. The caller must guarantee that no other thread will attempt to free
// the callout handle while calloutQueue::cancel is being invoked. Waits until
// all invocations of the callout are complete (this is necessary so that, if
// any object acceessed by the callout is freed after cancel, that free will not
// race with a callout thread).
uint64_t
CalloutQueue::cancel(CalloutQueue::Handle *handle)
{
    struct timespec start;
    struct timespec end;

    assert(calloutIsInit);

    verify(clock_gettime(CLOCK_MONOTONIC, &start) == 0);

    if (handle->state == CalloutQueue::StateDestroyed) {
        // Once state is "destroyed", the handle is, effectively, frozen. No
        // changes will be made to it by other threads so particular fields
        // can be read without a lock.
        goto CommonExit;
    }

    calloutHandleLock();

    // Must check again. Could have changed while we were grabbing the lock.
    if (handle->state == CalloutQueue::StateDestroyed) {
        calloutHandleUnlock();
        return handle->executeCount;
    }

    if (handle->state == CalloutQueue::StateNormal) {
        calloutHandleSoftCancel(handle);
    }

    assert(handle->state == CalloutQueue::StateSoftCancel);

    while (handle->refCountExecute > 0) {
        pthread_cond_wait(&handle->cancellable, &calloutHandleHTLock);
    }

    if (handle->state != CalloutQueue::StateDestroyed) {
        calloutQueueDestroyHandle(handle);
    }

    calloutHandleUnlock();

CommonExit:
    verify(clock_gettime(CLOCK_MONOTONIC, &end) == 0);

    StatsLib::
        statNonAtomicAddDouble(statCancelWaitTimeMsAverageNumerator,
                               (double) clkGetElapsedTimeInNanosSafe(&start,
                                                                     &end) /
                                   (double) NSecsPerMSec);
    StatsLib::statNonAtomicIncr(statCancelWaitTimeMsAverageDenominator);

    return handle->executeCount;
}
