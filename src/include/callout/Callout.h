// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CALLOUT_H_
#define _CALLOUT_H_

#include "primitives/Primitives.h"
#include "util/IntHashTable.h"
#include "bc/BufferCache.h"
#include "runtime/Schedulable.h"
#include "hash/Hash.h"

class CalloutQueue
{
  public:
    enum {
        Infinity = UINT64_MAX,
    };

    enum Flags {
        FlagsNone = 0,
        // Allow two or more instances of the same callout to execute in
        // parallel (default is to queue multiple instances behind one another).
        ParallelExecute = 1,
    };

    // Internal to libcallout. Mostly describes progress of destroying callout
    // handle.
    enum State {
        // "Normal" means a callout handle that can be scheduled and may or may
        // not be currently executing.
        StateNormal,
        // A "soft cancel" occurs when a client first requests a cancel. This
        // will prevent further instances from being scheduled but other
        // instances may still be running and the handle is not yet torn down.
        // This state must imply that a thread is waiting on
        // handle->cancellable.
        StateSoftCancel,
        // No longer executing, can no longer be scheduled, all resources
        // destroyed.
        StateDestroyed,
    };

    // Handle used to create/delete callouts.
    struct Handle {
        //
        // Members considered read-only except during construction/destruction.
        //

        void (*callback)(void *);  // Invoked on timer expiration.
        void *args;
        const char *description;      // User-supplied description string.
        struct itimerspec time;       // Absolute time to invoke callback.
        uint64_t recurCountOriginal;  // Number of times to execute.

        uint64_t calloutId;  // Uniquely identifies callout handle.

        int fd;  // timerfd for this handle.
        IntHashTableHook hook;

        uint64_t getCalloutId() const { return calloutId; };

        CalloutQueue::Flags flags;

        //
        // Members denoting current execution/cancel state.
        //

        CalloutQueue::State state;

        unsigned refCountExecute;  // Count of outstanding executions.
        unsigned refCountDefer;    // Deferred executions.
        uint64_t executeCount;     // Incremented every execution.

        pthread_cond_t cancellable;  // Signals once ready for destruction.

        void close();
    };

    Status insert(CalloutQueue::Handle *item,
                  long int seconds,
                  long int recurSeconds,
                  uint64_t recurCount,
                  void (*callback)(void *),
                  void *args,
                  const char *description,
                  CalloutQueue::Flags flags);

    uint64_t cancel(CalloutQueue::Handle *item);

    static Status createSingleton();
    static void deleteSingleton();
    static CalloutQueue *get();

  private:
    enum {
        InitCalloutDispatchCount = 16,  // # bc bufs used for dispatching.
        MaxEpollEvents = 32,  // Max timers to process per loop iteration.
        CalloutHandlesNumSlots = 251
    };

    // Struct used to dispatch a callout. Simply references the
    // CalloutQueue::Handle (which can't be used directly since there may be
    // more than one instance being dispatched at once).
    class CalloutDispatch : public Schedulable
    {
      public:
        CalloutDispatch() : Schedulable("CalloutDispatch") {}
        virtual ~CalloutDispatch() {}
        virtual void run();
        virtual void done();
        CalloutQueue::Handle *handle;
    };

    // static member
    static constexpr clockid_t clockId = CLOCK_MONOTONIC;
    static const char *moduleName;
    static CalloutQueue *calloutQueue;
    static bool calloutIsInit;

    // non static member

    // File descriptor passed to epoll to wait on the various timer fds.
    int epollFd;

    pthread_t processingThread;  // Executes calloutQueueProcess.
    bool processingThreadAlive;  // Ends execution of calloutQueueProcess once
                                 // false.
    int stopProcessingFd;        // Signals processing thread to check
                                 // processingThreadAlive.

    // calloutId -> CalloutQueue::Handle hashtable of all CalloutStatePending
    // callout handles.
    IntHashTable<uint64_t,
                 CalloutQueue::Handle,
                 &CalloutQueue::Handle::hook,
                 &CalloutQueue::Handle::getCalloutId,
                 CalloutHandlesNumSlots,
                 hashIdentity>
        calloutHandles;

    pthread_mutex_t calloutHandleHTLock;

    Atomic64 calloutIdNext;

    // Used for actual execution of callout handles.
    BcHandle *bcCalloutDispatch = NULL;

    // Global stats.
    StatGroupId statGroupId;
    StatHandle statCalloutHandleActiveCount;
    StatHandle statCalloutHandleActiveCountMaxReached;
    StatHandle statCalloutsDeferred;
    // Cancel wait time is best maintained as
    // (total wait times summed) / (count).
    StatHandle statCancelWaitTimeMsAverageNumerator;
    StatHandle statCancelWaitTimeMsAverageDenominator;

    // member method
    Status createInternal();
    void calloutDispatchById(uint64_t calloutId);
    void calloutQueueDestroyHandle(CalloutQueue::Handle *handle);

    Status calloutInitStats();

    void calloutHandleSoftCancel(CalloutQueue::Handle *handle);
    //
    // Functions for locking and unlocking callout handles currently use their
    // corresponding hash slot locks.
    //

    void calloutHandleLock()
    {
        verify(pthread_mutex_lock(&calloutHandleHTLock) == 0);
    }

    void calloutHandleUnlock()
    {
        verify(pthread_mutex_unlock(&calloutHandleHTLock) == 0);
    }

    // These remain static functions as they are passed as function pointers.
    static void *calloutQueueProcess(void *ignored);

  public:
    static constexpr uint64_t BcCalloutDispatchNumElems =
        InitCalloutDispatchCount;
    static constexpr uint64_t BcCalloutDispatchBufSize =
        sizeof(CalloutQueue::CalloutDispatch);
};
#endif
