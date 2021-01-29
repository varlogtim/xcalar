// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SCHEDOBJECT_H
#define SCHEDOBJECT_H

#include "primitives/Primitives.h"
#include "WaitBlock.h"
#include "runtime/Semaphore.h"
#include "runtime/Spinlock.h"
#include "util/AtomicTypes.h"

class Schedulable;
class Fiber;
class Scheduler;

//
// Unit of work in the Runtime. Scheduled by Scheduler on FiberSchedThreads.
// Execution can be suspended by waiting on a "Waitable" (e.g. Semaphore, Timer,
// etc.).
//

class SchedObject
{
  public:
    // XXX This limit is set to 9 primarily to allow more thorough testing.
    // Because the only multi-waiter is Semaphore::timedWait, we, at most,
    // will only need 2 blocks.
    static constexpr size_t WaitBlockCount = 9;

    enum class State {
        Created = 10,  // Constructor called, not yet run.
        Runnable,      // Scheduler can now run this.
        Running,       // Currently executing 'run()'.
        NotRunnable,   // Cannot be run until something occurs to make Runnable.
        Done,          // Finished executing 'run()'.
        Invalid,       // Destructed.
    };

    // Except in the case of DedicatedThreads, there's a one-to-one
    // correspondence between SchedObjects and Schedulables. Schedulables can be
    // thought of as the publicly exposed aspect of a SchedObject.
    Schedulable *getSchedulable() { return schedulable_; }

    Scheduler *getScheduler() { return scheduler_; }

    SchedObject(Schedulable *schedulable, Scheduler *scheduler);
    virtual ~SchedObject();

    //
    // Methods dealing with runnable state of this SchedObject.
    //

    State getState() const { return state_; }

    void setState(State state)
    {
        auto guard = stateLock_.take();
        state_ = state;
    }

    // Methods used during suspend/resume.
    virtual Status makeRunnable();
    virtual void makeNotRunnable();
    virtual void rollbackNotRunnable();
    void enableStateChanges();

    //
    // Methods involed in waiting on one or more Waitables or waking a waiting
    // SchedObject.
    //

    Status waitMultiple(Waitable **waitables,
                        size_t waitableCount,
                        WaitArg *args,
                        Waitable **wakerOut);
    bool wake(Status wakeStatus, Waitable *waker);
    void incWakers();
    void markToAbort();
    bool isMarkedToAbort();

    //
    // Public fields of SchedObject, often owned by other parts of the Runtime.
    //

    SchedObject *nextRunnable_ = nullptr;  // Owned by Scheduler.
    SchedObject *nextRunnableFiber_ = nullptr;
    SchedObject *nextNotRunnable_ = nullptr;  // Owned by Scheduler.
    SchedObject *prevNotRunnable_ = nullptr;  // Owned by Scheduler.

    uint64_t usecsAbsWake_ = 0;  // Owned by Timer.

    // SchedObject will be assigned a Fiber when run. It will be bound to that
    // Fiber, regardless of suspend/resumes, until done.
    Fiber *fiber_ = nullptr;

    //
    // Fields used for debugging purposes.
    //

#ifdef DEBUG_SEM
    // Fields populated if waiting on sem.
    Semaphore *sem;
    const char *fileName;
    const char *funcName;
    int lineNumber;
#endif

    void assertNotOnAnyWaitable();

#ifdef DEBUG
  protected:
    // Number of times this SchedObject was made not runnable.
    uint64_t notRunnableCount;
#endif

  private:
    bool dequeueAll();

    // Controls access to SchedObject state. Holding this will prevent other
    // threads from making this SchedObject runnable. Typically, access to
    // state_ is only done by the thread/Fiber executing the SchedObject. We
    // can encounter contention once the SchedObject is placed on a Scheduler's
    // queue.
    mutable Spinlock stateLock_;
    State state_;

    Schedulable *schedulable_;
    Scheduler *scheduler_;

    // Wait/wake state.
    mutable Spinlock waitLock_;
    Atomic32 wakers_;
    Status wakeStatus_;
    Waitable *waker_;
    WaitBlock waitBlocks_[WaitBlockCount];
};

#endif  // SCHEDOBJECT_H
