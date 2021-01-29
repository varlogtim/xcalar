// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "SchedObject.h"
#include "Scheduler.h"
#include "runtime/Runtime.h"
#include "RuntimeStats.h"
#include "Thread.h"

SchedObject::SchedObject(Schedulable *schedulable, Scheduler *scheduler)
    :
#ifdef DEBUG
      notRunnableCount(0),
#endif
      state_(State::Created),
      schedulable_(schedulable),
      scheduler_(scheduler),
      wakeStatus_(StatusUnknown),
      waker_(NULL)
{
    memZero(&wakers_, sizeof(wakers_));
    for (size_t ii = 0; ii < WaitBlockCount; ii++) {
        waitBlocks_[ii].schedObj = this;
    }
}

SchedObject::~SchedObject()
{
    assert(state_ == State::Created || state_ == State::Done);
    state_ = State::Invalid;

    assertNotOnAnyWaitable();
}

void
SchedObject::assertNotOnAnyWaitable()
{
#ifdef DEBUG
    for (size_t ii = 0; ii < WaitBlockCount; ii++) {
        assert(waitBlocks_[ii].waitable == NULL);
    }
#endif
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods dealing with the runnable state of this SchedObject.
//

// Prepare for suspend by marking SchedObjectr as not runnable and preventing
// state changes by holding stateLock_.
void  // virtual
SchedObject::makeNotRunnable()
{
    stateLock_.lock();

    assert(state_ == State::Running);
    state_ = State::NotRunnable;
#ifdef DEBUG
    notRunnableCount++;
#endif

    getScheduler()->makeNotRunnable(this);

    // stateLock_ still held!
}

// Undoes the effect of makeNotRunnable if we're not proceeding with a suspend.
void  // virtual
SchedObject::rollbackNotRunnable()
{
#ifdef DEBUG
    assert(stateLock_.mine());
#endif
    assert(state_ == State::NotRunnable);

    state_ = State::Running;
    getScheduler()->dequeueNotRunnable(this);
    stateLock_.unlock();
}

void
SchedObject::enableStateChanges()
{
    stateLock_.unlock();
}

Status  // virtual
SchedObject::makeRunnable()
{
    assert(atomicRead32(&wakers_) == 0);
    auto guard = stateLock_.take();
    assert(state_ == State::NotRunnable || state_ == State::Created);

    State prevState = state_;
    state_ = State::Runnable;
    Status status = getScheduler()->enqueue(this, prevState);

    return status;
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods involved in waiting on one or more Waitables or waking up a waiting
// SchedObject.
//

// Only entry point for a SchedObject to wait on a waitable.
//
// Insert this SchedObject into the waitQs of one or more Waitables. Put it to
// sleep until it is woken by any of the Waitables. Will exit this function
// after being woken, returning the wake status.
//
// XXX While we currently wait for *any* Waitable to wake our waiter, it's also
//     possible to wait for *all* Waitables to do so. This isn't implemented
//     because there isn't a need for it yet.
Status SchedObject::waitMultiple(
    Waitable **waitables,  // Array of Waitables.
    size_t waitableCount,  // Length of array.
    WaitArg *args,         // One per Waitable. Any args.
    Waitable **wakerOut)   // Which Waitable woke us?
{
    assert(wakeStatus_ == StatusUnknown);
    assert(waker_ == NULL);
    assert(usecsAbsWake_ == 0);

    Status status = StatusOk;
    SchedObject *schedObj = Thread::getRunningThread()->getRunningSchedObj();
    assert(schedObj != NULL);
    assert(schedObj->getState() == SchedObject::State::Running);
    assert(schedObj == this);

    // Must be done before "visible" in any waitQ.
    schedObj->makeNotRunnable();

    // A wake can certainly happen in the middle of waitMultiple. This prevents
    // it from proceeding beyond incrementing wakers_.
    waitLock_.lock();

    for (size_t ii = 0; ii < waitableCount; ii++) {
        // Get an unused WaitBlock.
        WaitBlock *block = NULL;
        for (size_t jj = 0; jj < WaitBlockCount; jj++) {
            if (waitBlocks_[jj].waitable == NULL) {
                assert(waitBlocks_[jj].next == NULL);
                assert(waitBlocks_[jj].prev == NULL);
                block = &waitBlocks_[jj];
                break;
            }
        }

        // It's the responsibility of the Runtime as a whole to make sure we
        // always have enough WaitBlocks. This is why this functionality is
        // only exposed in a very controlled way to outside consumers.
        assert(block != NULL);

        if (!waitables[ii]->doWait(block, &args[ii])) {
            // Wait was short-circuited. We now become a waker.
            dequeueAll();
            waitLock_.unlock();
            schedObj->rollbackNotRunnable();
            if (wakerOut != NULL) {
                *wakerOut = waitables[ii];
            }
            usecsAbsWake_ = 0;
            return status;
        }
    }

    waitLock_.unlock();

    // We are blocked and will be suspended.
    Stopwatch watch;
    if (scheduler_ != nullptr) {
        StatsLib::statAtomicIncr64(scheduler_->numSuspendedSchedObjs_);
    }

    Thread::getRunningThread()->suspendRunningSchedObj();

    // Welcome back! Somebody has woken this SchedObject.

    // Update the associated scheduler's stats, if possible.
    if (scheduler_ != nullptr) {
        StatsLib::statAtomicDecr64(scheduler_->numSuspendedSchedObjs_);
        StatsLib::statAtomicAdd64(scheduler_->timeSuspendedSchedObjs_,
                                  watch.getCurElapsedNSecs() / 1000);

        auto stats = schedulable_->stats();
        if (stats != nullptr) {
            stats->incSuspendedTime(watch.getCurElapsedNSecs() / 1000);
            stats->incSuspensions();
        }
    }

    // update the waker stats
    waker_->setSuspendedTime(watch.getCurElapsedNSecs() / 1000);
    waker_->incSuspensions();

    if (wakerOut != nullptr) *wakerOut = waker_;

    status = wakeStatus_;
    assert(status != StatusUnknown);
    wakeStatus_ = StatusUnknown;  // Reset.
    waker_ = nullptr;

    usecsAbsWake_ = 0;
    return status;
}

// Wake this waiting SchedObject. This can be done by multiple Waitables at the
// same time. Only one will end up dequeuing the SchedObject from *all* waitQs.
// That Waitable will be considered the one that actually "woke" the
// SchedObject. Returns true on the thread that actually woke the SchedObject,
// false otherwise.
bool
SchedObject::wake(Status wakeStatus, Waitable *waker)
{
    waitLock_.lock();

    bool woke = dequeueAll();

    waitLock_.unlock();

    // This can no longer increase because schedObj cannot be reached by any
    // waitQs.
    assert(atomicRead32(&wakers_) != 0);
    atomicDec32(&wakers_);

    // If woke == false, we cannot touch the SchedObject beyond this point,
    // since it may probably already be on the runtime FIFO already.

    if (!woke) {
        return false;
    }

    // Must wait for all threads to stop trying to wake this SchedObject. We
    // cannot allow the SchedObject to continue until this is true because,
    // otherwise, this may race with a subsequent wait.
    while (atomicRead32(&wakers_) != 0) {
    }

    waker_ = waker;
    wakeStatus_ = wakeStatus;

    return true;
}

// Notify potential racing wakers that some Waitable wants to wake this
// SchedObject.
void
SchedObject::incWakers()
{
    atomicInc32(&wakers_);
}

// Dequeue this SchedObject from all of the waitQs it's waiting on. Returns true
// if dequeued from at least one waitQ (this helps us tell if we're racing
// with another waker).
bool
SchedObject::dequeueAll()
{
#ifdef DEBUG
    assert(waitLock_.mine());
#endif

    bool dequeuedOnce = false;

    for (size_t ii = 0; ii < WaitBlockCount; ii++) {
        if (waitBlocks_[ii].waitable != NULL) {
            dequeuedOnce = true;
            waitBlocks_[ii].waitable->dequeue(&waitBlocks_[ii]);
        }

        assert(waitBlocks_[ii].next == NULL);
        assert(waitBlocks_[ii].prev == NULL);
    }

    return dequeuedOnce;
}
