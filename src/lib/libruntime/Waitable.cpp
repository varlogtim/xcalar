// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "runtime/Waitable.h"
#include "SchedObject.h"
#include "util/Atomics.h"

//
// Methods involved in waiting on a particular Waitable.
//

// Called by SchedObject to place itself on this Waitable's waitQ. Returns false
// if wait is short-circuited, true otherwise.
bool
Waitable::doWait(WaitBlock *waitBlock, WaitArg *waitArg)
{
    waitQLock_.lock();
    if (!prepareWait(waitBlock->schedObj, waitArg)) {
        // Wait was short-circuited (e.g. wait on this waitable has completed).
        waitQLock_.unlock();
        return false;
    }

    waitBlock->waitable = this;
    enqueue(waitBlock, waitArg);

    waitQLock_.unlock();
    return true;
}

// Default implementation of prepare just does nothing and doesn't
// short-circuit wait.
bool
Waitable::prepareWait(SchedObject *schedObj, WaitArg *arg)
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods involved in a single Waitable trying to wake a SchedObject.
//

// Wake schedObj. SchedObject will handle the case where there's multiple
// wakers. Caller must be holding waitQLock_. Always unlocks waitQLock_
// before returning.
bool
Waitable::wake(WaitBlock *block, Status wakeStatus)
{
    assert(this == block->waitable);
    assert(block != NULL);
#ifdef DEBUG
    assert(waitQLock_.mine());
#endif

    // Make ourselves visible to our other wakers. This var will be checked
    // after it is made impossible for others to increment it by removing from
    // all waitQs.
    block->schedObj->incWakers();

    // To remove block from this->waitQ, we need to be holding this->waitQLock_
    // and schedObj->waitLock. schedObj->waitLock must be acquired first. Thus,
    // drop this->waitQLock_ (schedObj->wakers protects us from proceeding
    // too late).
    waitQLock_.unlock();

    bool woke = block->schedObj->wake(wakeStatus, this);
    if (woke) {
        verifyOk(block->schedObj->makeRunnable());
    }

    return woke;
}

// Remove from this waitQ.
void
Waitable::dequeue(WaitBlock *block)
{
    assert(this == block->waitable);
    waitQLock_.lock();

    if (block->prev == NULL) {
        assert(waitQHead_ == block);
        waitQHead_ = block->next;
    } else {
        block->prev->next = block->next;
    }

    if (block->next == NULL) {
        assert(waitQTail_ == block);
        waitQTail_ = block->prev;
    } else {
        block->next->prev = block->prev;
    }

    assert((waitQHead_ == NULL && waitQTail_ == NULL) ||
           (waitQHead_ != NULL && waitQTail_ != NULL));

    block->next = NULL;
    block->prev = NULL;
    block->waitable = NULL;

    waitQLock_.unlock();
}

bool
Waitable::checkWaiting()
{
    bool waiterExists;
    waitQLock_.lock();
    waiterExists = (waitQHead_ != NULL);
    waitQLock_.unlock();

    return waiterExists;
}

void
Waitable::incSuspensions()
{
    atomicInc64(&suspensionCount_);
}
void
Waitable::setSuspendedTime(int64_t us)
{
    atomicAdd64(&suspendedTimeus_, us);
}
int64_t
Waitable::getSuspensionsCount()
{
    return atomicRead64(&suspensionCount_);
}
int64_t
Waitable::getSuspendedTime()
{
    return atomicRead64(&suspendedTimeus_);
}
