// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "runtime/Semaphore.h"
#include "Thread.h"
#include "runtime/Runtime.h"
#include "Timer.h"
#include "SchedObject.h"
#include "msg/Message.h"

Semaphore::Semaphore(uint64_t initialValue) : Waitable(), value_(initialValue)
{
    state_ = State::Init;
}

Semaphore::Semaphore() : Semaphore(InvalidValue)
{
    state_ = State::PreInit;
}

Semaphore::~Semaphore()
{
    assert(waitQHead_ == NULL);
    assert(waitQTail_ == NULL);
    state_ = State::Destructed;
}

void
Semaphore::init(uint64_t initialValue)
{
    assert(state_ != State::Init);
    state_ = State::Init;
    value_ = initialValue;
}

void
#ifdef DEBUG_SEM
Semaphore::wait(const char *fileNameIn,
                const char *funcNameIn,
                int lineNumberIn)
#else
Semaphore::wait()
#endif
{
    assert(state_ == State::Init);

    uint64_t counter = 1 << 10;
    while (counter--) {
        if (value_ && tryWaitInt()) {
            // got the semaphore token
            return;
        }
    }

    // Didn't get the token, going to wait.
    SchedObject *schedObj = Thread::getRunningThread()->getRunningSchedObj();
    assert(schedObj != NULL);

    Waitable *waitable = this;
    WaitArg arg;
    Waitable *waker;
    verifyOk(schedObj->waitMultiple(&waitable, 1, &arg, &waker));
    assert(waker == this);
}

Status
Semaphore::timedWait(uint64_t usecsRel)
{
    assert(state_ == State::Init);

    if (usecsRel == 0) {
        return StatusInval;
    }

    SchedObject *schedObj = Thread::getRunningThread()->getRunningSchedObj();
    assert(schedObj != NULL);

    // Construct args to waitMultiple (waiting on Timer and this Semaphore).
    Waitable *waitables[2] = {this,
                              Runtime::get()->getTimer(
                                  Txn::currentTxn().rtSchedId_)};
    WaitArg args[2];
    args[1].timer.usecsRel = usecsRel;

    Status retStatus = schedObj->waitMultiple(waitables, 2, args, NULL);
    assert(retStatus == StatusOk || retStatus == StatusTimedOut);
    return retStatus;
}

bool
Semaphore::prepareWait(SchedObject *schedObj, WaitArg *arg)
{
#ifdef DEBUG
    assert(waitQLock_.mine());
#endif

    if (value_ > 0) {
        value_--;
        // Don't wait. We've already got in.
        return false;
    }
    return true;
}

void
Semaphore::enqueue(WaitBlock *block, WaitArg *arg)
{
    // Simply insert block at tail of waitQ.
    block->next = NULL;
    block->prev = waitQTail_;

    if (waitQTail_ == NULL) {
        assert(waitQHead_ == NULL);
        waitQHead_ = block;
    } else {
        waitQTail_->next = block;
    }
    waitQTail_ = block;
}

bool
Semaphore::tryWait()
{
    return tryWaitInt();
}

// Not signal-handler safe.
void
Semaphore::post()
{
    assert(state_ == State::Init);

    while (true) {
        waitQLock_.lock();

        if (waitQHead_ == NULL) {
            // Nobody waiting, increment value for next waiter.
            assert(waitQTail_ == NULL);
            value_++;
            waitQLock_.unlock();
            memBarrier();
            break;
        }

        // wake releases waitQLock_.
        if (wake(waitQHead_, StatusOk)) {
            // We were the ones to wake this schedObj! Our slot has been passed
            // to him (or her) implicitly.
            break;
        }
    }
}
