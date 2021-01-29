// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "runtime/CondVar.h"
#include "SchedObject.h"
#include "Thread.h"
#include "runtime/Mutex.h"

CondVar::CondVar() : isBroadcasting_(false) {}

CondVar::~CondVar() {}

// Does actual wait - agnostic of what lock type we're using.
void
CondVar::waitInternal(WaitArg *arg)
{
    SchedObject *schedObj = Thread::getRunningThread()->getRunningSchedObj();
    assert(schedObj != NULL);

    Waitable *waitable = this;
    Waitable *waker;
    verifyOk(schedObj->waitMultiple(&waitable, 1, arg, &waker));
    assert(waker == this);
}

void
CondVar::wait(Mutex *mutex)
{
    // WaitArg allows us to unlock given lock only once we're visible in condvar
    // queue.
    WaitArg arg;
    arg.condvar.lType = WaitArg::LockType::Mutex;
    arg.condvar.lockPtr = (void *) mutex;

    waitInternal(&arg);

    mutex->lock();
}

void
CondVar::wait(Spinlock *slock)
{
    // WaitArg allows us to unlock given lock only once we're visible in condvar
    // queue.
    WaitArg arg;
    arg.condvar.lType = WaitArg::LockType::Spinlock;
    arg.condvar.lockPtr = (void *) slock;

    waitInternal(&arg);

    slock->lock();
}

void
CondVar::wait()
{
    WaitArg arg;
    arg.condvar.lType = WaitArg::LockType::None;
    arg.condvar.lockPtr = NULL;

    waitInternal(&arg);
}

void
CondVar::signal()
{
    while (true) {
        waitQLock_.lock();

        if (waitQHead_ == NULL) {
            // Nobody waiting, increment value for next waiter.
            assert(waitQTail_ == NULL);
            waitQLock_.unlock();
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

void
CondVar::broadcast()
{
    waitQLock_.lock();

    unsigned count = 0;

    if (isBroadcasting_) {
        waitQLock_.unlock();
        return;
    }
    isBroadcasting_ = true;

    WaitBlock *originalTail = waitQTail_;

    while (waitQHead_ != NULL) {
        WaitBlock *current = waitQHead_;

        // Don't care about return. It just tells use whether we were the waker.
        (void) wake(waitQHead_, StatusOk);
        waitQLock_.lock();
        count += 1;

        // This, along with isBroadcasting, prevents us from waking waiters who
        // arrive during broadcast.
        if (current == originalTail) {
            break;
        }
    }

    isBroadcasting_ = false;
    waitQLock_.unlock();
}

void
CondVar::enqueue(WaitBlock *block, WaitArg *arg)
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

    // Since we're now visible in the condvar queue, safe to unlock.
    if (arg->condvar.lockPtr != NULL) {
        switch (arg->condvar.lType) {
        case WaitArg::LockType::Mutex:
            reinterpret_cast<Mutex *>(arg->condvar.lockPtr)->unlock();
            break;
        case WaitArg::LockType::Spinlock:
            reinterpret_cast<Spinlock *>(arg->condvar.lockPtr)->unlock();
            break;
        default:
            assert(0 && "Unknown lock type");
            break;
        }
    }
}
