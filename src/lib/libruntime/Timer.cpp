// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <time.h>
#include "Timer.h"
#include "SchedObject.h"
#include "Thread.h"

Timer::Timer() {}

Timer::~Timer() {}

bool
Timer::prepareWait(SchedObject *schedObj, WaitArg *arg)
{
    assert(arg->timer.usecsRel > 0);
    uint64_t now = getCurrentUSecs();

    // Handle multiple Timer waits.
    if (schedObj->usecsAbsWake_ != 0) {
        schedObj->usecsAbsWake_ =
            xcMin(schedObj->usecsAbsWake_, now + arg->timer.usecsRel);
    } else {
        schedObj->usecsAbsWake_ = now + arg->timer.usecsRel;
    }

    return true;
}

void
Timer::wait(uint64_t usecsRel)
{
    SchedObject *schedObj = Thread::getRunningThread()->getRunningSchedObj();
    assert(schedObj != NULL);
    assert(schedObj->getState() == SchedObject::State::Running);

    Waitable *waitable = this;
    WaitArg arg;
    arg.timer.usecsRel = usecsRel;

    Waitable *waker;
    verify(schedObj->waitMultiple(&waitable, 1, &arg, &waker) ==
           StatusTimedOut);
    assert(waker == this);

    //
    // Welcome back! When suspendRunningSchedObj returns, the SchedObject has
    // been made runnable by a call to wakeExpired meaning the timer has
    // expired.
    //
}

// waitQ, for Timer, stores WaitBlocks in order of wake time. Do in-order
// insert.
void
Timer::enqueue(WaitBlock *block, WaitArg *arg)
{
    WaitBlock *cur;
    WaitBlock *prev;

    for (prev = NULL, cur = waitQHead_; cur != NULL;
         prev = cur, cur = cur->next) {
        if (block->schedObj->usecsAbsWake_ < cur->schedObj->usecsAbsWake_) {
            // Insert schedObj in between prev and cur.
            if (prev == NULL) {
                assert(cur == waitQHead_);
                block->prev = NULL;
                waitQHead_ = block;
            } else {
                prev->next = block;
                block->prev = prev;
            }
            block->next = cur;
            cur->prev = block;
            break;
        }
    }

    if (cur == NULL) {
        // Insert at end.
        assert(prev == waitQTail_);
        if (prev == NULL) {
            assert(waitQHead_ == NULL);
            waitQHead_ = block;
        } else {
            prev->next = block;
        }
        block->prev = prev;
        block->next = NULL;
        waitQTail_ = block;
    }
}

void
Timer::wakeExpired()
{
    while (true) {
        if (!waitQLock_.tryLock()) {
            // No sense in waiting for this lock. Do actual work.
            return;
        }

        // First, let's grab an expired schedObj, if there is one.

        // Is waitQ empty?
        if (waitQHead_ == NULL) {
            waitQLock_.unlock();
            break;
        }

        uint64_t now = getCurrentUSecs();
        if (waitQHead_->schedObj->usecsAbsWake_ > now) {
            waitQLock_.unlock();
            break;
        }

        // wake releases waitQLock_. Timer doesn't care about return value.
        wake(waitQHead_, StatusTimedOut);
    }
}

uint64_t
Timer::getCurrentUSecs() const
{
    struct timespec current;
    verify(clock_gettime(CLOCK_MONOTONIC, &current) == 0);
    uint64_t nsecs = current.tv_nsec + current.tv_sec * NSecsPerSec;
    return nsecs / NSecsPerUSec;
}

//  How long should before I need to wake up the next SchedObject?
uint64_t
Timer::getSleepUSecs() const
{
    if (!waitQLock_.tryLock()) {
        return LongSleepTimeUSec;
    }
    uint64_t sleepTime;
    uint64_t now = getCurrentUSecs();
    if (waitQHead_ != NULL) {
        uint64_t abs = waitQHead_->schedObj->usecsAbsWake_;
        if (abs > now) {
            sleepTime = abs - now;
        } else {
            sleepTime = 0;
        }
    } else {
        sleepTime = LongSleepTimeUSec;
    }
    waitQLock_.unlock();
    return sleepTime;
}
