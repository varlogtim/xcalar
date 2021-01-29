// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "runtime/Mutex.h"
#include "util/Atomics.h"
#include "util/System.h"
#include "Thread.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "Mutex";

Mutex::Mutex() : initState_(InitState::Inited), sem_(0)
{
    lockval_.ctr = 0;
    lockval_.state = Unlocked;
    owner_ = NULL;
}

Mutex::~Mutex()
{
    memBarrier();

    assert(initState_ == InitState::Inited);
    assert(owner_ == NULL && "Destructing lock while in-use");
    assert(lockval_.state == Unlocked);

    lockval_.ctr = 0;
    lockval_.state = Unlocked;
    initState_ = InitState::Destructed;
}

void
Mutex::lock()
{
    assert(initState_ == InitState::Inited);
    AtomicLockField oldValueDirtyRead, newValue, oldValueReturned;
    bool done = false;
    uint64_t loopCount;
    static const uint64_t LoopCountThresh = 1 << (10);

    while (!done) {
        // Dirty bit field read
        loopCount = 0;
        while (lockval_.state == Locked && ++loopCount < LoopCountThresh) {
            // Empty loop body intentional
        }

        oldValueDirtyRead.val = lockval_.val;

        newValue.ctr = (oldValueDirtyRead.ctr + 1);
        newValue.state = Locked;

        // We can only get the lock if it's currently unlocked
        oldValueDirtyRead.state = Unlocked;

        lock_.lock();
        oldValueReturned.val = atomicCmpXchg64((Atomic64 *) &lockval_.val,
                                               (int64_t) oldValueDirtyRead.val,
                                               (int64_t) newValue.val);
        lock_.unlock();
        if (oldValueDirtyRead.val == oldValueReturned.val) {
            done = true;
        } else {
            // Spun for too long; Time to sleep!
            numSuspended_++;
            sem_.semWait();
        }
    }

#ifdef DEBUG
    // Set lock owner.
    assert(owner_ == NULL && "Somebody already owns this lock!");
    Thread *thread = Thread::getRunningThread();
    if (thread != NULL) {
        owner_ = thread->getRunningSchedObj();
    }
#endif  // DEBUG
}

void
Mutex::unlock()
{
    assert(initState_ == InitState::Inited);
#ifdef DEBUG
    owner_ = NULL;
#endif  // DEBUG

    AtomicLockField oldValueDirtyRead, newValue, oldValueReturned;

    oldValueDirtyRead.val = lockval_.val;
    assert(oldValueDirtyRead.state == Locked);

    newValue.val = oldValueDirtyRead.val;
    newValue.state = Unlocked;

    lock_.lock();
    oldValueReturned.val = atomicCmpXchg64((Atomic64 *) &lockval_.val,
                                           (int64_t) oldValueDirtyRead.val,
                                           (int64_t) newValue.val);

    assert(oldValueReturned.val == oldValueDirtyRead.val);

    // Kick up the waiters.
    sem_.post();
    lock_.unlock();

    memBarrier();
}

bool
Mutex::tryLock()
{
    assert(initState_ == InitState::Inited);

    AtomicLockField oldValueDirtyRead, newValue, oldValueReturned;

    // Dirty bit field read
    if (lockval_.state == Locked) {
        return false;
    }
    oldValueDirtyRead.val = lockval_.val;

    newValue.ctr = (oldValueDirtyRead.ctr + 1);
    newValue.state = Locked;

    // We can only get the lock if it's currently unlocked
    oldValueDirtyRead.state = Unlocked;

    oldValueReturned.val = atomicCmpXchg64((Atomic64 *) &lockval_.val,
                                           (int64_t) oldValueDirtyRead.val,
                                           (int64_t) newValue.val);

    if (oldValueDirtyRead.val != oldValueReturned.val) {
        return false;
    }

#ifdef DEBUG
    // Set lock owner.
    assert(owner_ == NULL && "Somebody already owns this lock!");
    Thread *thread = Thread::getRunningThread();
    if (thread != NULL) {
        owner_ = thread->getRunningSchedObj();
    }
#endif  // DEBUG
    return true;
}

#ifdef DEBUG
bool
Mutex::mine()
{
    Thread *thread = Thread::getRunningThread();
    if (thread == NULL) {
        // owner_ can be NULL is case of threads not owned by runtime.
        return owner_ == NULL;
    }
    return thread->getRunningSchedObj() == owner_;
}
#endif  // DEBUG
