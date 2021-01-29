// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef MUTEX_H
#define MUTEX_H

#include "primitives/Primitives.h"
#include "util/Stopwatch.h"
#include "runtime/Guard.h"
#include "runtime/CondVar.h"
#include "runtime/Semaphore.h"

class SchedObject;

class Mutex final
{
  public:
    Mutex();
    ~Mutex();

    void lock();
    void unlock();

    // Takes the mutex and returns a scoped guard object that releases it upon
    // destruction.
    Guard<Mutex> take() { return Guard<Mutex>(this); }

    // true if aquired, false otherwise.
    bool tryLock();
#ifdef DEBUG
    bool mine();
#endif  // DEBUG

    // Disallow.
    Mutex(const Mutex &) = delete;
    Mutex &operator=(const Mutex &) = delete;

  private:
    static constexpr uint64_t Unlocked = 0;
    static constexpr uint64_t Locked = 1;

    enum class InitState {
        Inited = 70,
        Destructed = 80,
    };

    union AtomicLockField {
        uint64_t val;

        struct {
            uint64_t state : 1;
            uint64_t ctr : 63;
        };
    } __attribute__((aligned(8)));

    InitState initState_;
    Semaphore sem_;
    AtomicLockField lockval_;
    uint64_t numSuspended_ = 0;
    Spinlock lock_;
    SchedObject *owner_ = NULL;
};

#endif  // MUTEX_H
