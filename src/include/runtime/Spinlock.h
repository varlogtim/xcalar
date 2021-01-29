// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SPINLOCK_H
#define SPINLOCK_H

#include "primitives/Primitives.h"
#include "util/Stopwatch.h"
#include "runtime/Guard.h"

class SchedObject;

class Spinlock final
{
  public:
    Spinlock();
    ~Spinlock();

    void lock();
    void unlock();

    // Takes the mutex and returns a scoped guard object that releases it upon
    // destruction.
    Guard<Spinlock> take() { return Guard<Spinlock>(this); }

    // true if aquired, false otherwise.
    bool tryLock();
#ifdef DEBUG
    bool mine();
#endif  // DEBUG

    // Disallow.
    Spinlock(const Spinlock &) = delete;
    Spinlock &operator=(const Spinlock &) = delete;

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
    volatile AtomicLockField lockval_;
    SchedObject *owner_ = NULL;
};

#endif  // SPINLOCK_H
