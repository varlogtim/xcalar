// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#if !defined(RUNTIME_GUARD_H)
#define RUNTIME_GUARD_H

#include "primitives/Primitives.h"

// A simple scope-based guard that takes the lock upon creation and releases
// upon destruction (the RAII principle). This makes the locking code shorter
// and harder to misuse.
//
// It relies on the following "Lockable" contract:
//
//  lock() takes the lock in a blocking fashion.
//  unlock() releases the lock.
//
template <typename Lockable>
class Guard final
{
  public:
    explicit Guard(Lockable *lock) : lock_(lock)
    {
        assert(lock_);
        lock_->lock();
    }

    ~Guard()
    {
        if (lock_ != nullptr) lock_->unlock();
    }

    // Explicit unlock for dealing with more complex flows. Effectively
    // discharges the guard object.
    void unlock()
    {
        assert(lock_ != nullptr);
        lock_->unlock();
        lock_ = nullptr;
    }

    // Copying is disabled (as that makes no sense).
    Guard(const Guard &) = delete;
    Guard &operator=(const Guard &) = delete;

    // Support movement as we can move an object around. The moved-from object
    // is left in the discharged state.
    Guard(Guard &&rhs)
    {
        lock_ = rhs.lock_;
        rhs.lock_ = nullptr;
    }
    Guard &operator=(Guard &&rhs)
    {
        if (lock_ != nullptr) lock_->unlock();
        lock_ = rhs.lock_;
        rhs.lock_ = nullptr;
        return *this;
    }

  private:
    Lockable *lock_;
};

#endif
