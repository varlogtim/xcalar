// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef RW_LOCK_H
#define RW_LOCK_H

#include "primitives/Primitives.h"
#include "runtime/Mutex.h"
#include "runtime/CondVar.h"

class SchedObject;

class RwLock final
{
  public:
    RwLock();
    ~RwLock();

    enum class Type {
        Reader,
        Writer,
    };

    void lock(Type type);
    void unlock(Type type);

    // true => have the lock. false => no lock for you.
    bool tryLock(Type type);

  private:
    // Disallow.
    RwLock(const RwLock&) = delete;
    RwLock& operator=(const RwLock&) = delete;

    Spinlock lock_;
    CondVar cv_;
    int track_ = 0;
};

#endif  // RW_LOCK_H
