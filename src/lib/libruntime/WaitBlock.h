// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef WAITBLOCK_H
#define WAITBLOCK_H

#include "primitives/Primitives.h"

class SchedObject;
class Waitable;

// Represents a single Waiter waiting on a single Waitable.
struct WaitBlock {
    SchedObject *schedObj = nullptr;
    Waitable *waitable = nullptr;

    WaitBlock *next = nullptr;
    WaitBlock *prev = nullptr;
};

// Used to parameterize individual waits.
union WaitArg {
    enum class LockType {
        None,
        Spinlock,
        Mutex,
    };
    struct {
        uint64_t usecsRel;
    } timer;

    struct {
        LockType lType;
        void *lockPtr;
    } condvar;
};

#endif  // WAITBLOCK_H
