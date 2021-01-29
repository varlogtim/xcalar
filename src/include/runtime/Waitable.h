// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef WAITABLE_H
#define WAITABLE_H

#include "primitives/Primitives.h"
#include "runtime/Spinlock.h"
#include "util/AtomicTypes.h"
#include "util/Atomics.h"

struct WaitBlock;
union WaitArg;
class SchedObject;

//
// Runtime internal. An object that a SchedObject can "wait on".
//

class Waitable
{
  public:
    Waitable() : waitQHead_(NULL), waitQTail_(NULL)
    {
        atomicWrite64(&suspensionCount_, 0);
        atomicWrite64(&suspendedTimeus_, 0);
    }

    virtual ~Waitable() {}

    bool doWait(WaitBlock *waitBlock, WaitArg *waitArg);
    void dequeue(WaitBlock *block);

    // returns true if there are any fibers waiting on this object
    bool checkWaiting();

    // stats
    void incSuspensions();
    void setSuspendedTime(int64_t us);
    int64_t getSuspensionsCount();
    int64_t getSuspendedTime();

  protected:
    // Allow waitables to prepare a Waiter for wait and short-circuit the
    // wait by returning false.
    virtual bool prepareWait(SchedObject *waiter, WaitArg *arg);
    virtual void enqueue(WaitBlock *block, WaitArg *arg) = 0;
    bool wake(WaitBlock *block, Status wakeStatus);

    // A doubly linked queue of wait blocks.
    WaitBlock *waitQHead_;
    WaitBlock *waitQTail_;

    // number of times  and how long fibers suspended on this
    Atomic64 suspensionCount_;
    Atomic64 suspendedTimeus_;

    // Protects waitQ (head, tail, and links).
    mutable Spinlock waitQLock_;
};

#endif  // WAITABLE_H
