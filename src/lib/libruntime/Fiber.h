// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef FIBER_H
#define FIBER_H

#include <ucontext.h>
#include "primitives/Primitives.h"
#include "SchedObject.h"
#include "runtime/Spinlock.h"
#include "runtime/Semaphore.h"

//
// A Fiber is an abstraction representing a stack and execution context. A Fiber
// can be executed on any runtime thread. Fibers are not preempted by the
// runtime.
//
// The purpose of a Fiber is to execute many SchedObjects. The currently running
// SchedObject determines when the Fiber suspends. Because a SchedObject state
// will occupy the stack, Fibers must execute a SchedObject to completion before
// picking up another.
//

class Fiber final
{
  public:
    explicit Fiber(uint64_t id);
    ~Fiber();
    Status make(void (*func)());
    void reset();
    void retire();

    void run(SchedObject *schedObj);
    void welcomeBack();

    static void contextSwitch(Fiber *source,
                              Fiber *destination,
                              bool recycleSource);

    uint64_t getId() const;
    SchedObject *getCurrentSchedObj();

    Fiber *nextCache;  // Owned by FiberCache.

  private:
    static constexpr const char *ModuleName = "libruntime";

    enum class State {
        Invalid = 10,
        Created,
        Running,
        Suspended,
        Retired,
    };

    void makeContext();

    static void entryPointWrapper();
    void entryPoint();

    // Disallow.
    Fiber(const Fiber &) = delete;
    Fiber &operator=(const Fiber &) = delete;

    Fiber::State state_;
    uint64_t fiberId_;

    // Defines Fiber's stack. stackBase points to base of allocation. A guard
    // page is placed at the base and top of the stack. stackUserBase points to
    // the usable portion above the guard page.
    void *stackBase_;
    void *stackUserBase_;
    size_t stackCombinedBytes_;  // Size of user + guard.

    // Which SchedObject is this Fiber currently executing?
    SchedObject *currentSchedObj_;

    //
    // All below members are used only in special circumstances.
    //

    // If Fiber state is Suspended or NotRun, savedContext can be switch into to
    // run it.
    ucontext_t savedContext_;

    // Used for ASAN.
#ifdef ADDRESS_SANITIZER
    void *fakeStack_;
    bool fiberSwitchInProgress_;
#endif  // ADDRESS_SANITIZER

    // If Fiber state is NotRun, when switched into, Fiber will execute this
    // method.
    void (*externalEntryPoint_)();

    // During a context switch, *exactly one* of the two below members will tell
    // this Fiber what to do with the source Fiber.
    Fiber *fiberToSave_;

#ifdef XLR_VALGRIND
    unsigned vgStackId;
#endif  // XLR_VALGRIND

    Fiber *fiberToDelete_;
};

#endif  // FIBER_H
