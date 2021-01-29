// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <errno.h>
#ifdef XLR_VALGRIND
#include <valgrind/valgrind.h>
#endif

#include <sanitizer/common_interface_defs.h>

#include "Fiber.h"
#include "runtime/Runtime.h"
#include "FiberSchedThread.h"
#include "FiberCache.h"
#include "runtime/Schedulable.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

//
// Initialization and destruction of Fibers.
//

Fiber::Fiber(uint64_t id)
    : state_(Fiber::State::Invalid),
      fiberId_(id),
      stackBase_(NULL),
      stackUserBase_(NULL),
      stackCombinedBytes_(0),
      currentSchedObj_(NULL),
      externalEntryPoint_(NULL),
      fiberToSave_(NULL),
#ifdef XLR_VALGRIND
      vgStackId(0),
#endif
      fiberToDelete_(NULL)
{
}

Fiber::~Fiber()
{
    assert(state_ == Fiber::State::Invalid || state_ == Fiber::State::Retired ||
           state_ == Fiber::State::Created);
    assert(fiberToSave_ == NULL);
    assert(fiberToDelete_ == NULL);

    if (stackBase_ != NULL && stackBase_ != MAP_FAILED) {
        assert(stackCombinedBytes_ != 0);
#ifdef XLR_VALGRIND
        VALGRIND_STACK_DEREGISTER(vgStackId);
#endif
        verify(memUnmap(stackBase_, stackCombinedBytes_) == 0);
    }
}

// Create a new execution context. Point to fiberEntry which executes requested
// entry point. Called after stack is in place.
void
Fiber::makeContext()
{
    // 64-bit implementation of getcontext will fail if the sigprocmask syscall
    // it makes fails. Because it's just reading the current mask, it's
    // guaranteed to succeed (via 'man sigprocmask').
    verify(getcontext(&savedContext_) == 0);

    // The result of getcontext will become the base for our new context (via
    // 'man makecontext').
    savedContext_.uc_stack.ss_sp = stackUserBase_;
    savedContext_.uc_stack.ss_size = Runtime::get()->getStackBytes();

    // These have no previous context. We take explicit control over where this
    // context will be freed.
    savedContext_.uc_link = NULL;

    makecontext(&savedContext_, Fiber::entryPointWrapper, 0);
}

Status
Fiber::make(void (*func)())
{
    Status status = StatusOk;

    // Should be called exactly once after construction.
    assert(state_ == Fiber::State::Invalid);
    assert(externalEntryPoint_ == NULL);
    assert(stackBase_ == NULL);
    assert(stackUserBase_ == NULL);
    assert(fiberToSave_ == NULL);
    assert(fiberToDelete_ == NULL);

    // Create fiber stack. Make provision for guard page at the stack base
    // at top.
    const size_t stackUserBytes = Runtime::get()->getStackBytes();
    const size_t stackGuardBytes = Runtime::get()->getStackGuardBytes();
    stackCombinedBytes_ = stackUserBytes + stackGuardBytes * 2;

    stackBase_ = memMap(NULL,                 // No preference.
                        stackCombinedBytes_,  // Full needed size.
                        PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK,
                        -1,  // Due to MAP_ANONYMOUS.
                        0);
    if (stackBase_ == MAP_FAILED) {
        // It's easier to think of failure as NULL.
        stackBase_ = NULL;
    }
    if (stackBase_ == NULL) {
        return StatusNoMem;
    }

    // Guard page at the base of stack.
    if (mprotect(stackBase_, stackGuardBytes, PROT_NONE) != 0) {
        status = sysErrnoToStatus(errno);
        memUnmap(stackBase_, stackCombinedBytes_);
        stackBase_ = NULL;
        return status;
    }

    // Guard page at the top of stack.
    if (mprotect(static_cast<uint8_t *>(stackBase_) + stackGuardBytes +
                     stackUserBytes,
                 stackGuardBytes,
                 PROT_NONE) != 0) {
        status = sysErrnoToStatus(errno);
        memUnmap(stackBase_, stackCombinedBytes_);
        stackBase_ = NULL;
        return status;
    }

    stackUserBase_ = static_cast<uint8_t *>(stackBase_) + stackGuardBytes;

#ifdef XLR_VALGRIND
    vgStackId =
        VALGRIND_STACK_REGISTER(stackUserBase_,
                                (char *) stackUserBase_ + stackUserBytes);
#endif

    // Create execution context that calls requested entry point.
    externalEntryPoint_ = func;
    makeContext();
    state_ = Fiber::State::Created;

    return StatusOk;
}

void
Fiber::reset()
{
    if (state_ == Fiber::State::Created) {
        // Not yet used. Don't need to reset again.
        return;
    }
    assert(state_ == Fiber::State::Retired);

    makeContext();
    state_ = Fiber::State::Created;
}

void
Fiber::retire()
{
    state_ = Fiber::State::Retired;
}

void  // static
Fiber::entryPointWrapper()
{
    FiberSchedThread *thr = FiberSchedThread::getRunningThread();
    assert(thr != NULL);

    Fiber *fiber = thr->getRunningFiber();
    assert(fiber != NULL);

    fiber->entryPoint();
}

// Entry point for every new Fiber.
void
Fiber::entryPoint()
{
    assert(state_ == Fiber::State::Created);
    assert(externalEntryPoint_ != NULL);

    welcomeBack();
    externalEntryPoint_();
}

////////////////////////////////////////////////////////////////////////////////

//
// Accessors.
//

uint64_t
Fiber::getId() const
{
    return fiberId_;
}

SchedObject *
Fiber::getCurrentSchedObj()
{
    return currentSchedObj_;
}

////////////////////////////////////////////////////////////////////////////////

//
// Context switch.
//

void  // static
Fiber::contextSwitch(Fiber *source, Fiber *destination, bool recycleSource)
{
    assert(destination != NULL);
    assert(source != destination);
    assert(source == NULL || source->fiberToSave_ == NULL);
    assert(source == NULL || source->fiberToDelete_ == NULL);
    assert(source == NULL || source->state_ == Fiber::State::Running);
    assert(destination->state_ == Fiber::State::Created ||
           destination->state_ == Fiber::State::Suspended);

    if (destination->currentSchedObj_ != NULL) {
        destination->currentSchedObj_->setState(SchedObject::State::Running);
    }

    if (!recycleSource && source != NULL) {
        destination->fiberToSave_ = source;
#ifdef ADDRESS_SANITIZER
        if (source->fiberSwitchInProgress_) {
            source->fiberSwitchInProgress_ = false;
            __sanitizer_finish_switch_fiber(source->fakeStack_,
                                            (const void **) &source
                                                ->savedContext_.uc_stack.ss_sp,
                                            &source->savedContext_.uc_stack
                                                 .ss_size);
        }
        __sanitizer_start_switch_fiber(&destination->fakeStack_,
                                       destination->savedContext_.uc_stack
                                           .ss_sp,
                                       destination->savedContext_.uc_stack
                                           .ss_size);
        destination->fiberSwitchInProgress_ = true;
#endif  // ADDRESS_SANITIZER

        swapcontext(&source->savedContext_, &destination->savedContext_);

#ifdef ADDRESS_SANITIZER
        assert(source->fiberSwitchInProgress_ == true);
        source->fiberSwitchInProgress_ = false;
        __sanitizer_finish_switch_fiber(source->fakeStack_,
                                        (const void **) &source->savedContext_
                                            .uc_stack.ss_sp,
                                        &source->savedContext_.uc_stack
                                             .ss_size);
#endif  // ADDRESS_SANITIZER

        // Executed after returning from switch.
        source->welcomeBack();
        assert(source->currentSchedObj_ != NULL);  // Because we're resuming.
    } else {
        if (source != NULL) {
#ifdef ADDRESS_SANITIZER
            if (source->fiberSwitchInProgress_) {
                source->fiberSwitchInProgress_ = false;
                __sanitizer_finish_switch_fiber(source->fakeStack_,
                                                (const void **) &source
                                                    ->savedContext_.uc_stack
                                                    .ss_sp,
                                                &source->savedContext_.uc_stack
                                                     .ss_size);
            }
#endif  // ADDRESS_SANITIZER
            destination->fiberToDelete_ = source;
        }

#ifdef ADDRESS_SANITIZER
        __sanitizer_start_switch_fiber(&destination->fakeStack_,
                                       destination->savedContext_.uc_stack
                                           .ss_sp,
                                       destination->savedContext_.uc_stack
                                           .ss_size);
        destination->fiberSwitchInProgress_ = true;
#endif  // ADDRESS_SANITIZER

        setcontext(&destination->savedContext_);

        // Fiber we're running in the context of should be blown away after
        // setcontext.
        NotReached();
    }
}

////////////////////////////////////////////////////////////////////////////////

// Called wherever we have switched into a new context. Executed within the
// destination context. Primarily deals with cleaning up the source context.
void
Fiber::welcomeBack()
{
    FiberSchedThread *thr = FiberSchedThread::getRunningThread();

    assert(thr != NULL);
    assert(!(fiberToSave_ != NULL && fiberToDelete_ != NULL));
    assert(this == thr->getRunningFiber());

#ifdef RUNTIME_DEBUG
#ifdef DEBUG
    // Verify we're executing in context of the correct Fiber.
    volatile char dummy;

    assert(&dummy > stackUserBase_);
    assert(&dummy < (char *) stackUserBase_ + Runtime::get()->getStackBytes());

#endif  // DEBUG
    xSyslog(ModuleName,
            XlogDebug,
            "[thread %x] Running Fiber %llx",
            thr->getId(),
            (unsigned long long) fiberId_);
#endif  // RUNTIME_DEBUG

    state_ = Fiber::State::Running;

    if (fiberToSave_ != NULL) {
        fiberToSave_->state_ = Fiber::State::Suspended;

        // Context for fiberToSave_ was successfully saved by swapcontext.
        // It is now ready to be made runnable.
        fiberToSave_->currentSchedObj_->enableStateChanges();
        fiberToSave_ = NULL;
    } else if (fiberToDelete_ != NULL) {
        fiberToDelete_->state_ = Fiber::State::Retired;
        thr->cleanupFiber(fiberToDelete_);
        fiberToDelete_ = NULL;
    }
}

void
Fiber::run(SchedObject *schedObj)
{
    assert(this == FiberSchedThread::getRunningThread()->getRunningFiber());
    assert(state_ == Fiber::State::Running);

    // Bind SchedObject and Fiber together. They're bound until schedObj is
    // done.
    assert(currentSchedObj_ == NULL);
    assert(schedObj->fiber_ == NULL);
    currentSchedObj_ = schedObj;
    schedObj->fiber_ = this;

    currentSchedObj_->setState(SchedObject::State::Running);

#ifdef RUNTIME_DEBUG
    xSyslog(ModuleName,
            XlogDebug,
            "[thread %x] Running Fiber %llx SchedObject %p run",
            FiberSchedThread::getRunningThread()->getId(),
            (unsigned long long) fiberId_,
            schedObj);
#endif

    schedObj->getSchedulable()->run();

    // Important assert: make sure, if we switched away from this Fiber,
    // we switched back.
    assert(currentSchedObj_ == schedObj);
    assert(this == schedObj->fiber_);

#ifdef RUNTIME_DEBUG
    xSyslog(ModuleName,
            XlogDebug,
            "[thread %x] Running Fiber %llx SchedObject %p done",
            FiberSchedThread::getRunningThread()->getId(),
            (unsigned long long) fiberId_,
            currentSchedObj_);
#endif

    schedObj->getSchedulable()->done();
    schedObj->setState(SchedObject::State::Done);
    delete schedObj;
    schedObj = NULL;
    currentSchedObj_ = NULL;
}
