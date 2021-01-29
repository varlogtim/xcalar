// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SEMAPHORE_H
#define SEMAPHORE_H

#include "primitives/Primitives.h"
#include "runtime/Waitable.h"

class SchedObject;

#ifdef DEBUG
#define DEBUG_SEM
#endif

//
// A Semaphore for use on runtime-owned threads. If necessary, will suspend
// execution (as Fiber) in user-mode and create/resume another Fiber in its
// place.
//

class Semaphore final : public ::Waitable
{
  public:
    Semaphore();
    explicit Semaphore(uint64_t initialValue);
    ~Semaphore();

    void init(uint64_t initialValue);
#ifdef DEBUG_SEM
    void wait(const char *fileNameIn, const char *funcNameIn, int lineNumberIn);
#else
    void wait();
#endif
    // true if decrement succeeds. false otherwise.
    bool tryWait();
    void post();
    void destroy();

    // Returns StatusTimedOut or StatusOk
    Status timedWait(uint64_t usecsRel);

  protected:
    void enqueue(WaitBlock *block, WaitArg *arg) override;
    bool prepareWait(SchedObject *schedObj, WaitArg *arg) override;

  private:
    static constexpr uint64_t InvalidValue = 0xdeadbeefdeadbeef;

    // Avoid common bit patterns in tracking init state.
    enum class State {
        PreInit = 70,
        Init,
        Destructed,
    };

    MustInline bool tryWaitInt()
    {
        assert(state_ == State::Init);

        waitQLock_.lock();
        if (value_ > 0) {
            value_--;
            waitQLock_.unlock();
            return true;
        }
        waitQLock_.unlock();
        return false;
    }

    // Disallow.
    Semaphore(const Semaphore &) = delete;
    Semaphore &operator=(const Semaphore &) = delete;

    State state_;
    uint64_t value_;
};

#ifdef DEBUG_SEM
#define semWait() wait(__FILE__, __PRETTY_FUNCTION__, __LINE__)
#else
#define semWait wait
#endif

#endif  // SEMAPHORE_H
