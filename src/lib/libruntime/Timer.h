// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef TIMER_H
#define TIMER_H

#include "primitives/Primitives.h"
#include "runtime/Waitable.h"

class SchedObject;

class Timer final : public Waitable
{
  public:
    // Defines our sleep time if there's nothing to wake.
    static constexpr uint64_t LongSleepTimeUSec = USecsPerSec;

    Timer();
    virtual ~Timer() override;

    void wait(uint64_t usecsRel);
    void wakeExpired();

    uint64_t getCurrentUSecs() const;
    uint64_t getSleepUSecs() const;

  protected:
    virtual void enqueue(WaitBlock *block, WaitArg *arg) override;
    virtual bool prepareWait(SchedObject *schedObj, WaitArg *arg) override;
};

#endif  // TIMER_H
