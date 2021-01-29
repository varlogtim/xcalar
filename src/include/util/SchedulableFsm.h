// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SCHEDULABLE_FSM_H
#define SCHEDULABLE_FSM_H

#include "primitives/Primitives.h"
#include "runtime/Schedulable.h"
#include "runtime/Semaphore.h"

//
// Abstract class representing some schedulable FSM (Finite State Machine) that
// can be run by the runtime.
//
class FsmState;
class SchedulableFsm : public Schedulable
{
  public:
    SchedulableFsm(const char *name, FsmState *curState);
    void run() override;

    // Suspend the current running State.
    void suspend();

    // Resume running the current state that was suspended.
    void resume();

    // Set the next state to run. Returns current state.
    FsmState *setNextState(FsmState *nextState);

    // get current state.
    MustCheck FsmState *getCurState();

    // Returns current state.
    MustCheck FsmState *endFsm();

  private:
    FsmState *curState_ = NULL;
    Semaphore sem_;
};

//
// Abstract class representing some State of schedulable FSM (Finite State
// Machine) that can be run by the runtime.
//
class FsmState
{
  public:
    FsmState(const char *name, SchedulableFsm *schedFsm);
    virtual ~FsmState();

    // Override this to implement a State in FSM.
    enum class TraverseState {
        TraverseNext,
        TraverseStop,
    };
    virtual MustCheck TraverseState doWork() = 0;

    SchedulableFsm *getSchedFsm() { return schedFsm_; }

  private:
    const char *name_;
    SchedulableFsm *schedFsm_;
};

#endif  // SCHEDULABLE_FSM_H
