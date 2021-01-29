// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/SchedulableFsm.h"

//
// Schedulable FSM
//

SchedulableFsm::SchedulableFsm(const char *name, FsmState *curState)
    : Schedulable(name), curState_(curState), sem_(0)
{
}

void
SchedulableFsm::run()
{
    FsmState *myState = NULL;
    while (true) {
        myState = curState_;
        if (myState == NULL) {
            break;
        }
        FsmState::TraverseState ts = myState->doWork();
        if (ts == FsmState::TraverseState::TraverseNext) {
            continue;
        } else {
            assert(ts == FsmState::TraverseState::TraverseStop);
            break;
        }
    }
}

void
SchedulableFsm::suspend()
{
    sem_.semWait();
}

void
SchedulableFsm::resume()
{
    sem_.post();
}

FsmState *
SchedulableFsm::setNextState(FsmState *nextState)
{
    FsmState *myState = curState_;
    curState_ = nextState;
    return myState;
}

FsmState *
SchedulableFsm::getCurState()
{
    return curState_;
}

FsmState *
SchedulableFsm::endFsm()
{
    return setNextState(NULL);
}

//
// Schedulable FSM State
//
FsmState::FsmState(const char *name, SchedulableFsm *schedFsm)
    : name_(name), schedFsm_(schedFsm)
{
}

FsmState::~FsmState() {}
