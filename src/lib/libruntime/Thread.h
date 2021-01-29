// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef THREAD_H
#define THREAD_H

#include "primitives/Primitives.h"

class SchedObject;

//
// Represents a single pthread owned by the runtime. It is either used to run
// Fibers (FiberSchedThread) or dedicated to a particular purpose
// (DedicatedThread).
//

class Thread
{
  public:
    enum class State {
        Invalid = 10,
        Running,     // Running a SchedObject.
        NotRunning,  // Waiting for SchedObject, setting up, etc.
    };

    Thread();
    virtual ~Thread();

    virtual SchedObject *getRunningSchedObj() = 0;
    virtual void suspendRunningSchedObj() = 0;

    State getState() { return state; }

    static Thread *getRunningThread();

  protected:
    static void *threadEntryPointWrapper(void *arg);

    State state;

  private:
    virtual void threadEntryPoint() = 0;

    unsigned tid_;  // Thread's kernel TID.
};

#endif  // THREAD_H
