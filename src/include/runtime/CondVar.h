// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef CONDVAR_H
#define CONDVAR_H

#include "primitives/Primitives.h"
#include "runtime/Waitable.h"

class Mutex;

class CondVar final : public Waitable
{
  public:
    CondVar();
    ~CondVar();

    void wait();
    void wait(Mutex *mutex);
    void wait(Spinlock *slock);

    void signal();
    void broadcast();

  protected:
    void enqueue(WaitBlock *block, WaitArg *arg) override;

  private:
    void waitInternal(WaitArg *arg);
    bool isBroadcasting_;
};

#endif  // CONDVAR_H
