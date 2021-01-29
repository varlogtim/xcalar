// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef DEDICATEDTHREAD_H
#define DEDICATEDTHREAD_H

#include <pthread.h>
#include <semaphore.h>
#include "primitives/Primitives.h"
#include "Thread.h"
#include "SchedObject.h"
#include "msg/Xid.h"
#include "runtime/Txn.h"

//
// A thread in which work simply runs on top of a raw pthread. This thread isn't
// used to schedule Fibers. The thread is dedicated to performing the work of
// whoever created it. Can be used with normal pthread APIs (e.g. pthread_join).
//

class DedicatedThread final : public Thread
{
  public:
    DedicatedThread(void *(*entryPoint)(void *), void *arg);
    virtual ~DedicatedThread() override;

    Status create(pthread_t *thread, const pthread_attr_t *attr);

    virtual SchedObject *getRunningSchedObj() override;
    virtual void suspendRunningSchedObj() override;

    static DedicatedThread *getRunningThread() { return runningThread; }

  private:
    // Dummy SchedObject to represent the work being done on DedicatedThreads.
    // Basically handles all scheduler hooks without involving Scheduler.
    class DedicatedThreadSchedObject : public SchedObject
    {
      public:
        DedicatedThreadSchedObject(DedicatedThread *thread);
        virtual ~DedicatedThreadSchedObject() override;

        virtual void makeNotRunnable() override;
        virtual Status makeRunnable() override;
        virtual void rollbackNotRunnable() override;

      private:
        DedicatedThread *thr;
    };

    virtual void threadEntryPoint() override;

    // Disallow.
    DedicatedThread(const DedicatedThread &) = delete;
    DedicatedThread &operator=(const DedicatedThread &) = delete;

    DedicatedThreadSchedObject schedObj;

    // Implementation of suspend relies on sem_t.
    sem_t semSuspend;

    void *(*clientEntryPoint)(void *);
    void *clientArg;

    // Thread's TXN ID. Used to pass to descendant threads. Only valid during
    // the period where the parent sets it to its own value and the descendant
    // sets it in its context.
    Txn txn_;

    static thread_local DedicatedThread *runningThread;
};

#endif  // DEDICATEDTHREAD_H
