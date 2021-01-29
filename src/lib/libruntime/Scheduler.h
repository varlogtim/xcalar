// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <semaphore.h>
#include "primitives/Primitives.h"
#include "SchedObject.h"
#include "runtime/Spinlock.h"
#include "stat/Statistics.h"

//
// A simple FIFO scheduler queue. Only keeps track of runnable SchedObjects.
//

class Timer;

class Scheduler final
{
  public:
    Scheduler(const char *name, Timer *timer);
    ~Scheduler();
    MustCheck Status init();

    MustCheck Status enqueue(SchedObject *schedObj,
                             SchedObject::State prevState);
    MustCheck SchedObject *dequeue(bool fiberPrealloced);
    MustCheck SchedObject *dequeueTryPrealloc();
    MustCheck SchedObject *dequeueTryNonBlock();
    void transactionAbort();

    MustCheck bool isEmpty() const;

    // This SchedObject is not runnable. It's enqueued in its private
    // sem queue, but we're adding this to a global queue also for visibility
    void makeNotRunnable(SchedObject *schedObj);
    void dequeueNotRunnable(SchedObject *schedObj);

    void wakeThread();

    MustCheck bool isMarkedToAbortSchedObjs() { return markToAbortSchedObjs_; }

    void markToAbortSchedObjs();

    void clearAbortOfSchedObjs();

    const char *getName() { return name_; }

    StatHandle timeSuspendedSchedObjs_;
    StatHandle numSuspendedSchedObjs_;

  private:
    static SchedObject *Poison;
    static constexpr const uint32_t MaxTransactAbortPerIter = 16384;

    // Disallow.
    Scheduler(const Scheduler &) = delete;
    Scheduler &operator=(const Scheduler &) = delete;

    const char *name_;
    Timer *timer_;

    // Queue of runnable SchedObjects. This defines the set of items that
    // scheduling must decide between.
    SchedObject *runnableQHead;
    SchedObject *runnableQTail;

    // Queue of runnable SchedObjects. This defines the set of items that
    // scheduling must decide between.
    SchedObject *runnableFiberQHead;
    SchedObject *runnableFiberQTail;

    // Queue of not-runnable SchedObjects. Note that these schedObjects are
    // in 2 queues: this not-runnable queue and its private Semaphore queue
    SchedObject *notRunnableQHead;
    SchedObject *notRunnableQTail;

    mutable Spinlock notRunnableQLock_;
    mutable Spinlock runnableQLock_;

    // Block schedule requests until runnable SchedObject is available.
    mutable sem_t runnableQSem;

    bool markToAbortSchedObjs_;

    uint64_t lastTimerCheckUSecs_ = 0;
    uint64_t numDequeuesPrealloced_ = 0;

    static constexpr const size_t StatsCount = 8;
    StatGroupId groupId_;
    StatHandle stMarkToAbortSchedObjs_;
    StatHandle handleTxnAbort_;
    StatHandle txnAbortCount_;
    StatHandle schedulableMarkedToAbort_;
    StatHandle clearAbortOfSchedObjs_;
    StatHandle numRunnableSchedObjs_;
};

#endif  // SCHEDULER_H
