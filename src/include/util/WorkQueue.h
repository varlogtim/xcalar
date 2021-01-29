// Copyright 2014-2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _WORKQUEUE_H_
#define _WORKQUEUE_H_

#include <semaphore.h>

#include "primitives/Primitives.h"
#include "util/Atomics.h"
#include "stat/StatisticsTypes.h"
#include "common/InitLevel.h"
#include "runtime/Spinlock.h"

//
// Work Queues
//
// Work queues are nothing but a queue of work and a fixed number of threads
// to process that work.
//

class WorkQueue
{
    friend class WorkQueueMod;  // only WorkQueueMod may construct a work queue

  public:
    // individual work element to be processed by a work queue. users should
    // derive their work element from this class and specify the callback
    // they want to invoke
    class Elt
    {
      public:
        Elt() {}
        virtual ~Elt() {}
        virtual void func() = 0;
        Elt *prev;
        Elt *next;
    };

    enum {
        MaxNameLen = 255,  // maximum name for a work queue
    };

    //
    // Enqueue a work element to be processed by the work queue
    //
    // param[in] workElt element to queue
    //
    void enqueue(Elt *workElt);

    // total number of Worker threads.
    unsigned getNumWorkerThreads();

    static WorkQueue *init(const char *nameIn, unsigned initNumWorkerThreadsIn);
    void destroy();

  private:
    struct Thread {
        pthread_t pthread;
        Atomic32 shouldReap;
        WorkQueue *workQueue;
        struct timespec lastRan;
    };

    enum {
        // XXX This should be determined at run time.  With the current
        // generation 24-core / 48-thread processors, a 4 socket system could
        // have 192 processors online.  We don't want to limit ourselves the
        // 2/3rds of that.
        MaxNumWorkerThreads = 512,
        WorkerWaitIntervalUs = USecsPerMSec * 100,
        NoThreadsStarted = (unsigned) -1,
        MinimumWorkerCount = 1,
    };

    char name_[MaxNameLen + 1];

    // XXX FIXME consider making this a hashed-queue if contention on
    // workList causes too much serialization
    Elt *workListHead_;
    Spinlock workListLock_;

    struct timespec lastServiceTime_;
    unsigned numWorkerThreads_;
    sem_t sem_;
    WorkQueue::Thread threads_[MaxNumWorkerThreads];

    StatHandle queued_;
    StatHandle total_;
    StatHandle elemRun_;
    StatHandle avgRunTimeUsecNumerator_;
    StatHandle avgRunTimeUsecDenominator_;

    Status statsToInit();

    // must be static due to pthread_create()
    static void *worker(void *wThreadIn);
    void reapWorkers();
    unsigned addWorkers();
    void reapAllThreads();

    // force construction via init()
    WorkQueue() {}
    // force destruction via destroy()
    ~WorkQueue() {}

    WorkQueue(const WorkQueue &) = delete;
    WorkQueue &operator=(const WorkQueue &) = delete;
};

// Singleton instance.
class WorkQueueMod
{
    friend class WorkQueue;

  public:
    static WorkQueueMod *getInstance();
    Status init(InitLevel initLevel);
    void destroy();

  private:
    static WorkQueueMod *instance;

    // force construction via init
    WorkQueueMod() {}

    // force destruction via destroy
    ~WorkQueueMod() {}

    WorkQueueMod(const WorkQueueMod &) = delete;
    WorkQueueMod &operator=(const WorkQueueMod &) = delete;
};

extern WorkQueue *workQueueForIoToDisk;

#endif  // _WORKQUEUE_H_
