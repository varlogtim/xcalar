// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>

#include "StrlFunc.h"
#include "util/WorkQueue.h"
#include "util/System.h"
#include "util/Atomics.h"
#include "stat/Statistics.h"
#include "util/MemTrack.h"
#include "runtime/Runtime.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "WorkQueue";

WorkQueueMod *WorkQueueMod::instance = NULL;

// Used for Disk IO only. Note that this is pretty much a FIFO and the onus of
// managing depencies rest with the caller.
WorkQueue *workQueueForIoToDisk = NULL;

Status
WorkQueue::statsToInit()
{
    Status status = StatusOk;
    StatGroupId statsGrpId;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(this->name_, &statsGrpId, 5);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->queued_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "onQueue",
                                         this->queued_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->total_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "totalQueued",
                                         this->total_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->elemRun_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "elemRun",
                                         this->elemRun_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->avgRunTimeUsecNumerator_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "runTime.average.numeratorUsec",
                                         this->avgRunTimeUsecNumerator_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->avgRunTimeUsecDenominator_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "runTime.average.denominatorUsec",
                                         this->avgRunTimeUsecDenominator_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
#ifdef DEBUG
    if (status != StatusOk) {
        assert(0 && "statsToInit Failed");
    }
#endif  // DEBUG
    return status;
}

WorkQueueMod *
WorkQueueMod::getInstance()
{
    if (instance == NULL) {
        // XXX Expected to be single threaded and called from boot strapping
        // code only.
        instance =
            (WorkQueueMod *) memAllocExt(sizeof(WorkQueueMod), moduleName);
        if (instance != NULL) {
            instance = new (instance) WorkQueueMod();
        }
    }
    return instance;
}

Status
WorkQueueMod::init(InitLevel initLevel)
{
    const unsigned numCpus = XcSysHelper::get()->getNumOnlineCores();

    // Let' pick thread count same as core count for now. However since all IO
    // is Synchronous for now, it may just make sense to have a multiple of core
    // count.
    // XXX May be this should be a config param? But take care of this later.
    workQueueForIoToDisk = WorkQueue::init("diskIo.workQueue", numCpus);
    if (workQueueForIoToDisk == NULL) {
        this->destroy();
        return StatusNoMem;
    }

    return StatusOk;
}

void
WorkQueueMod::destroy()
{
    if (workQueueForIoToDisk != NULL) {
        workQueueForIoToDisk->destroy();
        workQueueForIoToDisk = NULL;
    }

    memFree(this->instance);
    this->instance = NULL;
}

void *
WorkQueue::worker(void *wThreadIn)
{
    WorkQueue::Thread *wThread = (WorkQueue::Thread *) wThreadIn;
    WorkQueue *workQueue = wThread->workQueue;
    WorkQueue::Elt *workElt;
    int ret;
    Status status;

    while (true) {
        do {
            // XXX The constant timed waits add overhead for no good reason. Why
            // not have a queue of available threads and post one when you want
            // to have it run?
            status = sysSemTimedWait(&workQueue->sem_, WorkerWaitIntervalUs);
            if (status == StatusTimedOut) {
                if (atomicRead32(&wThread->shouldReap)) {
                    return NULL;
                }
            }
        } while (status == StatusTimedOut || status == StatusIntr);
        assert(status == StatusOk);

        workQueue->workListLock_.lock();
        if (workQueue->workListHead_ == NULL) {
            workQueue->workListLock_.unlock();
            continue;
        }

        // dequeue and update service time atomically
        workElt = workQueue->workListHead_;
        if (workElt->next == workElt) {
            workQueue->workListHead_ = NULL;
        } else {
            workQueue->workListHead_ = workElt->next;
            workElt->prev->next = workElt->next;
            workElt->next->prev = workElt->prev;
        }

        ret = clock_gettime(CLOCK_MONOTONIC_RAW, &workQueue->lastServiceTime_);
        assert(ret == 0);
        workQueue->workListLock_.unlock();

        StatsLib::statAtomicDecr32(workQueue->queued_);

        // find elapsed time for each work element to compute average elapsed
        // time.
        struct timespec startTime, endTime;
        ret = clock_gettime(CLOCK_MONOTONIC_RAW, &startTime);
        assert(ret == 0);
        workElt->func();
        // must not touch workElt after callback as it may be destroyed
        workElt = NULL;

        ret = clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
        assert(ret == 0);
        uint64_t elapTimeUsec =
            clkGetElapsedTimeInNanosSafe(&startTime, &endTime) / NSecsPerUSec;
        StatsLib::statNonAtomicAdd64(workQueue->avgRunTimeUsecNumerator_,
                                     elapTimeUsec);
        StatsLib::statNonAtomicIncr(workQueue->avgRunTimeUsecDenominator_);
        StatsLib::statNonAtomicIncr(workQueue->elemRun_);
    }

    NotReached();

    return NULL;
}

void
WorkQueue::reapWorkers()
{
    unsigned ii;
    int ret;
    WorkQueue::Thread *wThread;

    for (ii = 0; ii < this->numWorkerThreads_; ii++) {
        wThread = &this->threads_[ii];
        atomicWrite32(&wThread->shouldReap, true);
    }

    for (ii = 0; ii < this->numWorkerThreads_; ii++) {
        wThread = &this->threads_[ii];
        do {
            ret = sysThreadJoin(wThread->pthread, NULL);
            assert(ret == 0);
        } while (ret != 0);
    }

    this->numWorkerThreads_ = 0;
}

unsigned
WorkQueue::addWorkers()
{
    unsigned ii;
    Status status = StatusOk;

    if (this->numWorkerThreads_ >= WorkQueue::MaxNumWorkerThreads) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add workers (%u >= %u)",
                this->numWorkerThreads_,
                WorkQueue::MaxNumWorkerThreads);
        return 0;
    }

    for (ii = 0; ii < this->numWorkerThreads_; ii++) {
        WorkQueue::Thread *wThread = &this->threads_[ii];
        wThread->workQueue = this;
        atomicWrite32(&wThread->shouldReap, false);
        status = Runtime::get()->createBlockableThread(&wThread->pthread,
                                                       NULL,
                                                       WorkQueue::worker,
                                                       wThread);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create workerThread: %s",
                    strGetFromStatus(status));
            break;
        }
    }

    return ii;
}

void
WorkQueue::reapAllThreads()
{
    while (this->workListHead_ != NULL) {
        // don't start reaping until the work queue drains
        sysUSleep(WorkerWaitIntervalUs);
    }

    if (this->numWorkerThreads_ != NoThreadsStarted) {
        reapWorkers();
    }
}

WorkQueue *
WorkQueue::init(const char *name, unsigned initNumWorkerThreads)
{
    int ret;
    WorkQueue *workQueue = NULL;

    // Create at least WorkQueue::MinimumWorkerCount threads
    initNumWorkerThreads =
        xcMax(initNumWorkerThreads, (unsigned) WorkQueue::MinimumWorkerCount);

    workQueue = (WorkQueue *) memAllocExt(sizeof(*workQueue), moduleName);
    if (workQueue == NULL) {
        return NULL;
    }
    new (workQueue) WorkQueue();

    workQueue->workListHead_ = NULL;

    strlcpy(workQueue->name_, name, sizeof(workQueue->name_));
    workQueue->numWorkerThreads_ = initNumWorkerThreads;

    Status status = workQueue->statsToInit();
    if (status != StatusOk) {
        workQueue->destroy();
        return NULL;
    }

    ret = sem_init(&workQueue->sem_, 0, 0);
    assert(ret == 0);
    ret = clock_gettime(CLOCK_MONOTONIC_RAW, &workQueue->lastServiceTime_);
    assert(ret == 0);

    unsigned numAdded = workQueue->addWorkers();
    if (numAdded != workQueue->numWorkerThreads_) {
        workQueue->numWorkerThreads_ = numAdded;
        workQueue->destroy();
        return NULL;
    }

    return workQueue;
}

void
WorkQueue::destroy()
{
    reapAllThreads();

    sem_destroy(&sem_);
    assert(workListHead_ == NULL);

    this->~WorkQueue();
    memFree(this);
}

void
WorkQueue::enqueue(WorkQueue::Elt *workElt)
{
    this->workListLock_.lock();

    if (workListHead_ == NULL) {
        workListHead_ = workElt;
        workElt->next = workElt->prev = workElt;
    } else {
        workElt->next = workListHead_;
        workElt->prev = workListHead_->prev;

        workListHead_->prev->next = workElt;
        workListHead_->prev = workElt;
    }

    this->workListLock_.unlock();

    StatsLib::statAtomicIncr32(this->queued_);
    StatsLib::statNonAtomicIncr(this->total_);
    sem_post(&this->sem_);
}

unsigned
WorkQueue::getNumWorkerThreads()
{
    return this->numWorkerThreads_;
}
