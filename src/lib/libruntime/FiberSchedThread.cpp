// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "FiberSchedThread.h"
#include "runtime/Runtime.h"
#include "Fiber.h"
#include "FiberCache.h"
#include "Timer.h"
#include "runtime/Schedulable.h"
#include "SchedObject.h"
#include "Scheduler.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "RuntimeStats.h"

thread_local FiberSchedThread *FiberSchedThread::runningThread_ = NULL;

FiberSchedThread::~FiberSchedThread()
{
    assert(runningFiber_ == NULL);
    assert(preallocedFiber_ == NULL);
}

Status
FiberSchedThread::create(RuntimeType runtimeType, Runtime::SchedId schedId)

{
    assert(this->state == Thread::State::Invalid);

    this->runtimeType_ = runtimeType;
    this->schedId_ = schedId;

    assert(this->preallocedFiber_ == NULL);
    Runtime::get()->getFiberCache()->getWithAlloc(&this->preallocedFiber_);
    if (this->preallocedFiber_ == NULL) {
        return StatusNoMem;
    }

    int ret = sysThreadCreate(&thread_,
                              NULL,
                              &Thread::threadEntryPointWrapper,
                              static_cast<Thread *>(this));
    if (ret != 0) {
        delete this->preallocedFiber_;
        this->preallocedFiber_ = NULL;
        return sysErrnoToStatus(ret);
    }

    return StatusOk;
}

void
FiberSchedThread::destroy()
{
    verify(sysThreadJoin(thread_, NULL) == 0);
    if (this->runningFiber_ != NULL) {
        this->runningFiber_->retire();
        Runtime::get()->getFiberCache()->put(this->runningFiber_);
        this->runningFiber_ = NULL;
    }
}

// Entrypoint running in context of newly created pthread. Simply creates a
// Fiber and jumps into its context.
// XXX Original pthread context is wasted.
void  // virtual
FiberSchedThread::threadEntryPoint()
{
    // Need a way to determine what thread we're running on regardless of fiber.
    runningThread_ = this;

    this->runningFiber_ = this->preallocedFiber_;
    this->preallocedFiber_ = NULL;

    this->state = Thread::State::NotRunning;
    Fiber::contextSwitch(NULL, this->runningFiber_, false);
}

////////////////////////////////////////////////////////////////////////////////

//
// Accessors.
//

Fiber *
FiberSchedThread::getRunningFiber()
{
    return runningFiber_;
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods involved in executing a SchedObject.
//

SchedObject *  // virtual
FiberSchedThread::getRunningSchedObj()
{
    // So we don't have to synchronize anything:
    assert(this == FiberSchedThread::getRunningThread());

    if (this->runningFiber_ == NULL) {
        return NULL;
    }
    return this->runningFiber_->getCurrentSchedObj();
}

// Main Runtime loop executed in each Fiber. Cannot be an member function
// because Fibers can jump threads.
void  // static
FiberSchedThread::main()
{
    Runtime *runtime = Runtime::get();
    RuntimeStats *stats = runtime->getStats();
    FiberSchedThread *thr = FiberSchedThread::getRunningThread();
    Scheduler *scheduler = runtime->getSched(thr->schedId_);
    thr->state = Thread::State::NotRunning;

    while (runtime->isActive() && !thr->markedToExit_) {
        if (scheduler->isMarkedToAbortSchedObjs()) {
            StatsLib::statAtomicIncr64(stats->fiberSchedThreadTxnAbort_);
            scheduler->clearAbortOfSchedObjs();
            scheduler->transactionAbort();
        }

        runtime->getTimer(thr->schedId_)->wakeExpired();

        // Preallocate new fiber for later. We may need to switch to a new Fiber
        // if execution of this one is suspended. In order to make forward
        // progress during a context switch (avoid fiber allocation failures
        // downstream), we pre-allocate that Fiber here.
        if (thr->preallocedFiber_ == NULL) {
            // Get a fiber from Fiber Cache. Note that Fiber Cache could be
            // empty.
            runtime->getFiberCache()->get(&thr->preallocedFiber_);
        }

        SchedObject *schedObj = NULL;

        if (unlikely(thr->preallocedFiber_ == NULL)) {
            // Don't have our preallocedFiber_, can't do anything that could
            // require one. So find a SchedObject with a Fiber already attached.
            // This typically happens with a suspended Fiber becomes runnable
            // again.
            schedObj = runtime->getSched(thr->schedId_)->dequeueTryPrealloc();
            if (schedObj == NULL) {
                // These SchedObjects have been marked "non-blocking" and won't
                // need a prealloced Fiber. It's up to whoever enqueued the
                // SchedObject to guarantee this is true (will panic otherwise).
                schedObj =
                    runtime->getSched(thr->schedId_)->dequeueTryNonBlock();
            }
        }

        if (unlikely(thr->preallocedFiber_ == NULL) && schedObj == NULL) {
            // If we still can't proceed, do a fiberCache allocation with
            // malloc. Note that this may fail when we are low on resources.
            runtime->getFiberCache()->getWithAlloc(&thr->preallocedFiber_);

            if (unlikely(thr->preallocedFiber_ == NULL)) {
                // Now is the opportunity to transaction abort for out of fiber
                // scenario. Note that this is aggressive and aborts all
                // qualified candidates.
                StatsLib::statAtomicIncr64(stats->fiberSchedThreadTxnAbort_);
                runtime->getSched(thr->schedId_)->transactionAbort();
                sysUSleep(AllocFailureSleepUSecs);
            }
            continue;
        }

        if (schedObj == NULL) {
            schedObj = runtime->getSched(thr->schedId_)
                           ->dequeue(thr->preallocedFiber_ != NULL);
            if (schedObj == NULL) {
                continue;
            }
        }

        thr->state = Thread::State::Running;
        if (schedObj->fiber_ != NULL) {
            // We no longer need the currently running Fiber because this one is
            // now runnable!
            thr->resumeFiber(schedObj->fiber_, true);
        } else {
            thr->runningFiber_->run(schedObj);
        }

        // Prepare for next iteration. It's important to re-grab our "current
        // thread" pointer here. The executingFiber may have switch threads
        // during execution (due to suspend/resume).
        memBarrier();
        thr = FiberSchedThread::getRunningThread();
        thr->state = Thread::State::NotRunning;
    }

    thr = FiberSchedThread::getRunningThread();

    if (thr->preallocedFiber_ != NULL) {
        runtime->getFiberCache()->put(thr->preallocedFiber_);
        thr->preallocedFiber_ = NULL;
    }

    thr->state = Thread::State::NotRunning;
    pthread_exit(NULL);
}

void  // virtual
FiberSchedThread::suspendRunningSchedObj()
{
    //
    // We are now officially switching context to another Fiber. Cannot fail
    // from here until welcomeBack completes.
    //

    assert(this == FiberSchedThread::getRunningThread());
    assert(this->state == Thread::State::Running);
    assert(this->preallocedFiber_ != NULL);

    if (getRunningFiber()
            ->getCurrentSchedObj()
            ->getSchedulable()
            ->isNonBlocking()) {
        // @SymbolCheckIgnore
        panic("Non-blocking SchedObject being suspended");
    }

#ifdef RUNTIME_DEBUG
    xSyslog(ModuleName,
            XlogDebug,
            "[thread %x] Suspending Fiber %llx",
            this->threadId_,
            (unsigned long long) runningFiber_->getId());
#endif  // RUNTIME_DEBUG

    //
    // Pick up a SchedObject to run next. We do this here to avoid using up the
    // pre-alloced Fiber if not necessary.
    //

    // XXX Prefer resuming a SchedObj that already has a stack to reduce
    //     allocations. This is probably over-kill.

    SchedObject *schedObj = NULL;

    schedObj = Runtime::get()->getSched(schedId_)->dequeueTryPrealloc();
    if (schedObj != NULL) {
        // Resume previous Fiber without recycling current (because we're
        // suspending it).
        this->resumeFiber(schedObj->fiber_, false);
    } else {
        // Switch back to Fiber executing the main loop. It'll decide how best
        // to precede.
        Fiber *prevFiber = this->runningFiber_;
        this->runningFiber_ = this->preallocedFiber_;
        this->preallocedFiber_ = NULL;

        Fiber::contextSwitch(prevFiber, this->runningFiber_, false);
    }

    // Welcome back!
    FiberSchedThread::getRunningThread()->state = Thread::State::Running;
}

void
FiberSchedThread::resumeFiber(Fiber *fiber, bool recycleRunningFiber)
{
    assert(this == FiberSchedThread::getRunningThread());

#ifdef RUNTIME_DEBUG
    xSyslog(ModuleName,
            XlogDebug,
            "[thread %x] Resume Fiber %llx",
            this->threadId_,
            (unsigned long long) fiber->getId());
#endif  // RUNTIME_DEBUG

    Fiber *prevFiber = this->runningFiber_;
    this->runningFiber_ = fiber;
    Fiber::contextSwitch(prevFiber, this->runningFiber_, recycleRunningFiber);

    // Welcome back!
    assert(!recycleRunningFiber);
}

void
FiberSchedThread::cleanupFiber(Fiber *fiber)
{
    if (this->preallocedFiber_ == NULL) {
        fiber->reset();
        this->preallocedFiber_ = fiber;
    } else {
        Runtime::get()->getFiberCache()->put(fiber);
    }
}

void
FiberSchedThread::markToExit()
{
    markedToExit_ = true;
    memBarrier();
}

void
FiberSchedThread::logFinish(Stats *stats)
{
    DCHECK(runningThread_ == this);
    DCHECK(stats != nullptr);

    Stopwatch watch;
    auto guard = pstats_.lock_.take();
    stats->incLockingTime(watch.getCurElapsedNSecs() / 1000);

    pstats_.opStats_[stats->name()].push(*stats);
}

void
FiberSchedThread::PerfStats::OpStats::push(const Stats &stats)
{
    // 1st step: evict the oldest 1s periods. We can use the given tstamp as the
    //           "now" value and save the clock call.
    const auto now = stats.counters().finish;
    while (!periods_.empty()) {
        // Deal with exceptions in the finish() path.
        if (periods_.front().samples.empty()) {
            periods_.pop_front();
            continue;
        }

        if (now - periods_.front().samples.front().tstamp <
            std::chrono::seconds(60)) {
            break;
        }
        periods_.pop_front();
    }

    // 2nd step: add this new sample. We are maintaining 1s periods.
    if (likely(!periods_.empty())) {
        // Check whether we are hitting the same 1s period. If not, start a new
        // period.
        if (!periods_.back().samples.empty() &&
            std::chrono::floor<std::chrono::seconds>(
                periods_.back().samples.front().tstamp) !=
                std::chrono::floor<std::chrono::seconds>(now)) {
            periods_.push_back({});
        }
    } else {
        periods_.push_back({});
    }

    periods_.back().samples.push_back(
        Sample{stats.counters().finish,
               std::chrono::duration_cast<std::chrono::microseconds>(
                   stats.counters().finish - stats.counters().start),
               std::chrono::microseconds(stats.counters().suspendedTimeus),
               stats.counters().suspensions,
               std::chrono::microseconds(stats.counters().infraLockingTimeus)});
}

void
FiberSchedThread::extractRuntimeStats(std::map<std::string, stats::Digest> *map)
{
    DCHECK(map != nullptr);

    const auto now = std::chrono::steady_clock::now();
    auto guard = pstats_.lock_.take();

    // Go through the periods and extract samples.
    for (auto &entry : pstats_.opStats_) {
        DCHECK(!entry.first.empty());
        auto &stats = entry.second;

        // 1st step: evict the oldest 1s periods that are beyond 60s.
        while (!stats.periods_.empty()) {
            // Deal with exceptions in the finish() path.
            if (stats.periods_.front().samples.empty()) {
                stats.periods_.pop_front();
                continue;
            }

            if (now - stats.periods_.front().samples.front().tstamp <
                std::chrono::seconds(60)) {
                break;
            }
            stats.periods_.pop_front();
        }

        if (stats.periods_.empty()) continue;
        auto &digest = (*map)[entry.first];

        // Export the individual samples.
        for (const auto &period : stats.periods_) {
            for (auto &sample : period.samples) {
                digest.infraTime.push_back(sample.lockingTime.count());
                digest.duration.push_back(sample.duration.count());
                digest.suspendedTime.push_back(sample.suspendedTime.count());
                digest.suspensions.push_back(sample.suspensions);
            }
        }
    }
}
