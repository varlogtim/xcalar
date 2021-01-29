// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "Scheduler.h"
#include "runtime/Schedulable.h"
#include "runtime/Runtime.h"
#include "Timer.h"
#include "msg/Message.h"

SchedObject *Scheduler::Poison = (SchedObject *) (0xfeedbeefbeefbeef);

Scheduler::Scheduler(const char *name, Timer *timer)
    : name_(name),
      timer_(timer),
      runnableQHead(NULL),
      runnableQTail(NULL),
      runnableFiberQHead(NULL),
      runnableFiberQTail(NULL),
      notRunnableQHead(NULL),
      notRunnableQTail(NULL),
      markToAbortSchedObjs_(false)
{
    verify(sem_init(&runnableQSem, 0, 0) == 0);
}

Scheduler::~Scheduler()
{
    assert(runnableQHead == NULL);
    assert(runnableQTail == NULL);
    assert(runnableFiberQHead == NULL);
    assert(runnableFiberQTail == NULL);
    assert(notRunnableQHead == NULL);
    assert(notRunnableQTail == NULL);
    verify(sem_destroy(&runnableQSem) == 0);
}

Status
Scheduler::init()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(name_, &groupId_, StatsCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleTxnAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&txnAbortCount_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&schedulableMarkedToAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&clearAbortOfSchedObjs_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&stMarkToAbortSchedObjs_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&numRunnableSchedObjs_);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&timeSuspendedSchedObjs_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&numSuspendedSchedObjs_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "handleTxnAbort",
                                         handleTxnAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "txnAbortCount",
                                         txnAbortCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "schedulableMarkedToAbort",
                                         schedulableMarkedToAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "clearAbortOfSchedObjs",
                                         clearAbortOfSchedObjs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "markToAbortSchedObjs",
                                         stMarkToAbortSchedObjs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "numRunnableSchedObjs",
                                         numRunnableSchedObjs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "timeSuspendedSchedObjs",
                                         timeSuspendedSchedObjs_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "numSuspendedSchedObjs",
                                         numSuspendedSchedObjs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}

////////////////////////////////////////////////////////////////////////////////

bool
Scheduler::isEmpty() const
{
    bool isEmpty;
    auto guard1 = runnableQLock_.take();
    auto guard2 = notRunnableQLock_.take();
    isEmpty = (runnableQHead == NULL && runnableFiberQHead == NULL &&
               notRunnableQHead == NULL);
    return isEmpty;
}

// Search for a runnable SchedObject holding a suspended Fiber to avoid
// having to allocate a new one.
SchedObject *
Scheduler::dequeueTryPrealloc()
{
    auto guard = runnableQLock_.take();

    SchedObject *current;
    SchedObject *prev;

    for (current = runnableFiberQHead, prev = NULL; current != NULL;
         prev = current, current = current->nextRunnable_) {
        assert(current->fiber_ != NULL);

        // Remove from queue.
        if (prev == NULL) {
            assert(runnableFiberQHead == current);
            runnableFiberQHead = current->nextRunnable_;
        } else {
            prev->nextRunnable_ = current->nextRunnable_;
        }
        if (runnableFiberQTail == current) {
            assert(current->nextRunnable_ == NULL);
            runnableFiberQTail = prev;
        }
        StatsLib::statAtomicDecr64(numRunnableSchedObjs_);
        return current;
    }

    // Didn't find anything. Give sem back.
    return NULL;
}

// Search for runnable SchedObject(s) that can be Transaction aborted with
// Out of Fiber condition and ACKed to remote node.
void
Scheduler::transactionAbort()
{
    MsgMgr *msgMgr = MsgMgr::get();
    uint32_t transAbortCount = 0;
    SchedObject *prev = NULL;
    SchedObject *abortTransactionQ = NULL;

    StatsLib::statAtomicIncr64(handleTxnAbort_);

    if (msgMgr == NULL) {
        return;
    }

    runnableQLock_.lock();

    // First identify the candidates in the runnableQ that can be transaction
    // aborted.
    SchedObject *current = runnableQHead;
    while (current != NULL) {
        Schedulable *schedulable = current->getSchedulable();
        bool transAbort =
            msgMgr->transactionAbortSched(schedulable, StatusNoMem);
        if (transAbort) {
#ifdef DEBUG
            current->assertNotOnAnyWaitable();
#endif
            // Remove from queue.
            if (prev == NULL) {
                assert(runnableQHead == current);
                runnableQHead = current->nextRunnable_;
            } else {
                prev->nextRunnable_ = current->nextRunnable_;
            }
            if (runnableQTail == current) {
                assert(current->nextRunnable_ == NULL);
                runnableQTail = prev;
            }

            SchedObject *tmp = current->nextRunnable_;
            current->nextRunnable_ = abortTransactionQ;
            abortTransactionQ = current;

            transAbortCount++;
            StatsLib::statAtomicIncr64(txnAbortCount_);
            if (transAbortCount == MaxTransactAbortPerIter) {
                current = NULL;
            } else {
                current = tmp;
            }
        } else {
            prev = current;
            current = current->nextRunnable_;
        }
    }
    runnableQLock_.unlock();

    // Walk the candidates and just transaction abort the Schedulables.
    // Note that we drop the runnableQLock_.
    current = abortTransactionQ;
    while (current != NULL) {
        StatsLib::statAtomicIncr64(schedulableMarkedToAbort_);
        Schedulable *schedulable = current->getSchedulable();
        SchedObject *tmp = current->nextRunnable_;
        schedulable->done();
        current->setState(SchedObject::State::Done);
        delete current;
        current = tmp;
    }
}

// Search for a runnable SchedObject that won't block.
SchedObject *
Scheduler::dequeueTryNonBlock()
{
    auto guard = runnableQLock_.take();

    SchedObject *current;
    SchedObject *prev;

    for (current = runnableQHead, prev = NULL; current != NULL;
         prev = current, current = current->nextRunnable_) {
        if (current->getSchedulable()->isNonBlocking()) {
            // Remove from queue.
            if (prev == NULL) {
                assert(runnableQHead == current);
                runnableQHead = current->nextRunnable_;
            } else {
                prev->nextRunnable_ = current->nextRunnable_;
            }
            if (runnableQTail == current) {
                assert(current->nextRunnable_ == NULL);
                runnableQTail = prev;
            }
            StatsLib::statAtomicDecr64(numRunnableSchedObjs_);
            return current;
        }
    }

    return NULL;
}

void
Scheduler::wakeThread()
{
    verify(sem_post(&runnableQSem) == 0);
}

void
Scheduler::markToAbortSchedObjs()
{
    StatsLib::statAtomicIncr64(stMarkToAbortSchedObjs_);
    markToAbortSchedObjs_ = true;
    memBarrier();
}

void
Scheduler::clearAbortOfSchedObjs()
{
    StatsLib::statAtomicIncr64(clearAbortOfSchedObjs_);
    markToAbortSchedObjs_ = false;
    memBarrier();
}

// Make scheduling decision and return next SchedObject to be run.
SchedObject *
Scheduler::dequeue(bool fiberPrealloced)
{
    Status status;
    SchedObject *ret = NULL;
    uint64_t currentTimeUSecs = timer_->getCurrentUSecs();
    uint64_t timeElapsedUSecs = currentTimeUSecs - lastTimerCheckUSecs_;

    if (timeElapsedUSecs < Timer::LongSleepTimeUSec &&
        sem_trywait(&runnableQSem) == 0) {
        status = StatusOk;
    } else {
        uint64_t sleepTime = timer_->getSleepUSecs();
        lastTimerCheckUSecs_ = timer_->getCurrentUSecs();
        if (sleepTime > 0) {
            sleepTime = xcMin(sleepTime, (uint64_t) Timer::LongSleepTimeUSec);
            status = sysSemTimedWait(&runnableQSem, sleepTime);
        } else {
            status = StatusTimedOut;
        }
    }

    if (!Runtime::get()->isActive()) {
        goto CommonExit;
    } else if (status == StatusTimedOut) {
        timer_->wakeExpired();
        goto CommonExit;
    } else if (status == StatusOk) {
        // Dequeue.
        runnableQLock_.lock();

        if (runnableQHead && runnableQHead->getSchedulable()->isPriority()) {
            // schedule priority first
            ret = runnableQHead;

            runnableQHead = ret->nextRunnable_;
            if (runnableQHead == NULL) {
                runnableQTail = NULL;
            }
        } else if (runnableFiberQHead && runnableQHead && fiberPrealloced) {
            // If Fiber is preallocated, then just round robin between
            // Runnables with Fiber and Runnables without Fiber.
            numDequeuesPrealloced_++;
            if (numDequeuesPrealloced_ % 2) {
                // schedule pre-alloced fibers second
                ret = runnableFiberQHead;

                runnableFiberQHead = ret->nextRunnable_;
                if (runnableFiberQHead == NULL) {
                    runnableFiberQTail = NULL;
                }
            } else {
                ret = runnableQHead;

                runnableQHead = ret->nextRunnable_;
                if (runnableQHead == NULL) {
                    runnableQTail = NULL;
                }
            }
        } else if (runnableFiberQHead) {
            // schedule pre-alloced fibers second
            ret = runnableFiberQHead;

            runnableFiberQHead = ret->nextRunnable_;
            if (runnableFiberQHead == NULL) {
                runnableFiberQTail = NULL;
            }
        } else if (runnableQHead) {
            ret = runnableQHead;

            runnableQHead = ret->nextRunnable_;
            if (runnableQHead == NULL) {
                runnableQTail = NULL;
            }
        }
        if (ret != NULL) {
            StatsLib::statAtomicDecr64(numRunnableSchedObjs_);
        }
        runnableQLock_.unlock();

        if (ret == NULL) {
            goto CommonExit;
        }

#ifdef DEBUG
        ret->nextRunnable_ = Poison;
#endif
        goto CommonExit;
    } else {
        assert(false);  // Invalid status.
    }

CommonExit:
    return ret;
}

// Please note that schedObj should only be visible to the caller
// (i.e. schedObj is not shared). After the call to enqueue though,
// it's placed in the globally visible runnableQ and any thread can
// then pick it up
Status
Scheduler::enqueue(SchedObject *schedObj, SchedObject::State prevState)
{
    assert(Runtime::get()->isActive());
    assert(schedObj->getState() == SchedObject::State::Runnable);

    if (prevState == SchedObject::State::NotRunnable) {
        schedObj->getScheduler()->dequeueNotRunnable(schedObj);
    } else {
        assert(prevState == SchedObject::State::Created);
    }

    runnableQLock_.lock();

    if (schedObj->fiber_ != NULL) {
        // Enqueue into runnableFiberQ
        schedObj->nextRunnable_ = NULL;
        if (runnableFiberQTail == NULL) {
            runnableFiberQHead = schedObj;
            runnableFiberQTail = schedObj;
        } else {
            runnableFiberQTail->nextRunnable_ = schedObj;
            runnableFiberQTail = schedObj;
        }
    } else {
        // Enqueue into runnableQ.
        schedObj->nextRunnable_ = NULL;
        if (runnableQTail == NULL) {
            runnableQHead = schedObj;
            runnableQTail = schedObj;
        } else if (schedObj->getSchedulable()->isPriority()) {
            // place non blocking schedObjs at the head of the queue
            schedObj->nextRunnable_ = runnableQHead;
            runnableQHead = schedObj;
        } else {
            runnableQTail->nextRunnable_ = schedObj;
            runnableQTail = schedObj;
        }
    }

    StatsLib::statAtomicIncr64(numRunnableSchedObjs_);
    runnableQLock_.unlock();
    verify(sem_post(&runnableQSem) == 0);

    return StatusOk;
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods dealing with queue or not-runnables.
//

// Like makeRunnable, makeNotRunnable assumes that schedObj is only
// visible to the calling thread. You would call makeNotRunnable on
// a live fiber/thread on a call to a blocking primitive (e.g. semWait).
// In that case, before you enqueue into that blocking primitve's queue
// thereby making it globally visible, make the call to notRunnable first.
// Also, the notRunnableQueue is for reference only. Do not rely on it
// to make scheduling decisions. Rely on the respective's blocking primitive's
// queue for the single source of truth.
void
Scheduler::makeNotRunnable(SchedObject *schedObj)
{
    assert(schedObj->getState() == SchedObject::State::NotRunnable);

    auto guard = notRunnableQLock_.take();

    if (notRunnableQHead == NULL) {
        assert(notRunnableQTail == NULL);
        notRunnableQHead = notRunnableQTail = schedObj;
        schedObj->nextNotRunnable_ = NULL;
        schedObj->prevNotRunnable_ = NULL;
    } else {
        schedObj->nextNotRunnable_ = notRunnableQHead;
        notRunnableQHead->prevNotRunnable_ = schedObj;
        schedObj->prevNotRunnable_ = NULL;
        notRunnableQHead = schedObj;
    }
}

void
Scheduler::dequeueNotRunnable(SchedObject *schedObj)
{
    auto guard = notRunnableQLock_.take();

    if (schedObj->prevNotRunnable_ == NULL &&
        schedObj->nextNotRunnable_ == NULL) {
        assert(notRunnableQHead == schedObj);
        assert(notRunnableQTail == schedObj);

        notRunnableQHead = NULL;
        notRunnableQTail = NULL;
    } else if (schedObj->prevNotRunnable_ == NULL) {
        assert(schedObj->nextNotRunnable_ != NULL);
        assert(notRunnableQHead == schedObj);

        notRunnableQHead = schedObj->nextNotRunnable_;
        notRunnableQHead->prevNotRunnable_ = NULL;
    } else if (schedObj->nextNotRunnable_ == NULL) {
        assert(schedObj->prevNotRunnable_ != NULL);
        assert(notRunnableQTail == schedObj);

        notRunnableQTail = schedObj->prevNotRunnable_;
        notRunnableQTail->nextNotRunnable_ = NULL;
    } else {
        schedObj->prevNotRunnable_->nextNotRunnable_ =
            schedObj->nextNotRunnable_;
        schedObj->nextNotRunnable_->prevNotRunnable_ =
            schedObj->prevNotRunnable_;
    }

#ifdef DEBUG
    schedObj->nextNotRunnable_ = Poison;
    schedObj->prevNotRunnable_ = Poison;
#endif
}
