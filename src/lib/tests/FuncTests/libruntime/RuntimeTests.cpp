// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <new>
#include <assert.h>
#include <semaphore.h>

#include "primitives/Primitives.h"
#include "test/FuncTests/RuntimeTests.h"
#include "runtime/Runtime.h"
#include "Timer.h"
#include "SchedObject.h"
#include "FiberSchedThread.h"
#include "util/Atomics.h"
#include "runtime/Tls.h"
#include "util/MemTrack.h"
#include "LibRuntimeFuncTestConfig.h"
#include "test/QA.h"
#include "util/System.h"

//
// Test initialization. An instance of RuntimeTests is created and torn down
// for every test case.
//

static constexpr const char *moduleName = "RuntimeTests";
constexpr RuntimeTests::Params RuntimeTests::paramsSanity;
constexpr RuntimeTests::Params RuntimeTests::paramsStress;
RuntimeTests::Params RuntimeTests::paramsCustom = paramsSanity;

RuntimeTests::RuntimeTests(const RuntimeTests::Params &params)
    : producerThreadSem_(0),
      dedicatedThreadSem_(0),
      progressProduce_(0),
      progressConsume_(0),
      switchySem_(params.switchySemInit),
      jumpySems_(NULL),
      crowdSems_(NULL),
      crowdCounts_(NULL),
      crowdDone_(0),
      condvarCountWaiting_(0),
      p_(params)
{
    atomicWrite64(&workProduced_, 0);
    atomicWrite32(&condvarWorkerDone_, 0);
}

RuntimeTests::~RuntimeTests() {}

// Save configured params into global static var for testCustom.
Status  // static
RuntimeTests::parseConfig(Config::Configuration *config,
                          char *key,
                          char *value,
                          bool stringentRules)
{
    if (strcasecmp(key,
                   strGetFromLibRuntimeFuncTestConfig(LibRuntimeInnerIters)) ==
        0) {
        paramsCustom.innerIters = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeDedicatedThreadCount)) == 0) {
        paramsCustom.dedicatedThreadCount = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeWorkPerDedicatedThread)) == 0) {
        paramsCustom.workPerDedicatedThread = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeProducerCount)) == 0) {
        paramsCustom.producerCount = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeWorkCountLazy)) == 0) {
        paramsCustom.workCountLazy = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeWorkCountSwitchy)) == 0) {
        paramsCustom.workCountSwitchy = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeWorkCountStacky)) == 0) {
        paramsCustom.workCountStacky = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeWorkCountSleepy)) == 0) {
        paramsCustom.workCountSleepy = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeSwitchyIters)) == 0) {
        paramsCustom.switchyIters = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeSwitchySemInit)) == 0) {
        paramsCustom.switchySemInit = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeFibDepth)) == 0) {
        paramsCustom.fibDepth = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeJumpyIters)) == 0) {
        paramsCustom.jumpyIters = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeJumpyStride)) == 0) {
        paramsCustom.jumpyStride = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeJumpySchedObjs)) == 0) {
        paramsCustom.jumpySchedObjs = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeJumpyTimeout)) == 0) {
        paramsCustom.jumpyTimeout = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCrowdSize)) == 0) {
        paramsCustom.crowdSize = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCrowdSems)) == 0) {
        paramsCustom.crowdSems = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCrowdSemInit)) == 0) {
        paramsCustom.crowdSemInit = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCrowdTimeout)) == 0) {
        paramsCustom.crowdTimeout = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCrowdIters)) == 0) {
        paramsCustom.crowdIters = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCondvarSchedObjs)) == 0) {
        paramsCustom.condvarSchedObjs = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibRuntimeFuncTestConfig(
                              LibRuntimeCondvarIters)) == 0) {
        paramsCustom.condvarIters = strtoll(value, NULL, 0);
    } else {
        return StatusUsrNodeIncorrectParams;
    }
    return StatusOk;
}

////////////////////////////////////////////////////////////////////////////////

//
// Test cases.
//

Status  // static
RuntimeTests::testSanity()
{
    RuntimeTests runtimeTests(RuntimeTests::paramsSanity);
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "RuntimeTests::testSanity Start.");

    status = runtimeTests.run();

    xSyslog(moduleName,
            XlogInfo,
            "RuntimeTests::testSanity Done with %s",
            strGetFromStatus(status));

    return status;
}

Status  // static
RuntimeTests::testStress()
{
    RuntimeTests runtimeTests(RuntimeTests::paramsStress);
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "RuntimeTests::testStress Start.");

    status = runtimeTests.run();

    xSyslog(moduleName,
            XlogInfo,
            "RuntimeTests::testStress Done with %s",
            strGetFromStatus(status));

    return status;
}

Status  // static
RuntimeTests::testCustom()
{
    RuntimeTests runtimeTests(RuntimeTests::paramsCustom);
    Status status = StatusOk;

    xSyslog(moduleName, XlogInfo, "RuntimeTests::testCustom Start.");

    status = runtimeTests.run();

    xSyslog(moduleName,
            XlogInfo,
            "RuntimeTests::testCustom Done with %s",
            strGetFromStatus(status));

    return status;
}

////////////////////////////////////////////////////////////////////////////////

//
// Fun little work functions that exhibit different behaviors interesting to the
// runtime. A unit of "work" means executing one of these once.
//

// Test function that doesn't do much of anything.
void
RuntimeTests::lazy()
{
    Semaphore sem(2);
    sem.semWait();
    for (volatile unsigned ii = 0; ii < p_.innerIters; ii++) {
        if (ii % 4 == 1) {
            ii += 1;
        }
    }
    sem.post();

    progressConsume_.post();
}

// Test function that triggers many context switches.
void
RuntimeTests::switchy()
{
    for (unsigned jj = 0; jj < p_.switchyIters; jj++) {
        switchySem_.semWait();
        // Do something pointless but cycle consuming.
        for (volatile unsigned ii = 0; ii < p_.innerIters; ii++) {
            if (ii % 4 == 1) {
                ii += 1;
            }
        }
        switchySem_.post();

        switchySem_.semWait();
        switchySem_.post();
    }

    progressConsume_.post();
}

// Use up that stack!
int
RuntimeTests::fib(int n)
{
    if (n == 0 || n == 1) {
        return 1;
    }
    if (n == 20) {
        // Only one thread may be executing fib(20) at a time. This is very
        // important.
        fib20_.lock();
        int val = fib(n - 1) + fib(n - 2);
        fib20_.unlock();
        return val;
    }

    return fib(n - 1) + fib(n - 2);
}

// Test function that uses a lot of stack space.
void
RuntimeTests::stacky()
{
    fib(rand() % p_.fibDepth);

    progressConsume_.post();
}

void
RuntimeTests::sleepy()
{
    uint64_t sleepyUSecs = (rand() % (USecsPerSec * 2)) + 1;

    uint64_t before =
        Runtime::get()->getTimer(Runtime::SchedId::Sched0)->getCurrentUSecs();
    Runtime::get()->getTimer(Runtime::SchedId::Sched0)->wait(sleepyUSecs);
    volatile uint64_t after =
        Runtime::get()->getTimer(Runtime::SchedId::Sched0)->getCurrentUSecs();

    // There's too much other crap going on for this to be woken in a
    // reasonable amount of time.
    uint64_t diff = after - before;
    assert(diff >= sleepyUSecs);

    progressConsume_.post();
}

////////////////////////////////////////////////////////////////////////////////

//
// Methods for testing wait multiple.
//

// Jumpy tests that a waitMultiple wakes the "correct" semaphores.

void
RuntimeTests::Jumpy::run()
{
    unsigned ii;

    // Get running SchedObject.
    FiberSchedThread *thread = FiberSchedThread::getRunningThread();
    assert(thread != NULL);
    SchedObject *schedObj = thread->getRunningSchedObj();
    assert(schedObj != NULL);

    // Setup args to waitMultiple;
    Waitable *waitables[p_.jumpyStride + 1];
    WaitArg waitArgs[p_.jumpyStride + 1];

    for (ii = 0; ii < p_.jumpyStride; ii++) {
        waitables[ii] = &sems_[index_ * p_.jumpyStride + ii];
    }
    waitables[ii] = Runtime::get()->getTimer(Runtime::SchedId::Sched0);
    waitArgs[ii].timer.usecsRel = p_.jumpyTimeout;

    // Wait.
    Waitable *waker;
    verifyOk(schedObj->waitMultiple(waitables,
                                    p_.jumpyStride + 1,
                                    waitArgs,
                                    &waker));

    // Determine who awoke us.
    for (ii = 0; ii < p_.jumpyStride; ii++) {
        if (waitables[ii] == waker) {
            break;
        }
    }
    assert(ii < p_.jumpyStride);  // Woke by one of the sems.

    // Post next sem in the chain, determined by who awoke us.
    unsigned nextIndex = (index_ + 1);
    sems_[nextIndex * p_.jumpyStride + ii].post();
}

void
RuntimeTests::jumpy()
{
    unsigned ii;
    const unsigned jumpySemCount = p_.jumpyStride * (p_.jumpySchedObjs + 1);
    assert(jumpySems_ == NULL);
    jumpySems_ = new (std::nothrow) Semaphore[jumpySemCount];
    assert(jumpySems_ != NULL);

    for (ii = 0; ii < jumpySemCount; ii++) {
        jumpySems_[ii].init(0);
    }

    for (ii = 0; ii < p_.jumpySchedObjs; ii++) {
        Jumpy *jumpy = new (std::nothrow) Jumpy(jumpySems_, ii, p_);
        assert(jumpy != NULL);
        verifyOk(Runtime::get()->schedule(jumpy));
    }

    // Poke a random sem in the range [0, stride). Verify that corresponding
    // sem in range [stride * jumpySchedObjs, stride * (jumpySchedObjs + 1) gets
    // posted.
    unsigned poke = rand() % p_.jumpyStride;
    jumpySems_[poke].post();
    verifyOk(jumpySems_[p_.jumpyStride * p_.jumpySchedObjs + poke].timedWait(
        p_.jumpyTimeout));

    delete[] jumpySems_;
    jumpySems_ = NULL;
}

// Crowd tests test that waitMultiple doesn't result in a too small or too large
// number of sem wakes.

void
RuntimeTests::crowd()
{
    unsigned ii;

    // Get running SchedObject.
    FiberSchedThread *thread = FiberSchedThread::getRunningThread();
    assert(thread != NULL);
    SchedObject *schedObj = thread->getRunningSchedObj();
    assert(schedObj != NULL);

    // Setup args to waitMultiple;
    Waitable *waitables[p_.crowdSems + 1];
    WaitArg waitArgs[p_.crowdSems + 1];

    for (ii = 0; ii < p_.crowdSems; ii++) {
        waitables[ii] = &crowdSems_[ii];
    }
    waitables[ii] = Runtime::get()->getTimer(Runtime::SchedId::Sched0);
    waitArgs[ii].timer.usecsRel = p_.crowdTimeout;

    // Wait.
    Waitable *waker;
    Status status =
        schedObj->waitMultiple(waitables, p_.crowdSems + 1, waitArgs, &waker);
    if (status == StatusTimedOut) {
        assert(waker == Runtime::get()->getTimer(Runtime::SchedId::Sched0));
        crowdDone_.post();
        return;
    }

    Semaphore *wakeSem = dynamic_cast<Semaphore *>(waker);
    assert(wakeSem != NULL);

    // Which sem woke us up?
    for (ii = 0; ii < p_.crowdSems; ii++) {
        if (&crowdSems_[ii] == wakeSem) {
            break;
        }
    }
    assert(ii < p_.crowdSems);

    crowdLock_.lock();

    atomicInc32(&crowdCounts_[ii]);
    assert((unsigned) atomicRead32(&crowdCounts_[ii]) <= p_.crowdSemInit);

    crowdFull_.wait(&crowdLock_);
    crowdLock_.unlock();
    crowdDone_.post();  // Tell genCrowd we're done.
}

void
RuntimeTests::genCrowd()
{
    unsigned ii;

    assert(crowdSems_ == NULL);
    crowdSems_ = new (std::nothrow) Semaphore[p_.crowdSems];
    assert(crowdSems_ != NULL);

    assert(crowdCounts_ == NULL);
    crowdCounts_ =
        (Atomic32 *) memAlloc(p_.crowdSems * sizeof(crowdCounts_[0]));
    assert(crowdCounts_ != NULL);
    memZero(crowdCounts_, p_.crowdSems * sizeof(crowdCounts_[0]));

    for (ii = 0; ii < p_.crowdSems; ii++) {
        crowdSems_[ii].init(p_.crowdSemInit);
    }

    for (ii = 0; ii < p_.crowdSize; ii++) {
        TestSchedulable *sched =
            new (std::nothrow) TestSchedulable(this, &RuntimeTests::crowd);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
    }

    // Busy wait until alls sems full.
    for (ii = 0; ii < p_.crowdSems; ii++) {
        while ((unsigned) atomicRead32(&crowdCounts_[ii]) < p_.crowdSemInit) {
        }
        assert((unsigned) atomicRead32(&crowdCounts_[ii]) == p_.crowdSemInit);
    }

    crowdLock_.lock();
    crowdFull_.broadcast();
    crowdLock_.unlock();

    // Wait for others to time out.
    for (ii = 0; ii < p_.crowdSize; ii++) {
        crowdDone_.semWait();
    }

    memFree(crowdCounts_);
    crowdCounts_ = NULL;
    delete[] crowdSems_;
    crowdSems_ = NULL;
}

void
RuntimeTests::condvarWorker()
{
    for (unsigned ii = 0; ii < p_.condvarIters; ii++) {
        condvarLock_.lock();
        condvarCountWaiting_++;
        condvar_.wait(&condvarLock_);

        condvarCountWaiting_--;
        condvar_.wait(&condvarLock_);
        condvarLock_.unlock();
    }
    assert((unsigned) atomicRead32(&condvarWorkerDone_) < p_.condvarSchedObjs);
    atomicInc32(&condvarWorkerDone_);
}

void
RuntimeTests::condvar()
{
    unsigned ii;

    atomicWrite32(&condvarWorkerDone_, 0);
    condvarCountWaiting_ = 0;

    for (ii = 0; ii < p_.condvarSchedObjs; ii++) {
        TestSchedulable *sched = new (std::nothrow)
            TestSchedulable(this, &RuntimeTests::condvarWorker);
        assert(sched != NULL);

        Status status = Runtime::get()->schedule(sched);
        assert(status == StatusOk);
    }

    for (unsigned jj = 0; jj < p_.condvarIters; jj++) {
        // Wait till all are at wait.
        condvarLock_.lock();
        while (condvarCountWaiting_ != p_.condvarSchedObjs) {
            condvarLock_.unlock();
            condvarLock_.lock();
        }
        condvarLock_.unlock();

        // Release and wait till all are waiting again.
        condvar_.broadcast();
        condvarLock_.lock();
        while (condvarCountWaiting_ != 0) {
            condvarLock_.unlock();
            condvarLock_.lock();
        }
        condvarLock_.unlock();

        // Release via signal.
        condvarLock_.lock();
        for (ii = 0; ii < p_.condvarSchedObjs; ii++) {
            condvar_.signal();
        }
        condvarLock_.unlock();
    }

    // Wait till all exited.
    while ((unsigned) atomicRead32(&condvarWorkerDone_) < p_.condvarSchedObjs) {
        sysUSleep(100000);  // 100 milli seconds
    }
    assert((unsigned) atomicRead32(&condvarWorkerDone_) == p_.condvarSchedObjs);
}

////////////////////////////////////////////////////////////////////////////////

struct ThreadArgs {
    Semaphore *sem;
};

//
// Test implementation: doing work in parallel in different ways.
//

Status
RuntimeTests::run()
{
    Status status;
    unsigned ii;
    Schedulable *sched;

    //
    // Spawn all necessary threads up-front
    //
    unsigned producerThreadCount = 0;
    unsigned producerThreadCountSuggested = p_.producerCount / 2;
    unsigned dedicatedThreadCount = 0;
    unsigned dedicatedThreadCountSuggested = p_.dedicatedThreadCount;

    pthread_t producerThreads[producerThreadCountSuggested];
    pthread_t dedicatedThreads[dedicatedThreadCountSuggested];

    //
    // Producer threads
    //
    for (ii = 0; ii < producerThreadCountSuggested; ii++) {
        status = Runtime::get()
                     ->createBlockableThread(&producerThreads[ii],
                                             this,
                                             &RuntimeTests::produceWorkWrapper);
        if (status == StatusOk) {
            producerThreadCount++;
        } else {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
            break;
        }
    }
    assert(producerThreadCount >= 1);
    xSyslog(moduleName,
            XlogInfo,
            "%u producer threads created",
            producerThreadCount);

    //
    // Dedicated threads
    //

    for (ii = 0; ii < dedicatedThreadCountSuggested; ii++) {
        status = Runtime::get()
                     ->createBlockableThread(&dedicatedThreads[ii],
                                             this,
                                             &RuntimeTests::dedicatedThread);
        if (status == StatusOk) {
            dedicatedThreadCount++;
        } else {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
            break;
        }
    }
    assert(dedicatedThreadCount >= 1);
    xSyslog(moduleName,
            XlogInfo,
            "%u dedicated threads created",
            dedicatedThreadCount);

    //
    // All necessary threads have been created. Time to start the test
    //

    //
    // Kick off threads/fibers that produce work.
    //
    for (ii = 0; ii < producerThreadCount; ii++) {
        producerThreadSem_.post();
    }

    unsigned producerFiberCount = p_.producerCount - producerThreadCount;
    for (ii = 0; ii < producerFiberCount; ii++) {
        sched = new (std::nothrow)
            TestSchedulable(this, &RuntimeTests::produceWork);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
    }

    // Wait for producers to finish.
    for (ii = 0; ii < producerThreadCount; ii++) {
        verify(sysThreadJoin(producerThreads[ii], NULL) == 0);
    }

    assert(p_.producerCount == producerFiberCount + producerThreadCount);
    for (ii = 0; ii < p_.producerCount; ii++) {
        progressProduce_.semWait();
    }

    //
    // Interleave some dedicated threads with Fiber scheduled threads.
    //
    for (ii = 0; ii < dedicatedThreadCount; ii++) {
        dedicatedThreadSem_.post();
    }

    //
    // Do miscellaneous things.
    //

    ShimSchedulable shim(this, &RuntimeTests::jumpy);  // Reused.
    for (ii = 0; ii < p_.jumpyIters; ii++) {
        verifyOk(Runtime::get()->schedule(&shim));
        shim.done_.semWait();
    }

    if (Runtime::get()->getThreadsCount(Runtime::SchedId::Sched0) > 1) {
        ShimSchedulable shim(this, &RuntimeTests::genCrowd);  // Reused.
        for (ii = 0; ii < p_.crowdIters; ii++) {
            verifyOk(Runtime::get()->schedule(&shim));
            shim.done_.semWait();
        }
    }

    condvar();

    //
    // Wait for all work to be consumed.
    //

    for (ii = 0; ii < dedicatedThreadCount; ii++) {
        void *ret;
        verify(sysThreadJoin(dedicatedThreads[ii], &ret) == 0);
        assert(ret == (void *) DedicatedThreadReturn);
    }

    unsigned workToConsume = (unsigned) atomicRead64(&workProduced_);

    for (ii = 0; ii < workToConsume; ii++) {
        progressConsume_.semWait();
    }
    // Check that exactly the right amount of work was consumed.
    verify(!progressConsume_.tryWait());

    return StatusOk;
}

void
RuntimeTests::produceWork()
{
    unsigned ii;
    unsigned countProduced = 0;
    TestSchedulable *sched;

    for (ii = 0; ii < p_.workCountLazy; ii++) {
        sched = new (std::nothrow) TestSchedulable(this, &RuntimeTests::lazy);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
        countProduced++;
    }

    for (ii = 0; ii < p_.workCountSwitchy; ii++) {
        sched =
            new (std::nothrow) TestSchedulable(this, &RuntimeTests::switchy);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
        countProduced++;
    }

    for (ii = 0; ii < p_.workCountStacky; ii++) {
        sched = new (std::nothrow) TestSchedulable(this, &RuntimeTests::stacky);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
        countProduced++;
    }

    for (ii = 0; ii < p_.workCountSleepy; ii++) {
        sched = new (std::nothrow) TestSchedulable(this, &RuntimeTests::sleepy);
        assert(sched != NULL);
        verifyOk(Runtime::get()->schedule(sched));
        countProduced++;
    }

    atomicAdd64(&workProduced_, countProduced);
    progressProduce_.post();
}

void *
RuntimeTests::produceWorkWrapper()
{
    // Wait until test master lets us know its ready to start
    producerThreadSem_.semWait();

    produceWork();
    return NULL;
}

void *
RuntimeTests::dedicatedThread()
{
    // Wait until test master lets us know its ready to start
    dedicatedThreadSem_.semWait();

    for (unsigned ii = 0; ii < p_.workPerDedicatedThread; ii++) {
        lazy();
        stacky();
        switchy();
        sleepy();
        atomicAdd64(&workProduced_, 4);
    }
    return (void *) DedicatedThreadReturn;
}
