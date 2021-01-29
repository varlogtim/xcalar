// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RUNTIME_TESTS_H_
#define _RUNTIME_TESTS_H_

#include "primitives/Primitives.h"
#include "runtime/Semaphore.h"
#include "runtime/Mutex.h"
#include "runtime/CondVar.h"
#include "runtime/Schedulable.h"
#include "util/AtomicTypes.h"
#include "config/Config.h"

class RuntimeTests final
{
  public:
    struct Params {
        // Number of times to do a very simple operation in the inner loop of a
        // test function. Should be very large.
        unsigned innerIters;

        // Work done on dedicated threads is interleaved with work done on
        // Fibers. This controls the number of those threads, and the amount
        // of work done by each.
        unsigned dedicatedThreadCount;
        unsigned workPerDedicatedThread;

        // Number of threads/Fibers producing work in parallel.
        unsigned producerCount;

        // How many units of work executing each personality.
        unsigned workCountLazy;
        unsigned workCountSwitchy;
        unsigned workCountStacky;
        unsigned workCountSleepy;

        // Switchy-specific params.
        unsigned switchyIters;    // How many sem ops does a single work do?
        unsigned switchySemInit;  // Value of switchy sem.

        // Fib-specific params.
        unsigned fibDepth;  // Controls number of stack frames.

        // Jumpy-specific params.
        unsigned jumpyIters;
        unsigned jumpyStride;
        unsigned jumpySchedObjs;
        unsigned jumpyTimeout;

        // Crowd-specific params.
        unsigned crowdSize;
        unsigned crowdSems;  // Limited by number of WaitBlocks - 1.
        unsigned crowdSemInit;
        unsigned crowdTimeout;
        unsigned crowdIters;

        unsigned condvarSchedObjs;
        unsigned condvarIters;
    };

    static constexpr Params paramsSanity = {
        .innerIters = 100000,
        .dedicatedThreadCount = 6,
        .workPerDedicatedThread = 12,
        .producerCount = 6,
        .workCountLazy = 260,
        .workCountSwitchy = 300,
        .workCountStacky = 28,
        .workCountSleepy = 260,
        .switchyIters = 4,
        .switchySemInit = 3,
        .fibDepth = 38,
        .jumpyIters = 30,
        .jumpyStride = 8,
        .jumpySchedObjs = 80,
        .jumpyTimeout = USecsPerSec * 16,
        .crowdSize = 200,
        .crowdSems = 8,
        .crowdSemInit = 3,
        .crowdTimeout = USecsPerSec * 2,
        .crowdIters = 4,
        .condvarSchedObjs = 100,
        .condvarIters = 200,
    };

    static constexpr Params paramsStress = {
        .innerIters = 10000000,
        .dedicatedThreadCount = 14,
        .workPerDedicatedThread = 18,
        .producerCount = 9,
        .workCountLazy = 800,
        .workCountSwitchy = 600,
        .workCountStacky = 64,
        .workCountSleepy = 400,
        .switchyIters = 8,
        .switchySemInit = 7,
        .fibDepth = 40,
        .jumpyIters = 44,
        .jumpyStride = 8,
        .jumpySchedObjs = 120,
        .jumpyTimeout = USecsPerSec * 20,
        .crowdSize = 1200,
        .crowdSems = 8,
        .crowdSemInit = 11,
        .crowdTimeout = USecsPerSec * 2,
        .crowdIters = 13,
        .condvarSchedObjs = 100,
        .condvarIters = 200,
    };

    RuntimeTests(const Params &params);
    ~RuntimeTests();

    // Actual test entry-points.
    static Status testSanity();
    static Status testStress();
    static Status testCustom();

    static Status parseConfig(Config::Configuration *config,
                              char *key,
                              char *value,
                              bool stringentRules);

  private:
    static constexpr uint64_t DedicatedThreadReturn = 7;

    // Schedulable that simpy executes asynchronously and deletes itself when
    // done.
    class TestSchedulable : public Schedulable
    {
      public:
        TestSchedulable(RuntimeTests *runtimeTests, void (RuntimeTests::*fn)())
            : Schedulable("TestSchedulable"),
              runtimeTests_(runtimeTests),
              fn_(fn)
        {
        }

        void run() override { (runtimeTests_->*fn_)(); }

        void done() override { delete this; }

      private:
        RuntimeTests *runtimeTests_;
        void (RuntimeTests::*fn_)();
    };

    // Allows whoever initiated work to wait for it to complete.
    class ShimSchedulable : public Schedulable
    {
      public:
        ShimSchedulable(RuntimeTests *runtimeTests, void (RuntimeTests::*fn)())
            : Schedulable("ShimSchedulable"),
              done_(0),
              runtimeTests_(runtimeTests),
              fn_(fn)
        {
        }

        void run() override { (runtimeTests_->*fn_)(); }

        void done() override { done_.post(); }

        Semaphore done_;

      private:
        RuntimeTests *runtimeTests_;
        void (RuntimeTests::*fn_)();
    };

    class Jumpy : public Schedulable
    {
      public:
        Jumpy(Semaphore *sems, unsigned index, const Params &params)
            : Schedulable("Jumpy"), sems_(sems), index_(index), p_(params)
        {
        }
        void run() override;
        void done() override { delete this; }

      private:
        Semaphore *sems_;
        unsigned index_;
        const Params &p_;
    };

    Status run();
    void produceWork();
    void *produceWorkWrapper();
    void *dedicatedThread();

    // Functions that use the runtime in fun ways.
    void lazy();
    void switchy();
    int fib(int n);
    void stacky();
    void sleepy();

    void jumpy();
    void crowd();
    void genCrowd();

    void condvar();
    void condvarWorker();

    // Notify when threads can begin their work
    Semaphore producerThreadSem_;
    Semaphore dedicatedThreadSem_;

    // Track production of work.
    Atomic64 workProduced_;
    Semaphore progressProduce_;

    // Tracks completed work.
    Semaphore progressConsume_;

    Mutex fib20_;
    Semaphore switchySem_;

    Semaphore *jumpySems_;
    Semaphore *crowdSems_;
    Atomic32 *crowdCounts_;
    CondVar crowdFull_;
    Semaphore crowdDone_;
    Mutex crowdLock_;

    CondVar condvar_;
    Mutex condvarLock_;
    unsigned condvarCountWaiting_;
    Atomic32 condvarWorkerDone_;

    // Parameters for this test run.
    const Params p_;

    // Used for testCustom.
    static Params paramsCustom;
};

#endif  // _RUNTIME_TESTS_H
