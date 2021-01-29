// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <libgen.h>

#include "StrlFunc.h"
#include "common/InitTeardown.h"
#include "primitives/Assert.h"
#include "primitives/Primitives.h"
#include "runtime/Async.h"
#include "runtime/Schedulable.h"
#include "stat/Statistics.h"
#include "test/FuncTests/RuntimeTests.h"
#include "util/SchedulableFsm.h"
#include "runtime/RwLock.h"
#include "libapis/LibApisRecv.h"
#include "test/QA.h"  // Must be last

static void *
runtimeTestSanityThread(void *ignored)
{
    verifyOk(RuntimeTests::testSanity());
    return NULL;
}

static Status
runtimeTestSanity()
{
    // Cannot be run on main thread.
    pthread_t thread;
    verify(Runtime::get()->createBlockableThread(&thread,
                                                 NULL,
                                                 runtimeTestSanityThread,
                                                 NULL) == StatusOk);
    verify(sysThreadJoin(thread, NULL) == 0);
    return StatusOk;
}

static Status
runtimeTestOneProc()
{
    Runtime *rt = Runtime::get();
    unsigned prevThreadCountSched0 =
        rt->getThreadsCount(Runtime::SchedId::Sched0);
    unsigned prevThreadCountSched1 =
        rt->getThreadsCount(Runtime::SchedId::Sched1);
    unsigned prevThreadCountSched2 =
        rt->getThreadsCount(Runtime::SchedId::Sched2);

    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched0,
                                         RuntimeType::RuntimeTypeThroughput,
                                         1));
    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched1,
                                         RuntimeType::RuntimeTypeThroughput,
                                         1));
    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched2,
                                         RuntimeType::RuntimeTypeLatency,
                                         1));

    verifyOk(runtimeTestSanity());

    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched0,
                                         RuntimeType::RuntimeTypeThroughput,
                                         prevThreadCountSched0));
    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched1,
                                         RuntimeType::RuntimeTypeThroughput,
                                         prevThreadCountSched1));
    verifyOk(rt->changeThreadsCountLocal(Runtime::SchedId::Sched2,
                                         RuntimeType::RuntimeTypeLatency,
                                         prevThreadCountSched2));

    return StatusOk;
}

static int
fib(int n)
{
    if (n == 0) {
        return 0;
    } else if (n == 1) {
        return 1;
    } else {
        // Recurse in parallel
        Future<int> first;
        Future<int> second;
        verifyOk(async(&first, fib, n - 1));
        verifyOk(async(&second, fib, n - 2));
        first.wait();
        second.wait();
        return first.get() + second.get();
    }
}
static int64_t
mySum(int64_t *numArray, int numNums)
{
    Future<int64_t> firstHalf;
    Future<int64_t> secondHalf;

    if (numNums < 1000) {
        int64_t sum = 0;
        for (int ii = 0; ii < numNums; ii++) {
            sum += numArray[ii];
        }
        return sum;
    }

    int mid = numNums / 2;
    verifyOk(async(&firstHalf, mySum, numArray, mid));
    verifyOk(async(&secondHalf, mySum, numArray + mid, numNums - mid));
    firstHalf.wait();
    secondHalf.wait();
    return firstHalf.get() + secondHalf.get();
}

class HelloClass
{
  public:
    int hello(const char *s)
    {
        return printf("hello world from an object instance %s\n", s);
    }
};

static void *
runtimeTestFuturesActual(void *ignored)
{
    Status status = StatusOk;

    // Basic test
    {
        Future<int> fut;
        status = async(&fut, printf, "hello world\n");
        assert(status == StatusOk);

        fut.wait();

        printf("got ret %d\n", fut.get());
    }

    // Big fib test
    {
        int fibNum = 20;
        int fibResult = fib(fibNum);
        printf("fib(%i) = %i\n", fibNum, fibResult);
    }

    // Parallel sum test
    {
        int64_t numNums = 100000;
        int64_t *nums = new int64_t[numNums];
        for (int64_t ii = 0; ii < numNums; ii++) {
            nums[ii] = ii;
        }
        int64_t sum = mySum(nums, numNums);
        assert(sum == ((numNums * (numNums - 1)) / 2));
        delete[] nums;
    }

    {
        HelloClass c;
        Future<int> fut;
        verifyOk(asyncObj(&fut, &c, &HelloClass::hello, "whats up"));
        fut.wait();
        fut.get();
    }

    return NULL;
}

static Status
testFutures()
{
    pthread_t thread;
    verify(Runtime::get()->createBlockableThread(&thread,
                                                 NULL,
                                                 runtimeTestFuturesActual,
                                                 NULL) == StatusOk);
    verify(sysThreadJoin(thread, NULL) == 0);
    return StatusOk;
}

class TestFsmHandleCompletion : public FsmState
{
  public:
    TestFsmHandleCompletion(SchedulableFsm *schedFsm)
        : FsmState("TestFsmHandleCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork();
};

class TestFsmStartWork : public FsmState
{
  public:
    TestFsmStartWork(SchedulableFsm *schedFsm)
        : FsmState("TestFsmStartWork", schedFsm)
    {
    }
    virtual TraverseState doWork();
};

class TestFsmSchedulable : public SchedulableFsm
{
    friend class TestFsmStartWork;
    friend class TestFsmHandleCompletion;

  public:
    static constexpr const size_t NumSchedulues = 1 << 10;

    TestFsmSchedulable(Mutex *runtimeDestroyLock)
        : SchedulableFsm("TestFsmSchedulable", NULL),
          startWork_(this),
          handleCompletion_(this)
    {
        atomicWrite64(&issue_, 0);
        atomicWrite64(&complete_, 0);
        atomicWrite64(&done_, 0);
        atomicWrite64(&pause_, 1);
        runtimeDestroyLock_ = runtimeDestroyLock;
    }

    virtual void done() override { atomicInc64(&done_); }

    void pauseFsm()
    {
        assert(atomicRead64(&pause_) == 0);
        atomicWrite64(&pause_, 1);
    }

    void restartFsm()
    {
        assert(atomicRead64(&pause_) == 1);
        atomicWrite64(&pause_, 0);
        setNextState(&startWork_);
        Runtime *rt = Runtime::get();
        for (size_t ii = 0; ii < NumSchedulues; ii++) {
            verifyOk(rt->schedule(this));
        }
    }
    Mutex *runtimeDestroyLock_;

  private:
    TestFsmStartWork startWork_;
    TestFsmHandleCompletion handleCompletion_;
    Atomic64 issue_;
    Atomic64 complete_;
    Atomic64 done_;
    Atomic64 pause_;
};

FsmState::TraverseState
TestFsmStartWork::doWork()
{
    TestFsmSchedulable *fsm = dynamic_cast<TestFsmSchedulable *>(getSchedFsm());
    fsm->setNextState(&fsm->handleCompletion_);
    atomicInc64(&fsm->issue_);
    return TraverseState::TraverseNext;
}

FsmState::TraverseState
TestFsmHandleCompletion::doWork()
{
    TestFsmSchedulable *fsm = dynamic_cast<TestFsmSchedulable *>(getSchedFsm());
    atomicInc64(&fsm->complete_);
    if (!atomicRead64(&fsm->pause_)) {
        fsm->setNextState(&fsm->startWork_);
        verifyOk(Runtime::get()->schedule(fsm));
    }
    return TraverseState::TraverseStop;
}

struct ArgsPerThread {
    size_t idx;
    pthread_t threadId;
    Mutex *lock;
    Status retStatus;
};

static TestFsmSchedulable *testFsm = NULL;

static void *
testChangeThread(void *args)
{
    ArgsPerThread *input = (ArgsPerThread *) args;
    static constexpr const unsigned LoopCount = 16;
    Status status = StatusOk;
    Runtime *rt = Runtime::get();

    for (unsigned ii = 0; ii < LoopCount; ii++) {
        for (uint8_t jj = 0; jj < static_cast<uint8_t>(Runtime::TotalSdkScheds);
             jj++) {
            unsigned newThreadsCount =
                (rand() % XcSysHelper::get()->getNumOnlineCores() + 1);
            RuntimeType type = RuntimeTypeInvalid;
            switch (static_cast<Runtime::SchedId>(jj)) {
            case Runtime::SchedId::Sched0:
                type = RuntimeType::RuntimeTypeThroughput;
                break;
            case Runtime::SchedId::Sched1:
                type = RuntimeType::RuntimeTypeThroughput;
                break;
            case Runtime::SchedId::Sched2:
                type = RuntimeType::RuntimeTypeLatency;
                break;
            case Runtime::SchedId::Immediate:
                type = RuntimeType::RuntimeTypeImmediate;
                // Runtime::SchedId::Immediate can only increase.
                newThreadsCount =
                    rt->getThreadsCount(Runtime::SchedId::Immediate);
                break;
            default:
                assert(0 && "Invalid sched id");
                break;
            }
            input->lock->lock();
            status =
                rt->changeThreadsCountLocal(static_cast<Runtime::SchedId>(jj),
                                            type,
                                            newThreadsCount);
            input->lock->unlock();
            BailIfFailed(status);
        }

        if (LoopCount % 4) {
            // Throw in a Runtime restart into mix.
            testFsm->pauseFsm();
            input->lock->lock();
            setUsrNodeForceShutdown();
            rt->drainAllRunnables();
            unsetUsrNodeForceShutdown();
            rt->destroy();
            StatsLib::get()->removeGroupIdFromHashTable("libruntime");
            status = rt->init(InitLevel::UsrNode);
            testFsm->restartFsm();
            input->lock->unlock();
            BailIfFailed(status);
        }
    }

CommonExit:
    input->retStatus = status;
    return NULL;
}

// Schedule lots of work on Runtime and have multiple threads change the number
// of threads in Latency and Throughput Runtime.
static Status
testChangeThreadsCount()
{
    size_t numThreads = XcSysHelper::get()->getNumOnlineCores();
    Mutex runtimeDestroy;
    ArgsPerThread args[numThreads];
    Status status = StatusOk;
    Runtime *rt = Runtime::get();

    testFsm = new (std::nothrow) TestFsmSchedulable(&runtimeDestroy);

    for (size_t ii = 0; ii < numThreads; ii++) {
        args[ii].idx = ii;
        args[ii].lock = &runtimeDestroy;
        runtimeDestroy.lock();
        verifyOk(rt->createBlockableThread(&args[ii].threadId,
                                           NULL,
                                           testChangeThread,
                                           &args[ii]));
        runtimeDestroy.unlock();
    }

    runtimeDestroy.lock();
    testFsm->restartFsm();
    runtimeDestroy.unlock();

    for (size_t ii = 0; ii < numThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    testFsm->pauseFsm();
    runtimeDestroy.lock();
    setUsrNodeForceShutdown();
    rt->drainAllRunnables();
    unsetUsrNodeForceShutdown();
    runtimeDestroy.unlock();

    delete testFsm;
    testFsm = NULL;

    return status;
}

class TestFsmCrossRtSemCompletion : public FsmState
{
  public:
    TestFsmCrossRtSemCompletion(SchedulableFsm *schedFsm)
        : FsmState("TestFsmCrossRtSemCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork();
};

class TestFsmCrossRtSemStartWork : public FsmState
{
  public:
    TestFsmCrossRtSemStartWork(SchedulableFsm *schedFsm)
        : FsmState("TestFsmCrossRtSemStartWork", schedFsm)
    {
    }
    virtual TraverseState doWork();
};

class TestFsmCrossRtSem : public SchedulableFsm
{
    friend class TestFsmCrossRtSemStartWork;
    friend class TestFsmCrossRtSemCompletion;

  public:
    static constexpr const int64_t NumSchedules = 1 << 12;

    TestFsmCrossRtSem()
        : SchedulableFsm("TestFsmCrossRtSem", NULL),
          startWork_(this),
          handleCompletion_(this)
    {
        atomicWrite64(&issue_, 0);
        atomicWrite64(&complete_, 0);
        pushSem_.init(0);
        setNextState(&startWork_);
    }

    virtual void done() override
    {
        if (atomicRead64(&complete_) == NumSchedules) {
            assert(atomicRead64(&issue_) == NumSchedules);
            doneSem_->post();
        }
    }

    void init(Semaphore *doneSem) { doneSem_ = doneSem; }
    Semaphore pushSem_;
    Atomic64 issue_;
    Atomic64 complete_;

  private:
    TestFsmCrossRtSemStartWork startWork_;
    TestFsmCrossRtSemCompletion handleCompletion_;
    Semaphore *doneSem_ = NULL;
};

FsmState::TraverseState
TestFsmCrossRtSemCompletion::doWork()
{
    TestFsmCrossRtSem *fsm = dynamic_cast<TestFsmCrossRtSem *>(getSchedFsm());
    Runtime *rt = Runtime::get();
    if (atomicInc64(&fsm->complete_) < TestFsmCrossRtSem::NumSchedules) {
        fsm->setNextState(&fsm->startWork_);
        Txn txn = Txn::newTxn(Txn::Mode::NonLRQ,
                              static_cast<Runtime::SchedId>(
                                  rand() % static_cast<uint8_t>(
                                               Runtime::TotalSdkScheds)));
        Txn::setTxn(txn);
        fsm->setNextState(&fsm->startWork_);
        verifyOk(rt->schedule(fsm));
        fsm->pushSem_.semWait();
    }
    return TraverseState::TraverseStop;
}

FsmState::TraverseState
TestFsmCrossRtSemStartWork::doWork()
{
    TestFsmCrossRtSem *fsm = dynamic_cast<TestFsmCrossRtSem *>(getSchedFsm());
    fsm->pushSem_.post();
    atomicInc64(&fsm->issue_);
    fsm->setNextState(&fsm->handleCompletion_);
    return TraverseState::TraverseNext;
}

static Status
testCrossRuntimeSem()
{
    size_t numCores = XcSysHelper::get()->getNumOnlineCores();
    Status status = StatusOk;
    size_t schedCount = numCores * 10;
    Runtime *rt = Runtime::get();

    Semaphore testSem[schedCount];
    TestFsmCrossRtSem testFsm[schedCount];

    for (size_t ii = 0; ii < schedCount; ii++) {
        Txn txn = Txn::newTxn(Txn::Mode::NonLRQ,
                              static_cast<Runtime::SchedId>(
                                  rand() % Runtime::TotalSdkScheds));
        Txn::setTxn(txn);
        testSem[ii].init(0);
        testFsm[ii].init(&testSem[ii]);
        verifyOk(rt->schedule(&testFsm[ii]));
    }

    for (size_t ii = 0; ii < schedCount; ii++) {
        testSem[ii].semWait();
    }

    return status;
}

class TestRwLockSched : public Schedulable
{
  public:
    TestRwLockSched(Atomic64 *track)
        : Schedulable("TestRwLockSched"), track_(track)
    {
    }
    ~TestRwLockSched() { assert(!(counter_ % 2)); }

    void run()
    {
        if (atomicRead64(track_) % 2) {
            lock_.lock(RwLock::Type::Writer);
        } else {
            while (!lock_.tryLock(RwLock::Type::Writer)) {
            }
        }
        counter_ = counter_ + 2;
        lock_.unlock(RwLock::Type::Writer);

        static constexpr const uint64_t LoopCount = 1 << 10;
        for (uint64_t ii = 0; ii < LoopCount; ii++) {
            if (ii % 2) {
                lock_.lock(RwLock::Type::Reader);
            } else {
                while (!lock_.tryLock(RwLock::Type::Reader)) {
                }
            }
            assert(!(counter_ % 2));
            lock_.unlock(RwLock::Type::Reader);
        }
    }

    void done() { atomicDec64(track_); }

  private:
    RwLock lock_;
    uint64_t counter_ = 0;
    Atomic64 *track_ = NULL;
};

static Status
testRwLock()
{
    Status status = StatusOk;
    static constexpr const size_t SchedCount = 1 << 10;
    Runtime *rt = Runtime::get();
    Atomic64 track;
    atomicWrite64(&track, SchedCount);
    TestRwLockSched curSched(&track);
    Semaphore sem(0);
    static constexpr const size_t SemTimedWaitUsec = 1000;

    for (size_t ii = 0; ii < SchedCount; ii++) {
        verifyOk(rt->schedule(&curSched));
    }

    while (atomicRead64(&track)) {
        sem.timedWait(SemTimedWaitUsec);
    }

    return StatusOk;
}

template <class T1, class T2>
class TestLocks : public Schedulable
{
  public:
    TestLocks(T1 *a, T2 *b, Atomic64 *track, int64_t *counter, bool op)
        : Schedulable("TextLocks"),
          a_(a),
          b_(b),
          track_(track),
          counter_(counter),
          op_(op)
    {
    }
    ~TestLocks() {}

    void run()
    {
        a_->lock();
        b_->lock();
        if (op_ == true) {
            counter_++;
        } else {
            counter_--;
        }
        b_->unlock();
        a_->unlock();
    }

    void done() { atomicDec64(track_); }

  private:
    T1 *a_;
    T2 *b_;
    Atomic64 *track_;
    int64_t *counter_;
    bool op_;
};

static Status
testMutex()
{
    Semaphore sem(0);
    Mutex mtxA, mtxB;
    Runtime *rt = Runtime::get();
    static constexpr const size_t SchedCount = 1 << 14;
    int64_t counter = SchedCount;
    Atomic64 track;
    atomicWrite64(&track, SchedCount * 2);
    TestLocks<Mutex, Mutex> tmtx1(&mtxA, &mtxB, &track, &counter, true);
    TestLocks<Mutex, Mutex> tmtx2(&mtxA, &mtxB, &track, &counter, false);
    static constexpr const size_t SemTimedWaitUsec = 1000;

    for (size_t ii = 0; ii < SchedCount; ii++) {
        verifyOk(rt->schedule(&tmtx1));
        verifyOk(rt->schedule(&tmtx2));
    }

    while (atomicRead64(&track)) {
        sem.timedWait(SemTimedWaitUsec);
    }

    verify(counter == SchedCount);

    return StatusOk;
}

static Status
testSpinlock()
{
    Semaphore sem(0);
    Spinlock sA, sB;
    Runtime *rt = Runtime::get();
    static constexpr const size_t SchedCount = 1 << 14;
    int64_t counter = SchedCount;
    Atomic64 track;
    atomicWrite64(&track, SchedCount * 2);
    TestLocks<Spinlock, Spinlock> ts1(&sA, &sB, &track, &counter, true);
    TestLocks<Spinlock, Spinlock> ts2(&sA, &sB, &track, &counter, false);
    static constexpr const size_t SemTimedWaitUsec = 1000;

    for (size_t ii = 0; ii < SchedCount; ii++) {
        verifyOk(rt->schedule(&ts1));
        verifyOk(rt->schedule(&ts2));
    }

    while (atomicRead64(&track)) {
        sem.timedWait(SemTimedWaitUsec);
    }
    verify(counter == SchedCount);

    return StatusOk;
}

class TestSemaphore : public Schedulable
{
  public:
    TestSemaphore(Semaphore *semA, Semaphore *semB, Atomic64 *track)
        : Schedulable("TestSemaphore"), semA_(semA), semB_(semB), track_(track)
    {
    }
    ~TestSemaphore() {}

    void run()
    {
        semA_->post();
        semB_->semWait();
    }

    void done() { atomicDec64(track_); }

  private:
    Semaphore *semA_;
    Semaphore *semB_;
    Atomic64 *track_;
};

static Status
testSemaphore()
{
    Semaphore sem(0);
    Semaphore semA(0);
    Semaphore semB(0);
    Runtime *rt = Runtime::get();
    static constexpr const size_t SchedCount = 1 << 14;
    Atomic64 track;
    atomicWrite64(&track, SchedCount * 2);
    TestSemaphore tsem1(&semA, &semB, &track);
    TestSemaphore tsem2(&semB, &semA, &track);
    static constexpr const size_t SemTimedWaitUsec = 1000;

    for (size_t ii = 0; ii < SchedCount; ii++) {
        verifyOk(rt->schedule(&tsem1));
        verifyOk(rt->schedule(&tsem2));
    }

    while (atomicRead64(&track)) {
        sem.timedWait(SemTimedWaitUsec);
    }

    return StatusOk;
}

static Status
testLocks()
{
    Mutex lock;

    // The normal automatic acquisition and release.
    {
        auto guard = lock.take();
        CHECK(!lock.tryLock());
    }
    CHECK(lock.tryLock());
    lock.unlock();

    // The manual release.
    {
        auto guard = lock.take();
        CHECK(!lock.tryLock());
        guard.unlock();
        CHECK(lock.tryLock());
    }

    lock.unlock();
    return StatusOk;
}

struct Node {
    uint64_t val_;
    Node *next;
    Node(uint64_t val)
    {
        val_ = val;
        next = NULL;
    }
};
struct List {
    Node *head = NULL;
    List(Node *node) { head = node; }
    Mutex lock;
};

struct ArgsPerThreadListOp {
    uint64_t start;
    uint64_t numNodes;
    List *list;
    pthread_t threadId;
};

static void *
testParallelInserts(void *args)
{
    ArgsPerThreadListOp *input = (ArgsPerThreadListOp *) args;
    List *list = input->list;

    int start = input->start;
    int end = start + input->numNodes;
    while (start < end) {
        Node *curr = new Node(start);

        list->lock.lock();
        curr->next = list->head->next;
        list->head->next = curr;
        list->lock.unlock();

        start += 1;
    }
    return NULL;
}

static void *
testParallelDeletes(void *args)
{
    ArgsPerThreadListOp *input = (ArgsPerThreadListOp *) args;
    List *list = input->list;
    uint64_t ret = 0;
    while (true) {
        list->lock.lock();
        Node *cur = list->head->next;
        if (cur == NULL) {
            list->lock.unlock();
            break;
        }
        ret += cur->val_;
        list->head->next = cur->next;
        list->lock.unlock();
        cur->next = NULL;
        delete cur;
    }
    return (void *) ret;
}

static Status
testSuspensions()
{
    size_t numThreads = XcSysHelper::get()->getNumOnlineCores();
    uint64_t numNodes = numThreads * 10000;
    Runtime *rt = Runtime::get();
    ArgsPerThreadListOp args[numThreads];
    uint64_t totalSumRet = 0;
    struct timespec timeStart;
    struct timespec timeEnd;

    // init input
    Node *head = new Node(-1);
    List *list = new List(head);
    uint64_t numNodesPerthread = numNodes / numThreads;
    DCHECK(numNodesPerthread > 0);
    DCHECK(numNodes % numThreads == 0);

    clock_gettime(CLOCK_MONOTONIC, &timeStart);
    for (size_t ii = 0; ii < numThreads; ii++) {
        args[ii].numNodes = numNodesPerthread;
        args[ii].start = ii * numNodesPerthread + 1;
        args[ii].list = list;
        verifyOk(rt->createBlockableThread(&args[ii].threadId,
                                           NULL,
                                           testParallelInserts,
                                           &args[ii]));
    }
    for (size_t ii = 0; ii < numThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);
    unsigned long long diffMsecs =
        (((timeEnd.tv_sec - timeStart.tv_sec) * NSecsPerSec) + timeEnd.tv_nsec -
         timeStart.tv_nsec) /
        NSecsPerMSec;

    printf("Created %lu nodes in %llu msecs\n", numNodes, diffMsecs);

    Node *cur = list->head->next;
    uint64_t len = 0;
    while (cur != NULL) {
        len += 1;
        cur = cur->next;
    }
    DCHECK(len == numNodes);

    // read them back by deleting nodes
    clock_gettime(CLOCK_MONOTONIC, &timeStart);
    for (size_t ii = 0; ii < numThreads; ii++) {
        args[ii].list = list;
        verifyOk(rt->createBlockableThread(&args[ii].threadId,
                                           NULL,
                                           testParallelDeletes,
                                           &args[ii]));
    }
    for (size_t ii = 0; ii < numThreads; ii++) {
        void *tSum;
        int ret = sysThreadJoin(args[ii].threadId, &tSum);
        verify(ret == 0);
        totalSumRet += (uint64_t) tSum;
    }
    uint64_t actualSum = ((numNodes * (numNodes + 1)) / 2);
    DCHECK(totalSumRet == actualSum);

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);
    diffMsecs = (((timeEnd.tv_sec - timeStart.tv_sec) * NSecsPerSec) +
                 timeEnd.tv_nsec - timeStart.tv_nsec) /
                NSecsPerMSec;
    printf("Deleted %lu nodes in %llu msecs\n", numNodes, diffMsecs);

    delete head;
    delete list;
    return StatusOk;
}

static TestCase testCases[] =
    {{"runtime test: change threads count",
      testChangeThreadsCount,
      TestCaseEnable,
      ""},
     {"runtime test: test semaphore", testSemaphore, TestCaseEnable, ""},
     {"runtime test: test mutex", testMutex, TestCaseEnable, ""},
     {"runtime test: test spinlock", testSpinlock, TestCaseEnable, ""},
     {"runtime test: cross runtime semaphore",
      testCrossRuntimeSem,
      TestCaseEnable,
      ""},
     {"runtime test: sanity", runtimeTestSanity, TestCaseEnable, ""},
     {"runtime test: one proc", runtimeTestOneProc, TestCaseEnable, ""},
     {"runtime test: futures", testFutures, TestCaseEnable, ""},
     {"runtime test: locks", testLocks, TestCaseEnable, ""},
     {"runtime test: check suspensions", testSuspensions, TestCaseEnable, ""},
     {"runtime test: RwLock", testRwLock, TestCaseEnable, ""}};

static TestCaseOptionMask testCaseOptionMask = (TestCaseOptionMask)(
    TestCaseOptDisableIsPass | TestCaseScheduleOnDedicatedThread);

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numTestsFailed = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    srand(time(NULL));

    numTestsFailed =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
