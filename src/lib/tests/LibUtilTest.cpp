// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <semaphore.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <libgen.h>
#include <fcntl.h>

#include "util/RefCount.h"
#include "util/Random.h"
#include "util/System.h"
#include "config/Config.h"
#include "primitives/Primitives.h"
#include "util/Lookaside.h"
#include "util/WorkQueue.h"
#include "util/Math.h"
#include "operators/XcalarEval.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "scalars/Scalars.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "util/Archive.h"
#include "util/Base64.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "util/SchedulableFsm.h"
#include "util/BitFlagEnums.h"

#include "test/QA.h"  // Must be last

static Status bitmapEnumTests();
static Status refCountTests();
static Status lookasideSanityTests();
static Status workQueueSanityTests();
static Status mathTests();
static Status randomTests();
static Status memTrackTests();
static Status newTrackTests();
static Status archiveTests();
static Status archivePackUnpackTests();
static Status base64Tests();
static Status testSchedulableFsms();

const char *cfgFile = "test-config.cfg";
char fullCfgFilePath[255];

static TestCase testCases[] = {
    {"Bitmap Enum tests", bitmapEnumTests, TestCaseEnable, ""},
    {"Refcount Tests", refCountTests, TestCaseEnable, ""},
    {"Lookaside sanity", lookasideSanityTests, TestCaseEnable, ""},
    {"Random Tests", randomTests, TestCaseEnable, ""},
    {"WorkQueue sanity", workQueueSanityTests, TestCaseEnable, ""},
    {"Math Tests", mathTests, TestCaseEnable, ""},
    {"Mem Track Tests", memTrackTests, TestCaseEnable, ""},
    {"New Track Tests", newTrackTests, TestCaseEnable, ""},
    {"Archive Tests", archiveTests, TestCaseEnable, ""},
    {"Archive Pack/Unpack Tests", archivePackUnpackTests, TestCaseEnable, ""},
    {"Base64 encode/decode Tests", base64Tests, TestCaseEnable, ""},
    {"Schedulable FSM Tests", testSchedulableFsms, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime);

#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
static const char *moduleName = "libutiltest";
#endif  // MEMORY_TRACKING

// We have a trivial FSM that has 2 states here.
// - Issue Work: Generates work and sticks them into the Runtime scheduler.
// - Handle Completions: Processes completions of work.

class TestFsmSchedulable : public SchedulableFsm
{
    friend class TestFsmIssueWork;
    friend class TestFsmHandleCompletion;

  public:
    static constexpr uint64_t TestFsmIters = 16;
    static constexpr uint64_t TestFsmWorkCount = 16384;

    TestFsmSchedulable(FsmState *curState, Semaphore *sem)
        : SchedulableFsm("TestFsmSchedulable", curState), doneSem_(sem)
    {
        atomicWrite64(&issueWork_, 0);
        atomicWrite64(&completeWork_, 0);
        atomicWrite64(&active_, 0);
    }

    virtual void done() override
    {
        if (atomicDec64(&active_) == 0) {
            FsmState *curState = endFsm();
            delete curState;
            doneSem_->post();
            delete this;
        }
    }

  private:
    Semaphore *doneSem_;
    Atomic64 issueWork_;
    Atomic64 completeWork_;
    Atomic64 active_;
};

class TestFsmHandleCompletion : public FsmState
{
  public:
    TestFsmHandleCompletion(SchedulableFsm *schedFsm)
        : FsmState("TestFsmHandleCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork()
    {
        TestFsmSchedulable *fsm =
            dynamic_cast<TestFsmSchedulable *>(getSchedFsm());
        atomicInc64(&fsm->completeWork_);
        assert(atomicRead64(&fsm->issueWork_) >=
               atomicRead64(&fsm->completeWork_));
        return TraverseState::TraverseStop;
    }
};

class TestFsmIssueWork : public FsmState
{
  public:
    TestFsmIssueWork(TestFsmSchedulable *schedFsm)
        : FsmState("TestFsmIssueWork", schedFsm)
    {
    }

    virtual TraverseState doWork()
    {
        TestFsmSchedulable *fsm =
            dynamic_cast<TestFsmSchedulable *>(getSchedFsm());

        // Do state transition to completion state.
        TestFsmHandleCompletion *nextState =
            new (std::nothrow) TestFsmHandleCompletion(getSchedFsm());
        assert(nextState != NULL);
        TestFsmIssueWork *curState =
            dynamic_cast<TestFsmIssueWork *>(fsm->setNextState(nextState));
        delete curState;
        curState = NULL;

        atomicAdd64(&fsm->issueWork_, TestFsmSchedulable::TestFsmWorkCount);
        atomicAdd64(&fsm->active_, TestFsmSchedulable::TestFsmWorkCount + 1);

        for (unsigned ii = 0; ii < TestFsmSchedulable::TestFsmWorkCount; ii++) {
            verifyOk(Runtime::get()->schedule(fsm));
        }

        return TraverseState::TraverseStop;
    }
};

Status
testSchedulableFsms()
{
    Semaphore fsmDone(0);
    for (uint64_t ii = 0; ii < TestFsmSchedulable::TestFsmIters; ii++) {
        SchedulableFsm *fsm =
            new (std::nothrow) TestFsmSchedulable(NULL, &fsmDone);
        assert(fsm != NULL);
        FsmState *state = new (std::nothrow)
            TestFsmIssueWork(dynamic_cast<TestFsmSchedulable *>(fsm));
        assert(state != NULL);
        verify(fsm->setNextState(state) == NULL);
        verifyOk(Runtime::get()->schedule(fsm));
        fsmDone.semWait();
    }
    return StatusOk;
}

struct TestEmployee {
    uint32_t salary;
    int32_t id;
    RefCount refCount;
};

static bool destructorCalled = false;

void
destroyEmployee(RefCount *refCount)
{
    TestEmployee *t = RefEntry(refCount, TestEmployee, refCount);

    memFree(t);
    destructorCalled = true;
}

enum class Permissions : int {
    Readable = 0x4,
    Writable = 0x2,
    Executable = 0x1,
};
enableBmaskOps(Permissions);

Status
bitmapEnumTests()
{
    Permissions p = Permissions::Readable | Permissions::Writable;
    assert(static_cast<bool>(p & Permissions::Readable));
    assert(static_cast<bool>(p & Permissions::Writable));
    assert(p == (Permissions::Readable ^ Permissions::Writable));

    p |= Permissions::Executable;
    assert(static_cast<bool>(p & Permissions::Readable));
    assert(static_cast<bool>(p & Permissions::Writable));
    assert(static_cast<bool>(p & Permissions::Executable));
    assert(p == (Permissions::Readable ^ Permissions::Writable ^
                 Permissions::Executable));

    p &= ~Permissions::Executable;
    assert(p == (Permissions::Readable ^ Permissions::Writable));

    p ^= Permissions::Readable;
    assert(p == Permissions::Writable);
    assert(static_cast<bool>(p & Permissions::Writable));

    p ^= (Permissions::Readable ^ Permissions::Executable);
    assert(static_cast<bool>(p & Permissions::Readable));
    assert(static_cast<bool>(p & Permissions::Writable));
    assert(static_cast<bool>(p & Permissions::Executable));
    assert(p == (Permissions::Readable ^ Permissions::Writable ^
                 Permissions::Executable));

    return StatusOk;
}

static Status
refCountTests()
{
    TestEmployee *t;

    t = (TestEmployee *) memAllocExt(sizeof(*t), moduleName);
    assert(t != NULL);
    t->id = 1;
    t->salary = 110000;
    refInit(&t->refCount, destroyEmployee);
    refGet(&t->refCount);
    refPut(&t->refCount);
    assert(destructorCalled == false);
    refPut(&t->refCount);
    assert(destructorCalled == true);

    return StatusOk;
}

struct TestElt {
    unsigned a;
    int b;
};

static LookasideList lookasideList;

static void *
lookasideWorker(void *arg)
{
    TestElt *dummyElt = (TestElt *) arg;

    sysSleep(1);

    lookasideFree(&lookasideList, dummyElt);

    return NULL;
}

static Status
lookasideSanityTests()
{
    enum { NumLookasideElems = 5 };
    TestElt *testElt[NumLookasideElems];
    TestElt *dummyElt;
    Status status;
    unsigned ii;
    int ret;
    pthread_t thread;
    pthread_attr_t attr;

    status = lookasideInit(sizeof(TestElt),
                           NumLookasideElems,
                           (LookasideListOpts) 0,
                           &lookasideList);
    if (status != StatusOk) {
        printf("Status: %s\n", strGetFromStatus(status));
    }
    assert(status == StatusOk);

    for (ii = 0; ii < NumLookasideElems; ii++) {
        testElt[ii] = (TestElt *) lookasideAlloc(&lookasideList);
        assert(testElt[ii] != NULL);

        // Test that alloc'ed addresses are aligned to 8-byte boundaries
        assert(((uintptr_t) testElt[ii] % 8) == 0);

        // Test write
        testElt[ii]->a = ii;
    }

    dummyElt = (TestElt *) lookasideAlloc(&lookasideList);
    assert(dummyElt == NULL);

    // lookasideAlloc causes a mutex lock, which is effectively a membar
    // So we can test read now
    for (ii = 0; ii < NumLookasideElems; ii++) {
        assert(testElt[ii]->a == ii);

        if (ii > 0) {
            assert(((uintptr_t) testElt[ii - 1] - (uintptr_t) testElt[ii]) >
                   sizeof(TestElt));
        }

        printf("testElt[%u]: %p testElt[%u]->a: %u\n",
               ii,
               testElt[ii],
               ii,
               testElt[ii]->a);
    }

    lookasideFree(&lookasideList, testElt[0]);
    dummyElt = (TestElt *) lookasideAlloc(&lookasideList);
    assert(dummyElt == testElt[0]);

    for (ii = 0; ii < NumLookasideElems; ii++) {
        lookasideFree(&lookasideList, testElt[ii]);
    }

    lookasideDestroy(&lookasideList);

    status = lookasideInit(sizeof(TestElt),
                           1,
                           LookasideEnableAllocWait,
                           &lookasideList);
    assert(status == StatusOk);

    dummyElt = (TestElt *) lookasideAlloc(&lookasideList);
    assert(dummyElt != NULL);

    ret = pthread_attr_init(&attr);
    assert(ret == 0);
    ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    assert(ret == 0);

    verifyOk(Runtime::get()->createBlockableThread(&thread,
                                                   &attr,
                                                   lookasideWorker,
                                                   dummyElt));

    dummyElt = (TestElt *) lookasideAlloc(&lookasideList);
    assert(dummyElt != NULL);

    lookasideFree(&lookasideList, dummyElt);
    lookasideDestroy(&lookasideList);

    return StatusOk;
}

class RandomWork : public WorkQueue::Elt
{
  public:
    RandomWork() {}
    virtual ~RandomWork() {}
    void func();

    uint64_t ii;
    sem_t *doneSem;
};

void
RandomWork::func()
{
    RandomWork *randomWork = this;
    RandHandle rndHdl;
    const unsigned numIters = 500;

    rndInitHandle(&rndHdl, sysGetTid());
    sysSleep(2);  // force watchdog to kick in and make new threads

    for (unsigned ii = 0; ii < numIters; ii++) {
        randomWork->ii = rndGenerate32(&rndHdl);
    }

    fprintf(stderr,
            "worker%llu: computed final val %llu\n",
            (unsigned long long) pthread_self(),
            (unsigned long long) randomWork->ii);

    sem_post(randomWork->doneSem);
}

static Status
workQueueSanityTests()
{
    StatsLib *statsLib = StatsLib::get();
    unsigned ii;
    WorkQueue *workQueue, *workQueue2;
    RandomWork randomWork[64];
    sem_t sem;

    verify(sem_init(&sem, 0, 0) == 0);

    Status status = WorkQueueMod::getInstance()->init(InitLevel::UsrNode);
    assert(status == StatusOk);
    WorkQueueMod::getInstance()->destroy();

    statsLib->removeGroupIdFromHashTable("diskIo.workQueue");
    status = WorkQueueMod::getInstance()->init(InitLevel::UsrNode);
    assert(status == StatusOk);

    // XXX Needs to be removed. Only work queue available should be in
    // Sched.cpp.
    statsLib->removeGroupIdFromHashTable("LibUtilTestWorkQueue");
    workQueue = WorkQueue::init("LibUtilTestWorkQueue", 16);
    assert(workQueue != NULL);
    workQueue->destroy();

    WorkQueueMod::getInstance()->destroy();

    statsLib->removeGroupIdFromHashTable("diskIo.workQueue");
    status = WorkQueueMod::getInstance()->init(InitLevel::UsrNode);
    assert(status == StatusOk);

    // XXX Needs to be removed. Only work queue available should be in
    // Sched.cpp.
    statsLib->removeGroupIdFromHashTable("LibUtilTestWorkQueue");
    workQueue = WorkQueue::init("LibUtilTestWorkQueue", 8);
    assert(workQueue != NULL);
    // XXX Needs to be removed. Only work queue available should be in
    // Sched.cpp.
    statsLib->removeGroupIdFromHashTable("LibUtilTestWorkQueue");
    workQueue2 = WorkQueue::init("LibUtilTestWorkQueue2", 8);
    assert(workQueue2 != NULL);

    for (ii = 0; ii < ArrayLen(randomWork); ii++) {
        randomWork[ii].ii = ii;
        randomWork[ii].doneSem = &sem;
        workQueue2->enqueue(&randomWork[ii]);
    }

    sysSleep(12);  // force watchdog to kick in and reap threads

    for (ii = 0; ii < ArrayLen(randomWork); ii++) {
        sem_wait(&sem);
    }

    workQueue->destroy();
    workQueue2->destroy();

    WorkQueueMod::getInstance()->destroy();

    return StatusOk;
}

static Status
mathTests()
{
    uint64_t ret;
    uint64_t val;

    ret = mathRound(1000.12345);
    assert(ret == 1000);
    ret = (uint64_t) mathLog(3, 27);
    assert(ret == 3);

    assert((unsigned) (mathLog((float64_t) 2, (float64_t) 0x40000000)) == 30);

    val = mathNearestPowerOf2(0);
    assert(val == 1);
    val = mathNearestPowerOf2(1);
    assert(val == 1);
    val = mathNearestPowerOf2(2);
    assert(val == 2);
    val = mathNearestPowerOf2(3);
    assert(val == 4);
    val = mathNearestPowerOf2(4);
    assert(val == 4);
    val = mathNearestPowerOf2(5);
    assert(val == 8);
    val = mathNearestPowerOf2(8);
    assert(val == 8);
    val = mathNearestPowerOf2(9);
    assert(val == 16);
    val = mathNearestPowerOf2(16);
    assert(val == 16);
    val = mathNearestPowerOf2(17);
    assert(val == 32);
    val = mathNearestPowerOf2(UINT32_MAX);
    assert(val == 1ULL << BitsPerUInt32);
    val = mathNearestPowerOf2(UINT16_MAX - 5);
    assert(val == 1 << BitsPerUInt16);

    assert(mathGetPrime(2) == 2);
    assert(mathGetPrime(3) == 3);
    assert(mathGetPrime(4) == 3);
    assert(mathGetPrime(5) == 3);
    assert(mathGetPrime(6) == 3);
    assert(mathGetPrime(7) == 7);
    assert(mathGetPrime(8) == 7);
    assert(mathGetPrime(12) == 7);
    assert(mathGetPrime(16) == 13);
    assert(mathGetPrime(32) == 31);
    assert(mathGetPrime(64) == 61);
    assert(mathGetPrime(1 << 26) == 67108859);

    return StatusOk;
}

static Status
randomTests()
{
    uint32_t val;
    uint64_t val2;
    RandHandle rndHdl;

    rndInitHandle(&rndHdl, sysGetTid());
    val = rndGenerate32(&rndHdl);
    fprintf(stderr, MSG_LOGTEST_INT_FMT, val);
    val2 = rndGenerate64(&rndHdl);
    fprintf(stderr, MSG_LOGTEST_LONG_FMT, val2);

    return StatusOk;
}

static Status
memTrackTests()
{
    size_t memSize;
#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
    const char *moduleName = "memTrack testing";
#endif  // MEMORY_TRACKING
    char **memArray;
    char *charArray;
    unsigned int numAllocs;
    unsigned int ii;
    char *p = NULL;

    memSize = 16;

    // Simple allocation and free test
    p = (char *) memAllocExt(memSize, moduleName);
    assert(p != NULL);
    // Make sure we can write to this
    memset(p, 0xf, memSize);
    memFree(p);
    p = NULL;

    memSize = 1000 * 1000 * 50;  // Allocate ~50 MB
    p = (char *) memAllocExt(memSize, moduleName);
    assert(p != NULL);
    memset(p, 0xba, memSize);
    memFree(p);
    p = NULL;

    // Try some aligned mallocs

    memSize = 32;
    // Start with small alignment
    p = (char *) memAllocAlignedExt(8, memSize, moduleName);
    assert(p != NULL);
    memset(p, 0xda, memSize);
    memAlignedFree(p);

    // Now let's do a large alignment
    memSize = 1025;
    p = (char *) memAllocAlignedExt(4096, memSize, moduleName);
    assert(p != NULL);
    memset(p, 0xda, memSize);
    memAlignedFree(p);

    // Allocate a larger amount of data blocks
    numAllocs = 10000;
    memArray = (char **) memAllocAligned(2048, numAllocs * sizeof(char *));
    assert(memArray != NULL);
    memSize = 1025;
    for (ii = 0; ii < numAllocs; ii++) {
        if (ii % 2) {
            p = (char *) memAllocAlignedExt(4096, memSize, moduleName);
        } else {
            p = (char *) memAllocExt(memSize, moduleName);
        }
        assert(p != NULL);
        memArray[ii] = p;
    }

    // Free all of those which were allocated
    for (ii = 0; ii < numAllocs; ii++) {
        p = memArray[ii];
        assert(p != NULL);
        if (ii % 2) {
            memAlignedFree(p);
        } else {
            memFree(p);
        }
    }

    // Manually make a new mem array that isn't aligned
    memAlignedFree(memArray);
    memSize = 257;
    memArray = (char **) memAlloc(memSize);
    assert(memArray != NULL);
    memset(memArray, 0xcd, memSize);

    // Now realloc the array we had
    memArray = (char **) memReallocExt(memArray, memSize, moduleName);
    assert(memArray != NULL);
    memset(memArray, 0xcd, memSize);
    memFree(memArray);

    memArray = (char **) memReallocExt(NULL, memSize, moduleName);
    assert(memArray != NULL);
    memset(memArray, 0xcd, memSize);

    memSize = 52;
    memArray = (char **) memRealloc(memArray, memSize);
    assert(memArray != NULL);
    memset(memArray, 0xcd, memSize);

    memFree(memArray);

    numAllocs = 512;
    memSize = 13;
    charArray = (char *) memCallocExt(numAllocs, memSize, moduleName);
    assert(memArray != NULL);
    for (ii = 0; ii < numAllocs * memSize; ii++) {
        assert(charArray[ii] == 0);
    }
    memFree(charArray);
    charArray = NULL;

    return StatusOk;
}

struct TestNewClass {
    int data;
};

static Status
newTrackTests()
{
    TestNewClass *singleClass = new TestNewClass();
    singleClass->data = 5;
    delete singleClass;

    singleClass = new (std::nothrow) TestNewClass();
    singleClass->data = 6;
    delete singleClass;

    int arraySize = 5;
    TestNewClass *classArray = new TestNewClass[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
        classArray[ii].data = 22;
    }
    delete[] classArray;

    classArray = new (std::nothrow) TestNewClass[arraySize];
    for (int ii = 0; ii < arraySize; ii++) {
        classArray[ii].data = 22;
    }
    delete[] classArray;

    char *st = new char[5];

    delete[] st;

    return StatusOk;
}

#define makePair(decodedString, encodedString)                              \
    {                                                                       \
        .decoded = (uint8_t *) decodedString,                               \
        .decodedSize = sizeof(decodedString) - 1, .encoded = encodedString, \
        .encodedSize = sizeof(encodedString) - 1                            \
    }
static Status
base64Tests()
{
    Status status;
    unsigned ii;
    typedef struct Base64EncodePair {
        uint8_t *decoded;
        size_t decodedSize;
        const char *encoded;
        size_t encodedSize;
    } Base64EncodePair;

    Base64EncodePair pairs[] = {
        makePair("hello world", "aGVsbG8gd29ybGQ="),
        makePair("merry christmas", "bWVycnkgY2hyaXN0bWFz"),
        makePair("double equals", "ZG91YmxlIGVxdWFscw=="),
    };

    for (ii = 0; ii < ArrayLen(pairs); ii++) {
        uint8_t *bufDecOut = NULL;
        size_t bufSizeOut = 0;
        status = base64Decode(pairs[ii].encoded,
                              pairs[ii].encodedSize,
                              &bufDecOut,
                              &bufSizeOut);
        assert(status == StatusOk);
        assert(bufSizeOut == pairs[ii].decodedSize);
        assert(memcmp(bufDecOut, pairs[ii].decoded, bufSizeOut) == 0);
        memFree(bufDecOut);
        bufDecOut = NULL;

        char *bufEncOut = NULL;
        status = base64Encode(pairs[ii].decoded,
                              pairs[ii].decodedSize,
                              &bufEncOut,
                              &bufSizeOut);
        assert(status == StatusOk);
        assert(bufSizeOut == pairs[ii].encodedSize);
        assert(memcmp(bufEncOut, pairs[ii].encoded, bufSizeOut) == 0);
        memFree(bufEncOut);
        bufEncOut = NULL;
    }

    return StatusOk;
}

static Status
archiveTests()
{
    Status status;
    FILE *fp = NULL;
    void *buf;
    void *fileContents;
    size_t bufSize, ret, fileSize;
    ArchiveManifest *manifest;
    long ret_len;
    char filePath[QaMaxQaDirLen + 1];

    ret = snprintf(filePath,
                   sizeof(filePath),
                   "%s/utilTest/archiveSanity.tar.gz",
                   qaGetQaDir());
    assert(ret <= sizeof(filePath));

    // @SymbolCheckIgnore
    fp = fopen(filePath, "rb");
    assert(fp != NULL);

    fseek(fp, 0L, SEEK_END);
    ret_len = ftell(fp);
    // Since a gzip'ed file is at least 9 bytes long (gzip header size),
    // we check for file length > 9 bytes.
    assert(ret_len > 9);
    bufSize = ret_len;
    fseek(fp, 0L, SEEK_SET);

    buf = memAlloc(bufSize);
    assert(buf != NULL);

    // @SymbolCheckIgnore
    ret = fread(buf, 1, bufSize, fp);
    assert(ret == bufSize);

    status = archiveUnpack(buf, bufSize, &manifest);
    assert(status == StatusOk);

    status =
        archiveGetFileData(manifest, "a.txt", &fileContents, &fileSize, NULL);
    assert(status == StatusOk);

    assert(strncmp((char *) fileContents, "Hello World\n", fileSize) == 0);

    status =
        archiveGetFileData(manifest, "b.txt", &fileContents, &fileSize, NULL);
    assert(status == StatusOk);

    assert(strncmp((char *) fileContents, "I'm in a package!\n", fileSize) ==
           0);

    status = archiveGetFileData(manifest,
                                "don't exist",
                                &fileContents,
                                &fileSize,
                                NULL);
    assert(status != StatusOk);

    archiveFreeManifest(manifest);
    manifest = NULL;
    // @SymbolCheckIgnore
    fclose(fp);
    fp = NULL;
    memFree(buf);
    buf = NULL;

    return StatusOk;
}

#define addFileContents(fileName, fileContents)      \
    {                                                \
        fileName, fileContents, sizeof(fileContents) \
    }

static Status
archivePackUnpackTests()
{
    Status status;
    ArchiveManifest *packManifest = NULL, *unpackManifest = NULL;
    void *packedBuf = NULL;
    size_t packedBufSize = 0;
    void *fileContents = NULL;
    size_t fileContentsSize;
    unsigned ii;
    char filePath[XcalarApiMaxPathLen + 1];
    typedef struct {
        const char *fileName;
        const char *fileContents;
        size_t fileContentsSize;
    } FileContents;

    FileContents files[] = {addFileContents("a.txt", "Hello World\n"),
                            addFileContents("b.txt", "I'm in a package!\n")};
    static constexpr const char *dirName = "multi/level/directory/";

    // First let's add a few files to a manifest and pack it to a buffer
    packManifest = archiveNewEmptyManifest();
    assert(packManifest != NULL);

    for (ii = 0; ii < ArrayLen(files); ii++) {
        printf("Adding %s (%lu bytes): %s\n",
               files[ii].fileName,
               files[ii].fileContentsSize,
               files[ii].fileContents);
        status = archiveAddFileToManifest(files[ii].fileName,
                                          (void *) files[ii].fileContents,
                                          files[ii].fileContentsSize,
                                          packManifest,
                                          NULL);
        assert(status == StatusOk);
    }

    // Add a multi level directory
    printf("Adding directory '%s'\n", dirName);
    status = archiveAddDirToManifest(dirName, packManifest);
    assert(status == StatusOk);

    // Add files to the multi level directory
    for (ii = 0; ii < ArrayLen(files); ii++) {
        printf("Adding '%s' to '%s'\n", files[ii].fileName, dirName);
        snprintf(filePath,
                 sizeof(filePath),
                 "%s%s",
                 dirName,
                 files[ii].fileName);
        status = archiveAddFileToManifest(filePath,
                                          (void *) files[ii].fileContents,
                                          files[ii].fileContentsSize,
                                          packManifest,
                                          NULL);
        assert(status == StatusOk);
    }

    status = archivePack(packManifest, &packedBuf, &packedBufSize);
    assert(status == StatusOk);
    assert(packedBuf != NULL);

    printf("Packed bufSize: %lu bytes\n", packedBufSize);

#ifdef TESTING
    {
        // Write archive to disk so that it can be checked
        FILE *fp = NULL;
        fp = fopen("/tmp/libutilTest.tar.gz", "w");
        assert(fp != NULL);
        assert(fwrite(packedBuf, 1, packedBufSize, fp) == packedBufSize);
        fclose(fp);
    }
#endif

    // Now let's try to unpack the buffer we just packed
    status = archiveUnpack(packedBuf, packedBufSize, &unpackManifest);
    assert(status == StatusOk);
    assert(unpackManifest != NULL);

    for (ii = 0; ii < ArrayLen(files); ii++) {
        printf("Checking if %s exists\n", files[ii].fileName);
        status = archiveGetFileData(unpackManifest,
                                    files[ii].fileName,
                                    &fileContents,
                                    &fileContentsSize,
                                    NULL);
        assert(status == StatusOk);
        assert(fileContents != NULL);
        assert(fileContentsSize == files[ii].fileContentsSize);
        assert(strncmp((char *) fileContents,
                       files[ii].fileContents,
                       files[ii].fileContentsSize) == 0);
    }

    // Check the directory and the files within it
    printf("Checking if '%s' exists\n", dirName);
    status = archiveGetFileData(unpackManifest,
                                dirName,
                                &fileContents,
                                &fileContentsSize,
                                NULL);
    assert(status == StatusOk);
    assert(fileContents == NULL);
    assert(fileContentsSize == 0);

    for (ii = 0; ii < ArrayLen(files); ii++) {
        printf("Checking if '%s' exists in '%s'\n",
               files[ii].fileName,
               dirName);
        snprintf(filePath,
                 sizeof(filePath),
                 "%s%s",
                 dirName,
                 files[ii].fileName);
        status = archiveGetFileData(unpackManifest,
                                    filePath,
                                    &fileContents,
                                    &fileContentsSize,
                                    NULL);
        assert(status == StatusOk);
        assert(fileContents != NULL);
        assert(fileContentsSize == files[ii].fileContentsSize);
        assert(strncmp((char *) fileContents,
                       files[ii].fileContents,
                       files[ii].fileContentsSize) == 0);
    }

    memFree(packedBuf);
    packedBuf = NULL;
    archiveFreeManifest(packManifest);
    archiveFreeManifest(unpackManifest);
    packManifest = unpackManifest = NULL;

    return StatusOk;
}

#ifndef RUST
int
main(int argc, char *argv[])
{
    int numTestsFailed = 0;
    Status status;
    int maxNumNodes = 1;
    int myNodeId = 0;
    char *dirTest = dirname(argv[0]);

    status = memTrackInit();
    assert(status == StatusOk);

    status = Config::init();
    assert(status == StatusOk);

    Config *config = Config::get();

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    status = config->loadConfigFiles(fullCfgFilePath);
    assert(status == StatusOk);

    status = config->setNumActiveNodes(maxNumNodes);
    assert(status == StatusOk);
    config->setMyNodeId(myNodeId);
    verifyOk(config->loadNodeInfo());
    status = config->loadBinaryInfo();
    assert(status == StatusOk);

    status = hashInit();
    assert(status == StatusOk);

    status = XcSysHelper::initSysHelper();
    assert(status == StatusOk);

    status = BufferCacheMgr::init(BufferCacheMgr::TypeNone, 0);
    assert(status == StatusOk);

    status = StatsLib::createSingleton();
    assert(status == StatusOk);

    status = archiveInit();
    assert(status == StatusOk);

    status = Runtime::get()->init(InitLevel::Basic);
    assert(status == StatusOk);

    numTestsFailed = qaRunTestSuite(testCases,
                                    (unsigned) ArrayLen(testCases),
                                    testCaseOptionMask);

    Runtime::get()->destroy();

    archiveDestroy();
    StatsLib::deleteSingleton();
    if (BufferCacheMgr::get()) {
        BufferCacheMgr::get()->destroy();
    }

    config->unloadConfigFiles();
    if (Config::get()) {
        Config::get()->destroy();
    }
    memTrackDestroy(true);
    return numTestsFailed;
}
#endif
