// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdarg.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <semaphore.h>
#include <libgen.h>

#include "common/InitTeardown.h"
#include "primitives/Primitives.h"
#include "stat/Statistics.h"
#include "strings/String.h"
#include "sys/Check.h"
#include "sys/Socket.h"
#include "test/QA.h"
#include "util/Atomics.h"
#include "util/System.h"

namespace
{
static Status clockTests();
static Status threadTests();
static Status utilTests();
static Status statusTests();
static Status atomicTests();
static Status socketTests();
Status assertionTests();

TestCase testCases[] = {{"clockTests", clockTests, TestCaseEnable, ""},
                        {"threadTests", threadTests, TestCaseEnable, ""},
                        {"utilTests", utilTests, TestCaseEnable, ""},
                        {"statusTests", statusTests, TestCaseEnable, ""},
                        {"atomicTests", atomicTests, TestCaseEnable, ""},
                        {"socketTests", socketTests, TestCaseEnable, ""},
                        {"assertionTests", assertionTests, TestCaseEnable, ""}};

TestCaseOptionMask testCaseOptionMask = TestCaseOptDisableIsPass;
pthread_t hdl;
#ifdef MEMORY_TRACKING  // This avoids an 'unused-variable'
const char *moduleName = "libsystest";
#endif  // MEMORY_TRACKING

Status
clockTests()
{
    uint64_t nanos1, nanos2;
    struct timespec ts1, ts2, increment;
    int ret;

    memZero(&ts1, sizeof(ts1));
    ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts1);
    assert(ret == 0);
    nanos1 = clkTimespecToNanos(&ts1);
    clkNanosToTimespec(nanos1, &ts1);
    nanos2 = clkTimespecToNanos(&ts1);
    assert(nanos1 == nanos2);
    ret = clock_gettime(CLOCK_REALTIME, &ts1);
    assert(ret == 0);
    clkNanosToTimespec(1000, &increment);
    ts2 = ts1;
    ts2.tv_sec += increment.tv_sec;
    ts2.tv_nsec += increment.tv_nsec;
    assert(clkTimespecToNanos(&ts2) == clkTimespecToNanos(&ts1) + 1000);

    return StatusOk;
}

Status
threadTests()
{
    Status sts;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int ret;

    // @SymbolCheckIgnore
    ret = pthread_mutex_init(&mutex, NULL);
    assert(ret == 0);
    ret = pthread_cond_init(&cond, NULL);
    assert(ret == 0);

    sts = sysCondTimedWait(&cond, &mutex, 1 * USecsPerSec);
    assert(sts == StatusTimedOut);

    return StatusOk;
}

Status
utilTests()
{
    assert(sysErrnoToStatus(EOWNERDEAD) == StatusOwnerDead);

    return StatusOk;
}

Status
statusTests()
{
    const char *poisonStr;

    poisonStr = strGetFromStatus(StatusHwPoison);

    assert(strcmp(poisonStr, "XCE-00000084 Memory page has hardware error") ==
           0);

    return StatusOk;
}

Status
atomicTests()
{
    Atomic32 a;
    Atomic64 b;

    atomicWrite32(&a, 100);
    assert(atomicRead32(&a) == 100);
    atomicWrite32(&a, 200);
    assert(atomicRead32(&a) == 200);
    assert(atomicAdd32(&a, 100) == 300);
    assert(atomicRead32(&a) == 300);
    assert(atomicInc32(&a) == 301);
    assert(atomicRead32(&a) == 301);
    assert(atomicSub32(&a, 100) == 201);
    assert(atomicRead32(&a) == 201);
    assert(atomicDec32(&a) == 200);
    assert(atomicRead32(&a) == 200);
    assert(atomicXchg32(&a, 400) == 200);
    assert(atomicRead32(&a) == 400);
    assert(atomicCmpXchg32(&a, 200, 1000) == 400);
    assert(atomicRead32(&a) == 400);
    assert(atomicCmpXchg32(&a, 400, 1000) == 400);
    assert(atomicRead32(&a) == 1000);

    atomicWrite64(&b, 100);
    assert(atomicRead64(&b) == 100);
    atomicWrite64(&b, 200);
    assert(atomicRead64(&b) == 200);
    assert(atomicAdd64(&b, 100) == 300);
    assert(atomicRead64(&b) == 300);
    assert(atomicInc64(&b) == 301);
    assert(atomicRead64(&b) == 301);
    assert(atomicSub64(&b, 100) == 201);
    assert(atomicRead64(&b) == 201);
    assert(atomicDec64(&b) == 200);
    assert(atomicRead64(&b) == 200);
    assert(atomicXchg64(&b, 400) == 200);
    assert(atomicRead64(&b) == 400);
    assert(atomicCmpXchg64(&b, 200, 1000) == 400);
    assert(atomicRead64(&b) == 400);
    assert(atomicCmpXchg64(&b, 400, 1000) == 400);
    assert(atomicRead64(&b) == 1000);

    return StatusOk;
}

void
initSgArray(SgArray *sgArray, void *buf, size_t len)
{
    SgElem *elem = sgArray->elem;
    sgArray->numElems = 1;
    elem[0].baseAddr = buf;
    elem[0].len = len;
}

void *
socketTestServer(void *_sem)
{
    SocketHandle masterSocket, clientSocket;
    SocketAddr bindAddr;
    Status sts;
    sem_t *connectSem = (sem_t *) _sem;
    SgArray *sgArray = (SgArray *) memAlloc(sizeof(SgArray) + sizeof(SgElem));
    assert(sgArray != NULL);
    uint32_t data1 = 0xdeadbeef;
    uint32_t data2 = 0xc0ffee;
    size_t bytesSent;

    sts = sockCreate(SockIPAddrAny,
                     31337,
                     SocketDomainUnspecified,
                     SocketTypeStream,
                     &masterSocket,
                     &bindAddr);
    assert(sts == StatusOk);
    sts = sockSetOption(masterSocket, SocketOptionReuseAddr);
    assert(sts == StatusOk);
    sts = sockBind(masterSocket, &bindAddr);
    assert(sts == StatusOk);
    sts = sockListen(masterSocket, 1);
    assert(sts == StatusOk);

    sem_post(connectSem);

    sts = sockAccept(masterSocket, NULL, &clientSocket);
    assert(sts == StatusOk);

    initSgArray(sgArray, &data1, sizeof(data2));
    sts = sockSendV(clientSocket, sgArray, &bytesSent, 0);
    assert(sts == StatusOk);
    assert(bytesSent == sizeof(data1));

    sts = sockSendConverged(clientSocket, &data2, sizeof(data2));
    assert(sts == StatusOk);

    sem_wait(connectSem);

    sockDestroy(&clientSocket);
    sockDestroy(&masterSocket);
    sem_destroy(connectSem);
    memFree(connectSem);
    memFree(sgArray);

    return NULL;
}

Status
socketTests()
{
    pthread_attr_t attr;
    Status sts;
    SocketHandle connectSocket;
    SocketAddr serverAddr;
    sem_t *connectSem;
    SgArray *sgArray = (SgArray *) memAlloc(sizeof(SgArray) + sizeof(SgElem));
    assert(sgArray != NULL);
    uint32_t data1 = 0;
    uint32_t data2 = 0;
    size_t bytesRecv;
    SocketHandleSet masterSet, workingSet;
    unsigned numReady;
    struct timespec timeout;
    int ret;

    connectSem = (sem_t *) memAllocExt(sizeof(*connectSem), moduleName);
    ret = sem_init(connectSem, 0, 0);
    assert(ret == 0);

    ret = pthread_attr_init(&attr);
    assert(ret == 0);
    ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    assert(ret == 0);
    verifyOk(Runtime::get()->createBlockableThread(&hdl,
                                                   &attr,
                                                   socketTestServer,
                                                   connectSem));

    sts = sockCreate(SOCK_LOCALHOST,
                     31337,
                     SocketDomainUnspecified,
                     SocketTypeStream,
                     &connectSocket,
                     &serverAddr);
    assert(sts == StatusOk);

    sem_wait(connectSem);
    sts = sockConnect(connectSocket, &serverAddr);
    assert(sts == StatusOk);

    sockHandleSetInit(&masterSet);
    assert(!sockHandleSetIncludes(&masterSet, connectSocket));
    sockHandleSetAdd(&masterSet, connectSocket);
    assert(sockHandleSetIncludes(&masterSet, connectSocket));
    sockHandleSetRemove(&masterSet, connectSocket);
    assert(!sockHandleSetIncludes(&masterSet, connectSocket));
    sockHandleSetAdd(&masterSet, connectSocket);
    assert(sockHandleSetIncludes(&masterSet, connectSocket));

    workingSet = masterSet;

    sts = sockSelect(&workingSet, NULL, NULL, NULL, &numReady);
    assert(sts == StatusOk);
    assert(numReady == 1);
    assert(sockHandleSetIncludes(&workingSet, connectSocket));

    initSgArray(sgArray, &data1, sizeof(data1));
    sts = sockRecvV(connectSocket, sgArray, &bytesRecv);
    assert(sts == StatusOk);
    assert(bytesRecv == sizeof(data1));
    assert(data1 == 0xdeadbeef);

    sts = sockRecvConverged(connectSocket, &data2, sizeof(data2));
    assert(sts == StatusOk);
    assert(data2 == 0xc0ffee);

    clkNanosToTimespec(1000, &timeout);
    workingSet = masterSet;
    sts = sockSelect(&workingSet, NULL, NULL, &timeout, &numReady);
    assert(sts == StatusOk);
    assert(numReady == 0);

    sem_post(connectSem);
    sockDestroy(&connectSocket);
    // server thread destroys the sem since he is the last one to wait on it
    pthread_attr_destroy(&attr);

    sts = sysEaiErrToStatus(EAI_BADFLAGS);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_NONAME);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_AGAIN);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_FAIL);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_FAMILY);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_SOCKTYPE);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_SERVICE);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_MEMORY);
    assert(sts != StatusOk);
    errno = ENOMEM;
    sts = sysEaiErrToStatus(EAI_SYSTEM);
    assert(sts == StatusNoMem);
    sts = sysEaiErrToStatus(EAI_OVERFLOW);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_NODATA);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_ADDRFAMILY);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_INPROGRESS);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_CANCELED);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_NOTCANCELED);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_ALLDONE);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_INTR);
    assert(sts != StatusOk);
    sts = sysEaiErrToStatus(EAI_IDN_ENCODE);
    assert(sts != StatusOk);

    memFree(sgArray);
    sgArray = NULL;

    return StatusOk;
}

// A user-defined type to test CHECK() macros.
struct Thing {
    int a;

    friend std::ostream &operator<<(std::ostream &st, const Thing &t)
    {
        st << "Thing: " << t.a;
        return st;
    }

    friend bool operator==(const Thing &t1, const Thing &t2)
    {
        return t1.a == t2.a;
    }
};

Status
assertionTests()
{
    Thing t1{5}, t2{6};
    CHECK_EQ(t1, t1);
    CHECK(!(t1 == t2));
    // Fails! CHECK_EQ(t1, t2);

    std::string s("blah");
    auto p = &s, q = &s;
    CHECK_EQ(p, q);
    DCHECK_EQ(p, q);
    // Fails! CHECK_EQ(p, nullptr);

    int i = 10;
    CHECK_LE(i, 10);
    DCHECK_LE(i, 10);
    CHECK_LT(i, 11);
    DCHECK_LT(i, 11);

    return StatusOk;
}
}

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::Basic,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                NULL,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    numTestsFailed =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    sysThreadJoin(hdl, NULL);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
