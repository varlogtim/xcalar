// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "xdb/Xdb.h"
#include "XdbTests.h"
#include "XdbTestsCommon.h"
#include "LibXdbFuncTestConfig.h"
#include "newtupbuf/NewTuplesCursor.h"
#include "test/QA.h"

static constexpr const char *moduleName = "XdbStress";
static constexpr const uint64_t MAGIC = 0xaabbccddeeffaa;

// Number of stress threads for running the Function tests.
static uint64_t xdbFuncStressThreads = 7;
static bool xdbCursorTestsEnable = true;
static bool xdbStringTestsEnable = true;
static bool xdbPgCursorThreadStressEnable = true;
static bool xdbOrderedVsUnorderedStressEnable = true;
static bool xdbPgCursorPerfTestEnable = true;
static bool xdbCreateLoadDropTestEnable = true;
static bool xdbSortStressEnable = true;
static uint64_t xdbSortNumRows = 1 << 15;

struct ArgXdbStressPerThread {
    pthread_t threadId;
    Status retStatus;
};

static void *
xdbStressPerThread(void *arg)
{
    ArgXdbStressPerThread *argInfo = (ArgXdbStressPerThread *) arg;
    Status status = StatusOk;
    uint64_t threadNum = argInfo->threadId;

    xSyslog(moduleName, XlogInfo, "thread %lu begin tests.", threadNum);

    if (xdbCursorTestsEnable) {
        xSyslog(moduleName, XlogInfo, "thread %lu xdbCursorTests.", threadNum);
        Status retStatus = xdbCursorTests();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbCursorTests failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbStringTestsEnable) {
        xSyslog(moduleName, XlogInfo, "thread %lu xdbStringTests.", threadNum);
        Status retStatus = xdbStringTests();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbStringTests failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbPgCursorThreadStressEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu xdbPgCursorThreadStress.",
                threadNum);
        Status retStatus = xdbPgCursorThreadStress();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbPgCursorThreadStress failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbOrderedVsUnorderedStressEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu xdbOrderedVsUnorderedStress.",
                threadNum);
        Status retStatus = xdbOrderedVsUnorderedStress();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbOrderedVsUnorderedStress failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbPgCursorPerfTestEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu xdbPgCursorPerfTest.",
                threadNum);
        Status retStatus = xdbPgCursorPerfTest();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbPgCursorPerfTest failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbCreateLoadDropTestEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu xdbCreateLoadDropTest.",
                threadNum);
        Status retStatus = xdbCreateLoadDropTest();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbCreateLoadDropTest failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (xdbSortStressEnable) {
        xSyslog(moduleName, XlogInfo, "thread %lu xdbSortTest.", threadNum);
        Status retStatus =
            xdbSortTest((uint64_t) xdbSortNumRows / xdbFuncStressThreads);
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu xdbSortTest failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
        if (retStatus == StatusNoXdbPageBcMem) {
            status = StatusOk;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "thread %lu ends tests with %s.\n",
            threadNum,
            strGetFromStatus(status));
    argInfo->retStatus = status;
    return NULL;
}

Status
xdbStress()
{
    Status status = StatusOk;

    ArgXdbStressPerThread *args = (ArgXdbStressPerThread *) memAlloc(
        sizeof(ArgXdbStressPerThread) * xdbFuncStressThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < xdbFuncStressThreads; ii++) {
        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       xdbStressPerThread,
                                                       &args[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    xSyslog(moduleName,
            XlogInfo,
            "xdbStress start, %lu threads have been created.",
            xdbFuncStressThreads);

    for (uint64_t ii = 0; ii < xdbFuncStressThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "xdbStress end, %lu threads have been joined with %s.",
            xdbFuncStressThreads,
            strGetFromStatus(status));

CommonExit:

    // Out of resource is a valid/Success case for Functional tests.
    if (status == StatusNoMem) {
        status = StatusOk;
    }

    if (args != NULL) {
        memFree(args);
        args = NULL;
    }

    return status;
}

struct NewThreadArg {
    NewTuplesBuffer **fTbufs;
    uint64_t startFixedTbuf;
    uint64_t endFixedTbuf;
    NewTupleMeta *tupleMeta;
};

void *
newThreadWrite(void *args)
{
    Status status;
    uint64_t numInserts = 0;
    NewThreadArg *arg = (NewThreadArg *) args;

    NewTupleValues tupleValues;
    DfFieldValue valueTmp;
    valueTmp.uint64Val = MAGIC;
    tupleValues.set(0, valueTmp, DfUInt64);

    for (uint64_t ii = arg->startFixedTbuf; ii < arg->endFixedTbuf; ii++) {
        do {
            NewTuplesBuffer *fTbuf = arg->fTbufs[ii];
            status = fTbuf->append(arg->tupleMeta, &tupleValues);
            numInserts++;
        } while (status == StatusOk);
    }

    return (void *) numInserts;
}

void *
newThreadRead(void *args)
{
    Status status;
    NewThreadArg *arg = (NewThreadArg *) args;
    uint64_t numReads = 0;
    size_t numFields = arg->tupleMeta->getNumFields();

    for (uint64_t ii = arg->startFixedTbuf; ii < arg->endFixedTbuf; ii++) {
        NewTuplesBuffer *fTbuf = arg->fTbufs[ii];
        NewTuplesCursor fCursor(fTbuf);
        NewTupleValues fTuplePtr;

        do {
            status = fCursor.getNext(arg->tupleMeta, &fTuplePtr);
            numReads++;
            if (status == StatusOk) {
                bool retIsValid;
                DfFieldValue value =
                    fTuplePtr.get(0, numFields, DfInt64, &retIsValid);
                assert(retIsValid);
                verify(value.uint64Val == MAGIC);
            }
        } while (status == StatusOk);
    }

    return (void *) numReads;
}

Status
newCursorBenchmark()
{
    Status status = StatusOk;
    unsigned numThreads = 1;  // sysconf(_SC_NPROCESSORS_ONLN);

    NewThreadArg threadArgs[numThreads];
    pthread_t threads[numThreads];
    pthread_attr_t attr;

    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    NewTupleMeta tupleMeta;
    uint64_t ii, jj = 0;
    size_t totalSize = 20 * GB;
    size_t bufSize = XdbMgr::bcSize();
    size_t numBufs = totalSize / bufSize;

    tupleMeta.setNumFields(1);
    tupleMeta.setFieldType(DfInt64, 0);
    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    struct timespec timeStart;
    struct timespec timeEnd;
    unsigned long long diffSecs;
    unsigned long long diffMsecs;
    float gbs;
    uint64_t numInserts = 0, numReads = 0;

    NewTuplesBuffer **fTbufs = new (std::nothrow) NewTuplesBuffer *[numBufs];
    BailIfNull(fTbufs);
    memZero(fTbufs, sizeof(NewTuplesBuffer *) * numBufs);

    for (jj = 0; jj < numBufs; jj++) {
        fTbufs[jj] =
            (NewTuplesBuffer *) XdbMgr::get()
                ->bcAlloc(XidInvalid, &status, XdbMgr::SlabHint::Default);
        BailIfNullWith(fTbufs[jj], status);
        new (fTbufs[jj]) NewTuplesBuffer((void *) fTbufs[jj], bufSize);
    }

    uint64_t curBuf, remainder;
    curBuf = 0;
    remainder = numBufs % numThreads;

    for (jj = 0; jj < numThreads; jj++) {
        threadArgs[jj].fTbufs = fTbufs;
        threadArgs[jj].startFixedTbuf = curBuf;
        threadArgs[jj].tupleMeta = &tupleMeta;

        curBuf += numBufs / numThreads;
        if (remainder > 0) {
            curBuf++;
            remainder--;
        }

        threadArgs[jj].endFixedTbuf = curBuf;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        sysThreadCreate(&threads[jj], &attr, newThreadWrite, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        void *inserts;
        sysThreadJoin(threads[jj], &inserts);

        numInserts += (uint64_t) inserts;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    xSyslog(moduleName,
            XlogErr,
            "New Tuple: Wrote %lu bytes of memory in %llu msecs %.2f GB/s",
            totalSize,
            diffMsecs,
            gbs);

    clock_gettime(CLOCK_MONOTONIC, &timeStart);

    for (jj = 0; jj < numThreads; jj++) {
        sysThreadCreate(&threads[jj], &attr, newThreadRead, &threadArgs[jj]);
    }

    for (jj = 0; jj < numThreads; jj++) {
        void *reads;

        sysThreadJoin(threads[jj], &reads);

        numReads += (uint64_t) reads;
    }

    clock_gettime(CLOCK_MONOTONIC, &timeEnd);

    diffSecs = timeEnd.tv_sec - timeStart.tv_sec;
    diffMsecs =
        ((diffSecs * NSecsPerSec) + timeEnd.tv_nsec - timeStart.tv_nsec) /
        NSecsPerMSec;

    gbs = (totalSize / GB) / ((float) diffMsecs / MSecsPerSec);

    xSyslog(moduleName,
            XlogErr,
            "New Tuple: Read %lu bytes of memory in %llu msecs %.2f GB/s",
            totalSize,
            diffMsecs,
            gbs);

CommonExit:
    if (fTbufs != NULL) {
        for (ii = 0; ii < numBufs; ii++) {
            if (fTbufs[ii] != NULL) {
                XdbMgr::get()->bcFree(fTbufs[ii]);
            }
        }
        delete[] fTbufs;
    }

    return status;
}

Status
xdbStressParseConfig(Config::Configuration *config,
                     char *key,
                     char *value,
                     bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibXdbFuncTestConfig(LibXdbFuncStressThreads)) ==
        0) {
        xdbFuncStressThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbCursorTestsEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbCursorTestsEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbCursorTestsEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbStringTestsEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbStringTestsEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbStringTestsEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbPgCursorThreadStressEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbPgCursorThreadStressEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbPgCursorThreadStressEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbOrderedVsUnorderedStressEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbOrderedVsUnorderedStressEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbOrderedVsUnorderedStressEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbPgCursorPerfTestEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbPgCursorPerfTestEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbPgCursorPerfTestEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbCreateLoadDropTestEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbCreateLoadDropTestEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbCreateLoadDropTestEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbSortTestsEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            xdbSortStressEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            xdbSortStressEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibNumRowsXdbSortTests)) == 0) {
        xdbSortNumRows = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsCursorTestAscending)) == 0) {
        setNumRowsCursorTestAscending(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsCursorTestDescending)) == 0) {
        setNumRowsCursorTestDescending(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsCursorTestInsertAndGetSingle)) ==
               0) {
        setNumRowsCursorTestInsertAndGetSingle(strtoll(value, NULL, 0));
    } else if (
        strcasecmp(key,
                   strGetFromLibXdbFuncTestConfig(
                       LibXdbNumRowsCursorTestInsertAndSeekThenVerifyOne)) ==
        0) {
        setNumRowsCursorTestInsertAndSeekThenVerifyOne(strtoll(value, NULL, 0));
    } else if (
        strcasecmp(key,
                   strGetFromLibXdbFuncTestConfig(
                       LibXdbNumRowsCursorTestInsertAndSeekThenGetNext)) == 0) {
        setNumRowsCursorTestInsertAndSeekThenGetNext(strtoll(value, NULL, 0));
    } else if (
        strcasecmp(key,
                   strGetFromLibXdbFuncTestConfig(
                       LibXdbNumRowsCursorTestPartiallyOrderedVsUnordered)) ==
        0) {
        setNumRowsCursorTestPartiallyOrderedVsUnordered(
            strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbPgCursorStressThreads)) == 0) {
        setXdbPgCursorStressThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsXdbPgCursorStress)) == 0) {
        setNumRowsXdbPgCursorStress(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbOrderedVsUnorderedStressThreads)) == 0) {
        setXdbOrderedVsUnorderedStressThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsXdbOrderedVsUnorderedStress)) == 0) {
        setNumRowsXdbOrderedVsUnorderedStress(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbOrderedVsUnorderedStressThreads)) == 0) {
        setXdbOrderedVsUnorderedStressThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumItersXdbOrderedVsUnorderedStress)) ==
               0) {
        setNumItersXdbOrderedVsUnorderedStress(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsXdbPgCursorPerfTest)) == 0) {
        setNumRowsXdbPgCursorPerfTest(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumRowsPerXdbForCreateLoadDropTest)) == 0) {
        setNumRowsPerXdbForCreateLoadDropTest(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbNumXdbsForCreateLoadDropTest)) == 0) {
        setNumXdbsForCreateLoadDropTest(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbCreateLoadDropTestThreads)) == 0) {
        setXdbCreateLoadDropTestThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbParallelKvInsertsThreads)) == 0) {
        setXdbParallelKvInsertsThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbParallelPageInsertsThreads)) == 0) {
        setXdbParallelPageInsertsThreads(strtoll(value, NULL, 0));
    } else if (strcasecmp(key,
                          strGetFromLibXdbFuncTestConfig(
                              LibXdbParallelCursorThreads)) == 0) {
        setXdbParallelCursorThreads(strtoll(value, NULL, 0));
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
