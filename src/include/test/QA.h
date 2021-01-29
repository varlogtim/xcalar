// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QA_H_
#define _QA_H_

#include <execinfo.h>
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>

#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "runtime/Semaphore.h"

// QA.h only to be used in test code. Here we deliberately redefine some of the
// debug macros to make them available in product builds if QA.h is included.
#ifdef XLR_QA
#ifdef NDEBUG
#undef NDEBUG
#include "assert.h"
#endif  // NDEBUG

#ifndef DEBUG
#define DEBUG
// Redefine buggyAssert
#undef buggyAssert
#define buggyAssert(cond) assert(cond)
// Redefine verify
#undef verify
#define verify(cond) assert(cond)
// Redefine verifyOk
#undef verifyOk
#define verifyOk(status) verify((status) == StatusOk)
#endif  // DEBUG
#else
#if defined NDEBUG || defined DEBUG
#error "Trying to include QA.h in non-test file in prod build"
#endif
#endif  // XLR_QA

constexpr size_t QaYelpUserFileSize = 27053171ULL;
constexpr size_t QaYelpUserNumRecords = 70817;
constexpr double QaYelpUserSumFans = 114674.0;
constexpr size_t QaYelpReviewFileSize = 304025587ULL;
constexpr size_t QaYelpReviewsNumRecords = 335022ULL;
constexpr size_t QaGDeltNumRecords = 141623ULL;

enum TestCaseEnableState : bool {
    TestCaseEnable = true,
    TestCaseDisable = false
};

enum TestCaseOption : uint8_t {
    TestCaseOptDisableIsPass = 0x1,
    TestCaseScheduleOnRuntime = 0x2,
    TestCaseScheduleOnDedicatedThread = 0x4,
    TestCaseSetTxn = 0x8,
};

typedef TestCaseOption TestCaseOptionMask;

struct TestCase {
    typedef Status (*TestFn)(void);
    const char *testName;
    TestFn func;
    TestCaseEnableState testEnabled;
    const char *witnessString;
};

class SchedTestCase : public Schedulable
{
  public:
    SchedTestCase(TestCase *testCase, sem_t *testCaseSem, Status *status)
        : Schedulable("SchedTestCase"),
          testCase(testCase),
          testCaseSem(testCaseSem),
          status(status)
    {
    }

    virtual ~SchedTestCase() {}

    virtual void run();
    virtual void done();

    TestCase *testCase = NULL;
    sem_t *testCaseSem = NULL;
    Status *status = NULL;
};

constexpr size_t QaMaxQaDirLen = 1023;

inline const char *
qaGetQaDir()
{
    return getenv("XLRQADIR");
}

int qaRunTestSuite(TestCase testCases[],
                   unsigned numTestCases,
                   TestCaseOptionMask optionMask);

#define qaPrintStackTrace(n, moduleName) \
    qaPrintStackTraceInt(n, moduleName, __FILE__, __func__, __LINE__)

void qaPrintStackTraceInt(int numStackFramesIn,
                          const char *moduleName,
                          const char *fileName,
                          const char *fnName,
                          int lineNumber);

#endif  // _QA_H_
