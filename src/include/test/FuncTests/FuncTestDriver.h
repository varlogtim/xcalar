// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _FUNC_TESTS_H_
#define _FUNC_TESTS_H_

#include "primitives/Primitives.h"
#include "msg/MessageTypes.h"
#include "test/FuncTests/FuncTestTypes.h"

class FuncTestDriver
{
  public:
    static Status createSingleton();
    static void deleteSingleton();
    static FuncTestDriver *get();
    Status runFuncTests(
        bool parallel,
        bool runAllTest,
        bool runOnAllNodes,
        unsigned numTestsIn,
        const char testNames[][FuncTestTypes::MaxTestNameLen + 1],
        XcalarApiOutput **outputOut,
        uint64_t *outputSizeOut);
    Status listTests(const char *testNamePattern,
                     char (**testNamesOut)[FuncTestTypes::MaxTestNameLen + 1],
                     unsigned *numTestsOut);

    void processTwoPcRequest(MsgEphemeral *eph, void *payload);

    void processTwoPcComplete(MsgEphemeral *eph, void *payload);

  private:
    struct FuncTestOutputPerNode {
        Status status;
        uint64_t numTestToRun;
        unsigned numTestReturned;
        FuncTestTypes::FuncTestOutput *testOutputs;
    };

    struct TwoPcPayload {
        bool parallel;
        bool runAllTests;
        unsigned numTestPatterns;
        char testNamePatterns[0][FuncTestTypes::MaxTestNameLen + 1];
    };

    struct TwoPcCompletionPayload {
        uint64_t numTestReturned;
        FuncTestTypes::FuncTestOutput funcTestsOutputArray[0];
    };

    FuncTestDriver(){};
    ~FuncTestDriver(){};

    static FuncTestDriver *funcTestDriver;
    static bool funcTestDriverInited;

    static void *startTestFn(void *arg);
    bool matchTest(const char *testName,
                   unsigned numTests,
                   const char testNames[][FuncTestTypes::MaxTestNameLen + 1]);

    struct RunFtLocalArgs {
        bool parallel;
        bool runAllTest;
        unsigned numTestsIn;
        const char (*testNames)[FuncTestTypes::MaxTestNameLen + 1];
        FuncTestTypes::FuncTestOutput *funcTestOutputArray;
        unsigned *numTestsOut;
        Semaphore *doneSem;
        Status *returnStatus;
    };
    static void *runTestFn(void *arg);

    Status runFuncTestsLocal(
        bool parallel,
        bool runAllTest,
        unsigned numTestsIn,
        const char testNames[][FuncTestTypes::MaxTestNameLen + 1],
        FuncTestTypes::FuncTestOutput *funcTestOutputArray,
        unsigned *numTestsOut);
    Status runFuncTestsInit(
        bool runAllTest,
        unsigned numTests,
        const char testNames[][FuncTestTypes::MaxTestNameLen + 1]);
    void runFuncTestsCleanOut(
        bool runAllTest,
        unsigned numTests,
        const char testNames[][FuncTestTypes::MaxTestNameLen + 1]);

    // fix up statuses that don't indicate test failure
    static StatusCode fixupStatus(StatusCode status);

    FuncTestDriver(const FuncTestDriver &) = delete;
    FuncTestDriver &operator=(const FuncTestDriver &) = delete;
};

#endif  // _FUNC_TESTS_H_
