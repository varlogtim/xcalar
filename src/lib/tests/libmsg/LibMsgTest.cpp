// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>

#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "config/Config.h"
#include "xdb/Xdb.h"
#include "test/QA.h"
#include "util/MemTrack.h"
#include "util/WorkQueue.h"
#include "common/InitTeardown.h"
#include "msg/TwoPcFuncDefs.h"
#include "msg/TwoPcFuncDefsClient.h"

// Function prototypes
static Status basicChecks();

// XXX Move these implementations into the inherited TwoPcAction.
static void immedTargetFn(MsgEphemeral *eph, void *payload);
static void pseudoImmedCompFn(MsgEphemeral *eph, void *payload);
static void recvTargetFn(MsgEphemeral *eph, void *payload);
static void recvTargetCompFn(MsgEphemeral *eph, void *payload);
static Status recvAssert(MsgEphemeral *eph, void *payload);

// Module-global variables used to test proper calling of the target 2PC
// functions
static uint64_t immedCalls = 0;
static uint64_t localWorkCalls = 0;
static uint64_t localCompCalls = 0;
static uint64_t recvCompCalls = 0;

// Constants used anywhere within the module
static const Xid myResource = 123456789;

// Test cases in this module
static TestCase testCases[] = {
    {"Message basic checks", basicChecks, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask = (TestCaseOptionMask)(
    TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime | TestCaseSetTxn);

void
TwoPcMsg2pcTestImmed1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmed1);
    immedTargetFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestImmed1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                            void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmed1);
    pseudoImmedCompFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestImmed1::recvDataCompletion(MsgEphemeral *ephemeral,
                                          void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmed1);
    recvAssert(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::schedLocalWork(MsgEphemeral *ephemeral, void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecv1);
    recvTargetFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::schedLocalCompletion(MsgEphemeral *ephemeral,
                                           void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecv1);
    recvTargetCompFn(ephemeral, payload);
}

void
TwoPcMsg2pcTestRecv1::recvDataCompletion(MsgEphemeral *ephemeral, void *payload)
{
    assert(ephemeral->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecv1);
    recvAssert(ephemeral, payload);
}

// Verify that the twoPc invocations work as expected for a single node
// XXX all combinations of twoPc options should be exhaustively tested
static Status
basicChecks()
{
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    const char *testString = "Basic checks test string";
    size_t testStringLen = strlen(testString) + 1;
    uint64_t oldImmedCalls;
    uint64_t oldLocalWorkCalls;
    uint64_t oldLocalCompCalls;
    Status status;

    oldImmedCalls = immedCalls;
    eph.status = StatusUnknown;

    // Immediate function, called directly from twoPc and not scheduled
    MsgMgr::get()
        ->twoPcEphemeralInit(&eph,                 // ephemeral
                             (void *) testString,  // payload
                             testStringLen,        // payload length
                             0,                    // twoPc does not alloc
                             TwoPcSlowPath,        // use malloc
                             TwoPcCallId::Msg2pcTestImmed1,  // target function
                             NULL,  // no return payload
                             (TwoPcBufLife)(TwoPcMemCopyInput));  // input only

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcTestImmed,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  0,
                                  TwoPcClassNonNested);
    assert(status == StatusOk);

    assert(!twoPcHandle.twoPcHandle);
    assert(eph.status == StatusUnknown);
    assert(immedCalls == oldImmedCalls + 1);
    oldImmedCalls = immedCalls;
    oldLocalWorkCalls = localWorkCalls;
    oldLocalCompCalls = localCompCalls;

    // Immediate function directed to all nodes.  This goes through the normal
    // local work schedule, local work complete functions.
    MsgMgr::get()
        ->twoPcEphemeralInit(&eph,                 // ephemeral
                             (void *) testString,  // payload
                             testStringLen,        // payload length
                             0,                    // twoPc does not alloc
                             TwoPcSlowPath,        // use malloc
                             TwoPcCallId::Msg2pcTestImmed1,  // target function
                             NULL,  // no return payload
                             (TwoPcBufLife)(TwoPcMemCopyInput));  // input only

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcTestImmed,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);

    assert(!twoPcHandle.twoPcHandle);
    assert(eph.status == StatusUnknown);
    assert(immedCalls == oldImmedCalls + 1);
    assert(localWorkCalls == oldLocalWorkCalls);
    assert(localCompCalls == oldLocalCompCalls + 1);
    assert(recvCompCalls == 0);
    oldLocalWorkCalls = localWorkCalls;
    oldLocalCompCalls = localCompCalls;

    // Normal local work call directed to one node.  This goes through the
    // local work, local completion functions.
    MsgMgr::get()
        ->twoPcEphemeralInit(&eph,                 // ephemeral
                             (void *) testString,  // payload
                             testStringLen,        // payload length
                             0,                    // twoPc does not alloc
                             TwoPcSlowPath,        // use malloc
                             TwoPcCallId::Msg2pcTestRecv1,  // target function
                             NULL,                          // no return payload
                             (TwoPcBufLife)(TwoPcMemCopyInput));  // input only

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcTestRecv,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcSingleNode,
                                  0,
                                  TwoPcClassNonNested);

    assert(!twoPcHandle.twoPcHandle);
    assert(eph.status == StatusUnknown);
    assert(immedCalls == oldImmedCalls + 1);
    assert(localWorkCalls == oldLocalWorkCalls + 1);
    assert(localCompCalls == oldLocalCompCalls + 1);
    oldLocalWorkCalls = localWorkCalls;
    oldLocalCompCalls = localCompCalls;

    // Normal local work call directed to all nodes.  This goes through the
    // local work, local completion functions.
    MsgMgr::get()
        ->twoPcEphemeralInit(&eph,                 // ephemeral
                             (void *) testString,  // payload
                             testStringLen,        // payload length
                             0,                    // twoPc does not alloc
                             TwoPcSlowPath,        // use malloc
                             TwoPcCallId::Msg2pcTestRecv1,  // target function
                             NULL,                          // no return payload
                             (TwoPcBufLife)(TwoPcMemCopyInput));  // input only

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  MsgTypeId::Msg2pcTestRecv,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                     MsgRecvHdrOnly),
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);

    assert(!twoPcHandle.twoPcHandle);
    assert(eph.status == StatusUnknown);
    assert(immedCalls == oldImmedCalls + 1);
    assert(localWorkCalls == oldLocalWorkCalls + 1);
    assert(localCompCalls == oldLocalCompCalls + 1);

    return StatusOk;
}

// Function that is invoked for our immediate test function on a "single node"
static void
immedTargetFn(MsgEphemeral *eph, void *payload)
{
    printf("Immediate function called \n");
    immedCalls++;

    eph->status = StatusOk;
}

// This is the work completions function called in the case where an immediate
// function is invoked via 2PC with TwoPcAllNodes.  It's called pseudo
// because it isn't really immediate.
static void
pseudoImmedCompFn(MsgEphemeral *eph, void *payload)
{
    printf(
        "Immediate local completion function called - immediate "
        "to all nodes \n");
    localCompCalls++;
    assert(eph->status == StatusOk);
}

// This is the local work function called for the normal style 2PC
static void
recvTargetFn(MsgEphemeral *eph, void *payload)
{
    printf("Local work function called \n");
    localWorkCalls++;

    eph->status = StatusOk;
}

// This is the local work completion function for the normal style 2PC
static void
recvTargetCompFn(MsgEphemeral *eph, void *payload)
{
    printf("Local work completion function called \n");
    localCompCalls++;
    assert(eph->status == StatusOk);
}

// This is the local work function called for the normal style 2PC
// This function would be called if we receive completion from remote nodes.
// Since we have no remote nodes, this should never happen.
static Status
recvAssert(MsgEphemeral *eph, void *payload)
{
    printf("Remote receive function called \n");
    assert(recvCompCalls == 0);
    assert(0);

    return StatusUnknown;
}

// Set up the environment for the test, run the test cases and destroy
// the environment
int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";

    char fullCfgFilePath[255];
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);
    int numFailedTests = 0;

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../../bin/usrnode/usrnode");

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

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
