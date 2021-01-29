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
#include "msg/Message.h"
#include "MsgTests.h"
#include "msg/TwoPcFuncDefs.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "LibMsgFuncTestConfig.h"
#include "util/Random.h"
#include "msgstream/MsgStream.h"

#include "test/FuncTests/MsgStreamFuncTestActions.h"

#include "test/QA.h"

static constexpr const char *moduleName = "MsgStress";

// this includes \0 and the number 15 so that atleast one of the chunks
// transmitted splits the string in different chunks
static constexpr size_t numCharsPerJunkString = 15;
static constexpr const char junkString[] = "junkstringjunk";
// about 7500 KB worth of junk string
static constexpr size_t totalJunkSize = (numCharsPerJunkString) * (500 * KB);
//
// Config param tunables.
//

// Number of stress threads for running the Function tests.
static uint64_t msgFuncStressThreads = 3;

static bool msgTestTypeBasicSuccessEnable = true;
static uint64_t msgTestTypeBasicSuccessThreads = 8;
static uint64_t msgTestTypeBasicSuccessIter = 1ULL << 4;

static bool msgTestTypeBasicFailureEnable = true;
static uint64_t msgTestTypeBasicFailureThreads = 8;
static uint64_t msgTestTypeBasicFailureIter = 1ULL << 4;

static bool msgTestTypeLargePayloadEnable = true;
static uint64_t msgTestTypeLargePayloadThreads = 8;
static uint64_t msgTestTypeLargePayloadIter = 1ULL << 4;
static constexpr size_t MsgTestTypeLargePayloadBufsize = 64 * KB;

static bool msgTestTypeStreamEnable = true;
static uint64_t msgTestTypeStreamSendCount = 1ULL << 5;
static constexpr size_t msgTestTypeStreamPayloadSize = 1021;

//
// MsgStreamFuncTestObjectAction Implementation.
//
MsgStreamFuncTestObjectAction::MsgStreamFuncTestObjectAction() {}

MsgStreamFuncTestObjectAction::~MsgStreamFuncTestObjectAction() {}

MsgStreamMgr::StreamObject
MsgStreamFuncTestObjectAction::getStreamObject() const
{
    return MsgStreamMgr::StreamObject::MsgStreamFuncTest;
}

Status
MsgStreamFuncTestObjectAction::startStreamLocalHandler(Xid streamId,
                                                       void **retObjectContext)
{
    *retObjectContext = (void *) ReceiveSideDummyCookie;
    return StatusOk;
}

Status
MsgStreamFuncTestObjectAction::endStreamLocalHandler(Xid streamId,
                                                     void *retObjectContext)
{
    verify(retObjectContext == (void *) ReceiveSideDummyCookie);
    return StatusOk;
}

Status
MsgStreamFuncTestObjectAction::streamReceiveLocalHandler(
    Xid streamId,
    void *retObjectContext,
    MsgStreamMgr::ProtocolDataUnit *payload)
{
    verify(retObjectContext == (void *) ReceiveSideDummyCookie);
    return StatusOk;
}

void
MsgStreamFuncTestObjectAction::streamSendCompletionHandler(
    Xid streamId, void *retObjectContext, void *sendContext, Status reason)
{
    SendSideCookie *sendSideCookie = (SendSideCookie *) retObjectContext;
    if (reason != StatusOk) {
        // Stream error propagation
        atomicWrite32(&sendSideCookie->streamStatus_, reason.code());
    }
    if (sendSideCookie->streamDone_ == false) {
        if (sendSideCookie->payloadSent_ ==
            (uint64_t) atomicInc64(&sendSideCookie->payloadReceived_)) {
            sendSideCookie->streamDone_ = true;
            sendSideCookie->semDone_.post();
        }
    } else {
        assert(sendSideCookie->payloadSent_ ==
               (uint64_t) atomicRead64(&sendSideCookie->payloadReceived_));
        sendSideCookie->semDone_.post();
    }
}

enum class MsgTestType : uint32_t {
    BasicSuccess,
    BasicFailure,
    LargePayload,
};

struct MsgTestPayload {
    static constexpr uint32_t SendMagic = 0xbaadcafe;
    static constexpr uint32_t RecvMagic = 0xc001cafe;
    uint32_t magic;
    MsgTestType type;
    size_t len;
    char buffer[0];
};

struct MsgTestEphOut {
    Status status;
    MsgTestType type;
    Status retStatus[0];
};

struct ArgMsgStressPerThread {
    pthread_t threadId;
    Status retStatus;
};

struct ArgMsgStressTestPerThread {
    MsgTestType type;
    pthread_t threadId;
    Status retStatus;
};

void
constructJunk(char *buffer, size_t bufferLength)
{
    char *cursor = NULL;

    assertStatic(ArrayLen(junkString) == numCharsPerJunkString);
    assert((strlen(junkString) + 1) == numCharsPerJunkString);
    assert((bufferLength % numCharsPerJunkString) == 0);

    cursor = buffer;
    while (cursor < buffer + (totalJunkSize)) {
        memcpy(cursor, junkString, numCharsPerJunkString);
        cursor[numCharsPerJunkString - 1] = '\0';
        cursor += numCharsPerJunkString;
    }
}

void
TwoPcMsg2pcTestImmedFuncTest::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmedFuncTest);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    verify(testPld->magic == MsgTestPayload::SendMagic);
    testPld->magic = MsgTestPayload::RecvMagic;
    eph->status = StatusOk;

    switch (testPld->type) {
    case MsgTestType::BasicSuccess:  // Pass through
    case MsgTestType::BasicFailure:  // Pass through
    case MsgTestType::LargePayload:
        break;

    default:
        verify(0);
        eph->status = StatusUnimpl;
        break;
    }
}

void
TwoPcMsg2pcTestImmedFuncTest::schedLocalCompletion(MsgEphemeral *eph,
                                                   void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmedFuncTest);
    MsgTestEphOut *ephOut = (MsgTestEphOut *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    verify(testPld->magic == MsgTestPayload::RecvMagic);
    if ((ephOut->status == StatusOk || ephOut->status == StatusUnknown) &&
        eph->status != StatusOk) {
        ephOut->status = eph->status;
    }

    switch (ephOut->type) {
    case MsgTestType::BasicSuccess:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::BasicFailure:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusFailed;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::LargePayload:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    default:
        verify(0);
        break;
    }
}

void
TwoPcMsg2pcTestImmedFuncTest::recvDataCompletion(MsgEphemeral *eph,
                                                 void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestImmedFuncTest);
    MsgTestEphOut *ephOut = (MsgTestEphOut *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    if ((ephOut->status == StatusOk || ephOut->status == StatusUnknown) &&
        eph->status != StatusOk) {
        ephOut->status = eph->status;
    }

    if (eph->status != StatusOk) {
        if (testPld != NULL) {
            verify(testPld->magic == MsgTestPayload::SendMagic);
        } else {
            assert(eph->status == StatusNoMem);
        }
    } else {
        verify(testPld->magic == MsgTestPayload::RecvMagic);
    }

    switch (ephOut->type) {
    case MsgTestType::BasicSuccess:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::BasicFailure:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusFailed;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::LargePayload:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    default:
        verify(0);
        break;
    }
}

void
TwoPcMsg2pcTestRecvFuncTest::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecvFuncTest);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    verify(testPld->magic == MsgTestPayload::SendMagic);
    testPld->magic = MsgTestPayload::RecvMagic;
    eph->status = StatusOk;

    switch (testPld->type) {
    case MsgTestType::BasicSuccess:  // Pass through
    case MsgTestType::BasicFailure:  // Pass through
    case MsgTestType::LargePayload:
        break;

    default:
        verify(0);
        eph->status = StatusUnimpl;
        break;
    }
}

void
TwoPcMsg2pcTestRecvFuncTest::schedLocalCompletion(MsgEphemeral *eph,
                                                  void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecvFuncTest);
    MsgTestEphOut *ephOut = (MsgTestEphOut *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    verify(testPld->magic == MsgTestPayload::RecvMagic);
    if ((ephOut->status == StatusOk || ephOut->status == StatusUnknown) &&
        eph->status != StatusOk) {
        ephOut->status = eph->status;
    }

    switch (ephOut->type) {
    case MsgTestType::BasicSuccess:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::BasicFailure:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusFailed;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::LargePayload:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    default:
        verify(0);
        break;
    }
}

void
TwoPcMsg2pcTestRecvFuncTest::recvDataCompletion(MsgEphemeral *eph,
                                                void *payload)
{
    verify(eph->twoPcFuncIndex == TwoPcCallId::Msg2pcTestRecvFuncTest);
    MsgTestEphOut *ephOut = (MsgTestEphOut *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);
    MsgTestPayload *testPld = (MsgTestPayload *) payload;
    if ((ephOut->status == StatusOk || ephOut->status == StatusUnknown) &&
        eph->status != StatusOk) {
        ephOut->status = eph->status;
    }

    if (eph->status != StatusOk) {
        if (testPld != NULL) {
            verify(testPld->magic == MsgTestPayload::SendMagic);
        } else {
            assert(eph->status == StatusNoMem);
        }
    } else {
        verify(testPld->magic == MsgTestPayload::RecvMagic);
    }

    switch (ephOut->type) {
    case MsgTestType::BasicSuccess:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::BasicFailure:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusFailed;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    case MsgTestType::LargePayload:
        if (eph->status == StatusOk) {
            ephOut->retStatus[dstNodeId] = StatusOk;
        } else {
            ephOut->retStatus[dstNodeId] = eph->status;
        }
        break;

    default:
        verify(0);
        break;
    }
}

static void *
msgTestTypeTestThread(void *arg)
{
    Status status = StatusOk;
    ArgMsgStressTestPerThread *argInfo = (ArgMsgStressTestPerThread *) arg;
    uint64_t threadId = argInfo->threadId;
    RandHandle rndHandle;
    rndInitHandle(&rndHandle, (uint32_t) threadId);
    unsigned numNodes = Config::get()->getActiveNodes();
    MsgTestEphOut *ephOut = NULL;
    MsgTestPayload *payload =
        (MsgTestPayload *) memAlloc((argInfo->type == MsgTestType::LargePayload)
                                        ? MsgTestTypeLargePayloadBufsize
                                        : sizeof(MsgTestPayload));
    if (payload == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    payload->magic = MsgTestPayload::SendMagic;
    payload->type = argInfo->type;
    payload->len = sizeof(MsgTestPayload);

    ephOut = (MsgTestEphOut *) memAlloc(sizeof(MsgTestEphOut) +
                                        sizeof(Status) * numNodes);
    if (ephOut == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    ephOut->status = StatusUnknown;
    ephOut->type = argInfo->type;

    for (uint64_t ii = 0; ii < msgTestTypeBasicSuccessIter; ii++) {
        MsgEphemeral eph;
        eph.status = StatusUnknown;
        TwoPcHandle twoPcHandle;
        for (unsigned jj = 0; jj < numNodes; jj++) {
            ephOut->retStatus[jj] = StatusUnknown;
        }

        MsgTypeId msgTypeId;
        TwoPcCallId callId;
        if (rndGenerate64(&rndHandle) % 2) {
            msgTypeId = MsgTypeId::Msg2pcTestImmed;
            callId = TwoPcCallId::Msg2pcTestImmedFuncTest;
        } else {
            msgTypeId = MsgTypeId::Msg2pcTestRecv;
            callId = TwoPcCallId::Msg2pcTestRecvFuncTest;
        }

        TwoPcNodeProperty nodeProp;
        NodeId dstNode;
        if (rndGenerate64(&rndHandle) % 2) {
            nodeProp = TwoPcSingleNode;
            if (msgTypeId == MsgTypeId::Msg2pcTestImmed) {
                dstNode = Config::get()->getMyNodeId();
            } else {
                dstNode = rndGenerate64(&rndHandle) % numNodes;
            }
        } else {
            nodeProp = TwoPcAllNodes;
            dstNode = TwoPcIgnoreNodeId;
        }

        // Immediate function, called directly from twoPc and not scheduled
        MsgMgr::get()
            ->twoPcEphemeralInit(&eph,              // ephemeral
                                 (void *) payload,  // payload
                                 payload->len,      // payload length
                                 0,                 // twoPc does not alloc
                                 TwoPcSlowPath,     // use malloc
                                 callId,            // target function
                                 ephOut,            // return payload
                                 (TwoPcBufLife)(
                                     TwoPcMemCopyInput));  // input only

        status = MsgMgr::get()->twoPc(&twoPcHandle,
                                      msgTypeId,
                                      TwoPcDoNotReturnHandle,
                                      &eph,
                                      (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                         MsgRecvHdrPlusPayload),
                                      TwoPcSyncCmd,
                                      nodeProp,
                                      dstNode,
                                      TwoPcClassNonNested);
        if (status != StatusOk) {
            goto CommonExit;
        }

        verify(!twoPcHandle.twoPcHandle);

        if (nodeProp == TwoPcAllNodes) {
            for (unsigned jj = 0; ephOut->status == StatusOk && jj < numNodes;
                 jj++) {
                if (argInfo->type == MsgTestType::BasicSuccess ||
                    argInfo->type == MsgTestType::LargePayload) {
                    verify(ephOut->retStatus[jj] == StatusOk);
                } else {
                    assert(argInfo->type == MsgTestType::BasicFailure);
                    verify(ephOut->retStatus[jj] == StatusFailed);
                }
            }
        } else {
            for (unsigned jj = 0; ephOut->status == StatusOk && jj < numNodes;
                 jj++) {
                if (jj == dstNode) {
                    if (argInfo->type == MsgTestType::BasicSuccess ||
                        argInfo->type == MsgTestType::LargePayload) {
                        verify(ephOut->retStatus[jj] == StatusOk);
                    } else {
                        assert(argInfo->type == MsgTestType::BasicFailure);
                        verify(ephOut->retStatus[jj] == StatusFailed);
                    }
                } else {
                    verify(ephOut->retStatus[jj] == StatusUnknown);
                }
            }
        }
    }

CommonExit:
    if (payload != NULL) {
        memFree(payload);
    }
    if (ephOut != NULL) {
        memFree(ephOut);
    }
    argInfo->retStatus = status;
    return NULL;
}

static Status
msgTestTypeBasicSuccess()
{
    Status status = StatusOk;
    ArgMsgStressTestPerThread *args = (ArgMsgStressTestPerThread *) memAlloc(
        sizeof(ArgMsgStressTestPerThread) * msgTestTypeBasicSuccessThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < msgTestTypeBasicSuccessThreads; ii++) {
        args[ii].type = MsgTestType::BasicSuccess;
        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       msgTestTypeTestThread,
                                                       &args[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (uint64_t ii = 0; ii < msgTestTypeBasicSuccessThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

static Status
msgTestTypeBasicFailure()
{
    Status status = StatusOk;
    ArgMsgStressTestPerThread *args = (ArgMsgStressTestPerThread *) memAlloc(
        sizeof(ArgMsgStressTestPerThread) * msgTestTypeBasicFailureThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < msgTestTypeBasicFailureThreads; ii++) {
        args[ii].type = MsgTestType::BasicFailure;
        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       msgTestTypeTestThread,
                                                       &args[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (uint64_t ii = 0; ii < msgTestTypeBasicFailureThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

static Status
msgTestTypeLargePayload()
{
    Status status = StatusOk;
    ArgMsgStressTestPerThread *args = (ArgMsgStressTestPerThread *) memAlloc(
        sizeof(ArgMsgStressTestPerThread) * msgTestTypeLargePayloadThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < msgTestTypeLargePayloadThreads; ii++) {
        args[ii].type = MsgTestType::LargePayload;
        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       msgTestTypeTestThread,
                                                       &args[ii]);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (uint64_t ii = 0; ii < msgTestTypeLargePayloadThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

static Status
msgTestTypeStream(uint64_t threadNum)
{
    Status status;
    Config *config = Config::get();
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned activeNodes = config->getActiveNodes();

    MsgStreamFuncTestObjectAction::SendSideCookie *sendSideCookie =
        (MsgStreamFuncTestObjectAction::SendSideCookie *) memAlloc(
            sizeof(MsgStreamFuncTestObjectAction::SendSideCookie));
    if (sendSideCookie == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < activeNodes; ii++) {
        if (ii == (unsigned) myNodeId) {
            // We are attempting to establish MsgStream from source node to a
            // destination node.
            continue;
        } else {
            Xid streamId;

            new (sendSideCookie) MsgStreamFuncTestObjectAction::
                SendSideCookie(threadNum, msgTestTypeStreamSendCount);

            status =
                msgStreamMgr
                    ->startStream(TwoPcSingleNode,
                                  ii,
                                  MsgStreamMgr::StreamObject::MsgStreamFuncTest,
                                  (void *) sendSideCookie,
                                  &streamId);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "thread %lu streamStart from src node %u to dest node "
                        "%u "
                        "failed with %s.",
                        threadNum,
                        myNodeId,
                        ii,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            for (uint64_t jj = 0; jj < msgTestTypeStreamSendCount; jj++) {
                MsgStreamMgr::StreamState streamState =
                    MsgStreamMgr::StreamState::StreamInProgress;
                uint32_t payloadLen = sizeof(MsgStreamMgr::ProtocolDataUnit) +
                                      msgTestTypeStreamPayloadSize;
                MsgStreamMgr::ProtocolDataUnit *payload =
                    (MsgStreamMgr::ProtocolDataUnit *) memAlloc(payloadLen);
                if (payload == NULL) {
                    status = StatusNoMem;
                    goto ErrorHandling;
                }

                status = msgStreamMgr->initPayloadToSend(streamId,
                                                         payload,
                                                         payloadLen);
                if (status != StatusOk) {
                    goto ErrorHandling;
                }

                *(uint64_t *) ((uintptr_t) payload +
                               sizeof(MsgStreamMgr::ProtocolDataUnit)) =
                    MsgStreamFuncTestObjectAction::PayloadMagic;
                status = msgStreamMgr->sendStream(streamId,
                                                  payload,
                                                  streamState,
                                                  NULL);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "thread %lu sendStream from src node %u to "
                            "dest node %u payload %lu failed with %s.",
                            threadNum,
                            myNodeId,
                            ii,
                            jj,
                            strGetFromStatus(status));
                    goto ErrorHandling;
                }

                memFree(payload);
                payload = NULL;

            ErrorHandling:
                if (status != StatusOk) {
                    if (payload != NULL) {
                        memFree(payload);
                        payload = NULL;
                    }

                    for (uint64_t kk = jj; kk < msgTestTypeStreamSendCount;
                         kk++) {
                        assert(sendSideCookie->streamDone_ == false);
                        if (sendSideCookie->payloadSent_ ==
                            (uint64_t) atomicInc64(
                                &sendSideCookie->payloadReceived_)) {
                            sendSideCookie->streamDone_ = true;
                            sendSideCookie->semDone_.post();
                        }
                    }
                    break;
                }
            }

            // Wait till all the completions have been received for the sent
            // payloads.
            sendSideCookie->semDone_.semWait();
            assert(sendSideCookie->streamDone_ == true);
            assert(sendSideCookie->payloadSent_ ==
                   (uint64_t) atomicRead64(&sendSideCookie->payloadReceived_));

            // Propagate stream status
            if (status == StatusOk) {
                status.fromStatusCode(
                    (StatusCode) atomicRead32(&sendSideCookie->streamStatus_));
            }

            if (status == StatusOk) {
                MsgStreamMgr::StreamState streamState =
                    MsgStreamMgr::StreamState::StreamIsDone;
                uint32_t payloadLen = sizeof(MsgStreamMgr::ProtocolDataUnit) +
                                      msgTestTypeStreamPayloadSize;
                MsgStreamMgr::ProtocolDataUnit *payload =
                    (MsgStreamMgr::ProtocolDataUnit *) memAlloc(payloadLen);
                if (payload == NULL) {
                    status = StatusNoMem;
                } else {
                    status = msgStreamMgr->initPayloadToSend(streamId,
                                                             payload,
                                                             payloadLen);
                    if (status == StatusOk) {
                        *(uint64_t *) ((uintptr_t) payload +
                                       sizeof(MsgStreamMgr::ProtocolDataUnit)) =
                            MsgStreamFuncTestObjectAction::PayloadMagic;
                        status = msgStreamMgr->sendStream(streamId,
                                                          payload,
                                                          streamState,
                                                          NULL);
                        memFree(payload);
                        payload = NULL;
                        if (status != StatusOk) {
                            xSyslog(moduleName,
                                    XlogErr,
                                    "thread %lu sendStream Done from src node "
                                    "%u "
                                    " to dest node %u failed with %s.",
                                    threadNum,
                                    myNodeId,
                                    ii,
                                    strGetFromStatus(status));
                        } else {
                            sendSideCookie->semDone_.semWait();
                            assert(sendSideCookie->streamDone_ == true);
                            assert(sendSideCookie->payloadSent_ ==
                                   (uint64_t) atomicRead64(
                                       &sendSideCookie->payloadReceived_));

                            // Propagate stream status
                            if (status == StatusOk) {
                                status.fromStatusCode((StatusCode) atomicRead32(
                                    &sendSideCookie->streamStatus_));
                            }
                        }
                    } else {
                        memFree(payload);
                        payload = NULL;
                    }
                }
            }

            msgStreamMgr->endStream(streamId);

            if (status != StatusOk) {
                goto CommonExit;
            }
        }
    }
    status = StatusOk;

CommonExit:
    if (sendSideCookie != NULL) {
        memFree(sendSideCookie);
        sendSideCookie = NULL;
    }
    if (status == StatusApiWouldBlock) {
        status = StatusOk;
    }
    return status;
}

static void *
msgStressPerThread(void *arg)
{
    ArgMsgStressPerThread *argInfo = (ArgMsgStressPerThread *) arg;
    Status status = StatusOk;
    uint64_t threadNum = argInfo->threadId;

    xSyslog(moduleName, XlogInfo, "thread %lu begin tests.", threadNum);

    if (msgTestTypeBasicSuccessEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu msgTestTypeBasicSuccess.",
                threadNum);
        Status retStatus = msgTestTypeBasicSuccess();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu msgTestTypeBasicSuccess failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (msgTestTypeBasicFailureEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu msgTestTypeBasicFailure.",
                threadNum);
        Status retStatus = msgTestTypeBasicFailure();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu msgTestTypeBasicFailure failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (msgTestTypeLargePayloadEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu msgTestTypeLargePayload.",
                threadNum);
        Status retStatus = msgTestTypeLargePayload();
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu msgTestTypeLargePayload failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    if (msgTestTypeStreamEnable) {
        xSyslog(moduleName,
                XlogInfo,
                "thread %lu msgTestTypeStream.",
                threadNum);
        Status retStatus = msgTestTypeStream(threadNum);
        if (retStatus != StatusOk) {
            status = retStatus;
            xSyslog(moduleName,
                    XlogErr,
                    "thread %lu msgTestTypeStream failed with %s.",
                    threadNum,
                    strGetFromStatus(retStatus));
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "thread %lu ends tests with %s.",
            threadNum,
            strGetFromStatus(status));
    argInfo->retStatus = status;
    return NULL;
}

Status
verifyJunk(char *buffer, size_t bufferLength)
{
    char *cursor = NULL;
    size_t totalOccurences = 0;

    assert(!(bufferLength % numCharsPerJunkString));
    assert(bufferLength == totalJunkSize);
    assertStatic(ArrayLen(junkString) == numCharsPerJunkString);
    assert((strlen(junkString) + 1) == numCharsPerJunkString);

    cursor = buffer;
    while (cursor < buffer + (totalJunkSize)) {
        assert(strlen(cursor) == numCharsPerJunkString - 1);
        assert(strncmp(cursor, junkString, numCharsPerJunkString) == 0);
        cursor += numCharsPerJunkString;
        totalOccurences++;
    }

    assert(totalOccurences == (totalJunkSize / numCharsPerJunkString));

    return StatusOk;
}

Status
msgStress()
{
    Status status = StatusOk;

    ArgMsgStressPerThread *args = (ArgMsgStressPerThread *) memAlloc(
        sizeof(ArgMsgStressPerThread) * msgFuncStressThreads);
    if (args == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (uint64_t ii = 0; ii < msgFuncStressThreads; ii++) {
        status = Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                       NULL,
                                                       msgStressPerThread,
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
            "msgStress start, %lu threads have been created.",
            msgFuncStressThreads);

    for (uint64_t ii = 0; ii < msgFuncStressThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "msgStress end, %lu threads have been joined with %s.",
            msgFuncStressThreads,
            strGetFromStatus(status));

    // Out of resource is a valid/Success case for Functional tests.
    if (status == StatusNoMem) {
        status = StatusOk;
    }
CommonExit:
    if (args != NULL) {
        memFree(args);
        args = NULL;
    }
    return status;
}

Status
msgStressParseConfig(Config::Configuration *config,
                     char *key,
                     char *value,
                     bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibMsgFuncTestConfig(LibMsgFuncStressThreads)) ==
        0) {
        msgFuncStressThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicSuccessEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            msgTestTypeBasicSuccessEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            msgTestTypeBasicSuccessEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicSuccessIter)) == 0) {
        msgTestTypeBasicSuccessIter = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicSuccessThreads)) == 0) {
        msgTestTypeBasicSuccessThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicFailureEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            msgTestTypeBasicFailureEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            msgTestTypeBasicFailureEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicFailureIter)) == 0) {
        msgTestTypeBasicFailureIter = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeBasicFailureThreads)) == 0) {
        msgTestTypeBasicFailureThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeLargePayloadEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            msgTestTypeLargePayloadEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            msgTestTypeLargePayloadEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeLargePayloadIter)) == 0) {
        msgTestTypeLargePayloadIter = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeLargePayloadThreads)) == 0) {
        msgTestTypeLargePayloadThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibMsgFuncTestConfig(
                              LibMsgTypeMsgStreamEnable)) == 0) {
        if (strcasecmp(value, "true") == 0) {
            msgTestTypeStreamEnable = true;
        } else if (strcasecmp(value, "false") == 0) {
            msgTestTypeStreamEnable = false;
        } else {
            status = StatusUsrNodeIncorrectParams;
        }
    } else {
        status = StatusUsrNodeIncorrectParams;
    }

    return status;
}
