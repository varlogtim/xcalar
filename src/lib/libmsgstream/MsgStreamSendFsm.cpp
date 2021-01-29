// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "util/MemTrack.h"
#include "msgstream/MsgStream.h"
#include "MsgStreamSendFsm.h"
#include "MsgStreamState.h"

static constexpr const char *moduleName = "LibMsgStream";

FsmState::TraverseState
StateMsgStreamSend::doWork()
{
    Status status;
    MsgStreamSendFsm *sendFsm = dynamic_cast<MsgStreamSendFsm *>(getSchedFsm());
    MsgStreamMgr::ProtocolDataUnit *pdu = sendFsm->getPayload();
    size_t payloadLen = pdu->bufferSize_;
    MsgStreamInfo *msgStreamInfo = sendFsm->getMsgStreamInfo();
    StateMsgStreamSendCompletion *nextState;
    StateMsgStreamSend *curState;
    MsgTypeId msgTypeId = MsgTypeId::Msg2pcStream;
    MsgStreamState *msgStreamState = MsgStreamState::get();
    MsgEphemeral eph;

    sendFsm->incOutstanding();

    // Transition state to handle completions.
    nextState = new (std::nothrow) StateMsgStreamSendCompletion(sendFsm);
    BailIfNull(nextState);

    curState =
        dynamic_cast<StateMsgStreamSend *>(sendFsm->setNextState(nextState));
    assert(curState == this);
    delete curState;
    curState = NULL;
    nextState = NULL;

    sendFsm->incOutstanding();
    sendFsm->setPayload(NULL);  // Payload is not valid after issuing twoPcAlt.
    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      pdu,
                                      payloadLen,
                                      0,
                                      TwoPcSlowPath,  // Slow path
                                      TwoPcCallId::Msg2pcStreamAction,
                                      sendFsm,
                                      (TwoPcBufLife)(TwoPcZeroCopyInput));

    status = MsgMgr::get()->twoPcAlt(msgTypeId,
                                     &eph,
                                     (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                        MsgRecvHdrOnly),
                                     TwoPcAltCmd,
                                     CallerBufForPayload,
                                     TwoPcSingleNode,
                                     msgStreamInfo->dstNodeId_,
                                     Txn::currentTxn());
    if (status != StatusOk) {
        sendFsm->decOutstanding();
        assert(sendFsm->getOutstanding() == 1);
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        sendFsm->setStatus(status);
        FsmState *cleanupState = sendFsm->getCurState();
        if (cleanupState != NULL) {
            verify(sendFsm->setNextState(NULL) == cleanupState);
            delete cleanupState;
            cleanupState = NULL;
        }
        msgStreamState->processSendCompletions(msgStreamInfo,
                                               sendFsm->getSendContext(),
                                               sendFsm->getStatus());
        assert(sendFsm->getOutstanding() == 1);
    }

    assert(sendFsm->getOutstanding() > 0);
    return TraverseState::TraverseStop;
}

FsmState::TraverseState
StateMsgStreamSendCompletion::doWork()
{
    MsgStreamSendFsm *sendFsm = dynamic_cast<MsgStreamSendFsm *>(getSchedFsm());
    MsgStreamInfo *msgStreamInfo = sendFsm->getMsgStreamInfo();
    FsmState *cleanupState;
    MsgStreamState *msgStreamState = MsgStreamState::get();

    cleanupState = sendFsm->getCurState();
    if (cleanupState != NULL) {
        verify(sendFsm->setNextState(NULL) == cleanupState);
        delete cleanupState;
        cleanupState = NULL;
    }

    msgStreamState->processSendCompletions(msgStreamInfo,
                                           sendFsm->getSendContext(),
                                           sendFsm->getStatus());
    assert(sendFsm->getOutstanding() > 0);
    return TraverseState::TraverseStop;
}
