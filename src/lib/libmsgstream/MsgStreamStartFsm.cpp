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
#include "MsgStreamStartFsm.h"
#include "MsgStreamState.h"

static constexpr const char *moduleName = "LibMsgStream";

FsmState::TraverseState
StateMsgStreamStart::doWork()
{
    MsgStreamMgr::ProtocolDataUnit *pdu = NULL;
    size_t payloadLen = sizeof(MsgStreamMgr::ProtocolDataUnit);
    Status status;
    MsgStreamStartFsm *startFsm =
        dynamic_cast<MsgStreamStartFsm *>(getSchedFsm());
    MsgStreamInfo *msgStreamInfo = startFsm->getMsgStreamInfo();
    StateMsgStreamStartCompletion *nextState;
    StateMsgStreamStart *curState;
    MsgTypeId msgTypeId = MsgTypeId::Msg2pcStream;
    MsgEphemeral eph;

    // XXX Assume that the payload here is always malloced here for now. Going
    // forward, we will probably allow the Stream object to choose the
    // appropriate memory pool instead like say Buf$.

    pdu =
        (MsgStreamMgr::ProtocolDataUnit *) memAllocExt(payloadLen, moduleName);
    BailIfNull(pdu);

    // Transition state to handle completions.
    nextState = new (std::nothrow) StateMsgStreamStartCompletion(startFsm);
    BailIfNull(nextState);

    curState =
        dynamic_cast<StateMsgStreamStart *>(startFsm->setNextState(nextState));
    assert(curState == this);
    delete curState;
    curState = NULL;
    nextState = NULL;

    // Set up the stream information to be dispatched to remote node.
    pdu->streamId_ = msgStreamInfo->streamId_;
    pdu->streamState_ = msgStreamInfo->streamState_;
    pdu->streamObject_ = msgStreamInfo->streamObject_;
    pdu->bufferSize_ = (uint32_t) payloadLen;
    pdu->srcNodeId_ = msgStreamInfo->srcNodeId_;
    pdu->dstNodeId_ = msgStreamInfo->dstNodeId_;
    pdu->twoPcNodeProperty_ = msgStreamInfo->twoPcNodeProperty_;

    // XXX Assume here that MsgStream is always slow path for now. May be we
    // could back the payload by Buf$ exclusively for fast path in the future.

    // XXX Assume TwoPcZeroCopyInput + TwoPcZeroCopyOutput always and both
    // come from malloc pool. This can be enhanced in the future to always come
    // from the Buf$ pool.

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      pdu,
                                      payloadLen,
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcStreamAction,
                                      startFsm,
                                      (TwoPcBufLife)(TwoPcZeroCopyInput |
                                                     TwoPcZeroCopyOutput));

    status = MsgMgr::get()->twoPcAlt(msgTypeId,
                                     &eph,
                                     (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                        MsgRecvHdrOnly),
                                     TwoPcAltCmd,
                                     CallerBufForPayload,
                                     TwoPcSingleNode,
                                     msgStreamInfo->dstNodeId_,
                                     Txn::currentTxn());
    BailIfFailed(status);

    startFsm->suspend();

CommonExit:
    if (pdu != NULL) {
        memFree(pdu);
        pdu = NULL;
    }

    if (status != StatusOk) {
        startFsm->setStatus(status);

        FsmState *cleanupState = startFsm->getCurState();
        assert(cleanupState != NULL);
        verify(startFsm->setNextState(NULL) == cleanupState);
        delete cleanupState;
        cleanupState = NULL;
    }

    return TraverseState::TraverseNext;
}

FsmState::TraverseState
StateMsgStreamStartCompletion::doWork()
{
    MsgStreamStartFsm *startFsm =
        dynamic_cast<MsgStreamStartFsm *>(getSchedFsm());
    FsmState *cleanupState = startFsm->getCurState();
    if (cleanupState != NULL) {
        verify(startFsm->setNextState(NULL) == cleanupState);
        delete cleanupState;
        cleanupState = NULL;
    }

    return TraverseState::TraverseNext;
}
