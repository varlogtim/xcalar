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
#include "MsgStreamEndFsm.h"
#include "MsgStreamSendFsm.h"
#include "runtime/Runtime.h"
#include "MsgStreamState.h"
#include "runtime/Tls.h"
#include "msg/Xid.h"

static constexpr const char *moduleName = "LibMsgStream";

MsgStreamMgr *MsgStreamMgr::instance = NULL;

//
// MsgStreamMgr Implementation
//
MsgStreamMgr *
MsgStreamMgr::get()
{
    return instance;
}

Status
MsgStreamMgr::init()
{
    Status status;

    assert(instance == NULL);
    instance = (MsgStreamMgr *) memAllocExt(sizeof(MsgStreamMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) MsgStreamMgr();

    status = MsgStreamState::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
MsgStreamMgr::destroy()
{
    if (MsgStreamState::get()) {
        MsgStreamState::get()->destroy();
    }
    instance->~MsgStreamMgr();
    memFree(instance);
    instance = NULL;
}

// MsgStream firsts establishes the stream cookie between source and
// destination(s) before doing any meaningful work.
Status
MsgStreamMgr::startStream(TwoPcNodeProperty twoPcNodeProperty,
                          NodeId dstNodeId,
                          StreamObject streamObject,
                          void *objectContext,
                          Xid *retStreamId)
{
    Status status;
    bool insertHt = false;
    bool scheduleFsm = false;
    StateMsgStreamStart *startState = NULL;
    MsgStreamStartFsm *startFsm = NULL;
    MsgStreamState *msgStreamState = MsgStreamState::get();
    msgStreamState->incStreamStart();
    XidMgr *xidMgr = XidMgr::get();
    Xid streamId = xidMgr->xidGetNext();
    Config *config = Config::get();

    // Track stream related information
    MsgStreamInfo *msgStreamInfo =
        new (std::nothrow) MsgStreamInfo(streamId,
                                         MsgStreamMgr::StreamState::StreamInit,
                                         twoPcNodeProperty,
                                         config->getMyNodeId(),
                                         dstNodeId,
                                         streamObject,
                                         objectContext);
    BailIfNull(msgStreamInfo);

    (*retStreamId) = msgStreamInfo->getStreamId();

    status = msgStreamState->streamHashInsert(msgStreamInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }
    insertHt = true;

    assert(msgStreamInfo->streamState_ == StreamState::StreamInit);
    msgStreamInfo->streamState_ = StreamState::StreamSetup;

    // Start establishment of stream from mynode to destination.
    startFsm = new (std::nothrow) MsgStreamStartFsm(NULL, msgStreamInfo);
    BailIfNull(startFsm);

    startState = new (std::nothrow) StateMsgStreamStart(startFsm);
    BailIfNull(startState);

    verify(startFsm->setNextState(startState) == NULL);
    startState = NULL;
    msgStreamInfo->streamStartFsm_ = startFsm;

    scheduleFsm = true;
    startFsm->run();  // Synchronous

    status = startFsm->getStatus();
    assert(startFsm->getCurState() == NULL);
    delete startFsm;
    startFsm = NULL;
    msgStreamInfo->streamStartFsm_ = NULL;

    if (status != StatusOk) {
        goto CommonExit;
    }

    // Stream established successfully.
    msgStreamInfo->streamState_ = StreamState::StreamEstablished;

CommonExit:
    if (status != StatusOk) {
        msgStreamState->incStreamStartError();

        if (scheduleFsm == false) {
            if (startFsm != NULL) {
                delete startFsm;
                startFsm = NULL;
            }

            if (startState != NULL) {
                delete startState;
                startState = NULL;
            }
        }

        if (insertHt == true) {
            MsgStreamInfo *tmp =
                msgStreamState->streamHashRemove(msgStreamInfo->getStreamId());
            assert(msgStreamInfo == tmp);  // Should not happen
        }

        if (msgStreamInfo != NULL) {
            delete msgStreamInfo;
            msgStreamInfo = NULL;
        }
    }

    return status;
}

void
MsgStreamMgr::endStream(Xid streamId)
{
    MsgStreamInfo *msgStreamInfo, *tmp;
    Status status;
    StateMsgStreamEnd *endState = NULL;
    MsgStreamEndFsm *endFsm = NULL;
    bool scheduleFsm = false;
    MsgStreamState *msgStreamState = MsgStreamState::get();
    msgStreamState->incStreamEnd();

    msgStreamInfo = msgStreamState->streamHashFind(streamId);
    if (msgStreamInfo == NULL) {
        status = StatusMsgStreamNotFound;
        goto CommonExit;
    }

    assert(msgStreamInfo->streamState_ == StreamState::StreamEstablished ||
           msgStreamInfo->streamState_ == StreamState::StreamInProgress ||
           msgStreamInfo->streamState_ == StreamState::StreamIsDone);
    msgStreamInfo->streamState_ = StreamState::StreamTeardown;

    endFsm = new (std::nothrow) MsgStreamEndFsm(NULL, msgStreamInfo);
    BailIfNull(endFsm);

    endState = new (std::nothrow) StateMsgStreamEnd(endFsm);
    BailIfNull(endState);

    verify(endFsm->setNextState(endState) == NULL);
    endState = NULL;
    msgStreamInfo->streamEndFsm_ = endFsm;

    scheduleFsm = true;
    endFsm->run();  // Synchronous

    status = endFsm->getStatus();
    assert(endFsm->getCurState() == NULL);
    delete endFsm;
    endFsm = NULL;
    msgStreamInfo->streamEndFsm_ = NULL;

    if (status != StatusOk) {
        goto CommonExit;
    }

    tmp = msgStreamState->streamHashRemove(msgStreamInfo->getStreamId());
    assert(msgStreamInfo == tmp);
    delete msgStreamInfo;
    msgStreamInfo = NULL;

CommonExit:
    if (status != StatusOk) {
        msgStreamState->incStreamEndError();

        if (scheduleFsm == false) {
            if (endState != NULL) {
                delete endState;
                endState = NULL;
            }

            if (endFsm != NULL) {
                delete endFsm;
                endFsm = NULL;
            }
        }
    }
}

Status
MsgStreamMgr::sendStream(Xid streamId,
                         ProtocolDataUnit *pdu,
                         StreamState streamState,
                         void *sendContext)
{
    Status status;
    MsgStreamInfo *msgStreamInfo;
    StateMsgStreamSend *sendState = NULL;
    MsgStreamSendFsm *sendFsm = NULL;
    bool scheduleFsm = false;
    MsgStreamState *msgStreamState = MsgStreamState::get();
    msgStreamState->incStreamSendPayload();

    msgStreamInfo = msgStreamState->streamHashFind(streamId);
    if (msgStreamInfo == NULL) {
        status = StatusMsgStreamNotFound;
        goto CommonExit;
    }

    if (streamState == StreamState::StreamIsDone) {
        pdu->streamState_ = StreamState::StreamIsDone;
    }

    msgStreamInfo->stateLock_.lock();
    assert(msgStreamInfo->streamState_ == StreamState::StreamEstablished ||
           msgStreamInfo->streamState_ == StreamState::StreamInProgress);
    assert(pdu->streamState_ == StreamState::StreamInProgress ||
           pdu->streamState_ == StreamState::StreamIsDone);
    msgStreamInfo->streamState_ = pdu->streamState_;
    msgStreamInfo->stateLock_.unlock();

    sendFsm = new (std::nothrow)
        MsgStreamSendFsm(NULL, msgStreamInfo, pdu, sendContext);
    BailIfNull(sendFsm);

    sendState = new (std::nothrow) StateMsgStreamSend(sendFsm);
    BailIfNull(sendState);

    verify(sendFsm->setNextState(sendState) == NULL);
    sendState = NULL;

    sendFsm->run();  // Send inline.
    sendFsm->done();
    scheduleFsm = true;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        msgStreamState->incStreamSendPayloadError();

        if (scheduleFsm == false) {
            if (sendState != NULL) {
                delete sendState;
                sendState = NULL;
            }

            if (sendFsm != NULL) {
                delete sendFsm;
                sendFsm = NULL;
            }
        }
    }
    return status;
}

Status
MsgStreamMgr::initPayloadToSend(Xid streamId,
                                void *payload,
                                uint32_t payloadLen)
{
    Status status;
    MsgStreamInfo *msgStreamInfo;
    MsgStreamState *msgStreamState = MsgStreamState::get();
    ProtocolDataUnit *pdu = (ProtocolDataUnit *) payload;

    msgStreamInfo = msgStreamState->streamHashFind(streamId);
    if (msgStreamInfo == NULL) {
        status = StatusMsgStreamNotFound;
        goto CommonExit;
    }

    if (payloadLen < sizeof(ProtocolDataUnit)) {
        status = StatusUnderflow;
        goto CommonExit;
    }

    if (payloadLen > MsgMgr::getMsgMaxPayloadSize()) {
        status = StatusOverflow;
        goto CommonExit;
    }

    pdu->streamId_ = msgStreamInfo->streamId_;
    pdu->srcNodeId_ = msgStreamInfo->srcNodeId_;
    pdu->dstNodeId_ = msgStreamInfo->dstNodeId_;
    pdu->bufferSize_ = payloadLen;
    pdu->twoPcNodeProperty_ = msgStreamInfo->twoPcNodeProperty_;
    pdu->streamState_ = StreamState::StreamInProgress;
    pdu->streamObject_ = msgStreamInfo->streamObject_;

    status = StatusOk;

CommonExit:
    return status;
}
