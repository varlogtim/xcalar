// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "msgstream/MsgStream.h"
#include "MsgStreamState.h"
#include "MsgStreamSendFsm.h"
#include "MsgStreamStartFsm.h"
#include "MsgStreamEndFsm.h"

static constexpr const char *moduleName = "LibMsgStream";

void
Msg2pcStreamActionImpl::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    MsgStreamMgr::ProtocolDataUnit *pdu =
        (MsgStreamMgr::ProtocolDataUnit *) payload;
    MsgStreamState *msgStreamState = MsgStreamState::get();

    switch (pdu->streamState_) {
    case MsgStreamMgr::StreamState::StreamSetup:
        status = msgStreamState->startStreamHandlerLocal(pdu);
        break;

    case MsgStreamMgr::StreamState::StreamInProgress:  // Pass through
    case MsgStreamMgr::StreamState::StreamIsDone:
        status = msgStreamState->streamReceiveHandlerLocal(pdu);
        break;

    case MsgStreamMgr::StreamState::StreamTeardown:
        status = msgStreamState->endStreamHandlerLocal(pdu);
        break;

    default:
        status = StatusUnimpl;
        assert(0);
        break;
    }

    eph->status = status;
}

void
Msg2pcStreamActionImpl::schedLocalCompletion(MsgEphemeral *eph, void *payload)
{
    // NOOP for now.
}

void
Msg2pcStreamActionImpl::recvDataCompletion(MsgEphemeral *eph, void *payload)
{
    MsgStreamState *msgStreamState = MsgStreamState::get();
    if (dynamic_cast<MsgStreamStartFsm *>((SchedulableFsm *) eph->ephemeral)) {
        msgStreamState->startStreamCompletionHandler(eph);
    } else if (dynamic_cast<MsgStreamEndFsm *>(
                   (SchedulableFsm *) eph->ephemeral)) {
        msgStreamState->endStreamCompletionHandler(eph);
    } else if (dynamic_cast<MsgStreamSendFsm *>(
                   (SchedulableFsm *) eph->ephemeral)) {
        msgStreamState->streamSendCompletionHandler(eph);
    } else {
        assert(0 && "Invalid Stream FSM");
    }
}
