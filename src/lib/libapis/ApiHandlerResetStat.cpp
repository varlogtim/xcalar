// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerResetStat.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "libapis/LibApisCommon.h"

ApiHandlerResetStat::ApiHandlerResetStat(XcalarApis api)
    : ApiHandler(api), nodeId_(0)
{
}

ApiHandler::Flags
ApiHandlerResetStat::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerResetStat::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    Config *config = Config::get();
    MsgMgr *msgMgr = MsgMgr::get();

    if (nodeId_ >= config->getActiveNodes()) {
        status = StatusNoSuchNode;
        goto CommonExit;
    }

    ResetStatsInput resetInput;
    resetInput.resetCumulativeStats = true;
    resetInput.resetHwmStats = true;

    msgMgr->twoPcEphemeralInit(&eph,
                               &resetInput,
                               sizeof(resetInput),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcResetStat1,
                               NULL,
                               (TwoPcBufLife)(TwoPcMemCopyInput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcResetStat,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           nodeId_,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

CommonExit:

    *outputOut = NULL;
    *outputSizeOut = 0;

    return status;
}

Status
ApiHandlerResetStat::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    Config *config = Config::get();

    apiInput_ = input;
    inputSize_ = inputSize;
    nodeId_ = input->statInput.nodeId;

    if (nodeId_ >= config->getActiveNodes()) {
        xSyslog(moduleName, XlogErr, "No such nodeId %ld", nodeId_);
        status = StatusNoSuchNode;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
