// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetStat.h"
#include "msg/MessageTypes.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "libapis/LibApisCommon.h"
#include "msgstream/MsgStream.h"
#include "util/MemTrack.h"

ApiHandlerGetStat::ApiHandlerGetStat(XcalarApis api) : ApiHandler(api) {}

ApiHandler::Flags
ApiHandlerGetStat::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetStat::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    *outputOut = NULL;
    *outputSizeOut = 0;

    return StatsLib::get()->getStatsReq(apiInput_->statInput.nodeId,
                                        outputOut,
                                        outputSizeOut);
}

Status
ApiHandlerGetStat::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status;
    Config *config = Config::get();

    apiInput_ = input;
    inputSize_ = inputSize;

    if (input->statInput.nodeId >= config->getActiveNodes()) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid nodeId %ld",
                input->statInput.nodeId);
        status = StatusNoSuchNode;
        goto CommonExit;
    }
CommonExit:
    return status;
}
