// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetStatGroupIdMap.h"
#include "msg/MessageTypes.h"
#include "msg/Message.h"
#include "config/Config.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "libapis/LibApisCommon.h"

ApiHandlerGetStatGroupIdMap::ApiHandlerGetStatGroupIdMap(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerGetStatGroupIdMap::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetStatGroupIdMap::run(XcalarApiOutput **outputOut,
                                 size_t *outputSizeOut)
{
    return StatsLib::get()->getStatsGroupIdMapReq(input_->nodeId,
                                                  outputOut,
                                                  outputSizeOut);
}

Status
ApiHandlerGetStatGroupIdMap::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    Config *config = Config::get();

    assert((uintptr_t) input == (uintptr_t) &input->statInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->statInput;

    if (inputSize != sizeof(*input_)) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->nodeId >= config->getActiveNodes()) {
        status = StatusNoSuchNode;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
