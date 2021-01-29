// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerPerNodeTop.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "libapis/LibApisCommon.h"

ApiHandlerPerNodeTop::ApiHandlerPerNodeTop(XcalarApis api) : ApiHandler(api) {}

ApiHandler::Flags
ApiHandlerPerNodeTop::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerPerNodeTop::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusOk;

    XcalarApiOutput *output = NULL;
    size_t outputSize;

    outputSize = XcalarApiSizeOfOutput(XcalarApiTopOutput) +
                 sizeof(XcalarApiTopOutputPerNode);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatsLib::get()->getTopStats(input_,
                                          &(output->outputResult.topOutput
                                                .topOutputPerNode[0]),
                                          true);
    BailIfFailed(status);

    output->outputResult.topOutput.status = StatusOk.code();
    output->outputResult.topOutput.numNodes = 1;

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        outputSize = 0;

        xSyslog(moduleName,
                XlogErr,
                "%s() Failure: %s",
                __func__,
                strGetFromStatus(status));
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
ApiHandlerPerNodeTop::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->topInput;
    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes)",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}
