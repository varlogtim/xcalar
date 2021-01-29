// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerCreateDht.h"
#include "msg/MessageTypes.h"
#include "operators/Dht.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "libapis/LibApisCommon.h"

ApiHandlerCreateDht::ApiHandlerCreateDht(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerCreateDht::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerCreateDht::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusUnknown;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        outputSize = 0;
        status = StatusNoMem;
        goto CommonExit;
    }

    // By default, we are persisting this DHT
    status = DhtMgr::get()->dhtCreate(input_->dhtName,
                                      input_->dhtArgs.lowerBound,
                                      input_->dhtArgs.upperBound,
                                      input_->dhtArgs.ordering,
                                      DoNotBroadcast,
                                      DoNotAutoRemove,
                                      NULL);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerCreateDht::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->createDhtInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->createDhtInput;

    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->dhtName[0] == '\0') {
        xSyslog(moduleName, XlogErr, "Dht name cannot be empty");
        status = StatusDhtEmptyDhtName;
        goto CommonExit;
    }

    if (input_->dhtArgs.upperBound < input_->dhtArgs.lowerBound) {
        xSyslog(moduleName,
                XlogErr,
                "lowerBound %lf > upperBound %lf",
                input_->dhtArgs.lowerBound,
                input_->dhtArgs.upperBound);
        status = StatusDhtUpperBoundLessThanLowerBound;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
