// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerDeleteDht.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "operators/Dht.h"
#include "libapis/LibApisCommon.h"

ApiHandlerDeleteDht::ApiHandlerDeleteDht(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerDeleteDht::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerDeleteDht::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    // By default, we will a delete the DHT
    status = DhtMgr::get()->dhtDelete(input_->dhtName);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerDeleteDht::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->deleteDhtInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->deleteDhtInput;

    if (input_->dhtNameLen >= ArrayLen(input_->dhtName)) {
        xSyslog(moduleName,
                XlogErr,
                "dhtNameLen %lu chars is too long. Max is %lu chars",
                input_->dhtNameLen,
                ArrayLen(input_->dhtName) - 1);
        status = StatusNoBufs;
        goto CommonExit;
    }

    // Do not allow the system DHTs to be deleted
    if (strcmp(input_->dhtName, DhtMgr::DhtSystemAscendingDht) == 0 ||
        strcmp(input_->dhtName, DhtMgr::DhtSystemDescendingDht) == 0 ||
        strcmp(input_->dhtName, DhtMgr::DhtSystemUnorderedDht) == 0 ||
        strcmp(input_->dhtName, DhtMgr::DhtSystemBroadcastDht) == 0 ||
        strcmp(input_->dhtName, DhtMgr::DhtSystemRandomDht) == 0) {
        xSyslog(moduleName,
                XlogErr,
                "Rejected attempt to delete system DHT: %s",
                input_->dhtName);
        status = StatusDhtProtected;
        goto CommonExit;
    }

    status = DhtMgr::get()->isValidDhtName(input_->dhtName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Dht name \"%s\" is not valid: %s",
                input_->dhtName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
