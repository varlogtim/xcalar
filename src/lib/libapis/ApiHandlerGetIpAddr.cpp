// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetIpAddr.h"
#include "msg/MessageTypes.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerGetIpAddr::ApiHandlerGetIpAddr(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetIpAddr::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunInline);
}

Status
ApiHandlerGetIpAddr::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    char *ipAddr;
    size_t strcpyret;
    Config *config = Config::get();

    // NOTE: if you're about to add something that can block here
    // please switch the flag from NeedsToRunInline to something else

    outputSize = XcalarApiSizeOfOutput(XcalarApiGetIpAddrOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    ipAddr = config->getIpAddr(input_->nodeId);

    strcpyret = strlcpy(output->outputResult.getIpAddrOutput.ipAddr,
                        ipAddr,
                        sizeof(output->outputResult.getIpAddrOutput.ipAddr));

    if (strcpyret >= sizeof(output->outputResult.getIpAddrOutput.ipAddr)) {
        status = StatusIpAddrTooLong;
    } else {
        status = StatusOk;
    }
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        outputSize = 0;
    }
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetIpAddr::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    NodeId nodeId;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->getIpAddrInput;
    nodeId = input_->nodeId;

    Config *config = Config::get();

    if (nodeId >= config->getActiveNodes()) {
        xSyslog(moduleName, XlogErr, "Invalid Node Id %d", nodeId);
        status = StatusInvalNodeId;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
