// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetNumNodes.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "libapis/LibApisCommon.h"

ApiHandlerGetNumNodes::ApiHandlerGetNumNodes(XcalarApis api) : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerGetNumNodes::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunInline);
}

Status
ApiHandlerGetNumNodes::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusOk;
    Config *config = Config::get();

    // NOTE: if you're about to add something that can block here
    // please switch the flag from NeedsToRunInline to something else

    outputSize = XcalarApiSizeOfOutput(XcalarApiGetNumNodesOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName, XlogErr, "Insufficient memory to allocate output");
        outputSize = 0;
        status = StatusNoMem;
        goto CommonExit;
    }

    output->outputResult.getNumNodesOutput.numNodes = config->getActiveNodes();

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetNumNodes::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    return StatusOk;
}
