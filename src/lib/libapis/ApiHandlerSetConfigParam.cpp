// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSetConfigParam.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "libapis/LibApisCommon.h"

ApiHandlerSetConfigParam::ApiHandlerSetConfigParam(XcalarApis api)
    : ApiHandler(api), input_(NULL), paramName_(NULL), paramValue_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSetConfigParam::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSetConfigParam::run(XcalarApiOutput **outputOut,
                              size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Config *cfg = Config::get();

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
    memZero(output, outputSize);

    status = cfg->setConfigParam(paramName_, paramValue_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Setting config parameter %s to %s failed: %s",
                paramName_,
                paramValue_,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerSetConfigParam::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    Config *cfg = Config::get();

    assert((uintptr_t) input == (uintptr_t) &input->setConfigParamInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->setConfigParamInput;

    if (cfg->getMyNodeId() != Config::ConfigApiNode) {
        // Limit setting config parameters to one node as the config
        // infrastructure cannot handle concurrent updates from different
        // nodes.
        xSyslog(moduleName,
                XlogErr,
                "Setting config parameters must be processed by node '%d'",
                Config::ConfigApiNode);
        status = StatusInval;
        goto CommonExit;
    }

    paramName_ = input_->paramName;
    paramValue_ = input_->paramValue;

CommonExit:

    return status;
}
