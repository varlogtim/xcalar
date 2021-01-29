// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListParametersInRetina.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"

ApiHandlerListParametersInRetina::ApiHandlerListParametersInRetina(
    XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListParametersInRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListParametersInRetina::run(XcalarApiOutput **output,
                                      size_t *outputSize)
{
    return DagLib::get()->listParametersInRetina(input_, output, outputSize);
}

Status
ApiHandlerListParametersInRetina::setArg(XcalarApiInput *input,
                                         size_t inputSize)
{
    Status status = StatusUnknown;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->listParametersInRetinaInput.listRetInput;

    status = DagLib::get()->isValidRetinaName(input_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "batch dataflow name \"%s\" is not valid: %s",
                input_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
