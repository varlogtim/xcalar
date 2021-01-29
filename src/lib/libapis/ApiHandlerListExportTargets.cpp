// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListExportTargets.h"
#include "msg/MessageTypes.h"
#include "export/DataTarget.h"
#include "sys/XLog.h"

ApiHandlerListExportTargets::ApiHandlerListExportTargets(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListExportTargets::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListExportTargets::run(XcalarApiOutput **output, size_t *outputSize)
{
    return DataTargetManager::getRef().listTargets(input_->targetTypePattern,
                                                   input_->targetNamePattern,
                                                   output,
                                                   outputSize);
}

Status
ApiHandlerListExportTargets::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->listTargetsInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listTargetsInput;

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

    status = StatusOk;
CommonExit:
    return status;
}
