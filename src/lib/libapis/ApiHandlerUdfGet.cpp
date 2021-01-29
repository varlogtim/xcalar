// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUdfGet.h"
#include "msg/MessageTypes.h"
#include "udf/UserDefinedFunction.h"
#include "libapis/LibApisCommon.h"

ApiHandlerUdfGet::ApiHandlerUdfGet(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerUdfGet::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerUdfGet::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = StatusUnknown;
    XcalarApiUdfContainer *udfContainer = NULL;

    udfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);
    BailIfNull(udfContainer);
    status = UserDefinedFunction::initUdfContainer(udfContainer,
                                                   userId_,
                                                   sessionInfo_,
                                                   NULL);
    BailIfFailed(status);

    status = UserDefinedFunction::get()->getUdf(input_,
                                                udfContainer,
                                                output,
                                                outputSize);

CommonExit:
    if (udfContainer != NULL) {
        memFree(udfContainer);
        udfContainer = NULL;
    }
    return status;
}

Status
ApiHandlerUdfGet::setArg(XcalarApiInput *input, size_t inputSize)
{
    assert((uintptr_t) input == (uintptr_t) &input->udfGetInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->udfGetInput;
    return StatusOk;
}
