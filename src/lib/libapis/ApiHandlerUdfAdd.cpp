// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUdfAdd.h"
#include "msg/MessageTypes.h"
#include "udf/UserDefinedFunction.h"
#include "udf/UdfTypes.h"
#include "dag/Dag.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerUdfAdd::ApiHandlerUdfAdd(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerUdfAdd::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately | MayNeedSessionOrGraph);
}

Status
ApiHandlerUdfAdd::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;
    XcalarApiUdfContainer *udfContainer = NULL;
    XcalarApiUdfContainer myudfContainer;

    if (dstGraph_ == NULL) {
        // dstGraph_ being NULL implies shared UDFs so init the udf container
        // to contain empty user/session name
        status = UserDefinedFunction::initUdfContainer(&myudfContainer,
                                                       NULL,
                                                       NULL,
                                                       NULL);
        BailIfFailed(status);
        udfContainer = &myudfContainer;
    } else {
        udfContainer = dstGraph_->getUdfContainer();
    }
    status = UserDefinedFunction::get()->addUdf(input_,
                                                udfContainer,
                                                output,
                                                outputSize);
CommonExit:
    return status;
}

Status
ApiHandlerUdfAdd::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize = 0;

    assert((uintptr_t) input == (uintptr_t) &input->udfAddUpdateInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->udfAddUpdateInput;
    expectedInputSize = sizeof(*input_) + input_->sourceSize;

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes, sourceSize: %lu bytes",
                inputSize,
                expectedInputSize,
                input_->sourceSize);
        status = StatusInval;
        goto CommonExit;
    }

    if (!isValidUdfType(input_->type)) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid udf type %u specified",
                input_->type);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
