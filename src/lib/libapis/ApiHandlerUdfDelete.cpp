// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUdfDelete.h"
#include "msg/MessageTypes.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "udf/UserDefinedFunction.h"
#include "libapis/LibApisCommon.h"
#include "dag/DagLib.h"

ApiHandlerUdfDelete::ApiHandlerUdfDelete(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerUdfDelete::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately | MayNeedSessionOrGraph);
}

Status
ApiHandlerUdfDelete::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusUnknown;
    XcalarApiUdfContainer *udfContainer = NULL;
    XcalarApiUdfContainer myudfContainer;

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

    if (dstGraph_ == NULL) {
        status = UserDefinedFunction::initUdfContainer(&myudfContainer,
                                                       NULL,
                                                       NULL,
                                                       NULL);
        BailIfFailed(status);
        udfContainer = &myudfContainer;
    } else {
        udfContainer = dstGraph_->getUdfContainer();
    }

    status = UserDefinedFunction::get()->deleteUdf(input_, udfContainer);
CommonExit:

    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerUdfDelete::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->udfDeleteInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->udfDeleteInput;

    status = StatusOk;
    return status;
}
