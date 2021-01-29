// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerPreview.h"
#include "msg/MessageTypes.h"
#include "dataset/AppLoader.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerPreview::ApiHandlerPreview(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerPreview::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerPreview::run(XcalarApiOutput **output, size_t *outputSize)
{
    return AppLoader::preview(input_, userId_, output, outputSize);
}

Status
ApiHandlerPreview::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->previewInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->previewInput;

    if (sizeof(*input_) + input_->inputLen != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) + inputLen = %lu + %lu = %lu bytes",
                inputSize,
                sizeof(*input_),
                input_->inputLen,
                sizeof(*input) + input_->inputLen);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
