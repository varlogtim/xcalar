// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSupportGenerate.h"
#include "msg/MessageTypes.h"
#include "support/SupportBundle.h"
#include "libapis/LibApisCommon.h"

ApiHandlerSupportGenerate::ApiHandlerSupportGenerate(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSupportGenerate::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSupportGenerate::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = StatusOk;

    status = SupportBundle::get()->supportDispatch(input_->generateMiniBundle,
                                                   input_->supportCaseId,
                                                   output,
                                                   outputSize);

    return status;
}

Status
ApiHandlerSupportGenerate::setArg(XcalarApiInput *input, size_t inputSize)
{
    assert((uintptr_t) input == (uintptr_t) &input->supportGenerateInput);

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->supportGenerateInput;

    return StatusOk;
}
