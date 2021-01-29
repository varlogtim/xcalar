// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListRetinas.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"

ApiHandlerListRetinas::ApiHandlerListRetinas(XcalarApis api) : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerListRetinas::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListRetinas::run(XcalarApiOutput **output, size_t *outputSize)
{
    return DagLib::get()->listRetinas(apiInput_->listRetinasInput.namePattern,
                                      output,
                                      outputSize);
}

Status
ApiHandlerListRetinas::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    return StatusOk;
}
