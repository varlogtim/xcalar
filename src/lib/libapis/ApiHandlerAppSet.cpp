// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerAppSet.h"
#include "app/AppMgr.h"
#include "libapis/LibApisCommon.h"

ApiHandlerAppSet::ApiHandlerAppSet(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerAppSet::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerAppSet::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = AppMgr::get()->updateApp(input_->name,
                                             input_->hostType,
                                             input_->flags,
                                             input_->exec,
                                             input_->execSize);
    if (status == StatusAppNotFound) {
        return AppMgr::get()->createApp(input_->name,
                                        input_->hostType,
                                        input_->flags,
                                        input_->exec,
                                        input_->execSize);
    }
    return status;
}

Status
ApiHandlerAppSet::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->appSetInput;
    return StatusOk;
}
