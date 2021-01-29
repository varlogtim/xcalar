// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionList.h"
#include "msg/MessageTypes.h"
#include "usr/Users.h"
#include "sys/XLog.h"

ApiHandlerSessionList::ApiHandlerSessionList(XcalarApis api) : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerSessionList::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionList::run(XcalarApiOutput **output, size_t *outputSize)
{
    UserMgr *userMgr = UserMgr::get();
    Status status;
    status = userMgr->list(userId_, input_->pattern, output, outputSize);
    return status;
}

Status
ApiHandlerSessionList::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->listSessionInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listSessionInput;

    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes)",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    if (userId_ == NULL) {
        xSyslog(moduleName, XlogErr, "userId cannot be NULL");
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->patternLength > ArrayLen(input_->pattern)) {
        xSyslog(moduleName,
                XlogErr,
                "patternLength (%lu) is too long. Max is %lu)",
                input_->patternLength,
                ArrayLen(input_->pattern));
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
