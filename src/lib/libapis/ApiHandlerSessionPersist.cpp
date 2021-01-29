// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionPersist.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "usr/Users.h"

ApiHandlerSessionPersist::ApiHandlerSessionPersist(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSessionPersist::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionPersist::run(XcalarApiOutput **output, size_t *outputSize)
{
    return UserMgr::get()->persist(userId_, input_, output, outputSize);
}

Status
ApiHandlerSessionPersist::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    // Not a typo - Persist also uses the session delete struct
    assert((uintptr_t) input == (uintptr_t) &input->sessionDeleteInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->sessionDeleteInput;

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

    if (input_->sessionNameLength >= ArrayLen(input_->sessionName)) {
        xSyslog(moduleName,
                XlogErr,
                "Session name length is too long (%lu chars). Max is %lu chars",
                input_->sessionNameLength,
                ArrayLen(input_->sessionName) - 1);
        status = StatusInval;
        goto CommonExit;
    }

    if (userId_ == NULL) {
        xSyslog(moduleName, XlogErr, "UserId cannot be NULL!");
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
