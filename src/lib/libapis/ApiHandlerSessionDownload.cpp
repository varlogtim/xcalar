// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionDownload.h"
#include "usr/Users.h"

ApiHandlerSessionDownload::ApiHandlerSessionDownload(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSessionDownload::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionDownload::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status;

    status =
        UserMgr::get()->download(userId_, input_, outputOut, outputSizeOut);

    return status;
}

Status
ApiHandlerSessionDownload::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->sessionDownloadInput;

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
                "Session name length too long (%lu chars). Max is %lu chars",
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

CommonExit:

    return status;
}
