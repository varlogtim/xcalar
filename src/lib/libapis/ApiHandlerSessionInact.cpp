// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionInact.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "usr/Users.h"
#include "util/MemTrack.h"

ApiHandlerSessionInact::ApiHandlerSessionInact(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSessionInact::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionInact::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;
    UserMgr *userMgr = UserMgr::get();
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(*sessionGenericOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Size required: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    sessionGenericOutput = &output->outputResult.sessionGenericOutput;
    memZero(sessionGenericOutput, sizeof(*sessionGenericOutput));

    status = userMgr->inactivate(userId_, input_, sessionGenericOutput);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerSessionInact::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

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
                "sessionNameLength too long (%lu chars). Max is %lu chars",
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

    if (input_->noCleanup) {
        xSyslog(moduleName, XlogErr, "noCleanup must be 'false'");
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
