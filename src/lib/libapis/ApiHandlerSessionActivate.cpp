// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionActivate.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "usr/Users.h"

ApiHandlerSessionActivate::ApiHandlerSessionActivate(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerSessionActivate::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionActivate::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiSessionGenericOutput *sessionGenericOutput = NULL;
    size_t outputSize = 0;
    UserMgr *userMgr = UserMgr::get();

    outputSize = XcalarApiSizeOfOutput(*sessionGenericOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output. "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    sessionGenericOutput = &output->outputResult.sessionGenericOutput;

    status = userMgr->activate(userId_, input_, sessionGenericOutput);
    if (status != StatusOk) {
        if (status == StatusSessionUsrAlreadyExists) {
            Status tmpStatus =
                userMgr->getOwnerNodeIdAndFillIn(userId_->userIdName,
                                                 sessionGenericOutput);
            if (tmpStatus != StatusOk) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Failure to propagate node info to XD %s",
                              strGetFromStatus(tmpStatus));
            }
        }

        goto CommonExit;
    }

CommonExit:

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
ApiHandlerSessionActivate::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->sessionActivateInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->sessionActivateInput;

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
                "sessionNameLength (%lu chars) is too long. Max is %lu chars",
                input_->sessionNameLength,
                ArrayLen(input_->sessionName) - 1);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:

    return status;
}
