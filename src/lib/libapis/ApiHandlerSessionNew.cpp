// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionNew.h"
#include "msg/MessageTypes.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "usr/Users.h"

ApiHandlerSessionNew::ApiHandlerSessionNew(XcalarApis api) : ApiHandler(api) {}

ApiHandler::Flags
ApiHandlerSessionNew::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionNew::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    size_t outputSize = 0;
    XcalarApiOutput *output = NULL;
    XcalarApiSessionNewOutput *sessionNewOutput = NULL;
    UserMgr *userMgr = UserMgr::get();

    outputSize = XcalarApiSizeOfOutput(*sessionNewOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        outputSize = 0;
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output. "
                "(size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    sessionNewOutput = &output->outputResult.sessionNewOutput;
    memZero(sessionNewOutput, sizeof(*sessionNewOutput));

    status = userMgr->create(userId_, input_, sessionNewOutput);
    if (status == StatusOk) {
        assert(sessionNewOutput->sessionId != 0);
    }

    if (status == StatusSessionUsrAlreadyExists) {
        Status tmpStatus =
            userMgr->getOwnerNodeIdAndFillIn(userId_->userIdName,
                                             &sessionNewOutput
                                                  ->sessionGenericOutput);
        if (tmpStatus != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failure to propagate node info to XD %s",
                          strGetFromStatus(tmpStatus));
        }
    }

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerSessionNew::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->sessionNewInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->sessionNewInput;

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
                "sessionNameLength (%lu) too long. Max is %lu chars",
                input_->sessionNameLength,
                ArrayLen(input_->sessionName) - 1);
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->sessionNameLength == 0 || input_->sessionName[0] == ' ' ||
        input_->sessionName[input_->sessionNameLength - 1] == ' ') {
        xSyslog(moduleName,
                XlogErr,
                "Session name must be specified and cannot have leading or "
                "trailing spaces");
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->forkedSessionNameLength >=
        ArrayLen(input_->forkedSessionName)) {
        xSyslog(moduleName,
                XlogErr,
                "forkedSessionNameLength (%lu) too long. Max is %lu chars",
                input_->forkedSessionNameLength,
                ArrayLen(input_->forkedSessionName) - 1);
        status = StatusInval;
        goto CommonExit;
    }

    if (userId_ == NULL) {
        xSyslog(moduleName, XlogErr, "userId cannot be NULL");
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
