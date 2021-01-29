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
#include "libapis/ApiHandlerSessionUpload.h"
#include "usr/Users.h"

ApiHandlerSessionUpload::ApiHandlerSessionUpload(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerSessionUpload::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionUpload::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status;
    XcalarApiSessionNewOutput *sessionNewOutput = NULL;
    size_t outputSize = 0;
    XcalarApiOutput *output = NULL;
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

    status = userMgr->upload(userId_, input_, sessionNewOutput);

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
ApiHandlerSessionUpload::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->sessionUploadInput;
    size_t expectedInputSize = sizeof(*input_) + input_->sessionContentCount;

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize (%lu bytes), sessionContentCount "
                "(%lu bytes)",
                inputSize,
                expectedInputSize,
                input_->sessionContentCount);
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
