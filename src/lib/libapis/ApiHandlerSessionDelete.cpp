// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerSessionDelete.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "usr/Users.h"

ApiHandlerSessionDelete::ApiHandlerSessionDelete(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerSessionDelete::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerSessionDelete::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
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

    status = userMgr->doDelete(userId_, input_, sessionGenericOutput);
    if (status != StatusOk) {
        if (status == StatusSessionUsrAlreadyExists) {
            Status tmpStatus =
                userMgr->getOwnerNodeIdAndFillIn(userId_->userIdName,
                                                 &output->outputResult
                                                      .sessionGenericOutput);
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
ApiHandlerSessionDelete::setArg(XcalarApiInput *input, size_t inputSize)
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

    status = StatusOk;
CommonExit:
    return status;
}
