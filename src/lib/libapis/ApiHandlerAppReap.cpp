// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "StrlFunc.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerAppReap.h"
#include "app/AppMgr.h"
#include "app/AppGroup.h"
#include "util/MemTrack.h"
#include "libapis/LibApisCommon.h"
#include "sys/XLog.h"

ApiHandlerAppReap::ApiHandlerAppReap(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerAppReap::getFlags()
{
    return (Flags)(NeedsAck);
}

Status
ApiHandlerAppReap::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    size_t outStrSize = 0;
    size_t errStrSize = 0;
    char *outStr = NULL;
    char *errStr = NULL;
    XcalarApiAppReapOutput *reapOutput = NULL;
    size_t outputSize = 0;
    Status status;
    XcalarApiOutput *output = NULL;
    bool appInternalError = false;
    // A timeout of 0 means that the call will never timeout.
    // Make App reap calls return immediately if app still running,
    // so caller can retry/cancel for StatusAppInProgress
    static constexpr const uint64_t AppWaitTimeoutUsecs = 1;

    if (input_->cancel) {
        status = AppMgr::get()->abortMyAppRun(input_->appGroupId,
                                              StatusCanceled,
                                              &appInternalError);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App Abort %lu failed on abortMyAppRun: %s",
                    input_->appGroupId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    status = AppMgr::get()->waitForMyAppResult(input_->appGroupId,
                                               AppWaitTimeoutUsecs,
                                               &outStr,
                                               &errStr,
                                               &appInternalError);

    if (input_->cancel &&
        (status == StatusCanceled || status == StatusTimedOut)) {
        status = StatusOk;
    } else if (!input_->cancel && status == StatusTimedOut) {
        status = StatusAppInProgress;
    }

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Reap %lu failed on waitForMyAppResult: %s",
                input_->appGroupId,
                strGetFromStatus(status));
    }

    if (outStr != NULL) {
        outStrSize = strlen(outStr) + 1;
    }
    if (errStr != NULL) {
        errStrSize = strlen(errStr) + 1;
    }

    outputSize =
        XcalarApiSizeOfOutput(typeof(*reapOutput)) + outStrSize + errStrSize;
    output = (XcalarApiOutput *) memAlloc(outputSize);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Reap %lu failed: %s",
                input_->appGroupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    memZero(output, outputSize);
    reapOutput = &output->outputResult.appReapOutput;
    reapOutput->outStrSize = outStrSize;
    reapOutput->errStrSize = errStrSize;

    if (outStrSize > 0) {
        strlcpy(reapOutput->str, outStr, outStrSize);
    }
    if (errStrSize > 0) {
        strlcpy(reapOutput->str + outStrSize, errStr, errStrSize);
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

CommonExit:
    if (outStr != NULL) {
        memFree(outStr);
    }
    if (errStr != NULL) {
        memFree(errStr);
    }
    return status;
}

Status
ApiHandlerAppReap::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->appReapInput;
    return StatusOk;
}
