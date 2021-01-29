// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerTarget.h"
#include "msg/MessageTypes.h"
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerTarget::ApiHandlerTarget(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerTarget::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerTarget::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;
    *output = NULL;
    XcalarApiTargetOutput *targetOutput;
    const char *targetAppName = AppMgr::TargetAppName;
    App *targetApp = NULL;
    char *appOutBlob = NULL;
    char *appOutput = NULL;
    int outStrLen;
    char *errorStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    const char *userName = "";

    targetApp = AppMgr::get()->openAppHandle(targetAppName, &handle);
    if (targetApp == NULL) {
        status = StatusNoEnt;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Target app %s does not exist",
                      targetAppName);
        goto CommonExit;
    }

    if (userId_ != NULL) {
        userName = userId_->userIdName;
    }

    status = AppMgr::get()->runMyApp(targetApp,
                                     AppGroup::Scope::Local,
                                     userName,
                                     sessionInfo_ ? sessionInfo_->sessionId : 0,
                                     input_->inputJson,
                                     0,
                                     &appOutBlob,
                                     &errorStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Target app %s failed with output:%s, error:%s",
                      targetAppName,
                      appOutBlob,
                      errorStr);
        goto CommonExit;
    }

    status = AppMgr::extractSingleAppOut(appOutBlob, &appOutput);
    BailIfFailed(status);

    outStrLen = strlen(appOutput);

    *outputSize = XcalarApiSizeOfOutput(*targetOutput) + outStrLen + 1;
    *output = (XcalarApiOutput *) memAlloc(*outputSize);
    BailIfNull(*output);

    targetOutput = &(*output)->outputResult.targetOutput;

    memcpy(targetOutput->outputJson, appOutput, outStrLen + 1);
    assert(targetOutput->outputJson[outStrLen] == '\0');
    targetOutput->outputLen = outStrLen + 1;

CommonExit:
    if (targetApp) {
        AppMgr::get()->closeAppHandle(targetApp, handle);
        targetApp = NULL;
    }
    if (appOutBlob) {
        memFree(appOutBlob);
        appOutBlob = NULL;
    }
    if (appOutput) {
        memFree(appOutput);
        appOutput = NULL;
    }
    if (errorStr) {
        memFree(errorStr);
        errorStr = NULL;
    }
    if (status != StatusOk) {
        if (*output) {
            memFree(*output);
            *output = NULL;
        }
    }
    return status;
}

Status
ApiHandlerTarget::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->previewInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->targetInput;

    if (sizeof(*input_) + input_->inputLen != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) + inputLen = %lu + %lu = %lu bytes",
                inputSize,
                sizeof(*input_),
                input_->inputLen,
                sizeof(*input) + input_->inputLen);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
