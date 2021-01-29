// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerAppRun.h"
#include "app/AppMgr.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerAppRun::ApiHandlerAppRun(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerAppRun::getFlags()
{
    return (Flags)(NeedsAck);
}

Status
ApiHandlerAppRun::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    size_t outputSize = XcalarApiSizeOfOutput(XcalarApiAppRunOutput);
    LibNsTypes::NsHandle handle;
    char *errorStr = NULL;
    App *app = NULL;
    AppGroup::Id appGroupId;
    bool appInternalError = false;
    XcalarApiOutput *output = (XcalarApiOutput *) memAlloc(outputSize);

    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Run %s failed: %s",
                input_->name,
                strGetFromStatus(status));
        return StatusNoMem;
    }
    memZero(output, outputSize);

    app = AppMgr::get()->openAppHandle(input_->name, &handle);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "App Run %s failed on openAppHandle: %s",
                input_->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        AppMgr::get()->runMyAppAsync(app,
                                     input_->scope,
                                     userId_->userIdName,
                                     sessionInfo_ ? sessionInfo_->sessionId : 0,
                                     input_->inStr,
                                     0,
                                     &appGroupId,
                                     &errorStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Run %s failed on runMyAppAsync: %s",
                input_->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    output->outputResult.appRunOutput.appGroupId = appGroupId;

    if (errorStr != NULL) {
        // XXX Propagate to caller.
        xSyslog(ModuleName,
                XlogErr,
                "App Run %s failed: %s",
                input_->name,
                errorStr);
        memFree(errorStr);
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        assert(output != NULL);
        memFree(output);
        output = NULL;
    }
    if (app) {
        AppMgr::get()->closeAppHandle(app, handle);
    }

    return status;
}

Status
ApiHandlerAppRun::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->appRunInput;
    return StatusOk;
}
