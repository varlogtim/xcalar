// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetDatasetsInfo.h"
#include "dataset/Dataset.h"
#include "sys/XLog.h"

ApiHandlerGetDatasetsInfo::ApiHandlerGetDatasetsInfo(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetDatasetsInfo::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetDatasetsInfo::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status;

    status = Dataset::get()->getDatasetsInfo(input_->datasetsNamePattern,
                                             outputOut,
                                             outputSizeOut);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get info for dataset(s) matching '%s': %s",
                      input_->datasetsNamePattern,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerGetDatasetsInfo::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->getDatasetsInfoInput;

    return status;
}
