// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListUserDatasets.h"
#include "dataset/Dataset.h"
#include "sys/XLog.h"

ApiHandlerListUserDatasets::ApiHandlerListUserDatasets(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListUserDatasets::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListUserDatasets::run(XcalarApiOutput **outputOut,
                                size_t *outputSizeOut)
{
    Status status;

    status = Dataset::get()->listUserDatasets(input_->userIdName,
                                              outputOut,
                                              outputSizeOut);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get list of datasets for user '%s': %s",
                      input_->userIdName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerListUserDatasets::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listUserDatasetsInput;

    return status;
}
