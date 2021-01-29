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
#include "libapis/ApiHandlerListDatasetUsers.h"
#include "dataset/Dataset.h"
#include "sys/XLog.h"

ApiHandlerListDatasetUsers::ApiHandlerListDatasetUsers(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListDatasetUsers::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListDatasetUsers::run(XcalarApiOutput **outputOut,
                                size_t *outputSizeOut)
{
    Status status;

    status = Dataset::get()->listDatasetUsers(input_->datasetName,
                                              outputOut,
                                              outputSizeOut);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get users of dataset '%s': %s",
                      input_->datasetName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerListDatasetUsers::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listDatasetUsersInput;

    return status;
}
