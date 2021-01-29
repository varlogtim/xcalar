// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListDatasets.h"
#include "msg/MessageTypes.h"
#include "dataset/Dataset.h"
#include "sys/XLog.h"

ApiHandlerListDatasets::ApiHandlerListDatasets(XcalarApis api) : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerListDatasets::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListDatasets::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = StatusOk;

    status = Dataset::get()->listDatasets(output, outputSize);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to obtain list of datasets: %s",
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerListDatasets::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    return StatusOk;
}
