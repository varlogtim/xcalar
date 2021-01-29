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
#include "libapis/ApiHandlerDatasetUnload.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"

ApiHandlerDatasetUnload::ApiHandlerDatasetUnload(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerDatasetUnload::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerDatasetUnload::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = StatusOk;
    Dataset *ds = Dataset::get();

    status = ds->unloadDatasets(input_->datasetNamePattern, output, outputSize);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to unload dataset(s) matching supplied pattern "
                      "'%s': %s",
                      input_->datasetNamePattern,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerDatasetUnload::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->datasetUnloadInput;

    // Do some sanity checks

    if (sizeof(*input_) != inputSize) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Input size provided (%lu bytes) does not match "
                      "sizeof(*input_) = %lu bytes",
                      inputSize,
                      sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:

    return status;
}
