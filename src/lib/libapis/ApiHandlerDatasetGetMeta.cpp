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
#include "libapis/ApiHandlerDatasetGetMeta.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "operators/XcalarEval.h"

ApiHandlerDatasetGetMeta::ApiHandlerDatasetGetMeta(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerDatasetGetMeta::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerDatasetGetMeta::run(XcalarApiOutput **outputOut,
                              size_t *outputSizeOut)
{
    Status status = StatusOk;
    Dataset *ds = Dataset::get();

    status = ds->getDatasetMetaAsStr(input_, outputOut, outputSizeOut);
    if (status != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Failed to get dataset meta for '%s': %s",
                      input_->datasetName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

Status
ApiHandlerDatasetGetMeta::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->datasetGetMetaInput;

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

    if (!XcalarEval::get()->isValidDatasetName(input_->datasetName)) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Invalid dataset name provided: %s",
                      input_->datasetName);
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:

    return status;
}
