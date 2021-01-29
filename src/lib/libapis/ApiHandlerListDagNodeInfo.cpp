// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListDagNodeInfo.h"
#include "msg/MessageTypes.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "sys/XLog.h"

ApiHandlerListDagNodeInfo::ApiHandlerListDagNodeInfo(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerListDagNodeInfo::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerListDagNodeInfo::run(XcalarApiOutput **output, size_t *outputSize)
{
    assert(dstGraph_ != NULL);
    return dstGraph_->listDagNodeInfo(input_->namePattern,
                                      output,
                                      outputSize,
                                      input_->srcType,
                                      userId_);
}

Status
ApiHandlerListDagNodeInfo::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->listDagNodesInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listDagNodesInput;

    if (!isValidSourceType(input_->srcType)) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid srcType provided: %d",
                input_->srcType);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
