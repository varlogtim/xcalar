// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerCancelOp.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "util/MemTrack.h"

ApiHandlerCancelOp::ApiHandlerCancelOp(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerCancelOp::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerCancelOp::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusUnknown;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    status = dstGraph_->cancelOp(input_);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerCancelOp::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    DagTypes::NodeId nodeId;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->dagTableNameInput.tableInput;

    xSyslog(moduleName, XlogInfo, "Starting op cancel of %s", input_);

    if (inputSize - 1 > XcalarApiMaxTableNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Table name len too long %lu chars. Max is %u chars",
                inputSize - 1,
                XcalarApiMaxTableNameLen);
        status = StatusInvalidTableName;
        goto CommonExit;
    }

    status =
        dstGraph_->getDagNodeId(input_, Dag::TableScope::LocalOnly, &nodeId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieving node \"%s\": %s",
                input_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
