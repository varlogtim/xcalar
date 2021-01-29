// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerRenameNode.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"

ApiHandlerRenameNode::ApiHandlerRenameNode(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerRenameNode::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerRenameNode::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
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
                "Required size: %lu bytes",
                outputSize);
        outputSize = 0;
        status = StatusNoMem;
        goto CommonExit;
    }

    status = dstGraph_->renameDagNode(input_->oldName, input_->newName);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerRenameNode::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    DagTypes::NodeId nodeId;

    assert((uintptr_t) input == (uintptr_t) &input->renameNodeInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->renameNodeInput;

    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeId(input_->oldName,
                                     Dag::TableScope::LocalOnly,
                                     &nodeId);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieving node \"%s\": %s",
                input_->oldName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
