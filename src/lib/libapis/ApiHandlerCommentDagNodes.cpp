// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerCommentDagNodes.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"

ApiHandlerCommentDagNodes::ApiHandlerCommentDagNodes(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerCommentDagNodes::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerCommentDagNodes::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
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

    status = dstGraph_->commentDagNodes(input_->comment,
                                        input_->nodeNamesCount,
                                        input_->nodeNames);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerCommentDagNodes::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    DagTypes::NodeId nodeId;

    assert((uintptr_t) input == (uintptr_t) &input->renameNodeInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->commentDagNodesInput;

    for (unsigned ii = 0; ii < input_->nodeNamesCount; ii++) {
        status = dstGraph_->getDagNodeId(input_->nodeNames[ii],
                                         Dag::TableScope::LocalOnly,
                                         &nodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving node \"%s\": %s",
                    input_->nodeNames[ii],
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}
