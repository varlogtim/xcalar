// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetTableRefCount.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"

ApiHandlerGetTableRefCount::ApiHandlerGetTableRefCount(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetTableRefCount::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerGetTableRefCount::run(XcalarApiOutput **outputOut,
                                size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiGetTableRefCountOutput *refCountOutput;
    XcalarApiDagOutput *dagOutput;
    size_t dummyOutputSize;

    outputSize = XcalarApiSizeOfOutput(*refCountOutput);
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

    refCountOutput = &output->outputResult.getTableRefCountOutput;
    assert((uintptr_t) refCountOutput == (uintptr_t) &output->outputResult);

    status = dstGraph_->getChildDagNode(input_->tableName,
                                        &dagOutput,
                                        &dummyOutputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error getting child nodes for node \"%s\": %s",
                input_->tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    refCountOutput->refCount = 0;
    for (uint64_t ii = 0; ii < dagOutput->numNodes; ++ii) {
        if (dagOutput->node[ii]->hdr.state == DgDagStateProcessing) {
            refCountOutput->refCount++;
        }
    }
    memFree(dagOutput);

    refCountOutput->refCount++;
    assert(refCountOutput->refCount >= 1);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetTableRefCount::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    DagTypes::NodeId nodeId;

    assert((uintptr_t) input == (uintptr_t) &input->getTableRefCountInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->getTableRefCountInput;

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

    status = dstGraph_->getDagNodeId(input_->tableName,
                                     Dag::TableScope::LocalOnly,
                                     &nodeId);
    if (status == StatusDagNodeNotFound) {
        status = StatusTableNotFound;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieving tableId for \"%s\": %s",
                input_->tableName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
