// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetOpStats.h"
#include "msg/MessageTypes.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "sys/XLog.h"

ApiHandlerGetOpStats::ApiHandlerGetOpStats(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetOpStats::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerGetOpStats::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    DgDagState state;
    DagTypes::NodeId dagNodeId = DagTypes::InvalidDagNodeId;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    // XXX TODO Do we need to support fully qualified table names?
    status =
        dstGraph_->getDagNodeId(input_, Dag::TableScope::LocalOnly, &dagNodeId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeState(dagNodeId, &state);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getOpStats(dagNodeId, state, &output, &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetOpStats::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    DagTypes::NodeId nodeId;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->dagTableNameInput.tableInput;

    if (inputSize - 1 > XcalarApiMaxTableNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Table name len too long %lu chars. Max is %u chars",
                inputSize - 1,
                XcalarApiMaxTableNameLen);
        status = StatusInvalidTableName;
        goto CommonExit;
    }

    // XXX TODO Do we need to support fully qualified table names?
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
