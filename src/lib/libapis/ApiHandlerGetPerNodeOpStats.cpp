// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetPerNodeOpStats.h"
#include "msg/MessageTypes.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "sys/XLog.h"

ApiHandlerGetPerNodeOpStats::ApiHandlerGetPerNodeOpStats(XcalarApis api)
    : ApiHandler(api), input_(NULL), nodeId_(DagTypes::InvalidDagNodeId)
{
}

ApiHandler::Flags
ApiHandlerGetPerNodeOpStats::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerGetPerNodeOpStats::run(XcalarApiOutput **outputOut,
                                 size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    bool refAcquired = false;
    Status status = StatusUnknown;
    TableNsMgr::TableHandleTrack handleTrack;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    status = dstGraph_->getDagNodeRefById(nodeId_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get refCount for table \"%s\" (%lu): %s",
                input_,
                nodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }
    refAcquired = true;

    status = dstGraph_->getTableIdFromNodeId(nodeId_, &handleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    nodeId_,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(dstGraph_->getSessionContainer(),
                                    handleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &handleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    handleTrack.tableId,
                    strGetFromStatus(status));
    handleTrack.tableHandleValid = true;

    status = dstGraph_->getPerNodeOpStats(nodeId_, &output, &outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get per node operation stats for table \"%s\" "
                "(%lu): %s",
                input_,
                nodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (refAcquired) {
        dstGraph_->putDagNodeRefById(nodeId_);
        refAcquired = false;
    }

    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetPerNodeOpStats::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->dagTableNameInput.tableInput;

    if (inputSize - 1 > XcalarApiMaxTableNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Table name length too long (%lu chars). Max is %u chars",
                inputSize - 1,
                XcalarApiMaxTableNameLen);
        status = StatusNoBufs;
        goto CommonExit;
    }

    // XXX TODO Do we need to support fully qualified table names?
    status =
        dstGraph_->getDagNodeId(input_, Dag::TableScope::LocalOnly, &nodeId_);
    if (status == StatusDagNodeNotFound) {
        status = StatusTableNotFound;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get tableId for table \"%s\": %s",
                input_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
