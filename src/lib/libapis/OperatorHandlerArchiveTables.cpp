// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerArchiveTables.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "msg/Xid.h"
#include "stat/Statistics.h"
#include "xdb/Xdb.h"
#include "strings/String.h"

OperatorHandlerArchiveTables::OperatorHandlerArchiveTables(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerArchiveTables::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | IsOperator);
}

const char *
OperatorHandlerArchiveTables::getDstNodeName()
{
    return nodeName_;
}

const char *
OperatorHandlerArchiveTables::getDstNodeName(XcalarApiOutput *output)
{
    return getDstNodeName();
}

Status
OperatorHandlerArchiveTables::setArg(XcalarApiInput *input,
                                     size_t inputSize,
                                     bool parentNodeIdsToBeProvided)
{
    // XXX: Archive tables API to be nuked
    return StatusUnimpl;
}

Status
OperatorHandlerArchiveTables::run(XcalarApiOutput **outputOut,
                                  size_t *outputSizeOut,
                                  void *optimizerContext)
{
    // XXX: Archive tables API to be nuked
    return StatusUnimpl;
}

Status
OperatorHandlerArchiveTables::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    // Should never be called
    assert(0);
    return StatusUnimpl;
}

Status
OperatorHandlerArchiveTables::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    *numParentsOut = 0;
    *parentNodeIdsOut = NULL;
    *parentGraphIdsOut = NULL;
    *sessionContainersOut = NULL;
    return StatusOk;
}

Status
OperatorHandlerArchiveTables::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                            DagTypes::GraphType srcGraphType,
                                            const char *nodeNameIn,
                                            uint64_t numParents,
                                            Dag **parentGraphs,
                                            DagTypes::NodeId *parentNodeIds)
{
    Status status;

    // One of the difference between queryGraph and workspaceGraph.
    // Mutations (e.g. delete, rename, etc) is a node in a queryGraph,
    // but a state in the workspaceGraph
    if (dstGraphType_ == DagTypes::QueryGraph) {
        status = dstGraph_->createNewDagNode(api_,
                                             apiInput_,
                                             inputSize_,
                                             dstXdbId_,
                                             dstTableId_,
                                             nodeName_,
                                             numParents,
                                             parentGraphs,
                                             parentNodeIds,
                                             &dstNodeId_);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create deleteObj node: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        status = StatusOk;
    }

CommonExit:
    if (status != StatusOk) {
        if (dstNodeId_ != DagTypes::InvalidDagNodeId) {
            Status status2 =
                dstGraph_->dropAndChangeState(dstNodeId_, DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        dstNodeId_,
                        strGetFromStatus(status2));
            }
            dstNodeId_ = DagTypes::InvalidDagNodeId;
        }
    }

    *dstNodeIdOut = dstNodeId_;
    return status;
}
