// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <usr/Users.h>
#include "primitives/Primitives.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerDeleteObjects.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "msg/Xid.h"
#include "stat/Statistics.h"
#include "xdb/Xdb.h"

OperatorHandlerDeleteObjects::OperatorHandlerDeleteObjects(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerDeleteObjects::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | IsOperator);
}

const char *
OperatorHandlerDeleteObjects::getDstNodeName()
{
    return nodeName_;
}

const char *
OperatorHandlerDeleteObjects::getDstNodeName(XcalarApiOutput *output)
{
    return getDstNodeName();
}

Status
OperatorHandlerDeleteObjects::setArg(XcalarApiInput *input,
                                     size_t inputSize,
                                     bool parentNodeIdsToBeProvided)
{
    Status status;
    size_t ret;
    // Workaround for IMD table lock
    KvStoreId KvStoreId;
    char *lockKey = NULL;
    char *value = NULL;
    size_t valueSize = 0;

    assert((uintptr_t) input == (uintptr_t) &input->deleteDagNodesInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->deleteDagNodesInput;
    dstNodeId_ = XidMgr::get()->xidGetNext();

    if (!isValidSourceType(input_->srcType)) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid srcType provided: %d",
                input_->srcType);
        status = StatusInval;
        goto CommonExit;
    }

    ret = snprintf(nodeName_,
                   sizeof(nodeName_),
                   XcalarApiDeleteObjNodePrefix "%lu",
                   XidMgr::get()->xidGetNext());
    if (ret >= sizeof(nodeName_)) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    // XXX Workaround for IMD table lock
    if (dstGraphType_ == DagTypes::WorkspaceGraph && userId_ != NULL &&
        sessionInfo_ != NULL) {
        status =
            UserMgr::get()->getKvStoreId(userId_, sessionInfo_, &KvStoreId);
        BailIfFailed(status);
        lockKey = (char *) memAlloc(strlen(NonPersistIMDKey) +
                                    strlen(input_->namePattern) + 1);
        BailIfNull(lockKey);
        strcpy(lockKey, NonPersistIMDKey);
        strcat(lockKey, input_->namePattern);
        status =
            KvStoreLib::get()->lookup(KvStoreId, lockKey, &value, &valueSize);
        if (status == StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Table: %s is already locked, can't drop",
                    input_->namePattern);
            status = StatusIMDTableAlreadyLocked;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    if (value != NULL) {
        memFree(value);
        value = NULL;
    }
    if (lockKey != NULL) {
        memFree(lockKey);
        lockKey = NULL;
    }
    return status;
}

Status
OperatorHandlerDeleteObjects::run(XcalarApiOutput **output,
                                  size_t *outputSize,
                                  void *optimizerContext)
{
    return dstGraph_->bulkDropNodes(input_->namePattern,
                                    output,
                                    outputSize,
                                    input_->srcType,
                                    userId_,
                                    input_->deleteCompletely);
}

Status
OperatorHandlerDeleteObjects::runHandler(
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
OperatorHandlerDeleteObjects::getParentNodes(
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
OperatorHandlerDeleteObjects::createDagNode(DagTypes::NodeId *dstNodeIdOut,
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
