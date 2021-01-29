// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisRecv.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerGetRowNum.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"

OperatorHandlerGetRowNum::OperatorHandlerGetRowNum(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerGetRowNum::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerGetRowNum::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerGetRowNum::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.getRowNumOutput.tableName;
}

Status
OperatorHandlerGetRowNum::setArg(XcalarApiInput *input,
                                 size_t inputSize,
                                 bool parentNodeIdsToBeProvided)
{
    Status status;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->getRowNumInput);
    apiInput_ = input;
    input_ = &input->getRowNumInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    input_->dstTable.tableId = dstNodeId_;
    dstXdbId_ = XidMgr::get()->xidGetNext();
    input_->dstTable.xdbId = dstXdbId_;
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        dstTableId_ = tnsMgr->genTableId();
    }

    // Please call init() before setArg()
    assert(dstGraph_ != NULL);

    // Do some sanity checks
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

    if (!xcalarEval->isValidTableName(input_->dstTable.tableName)) {
        xSyslog(moduleName,
                XlogErr,
                "Destination table name (%s) is not valid",
                input_->dstTable.tableName);
        status = StatusInval;
        goto CommonExit;
    }

    if (strlen(input_->dstTable.tableName) == 0) {
        int ret = snprintf(input_->dstTable.tableName,
                           DagTypes::MaxNameLen + 1,
                           XcalarTempDagNodePrefix "%lu",
                           dstNodeId_);
        if (ret >= DagTypes::MaxNameLen + 1) {
            status = StatusOverflow;
            goto CommonExit;
        }
    }

    if (!xcalarEval->isValidFieldName(input_->newFieldName)) {
        xSyslog(moduleName,
                XlogErr,
                "New field name (%s) is not valid",
                input_->newFieldName);
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
OperatorHandlerGetRowNum::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *getRowNumOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(*getRowNumOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output struct. "
                "(Size required: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }
    memZero(output, outputSize);

    getRowNumOutput = &output->outputResult.getRowNumOutput;
    assert((uintptr_t) getRowNumOutput == (uintptr_t) &output->outputResult);

    status = parentGraphs_[parIdx]->getDagNodeState(input_->srcTable.tableId,
                                                    &state);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get state of table \"%s\" (%lu): %s",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (state != DgDagStateReady) {
        xSyslog(moduleName,
                XlogErr,
                "Parent table \"%s\" (%lu) in state: %s. Aborting getRowNum",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status = Operators::get()->getRowNum(parentGraphs_[parIdx],
                                         dstGraph_,
                                         input_,
                                         optimizerContext,
                                         userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       getRowNumOutput->tableName,
                                       sizeof(getRowNumOutput->tableName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get name of table created: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
OperatorHandlerGetRowNum::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    uint64_t numParents = 1;
    Status status;
    DagTypes::NodeId *parentNodeIds = NULL;
    DagTypes::DagId *parentGraphIds = NULL;
    XcalarApiUdfContainer *sessionContainers = NULL;
    unsigned parIdx = 0;

    assert(!parentNodeIdsToBeProvided_);

    parentNodeIds = new (std::nothrow) DagTypes::NodeId[numParents];
    BailIfNullMsg(parentNodeIds,
                  StatusNoMem,
                  moduleName,
                  "Failed getParentNodes numParents %lu: %s",
                  numParents,
                  strGetFromStatus(status));

    parentGraphIds = new (std::nothrow) DagTypes::DagId[numParents];
    BailIfNullMsg(parentGraphIds,
                  StatusNoMem,
                  moduleName,
                  "Failed getParentNodes numParents %lu: %s",
                  numParents,
                  strGetFromStatus(status));

    sessionContainers = new (std::nothrow) XcalarApiUdfContainer[numParents];
    BailIfNullMsg(sessionContainers,
                  StatusNoMem,
                  moduleName,
                  "Failed getParentNodes numParents %lu: %s",
                  numParents,
                  strGetFromStatus(status));

    parIdx = 0;
    status = getSourceDagNode(input_->srcTable.tableName,
                              dstGraph_,
                              &sessionContainers[parIdx],
                              &parentGraphIds[parIdx],
                              &parentNodeIds[parIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getParentNodes source \"%s\", numParents %lu: %s",
                    input_->srcTable.tableName,
                    numParents,
                    strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        numParents = 0;
        if (parentNodeIds != NULL) {
            delete[] parentNodeIds;
            parentNodeIds = NULL;
        }
        if (parentGraphIds != NULL) {
            delete[] parentGraphIds;
            parentGraphIds = NULL;
        }
        if (sessionContainers != NULL) {
            delete[] sessionContainers;
            sessionContainers = NULL;
        }
    }

    *parentNodeIdsOut = parentNodeIds;
    *numParentsOut = numParents;
    *parentGraphIdsOut = parentGraphIds;
    *sessionContainersOut = sessionContainers;

    return status;
}

Status
OperatorHandlerGetRowNum::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                        DagTypes::GraphType srcGraphType,
                                        const char *nodeName,
                                        uint64_t numParents,
                                        Dag **parentGraphs,
                                        DagTypes::NodeId *parentNodeIds)
{
    Status status;
    unsigned parIdx = 0;

    assert(nodeName != NULL);

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    if (!parentNodeIdsToBeProvided_ &&
        dstGraphType_ == DagTypes::WorkspaceGraph) {
        // Then we better make sure the names are valid
        status = parentGraphs[parIdx]
                     ->getDagNodeId(input_->srcTable.tableName,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &input_->srcTable.tableId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving dagNodeId for \"%s\": %s",
                    input_->srcTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status =
            parentGraphs[parIdx]->getXdbId(input_->srcTable.tableName,
                                           Dag::TableScope::FullyQualOrLocal,
                                           &input_->srcTable.xdbId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving tableId for \"%s\": %s",
                    input_->srcTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == 1);
        assert(parentNodeIds != NULL);

        if (numParents != 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid number of parents provided: %lu",
                    numParents);
            status = StatusInval;
            goto CommonExit;
        }

        if (parentNodeIds == NULL) {
            xSyslog(moduleName, XlogErr, "parentNodeIds cannot be NULL!");
            status = StatusInval;
            goto CommonExit;
        }

        input_->srcTable.tableId = parentNodeIds[parIdx];
        input_->srcTable.xdbId =
            parentGraphs[parIdx]->getXdbIdFromNodeId(parentNodeIds[parIdx]);
    }

    assert((uintptr_t) apiInput_ == (uintptr_t) input_);
    status = dstGraph_->createNewDagNode(api_,
                                         apiInput_,
                                         inputSize_,
                                         dstXdbId_,
                                         dstTableId_,
                                         (char *) nodeName,
                                         numParents,
                                         parentGraphs,
                                         parentNodeIds,
                                         &dstNodeId_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create getRowNum node %s: %s",
                nodeName,
                strGetFromStatus(status));
        goto CommonExit;
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

Status
OperatorHandlerGetRowNum::createXdb(void *optimizerContext)
{
    Status status;
    DfFieldType outputType = DfInt64;

    char *field;
    field = input_->newFieldName;

    status = createSrcXdbCopy(input_->srcTable.xdbId,
                              dstXdbId_,
                              &outputType,
                              &field,
                              1,
                              optimizerContext);
    BailIfFailedMsg(moduleName,
                    status,
                    "GetRowNum(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

CommonExit:
    return status;
}
