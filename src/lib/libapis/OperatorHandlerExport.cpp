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
#include "libapis/OperatorHandlerExport.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "sys/XLog.h"
#include "export/Export.h"

OperatorHandlerExport::OperatorHandlerExport(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerExport::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | IsOperator);
}

const char *
OperatorHandlerExport::getDstNodeName()
{
    return input_->exportName;
}

const char *
OperatorHandlerExport::getDstNodeName(XcalarApiOutput *output)
{
    return getDstNodeName();
}

Status
OperatorHandlerExport::setArg(XcalarApiInput *input,
                              size_t inputSize,
                              bool parentNodeIdsToBeProvided)
{
    Status status;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    size_t expectedInputSize;

    assert((uintptr_t) input == (uintptr_t) &input->exportInput);
    apiInput_ = input;
    input_ = &input->exportInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    dstXdbId_ = XidMgr::get()->xidGetNext();
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        dstTableId_ = tnsMgr->genTableId();
    }

    // Please call init() before setArg()
    assert(dstGraph_ != NULL);

    expectedInputSize = sizeof(*input_) + (sizeof(input_->meta.columns[0]) *
                                           input_->meta.numColumns);
    // Do some sanity checks
    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (numCols: %d)",
                inputSize,
                expectedInputSize,
                input_->meta.numColumns);
        status = StatusInval;
        goto CommonExit;
    }

    if (strlen(input_->exportName) == 0) {
        int ret = snprintf(input_->exportName,
                           DagTypes::MaxNameLen + 1,
                           XcalarTempDagNodePrefix "%lu",
                           dstNodeId_);
        if (ret >= DagTypes::MaxNameLen + 1) {
            status = StatusOverflow;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
OperatorHandlerExport::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    ExportAppHost exportHost;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
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

    status = parentGraphs_[parIdx]->getDagNodeState(input_->srcTable.tableId,
                                                    &state);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get state of dagNode \"%s\" (%lu): %s",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (state != DgDagStateReady) {
        xSyslog(moduleName,
                XlogErr,
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting export",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status = exportHost.exportTable(dstGraph_,
                                    dstNodeId_,
                                    dstXdbId_,
                                    input_,
                                    userId_);
    BailIfFailed(status);

CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    if (status != StatusOk) {
        if (status == StatusExportTooManyColumns) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Export failed on %s since %s. Max allowed: %d",
                          input_->srcTable.tableName,
                          strGetFromStatus(status),
                          TupleMaxNumValuesPerRecord);
        } else {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Operation failed on %s with id: %lu since %s",
                          input_->srcTable.tableName,
                          input_->srcTable.tableId,
                          strGetFromStatus(status));
        }
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
OperatorHandlerExport::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    uint64_t numParents = 0;
    Status status;
    DagTypes::NodeId *parentNodeIds = NULL;
    DagTypes::DagId *parentGraphIds = NULL;
    XcalarApiUdfContainer *sessionContainers = NULL;
    unsigned parIdx = 0;

    assert(!parentNodeIdsToBeProvided_);
    numParents = 1;

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

    status = getSourceDagNode(input_->srcTable.tableName,
                              dstGraph_,
                              &sessionContainers[parIdx],
                              &parentGraphIds[parIdx],
                              &parentNodeIds[parIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getParentNodes source \"%s\" numParents %lu: %s",
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
OperatorHandlerExport::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                     DagTypes::GraphType srcGraphType,
                                     const char *nodeName,
                                     uint64_t numParents,
                                     Dag **parentGraphs,
                                     DagTypes::NodeId *parentNodeIds)
{
    Status status;
    unsigned parIdx = 0;

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == 1);
        assert(parentNodeIds != NULL);

        if (numParents != 1) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid number of parents provided: %lu. Required: %u",
                    numParents,
                    1);
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
                "Failed to create export node %s: %s",
                nodeName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (dstNodeId_ != DagTypes::InvalidDagNodeId) {
            Status status2;
            status2 =
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

    if (dstNodeIdOut != NULL) {
        *dstNodeIdOut = dstNodeId_;
    }

    return status;
}
