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
#include "libapis/OperatorHandlerProject.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"

OperatorHandlerProject::OperatorHandlerProject(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerProject::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerProject::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerProject::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.projectOutput.tableName;
}

Status
OperatorHandlerProject::setArg(XcalarApiInput *input,
                               size_t inputSize,
                               bool parentNodeIdsToBeProvided)
{
    Status status;
    size_t expectedInputSize;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->projectInput);
    apiInput_ = input;
    input_ = &input->projectInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    input_->dstTable.tableId = dstNodeId_;
    dstXdbId_ = XidMgr::get()->xidGetNext();
    input_->dstTable.xdbId = dstXdbId_;
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        dstTableId_ = tnsMgr->genTableId();
    }

    expectedInputSize =
        sizeof(*input_) + (sizeof(input_->columnNames[0]) * input_->numColumns);

    // Please call init() before setArg()
    assert(dstGraph_ != NULL);

    // Do some sanity checks
    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes, numColumns: %u",
                inputSize,
                expectedInputSize,
                input_->numColumns);
        status = StatusInval;
        goto CommonExit;
    }

    if (!xcalarEval->isValidTableName(input_->dstTable.tableName)) {
        xSyslog(moduleName,
                XlogErr,
                "Destination table name (%s) is invalid",
                input_->dstTable.tableName);
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->numColumns == 0) {
        xSyslog(moduleName, XlogErr, "Projection cannot have 0 columns");
        status = StatusInval;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < input_->numColumns; ii++) {
        if (input_->columnNames[ii][0] == '\0') {
            xSyslog(moduleName,
                    XlogErr,
                    "Projection cannot have empty columns: "
                    "column %u is empty",
                    ii);
            status = StatusInval;
            goto CommonExit;
        }
    }

    if (input_->dstTable.tableName[0] == '\0') {
        int ret = snprintf(input_->dstTable.tableName,
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
OperatorHandlerProject::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *projectOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(*projectOutput);
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

    projectOutput = &output->outputResult.projectOutput;
    assert((uintptr_t) projectOutput == (uintptr_t) &output->outputResult);

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
                "Parent table \"%s\" (%lu) in state: %s. Aborting project",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status =
        Operators::get()->project(dstGraph_, input_, optimizerContext, userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       projectOutput->tableName,
                                       sizeof(projectOutput->tableName));
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
OperatorHandlerProject::getParentNodes(
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

    parIdx = 0;
    status = getSourceDagNode(input_->srcTable.tableName,
                              dstGraph_,
                              &sessionContainers[parIdx],
                              &parentGraphIds[parIdx],
                              &parentNodeIds[parIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getParentNodes table \"%s\" numParents %lu: %s",
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
OperatorHandlerProject::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                      DagTypes::GraphType srcGraphType,
                                      const char *nodeName,
                                      uint64_t numParents,
                                      Dag **parentGraphs,
                                      DagTypes::NodeId *parentNodeIds)
{
    Status status;
    unsigned parIdx = 0;

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
            parentGraphs[parIdx]->getXdbIdFromNodeId(input_->srcTable.tableId);
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
                "Failed to create project node %s: %s",
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

    *dstNodeIdOut = dstNodeId_;
    return status;
}

Status
OperatorHandlerProject::createXdb(void *optimizerContext)
{
    Status status;

    XdbId srcXdbId = input_->srcTable.xdbId;
    XdbId dstXdbId = input_->dstTable.xdbId;

    unsigned numDatasets = 0;
    DsDatasetId datasetIds[TupleMaxNumValuesPerRecord];
    XdbMeta *srcMeta = NULL;
    NewTupleMeta dstTupMeta;

    const char *immediateNames[input_->numColumns];
    const char *fatptrPrefixNames[TupleMaxNumValuesPerRecord];
    unsigned numImmediates = 0, numFatptrs = 0;
    DhtId dhtId;
    uint64_t ii;
    XdbMgr *xdbMgr = XdbMgr::get();

    status = xdbMgr->xdbGet(srcXdbId, NULL, &srcMeta);
    assert(status == StatusOk);

    // copy over src keys
    unsigned numKeys = srcMeta->numKeys;
    const char *keyNames[numKeys];
    DfFieldType keyTypes[numKeys];
    int keyIndexes[numKeys];
    Ordering keyOrdering[numKeys];
    bool keyKept;

    for (ii = 0; ii < numKeys; ii++) {
        keyNames[ii] = srcMeta->keyAttr[ii].name;
        keyTypes[ii] = srcMeta->keyAttr[ii].type;
        keyIndexes[ii] = srcMeta->keyAttr[ii].valueArrayIndex;
        keyOrdering[ii] = srcMeta->keyAttr[ii].ordering;
    }

    assert(srcXdbId != XdbIdInvalid);
    if (input_->numColumns >= TupleMaxNumValuesPerRecord) {
        status = StatusOverflow;
        goto CommonExit;
    }

    dhtId = srcMeta->dhtId;

    status =
        Operators::populateValuesDescWithProjectedFields(numKeys,
                                                         keyIndexes,
                                                         srcMeta->kvNamedMeta
                                                             .kvMeta_.tupMeta_,
                                                         &dstTupMeta,
                                                         srcMeta,
                                                         input_->columnNames,
                                                         input_->numColumns,
                                                         immediateNames,
                                                         &numImmediates,
                                                         fatptrPrefixNames,
                                                         &numFatptrs,
                                                         false,
                                                         &keyKept);
    BailIfFailed(status);

    if (numImmediates == input_->numColumns) {
        numDatasets = 0;
    } else {
        numDatasets = srcMeta->numDatasets;
        for (ii = 0; ii < numDatasets; ii++) {
            datasetIds[ii] = srcMeta->datasets[ii]->getDatasetId();
        }
    }

    assert(dhtId != DhtInvalidDhtId);

    if (!keyKept) {
        // use default key
        numKeys = 1;
        keyNames[0] = DsDefaultDatasetKeyName;
        keyTypes[0] = DfInt64;
        keyIndexes[0] = NewTupleMeta::DfInvalidIdx;
        dhtId = XidMgr::XidSystemRandomDht;
        keyOrdering[0] = Random;
    }

    if (numImmediates + numFatptrs == 0) {
        xSyslogTxnBuf(moduleName, XlogErr, "Project removed all columns");
        status = StatusInval;
        goto CommonExit;
    }

    status = xdbMgr->xdbCreate(dstXdbId,
                               numKeys,
                               keyNames,
                               keyTypes,
                               keyIndexes,
                               keyOrdering,
                               &dstTupMeta,
                               datasetIds,
                               numDatasets,
                               &immediateNames[0],
                               numImmediates,
                               fatptrPrefixNames,
                               numFatptrs,
                               XdbGlobal,
                               dhtId);
    dhtId = DhtInvalidDhtId;
    BailIfFailed(status);

    xdbCreated_ = true;

CommonExit:
    return status;
}
