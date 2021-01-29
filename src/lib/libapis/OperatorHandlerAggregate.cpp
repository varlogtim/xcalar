// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisRecv.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerAggregate.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"
#include "scalars/Scalars.h"
#include "operators/XcalarEval.h"
#include "udf/UserDefinedFunction.h"

OperatorHandlerAggregate::OperatorHandlerAggregate(XcalarApis api)
    : OperatorHandler(api),
      input_(NULL),
      numAggVariables_(0),
      numVariables_(0),
      variableNames_(NULL),
      astCreated_(false)
{
}

OperatorHandlerAggregate::~OperatorHandlerAggregate()
{
    destroyArg();
}

ApiHandler::Flags
OperatorHandlerAggregate::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | IsOperator);
}

void
OperatorHandlerAggregate::destroyArg()
{
    if (variableNames_ != NULL) {
        memFree(variableNames_);
        variableNames_ = NULL;
    }

    if (astCreated_) {
        // variableNames is pointing to stuff inside the ast.
        // So before freeing ast, need to make sure variableNames doesn't
        // exist
        assert(variableNames_ == NULL);
        XcalarEval::get()->freeCommonAst(&ast_);
        astCreated_ = false;
    }

    numVariables_ = 0;
    numAggVariables_ = 0;
    input_ = NULL;
}

const char *
OperatorHandlerAggregate::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerAggregate::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.aggregateOutput.tableName;
}

Status
OperatorHandlerAggregate::setArg(XcalarApiInput *input,
                                 size_t inputSize,
                                 bool parentNodeIdsToBeProvided)
{
    Status status;
    unsigned numUniqueVariables = 0;
    unsigned numVariables = 0;
    unsigned ii;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->aggregateInput);
    apiInput_ = input;
    input_ = &input->aggregateInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    input_->dstTable.tableId = dstNodeId_;
    dstXdbId_ = XidMgr::get()->xidGetNext();
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        dstTableId_ = tnsMgr->genTableId();
    }
    input_->dstTable.xdbId = dstXdbId_;

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

    status = xcalarEval->parseEvalStr(input_->evalStr,
                                      XcalarFnTypeAgg,
                                      dstGraph_->getUdfContainer(),
                                      &ast_,
                                      NULL,
                                      &numVariables,
                                      NULL,
                                      NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    astCreated_ = true;

    variableNames_ =
        (const char **) memAlloc(sizeof(*variableNames_) * numVariables);
    if (variableNames_ == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate variableNames "
                "(numVariables: %u)",
                numVariables);
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarEval->getUniqueVariablesList(&ast_,
                                                variableNames_,
                                                numVariables,
                                                &numUniqueVariables);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(numUniqueVariables <= numVariables);
    numVariables_ = numUniqueVariables;

    numAggVariables_ = 0;
    for (ii = 0; ii < numUniqueVariables; ii++) {
        if (*variableNames_[ii] == OperatorsAggregateTag) {
            numAggVariables_++;
        }
    }

CommonExit:
    if (status != StatusOk) {
        destroyArg();
    }

    return status;
}

Status
OperatorHandlerAggregate::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiAggregateOutput *aggregateOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    Scalar *scalarOut = NULL;
    DfFieldValue fieldVal;
    json_t *fakeRecord = NULL;
    char *fakeRecordStr = NULL;
    int ret;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(*aggregateOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize + XdbMgr::bcSize(),
                                             moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output struct. "
                "(Size required: %lu bytes)",
                outputSize + XdbMgr::bcSize());
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }
    memZero(output, outputSize);

    aggregateOutput = &output->outputResult.aggregateOutput;
    assert((uintptr_t) aggregateOutput == (uintptr_t) &output->outputResult);

    scalarOut = Scalar::allocScalar(DfMaxFieldValueSize);
    if (scalarOut == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate scalarOut. "
                "(Size required: %u bytes)",
                DfMaxFieldValueSize);
        status = StatusNoMem;
        goto CommonExit;
    }

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
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting aggregate",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status = Operators::get()->aggregate(dstGraph_,
                                         input_,
                                         scalarOut,
                                         false,
                                         userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       aggregateOutput->tableName,
                                       sizeof(aggregateOutput->tableName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get name of table created: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = scalarOut->getValue(&fieldVal);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error extracting result from scalar: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // This lets us construct a 'record' where the only field is the
    // aggregate result
    fakeRecord = json_object();
    BailIfNull(fakeRecord);

    status = DataFormat::get()->addJsonFieldImm(fakeRecord,
                                                NULL,
                                                "Value",
                                                scalarOut->fieldType,
                                                &fieldVal);
    BailIfFailed(status);

    fakeRecordStr = json_dumps(fakeRecord, JSON_COMPACT);
    BailIfNull(fakeRecordStr);

    ret = strlcpy(aggregateOutput->jsonAnswer, fakeRecordStr, XdbMgr::bcSize());
    if (ret >= (int) XdbMgr::bcSize()) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // +1 to account for the null terminator
    outputSize += ret + 1;
    status = dstGraph_->setScalarResult(dstNodeId_, scalarOut);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error binding scalar result to dagNode \"%s\" (%lu): %s",
                aggregateOutput->tableName,
                dstNodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    if (scalarOut != NULL) {
        Scalar::freeScalar(scalarOut);
        scalarOut = NULL;
    }
    if (fakeRecord != NULL) {
        json_decref(fakeRecord);
        fakeRecord = NULL;
    }
    if (fakeRecordStr != NULL) {
        memFree(fakeRecordStr);
        fakeRecordStr = NULL;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
OperatorHandlerAggregate::getParentNodes(
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
    unsigned ii, parIdx = 0;

    assert(!parentNodeIdsToBeProvided_);
    numParents = 1 + numAggVariables_;

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

    parIdx++;
    for (ii = 0; ii < numVariables_; ii++) {
        if (*variableNames_[ii] == OperatorsAggregateTag) {
            assert(parIdx < numParents);
            status = getSourceDagNode(&variableNames_[ii][1],
                                      dstGraph_,
                                      &sessionContainers[parIdx],
                                      &parentGraphIds[parIdx],
                                      &parentNodeIds[parIdx]);
            if (status != StatusOk) {
                // Working in WorkspaceGraph should have all aggregates
                if (dstGraphType_ == DagTypes::WorkspaceGraph) {
                    BailIfFailedMsg(moduleName,
                                    status,
                                    "Error retrieving variable \"%s\": %s",
                                    variableNames_[ii],
                                    strGetFromStatus(status));
                } else {
                    // QueryGraph may not have all the aggregate
                    // It's ok to continue
                    status = StatusOk;
                    continue;
                }
            }
            parIdx++;
        }
    }
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == parIdx);
    } else {
        numParents = parIdx;
    }

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
OperatorHandlerAggregate::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                        DagTypes::GraphType srcGraphType,
                                        const char *nodeName,
                                        uint64_t numParents,
                                        Dag **parentGraphs,
                                        DagTypes::NodeId *parentNodeIds)
{
    Status status;
    unsigned ii, parIdx = 0;

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    parIdx = 0;
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

        parIdx++;
        for (ii = 0; ii < numVariables_; ii++) {
            if (*variableNames_[ii] == OperatorsAggregateTag) {
                DagTypes::NodeId aggNodeId;
                status = parentGraphs[parIdx]
                             ->getDagNodeId(&variableNames_[ii][1],
                                            Dag::TableScope::FullyQualOrLocal,
                                            &aggNodeId);
                if (status == StatusDagNodeNotFound) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Could not find global variable \"%s\"",
                            variableNames_[ii]);
                    status = StatusGlobalVariableNotFound;
                    goto CommonExit;
                } else if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Error retrieving variable \"%s\": %s",
                            variableNames_[ii],
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                parIdx++;
            }
        }
    }

    parIdx = 0;
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == 1 + numAggVariables_);
        assert(parentNodeIds != NULL);

        if (numParents != 1 + numAggVariables_) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid number of parents provided: %lu. Required: %u",
                    numParents,
                    1 + numAggVariables_);
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
                "Failed to create aggregate node %s: %s",
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

    if (dstNodeIdOut != NULL) {
        *dstNodeIdOut = dstNodeId_;
    }

    return status;
}
