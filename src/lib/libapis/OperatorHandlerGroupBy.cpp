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
#include "libapis/OperatorHandlerGroupBy.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"
#include "udf/UserDefinedFunction.h"
#include "strings/String.h"

OperatorHandlerGroupBy::OperatorHandlerGroupBy(XcalarApis api)
    : OperatorHandler(api), input_(NULL), numAggVariables_(0)
{
}

OperatorHandlerGroupBy::~OperatorHandlerGroupBy()
{
    destroyArg();
}

ApiHandler::Flags
OperatorHandlerGroupBy::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

void
OperatorHandlerGroupBy::destroyArg()
{
    if (aggVariableNames_ != NULL) {
        memFree(aggVariableNames_);
        aggVariableNames_ = NULL;
    }

    if (asts_ != NULL && astsCreated_ != NULL) {
        for (unsigned ii = 0; ii < numAsts_; ii++) {
            if (astsCreated_[ii]) {
                XcalarEval::get()->freeCommonAst(&asts_[ii]);
                astsCreated_[ii] = false;
            }
        }
    }

    if (asts_ != NULL) {
        memFree(asts_);
        asts_ = NULL;
    }

    if (astsCreated_ != NULL) {
        memFree(astsCreated_);
        astsCreated_ = NULL;
    }

    numAggVariables_ = 0;
    input_ = NULL;
}

const char *
OperatorHandlerGroupBy::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerGroupBy::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.groupByOutput.tableName;
}

Status
OperatorHandlerGroupBy::generateFullKeyMap()
{
    Status status;
    bool srcRef = false;
    XcalarApiKeyInput *keyInputTmp = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    TableNsMgr::TableHandleTrack handleTrack;
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();
    unsigned parIdx = 0;

    // get dag ref on parents to prevent them from getting deleted
    DgDagState dgState;
    status =
        parentGraphs_[parIdx]->getDagNodeStateAndRef(input_->srcTable.tableId,
                                                     &dgState);
    BailIfFailed(status);
    srcRef = true;

    if (dgState != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        goto CommonExit;
    }

    status =
        parentGraphs_[parIdx]->getTableIdFromNodeId(input_->srcTable.tableId,
                                                    &handleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    input_->srcTable.tableId,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(sessionContainer,
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

    // repopulate the supplied keyInput with all of the keys in the right order
    XdbMeta *srcMeta;
    status = XdbMgr::get()->xdbGet(input_->srcTable.xdbId, NULL, &srcMeta);
    BailIfFailed(status);

    keyInputTmp =
        (XcalarApiKeyInput *) memAlloc(sizeof(*keyInputTmp) * srcMeta->numKeys);
    BailIfNull(keyInputTmp);
    memZero(keyInputTmp, sizeof(*keyInputTmp) * srcMeta->numKeys);

    for (unsigned ii = 0; ii < srcMeta->numKeys; ii++) {
        status = strStrlcpy(keyInputTmp[ii].keyName,
                            srcMeta->keyAttr[ii].name,
                            sizeof(keyInputTmp[ii].keyName));
        BailIfFailed(status);

        keyInputTmp[ii].ordering = srcMeta->keyAttr[ii].ordering;
        keyInputTmp[ii].type = srcMeta->keyAttr[ii].type;

        // by default the new keyFieldName is the same as the original key name
        status = strStrlcpy(keyInputTmp[ii].keyFieldName,
                            srcMeta->keyAttr[ii].name,
                            sizeof(keyInputTmp[ii].keyFieldName));
        BailIfFailed(status);

        // check if we want a different keyFieldName from the supplied keyInput
        // the first condition handles legacy queries where only the first key
        // could be renamed
        for (unsigned jj = 0; jj < input_->numKeys; jj++) {
            if ((strlen(input_->keys[jj].keyName) == 0 && ii == 0) ||
                (strlen(input_->keys[jj].keyFieldName) > 0 &&
                 strcmp(srcMeta->keyAttr[ii].name, input_->keys[jj].keyName) ==
                     0)) {
                status = strStrlcpy(keyInputTmp[ii].keyFieldName,
                                    input_->keys[jj].keyFieldName,
                                    sizeof(keyInputTmp[ii].keyFieldName));
                BailIfFailed(status);
            }
        }
    }

    // replace old input with the new input we just constructed
    input_->numKeys = srcMeta->numKeys;
    memcpy(input_->keys, keyInputTmp, sizeof(*keyInputTmp) * srcMeta->numKeys);

CommonExit:
    if (srcRef) {
        parentGraphs_[parIdx]->putDagNodeRefById(input_->srcTable.tableId);
        srcRef = false;
    }

    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }

    if (keyInputTmp) {
        memFree(keyInputTmp);
        keyInputTmp = NULL;
    }

    return status;
}

Status
OperatorHandlerGroupBy::setArg(XcalarApiInput *input,
                               size_t inputSize,
                               bool parentNodeIdsToBeProvided)
{
    Status status;
    unsigned *numUniqueVariables = NULL;
    unsigned numVariables = 0;
    const char ***variableNames = NULL;
    unsigned ii, jj;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    // In case this is not the first time we're calling setArg();
    destroyArg();

    assert((uintptr_t) input == (uintptr_t) &input->groupByInput);
    apiInput_ = input;
    input_ = &input->groupByInput;
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

    size_t expectedSize = input_->numEvals * (XcalarApiMaxEvalStringLen + 1 +
                                              XcalarApiMaxFieldNameLen + 1) +
                          sizeof(*input_);

    // Do some sanity checks
    if (expectedSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes)",
                inputSize,
                expectedSize);
        status = StatusInval;
        goto CommonExit;
    }

    xcalarApiDeserializeGroupByInput(input_);

    if (input_->numEvals == 0) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Must specify at least one eval string");
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->groupAll) {
        if (input_->includeSrcTableSample) {
            xSyslog(moduleName,
                    XlogErr,
                    "GroupAll flag is incompatable with includeSample");
            status = StatusInval;
            goto CommonExit;
        }
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

    for (unsigned ii = 0; ii < input_->numEvals; ii++) {
        if (!xcalarEval->isValidFieldName(input_->newFieldNames[ii])) {
            xSyslog(moduleName,
                    XlogErr,
                    "New field name (%s) is not valid",
                    input_->newFieldNames[ii]);
            status = StatusInval;
            goto CommonExit;
        }

        // check that new field name does not clash with the key field names
        for (unsigned jj = 0; jj < input_->numKeys; jj++) {
            if (strcmp(input_->newFieldNames[ii],
                       input_->keys[jj].keyFieldName) == 0) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "New field name (%s) cannot conflict "
                              "with existing field name",
                              input_->keys[ii].keyFieldName);
                status = StatusInval;
                goto CommonExit;
            }
        }
    }

    for (unsigned ii = 0; ii < input_->numKeys; ii++) {
        if (!xcalarEval->isValidFieldName(input_->keys[ii].keyFieldName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "New key field name (%s) is not valid",
                    input_->keys[ii].keyFieldName);
            status = StatusInval;
            goto CommonExit;
        }

        for (unsigned jj = ii + 1; jj < input_->numKeys; jj++) {
            if (strcmp(input_->keys[ii].keyFieldName,
                       input_->keys[jj].keyFieldName) == 0) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Duplicated keyFieldName %s",
                              input_->keys[ii].keyFieldName);
                status = StatusInval;
                goto CommonExit;
            }
        }
    }

    numAggVariables_ = 0;
    numAsts_ = input_->numEvals;

    asts_ = (XcalarEvalAstCommon *) memAlloc(numAsts_ * sizeof(*asts_));
    BailIfNull(asts_);

    astsCreated_ = (bool *) memAlloc(numAsts_ * sizeof(astsCreated_));
    BailIfNull(astsCreated_);
    memZero(astsCreated_, numAsts_ * sizeof(astsCreated_));

    variableNames =
        (const char ***) memAlloc(numAsts_ * sizeof(*variableNames));
    BailIfNull(variableNames);
    memZero(variableNames, numAsts_ * sizeof(*variableNames));

    numUniqueVariables =
        (unsigned *) memAlloc(numAsts_ * sizeof(*numUniqueVariables));
    BailIfNull(numUniqueVariables);
    memZero(numUniqueVariables, numAsts_ * sizeof(*numUniqueVariables));

    for (ii = 0; ii < input_->numEvals; ii++) {
        status = xcalarEval->parseEvalStr(input_->evalStrs[ii],
                                          XcalarFnTypeAgg,
                                          dstGraph_->getUdfContainer(),
                                          &asts_[ii],
                                          NULL,
                                          &numVariables,
                                          NULL,
                                          NULL);
        if (status != StatusOk) {
            goto CommonExit;
        }
        astsCreated_[ii] = true;

        if (xcalarEval->isComplex(asts_[ii].rootNode) && input_->numEvals > 1) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Cannot perform multi-operation evals in a multi-eval"
                          "groupBy. EvalString: %s",
                          input_->evalStrs[ii]);
            status = StatusInval;
            goto CommonExit;
        }

        variableNames[ii] =
            (const char **) memAlloc(sizeof(*variableNames[ii]) * numVariables);
        if (variableNames[ii] == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate variableNames "
                    "(numVariables: %u)",
                    numVariables);
            status = StatusNoMem;
            goto CommonExit;
        }

        status = xcalarEval->getUniqueVariablesList(&asts_[ii],
                                                    variableNames[ii],
                                                    numVariables,
                                                    &numUniqueVariables[ii]);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(numUniqueVariables[ii] <= numVariables);

        for (jj = 0; jj < numUniqueVariables[ii]; jj++) {
            if (*variableNames[ii][jj] == OperatorsAggregateTag) {
                numAggVariables_++;
            }
        }
    }

    aggVariableNames_ =
        (const char **) memAlloc(numAggVariables_ * sizeof(*aggVariableNames_));
    BailIfNull(aggVariableNames_);

    int aggIdx;
    aggIdx = 0;
    for (ii = 0; ii < input_->numEvals; ii++) {
        for (jj = 0; jj < numUniqueVariables[ii]; jj++) {
            if (*variableNames[ii][jj] == OperatorsAggregateTag) {
                aggVariableNames_[aggIdx] = variableNames[ii][jj];
                aggIdx++;
            }
        }
    }

CommonExit:
    if (variableNames != NULL) {
        for (unsigned ii = 0; ii < input_->numEvals; ii++) {
            if (variableNames[ii] != NULL) {
                memFree(variableNames[ii]);
                variableNames[ii] = NULL;
            }
        }
        memFree(variableNames);
        variableNames = NULL;
    }
    if (numUniqueVariables != NULL) {
        memFree(numUniqueVariables);
        numUniqueVariables = NULL;
    }
    if (status != StatusOk) {
        destroyArg();
    }
    return status;
}

Status
OperatorHandlerGroupBy::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *groupByOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(*groupByOutput);
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

    groupByOutput = &output->outputResult.groupByOutput;
    assert((uintptr_t) groupByOutput == (uintptr_t) &output->outputResult);

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
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting groupBy",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < numAsts_; ii++) {
        unsigned cur = 0;
        size_t size = XcalarApiMaxEvalStringLen + 1;

        status = XcalarEval::get()->reverseParseAst(XcalarEvalAstGroupOperator,
                                                    asts_[ii].rootNode,
                                                    input_->evalStrs[ii],
                                                    cur,
                                                    size,
                                                    XcalarEval::NoFlags);
        BailIfFailedMsg(moduleName,
                        status,
                        "GroupBy(%lu) failed reverseParseAst %s",
                        input_->dstTable.tableId,
                        strGetFromStatus(status));
    }

    status = Operators::get()->groupBy(dstGraph_,
                                       input_,
                                       inputSize_,
                                       optimizerContext,
                                       userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       groupByOutput->tableName,
                                       sizeof(groupByOutput->tableName));
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
OperatorHandlerGroupBy::getParentNodes(
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
                    "Failed getParentNodes source \"%s\", numParents %lu: %s",
                    input_->srcTable.tableName,
                    numParents,
                    strGetFromStatus(status));

    parIdx++;
    for (ii = 0; ii < numAggVariables_; ii++) {
        if (*aggVariableNames_[ii] == OperatorsAggregateTag) {
            assert(parIdx < numParents);
            status = getSourceDagNode(&aggVariableNames_[ii][1],
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
                                    aggVariableNames_[ii],
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
OperatorHandlerGroupBy::createDagNode(DagTypes::NodeId *dstNodeIdOut,
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
        for (ii = 0; ii < numAggVariables_; ii++) {
            DagTypes::NodeId aggNodeId;

            status = parentGraphs[parIdx]
                         ->getDagNodeId(&aggVariableNames_[ii][1],
                                        Dag::TableScope::FullyQualOrLocal,
                                        &aggNodeId);
            if (status == StatusDagNodeNotFound) {
                xSyslog(moduleName,
                        XlogErr,
                        "Could not find global variable \"%s\"",
                        aggVariableNames_[ii]);
                status = StatusGlobalVariableNotFound;
                goto CommonExit;
            } else if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving variable \"%s\": %s",
                        aggVariableNames_[ii],
                        strGetFromStatus(status));
                goto CommonExit;
            }
            parIdx++;
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

        // once our parents are set up correctly, we have enough state to
        // generate the full key mapping in the groupBy input
        status = generateFullKeyMap();
        BailIfFailed(status);
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
                "Failed to create groupBy node %s: %s",
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

bool
OperatorHandlerGroupBy::useEvalContextOptimization(XdbMeta *srcMeta)
{
    if (input_->includeSrcTableSample || input_->icvMode) {
        return false;
    }

    // see if we can make use of the evalContext optimization. All of the
    // asts must pass the check in order to make it
    GroupEvalContext evalCtx;
    for (unsigned ii = 0; ii < numAsts_; ii++) {
        bool valid = evalCtx.initFromAst(&asts_[ii], srcMeta, 0);
        if (!valid) {
            return false;
        }
    }

    return true;
}

Status
OperatorHandlerGroupBy::createXdb(void *optimizerContext)
{
    Status status;
    DfFieldType outputTypes[numAsts_];
    Status groupAllStatus[input_->numEvals];
    bool addKeyAsImmediate = !input_->includeSrcTableSample;
    XdbMeta *xdbMeta;
    Scalar *aggResults[input_->numEvals];
    memZero(aggResults, sizeof(aggResults));
    NewKeyValueEntry *dstEntry = NULL;

    status = xdbMgr_->xdbGet(input_->srcTable.xdbId, NULL, &xdbMeta);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(xdbMeta != NULL);
    bool useEvalContext;
    useEvalContext = useEvalContextOptimization(xdbMeta);

    for (unsigned ii = 0; ii < numAsts_; ii++) {
        status =
            XcalarEval::get()->fixupTypeSensitiveFns(XcalarEvalAstGroupOperator,
                                                     asts_[ii].rootNode,
                                                     xdbMeta);
        BailIfFailed(status);

        if (input_->icvMode) {
            outputTypes[ii] = DfString;
        } else {
            if (useEvalContext) {
                GroupEvalContext evalCtx;
                evalCtx.initFromAst(&asts_[ii], xdbMeta, 0);

                outputTypes[ii] =
                    evalCtx.getOutputType(evalCtx.accType, evalCtx.argType);
            } else {
                outputTypes[ii] = XcalarEval::get()->getOutputType(&asts_[ii]);
            }
        }
    }

    if (input_->groupAll) {
        // GroupAll is really an aggregate
        for (unsigned ii = 0; ii < input_->numEvals; ii++) {
            XcalarApiAggregateInput aggInput;

            aggInput.srcTable = input_->srcTable;
            aggInput.dstTable = input_->dstTable;
            strlcpy(aggInput.evalStr,
                    input_->evalStrs[ii],
                    sizeof(aggInput.evalStr));

            aggResults[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
            BailIfNull(aggResults[ii]);

            groupAllStatus[ii] = Operators::get()->aggregate(dstGraph_,
                                                             &aggInput,
                                                             aggResults[ii],
                                                             false,
                                                             userId_);
            // For non-fatal failures drive on to perform the rest
            // of evals. When its time to insert the agg eval results
            // into the xdb(which is done below), check for this status
            if (XcalarEval::isFatalError(groupAllStatus[ii])) {
                status = groupAllStatus[ii];
                goto CommonExit;
            }
        }
    } else {
        if (xdbMeta->keyAttr[0].valueArrayIndex == NewTupleMeta::DfInvalidIdx) {
            // can't perform a groupBy on an invalid key
            status = StatusNoKey;
            goto CommonExit;
        }
    }

    if (!addKeyAsImmediate) {
        // this means we are including the srctable sample, just add outputs
        status = createSrcXdbCopy(input_->srcTable.xdbId,
                                  dstXdbId_,
                                  outputTypes,
                                  input_->newFieldNames,
                                  numAsts_,
                                  optimizerContext);
    } else if (!input_->groupAll) {
        // only create an xdb with key and outputs
        unsigned numKeys = xdbMeta->numKeys;
        const char *keyNames[numKeys];
        DfFieldType keyTypes[numKeys];
        int keyIndexes[numKeys];
        Ordering keyOrdering[numKeys];
        unsigned numImmediates = numKeys + numAsts_;
        const char *immediateNames[numImmediates];

        if (numImmediates > TupleMaxNumValuesPerRecord) {
            status = StatusFieldLimitExceeded;
            goto CommonExit;
        }

        // copy over src keys applying any key renames from input
        for (unsigned ii = 0; ii < numKeys; ii++) {
            // input keys are guarenteed to be ordered by generateFullKeyMap
            keyNames[ii] = input_->keys[ii].keyFieldName;
            keyTypes[ii] = xdbMeta->keyAttr[ii].type;
            keyOrdering[ii] = xdbMeta->keyAttr[ii].ordering;
        }

        NewTupleMeta tupMeta;
        tupMeta.setNumFields(numImmediates);
        // keys first
        for (unsigned ii = 0; ii < numKeys; ii++) {
            tupMeta.setFieldType(keyTypes[ii], ii);
            immediateNames[ii] = keyNames[ii];
            keyIndexes[ii] = ii;
        }

        // then outputs
        for (unsigned ii = 0; ii < numAsts_; ii++) {
            tupMeta.setFieldType(outputTypes[ii], ii + numKeys);
            immediateNames[ii + numKeys] = input_->newFieldNames[ii];
        }

        status = xdbMgr_->xdbCreate(dstXdbId_,
                                    numKeys,
                                    keyNames,
                                    keyTypes,
                                    keyIndexes,
                                    keyOrdering,
                                    &tupMeta,
                                    NULL,
                                    0,
                                    immediateNames,
                                    numImmediates,
                                    NULL,
                                    0,
                                    XdbGlobal,
                                    xdbMeta->dhtId);
    } else {
        assert(input_->groupAll);
        // output just the results
        unsigned numImmediates = numAsts_;
        const char *immediateNames[numImmediates];

        if (numImmediates > TupleMaxNumValuesPerRecord) {
            status = StatusFieldLimitExceeded;
            goto CommonExit;
        }

        NewTupleMeta tupMeta;
        tupMeta.setNumFields(numImmediates);
        for (size_t ii = 0; ii < numAsts_; ii++) {
            tupMeta.setFieldType(outputTypes[ii], ii);
            immediateNames[ii] = input_->newFieldNames[ii];
        }

        status = xdbMgr_->xdbCreate(dstXdbId_,
                                    DsDefaultDatasetKeyName,
                                    DfInt64,
                                    NewTupleMeta::DfInvalidIdx,
                                    &tupMeta,
                                    NULL,
                                    0,
                                    immediateNames,
                                    numImmediates,
                                    NULL,
                                    0,
                                    Random,
                                    XdbGlobal,
                                    XidMgr::XidSystemRandomDht);
    }

    BailIfFailedMsg(moduleName,
                    status,
                    "GroupBy(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

    if (input_->groupAll) {
        Xdb *dstXdb = NULL;
        XdbMeta *dstMeta = NULL;

        DfFieldValue key;
        // key is just the value 0
        key.int64Val = 0;

        status = xdbMgr_->xdbGet(input_->dstTable.xdbId, &dstXdb, &dstMeta);
        BailIfFailed(status);

        dstEntry =
            new (std::nothrow) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);
        BailIfNull(dstEntry);

        const NewTupleMeta *dstTupMeta;
        dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;

        for (unsigned ii = 0; ii < input_->numEvals; ii++) {
            DfFieldValue fieldValue;

            if (input_->icvMode && groupAllStatus[ii] != StatusOk) {
                // return value is status string
                const char *statusStr = strGetFromStatus(groupAllStatus[ii]);
                fieldValue.stringVal.strActual = statusStr;
                fieldValue.stringVal.strSize = strlen(statusStr) + 1;

                dstEntry->tuple_.set(ii,
                                     fieldValue,
                                     dstTupMeta->getFieldType(ii));
            } else if (!input_->icvMode && groupAllStatus[ii] == StatusOk) {
                if (dstTupMeta->getFieldType(ii) == DfScalarObj) {
                    fieldValue.scalarVal = aggResults[ii];
                } else {
                    status = aggResults[ii]->getValue(&fieldValue);
                    BailIfFailed(status);
                }
                dstEntry->tuple_.set(ii,
                                     fieldValue,
                                     dstTupMeta->getFieldType(ii));
            }
        }

        status = xdbMgr_->xdbInsertKvNoLock(dstXdb,
                                            0,
                                            &key,
                                            &dstEntry->tuple_,
                                            XdbInsertSlotHash);
        BailIfFailed(status);
    }

CommonExit:
    for (unsigned ii = 0; ii < input_->numEvals; ii++) {
        if (aggResults[ii] != NULL) {
            Scalar::freeScalar(aggResults[ii]);
            aggResults[ii] = NULL;
        }
    }
    if (dstEntry) {
        delete dstEntry;
        dstEntry = NULL;
    }

    return status;
}
