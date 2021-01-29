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
#include "libapis/OperatorHandlerFilter.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"
#include "parent/Parent.h"

OperatorHandlerFilter::OperatorHandlerFilter(XcalarApis api)
    : OperatorHandler(api),
      input_(NULL),
      numAggVariables_(0),
      numVariables_(0),
      variableNames_(NULL),
      astCreated_(false)
{
}

OperatorHandlerFilter::~OperatorHandlerFilter()
{
    destroyArg();
}

ApiHandler::Flags
OperatorHandlerFilter::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

void
OperatorHandlerFilter::destroyArg()
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
OperatorHandlerFilter::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerFilter::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.filterOutput.tableName;
}

Status
OperatorHandlerFilter::setArg(XcalarApiInput *input,
                              size_t inputSize,
                              bool parentNodeIdsToBeProvided)
{
    Status status;
    unsigned numUniqueVariables = 0;
    unsigned numVariables = 0;
    unsigned ii;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    // In case this is not the first time we're calling setArg();
    destroyArg();
    assert(!astCreated_);
    assert(variableNames_ == NULL);

    assert((uintptr_t) input == (uintptr_t) &input->filterInput);
    apiInput_ = input;
    input_ = &input->filterInput;
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
                "sizeof(*input_) = %lu bytes)",
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

    status = xcalarEval->parseEvalStr(input_->filterStr,
                                      XcalarFnTypeEval,
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

// The following is duplicate of OperatorHandlerMap::genSummaryFailureTab
// with some changes. The main difference being that in the map case, there
// might be multiple evals, but in filter it is strictly one eval string.
//
// Following routine summarizes the failures encountered by this filter
// operation, by running the following operations to generate a final
// table with the summarized failure information in a two-column schema
// ("failure-count", "failure-description"):
//
// NOTE: Step 0 occurs in this function; the rest in genSummaryFailureOneTab()
//
// 0. Map re-run in ICV mode (generating only the failed rows) -> table
// A
//
// Then call genSummaryFailureOneTab() which does following:
//
//  1. Index table A on "failure-description" column (for group-by) -> table B
//  2. Group-by "failure-description" to produce 'failure-count' col -> table C
//  3. Sort (in descending order) table C on 'failure-count' col -> table D
//
// Return ID of table D in "failureTableIdsOut" out-param
//

Status
OperatorHandlerFilter::genSummaryFailureTab(
    DagTypes::NodeId *failureTableIdsOut[XcalarApiMaxFailureEvals])
{
    Status status = StatusOk;
    Status status2 = StatusOk;
    char failMapTableNameOut[XcalarApiMaxTableNameLen + 1];
    // flags to be used in CommonExit block
    bool dropMapTable = false;

    DagTypes::NodeId failureTabId = DagTypes::InvalidDagNodeId;
    DagTypes::NamedInput namedInput;
    uint64_t rowCount = 0;
    bool dropFtab = false;
    char *newFieldTemp = NULL;
    char *evalTemp = NULL;
    size_t ret;

    // Since the filter encountered errors in some rows
    //
    // XXX: see stepThrouhNode which also sets
    // queryJob->currentRunningNodeId and
    // queryJob->currentRunningNodeName not doing this here - it'll
    // impact progress reporting so need to think about this

    // Step 0 -> Map with ICV flag on
    //           table input_->srcTable.tableName to yield
    //           failMapTableNameOut

    newFieldTemp =
        (char *) memAllocExt((XcalarApiMaxFieldNameLen + 1), moduleName);
    BailIfNull(newFieldTemp);

    ret = snprintf(newFieldTemp,
                   XcalarApiMaxFieldNameLen + 1,
                   "%s",
                   newFieldColName);

    if (ret >= (XcalarApiMaxFieldNameLen + 1)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "New Field column name \"%s\" is too long",
                newFieldColName);
        goto CommonExit;
    }

    evalTemp =
        (char *) memAllocExt((XcalarApiMaxEvalStringLen + 1), moduleName);
    BailIfNull(evalTemp);

    ret = snprintf(evalTemp,
                   XcalarApiMaxEvalStringLen + 1,
                   "%s",
                   input_->filterStr);

    if (ret >= (XcalarApiMaxEvalStringLen + 1)) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "eval \"%s\" is too long",
                input_->filterStr);
        goto CommonExit;
    }

    // filter doesn't support ICV mode, but map does. Since the results from a
    // map would be identical to that from a filter, we can use a map to
    // generate failures table
    status = Operators::get()->mapApiInternal(userId_,
                                              (sessionInfo_ != NULL)
                                                  ? sessionInfo_->sessionName
                                                  : NULL,
                                              dstGraph_,
                                              input_->srcTable.tableName,
                                              1,
                                              (const char **) &evalTemp,
                                              (const char **) &newFieldTemp,
                                              true,   // icvMode is on
                                              false,  // keep current txn
                                              &failureTabId,
                                              failMapTableNameOut);

    BailIfFailed(status);
    dropMapTable = true;

    status = Operators::get()
                 ->genSummaryFailureOneTab(failMapTableNameOut,
                                           0,
                                           &failureTabId,
                                           newFieldTemp,
                                           (sessionInfo_ != NULL)
                                               ? sessionInfo_->sessionName
                                               : NULL,
                                           userId_,
                                           dstGraph_);
    if (status == StatusOk) {
        assert(failureTabId != DagTypes::InvalidDagNodeId);

        namedInput.isTable = true;
        namedInput.nodeId = failureTabId;

        status = Operators::get()->getRowCount(&namedInput,
                                               dstGraph_->getId(),
                                               &rowCount,
                                               userId_);

        if (rowCount > 0) {
            *failureTableIdsOut[0] = failureTabId;
        } else {
            // empty failure table - drop it
            dropFtab = true;
        }
    } else {
        // this assert is just to catch all status codes for which
        // failureTabId may be valid (so far, Canceled, and
        // NoXdbPageBcMem).  There's not much consequence in non-debug - if
        // there's a valid failureTabId, it must be dropped since status is
        // not OK.
        assert(status == StatusCanceled || status == StatusNoXdbPageBcMem ||
               failureTabId == DagTypes::InvalidDagNodeId);
        if (failureTabId == DagTypes::InvalidDagNodeId) {
            xSyslog(moduleName,
                    XlogErr,
                    "genSummaryFailureTab: failed to generate failure table "
                    "for eval string: %s",
                    strGetFromStatus(status));
        } else {
            dropFtab = true;
            xSyslog(moduleName,
                    XlogErr,
                    "genSummaryFailureTab: operation failed "
                    "for eval string: %s",
                    strGetFromStatus(status));
        }
    }
    if (dropFtab) {
        Status status2;
        char failureTabName[XcalarApiMaxTableNameLen + 1];

        snprintf(failureTabName,
                 sizeof(failureTabName),
                 "<No Failure Table Name>");

        status2 = dstGraph_->getDagNodeName(failureTabId,
                                            failureTabName,
                                            sizeof(failureTabName));

        if (status2 == StatusOk) {
            status2 =
                dstGraph_->dropNode(failureTabName, SrcTable, NULL, NULL, true);
        }
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "genSummaryFailureTab: failed to drop table "
                    "\"%s\": %s",
                    failureTabName,
                    strGetFromStatus(status2));
        }
    }

CommonExit:
    if (dropMapTable) {
        status2 = dstGraph_->dropNode(failMapTableNameOut,
                                      SrcTable,
                                      NULL,
                                      NULL,
                                      true);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop node %s during map failure "
                    "processing",
                    failMapTableNameOut);
        }
    }
    if (newFieldTemp != NULL) {
        memFree(newFieldTemp);
    }
    if (evalTemp != NULL) {
        memFree(evalTemp);
    }
    return (status);
}

Status
OperatorHandlerFilter::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *filterOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    size_t size = sizeof(input_->filterStr);
    unsigned cur = 0;
    unsigned parIdx = 0;
    XcalarApiOutput *getOpStatsOutput = NULL;
    size_t getOpStatsOutputSize = 0;
    OpEvalErrorStats *opEvalErrs;
    uint64_t udfFailures = 0;
    uint64_t xdfFailures = 0;
    uint64_t totalFailures = 0;

    assert(failureTableIdOut != NULL);
    // There will only be one faiure table since filter only supports a single
    // eval. But, we will just init all anyways.
    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        *failureTableIdOut[ii] = DagTypes::InvalidDagNodeId;
    }

    outputSize = XcalarApiSizeOfOutput(*filterOutput);
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

    filterOutput = &output->outputResult.filterOutput;
    assert((uintptr_t) filterOutput == (uintptr_t) &output->outputResult);

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
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting filter",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status = XcalarEval::get()->reverseParseAst(XcalarEvalAstOperator,
                                                ast_.rootNode,
                                                input_->filterStr,
                                                cur,
                                                size,
                                                XcalarEval::NoFlags);

    BailIfFailedMsg(moduleName,
                    status,
                    "Filter(%lu) failed reverseParseAst %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    status =
        Operators::get()->filter(dstGraph_, input_, optimizerContext, userId_);

    BailIfFailed(status);

    status = dstGraph_->getDagNodeState(dstNodeId_, &state);
    BailIfFailed(status);

    status = dstGraph_->getOpStats(dstNodeId_,
                                   state,
                                   &getOpStatsOutput,
                                   &getOpStatsOutputSize);
    BailIfFailed(status);

    assert(getOpStatsOutput->outputResult.opStatsOutput.api == XcalarApiFilter);

    opEvalErrs = &getOpStatsOutput->outputResult.opStatsOutput.opDetails
                      .errorStats.evalErrorStats;

    udfFailures = opEvalErrs->evalUdfErrorStats.numEvalUdfError;
    xdfFailures = opEvalErrs->evalXdfErrorStats.numTotal;
    totalFailures = udfFailures + xdfFailures;

    if (totalFailures != 0) {
        if (txn_.mode_ == Txn::Mode::LRQ &&
            !XcalarConfig::get()->enableFailureSummaryReport_) {
            xSyslog(moduleName,
                    XlogErr,
                    "Filter encountered %lu Scalar Function failures! Not "
                    "Attempting "
                    "to summarize these failures in optimized and non-debug "
                    "mode",
                    totalFailures);
            // to document that this is non-fatal in both cases
            status = StatusOk;
        } else {
            status = genSummaryFailureTab(failureTableIdOut);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Filter encountered %lu Scalar Function failures! "
                        "Attempt to "
                        "summarize these failures failed due to \"%s\"",
                        totalFailures,
                        strGetFromStatus(status));
                status = StatusOk;  // non-fatal
            }
        }
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       filterOutput->tableName,
                                       sizeof(filterOutput->tableName));
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
OperatorHandlerFilter::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    uint64_t numParents = 0;
    Status status;
    DagTypes::NodeId *parentNodeIds = NULL;
    unsigned ii, parIdx = 0;
    DagTypes::DagId *parentGraphIds = NULL;
    XcalarApiUdfContainer *sessionContainers = NULL;

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
                    "Failed getParentNodes source \"%s\" numParents %lu: %s",
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
OperatorHandlerFilter::createDagNode(DagTypes::NodeId *dstNodeIdOut,
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
                "Failed to create filter node %s: %s",
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
                        "Failed to drop and change dagNode node (%lu) state to "
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

Status
OperatorHandlerFilter::createXdb(void *optimizerContext)
{
    Status status;
    XdbMeta *srcMeta;
    status = XdbMgr::get()->xdbGet(input_->srcTable.xdbId, NULL, &srcMeta);
    assert(status == StatusOk);

    status = XcalarEval::get()->fixupTypeSensitiveFns(XcalarEvalAstOperator,
                                                      ast_.rootNode,
                                                      srcMeta);
    BailIfFailed(status);

    status = createSrcXdbCopy(input_->srcTable.xdbId,
                              dstXdbId_,
                              NULL,
                              NULL,
                              0,
                              optimizerContext);

    BailIfFailedMsg(moduleName,
                    status,
                    "Filter(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

CommonExit:
    return status;
}
