// Copyright 2016- 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisRecv.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerMap.h"
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
#include "queryeval/QueryEvaluate.h"
#include "table/ResultSet.h"

OperatorHandlerMap::OperatorHandlerMap(XcalarApis api)
    : OperatorHandler(api),
      input_(NULL),
      numAggVariables_(0),
      astsCreated_(NULL)
{
}

OperatorHandlerMap::~OperatorHandlerMap()
{
    destroyArg();
}

ApiHandler::Flags
OperatorHandlerMap::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

void
OperatorHandlerMap::destroyArg()
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
OperatorHandlerMap::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerMap::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.mapOutput.tableName;
}

Status
OperatorHandlerMap::setArg(XcalarApiInput *input,
                           size_t inputSize,
                           bool parentNodeIdsToBeProvided)
{
    Status status;
    unsigned numVariables;
    unsigned ii, jj;
    XcalarEval *xcalarEval = XcalarEval::get();
    const char ***variableNames = NULL;
    unsigned *numUniqueVariables = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    // In case this is not the first time we're calling setArg();
    destroyArg();
    assert(variableNames == NULL);

    assert((uintptr_t) input == (uintptr_t) &input->mapInput);
    apiInput_ = input;
    input_ = &input->mapInput;
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

    xcalarApiDeserializeMapInput(input_);

    if (input_->numEvals == 0) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Must specify at least one eval string");
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
                                          XcalarFnTypeEval,
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

        if (!xcalarEval->isValidFieldName(input_->newFieldNames[ii])) {
            xSyslog(moduleName,
                    XlogErr,
                    "New field name (%s) is not valid",
                    input_->newFieldNames[ii]);
            status = StatusInval;
            goto CommonExit;
        }
    }

    status = StatusOk;
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

//
// Following routine summarizes the failures encountered by this map
// operation, by running the following operations to generate a final
// table with the summarized failure information in a two-column schema
// ("failure-count", "failure-description"):
//
// NOTE: Step 0 occurs in this function; the rest in genSummaryFailureOneTab()
//
// 0. Map re-run in ICV mode (generating only the failed rows) -> table
// A
//
// For each eval string, call genSummaryFailureOneTab() which does following:
//
//  1. Index table A on "failure-description" column (for group-by) -> table B
//  2. Group-by "failure-description" to produce 'failure-count' col -> table C
//  3. Sort (in descending order) table C on 'failure-count' col -> table D
//
// Return ID of table D in "failureTableIdsOut" out-param
//

Status
OperatorHandlerMap::genSummaryFailureTabs(
    DagTypes::NodeId *failureTableIdsOut[XcalarApiMaxFailureEvals])
{
    Status status = StatusOk;
    Status status2 = StatusOk;
    char failMapTableNameOut[XcalarApiMaxTableNameLen + 1];
    // flags to be used in CommonExit block
    bool dropMapTable = false;
    uint32_t fTabIdx = 0;
    char *sessionName = NULL;

    // Since the map encountered errors in some rows
    //
    // XXX: see stepThrouhNode which also sets
    // queryJob->currentRunningNodeId and
    // queryJob->currentRunningNodeName not doing this here - it'll
    // impact progress reporting so need to think about this

    for (int jj = 0; jj < XcalarApiMaxFailureEvals; jj++) {
        // not strictly needed since i/f requires caller to init
        *failureTableIdsOut[jj] = DagTypes::InvalidDagNodeId;
    }

    // Step 0 -> MAP with ICV flag on
    //           table input_->srcTable.tableName to yield
    //           failMapTableNameOut

    if (sessionInfo_ != NULL) {
        sessionName = sessionInfo_->sessionName;
    }
    status =
        Operators::get()->mapApiInternal(userId_,
                                         (sessionInfo_ != NULL)
                                             ? sessionInfo_->sessionName
                                             : NULL,
                                         dstGraph_,
                                         input_->srcTable.tableName,
                                         input_->numEvals,
                                         (const char **) input_->evalStrs,
                                         (const char **) input_->newFieldNames,
                                         true,   // icvMode is on
                                         false,  // keep current txn
                                         NULL,   // dstNodeIdOut
                                         failMapTableNameOut);
    BailIfFailed(status);
    dropMapTable = true;

    for (uint32_t ii = 0; ii < input_->numEvals; ii++) {
        DagTypes::NodeId failureTabId = DagTypes::InvalidDagNodeId;
        DagTypes::NamedInput namedInput;
        uint64_t rowCount = 0;
        bool dropFtab = false;

        status = Operators::get()
                     ->genSummaryFailureOneTab(failMapTableNameOut,
                                               ii,
                                               &failureTabId,
                                               input_->newFieldNames[ii],
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
                if (fTabIdx < XcalarApiMaxFailureEvals) {
                    *failureTableIdsOut[fTabIdx++] = failureTabId;
                } else {
                    // failures to be reported but no room - drop the table
                    dropFtab = true;
                }
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
                        "genSummaryFailureTabs: failed to generate failure "
                        "table for eval string number %d: %s",
                        ii,
                        strGetFromStatus(status));
            } else {
                dropFtab = true;
                xSyslog(moduleName,
                        XlogErr,
                        "genSummaryFailureTabs: operation canceled "
                        "for eval string number %d: %s",
                        ii,
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
                status2 = dstGraph_->dropNode(failureTabName,
                                              SrcTable,
                                              NULL,
                                              NULL,
                                              true);
            }
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "genSummaryFailureTabs: failed to drop table "
                        "\"%s\": %s",
                        failureTabName,
                        strGetFromStatus(status2));
            }
            if (rowCount != 0) {
                break;  // dropped a non-empty failure table due to
                        // exceeding reporting limit - so no point
                        // in continuing - all future tables will need
                        // to be dropped anyway - so just quit
            }
        }

        // reset for next eval in iter
        status = StatusOk;  // failure to get a failtab for an eval isn't fatal
        failureTabId = DagTypes::InvalidDagNodeId;
        rowCount = 0;
        dropFtab = false;
    }

CommonExit:
    if (dropMapTable == true) {
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
    return status;
}

Status
OperatorHandlerMap::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTabIdsOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *mapOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    DgDagState dstNodeState = DgDagStateError;
    XcalarApiOutput *getOpStatsOutput = NULL;
    size_t getOpStatsOutputSize = 0;
    OpEvalErrorStats *opEvalErrs;
    uint64_t udfFailures = 0;
    unsigned parIdx = 0;
    uint64_t xdfFailures = 0;
    uint64_t totalFailures = 0;

    assert(failureTabIdsOut != NULL);
    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        *failureTabIdsOut[ii] = DagTypes::InvalidDagNodeId;
    }
    outputSize = XcalarApiSizeOfOutput(*mapOutput);
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

    mapOutput = &output->outputResult.mapOutput;
    assert((uintptr_t) mapOutput == (uintptr_t) &output->outputResult);

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
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting "
                "map",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                strGetFromDgDagState(state));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < numAsts_; ii++) {
        unsigned cur = 0;
        size_t size = XcalarApiMaxEvalStringLen + 1;

        status = XcalarEval::get()->reverseParseAst(XcalarEvalAstOperator,
                                                    asts_[ii].rootNode,
                                                    input_->evalStrs[ii],
                                                    cur,
                                                    size,
                                                    XcalarEval::NoFlags);
        BailIfFailedMsg(moduleName,
                        status,
                        "Map(%lu) failed reverseParseAst %s",
                        input_->dstTable.tableId,
                        strGetFromStatus(status));
    }

    status = Operators::get()->map(dstGraph_,
                                   input_,
                                   inputSize_,
                                   optimizerContext,
                                   userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeState(dstNodeId_, &dstNodeState);
    BailIfFailed(status);

    status = dstGraph_->getOpStats(dstNodeId_,
                                   dstNodeState,
                                   &getOpStatsOutput,
                                   &getOpStatsOutputSize);
    BailIfFailed(status);

    assert(getOpStatsOutput->outputResult.opStatsOutput.api == XcalarApiMap);

    opEvalErrs = &getOpStatsOutput->outputResult.opStatsOutput.opDetails
                      .errorStats.evalErrorStats;

    udfFailures = opEvalErrs->evalUdfErrorStats.numEvalUdfError;
    xdfFailures = opEvalErrs->evalXdfErrorStats.numTotal;
    totalFailures = udfFailures + xdfFailures;

    if (input_->icvMode == false && totalFailures != 0) {
        if (txn_.mode_ == Txn::Mode::LRQ &&
            !XcalarConfig::get()->enableFailureSummaryReport_) {
            xSyslog(moduleName,
                    XlogErr,
                    "Map encountered %lu Scalar Function failures! "
                    "Not Attempting to summarize these failures in optimized "
                    "and non-debug mode",
                    totalFailures);
            // to document that this is non-fatal in both cases
            status = StatusOk;
        } else {
            status = genSummaryFailureTabs(failureTabIdsOut);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Map encountered %lu Scalar Function failures! "
                        "Attempt to summarize these failures failed due to "
                        "\"%s\"",
                        totalFailures,
                        strGetFromStatus(status));
                status = StatusOk;  // non-fatal
            }
        }
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       mapOutput->tableName,
                                       sizeof(mapOutput->tableName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get name of table created: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:

    if (getOpStatsOutput != NULL) {
        memFree(getOpStatsOutput);
        getOpStatsOutput = NULL;
    }
    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
OperatorHandlerMap::getParentNodes(uint64_t *numParentsOut,
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
        assert(parIdx < numParents);
        status = getSourceDagNode(&aggVariableNames_[ii][1],
                                  dstGraph_,
                                  &sessionContainers[parIdx],
                                  &parentGraphIds[parIdx],
                                  &parentNodeIds[parIdx]);
        if (status != StatusOk) {
            // Working in WorkspaceGraph should have all aggregates
            if (dstGraphType_ == DagTypes::WorkspaceGraph) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving variable \"%s\": %s",
                        aggVariableNames_[ii],
                        strGetFromStatus(status));
                goto CommonExit;
            } else {
                // QueryGraph may not have all the aggregate
                // It's ok to continue
                status = StatusOk;
                continue;
            }
        }
        parIdx++;
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
OperatorHandlerMap::createDagNode(DagTypes::NodeId *dstNodeIdOut,
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
        assert(numParents == 1 + numAggVariables_);
        assert(parentNodeIds != NULL);

        if (numParents != 1 + numAggVariables_) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid number of parents provided: %lu. "
                    "Required: %u",
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
                "Failed to create map node %s: %s",
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
                        "Failed to drop and change dagNode (%lu) state "
                        "to "
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
OperatorHandlerMap::createXdb(void *optimizerContext)
{
    Status status;
    DfFieldType outputTypes[numAsts_];

    XdbMeta *srcMeta;
    status = XdbMgr::get()->xdbGet(input_->srcTable.xdbId, NULL, &srcMeta);
    assert(status == StatusOk);

    for (unsigned ii = 0; ii < numAsts_; ii++) {
        status = XcalarEval::get()->fixupTypeSensitiveFns(XcalarEvalAstOperator,
                                                          asts_[ii].rootNode,
                                                          srcMeta);
        BailIfFailed(status);

        if (input_->icvMode) {
            outputTypes[ii] = DfString;
        } else {
            outputTypes[ii] = XcalarEval::get()->getOutputType(&asts_[ii]);
        }

        if (!containsUdf_) {
            containsUdf_ = XcalarEval::get()->containsUdf(asts_[ii].rootNode);
        }
    }

    status = createSrcXdbCopy(input_->srcTable.xdbId,
                              dstXdbId_,
                              outputTypes,
                              input_->newFieldNames,
                              numAsts_,
                              optimizerContext);
    BailIfFailedMsg(moduleName,
                    status,
                    "Map(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

CommonExit:
    return status;
}
