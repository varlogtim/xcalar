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
#include "libapis/OperatorHandlerUnion.h"
#include "libapis/WorkItem.h"
#include "util/MemTrack.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/DhtTypes.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "operators/XcalarEval.h"
#include "strings/String.h"

OperatorHandlerUnion::OperatorHandlerUnion(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

OperatorHandlerUnion::~OperatorHandlerUnion()
{
    if (newApiInput_) {
        memFree(newApiInput_);
        newApiInput_ = NULL;
    }
}

ApiHandler::Flags
OperatorHandlerUnion::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerUnion::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerUnion::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.unionOutput.tableName;
}

Status
OperatorHandlerUnion::generateFullRenameMaps()
{
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status;
    bool parentRefs[input_->numSrcTables];
    memZero(parentRefs, sizeof(parentRefs));
    TableNsMgr *tnsMgr = TableNsMgr::get();
    TableNsMgr::TableHandleTrack *handleTrack = NULL;
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();

    // shouldn't be messing with the rename map in LRQ mode
    assert(Txn::currentTxn().mode_ == Txn::Mode::NonLRQ);

    if (dstGraph_->getDagType() == DagTypes::QueryGraph) {
        // don't have enough info to generate rename maps in a queryGraph
        return StatusOk;
    }

    const char *srcTableNames[input_->numSrcTables];
    XcalarApiRenameMap *renameMaps[input_->numSrcTables];
    memZero(renameMaps, sizeof(renameMaps));

    handleTrack =
        new (std::nothrow) TableNsMgr::TableHandleTrack[input_->numSrcTables];
    BailIfNull(handleTrack);

    for (unsigned tblIdx = 0; tblIdx < input_->numSrcTables; tblIdx++) {
        DgDagState dgState;
        status = parentGraphs_[tblIdx]
                     ->getDagNodeStateAndRef(input_->srcTables[tblIdx].tableId,
                                             &dgState);
        BailIfFailed(status);
        parentRefs[tblIdx] = true;

        if (dgState != DgDagStateReady) {
            status = StatusDgDagNodeNotReady;
            goto CommonExit;
        }

        status = parentGraphs_[tblIdx]
                     ->getTableIdFromNodeId(input_->srcTables[tblIdx].tableId,
                                            &handleTrack[tblIdx].tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        input_->srcTables[tblIdx].tableId,
                        strGetFromStatus(status));

        status = tnsMgr->openHandleToNs(sessionContainer,
                                        handleTrack[tblIdx].tableId,
                                        LibNsTypes::ReaderShared,
                                        &handleTrack[tblIdx].tableHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        handleTrack[tblIdx].tableId,
                        strGetFromStatus(status));
        handleTrack[tblIdx].tableHandleValid = true;

        XdbMeta *xdbMeta;
        status =
            xdbMgr->xdbGet(input_->srcTables[tblIdx].xdbId, NULL, &xdbMeta);
        BailIfFailed(status);

        if (input_->renameMapSizes[tblIdx] == 0) {
            // generate a new rename map containing all the columns
            unsigned numCols =
                xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();

            renameMaps[tblIdx] = new (std::nothrow) XcalarApiRenameMap[numCols];
            BailIfNull(renameMaps[tblIdx]);

            for (unsigned jj = 0; jj < numCols; jj++) {
                char *name = xdbMeta->kvNamedMeta.valueNames_[tblIdx];
                status = strStrlcpy(renameMaps[tblIdx][jj].oldName,
                                    name,
                                    sizeof(renameMaps[tblIdx][jj].oldName));
                BailIfFailed(status);
                status = strStrlcpy(renameMaps[tblIdx][jj].newName,
                                    name,
                                    sizeof(renameMaps[tblIdx][jj].newName));
                BailIfFailed(status);
                renameMaps[tblIdx][jj].type =
                    xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(tblIdx);
                renameMaps[tblIdx][jj].isKey = false;
            }

            input_->renameMapSizes[tblIdx] = numCols;
        } else {
            // reuse the existing one
            renameMaps[tblIdx] = new (std::nothrow)
                XcalarApiRenameMap[input_->renameMapSizes[tblIdx]];
            BailIfNull(renameMaps[tblIdx]);

            memcpy(renameMaps[tblIdx],
                   input_->renameMap[tblIdx],
                   input_->renameMapSizes[tblIdx] *
                       sizeof(*renameMaps[tblIdx]));
        }

        // populate key bits
        for (unsigned jj = 0; jj < input_->renameMapSizes[tblIdx]; jj++) {
            for (unsigned kk = 0; kk < xdbMeta->numKeys; kk++) {
                if (strcmp(renameMaps[tblIdx][jj].oldName,
                           xdbMeta->keyAttr[kk].name) == 0) {
                    renameMaps[tblIdx][jj].isKey = true;
                    break;
                }
            }
        }

        srcTableNames[tblIdx] = input_->srcTables[tblIdx].tableName;
    }

    {
        XcalarWorkItem *workItem =
            xcalarApiMakeUnionWorkItem(input_->numSrcTables,
                                       srcTableNames,
                                       input_->dstTable.tableName,
                                       input_->renameMapSizes,
                                       renameMaps,
                                       input_->dedup,
                                       input_->unionType);
        BailIfNull(workItem);

        newApiInput_ = workItem->input;
        inputSize_ = workItem->inputSize;
        for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
            newApiInput_->unionInput.srcTables[ii] = input_->srcTables[ii];
        }
        newApiInput_->unionInput.dstTable = input_->dstTable;

        apiInput_ = newApiInput_;
        input_ = &newApiInput_->unionInput;

        memFree(workItem);
    }

CommonExit:
    for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
        if (parentRefs[ii]) {
            parentGraphs_[ii]->putDagNodeRefById(input_->srcTables[ii].tableId);
            parentRefs[ii] = false;
        }
        if (handleTrack && handleTrack[ii].tableHandleValid) {
            tnsMgr->closeHandleToNs(&handleTrack[ii].tableHandle);
            handleTrack[ii].tableHandleValid = false;
        }
    }

    if (handleTrack) {
        delete[] handleTrack;
        handleTrack = NULL;
    }

    if (status != StatusOk) {
        if (newApiInput_) {
            memFree(newApiInput_);
        }
        newApiInput_ = NULL;
    }
    for (unsigned tblIdx = 0; tblIdx < input_->numSrcTables; tblIdx++) {
        if (renameMaps[tblIdx]) {
            delete[] renameMaps[tblIdx];
            renameMaps[tblIdx] = NULL;
        }
    }

    return status;
}

Status
OperatorHandlerUnion::setArg(XcalarApiInput *input,
                             size_t inputSize,
                             bool parentNodeIdsToBeProvided)
{
    Status status;
    size_t expectedSize;
    bool foundInvalid;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->unionInput);
    apiInput_ = input;
    input_ = &input->unionInput;
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

    xcalarApiDeserializeUnionInput(input_);

    // Do some sanity checks
    if (!xcalarEval->isValidTableName(input_->dstTable.tableName)) {
        xSyslog(moduleName,
                XlogErr,
                "dstTable name (%s) is not valid",
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

    // there should be at least one source table
    if (input_->numSrcTables < 1) {
        xSyslog(moduleName,
                XlogErr,
                "There should be at least one source table");
        status = StatusInval;
        goto CommonExit;
    }

    foundInvalid = false;
    expectedSize = sizeof(*input_);

    // We'll check all the new column names even if a bad one is found
    for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
        expectedSize += sizeof(*input_->srcTables);
        expectedSize += sizeof(*input_->renameMapSizes);
        expectedSize += sizeof(*input_->renameMap);

        expectedSize +=
            sizeof(*input_->renameMap[ii]) * input_->renameMapSizes[ii];

        for (unsigned jj = 0; jj < input_->renameMapSizes[ii]; jj++) {
            char *newName = input_->renameMap[ii][jj].newName;

            if (!xcalarEval->isValidFieldName(newName)) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "New field name (%s) is invalid",
                              newName);
                foundInvalid = true;
            }

            // make sure there are no duplicate dstNames
            for (unsigned kk = jj + 1; kk < input_->renameMapSizes[ii]; kk++) {
                if (strcmp(newName, input_->renameMap[ii][kk].newName) == 0) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "New field name (%s) is duplicated in "
                                  "table %s",
                                  newName,
                                  input_->srcTables[ii].tableName);
                    foundInvalid = true;
                }
            }
        }
    }
    if (foundInvalid) {
        status = StatusInval;
        goto CommonExit;
    }

    if (expectedSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedSize = %lu bytes)",
                inputSize,
                expectedSize);
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
OperatorHandlerUnion::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *unionOutput = NULL;
    Status status;
    DgDagState dagState = DgDagStateError;
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(*unionOutput);
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

    unionOutput = &output->outputResult.unionOutput;
    assert((uintptr_t) unionOutput == (uintptr_t) &output->outputResult);

    for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
        status =
            parentGraphs_[ii]->getDagNodeState(input_->srcTables[ii].tableId,
                                               &dagState);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get state of dagNode \"%s\" (%lu): %s",
                    input_->srcTables[ii].tableName,
                    input_->srcTables[ii].tableId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (dagState != DgDagStateReady) {
            xSyslog(moduleName,
                    XlogErr,
                    "Parent dag node \"%s\" (%lu) in state: %s. Aborting union",
                    input_->srcTables[ii].tableName,
                    input_->srcTables[ii].tableId,
                    strGetFromDgDagState(dagState));
            status = StatusDgDagNodeError;
            goto CommonExit;
        }
    }

    status = Operators::get()->unionOp(dstGraph_,
                                       input_,
                                       inputSize_,
                                       optimizerContext,
                                       userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       unionOutput->tableName,
                                       sizeof(unionOutput->tableName));
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
OperatorHandlerUnion::getParentNodes(
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

    assert(!parentNodeIdsToBeProvided_);
    numParents = input_->numSrcTables;

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

    for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
        status = getSourceDagNode(input_->srcTables[ii].tableName,
                                  dstGraph_,
                                  &sessionContainers[ii],
                                  &parentGraphIds[ii],
                                  &parentNodeIds[ii]);
        BailIfFailedMsg(moduleName,
                        status,
                        "fail to retrieve dagNodeId for \"%s\", numParents "
                        "%lu: %s",
                        input_->srcTables[ii].tableName,
                        numParents,
                        strGetFromStatus(status));
    }
    status = StatusOk;
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
OperatorHandlerUnion::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                    DagTypes::GraphType srcGraphType,
                                    const char *nodeName,
                                    uint64_t numParents,
                                    Dag **parentGraphs,
                                    DagTypes::NodeId *parentNodeIds)
{
    Status status;

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    if (!parentNodeIdsToBeProvided_ &&
        dstGraphType_ == DagTypes::WorkspaceGraph) {
        for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
            // Then we better make sure the names are valid
            status = parentGraphs[ii]
                         ->getDagNodeId(input_->srcTables[ii].tableName,
                                        Dag::TableScope::FullyQualOrLocal,
                                        &input_->srcTables[ii].tableId);
            if (status == StatusDagNodeNotFound) {
                status = StatusTableNotFound;
            }

            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving dagNodeId for \"%s\": %s",
                        input_->srcTables[ii].tableName,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            status =
                parentGraphs[ii]->getXdbId(input_->srcTables[ii].tableName,
                                           Dag::TableScope::FullyQualOrLocal,
                                           &input_->srcTables[ii].xdbId);
            if (status == StatusDagNodeNotFound) {
                status = StatusTableNotFound;
            }

            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving tableId for \"%s\": %s",
                        input_->srcTables[ii].tableName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }

        status = generateFullRenameMaps();
        BailIfFailed(status);
    }

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == input_->numSrcTables);
        assert(parentNodeIds != NULL);

        if (numParents != input_->numSrcTables) {
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

        for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
            input_->srcTables[ii].tableId = parentNodeIds[ii];
            input_->srcTables[ii].xdbId = parentGraphs[ii]->getXdbIdFromNodeId(
                input_->srcTables[ii].tableId);
        }
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
                "Failed to create union node %s: %s",
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
OperatorHandlerUnion::createXdb(void *optimizerContext)
{
    Status status;
    unsigned numImmediates = 0;
    DhtId dhtId;
    NewTupleMeta tupMeta;
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numKeys;

    XdbMeta *srcMeta;

    status = xdbMgr->xdbGet(input_->srcTables[0].xdbId, NULL, &srcMeta);
    assert(status == StatusOk);

    numKeys = srcMeta->numKeys;

    const char *immediateNames[TupleMaxNumValuesPerRecord];
    DfFieldType immediateTypes[TupleMaxNumValuesPerRecord];
    const char *valueNames[TupleMaxNumValuesPerRecord];

    DfFieldType keyTypes[srcMeta->numKeys];
    const char *keyNames[srcMeta->numKeys];
    int keyIndexes[srcMeta->numKeys];
    Ordering keyOrdering[srcMeta->numKeys];

    if (input_->dedup || input_->unionType != UnionStandard) {
        dhtId = srcMeta->dhtId;
        for (unsigned ii = 0; ii < numKeys; ii++) {
            keyTypes[ii] = srcMeta->keyAttr[ii].type;
            keyIndexes[ii] = srcMeta->keyAttr[ii].valueArrayIndex;
            keyNames[ii] = srcMeta->keyAttr[ii].name;
            keyOrdering[ii] = srcMeta->keyAttr[ii].ordering;

            // check if key has been renamed
            for (unsigned jj = 0; jj < input_->renameMapSizes[0]; jj++) {
                if (strcmp(keyNames[ii], input_->renameMap[0][jj].oldName) ==
                    0) {
                    keyNames[ii] = input_->renameMap[0][jj].newName;
                    break;
                }
            }
        }
    } else {
        status = DhtMgr::get()->dhtGetDhtId(DhtMgr::DhtSystemRandomDht, &dhtId);
        assert(status == StatusOk);

        keyTypes[0] = DfInt64;
        keyNames[0] = DsDefaultDatasetKeyName;
        keyIndexes[0] = NewTupleMeta::DfInvalidIdx;
        numKeys = 1;
        keyOrdering[0] = Random;
    }

    for (unsigned ii = 0; ii < input_->numSrcTables; ii++) {
        const NewTupleMeta *srcTupMeta;
        status = xdbMgr->xdbGet(input_->srcTables[ii].xdbId, NULL, &srcMeta);
        assert(status == StatusOk);

        srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;

        if (input_->dedup || input_->unionType != UnionStandard) {
            if (numKeys != srcMeta->numKeys) {
                status = StatusUnionTypeMismatch;
                goto CommonExit;
            }

            for (unsigned key = 0; key < numKeys; key++) {
                // need to do some key validation
                assert(keyTypes[key] != DfScalarObj);
                assert(srcMeta->keyAttr[key].type != DfScalarObj);
                if (keyTypes[key] != DfUnknown &&
                    srcMeta->keyAttr[key].type != DfUnknown &&
                    keyTypes[key] != srcMeta->keyAttr[key].type) {
                    status = StatusUnionTypeMismatch;
                    goto CommonExit;
                }

                if (srcMeta->keyAttr[key].valueArrayIndex ==
                    NewTupleMeta::DfInvalidIdx) {
                    status = StatusNoKey;
                    goto CommonExit;
                }

                // key must be supplied in the renameMap
                unsigned jj;
                for (jj = 0; jj < input_->renameMapSizes[ii]; jj++) {
                    if (strcmp(srcMeta->keyAttr[key].name,
                               input_->renameMap[ii][jj].oldName) == 0) {
                        break;
                    }
                }

                if (jj == input_->renameMapSizes[ii]) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "key %s is missing from input columns for "
                                  "table %s",
                                  srcMeta->keyAttr[key].name,
                                  input_->srcTables[ii].tableName);
                    status = StatusInval;
                    goto CommonExit;
                }
            }

            if (dhtId != srcMeta->dhtId) {
                status = StatusUnionDhtMismatch;
                goto CommonExit;
            }
        }

        // retrieve value names and types
        for (unsigned jj = 0; jj < input_->renameMapSizes[ii]; jj++) {
            for (unsigned kk = 0; kk < srcTupMeta->getNumFields(); kk++) {
                if (numImmediates >= TupleMaxNumValuesPerRecord) {
                    status = StatusFieldLimitExceeded;
                    goto CommonExit;
                }

                DfFieldType fieldType = srcTupMeta->getFieldType(kk);

                if (strcmp(srcMeta->kvNamedMeta.valueNames_[kk],
                           input_->renameMap[ii][jj].oldName) == 0) {
                    if (input_->renameMap[ii][jj].type == DfFatptr &&
                        fieldType == DfFatptr) {
                        status = StatusInval;
                        xSyslogTxnBuf(moduleName,
                                      XlogErr,
                                      "field %s cannot have type %s",
                                      input_->renameMap[ii][jj].oldName,
                                      strGetFromDfFieldType(fieldType));
                        goto CommonExit;
                    } else if (input_->renameMap[ii][jj].type != DfFatptr &&
                               fieldType != DfFatptr) {
                        unsigned imm;
                        for (imm = 0; imm < numImmediates; imm++) {
                            if (strcmp(input_->renameMap[ii][jj].newName,
                                       immediateNames[imm]) == 0) {
                                if (immediateTypes[imm] == DfScalarObj ||
                                    immediateTypes[imm] == DfUnknown) {
                                    // update dynamic type
                                    immediateTypes[imm] = fieldType;
                                } else {
                                    // if a static type has been set, enforce it
                                    if (immediateTypes[imm] != fieldType) {
                                        status = StatusInval;
                                        xSyslogTxnBuf(moduleName,
                                                      XlogErr,
                                                      "%s has type %s, "
                                                      "should have type %s",
                                                      input_->renameMap[ii][jj]
                                                          .oldName,
                                                      strGetFromDfFieldType(
                                                          fieldType),
                                                      strGetFromDfFieldType(
                                                          immediateTypes[imm]));
                                        goto CommonExit;
                                    }
                                }

                                break;
                            }
                        }

                        if (imm == numImmediates) {
                            // found a new immediate to add
                            immediateNames[imm] =
                                input_->renameMap[ii][jj].newName;
                            immediateTypes[imm] = fieldType;
                            numImmediates++;
                        }
                    }
                }
            }
        }
    }

    // setup valueDesc
    tupMeta.setNumFields(numImmediates);

    // now add immediates
    for (unsigned ii = 0; ii < numImmediates; ii++) {
        valueNames[ii] = immediateNames[ii];
        tupMeta.setFieldType(immediateTypes[ii], ii);
    }

    // recalculate keyIndexes
    if (input_->dedup || input_->unionType != UnionStandard) {
        for (unsigned ii = 0; ii < numKeys; ii++) {
            unsigned jj = 0;
            for (jj = 0; jj < tupMeta.getNumFields(); jj++) {
                if (strcmp(keyNames[ii], valueNames[jj]) == 0) {
                    keyIndexes[ii] = jj;
                    break;
                }
            }

            if (jj == tupMeta.getNumFields()) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Key %s must be part of input columns",
                              keyNames[ii]);
                goto CommonExit;
            }
        }
    }

    status = XdbMgr::get()->xdbCreate(dstXdbId_,
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
                                      dhtId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Union(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));
    xdbCreated_ = true;

CommonExit:
    return status;
}
