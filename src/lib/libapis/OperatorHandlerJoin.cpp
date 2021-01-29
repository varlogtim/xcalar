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
#include "libapis/OperatorHandlerJoin.h"
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
#include "usr/Users.h"

OperatorHandlerJoin::OperatorHandlerJoin(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

OperatorHandlerJoin::~OperatorHandlerJoin()
{
    if (aggVariableNames_) {
        memFree(aggVariableNames_);
        aggVariableNames_ = NULL;
    }

    if (astCreated_) {
        XcalarEval::get()->freeCommonAst(&ast_);
        astCreated_ = false;
    }

    numAggVariables_ = 0;

    if (broadcastTableCreated_) {
        TableNsMgr *tnsMgr = TableNsMgr::get();
        XcalarApiUdfContainer *sessionContainer =
            dstGraph_->getSessionContainer();
        if (broadcastRef_) {
            dstGraph_->putDagNodeRefById(broadcastTableId_);
            broadcastRef_ = false;
        }

        if (broadcastHandleTrack_.tableHandleValid) {
            UserMgr::get()->untrackResultsets(broadcastHandleTrack_.tableId,
                                              &sessionContainer->userId,
                                              &sessionContainer->sessionInfo);
            tnsMgr->closeHandleToNs(&broadcastHandleTrack_.tableHandle);
            broadcastHandleTrack_.tableHandleValid = false;
        }

        Status status;
        status =
            dstGraph_->cleanAndDeleteDagNode(broadcastTable_,
                                             SrcTable,
                                             DagTypes::RemoveNodeFromNamespace,
                                             NULL,
                                             NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to delete broadcastTable %s: %s",
                    broadcastTable_,
                    strGetFromStatus(status));
        }

        broadcastTableCreated_ = false;
    }

    if (newApiInput_) {
        memFree(newApiInput_);
        newApiInput_ = NULL;
    }
}

ApiHandler::Flags
OperatorHandlerJoin::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerJoin::getDstNodeName()
{
    return input_->joinTable.tableName;
}

const char *
OperatorHandlerJoin::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.joinOutput.tableName;
}

Status
OperatorHandlerJoin::validateKeys(XdbMeta *leftXdbMeta, XdbMeta *rightXdbMeta)
{
    Status status;

    if (leftXdbMeta->numKeys != rightXdbMeta->numKeys) {
        status = StatusInval;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Left num join keys: %d does not match right num "
                      "join keys: %d",
                      leftXdbMeta->numKeys,
                      rightXdbMeta->numKeys);
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < leftXdbMeta->numKeys; ii++) {
        assert(leftXdbMeta->keyAttr[ii].type != DfScalarObj);
        assert(rightXdbMeta->keyAttr[ii].type != DfScalarObj);
        if (leftXdbMeta->keyAttr[ii].type != DfUnknown &&
            rightXdbMeta->keyAttr[ii].type != DfUnknown &&
            leftXdbMeta->keyAttr[ii].type != rightXdbMeta->keyAttr[ii].type) {
            status = StatusJoinTypeMismatch;

            xSyslogTxnBuf(
                moduleName,
                XlogErr,
                "Left key %s type: %s does not match right key %s type: %s",
                leftXdbMeta->keyAttr[ii].name,
                strGetFromDfFieldType(leftXdbMeta->keyAttr[ii].type),
                rightXdbMeta->keyAttr[ii].name,
                strGetFromDfFieldType(rightXdbMeta->keyAttr[ii].type));

            goto CommonExit;
        }

        if (leftXdbMeta->keyAttr[ii].valueArrayIndex ==
                NewTupleMeta::DfInvalidIdx ||
            rightXdbMeta->keyAttr[ii].valueArrayIndex ==
                NewTupleMeta::DfInvalidIdx) {
            // can't perform a join on an invalid key
            status = StatusNoKey;
            goto CommonExit;
        }

        if (leftXdbMeta->keyOrderings[ii] & DescendingFlag ||
            rightXdbMeta->keyOrderings[ii] & DescendingFlag) {
            status = StatusJoinInvalidOrdering;
            goto CommonExit;
        }
    }

    if (leftXdbMeta->dhtId != rightXdbMeta->dhtId) {
        status = StatusJoinDhtMismatch;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
OperatorHandlerJoin::generateFullRenameMap()
{
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    bool leftRef = false, rightRef = false;
    TableNsMgr::TableHandleTrack leftHandleTrack, rightHandleTrack;
    newApiInput_ = NULL;
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();
    int ret;
    char prefix[DfMaxFieldNameLen + 1];

    // Comment out this for now because we don't have
    // exact fields of interest from optimizer
    // shouldn't be messing with the rename map in LRQ mode
    // assert(Txn::currentTxn().mode_ == Txn::Mode::NonLRQ);

    if (dstGraph_->hdr_.graphType == DagTypes::QueryGraph) {
        // don't have enough info to generate rename maps in a queryGraph
        return StatusOk;
    }

    // get dag ref on parents to prevent them from getting deleted
    DgDagState dgState;
    status = parentGraphs_[DagTypes::LeftTableIdx]
                 ->getDagNodeStateAndRef(input_->leftTable.tableId, &dgState);
    BailIfFailed(status);
    leftRef = true;

    if (dgState != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        goto CommonExit;
    }

    status = parentGraphs_[DagTypes::RightTableIdx]
                 ->getDagNodeStateAndRef(input_->rightTable.tableId, &dgState);
    BailIfFailed(status);
    rightRef = true;

    if (dgState != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        goto CommonExit;
    }

    // Get shared read access to left and right source tables.
    status = parentGraphs_[DagTypes::LeftTableIdx]
                 ->getTableIdFromNodeId(input_->leftTable.tableId,
                                        &leftHandleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    input_->leftTable.tableId,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(sessionContainer,
                                    leftHandleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &leftHandleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    leftHandleTrack.tableId,
                    strGetFromStatus(status));
    leftHandleTrack.tableHandleValid = true;

    status = parentGraphs_[DagTypes::RightTableIdx]
                 ->getTableIdFromNodeId(input_->rightTable.tableId,
                                        &rightHandleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    input_->rightTable.tableId,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(sessionContainer,
                                    rightHandleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &rightHandleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    rightHandleTrack.tableId,
                    strGetFromStatus(status));
    rightHandleTrack.tableHandleValid = true;

    XdbMeta *leftMeta, *rightMeta;

    status = xdbMgr->xdbGet(input_->leftTable.xdbId, NULL, &leftMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(input_->rightTable.xdbId, NULL, &rightMeta);
    BailIfFailed(status);

    {
        // add all columns to renameMaps
        unsigned leftCols =
            leftMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        XcalarApiRenameMap leftMap[leftCols];

        unsigned rightCols =
            rightMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        XcalarApiRenameMap rightMap[rightCols];

        for (unsigned ii = 0; ii < leftCols; ii++) {
            char *leftName = leftMeta->kvNamedMeta.valueNames_[ii];
            status = strStrlcpy(leftMap[ii].oldName,
                                leftName,
                                sizeof(leftMap[ii].oldName));
            BailIfFailed(status);
            status = strStrlcpy(leftMap[ii].newName,
                                leftName,
                                sizeof(leftMap[ii].newName));
            BailIfFailed(status);
            leftMap[ii].type =
                leftMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii);
            leftMap[ii].isKey = false;

            for (unsigned kk = 0; kk < leftMeta->numKeys; kk++) {
                if (leftMeta->keyAttr[kk].valueArrayIndex == (int) ii) {
                    leftMap[ii].isKey = true;
                }
            }

            for (unsigned jj = 0; jj < input_->numLeftColumns; jj++) {
                bool typeMatch = FatptrTypeMatch(input_->renameMap[jj].type,
                                                 leftMap[ii].type);
                if (typeMatch &&
                    strcmp(leftName, input_->renameMap[jj].oldName) == 0) {
                    // overwrite with user specified name
                    status = strStrlcpy(leftMap[ii].newName,
                                        input_->renameMap[jj].newName,
                                        sizeof(leftMap[ii].newName));
                    BailIfFailed(status);
                    break;
                } else if (input_->renameMap[jj].type == DfFatptr) {
                    ret = snprintf(prefix,
                                   sizeof(prefix),
                                   "%s%s",
                                   input_->renameMap[jj].oldName,
                                   DfFatptrPrefixDelimiterReplaced);
                    assert(ret <= (int) sizeof(prefix) - 1);

                    if (strncmp(leftName, prefix, ret) == 0) {
                        status = strSnprintf(leftMap[ii].newName,
                                             sizeof(leftMap[ii].newName),
                                             "%s%s%s",
                                             input_->renameMap[jj].newName,
                                             DfFatptrPrefixDelimiterReplaced,
                                             &leftName[ret]);
                        BailIfFailed(status);
                        break;
                    }
                }
            }
        }

        for (unsigned ii = 0; ii < rightCols; ii++) {
            char *rightName = rightMeta->kvNamedMeta.valueNames_[ii];
            status = strStrlcpy(rightMap[ii].oldName,
                                rightName,
                                sizeof(rightMap[ii].oldName));
            BailIfFailed(status);
            status = strStrlcpy(rightMap[ii].newName,
                                rightName,
                                sizeof(rightMap[ii].newName));
            BailIfFailed(status);
            rightMap[ii].type =
                rightMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii);
            rightMap[ii].isKey = false;

            for (unsigned kk = 0; kk < rightMeta->numKeys; kk++) {
                if (rightMeta->keyAttr[kk].valueArrayIndex == (int) ii) {
                    rightMap[ii].isKey = true;
                }
            }

            for (unsigned jj = input_->numLeftColumns;
                 jj < input_->numLeftColumns + input_->numRightColumns;
                 jj++) {
                bool typeMatch = FatptrTypeMatch(input_->renameMap[jj].type,
                                                 rightMap[ii].type);
                if (typeMatch &&
                    strcmp(rightName, input_->renameMap[jj].oldName) == 0) {
                    // overwrite with user specified name
                    status = strStrlcpy(rightMap[ii].newName,
                                        input_->renameMap[jj].newName,
                                        sizeof(rightMap[ii].newName));
                    BailIfFailed(status);
                    break;
                } else if (input_->renameMap[jj].type == DfFatptr) {
                    ret = snprintf(prefix,
                                   sizeof(prefix),
                                   "%s%s",
                                   input_->renameMap[jj].oldName,
                                   DfFatptrPrefixDelimiterReplaced);
                    assert(ret <= (int) sizeof(prefix) - 1);

                    if (strncmp(rightName, prefix, ret) == 0) {
                        status = strSnprintf(rightMap[ii].newName,
                                             sizeof(rightMap[ii].newName),
                                             "%s%s%s",
                                             input_->renameMap[jj].newName,
                                             DfFatptrPrefixDelimiterReplaced,
                                             &rightName[ret]);
                        BailIfFailed(status);
                        break;
                    }
                }
            }
        }

        inputSize_ = sizeof(*input_) +
                     (leftCols + rightCols) * sizeof(*input_->renameMap);
        newApiInput_ = (XcalarApiInput *) memAlloc(inputSize_);
        BailIfNull(newApiInput_);

        XcalarApiJoinInput *newJoinInput = &newApiInput_->joinInput;

        *newJoinInput = *input_;
        newJoinInput->numLeftColumns = leftCols;
        newJoinInput->numRightColumns = rightCols;
        memcpy(newJoinInput->renameMap, leftMap, leftCols * sizeof(*leftMap));
        memcpy(&newJoinInput->renameMap[leftCols],
               rightMap,
               rightCols * sizeof(*rightMap));

        apiInput_ = newApiInput_;
        input_ = newJoinInput;
    }

CommonExit:
    if (leftRef) {
        parentGraphs_[DagTypes::LeftTableIdx]->putDagNodeRefById(
            input_->leftTable.tableId);
        leftRef = false;
    }

    if (rightRef) {
        parentGraphs_[DagTypes::RightTableIdx]->putDagNodeRefById(
            input_->rightTable.tableId);
        rightRef = false;
    }

    if (leftHandleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&leftHandleTrack.tableHandle);
        leftHandleTrack.tableHandleValid = false;
    }

    if (rightHandleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&rightHandleTrack.tableHandle);
        rightHandleTrack.tableHandleValid = false;
    }

    if (status != StatusOk) {
        if (newApiInput_) {
            memFree(newApiInput_);
        }
        newApiInput_ = NULL;
    }

    return status;
}

Status
OperatorHandlerJoin::addKeyToRenameMap()
{
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    bool leftRef = false, rightRef = false;
    TableNsMgr::TableHandleTrack leftHandleTrack, rightHandleTrack;
    newApiInput_ = NULL;
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();
    bool useLeftKey = true;
    unsigned leftCols = input_->numLeftColumns;
    unsigned rightCols = input_->numRightColumns;
    unsigned extraKeyCount = 0;

    // CrossJoin doesn't need key, rightOuterJoin use right key
    if (input_->joinType == CrossJoin) {
        return StatusOk;
    } else if (input_->joinType == RightOuterJoin) {
        useLeftKey = false;
    }

    if (dstGraph_->hdr_.graphType == DagTypes::QueryGraph) {
        // don't have enough info to generate rename maps in a queryGraph
        return StatusOk;
    }

    // get dag ref on parents to prevent them from getting deleted
    DgDagState dgState;
    status = parentGraphs_[DagTypes::LeftTableIdx]
                 ->getDagNodeStateAndRef(input_->leftTable.tableId, &dgState);
    BailIfFailed(status);
    leftRef = true;

    if (dgState != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        goto CommonExit;
    }

    status = parentGraphs_[DagTypes::RightTableIdx]
                 ->getDagNodeStateAndRef(input_->rightTable.tableId, &dgState);
    BailIfFailed(status);
    rightRef = true;

    if (dgState != DgDagStateReady) {
        status = StatusDgDagNodeNotReady;
        goto CommonExit;
    }

    // Get shared read access to left and right source tables.
    status = parentGraphs_[DagTypes::LeftTableIdx]
                 ->getTableIdFromNodeId(input_->leftTable.tableId,
                                        &leftHandleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    input_->leftTable.tableId,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(sessionContainer,
                                    leftHandleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &leftHandleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    leftHandleTrack.tableId,
                    strGetFromStatus(status));
    leftHandleTrack.tableHandleValid = true;

    status = parentGraphs_[DagTypes::RightTableIdx]
                 ->getTableIdFromNodeId(input_->rightTable.tableId,
                                        &rightHandleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    input_->rightTable.tableId,
                    strGetFromStatus(status));

    status = tnsMgr->openHandleToNs(sessionContainer,
                                    rightHandleTrack.tableId,
                                    LibNsTypes::ReaderShared,
                                    &rightHandleTrack.tableHandle,
                                    TableNsMgr::OpenSleepInUsecs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to open handle to table %ld: %s",
                    rightHandleTrack.tableId,
                    strGetFromStatus(status));
    rightHandleTrack.tableHandleValid = true;

    XdbMeta *leftMeta, *rightMeta;

    status = xdbMgr->xdbGet(input_->leftTable.xdbId, NULL, &leftMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(input_->rightTable.xdbId, NULL, &rightMeta);
    BailIfFailed(status);

    // Loop through keys to figure out expected rename map size
    // and generate extra key rename map
    unsigned numKeys, renameMapSize;
    XcalarApiRenameMap *keySideRenameMap;
    XdbMeta *keyMeta;
    if (useLeftKey) {
        numKeys = leftMeta->numKeys;
        keyMeta = leftMeta;
        renameMapSize = input_->numLeftColumns;
        keySideRenameMap = input_->renameMap;
    } else {
        numKeys = rightMeta->numKeys;
        keyMeta = rightMeta;
        renameMapSize = input_->numRightColumns;
        keySideRenameMap = &input_->renameMap[input_->numLeftColumns];
    }

    XcalarApiRenameMap extraKeyRenameMap[numKeys];
    for (unsigned ii = 0; ii < numKeys; ii++) {
        const char *keyName = keyMeta->keyAttr[ii].name;
        DfFieldType keyType = keyMeta->keyAttr[ii].type;
        bool found = false;
        for (unsigned jj = 0; jj < renameMapSize; jj++) {
            if (FatptrTypeMatch(keySideRenameMap[jj].type, keyType) &&
                strcmp(keySideRenameMap[jj].oldName, keyName) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            status =
                strStrlcpy(extraKeyRenameMap[extraKeyCount].oldName,
                           keyName,
                           sizeof(extraKeyRenameMap[extraKeyCount].oldName));
            BailIfFailed(status);
            status =
                strStrlcpy(extraKeyRenameMap[extraKeyCount].newName,
                           keyName,
                           sizeof(extraKeyRenameMap[extraKeyCount].newName));
            BailIfFailed(status);
            extraKeyRenameMap[extraKeyCount].type = keyType;
            extraKeyRenameMap[extraKeyCount].isKey = true;
            extraKeyCount++;
        }
    }

    if (extraKeyCount) {
        inputSize_ = sizeof(*input_) + (leftCols + rightCols + extraKeyCount) *
                                           sizeof(*input_->renameMap);
        newApiInput_ = (XcalarApiInput *) memAlloc(inputSize_);
        BailIfNull(newApiInput_);

        XcalarApiJoinInput *newJoinInput = &newApiInput_->joinInput;

        *newJoinInput = *input_;
        newJoinInput->numLeftColumns = leftCols;
        newJoinInput->numRightColumns = rightCols;
        memcpy(newJoinInput->renameMap,
               input_->renameMap,
               leftCols * sizeof(*input_->renameMap));
        if (useLeftKey) {
            newJoinInput->numLeftColumns += extraKeyCount;
            memcpy(&newJoinInput->renameMap[leftCols],
                   extraKeyRenameMap,
                   extraKeyCount * sizeof(*extraKeyRenameMap));
            memcpy(&newJoinInput->renameMap[leftCols + extraKeyCount],
                   &input_->renameMap[leftCols],
                   rightCols * sizeof(*input_->renameMap));
        } else {
            newJoinInput->numRightColumns += extraKeyCount;
            memcpy(&newJoinInput->renameMap[leftCols],
                   &input_->renameMap[leftCols],
                   rightCols * sizeof(*input_->renameMap));
            memcpy(&newJoinInput->renameMap[leftCols + rightCols],
                   extraKeyRenameMap,
                   extraKeyCount * sizeof(*extraKeyRenameMap));
        }

        apiInput_ = newApiInput_;
        input_ = newJoinInput;
    }

CommonExit:
    if (leftRef) {
        parentGraphs_[DagTypes::LeftTableIdx]->putDagNodeRefById(
            input_->leftTable.tableId);
        leftRef = false;
    }

    if (rightRef) {
        parentGraphs_[DagTypes::RightTableIdx]->putDagNodeRefById(
            input_->rightTable.tableId);
        rightRef = false;
    }

    if (leftHandleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&leftHandleTrack.tableHandle);
        leftHandleTrack.tableHandleValid = false;
    }

    if (rightHandleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&rightHandleTrack.tableHandle);
        rightHandleTrack.tableHandleValid = false;
    }

    if (status != StatusOk) {
        if (newApiInput_) {
            memFree(newApiInput_);
        }
        newApiInput_ = NULL;
    }

    return status;
}

Status
OperatorHandlerJoin::setArg(XcalarApiInput *input,
                            size_t inputSize,
                            bool parentNodeIdsToBeProvided)
{
    Status status;
    size_t expectedSize;
    bool foundInvalid;
    XcalarEval *xcalarEval = XcalarEval::get();
    const char **variableNames = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->joinInput);
    apiInput_ = input;
    input_ = &input->joinInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    input_->joinTable.tableId = dstNodeId_;
    dstXdbId_ = XidMgr::get()->xidGetNext();
    input_->joinTable.xdbId = dstXdbId_;
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        dstTableId_ = tnsMgr->genTableId();
    }

    // Please call init() before setArg()
    assert(dstGraph_ != NULL);

    // Do some sanity checks
    if (!isValidJoinOperator(input_->joinType)) {
        xSyslog(moduleName,
                XlogErr,
                "JoinType: %d is not a valid joinType",
                input_->joinType);
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->numLeftColumns + input_->numRightColumns >
        TupleMaxNumValuesPerRecord) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "numLeftColumns (%u) and numRightColumns (%u) exceeds "
                      "max num"
                      " columns (%u)",
                      input_->numLeftColumns,
                      input_->numRightColumns,
                      TupleMaxNumValuesPerRecord);
        status = StatusInval;
        goto CommonExit;
    }

    if (!xcalarEval->isValidTableName(input_->joinTable.tableName)) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "joinTable name (%s) is not valid",
                      input_->joinTable.tableName);
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->joinTable.tableName[0] == '\0') {
        int ret = snprintf(input_->joinTable.tableName,
                           DagTypes::MaxNameLen + 1,
                           XcalarTempDagNodePrefix "%lu",
                           dstNodeId_);
        if (ret >= DagTypes::MaxNameLen + 1) {
            status = StatusOverflow;
            goto CommonExit;
        }
    }

    foundInvalid = false;
    // We'll check all the new column names even if a bad one is found
    for (unsigned ii = 0; ii < input_->numLeftColumns + input_->numRightColumns;
         ii++) {
        if (!xcalarEval->isValidFieldName(input_->renameMap[ii].newName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "New field name (%s) is invalid",
                    input_->renameMap[ii].newName);
            foundInvalid = true;
        }
    }
    if (foundInvalid) {
        status = StatusInval;
        goto CommonExit;
    }

    expectedSize =
        sizeof(*input_) + (sizeof(input_->renameMap[0]) *
                           (input_->numLeftColumns + input_->numRightColumns));
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

    for (unsigned ii = 0; ii < input_->numLeftColumns; ii++) {
        for (unsigned jj = ii + 1; jj < input_->numLeftColumns; jj++) {
            if (strcmp(input_->renameMap[ii].oldName,
                       input_->renameMap[jj].oldName) == 0 &&
                strcmp(input_->renameMap[ii].newName,
                       input_->renameMap[jj].newName) != 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Found duplicate source column %s in left "
                              "columns",
                              input_->renameMap[ii].oldName);
                goto CommonExit;
            }

            if (strcmp(input_->renameMap[ii].newName,
                       input_->renameMap[jj].newName) == 0 &&
                strcmp(input_->renameMap[ii].oldName,
                       input_->renameMap[jj].oldName) != 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Found duplicate dest column %s in left columns",
                              input_->renameMap[ii].newName);
                goto CommonExit;
            }
        }
    }

    for (unsigned ii = input_->numLeftColumns;
         ii < input_->numRightColumns + input_->numLeftColumns;
         ii++) {
        for (unsigned jj = ii + 1;
             jj < input_->numRightColumns + input_->numLeftColumns;
             jj++) {
            if (strcmp(input_->renameMap[ii].oldName,
                       input_->renameMap[jj].oldName) == 0 &&
                strcmp(input_->renameMap[ii].newName,
                       input_->renameMap[jj].newName) != 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Found duplicate source column %s in right "
                              "columns",
                              input_->renameMap[ii].oldName);
                goto CommonExit;
            }

            if (strcmp(input_->renameMap[ii].newName,
                       input_->renameMap[jj].newName) == 0 &&
                strcmp(input_->renameMap[ii].oldName,
                       input_->renameMap[jj].oldName) != 0) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Found duplicate dest column %s in right columns",
                              input_->renameMap[ii].newName);
                goto CommonExit;
            }
        }
    }

    if (foundInvalid) {
        status = StatusInval;
        goto CommonExit;
    }

    numAggVariables_ = 0;
    if (input_->filterString[0] != '\0') {
        unsigned numVariables;
        unsigned numUniqueVariables;
        status = xcalarEval->parseEvalStr(input_->filterString,
                                          XcalarFnTypeEval,
                                          dstGraph_->getUdfContainer(),
                                          &ast_,
                                          NULL,
                                          &numVariables,
                                          NULL,
                                          NULL);
        BailIfFailed(status);
        astCreated_ = true;

        if (xcalarEval->containsUdf(ast_.rootNode)) {
            status = StatusUdfNotSupportedInCrossJoins;
            goto CommonExit;
        }

        variableNames =
            (const char **) memAlloc(sizeof(*variableNames) * numVariables);
        if (variableNames == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate "
                    "for %u variables",
                    numVariables);
            goto CommonExit;
        }

        status = xcalarEval->getUniqueVariablesList(&ast_,
                                                    variableNames,
                                                    numVariables,
                                                    &numUniqueVariables);
        BailIfFailed(status);

        aggVariableNames_ = (const char **) memAlloc(
            numUniqueVariables * sizeof(*aggVariableNames_));
        BailIfNull(aggVariableNames_);

        for (unsigned ii = 0; ii < numUniqueVariables; ii++) {
            if (strstr(variableNames[ii], DfFatptrPrefixDelimiter)) {
                status = StatusInval;
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Cannot perform join eval on prefixed field %s",
                              variableNames[ii]);
                goto CommonExit;
            } else if (variableNames[ii][0] == OperatorsAggregateTag) {
                aggVariableNames_[numAggVariables_++] = variableNames[ii];
            }
        }

        aggVariableNames_ =
            (const char **) memRealloc(aggVariableNames_,
                                       numAggVariables_ *
                                           sizeof(*aggVariableNames_));
        if (numAggVariables_ != 0) {
            BailIfNull(aggVariableNames_);
        }
    }

CommonExit:
    if (variableNames != NULL) {
        memFree(variableNames);
        variableNames = NULL;
    }

    return status;
}

Status
OperatorHandlerJoin::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *joinOutput = NULL;
    Status status;
    DgDagState leftTableState = DgDagStateError;
    DgDagState rightTableState = DgDagStateError;
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(*joinOutput);
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

    joinOutput = &output->outputResult.joinOutput;
    assert((uintptr_t) joinOutput == (uintptr_t) &output->outputResult);

    status = parentGraphs_[DagTypes::LeftTableIdx]
                 ->getDagNodeState(input_->leftTable.tableId, &leftTableState);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get state of dagNode \"%s\" (%lu): %s",
                input_->leftTable.tableName,
                input_->leftTable.tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (leftTableState != DgDagStateReady) {
        xSyslog(moduleName,
                XlogErr,
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting join",
                input_->leftTable.tableName,
                input_->leftTable.tableId,
                strGetFromDgDagState(leftTableState));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status =
        parentGraphs_[DagTypes::RightTableIdx]
            ->getDagNodeState(input_->rightTable.tableId, &rightTableState);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get state of dagNode \"%s\" (%lu): %s",
                input_->rightTable.tableName,
                input_->rightTable.tableId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (rightTableState != DgDagStateReady) {
        xSyslog(moduleName,
                XlogErr,
                "Parent dag node \"%s\" (%lu) in state: %s. Aborting join",
                input_->rightTable.tableName,
                input_->rightTable.tableId,
                strGetFromDgDagState(rightTableState));
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status =
        Operators::get()->join(dstGraph_, input_, optimizerContext, userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       joinOutput->tableName,
                                       sizeof(joinOutput->tableName));
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
OperatorHandlerJoin::getParentNodes(
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
    unsigned ii, parIdx;

    assert(!parentNodeIdsToBeProvided_);
    numParents = 2 + numAggVariables_;

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

    status = getSourceDagNode(input_->leftTable.tableName,
                              dstGraph_,
                              &sessionContainers[DagTypes::LeftTableIdx],
                              &parentGraphIds[DagTypes::LeftTableIdx],
                              &parentNodeIds[DagTypes::LeftTableIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "fail to retrieve dagNodeId for \"%s\", numParents %lu: %s",
                    input_->leftTable.tableName,
                    numParents,
                    strGetFromStatus(status));

    status = getSourceDagNode(input_->rightTable.tableName,
                              dstGraph_,
                              &sessionContainers[DagTypes::RightTableIdx],
                              &parentGraphIds[DagTypes::RightTableIdx],
                              &parentNodeIds[DagTypes::RightTableIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "fail to retrieve dagNodeId for \"%s\", numParents %lu: %s",
                    input_->rightTable.tableName,
                    numParents,
                    strGetFromStatus(status));

    parIdx = 2;
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
    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == parIdx);
    } else {
        numParents = parIdx;
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
OperatorHandlerJoin::createDagNode(DagTypes::NodeId *dstNodeIdOut,
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
        status = parentGraphs[DagTypes::LeftTableIdx]
                     ->getDagNodeId(input_->leftTable.tableName,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &input_->leftTable.tableId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving dagNodeId for \"%s\": %s",
                    input_->leftTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = parentGraphs[DagTypes::LeftTableIdx]
                     ->getXdbId(input_->leftTable.tableName,
                                Dag::TableScope::FullyQualOrLocal,
                                &input_->leftTable.xdbId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving tableId for \"%s\": %s",
                    input_->leftTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = parentGraphs[DagTypes::RightTableIdx]
                     ->getDagNodeId(input_->rightTable.tableName,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &input_->rightTable.tableId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving dagNodeId for \"%s\": %s",
                    input_->rightTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = parentGraphs[DagTypes::RightTableIdx]
                     ->getXdbId(input_->rightTable.tableName,
                                Dag::TableScope::FullyQualOrLocal,
                                &input_->rightTable.xdbId);
        if (status == StatusDagNodeNotFound) {
            status = StatusTableNotFound;
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving tableId for \"%s\": %s",
                    input_->rightTable.tableName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        parIdx = 2;
        for (unsigned ii = 0; ii < numAggVariables_; ii++) {
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

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        assert(numParents == 2 + numAggVariables_);
        assert(parentNodeIds != NULL);

        if (numParents != 2 + numAggVariables_) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid number of parents provided: %lu. Required: %u",
                    numParents,
                    2 + numAggVariables_);
            status = StatusInval;
            goto CommonExit;
        }

        if (parentNodeIds == NULL) {
            xSyslog(moduleName, XlogErr, "parentNodeIds cannot be NULL!");
            status = StatusInval;
            goto CommonExit;
        }

        input_->leftTable.tableId = parentNodeIds[DagTypes::LeftTableIdx];
        input_->leftTable.xdbId =
            parentGraphs[DagTypes::LeftTableIdx]->getXdbIdFromNodeId(
                input_->leftTable.tableId);

        input_->rightTable.tableId = parentNodeIds[DagTypes::RightTableIdx];
        input_->rightTable.xdbId =
            parentGraphs[DagTypes::RightTableIdx]->getXdbIdFromNodeId(
                input_->rightTable.tableId);

        if (input_->keepAllColumns) {
            status = generateFullRenameMap();
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error generating full rename map for node %lu: %s",
                        dstNodeId_,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            status = addKeyToRenameMap();
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error adding key to rename map for node %lu: %s",
                        dstNodeId_,
                        strGetFromStatus(status));
                goto CommonExit;
            }
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
                "Failed to create join node %s: %s",
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
OperatorHandlerJoin::createXdb(void *optimizerContext)
{
    Status status;
    unsigned numDatasets = 0;
    DsDatasetId datasetIds[TupleMaxNumValuesPerRecord];
    Xdb *leftXdb, *rightXdb;
    XdbMeta *leftXdbMeta = NULL;
    XdbMeta *rightXdbMeta = NULL;
    NewTupleMeta tupleMeta;
    const char *immediateNames[TupleMaxNumValuesPerRecord];
    const char *fatptrPrefixNames[TupleMaxNumValuesPerRecord];
    unsigned numImmediates, numFatptrs;
    XdbMgr *xdbMgr = XdbMgr::get();
    size_t leftSize, rightSize;
    unsigned numKeys;
    const char **keyNames = NULL;
    DfFieldType *keyTypes = NULL;
    int *keyIndexes = NULL;
    Ordering *keyOrdering = NULL;
    bool useLeftKey = false;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    XcalarApiUdfContainer *sessionContainer = dstGraph_->getSessionContainer();

    if (input_->joinType == CrossJoin) {
        status =
            parentGraphs_[DagTypes::LeftTableIdx]
                ->getDagNodeSourceSize(input_->leftTable.tableId, &leftSize);
        BailIfFailed(status);

        status =
            parentGraphs_[DagTypes::RightTableIdx]
                ->getDagNodeSourceSize(input_->rightTable.tableId, &rightSize);
        BailIfFailed(status);

        if (leftSize < rightSize) {
            tableToBroadcast_ = input_->leftTable.tableName;
            useLeftKey = false;
        } else {
            tableToBroadcast_ = input_->rightTable.tableName;
            useLeftKey = true;
        }

        size_t ret = snprintf(broadcastTable_,
                              sizeof(broadcastTable_),
                              "%s-%lu-%s",
                              XcalarApiBroadcastTablePrefix,
                              dstNodeId_,
                              tableToBroadcast_);
        if (ret >= sizeof(broadcastTable_)) {
            status = StatusOverflow;
            goto CommonExit;
        }

        // replace hashtags with _ for the UI
        char *ptr;
        ptr = strchr(broadcastTable_, '#');
        if (ptr != NULL) {
            *ptr = '_';
        }

        status = Operators::get()->broadcastTable(dstGraph_,
                                                  userId_,
                                                  tableToBroadcast_,
                                                  broadcastTable_,
                                                  &broadcastTableId_,
                                                  optimizerContext);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to broadcast table %s for join %s: %s",
                        tableToBroadcast_,
                        input_->joinTable.tableName,
                        strGetFromStatus(status));
        broadcastTableCreated_ = true;

        status = dstGraph_->getDagNodeRefById(broadcastTableId_);
        BailIfFailed(status);

        broadcastRef_ = true;

        status =
            dstGraph_->getTableIdFromNodeId(broadcastTableId_,
                                            &broadcastHandleTrack_.tableId);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed getTableIdFromNodeId for dagNode %lu: %s",
                        broadcastTableId_,
                        strGetFromStatus(status));

        status = tnsMgr->openHandleToNs(sessionContainer,
                                        broadcastHandleTrack_.tableId,
                                        LibNsTypes::ReaderShared,
                                        &broadcastHandleTrack_.tableHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        broadcastHandleTrack_.tableId,
                        strGetFromStatus(status));
        broadcastHandleTrack_.tableHandleValid = true;

        XdbId broadcastXdbId = dstGraph_->getXdbIdFromNodeId(broadcastTableId_);
        Xdb *broadcastXdb;

        status = XdbMgr::get()->xdbGet(broadcastXdbId, &broadcastXdb, NULL);
        assert(status == StatusOk);

        if (!useLeftKey) {
            input_->leftTable.xdbId = broadcastXdbId;
        } else {
            input_->rightTable.xdbId = broadcastXdbId;
        }
    } else if (input_->joinType == RightOuterJoin) {
        useLeftKey = false;
    } else {
        useLeftKey = true;
    }

    status = xdbMgr->xdbGet(input_->leftTable.xdbId, &leftXdb, &leftXdbMeta);
    assert(status == StatusOk);

    status = xdbMgr->xdbGet(input_->rightTable.xdbId, &rightXdb, &rightXdbMeta);
    assert(status == StatusOk);

    tupleMeta.setNumFields(
        leftXdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields() +
        rightXdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields());

    XdbMeta *srcMeta;
    // choose which table to take meta from
    if (useLeftKey) {
        srcMeta = leftXdbMeta;
    } else {
        srcMeta = rightXdbMeta;
    }

    if (input_->joinType != CrossJoin) {
        status = validateKeys(leftXdbMeta, rightXdbMeta);
        BailIfFailed(status);
    }

    numKeys = input_->joinType == CrossJoin ? 1 : srcMeta->numKeys;

    keyNames = (const char **) memAlloc(numKeys * sizeof(*keyNames));
    BailIfNull(keyNames);

    keyTypes = (DfFieldType *) memAlloc(numKeys * sizeof(*keyTypes));
    BailIfNull(keyTypes);

    keyIndexes = (int *) memAlloc(numKeys * sizeof(*keyIndexes));
    BailIfNull(keyIndexes);

    keyOrdering = (Ordering *) memAlloc(numKeys * sizeof(*keyOrdering));
    BailIfNull(keyOrdering);

    if (input_->joinType == CrossJoin) {
        keyNames[0] = DsDefaultDatasetKeyName;
        keyTypes[0] = DfInt64;
        keyOrdering[0] = Ordering::Unordered;
    } else {
        for (unsigned ii = 0; ii < numKeys; ii++) {
            keyNames[ii] = srcMeta->keyAttr[ii].name;
            keyTypes[ii] = srcMeta->keyAttr[ii].type;
            keyOrdering[ii] = srcMeta->keyAttr[ii].ordering;
        }
    }

    // check for prefix / immediate name collisions and populate names
    status =
        Operators::populateJoinTupMeta(useLeftKey,
                                       leftXdbMeta,
                                       rightXdbMeta,
                                       input_->numLeftColumns,
                                       input_->renameMap,
                                       input_->numRightColumns,
                                       &input_
                                            ->renameMap[input_->numLeftColumns],
                                       immediateNames,
                                       fatptrPrefixNames,
                                       input_->joinType,
                                       numKeys,
                                       keyNames,
                                       keyIndexes,
                                       keyTypes,
                                       input_->collisionCheck,
                                       &tupleMeta,
                                       &numImmediates,
                                       &numFatptrs);
    BailIfFailed(status);

    assert(numImmediates <= TupleMaxNumValuesPerRecord);

    assert(numFatptrs <= TupleMaxNumValuesPerRecord);
    numDatasets = leftXdbMeta->numDatasets + rightXdbMeta->numDatasets;
    assert(numDatasets <= TupleMaxNumValuesPerRecord);

    // Populate datasetIds
    {
        const unsigned leftNumDatasets = leftXdbMeta->numDatasets;
        for (unsigned ii = 0; ii < leftNumDatasets; ii++) {
            datasetIds[ii] = leftXdbMeta->datasets[ii]->getDatasetId();
        }
        for (unsigned ii = leftNumDatasets; ii < numDatasets; ii++) {
            datasetIds[ii] =
                rightXdbMeta->datasets[ii - leftNumDatasets]->getDatasetId();
        }
    }

#ifdef DEBUG
    {
        for (size_t ii = 0; ii < numKeys; ii++) {
            size_t jj = 0;
            for (jj = 0; jj < numImmediates; jj++) {
                if (!strcmp(keyNames[ii], immediateNames[jj])) {
                    break;
                }
            }
            if (keyIndexes[ii] != NewTupleMeta::DfInvalidIdx) {
                assert(jj < numImmediates);
            }
        }
    }
#endif  // DEBUG

    status = xdbMgr->xdbCreate(dstXdbId_,
                               numKeys,
                               keyNames,
                               keyTypes,
                               keyIndexes,
                               keyOrdering,
                               &tupleMeta,
                               datasetIds,
                               numDatasets,
                               immediateNames,
                               numImmediates,
                               fatptrPrefixNames,
                               numFatptrs,
                               XdbGlobal,
                               srcMeta->dhtId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    xdbCreated_ = true;

CommonExit:
    if (keyNames != NULL) {
        memFree(keyNames);
        keyNames = NULL;
    }
    if (keyTypes != NULL) {
        memFree(keyTypes);
        keyTypes = NULL;
    }
    if (keyIndexes != NULL) {
        memFree(keyIndexes);
        keyIndexes = NULL;
    }
    if (keyOrdering != NULL) {
        memFree(keyOrdering);
        keyOrdering = NULL;
    }
    return status;
}
