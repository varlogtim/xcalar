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
#include "libapis/OperatorHandlerSelect.h"
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
#include "ns/LibNsTypes.h"
#include "ns/LibNs.h"
#include "optimizer/Optimizer.h"
#include "xdb/HashTree.h"
#include "queryparser/QueryParser.h"

OperatorHandlerSelect::OperatorHandlerSelect(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

OperatorHandlerSelect::~OperatorHandlerSelect()
{
    if (handleInit_) {
        HashTreeMgr::get()->closeHandleToHashTree(&refHandle_,
                                                  input_->srcTable.tableId,
                                                  input_->srcTable.tableName);
    }
}

ApiHandler::Flags
OperatorHandlerSelect::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator |
                   DoNotGetParentRef);
}

const char *
OperatorHandlerSelect::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerSelect::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.selectOutput.tableName;
}

Status
OperatorHandlerSelect::setArg(XcalarApiInput *input,
                              size_t inputSize,
                              bool parentNodeIdsToBeProvided)
{
    Status status;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->synthesizeInput);
    apiInput_ = input;
    input_ = &input->selectInput;
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
    if (sizeof(*input_) + input_->evalInputCount != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "Expected size = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    if (!XcalarEval::get()->isValidTableName(input_->dstTable.tableName)) {
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
            xSyslog(moduleName,
                    XlogErr,
                    "Destination table name generation failed: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (strlen(input_->filterString) > 0 &&
        XcalarEval::get()->containsUdf(input_->filterString)) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Select filter %s cannot contain udfs",
                      input_->filterString);
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
OperatorHandlerSelect::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *selectOutput = NULL;
    Status status;
    size_t outputSize = 0;
    HashTreeMgr *ht = HashTreeMgr::get();

    outputSize = XcalarApiSizeOfOutput(*selectOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Select on publish table %s failed, insufficient memory to "
                "allocate output struct. (Size required: %lu bytes)",
                input_->srcTable.tableName,
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }
    memZero(output, outputSize);

    selectOutput = &output->outputResult.selectOutput;
    assert((uintptr_t) selectOutput == (uintptr_t) &output->outputResult);

    status = ht->selectHashTree(input_->dstTable,
                                input_->joinTable,
                                input_->srcTable.tableId,
                                input_->srcTable.tableName,
                                input_->minBatchId,
                                input_->maxBatchId,
                                input_->filterString,
                                input_->numColumns,
                                input_->columns,
                                input_->evalInputCount,
                                (char *) input_->evalInput,
                                input_->limitRows,
                                dstGraph_->getId());
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed select on publish table %s dst table %s "
                "batch %ld %ld: %s",
                input_->srcTable.tableName,
                input_->dstTable.tableName,
                input_->minBatchId,
                input_->maxBatchId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       selectOutput->tableName,
                                       sizeof(selectOutput->tableName));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed select on publish table %s dst table %s "
                "batch %ld %ld. could not get name of table %s created: %s",
                input_->srcTable.tableName,
                input_->dstTable.tableName,
                input_->minBatchId,
                input_->maxBatchId,
                selectOutput->tableName,
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
OperatorHandlerSelect::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    if (strlen(input_->joinTable.tableName) == 0) {
        *numParentsOut = 0;
        *parentNodeIdsOut = NULL;
        return StatusOk;
    }

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
    status = getSourceDagNode(input_->joinTable.tableName,
                              dstGraph_,
                              &sessionContainers[parIdx],
                              &parentGraphIds[parIdx],
                              &parentNodeIds[parIdx]);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getParentNodes table \"%s\" numParents %lu: %s",
                    input_->joinTable.tableName,
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
OperatorHandlerSelect::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                     DagTypes::GraphType srcGraphType,
                                     const char *nodeName,
                                     uint64_t numParents,
                                     Dag **parentGraphs,
                                     DagTypes::NodeId *parentNodeIds)
{
    Status status;
    unsigned parIdx = 0;
    HashTreeMgr *ht = HashTreeMgr::get();

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        if (strlen(input_->joinTable.tableName) > 0) {
            status = parentGraphs[parIdx]
                         ->getDagNodeId(input_->joinTable.tableName,
                                        Dag::TableScope::FullyQualOrLocal,
                                        &input_->joinTable.tableId);
            if (status == StatusDagNodeNotFound) {
                status = StatusTableNotFound;
            }

            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving dagNodeId for \"%s\": %s",
                        input_->joinTable.tableName,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            status = parentGraphs[parIdx]
                         ->getXdbId(input_->joinTable.tableName,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &input_->joinTable.xdbId);
            if (status == StatusDagNodeNotFound) {
                status = StatusTableNotFound;
            }

            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving tableId for \"%s\": %s",
                        input_->joinTable.tableName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }

        status = ht->openHandleToHashTree(input_->srcTable.tableName,
                                          LibNsTypes::ReadSharedWriteExclReader,
                                          &refHandle_,
                                          HashTreeMgr::OpenRetry::True);
        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error retrieving hashTree for \"%s\": %s",
                          input_->srcTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        input_->srcTable.tableId = refHandle_.hashTreeId;
        handleInit_ = true;

        if (!refHandle_.active) {
            status = StatusPubTableInactive;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error retrieving hashTree for \"%s\": %s",
                          input_->srcTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        hashTree_ = ht->getHashTreeById(refHandle_.hashTreeId);
        if (hashTree_ == NULL) {
            status = StatusPubTableNameNotFound;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Error retrieving hashTree for \"%s\": %s",
                          input_->srcTable.tableName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

#ifdef DEBUG
        if (input_->maxBatchId == HashTree::InvalidBatchId &&
            input_->minBatchId == HashTree::InvalidBatchId) {
            assert(refHandle_.currentBatchId >= 0);
        }
#endif

        if (input_->maxBatchId == HashTree::InvalidBatchId) {
            // pick the latest batch
            input_->maxBatchId = refHandle_.currentBatchId;
        }

        if (input_->minBatchId == HashTree::InvalidBatchId) {
            // pick the smallest batch
            input_->minBatchId = 0;
        }

        if (input_->maxBatchId < input_->minBatchId) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Batch id max %ld is less than batch id min %ld",
                          input_->maxBatchId,
                          input_->minBatchId);
            goto CommonExit;
        }
    }

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
                "Failed to create select node %s: %s",
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
OperatorHandlerSelect::createXdb(void *optimizerContext)
{
    Status status = StatusOk;
    XdbMeta *srcMeta = hashTree_->getXdbMeta();
    NewTupleMeta *hashTreeTupMeta = hashTree_->getTupMeta();
    unsigned numHashTreeCols = hashTreeTupMeta->getNumFields();

    NewTupleMeta tupMeta;
    const char *immediateNames[TupleMaxNumValuesPerRecord];
    int ret;

    bool keyReplaced = false;
    unsigned numKeys = srcMeta->numKeys;
    const char *keyNames[numKeys];
    DfFieldType keyTypes[numKeys];
    int keyIndexes[numKeys];
    Ordering keyOrderings[numKeys];

    json_t *evalInputJson = NULL, *maps = NULL, *groupBys = NULL, *join = NULL;
    unsigned numMaps = 0, numGroupBys = 0;
    XcalarApiRenameMap *renameMap = NULL;

    for (unsigned ii = 0; ii < numKeys; ii++) {
        keyNames[ii] = srcMeta->keyAttr[ii].name;
        keyTypes[ii] = srcMeta->keyAttr[ii].type;
        keyIndexes[ii] = srcMeta->keyAttr[ii].valueArrayIndex;
        keyOrderings[ii] = srcMeta->keyAttr[ii].ordering;
    }

    json_error_t err;
    if (input_->evalInputCount) {
        evalInputJson = json_loads((char *) input_->evalInput, 0, &err);
    }

    if (evalInputJson) {
        if (strlen(input_->filterString) == 0) {
            json_t *filter =
                json_object_get(evalInputJson, QpSelect::FilterKey);
            // Check that the json key and value are both valid.
            // Return parse error when value is not valid.
            if (filter && (json_string_value(filter) != NULL)) {
                strlcpy(input_->filterString,
                        json_string_value(filter),
                        sizeof(input_->filterString));
            } else if (filter != NULL) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }
        }

        maps = json_object_get(evalInputJson, QpSelect::MapsKey);
        if (maps) {
            numMaps = json_array_size(maps);
        }

        groupBys = json_object_get(evalInputJson, QpSelect::GroupBysKey);
        if (groupBys) {
            numGroupBys = json_array_size(groupBys);
        }

        join = json_object_get(evalInputJson, QpSelect::JoinKey);
    }

    if (numHashTreeCols + numMaps + numGroupBys > TupleMaxNumValuesPerRecord) {
        status = StatusFieldLimitExceeded;
        goto CommonExit;
    }

    if ((XcalarConfig::get()->autoCreateIndex_ || input_->createIndex) &&
        strlen(input_->filterString) > 0) {
        EvalContext filterCtx;
        FilterRange filterRange;

        status = filterCtx.setupAst(input_->filterString, srcMeta);
        BailIfFailed(status);

        if (filterRange.init(&filterCtx, srcMeta)) {
            HashTree::Index *index = hashTree_->getIndex(filterRange.keyName_);
            if (index == NULL) {
                status = HashTreeMgr::get()
                             ->addIndexToHashTree(input_->dstTable.tableName,
                                                  refHandle_.hashTreeId,
                                                  filterRange.keyName_);
                if (status != StatusOk) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Failed to create index on key %s: %s",
                                  filterRange.keyName_,
                                  strGetFromStatus(status));
                    goto CommonExit;
                }
            }
        }
    }

    // select all columns from the hash tree
    for (unsigned ii = 0; ii < numHashTreeCols; ii++) {
        immediateNames[ii] = srcMeta->kvNamedMeta.valueNames_[ii];
        tupMeta.setFieldType(srcMeta->kvNamedMeta.kvMeta_.tupMeta_
                                 ->getFieldType(ii),
                             ii);
    }

    tupMeta.setNumFields(numHashTreeCols);

    // copy over any join columns
    if (join) {
        XdbMeta *joinMeta;
        status =
            XdbMgr::get()->xdbGet(input_->joinTable.xdbId, NULL, &joinMeta);
        assert(status == StatusOk);

        status = OperatorHandlerJoin::validateKeys(srcMeta, joinMeta);
        BailIfFailed(status);

        const char *joinTable = "", *joinType = "";
        json_t *columns = NULL;

        ret = json_unpack_ex(join,
                             &err,
                             0,
                             QpSelect::JoinFormatString,
                             QpSelect::SourceKey,
                             &joinTable,
                             QpSelect::JoinTypeKey,
                             &joinType,
                             QpSelect::ColumnsKey,
                             &columns);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        JoinOperator joinOp = strToJoinOperator(joinType);
        if (joinOp != InnerJoin) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "join in select only supports InnerJoin type");
            status = StatusInval;
            goto CommonExit;
        }

        if (columns) {
            unsigned numCols = json_array_size(columns);

            renameMap =
                (XcalarApiRenameMap *) memAlloc(numCols * sizeof(*renameMap));
            BailIfNull(renameMap);

            status =
                QueryCmdParser::parseColumnsArray(columns, &err, renameMap);
            BailIfFailed(status);

            for (unsigned ii = 0; ii < numCols; ii++) {
                if (renameMap[ii].type == DfUnknown) {
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "must specify valid type for column %s in "
                                  "join eval input",
                                  renameMap[ii].oldName);
                    status = StatusInval;
                    goto CommonExit;
                }

                for (unsigned jj = 0; jj < tupMeta.getNumFields(); jj++) {
                    if (strcmp(renameMap[ii].newName, immediateNames[jj]) ==
                        0) {
                        xSyslogTxnBuf(moduleName,
                                      XlogErr,
                                      "duplicate field name %s",
                                      renameMap[ii].newName);
                        status = StatusImmediateNameCollision;
                        goto CommonExit;
                    }
                }

                tupMeta.addField(immediateNames,
                                 renameMap[ii].newName,
                                 renameMap[ii].type);
            }
        }
    }

    // copy over any newly created map fields
    for (unsigned ii = 0; ii < numMaps; ii++) {
        json_t *map = json_array_get(maps, ii);
        const char *evalString = "", *fieldName = "";

        ret = json_unpack_ex(map,
                             &err,
                             0,
                             QueryCmdParser::JsonEvalFormatString,
                             QueryCmdParser::NewFieldKey,
                             &fieldName,
                             QueryCmdParser::EvalStringKey,
                             &evalString);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        DfFieldType outputType =
            XcalarEval::get()->getOutputTypeXdf(evalString);

        for (unsigned kk = 0; kk < numKeys; kk++) {
            if (strcmp(keyNames[kk], fieldName) == 0) {
                keyReplaced = true;
                break;
            }
        }

        tupMeta.addField(immediateNames, fieldName, outputType);
    }

    // copy over any newly created groupBy fields
    for (unsigned ii = 0; ii < numGroupBys; ii++) {
        json_t *groupBy = json_array_get(groupBys, ii);
        const char *newFieldName = "", *accName = "", *groupField = "";
        AccumulatorType accType;
        DfFieldType argType = DfUnknown, outputType;

        ret = json_unpack_ex(groupBy,
                             &err,
                             0,
                             QpSelect::GroupByFormatString,
                             QpSelect::FunctionKey,
                             &accName,
                             QpSelect::ArgKey,
                             &groupField,
                             QueryCmdParser::NewFieldKey,
                             &newFieldName);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        accType = strToAccumulatorType(accName);
        if (!isValidAccumulatorType(accType)) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "%s in not a valid select group eval",
                          accName);
            status = StatusInval;
            goto CommonExit;
        }

        status = GroupEvalContext::getArgType(groupField,
                                              immediateNames,
                                              &tupMeta,
                                              argType);
        BailIfFailed(status);

        for (unsigned kk = 0; kk < numKeys; kk++) {
            if (strcmp(keyNames[kk], newFieldName) == 0) {
                keyReplaced = true;
                break;
            }
        }

        outputType = GroupEvalContext::getOutputType(accType, argType);
        tupMeta.addField(immediateNames, newFieldName, outputType);
    }

    if (keyReplaced) {
        numKeys = 1;
        keyNames[0] = DsDefaultDatasetKeyName;
        keyTypes[0] = DfInt64;
        keyIndexes[0] = InvalidIdx;
        keyOrderings[0] = Random;
    }

    if (input_->numColumns == 0) {
        // take all of the immediates and create an xdb
        status = XdbMgr::get()->xdbCreate(dstXdbId_,
                                          numKeys,
                                          keyNames,
                                          keyTypes,
                                          keyIndexes,
                                          keyOrderings,
                                          &tupMeta,
                                          NULL,
                                          0,
                                          immediateNames,
                                          tupMeta.getNumFields(),
                                          NULL,
                                          0,
                                          XdbGlobal,
                                          srcMeta->dhtId);
    } else {
        // we only want a subset of the columns
        NewTupleMeta tupMetaSubset;
        const char *immediateNamesSubset[TupleMaxNumValuesPerRecord];

        tupMetaSubset.setNumFields(input_->numColumns);

        if (!keyReplaced) {
            for (unsigned ii = 0; ii < numKeys; ii++) {
                keyIndexes[ii] = NewTupleMeta::DfInvalidIdx;

                // check if key has been renamed
                for (unsigned jj = 0; jj < input_->numColumns; jj++) {
                    if (strcmp(input_->columns[jj].oldName,
                               srcMeta->keyAttr[ii].name) == 0) {
                        keyNames[ii] = input_->columns[jj].newName;
                        keyIndexes[ii] = jj;

                        break;
                    }
                }

                if (keyIndexes[ii] == NewTupleMeta::DfInvalidIdx) {
                    if (ii == 0) {
                        // table does not have a key, use the default key
                        keyNames[0] = DsDefaultDatasetKeyName;
                        keyTypes[0] = DfInt64;
                        keyOrderings[0] = Random;
                        numKeys = 1;
                    } else {
                        // we didn't see the next key in the order, truncate the
                        // number of keys
                        numKeys = ii;
                    }
                    break;
                }
            }
        }

        for (unsigned ii = 0; ii < input_->numColumns; ii++) {
            unsigned jj;
            for (jj = 0; jj < tupMeta.getNumFields(); jj++) {
                if (strcmp(input_->columns[ii].oldName, immediateNames[jj]) ==
                    0) {
                    tupMetaSubset.setFieldType(tupMeta.getFieldType(jj), ii);
                    break;
                }
            }

            if (jj == tupMeta.getNumFields()) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Column %s not found in table %s",
                              input_->columns[ii].oldName,
                              input_->srcTable.tableName);
                status = StatusInval;
                goto CommonExit;
            }

            immediateNamesSubset[ii] = input_->columns[ii].newName;
        }

        // take the subset of the immediates and create an xdb
        status = XdbMgr::get()->xdbCreate(dstXdbId_,
                                          numKeys,
                                          keyNames,
                                          keyTypes,
                                          keyIndexes,
                                          keyOrderings,
                                          &tupMetaSubset,
                                          NULL,
                                          0,
                                          immediateNamesSubset,
                                          input_->numColumns,
                                          NULL,
                                          0,
                                          XdbGlobal,
                                          srcMeta->dhtId);
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed createXdb %lu: %s",
                dstXdbId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (evalInputJson) {
        json_decref(evalInputJson);
        evalInputJson = NULL;
    }

    if (renameMap) {
        memFree(renameMap);
    }

    return status;
}
