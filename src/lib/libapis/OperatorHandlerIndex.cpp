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
#include "libapis/OperatorHandlerIndex.h"
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
#include "udf/UserDefinedFunction.h"
#include "strings/String.h"

OperatorHandlerIndex::OperatorHandlerIndex(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

ApiHandler::Flags
OperatorHandlerIndex::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerIndex::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerIndex::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.indexOutput.tableName;
}

// converts DfFatptrPrefixDelimiter into -
void
OperatorHandlerIndex::generatePrefixedKeyName(char *keyName)
{
    char *prefixPtr = strstr(keyName, DfFatptrPrefixDelimiter);
    char *fieldName = prefixPtr + strlen(DfFatptrPrefixDelimiter);

    prefixPtr[0] = '-';

    prefixPtr++;
    for (unsigned ii = 0; ii <= strlen(fieldName); ii++) {
        prefixPtr[ii] = fieldName[ii];
    }
}

Status
OperatorHandlerIndex::setArg(XcalarApiInput *input,
                             size_t inputSize,
                             bool parentNodeIdsToBeProvided)
{
    Status status;
    XcalarEval *xcalarEval = XcalarEval::get();
    DhtMgr *dhtMgr = DhtMgr::get();
    DhtId dhtId;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->indexInput);
    apiInput_ = input;
    input_ = &input->indexInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;

    // Grab the parent DagNode Id incase you need to create the DagNode for
    // Bulk load later on. Note that this is done before getting the DagNode ID
    // for index to maintain the chronology.
    parentNodeId_ = XidMgr::get()->xidGetNext();

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
                "Expected size = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    if (strncmp(input_->source.name,
                XcalarApiDatasetPrefix,
                XcalarApiDatasetPrefixLen) == 0) {
        input_->source.isTable = false;
    } else {
        input_->source.isTable = true;
    }

    if (input_->dhtName[0] == '\0' &&
        input_->keys[0].ordering == OrderingInvalid) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Either dhtName or ordering must be provided");
        status = StatusInval;
        goto CommonExit;
    }

    if (input_->dhtName[0] != '\0') {
        status = dhtMgr->dhtGetDhtId(input_->dhtName, &dhtId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get Dht Id for '%s': %s",
                    input_->dhtName,
                    strGetFromStatus(status));
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

    if (!input_->source.isTable) {
        if (!xcalarEval->isValidFieldName(input_->fatptrPrefixName)) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Prefix name (%s) is not valid",
                          input_->fatptrPrefixName);
            status = StatusInval;
            goto CommonExit;
        }
    }

    for (unsigned ii = 0; ii < input_->numKeys; ii++) {
        if (strlen(input_->keys[ii].keyFieldName) == 0) {
            if (strcmp(input_->keys[ii].keyName, DsDefaultDatasetKeyName) ==
                0) {
                // don't rename default key
                strlcpy(input_->keys[ii].keyFieldName,
                        input_->keys[ii].keyName,
                        sizeof(input_->keys[ii].keyFieldName));
            } else if (input_->source.isTable) {
                strlcpy(input_->keys[ii].keyFieldName,
                        input_->keys[ii].keyName,
                        sizeof(input_->keys[ii].keyFieldName));

                if (strstr(input_->keys[ii].keyFieldName,
                           DfFatptrPrefixDelimiter)) {
                    generatePrefixedKeyName(input_->keys[ii].keyFieldName);
                } else {
                    AccessorNameParser ap;
                    Accessor accessors;
                    status = ap.parseAccessor(input_->keys[ii].keyFieldName,
                                              &accessors);
                    BailIfFailed(status);

                    if (accessors.nameDepth > 1) {
                        // We cannot have a nested field accessor
                        // for not prefixed fields
                        // there must have been an error with the string
                        status = StatusInval;
                        xSyslogTxnBuf(moduleName,
                                      XlogErr,
                                      "Field %s cannot have nested delimiters",
                                      input_->keys[ii].keyFieldName);
                        goto CommonExit;
                    }

                    strlcpy(input_->keys[ii].keyFieldName,
                            accessors.names[0].value.field,
                            sizeof(input_->keys[ii].keyFieldName));
                }
            } else {
                snprintf(input_->keys[ii].keyFieldName,
                         sizeof(input_->keys[ii].keyFieldName),
                         "%s-%s",
                         input_->fatptrPrefixName,
                         input_->keys[ii].keyName);
            }
        }

        if (input_->keys[ii].type == DfScalarObj) {
            // ScalarObj is a meta type and cannot be a keyType on its own
            // default to DfUnknown
            input_->keys[ii].type = DfUnknown;
        }

        if (strcmp(input_->keys[ii].keyName, DsDefaultDatasetKeyName) == 0) {
            input_->keys[ii].type = DfInt64;
        }

        if (!xcalarEval->isValidFieldName(input_->keys[ii].keyFieldName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Key field name (%s) is not valid",
                    input_->keys[ii].keyFieldName);
            status = StatusInval;
            goto CommonExit;
        }
    }

    for (unsigned ii = 0; ii < input_->numKeys; ii++) {
        for (unsigned jj = ii + 1; jj < input_->numKeys; jj++) {
            if (strcmp(input_->keys[ii].keyFieldName,
                       input_->keys[jj].keyFieldName) == 0) {
                xSyslogTxnBuf(moduleName,
                              XlogErr,
                              "Key field name (%s) is duplicated",
                              input_->keys[ii].keyFieldName);
                status = StatusInval;
                goto CommonExit;
            }
        }
    }
CommonExit:
    return status;
}

Status
OperatorHandlerIndex::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *indexOutput = NULL;
    Status status;
    size_t outputSize = 0;
    DgDagState state = DgDagStateError;
    unsigned parIdx = 0;

    outputSize = XcalarApiSizeOfOutput(*indexOutput);
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

    indexOutput = &output->outputResult.indexOutput;
    assert((uintptr_t) indexOutput == (uintptr_t) &output->outputResult);

    if (input_->source.isTable) {
        status = parentGraphs_[parIdx]->getDagNodeState(input_->source.nodeId,
                                                        &state);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get state of table \"%s\" (%lu): %s",
                    input_->source.name,
                    input_->source.nodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (state != DgDagStateReady) {
            xSyslog(moduleName,
                    XlogErr,
                    "Parent table \"%s\" (%lu) in state: %s. Aborting index",
                    input_->source.name,
                    input_->source.nodeId,
                    strGetFromDgDagState(state));
            status = StatusDgDagNodeError;
            goto CommonExit;
        }
    }

    status =
        Operators::get()->index(dstGraph_, input_, optimizerContext, userId_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       indexOutput->tableName,
                                       sizeof(indexOutput->tableName));
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
OperatorHandlerIndex::createDagNodeForTrackingDatasetRefCount(
    const char *datasetName,
    DagTypes::NodeId parentNodeId,
    XcalarApiUdfContainer *udfContainer)
{
    // Dataset doesn't exist. That means someone else
    // must have loaded it for us
    // Lookup in shared dataset pool
    DsDataset *dataset = NULL;
    DagTypes::NodeId dagNodeId = DagTypes::InvalidDagNodeId;
    Status status;
    OperatorHandler *operatorHandler = NULL;
    Dataset *ds = Dataset::get();
    XcalarWorkItem *workItem = NULL;
    DatasetRefHandle *dsRefHandle = NULL;
    bool dagNodeCreated = false;
    Txn prevTxn = Txn();

    dsRefHandle = (DatasetRefHandle *) memAlloc(sizeof(*dsRefHandle));
    if (dsRefHandle == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // This will increase refCount on dataset
    status = ds->openHandleToDatasetByName(datasetName,
                                           userId_,
                                           &dataset,
                                           LibNsTypes::ReaderShared,
                                           dsRefHandle);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(strstr(datasetName, XcalarApiDatasetPrefix) == datasetName);

    workItem = xcalarApiMakeBulkLoadWorkItem(datasetName, &dataset->loadArgs_);
    if (workItem == NULL) {
        xSyslog(moduleName, XlogErr, "Failed to create workItem");
        status = StatusNoMem;
        goto CommonExit;
    }

    assert(dstGraphType_ == DagTypes::WorkspaceGraph);
    status = xcApiGetOperatorHandlerInited(&operatorHandler,
                                           workItem,
                                           userId_,
                                           dstGraph_,
                                           true,
                                           &prevTxn);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Use the parentNode Id stashed upstream and override default DagNodeId,
    // so the chronology of Dag branches are maintained.
    operatorHandler->dstNodeId_ = parentNodeId;
    status = operatorHandler->createDagNode(&dagNodeId, DagTypes::QueryGraph);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dagNodeCreated = true;
    status = dstGraph_->setContext(dagNodeId, dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not save dataset with dagNode %lu: %s",
                dagNodeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = UserDefinedFunction::get()
                 ->copyUdfToWorkbook(dataset->loadArgs_.parseArgs.parserFnName,
                                     udfContainer);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to copy Scalar Function '%s' to workbook '%s' for user "
                "'%s': %s",
                dataset->loadArgs_.parseArgs.parserFnName,
                udfContainer->sessionInfo.sessionName,
                udfContainer->userId.userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    dataset = NULL;  // Handed off to dagNode
    dsRefHandle = NULL;

    status = dstGraph_->changeDagNodeState(dagNodeId, DgDagStateReady);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (dataset != NULL) {
        Status status2 = ds->closeHandleToDataset(dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not close dataset %s: %s",
                    dataset->name_,
                    strGetFromStatus(status2));
        }
        dataset = NULL;
    }
    if (prevTxn.valid()) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(prevTxn);
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (status != StatusOk) {
        if (dagNodeCreated) {
            // Only safe to change to error state if we have the reference to it
            Status status2 =
                dstGraph_->dropAndChangeState(dagNodeId, DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        dagNodeId,
                        strGetFromStatus(status2));
            }
        }

        if (dsRefHandle != NULL) {
            memFree(dsRefHandle);
            dsRefHandle = NULL;
        }
    }

    return status;
}

Status
OperatorHandlerIndex::trackExistingDatasetNode(DagTypes::NodeId nodeId)
{
    Status status = StatusOk;
    dstGraph_->lock();
    bool graphLocked = true;
    DsDataset *dataset = NULL;
    DatasetRefHandle *dsRefHandle = NULL;

    DagNodeTypes::Node *dagNode;

    status = dstGraph_->lookupNodeById(nodeId, &dagNode);
    BailIfFailed(status);

    if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiBulkLoad) {
        goto CommonExit;
    }

    if (dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped) {
        // need to grab another ref
        dsRefHandle = (DatasetRefHandle *) memAlloc(sizeof(*dsRefHandle));
        if (dsRefHandle == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        // This will increase refCount on dataset
        status =
            Dataset::get()->openHandleToDatasetByName(input_->source.name,
                                                      userId_,
                                                      &dataset,
                                                      LibNsTypes::ReaderShared,
                                                      dsRefHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not get ref on dataset %s: %s",
                    input_->source.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = dstGraph_->setContext(nodeId, dsRefHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not save dataset with dagNode %lu: %s",
                    nodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        dataset = NULL;  // Handed off to dagNode
        dsRefHandle = NULL;

        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateReady;
    }

CommonExit:
    if (graphLocked) {
        dstGraph_->unlock();
        graphLocked = false;
    }

    if (dataset != NULL) {
        Status status2 = Dataset::get()->closeHandleToDataset(dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Could not close dataset %s: %s",
                    dataset->name_,
                    strGetFromStatus(status2));
        }
        dataset = NULL;
    }

    if (dsRefHandle) {
        memFree(dsRefHandle);
        dsRefHandle = NULL;
    }

    return status;
}

Status
OperatorHandlerIndex::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    uint64_t numParents = 0;
    Status status;
    DagTypes::NodeId *parentNodeIds = NULL;
    DagTypes::DagId *parentGraphIds = NULL;
    bool parentNodeCreated = false;
    unsigned parIdx = 0;
    XcalarApiUdfContainer *sessionContainers = NULL;

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
    status = getSourceDagNode(input_->source.name,
                              dstGraph_,
                              &sessionContainers[parIdx],
                              &parentGraphIds[parIdx],
                              &parentNodeIds[parIdx]);
    if (status == StatusDagNodeNotFound &&
        dstGraphType_ == DagTypes::WorkspaceGraph && !input_->source.isTable) {
        // Get the parent Dag Node Id we stashed away in setArgs.
        parentNodeIds[parIdx] = parentNodeId_;
        parentGraphIds[parIdx] = dstGraph_->getId();
        UserDefinedFunction::copyContainers(&sessionContainers[parIdx],
                                            dstGraph_->getSessionContainer());

        // This tracking dag node is created when the XD user creates a table
        // from a dataset loaded by another user without first locking the
        // dataset. This dag node effectivelly provides the lock.
        status =
            createDagNodeForTrackingDatasetRefCount(input_->source.name,
                                                    parentNodeIds[parIdx],
                                                    dstGraph_
                                                        ->getUdfContainer());
        if (status != StatusOk) {
            goto CommonExit;
        }
        parentNodeCreated = true;
    } else if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "fail to retrieve dagNodeId for \"%s\": %s",
                input_->source.name,
                strGetFromStatus(status));
        goto CommonExit;
    } else {
        assert(status == StatusOk);
        if (dstGraphType_ == DagTypes::WorkspaceGraph &&
            !input_->source.isTable) {
            // A tracking node has already been created, however it could have
            // been dropped, causing it to release it's reference on the dataset
            // Need to get the ref back and reset the state
            status = trackExistingDatasetNode(parentNodeIds[parIdx]);
            BailIfFailed(status);
        }
    }

CommonExit:
    if (status != StatusOk) {
        if (parentNodeCreated) {
            // Only safe to change to error state if we have the reference to it
            Status status2 =
                dstGraph_->dropAndChangeState(parentNodeIds[parIdx],
                                              DgDagStateError);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop and change dagNode (%lu) state to "
                        "Error: %s",
                        parentNodeIds[parIdx],
                        strGetFromStatus(status2));
            }
            parentNodeCreated = false;
            parentNodeIds[parIdx] = DagTypes::InvalidDagNodeId;
        }

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
OperatorHandlerIndex::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                    DagTypes::GraphType srcGraphType,
                                    const char *nodeName,
                                    uint64_t numParents,
                                    Dag **parentGraphs,
                                    DagTypes::NodeId *parentNodeIds)
{
    DsDataset *dataset = NULL;
    Dataset *ds = Dataset::get();
    DatasetRefHandle dsRefHandle;
    Status status;
    unsigned parIdx = 0;

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    if (!parentNodeIdsToBeProvided_ &&
        dstGraphType_ == DagTypes::WorkspaceGraph) {
        // Then we better make sure the names are valid
        status = parentGraphs[parIdx]
                     ->getDagNodeId(input_->source.name,
                                    Dag::TableScope::FullyQualOrLocal,
                                    &input_->source.nodeId);
        if (status != StatusOk) {
            if (input_->source.isTable) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving tableId for \"%s\": %s",
                        input_->source.name,
                        strGetFromStatus(status));
                goto CommonExit;
            } else {
                status = ds->openHandleToDatasetByName(input_->source.name,
                                                       userId_,
                                                       &dataset,
                                                       LibNsTypes::ReaderShared,
                                                       &dsRefHandle);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Error retrieving dataset for \"%s\": %s",
                            input_->source.name,
                            strGetFromStatus(status));
                    goto CommonExit;
                }

                input_->source.xid = dataset->datasetId_;
            }
        }

        status =
            parentGraphs[parIdx]->getXdbId(input_->source.name,
                                           Dag::TableScope::FullyQualOrLocal,
                                           &input_->source.xid);
        BailIfFailed(status);
    }

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
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

        if (input_->source.isTable) {
            input_->source.nodeId = parentNodeIds[parIdx];
            input_->source.xid =
                parentGraphs[parIdx]->getXdbIdFromNodeId(parentNodeIds[parIdx]);
        } else {
            status = parentGraphs[parIdx]
                         ->getDatasetIdFromDagNodeId(parentNodeIds[parIdx],
                                                     &input_->source.xid);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Could not look up datasetId from dagNodeId %lu",
                        parentNodeIds[parIdx]);
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
                "Failed to create index node %s: %s",
                nodeName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (dataset != NULL) {
        Status status2 = ds->closeHandleToDataset(&dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error closing dataset for \"%s\": %s",
                    input_->source.name,
                    strGetFromStatus(status2));
        }
    }

    if (status != StatusOk) {
        if (dstNodeId_ != DagTypes::InvalidDagNodeId) {
            // Only safe to change to error state if we have the reference to it
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
OperatorHandlerIndex::createXdb(void *optimizerContext)
{
    Status status = StatusOk;
    DsDatasetId srcDatasetId;
    XdbId srcXdbId = XidInvalid;
    DhtId dhtId = DhtInvalidDhtId;
    char customDhtName[DfMaxFieldNameLen];
    DhtMgr *dhtMgr = DhtMgr::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    NewTupleMeta tupMeta;
    DsDatasetId *datasetIds = NULL;
    const char **immediateNames = NULL;
    const char **fatptrPrefixNames = NULL;
    unsigned numImmediates = 0, numFatptrs = 0, numDatasets = 0;
    XdbMeta *srcMeta;
    Ordering keyOrdering[input_->numKeys];
    int keyIndex[input_->numKeys];
    DfFieldType keyTypes[input_->numKeys];
    const char *keyNames[input_->numKeys];

    immediateNames = (const char **) memAlloc(TupleMaxNumValuesPerRecord *
                                              sizeof(*immediateNames));
    BailIfNull(immediateNames);

    fatptrPrefixNames = (const char **) memAlloc(TupleMaxNumValuesPerRecord *
                                                 sizeof(*fatptrPrefixNames));
    BailIfNull(fatptrPrefixNames);

    datasetIds = (DsDatasetId *) memAlloc(TupleMaxNumValuesPerRecord *
                                          sizeof(*datasetIds));
    BailIfNull(datasetIds);

    if (input_->source.isTable) {
        if (optimizerContext) {
            // need to let the optimizer know about our new key names
            for (unsigned ii = 0; ii < input_->numKeys; ii++) {
                char keyName[DfMaxFieldNameLen + 1];

                status = strStrlcpy(keyName,
                                    input_->keys[ii].keyFieldName,
                                    sizeof(keyName));
                BailIfFailed(status);

                status =
                    Optimizer::get()->addField((Optimizer::DagNodeAnnotations *)
                                                   optimizerContext,
                                               keyName,
                                               "",
                                               true);
                BailIfFailed(status);
            }
        }
        srcDatasetId = DsDatasetIdInvalid;
        srcXdbId = input_->source.xid;

        status = xdbMgr->xdbGet(srcXdbId, NULL, &srcMeta);
        BailIfFailed(status);

        status = copySrcXdbMeta(optimizerContext,
                                srcMeta,
                                &tupMeta,
                                numDatasets,
                                datasetIds,
                                numImmediates,
                                immediateNames,
                                numFatptrs,
                                fatptrPrefixNames);
        BailIfFailed(status);

        for (unsigned ii = 0; ii < input_->numKeys; ii++) {
            // Index table case only
            char keyNameTmp[XcalarApiMaxFieldNameLen + 1];
            strlcpy(keyNameTmp, input_->keys[ii].keyName, sizeof(keyNameTmp));

            keyIndex[ii] = NewTupleMeta::DfInvalidIdx;
            keyNames[ii] = input_->keys[ii].keyFieldName;
            keyTypes[ii] = input_->keys[ii].type;
            keyOrdering[ii] = input_->keys[ii].ordering;

            if (strstr(input_->keys[ii].keyName, DsDefaultDatasetKeyName)) {
                continue;
            }

            // obtain keyIndex from immediate fields
            unsigned immSeen = 0;
            for (size_t jj = 0; jj < tupMeta.getNumFields(); jj++) {
                DfFieldType fieldType = tupMeta.getFieldType(jj);
                if (fieldType == DfFatptr) {
                    continue;
                }

                if (strcmp(keyNames[ii], immediateNames[immSeen]) == 0) {
                    keyIndex[ii] = jj;

                    if (fieldType != DfScalarObj) {
                        keyTypes[ii] = fieldType;
                    } else if (keyTypes[ii] != DfUnknown) {
                        tupMeta.setFieldType(keyTypes[ii], jj);
                    }
                }

                immSeen++;
            }

            if (keyIndex[ii] == NewTupleMeta::DfInvalidIdx) {
                // update tupMeta and add key as a new immediate
                keyIndex[ii] = tupMeta.getNumFields();
                if (keyIndex[ii] == TupleMaxNumValuesPerRecord) {
                    status = StatusFieldLimitExceeded;
                    goto CommonExit;
                }

                tupMeta.setFieldType(keyTypes[ii], keyIndex[ii]);
                tupMeta.setNumFields(tupMeta.getNumFields() + 1);

                immediateNames[numImmediates++] = keyNames[ii];
            }
        }
    } else {
        srcDatasetId = input_->source.xid;

        tupMeta.setNumFields(1);
        tupMeta.setFieldType(DfFatptr, 0);
        assert(srcDatasetId != DsDatasetIdInvalid);

        numDatasets = numFatptrs = 1;
        numImmediates = 0;
        datasetIds[0] = srcDatasetId;
        fatptrPrefixNames[0] = input_->fatptrPrefixName;

        for (unsigned ii = 0; ii < input_->numKeys; ii++) {
            keyTypes[ii] = input_->keys[ii].type;
            keyNames[ii] = input_->keys[ii].keyName;
            keyOrdering[ii] = input_->keys[ii].ordering;

            if (strstr(input_->keys[ii].keyName, DsDefaultDatasetKeyName)) {
                keyIndex[ii] = NewTupleMeta::DfInvalidIdx;
                keyTypes[ii] = DfInt64;
                keyOrdering[ii] = Random;
            } else {
                // add key as immediate
                // update tupMeta
                keyIndex[ii] = tupMeta.getNumFields();
                if (keyIndex[ii] == TupleMaxNumValuesPerRecord) {
                    status = StatusFieldLimitExceeded;
                    goto CommonExit;
                }
                tupMeta.setFieldType(keyTypes[ii], keyIndex[ii]);
                tupMeta.setNumFields(tupMeta.getNumFields() + 1);

                keyNames[ii] = input_->keys[ii].keyFieldName;
                immediateNames[numImmediates++] = keyNames[ii];
            }
        }
    }

    if (input_->dhtName[0] == '\0') {
        if (input_->keys[0].ordering == OrderingInvalid) {
            // can only specify 1 of dhtName or ordering
            return StatusInval;
        }

        // Use default dht
        if (input_->broadcast) {
            status = dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemBroadcastDht, &dhtId);
        } else if (input_->keys[0].ordering & PartiallySortedFlag ||
                   !(input_->keys[0].ordering & SortedFlag)) {
            if (input_->keys[0].ordering == Random ||
                strstr(input_->keys[0].keyName, DsDefaultDatasetKeyName)) {
                input_->keys[0].ordering = Random;
                status =
                    dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemRandomDht, &dhtId);
            } else {
                status =
                    dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemUnorderedDht, &dhtId);
            }
        } else {
            if (input_->source.isTable) {
                // create a new dht after calculating min/max of the key
                // you want to sort on, use the dstTableName as it's name
                // this will be deleted after index has finished
                // The dhtId is returned from the creation as we cannot call
                // dhtGetDhtId since that does an open/close which would
                // auto-remove the newly created dht.
                snprintf(customDhtName,
                         sizeof(customDhtName),
                         "%s%lu",
                         XcalarApiTempDhtPrefix,
                         input_->dstTable.tableId);
                status = op_->createRangeHashDht(dstGraph_,
                                                 input_->source.nodeId,
                                                 srcXdbId,
                                                 input_->dstTable.tableId,
                                                 input_->dstTable.xdbId,
                                                 input_->keys[0].keyName,
                                                 customDhtName,
                                                 input_->keys[0].ordering,
                                                 userId_,
                                                 &dhtId);
                BailIfFailedMsg(moduleName,
                                status,
                                "Index(%lu) failed createDht %s",
                                input_->dstTable.tableId,
                                strGetFromStatus(status));
            } else {
                if (input_->keys[0].ordering & AscendingFlag) {
                    status = dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemAscendingDht,
                                                 &dhtId);
                } else if (input_->keys[0].ordering & DescendingFlag) {
                    status = dhtMgr->dhtGetDhtId(DhtMgr::DhtSystemDescendingDht,
                                                 &dhtId);
                } else {
                    assert(0);
                }

                BailIfFailedMsg(moduleName,
                                status,
                                "Index(%lu) failed getDht %s",
                                input_->dstTable.tableId,
                                strGetFromStatus(status));
                // System DHTs are created during startup and
                // should always be found
                assert(status != StatusNsNotFound);
            }
        }
    } else {
        // Get the DHT object from the global name space so that the
        // ordering info can be obtained.
        Dht *dht;
        status = dhtMgr->dhtGetDhtId(input_->dhtName, &dhtId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to get DHT ID for '%s': %s",
                    input_->dhtName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        dht = dhtMgr->dhtGetDht(dhtId);
        assert(dht != NULL);

        if (dht->ordering == Random) {
            keyOrdering[0] = input_->keys[0].ordering;
        } else {
            keyOrdering[0] = dht->ordering;
        }
    }

    status = xdbMgr->xdbCreate(dstXdbId_,
                               input_->numKeys,
                               keyNames,
                               keyTypes,
                               keyIndex,
                               keyOrdering,
                               &tupMeta,
                               datasetIds,
                               numDatasets,
                               &immediateNames[0],
                               numImmediates,
                               fatptrPrefixNames,
                               numFatptrs,
                               XdbGlobal,
                               dhtId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Index(%lu) failed xdbCreate %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

CommonExit:
    memFree(fatptrPrefixNames);
    memFree(immediateNames);
    memFree(datasetIds);

    return status;
}
