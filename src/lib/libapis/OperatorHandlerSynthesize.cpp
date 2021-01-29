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
#include "libapis/OperatorHandlerSynthesize.h"
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

OperatorHandlerSynthesize::OperatorHandlerSynthesize(XcalarApis api)
    : OperatorHandler(api), input_(NULL)
{
}

OperatorHandlerSynthesize::~OperatorHandlerSynthesize()
{
    Dataset *ds = Dataset::get();
    if (dsRefHandleInit_) {
        Status status2 = ds->closeHandleToDataset(&dsRefHandle_);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error closing dataset for \"%s\": %s",
                    input_->source.name,
                    strGetFromStatus(status2));
        }
    }
}

ApiHandler::Flags
OperatorHandlerSynthesize::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerSynthesize::getDstNodeName()
{
    return input_->dstTable.tableName;
}

const char *
OperatorHandlerSynthesize::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.synthesizeOutput.tableName;
}

// See if we need to do any manipulation of the src xdb, if we don't
// simply use it as our dst xdb
bool
OperatorHandlerSynthesize::checkIfSrcXdbCompatibleForReuse(XdbId srcXdbId)
{
    Status status = StatusOk;
    if (input_->sameSession) {
        // we need to recreate this table
        return false;
    }

    // If filter string is there, always recreate the table. This is
    // because filter string could be brought in to synthesize node
    // by optimizer and filter string could be parameterized
    // as well
    if (input_->filterString[0] != '\0') {
        return false;
    }

    XdbMeta *srcMeta;
    status = XdbMgr::get()->xdbGet(srcXdbId, NULL, &srcMeta);
    if (status != StatusOk) {
        return false;
    }

    const NewTupleMeta *srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;

    unsigned jj;
    for (unsigned ii = 0; ii < input_->columnsCount; ii++) {
        for (jj = 0; jj < srcTupMeta->getNumFields(); jj++) {
            // see if all our requested fields are present in srcXdb
            // and we don't want to do any renames
            if (strcmp(srcMeta->kvNamedMeta.valueNames_[jj],
                       input_->columns[ii].oldName) == 0 &&
                strcmp(input_->columns[ii].oldName,
                       input_->columns[ii].newName) == 0) {
                break;
            }
        }

        if (jj == srcTupMeta->getNumFields()) {
            // could not find field, src incompatible
            return false;
        }
    }

    return true;
}

Status
OperatorHandlerSynthesize::setArg(XcalarApiInput *input,
                                  size_t inputSize,
                                  bool parentNodeIdsToBeProvided)
{
    Status status;
    XcalarEval *xcalarEval = XcalarEval::get();
    TableNsMgr *tnsMgr = TableNsMgr::get();

    assert((uintptr_t) input == (uintptr_t) &input->synthesizeInput);
    apiInput_ = input;
    input_ = &input->synthesizeInput;
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
    if (sizeof(*input_) + input_->columnsCount * sizeof(*input_->columns) !=
        inputSize) {
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

    if (input_->columnsCount > TupleMaxNumValuesPerRecord) {
        xSyslog(moduleName,
                XlogErr,
                "Too many columns %d > %d",
                input_->columnsCount,
                TupleMaxNumValuesPerRecord);
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

    if (XcalarEval::get()->containsUdf(input_->filterString)) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Synthesize filter %s cannot contain udfs",
                      input_->filterString);
        status = StatusInval;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < input_->columnsCount; ii++) {
        if (!xcalarEval->isValidFieldName(input_->columns[ii].newName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error with %s, Dst col name (%s) is not valid",
                    input_->dstTable.tableName,
                    input_->columns[ii].newName);
            status = StatusInval;
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

Status
OperatorHandlerSynthesize::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiNewTableOutput *synthesizeOutput = NULL;
    Status status;
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(*synthesizeOutput);
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

    synthesizeOutput = &output->outputResult.synthesizeOutput;
    assert((uintptr_t) synthesizeOutput == (uintptr_t) &output->outputResult);

    xSyslog(moduleName,
            XlogDebug,
            "Starting synthesize %s, nodeId %lu, xdbId %lu with source "
            "%s, nodeId %lu, xdbId %lu",
            input_->dstTable.tableName,
            input_->dstTable.tableId,
            input_->dstTable.xdbId,
            input_->source.name,
            input_->source.nodeId,
            input_->source.xid);

    if (srcXdbReused_) {
        xSyslog(moduleName,
                XlogInfo,
                "Synthesize %s, nodeId %lu, xdbId %lu reusing source "
                "%s, nodeId %lu, xdbId %lu",
                input_->dstTable.tableName,
                input_->dstTable.tableId,
                input_->dstTable.xdbId,
                input_->source.name,
                input_->source.nodeId,
                input_->source.xid);
    } else if (input_->aggResult != NULL) {
        // source was an aggregate
        status = dstGraph_->setScalarResult(dstNodeId_, input_->aggResult);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error binding scalar result to dagNode \"%s\" (%lu): %s",
                    input_->dstTable.tableName,
                    dstNodeId_,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        status = Operators::get()->synthesize(dstGraph_,
                                              input_,
                                              inputSize_,
                                              optimizerContext);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       synthesizeOutput->tableName,
                                       sizeof(synthesizeOutput->tableName));
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
OperatorHandlerSynthesize::getParentNodes(
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

    if (!input_->sameSession && dstGraphType_ == DagTypes::QueryGraph) {
        // XXX TODO
        // Synthesize sameSession flag is a violation of Shared Tables
        // abstraction and pre-dates it. Need to find a way to make this
        // flag deprecated.

        // Parent exists outside of this graph, so for Query parsing,
        // don't bother looking up the parent that's outsize this
        // Session scope.
        goto CommonExit;
    }

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
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getParentNodes table \"%s\" numParents %lu: %s",
                    input_->source.name,
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
OperatorHandlerSynthesize::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                         DagTypes::GraphType srcGraphType,
                                         const char *nodeName,
                                         uint64_t numParents,
                                         Dag **parentGraphs,
                                         DagTypes::NodeId *parentNodeIds)
{
    Status status;
    DsDataset *dataset = NULL;
    Dataset *ds = Dataset::get();

    // If dstGraph is a query, then we can make any operator the root
    // (i.e. operators don't need to have parents)
    if (!parentNodeIdsToBeProvided_ &&
        dstGraphType_ == DagTypes::WorkspaceGraph) {
        // Then we better make sure the names are valid
        assert(numParents == 1);
        status =
            parentGraphs[0]->getDagNodeId(input_->source.name,
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
                                                       &dsRefHandle_);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Error retrieving dataset for \"%s\": %s",
                            input_->source.name,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                dsRefHandleInit_ = true;
            }
        }

        if (input_->source.isTable) {
            status =
                parentGraphs[0]->getXdbId(input_->source.name,
                                          Dag::TableScope::FullyQualOrLocal,
                                          &input_->source.xid);
        } else {
            if (dataset == NULL) {
                status = ds->openHandleToDatasetByName(input_->source.name,
                                                       userId_,
                                                       &dataset,
                                                       LibNsTypes::ReaderShared,
                                                       &dsRefHandle_);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Error retrieving dataset for \"%s\": %s",
                            input_->source.name,
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                dsRefHandleInit_ = true;
            }

            input_->source.xid = dataset->datasetId_;
        }
    }

    if (dstGraphType_ == DagTypes::WorkspaceGraph) {
        if (numParents == 1) {
            input_->source.nodeId = parentNodeIds[0];

            if (input_->source.isTable) {
                input_->source.xid =
                    parentGraphs[0]->getXdbIdFromNodeId(input_->source.nodeId);
            } else {
                status =
                    dstGraph_->getDatasetIdFromDagNodeId(parentNodeIds[0],
                                                         &input_->source.xid);
                if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Could not look up datasetId from dagNodeId %lu",
                            parentNodeIds[0]);
                    goto CommonExit;
                }
            }
        }

        if (input_->source.isTable) {
            srcXdbReused_ = checkIfSrcXdbCompatibleForReuse(input_->source.xid);
            if (srcXdbReused_) {
                dstXdbId_ = input_->source.xid;
                input_->dstTable.xdbId = dstXdbId_;
            }
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
                "Failed to create synthesize node %s: %s",
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
OperatorHandlerSynthesize::createXdb(void *optimizerContext)
{
    Status status = StatusOk;
    DhtId dhtId = DhtInvalidDhtId;
    XdbMeta *srcMeta = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numImmediates = input_->columnsCount;
    const char *immediateNames[TupleMaxNumValuesPerRecord];
    NewTupleMeta tupMeta;
    unsigned numKeys;
    const char *keyNames[TupleMaxNumValuesPerRecord];
    DfFieldType keyTypes[TupleMaxNumValuesPerRecord];
    int keyIndexes[TupleMaxNumValuesPerRecord];
    Ordering ordering[TupleMaxNumValuesPerRecord];
    bool noKey = !input_->source.isTable;

    if (srcXdbReused_) {
        goto CommonExit;
    }

    if (input_->aggResult != NULL) {
        // don't need an xdb for aggregates
        goto CommonExit;
    }

    if (input_->source.isTable) {
        status = xdbMgr->xdbGet(input_->source.xid, NULL, &srcMeta);
        BailIfFailed(status);
    }

    // figure out the dst types using both input and the src meta
    tupMeta.setNumFields(input_->columnsCount);
    for (unsigned ii = 0; ii < input_->columnsCount; ii++) {
        DfFieldType dstType = input_->columns[ii].type;
        AccessorNameParser ap;
        Accessor ac;
        char *prefix = input_->columns[ii].oldName;
        size_t prefixLen = 0;
        if (strstr(input_->columns[ii].oldName, DfFatptrPrefixDelimiter)) {
            prefixLen =
                strstr(input_->columns[ii].oldName, DfFatptrPrefixDelimiter) -
                prefix;
        }

        status = ap.parseAccessor(input_->columns[ii].oldName, &ac);
        BailIfFailed(status);

        if (srcMeta != NULL) {
            DfFieldType srcType = DfUnknown;
            const NewTupleMeta *srcTupMeta =
                srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
            size_t jj;
            for (jj = 0; jj < srcTupMeta->getNumFields(); jj++) {
                if (srcTupMeta->getFieldType(jj) != DfFatptr) {
                    if (ac.nameDepth == 1 &&
                        strcmp(srcMeta->kvNamedMeta.valueNames_[jj],
                               ac.names[0].value.field) == 0) {
                        srcType = srcTupMeta->getFieldType(jj);
                        break;
                    }
                } else if (prefixLen) {
                    // Type is DfFatPtr
                    if (strlen(srcMeta->kvNamedMeta.valueNames_[jj]) ==
                            prefixLen &&
                        strncmp(srcMeta->kvNamedMeta.valueNames_[jj],
                                prefix,
                                prefixLen) == 0) {
                        break;
                    }
                } else if (strcmp(srcMeta->kvNamedMeta.valueNames_[jj],
                                  prefix) == 0) {
                    // Type is DfFatPtr and does not have
                    // DfFatptrPrefixDelimiter.
                    break;
                }
            }

            if (input_->sameSession == false &&
                jj == srcTupMeta->getNumFields()) {
                // we didn't find the field we were looking for in the src table

                if (prefixLen == 0) {
                    // for immediate fields, just error out
                    status = StatusInval;
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Could not find field %s",
                                  input_->columns[ii].oldName);
                    goto CommonExit;
                } else {
                    // if our field is a fatptr and it's missing from the
                    // src, there is a chance that the user has already
                    // converted the fatptr to an immediate.
                    // Try assuming that the user named the immediate
                    // by stripping away the prefix and taking just the
                    // columnName. for example: a::b -> b
                    char immediateName[XcalarApiMaxFieldNameLen + 1];
                    char *prefixPtr = strstr(input_->columns[ii].oldName,
                                             DfFatptrPrefixDelimiter);
                    assert(prefixPtr != NULL);

                    strlcpy(immediateName,
                            prefixPtr + strlen(DfFatptrPrefixDelimiter),
                            sizeof(immediateName));

                    for (jj = 0; jj < srcTupMeta->getNumFields(); jj++) {
                        if (srcTupMeta->getFieldType(jj) != DfFatptr) {
                            if (strcmp(srcMeta->kvNamedMeta.valueNames_[jj],
                                       immediateName) == 0) {
                                srcType = srcTupMeta->getFieldType(jj);
                                break;
                            }
                        }
                    }

                    if (jj == srcTupMeta->getNumFields()) {
                        // our trick did not work, report the field as missing
                        status = StatusInval;
                        xSyslogTxnBuf(moduleName,
                                      XlogErr,
                                      "Could not find field %s",
                                      input_->columns[ii].oldName);
                        goto CommonExit;
                    }

                    strlcpy(input_->columns[ii].oldName,
                            immediateName,
                            sizeof(immediateName));
                }
            }

            if (srcType != DfUnknown &&
                (dstType == DfScalarObj || dstType == DfUnknown)) {
                // if we don't care about the type, use the src type
                dstType = srcType;
            }
        }

        if (dstType == DfFatptr || dstType == DfUnknown) {
            // if the type is still not resolved, use ScalarObj
            dstType = DfScalarObj;
        }
        tupMeta.setFieldType(dstType, ii);

        immediateNames[ii] = input_->columns[ii].newName;
    }

    // setup key info
    if (input_->source.isTable) {
        numKeys = srcMeta->numKeys;
        dhtId = srcMeta->dhtId;

        for (unsigned ii = 0; ii < srcMeta->numKeys; ii++) {
            keyNames[ii] = srcMeta->keyAttr[ii].name;
            keyTypes[ii] = srcMeta->keyAttr[ii].type;
            ordering[ii] = srcMeta->keyOrderings[ii];
            keyIndexes[ii] = NewTupleMeta::DfInvalidIdx;

            if (strstr(keyNames[ii], DsDefaultDatasetKeyName)) {
                continue;
            }

            // check if key was kept
            for (unsigned jj = 0; jj < numImmediates; jj++) {
                if (strcmp(keyNames[ii], immediateNames[jj]) == 0) {
                    keyIndexes[ii] = jj;
                    tupMeta.setFieldType(keyTypes[ii], keyIndexes[ii]);
                    break;
                }
            }

            if (keyIndexes[ii] == NewTupleMeta::DfInvalidIdx) {
                // key was not kept, create a table without a key
                noKey = true;
                break;
            }
        }
    }

    if (noKey) {
        numKeys = 1;

        keyNames[0] = DsDefaultDatasetKeyName;
        keyIndexes[0] = NewTupleMeta::DfInvalidIdx;
        keyTypes[0] = DfInt64;

        ordering[0] = Random;
        status = DhtMgr::get()->dhtGetDhtId(DhtMgr::DhtSystemRandomDht, &dhtId);
        assert(status == StatusOk);
    }

    status = XdbMgr::get()->xdbCreate(dstXdbId_,
                                      numKeys,
                                      keyNames,
                                      keyTypes,
                                      keyIndexes,
                                      ordering,
                                      &tupMeta,
                                      NULL,
                                      0,
                                      (const char **) immediateNames,
                                      numImmediates,
                                      NULL,
                                      0,
                                      XdbGlobal,
                                      dhtId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Synthesize(%lu) failed createXdb %s",
                    input_->dstTable.tableId,
                    strGetFromStatus(status));

    xdbCreated_ = true;

CommonExit:
    return status;
}
