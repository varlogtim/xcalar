// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/OperatorHandler.h"
#include "libapis/OperatorHandlerBulkLoad.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "msg/Xid.h"
#include "stat/Statistics.h"
#include "xdb/Xdb.h"
#include "util/Uuid.h"
#include "operators/XcalarEval.h"

OperatorHandlerBulkLoad::OperatorHandlerBulkLoad(XcalarApis api)
    : OperatorHandler(api), input_(NULL), preserveDatasetUuid_(false)
{
}

ApiHandler::Flags
OperatorHandlerBulkLoad::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsXdb | IsOperator);
}

const char *
OperatorHandlerBulkLoad::getDstNodeName()
{
    return input_->datasetName;
}

const char *
OperatorHandlerBulkLoad::getDstNodeName(XcalarApiOutput *output)
{
    return output->outputResult.loadOutput.dataset.name;
}

Status
OperatorHandlerBulkLoad::setPreserveDatasetUuidMode()
{
    preserveDatasetUuid_ = true;
    return StatusOk;
}

Status
OperatorHandlerBulkLoad::unsetPreserveDatasetUuidMode()
{
    preserveDatasetUuid_ = false;
    return StatusOk;
}

Status
OperatorHandlerBulkLoad::setArg(XcalarApiInput *input,
                                size_t inputSize,
                                bool parentNodeIdsToBeProvided)
{
    Status status;
    XcalarEval *xcalarEval = XcalarEval::get();
    Dataset *ds = Dataset::get();
    XcalarApiBulkLoadInput *inputFromCreateDataset = NULL;

    assert((uintptr_t) input == (uintptr_t) &input->loadInput);

    // Get the load input specified with the dataset creation.  If it cannot be
    // found we'll use the load input as specified in the call.  This is needed
    // to handle retina loading/execution.
    status = ds->getDatasetMetaAsApiInput(input->loadInput.datasetName,
                                          &inputFromCreateDataset);
    if (status != StatusOk && status != StatusDsMetaDataNotFound) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get dataset meta for '%s': %s",
                input->loadInput.datasetName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (status == StatusOk) {
        // Substitute current input with inputFromCreateDataset
        memcpy(input, inputFromCreateDataset, sizeof(*input));
        assert((uintptr_t) input == (uintptr_t) &input->loadInput);
    } else {
        // Use the API input supplied in the bulk load
        xSyslog(moduleName,
                XlogInfo,
                "Using dataset meta specified in load arguments for '%s' as "
                "pre-created dataset meta was not found.",
                input->loadInput.datasetName);
    }

    apiInput_ = input;
    input_ = &input->loadInput;
    inputSize_ = inputSize;
    parentNodeIdsToBeProvided_ = parentNodeIdsToBeProvided;
    dstNodeId_ = XidMgr::get()->xidGetNext();
    input_->dagNodeId = dstNodeId_;
    if (strlen(input_->loadArgs.xdbLoadArgs.keyName) > 0) {
        dstXdbId_ = XidMgr::get()->xidGetNext();
        input_->dstXdbId = dstXdbId_;
    } else {
        input_->dstXdbId = XdbIdInvalid;
    }

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

    if (input_->loadArgs.maxSize < 0) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid maxSize provided: %lu",
                input_->loadArgs.maxSize);
        status = StatusInval;
        goto CommonExit;
    }

    if (!xcalarEval->isValidDatasetName(input_->datasetName)) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid dataset name provided: %s",
                input_->datasetName);
        status = StatusInval;
        goto CommonExit;
    }

    if (!preserveDatasetUuid_) {
        // Generate a new datasetUuid
        status = uuidGenerate(&input_->loadArgs.datasetUuid);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to generate datasetUuid: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;

CommonExit:

    if (inputFromCreateDataset != NULL) {
        memFree(inputFromCreateDataset);
        inputFromCreateDataset = NULL;
    }

    return status;
}

Status
OperatorHandlerBulkLoad::runHandler(
    XcalarApiOutput **outputOut,
    size_t *outputSizeOut,
    void *optimizerContext,
    DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals])
{
    XcalarApiOutput *output = NULL;
    XcalarApiBulkLoadOutput *bulkLoadOutput = NULL;
    size_t outputSize = 0;
    bool dsHandleValid = false, dsCreated = false;
    Status status;
    Dataset *ds = Dataset::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    DsDataset *dataset = NULL;
    XdbId xdbId = XdbIdInvalid;
    void *dagNodeContext = NULL;
    DatasetRefHandle *dsRefHandle = NULL;

    assert(dstNodeId_ != DagTypes::InvalidDagNodeId);

    // Note that this dsRefHandle will be handed off to Dag load node and
    // will be freed when the Dag node is dropped.
    dsRefHandle = (DatasetRefHandle *) memAlloc(sizeof(*dsRefHandle));
    if (dsRefHandle == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = dstGraph_->getContext(dstNodeId_, &dagNodeContext);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not retrieve context from dagNodeId %lu: %s",
                dstNodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(dagNodeContext == NULL);

    outputSize = XcalarApiSizeOfOutput(*bulkLoadOutput);
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

    bulkLoadOutput = &output->outputResult.loadOutput;
    assert((uintptr_t) bulkLoadOutput == (uintptr_t) &output->outputResult);

    status = Operators::get()->loadDataset(dstGraph_,
                                           input_,
                                           optimizerContext,
                                           bulkLoadOutput,
                                           &dataset,
                                           dsRefHandle,
                                           &xdbId,
                                           userId_);
    if (status == StatusDsDatasetLoaded) {
        dsHandleValid = true;
        status = StatusOk;
    } else if (status == StatusOk) {
        dsCreated = true;
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = dstGraph_->getDagNodeName(dstNodeId_,
                                       bulkLoadOutput->dataset.name,
                                       sizeof(bulkLoadOutput->dataset.name));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not get name of dataset created: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(dsHandleValid || dsCreated);
    status = dstGraph_->setContext(dstNodeId_, dsRefHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not save dataset with dagNode %lu: %s",
                dstNodeId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    dataset = NULL;
    dsRefHandle = NULL;
    status = StatusOk;

CommonExit:

    if (status != StatusOk) {
        if (xdbId != XdbIdInvalid) {
            xdbMgr->xdbDrop(xdbId);
            xdbId = XdbIdInvalid;
        }

        if (dsHandleValid || dsCreated) {
            Status status2 = ds->closeHandleToDataset(dsRefHandle);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error closing dataset %s: %s",
                        dataset->name_,
                        strGetFromStatus(status2));
            }
        }

        if (dsCreated) {
            Status status2 = ds->unloadDataset(dataset);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error dropping dataset %s: %s",
                        dataset->name_,
                        strGetFromStatus(status2));
            }
        }

        if (dsRefHandle != NULL) {
            memFree(dsRefHandle);
            dsRefHandle = NULL;
        }
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
OperatorHandlerBulkLoad::getParentNodes(
    uint64_t *numParentsOut,
    XcalarApiUdfContainer **sessionContainersOut,
    DagTypes::NodeId **parentNodeIdsOut,
    DagTypes::DagId **parentGraphIdsOut)
{
    *numParentsOut = 0;
    *parentNodeIdsOut = NULL;
    *parentGraphIdsOut = NULL;
    *sessionContainersOut = NULL;
    return StatusOk;
}

Status
OperatorHandlerBulkLoad::prefixDatasetName(const char *nameIn,
                                           char *nameBuf,
                                           size_t nameBufSize)
{
    char tmpName[DagTypes::MaxNameLen + 1];
    size_t ret;
    const char *name = nameIn;

    if (strncmp(nameIn, XcalarApiDatasetPrefix, XcalarApiDatasetPrefixLen) ==
        0) {
        // This dataset already has a prefix, don't need to do it again
        snprintf(nameBuf, nameBufSize, "%s", name);
        return StatusOk;
    }

    if (nameIn[0] == '\0') {
        ret = snprintf(tmpName,
                       sizeof(tmpName),
                       XcalarTempDagNodePrefix "%lu",
                       XidMgr::get()->xidGetNext());
        assert(ret < sizeof(tmpName));
        if (ret >= sizeof(tmpName)) {
            // There's nohing the user could have done. We're just handling
            // this error for completeness sake
            return StatusNoBufs;
        }
        name = tmpName;
    }

    ret = snprintf(nameBuf, nameBufSize, XcalarApiDatasetPrefix "%s", name);
    if (ret >= nameBufSize) {
        xSyslog(moduleName,
                XlogErr,
                "Dataset name \"" XcalarApiDatasetPrefix
                "%s\" is too long "
                "(%lu chars). Max is %lu chars.",
                name,
                ret,
                nameBufSize);
        return StatusNoBufs;
    }

    return StatusOk;
}

Status
OperatorHandlerBulkLoad::createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                       DagTypes::GraphType srcGraphType,
                                       const char *nodeNameIn,
                                       uint64_t numParents,
                                       Dag **parentGraphs,
                                       DagTypes::NodeId *parentNodeIds)
{
    Status status;
    char nameBuf[DagTypes::MaxNameLen + 1];
    const char *nodeName = nodeNameIn;
    size_t ret;

    if (srcGraphType == DagTypes::InvalidGraph && dstXdbId_ == XidInvalid) {
        status = prefixDatasetName(nodeNameIn, nameBuf, sizeof(nameBuf));
        if (status != StatusOk) {
            goto CommonExit;
        }

        ret =
            strlcpy(input_->datasetName, nameBuf, sizeof(input_->datasetName));
        if (ret >= sizeof(input_->datasetName)) {
            xSyslog(moduleName,
                    XlogErr,
                    "Dataset name \"%s\" is too long (%lu chars). Max is %lu "
                    "chars",
                    nameBuf,
                    ret,
                    sizeof(input_->datasetName));
            status = StatusNoBufs;
            goto CommonExit;
        }
        nodeName = nameBuf;
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
        // Change to a more user-friendly msg
        if (status == StatusDgDagAlreadyExists) {
            status = StatusDatasetNameAlreadyExists;
        }

        xSyslog(moduleName,
                XlogErr,
                "Failed to create load node %s: %s",
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
OperatorHandlerBulkLoad::createXdb(void *optimizerContext)
{
    Status status = StatusOk;

    if (strlen(input_->loadArgs.xdbLoadArgs.keyName) > 0) {
        status = Dataset::get()->createDatasetXdb(input_);
        BailIfFailed(status);

        xdbCreated_ = true;
    }

CommonExit:
    return status;
}
