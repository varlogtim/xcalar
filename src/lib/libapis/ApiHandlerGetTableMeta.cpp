// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetTableMeta.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "dataset/Dataset.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"
#include "operators/Operators.h"

ApiHandlerGetTableMeta::ApiHandlerGetTableMeta(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetTableMeta::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerGetTableMeta::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status = StatusUnknown;

    status = Operators::get()->getTableMeta(input_,
                                            output,
                                            outputSize,
                                            input_->tableNameInput.isTable
                                                ? fqTableState_.graphId
                                                : dstGraph_->getId(),
                                            userId_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error getting table meta for table %ld: %s",
                input_->tableNameInput.nodeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
ApiHandlerGetTableMeta::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    XcalarApiDagNode *dagNode = NULL;
    DsDataset *dataset = NULL;
    Dataset *ds = Dataset::get();
    DatasetRefHandle dsRefHandle;
    Dag *srcGraph = NULL;

    assert((uintptr_t) input == (uintptr_t) &input->getTableMetaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->getTableMetaInput;

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

    input_->tableNameInput.isTable = true;

    status = fqTableState_.setUp(input_->tableNameInput.name, dstGraph_);
    if (status == StatusOk) {
        status =
            fqTableState_.graph->getDagNodeId(input_->tableNameInput.name,
                                              Dag::TableScope::FullyQualOrLocal,
                                              &input_->tableNameInput.nodeId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retrieving dagNode \"%s\": %s",
                    input_->tableNameInput.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        srcGraph = fqTableState_.graph;
    } else if (status == StatusDagNodeNotFound) {
        status = ds->openHandleToDatasetByName(input_->tableNameInput.name,
                                               userId_,
                                               &dataset,
                                               LibNsTypes::ReaderShared,
                                               &dsRefHandle);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error retriving dataset for \"%s\": %s",
                    input_->tableNameInput.name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        input_->tableNameInput.xid = dataset->getDatasetId();
        input_->tableNameInput.isTable = false;
        srcGraph = dstGraph_;
        goto CommonExit;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieving dagNode \"%s\": %s",
                input_->tableNameInput.name,
                strGetFromStatus(status));
        goto CommonExit;
    } else {
        status = srcGraph->getDagNode(input_->tableNameInput.nodeId, &dagNode);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to retrieve dagNode \"%s\" (%lu): %s",
                    input_->tableNameInput.name,
                    input_->tableNameInput.nodeId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (dagNode->hdr.state != DgDagStateReady) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error! Dag node \"%s\" (%lu) in state: %s",
                    input_->tableNameInput.name,
                    input_->tableNameInput.nodeId,
                    strGetFromDgDagState(dagNode->hdr.state));
            status = StatusDgDagNodeError;
            goto CommonExit;
        }

        if (dagNode->hdr.api == XcalarApiBulkLoad) {
            input_->tableNameInput.isTable = false;
            status =
                srcGraph
                    ->getDatasetIdFromDagNodeId(input_->tableNameInput.nodeId,
                                                &input_->tableNameInput.xid);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error retrieving datasetId from dagNode \"%s\" (%lu): "
                        "%s",
                        input_->tableNameInput.name,
                        input_->tableNameInput.nodeId,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            input_->tableNameInput.isTable = true;
        }
    }

    status = StatusOk;
CommonExit:
    if (dataset != NULL) {
        Status status2 = ds->closeHandleToDataset(&dsRefHandle);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error closing dataset for \"%s\": %s",
                    input_->tableNameInput.name,
                    strGetFromStatus(status2));
        }
    }

    if (dagNode != NULL) {
        memFree(dagNode);
    }

    return status;
}
