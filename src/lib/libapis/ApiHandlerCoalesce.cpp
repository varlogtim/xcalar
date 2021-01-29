// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerCoalesce.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"
#include "ns/LibNs.h"

ApiHandlerCoalesce::ApiHandlerCoalesce(XcalarApis api) : ApiHandler(api) {}

ApiHandlerCoalesce::~ApiHandlerCoalesce()
{
    if (handleInit_) {
        HashTreeMgr::get()->closeHandleToHashTree(&refHandle_,
                                                  input_->srcTable.tableId,
                                                  input_->srcTable.tableName);
    }
}

ApiHandler::Flags
ApiHandlerCoalesce::getFlags()
{
    return (Flags)(NeedsAck);
}

Status
ApiHandlerCoalesce::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusOk;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed coalesce on publish table %s %lu Insufficient memory "
                "to allocate output (Required size: %lu bytes)",
                input_->srcTable.tableName,
                input_->srcTable.tableId,
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    status = HashTreeMgr::get()->coalesceHashTree(&refHandle_,
                                                  input_->srcTable.tableName,
                                                  input_->srcTable.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed coalesce on publish table %s %lu: %s",
                    input_->srcTable.tableName,
                    input_->srcTable.tableId,
                    strGetFromStatus(status));

CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerCoalesce::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &apiInput_->coalesceInput;

    status =
        HashTreeMgr::get()->openHandleToHashTree(input_->srcTable.tableName,
                                                 LibNsTypes::WriterExcl,
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
                      "Error retrieving hashtree for \"%s\": %s",
                      input_->srcTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    if (refHandle_.restoring) {
        status = StatusPubTableRestoring;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Error retrieving hashtree for \"%s\": %s",
                      input_->srcTable.tableName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}
