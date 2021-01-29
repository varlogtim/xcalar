// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerMakeRetina.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"

ApiHandlerMakeRetina::ApiHandlerMakeRetina(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerMakeRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerMakeRetina::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    DagLib *dagLib = DagLib::get();
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusUnknown;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    status = dagLib->makeRetina(dstGraph_, input_, inputSize_, true);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerMakeRetina::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->makeRetinaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->makeRetinaInput;

    status = xcalarApiDeserializeRetinaInput(input_, inputSize);
    for (uint64_t ii = 0; ii < input_->numSrcTables; ii++) {
        DagNodeTypes::Node *node;

        // XXX TODO May need to support fully qualified table names
        status = dstGraph_->lookupNodeByName(input_->srcTables[ii].source.name,
                                             &node,
                                             Dag::TableScope::LocalOnly,
                                             true);
        BailIfFailedTxnMsg(moduleName,
                           status,
                           "Could not find batch dataflow source %s:",
                           input_->srcTables[ii].source.name);

        // XXX: re-enable Aggregate once Xc-10730 is addressed
        if (node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate ||
            node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiExport) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Cannot use node %s as batch dataflow source, "
                          "%s is not a valid api",
                          input_->srcTables[ii].source.name,
                          strGetFromXcalarApis(
                              node->dagNodeHdr.apiDagNodeHdr.api));

            status = StatusInval;
            goto CommonExit;
        }
    }

    for (uint64_t ii = 0; ii < input_->numTargetTables; ++ii) {
        DagNodeTypes::Node *node;

        // Fail if the number of columns exceed the max allowed.
        if (input_->tableArray[ii]->numColumns > TupleMaxNumValuesPerRecord) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Number of columns (%u) exceeeds"
                          " maximum allowed (%u) for creating this batch "
                          "dataflow (%s)",
                          input_->tableArray[ii]->numColumns,
                          TupleMaxNumValuesPerRecord,
                          input_->retinaName);
            status = StatusFieldLimitExceeded;
            goto CommonExit;
        }

        // XXX TODO May need to support fully qualified table names
        status =
            dstGraph_->lookupNodeByName(input_->tableArray[ii]->target.name,
                                        &node,
                                        Dag::TableScope::LocalOnly,
                                        true);
        BailIfFailedTxnMsg(moduleName,
                           status,
                           "Could not find batch dataflow target %s:",
                           input_->tableArray[ii]->target.name);

        if (node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiBulkLoad ||
            node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate ||
            node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiExport) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Cannot use node %s as batch dataflow target, "
                          "%s is not a valid api",
                          input_->tableArray[ii]->target.name,
                          strGetFromXcalarApis(
                              node->dagNodeHdr.apiDagNodeHdr.api));

            status = StatusInval;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}
