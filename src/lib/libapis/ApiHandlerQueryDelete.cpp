// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerQueryDelete.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "querymanager/QueryManager.h"
#include "dag/DagLib.h"

ApiHandlerQueryDelete::ApiHandlerQueryDelete(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerQueryDelete::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerQueryDelete::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    QueryManager *qm = QueryManager::get();

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        goto CommonExit;
    }

    status = qm->requestQueryDelete(input_->queryName);

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        outputSize = 0;
    }

    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerQueryDelete::setArg(XcalarApiInput *input, size_t inputSize)
{
    assert((uintptr_t) input == (uintptr_t) &input->queryStateInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->queryStateInput;
    return StatusOk;
}
