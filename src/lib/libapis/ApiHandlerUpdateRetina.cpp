// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUpdateRetina.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "udf/UserDefinedFunction.h"

ApiHandlerUpdateRetina::ApiHandlerUpdateRetina(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerUpdateRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerUpdateRetina::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    Status status = StatusUnknown;
    XcalarApiUdfContainer *udfContainer = NULL;

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

    udfContainer =
        (XcalarApiUdfContainer *) memAllocExt(sizeof(XcalarApiUdfContainer),
                                              moduleName);

    BailIfNull(udfContainer);

    // Note that the parsing of the input query string (supplied via
    // input_->retinaJson) is expected to only contain references to UDFs which
    // are already in the retina. There's no way today for the user via XD, to
    // add new UDFs to a retina, or to insert references to workbook UDFs in the
    // input query string here. So the container must always be the retina
    // container here.
    status = UserDefinedFunction::initUdfContainer(udfContainer,
                                                   NULL,
                                                   NULL,
                                                   input_->retinaName);
    BailIfFailed(status);

    status = DagLib::get()->updateRetina(input_->retinaName,
                                         input_->retinaJson,
                                         input_->retinaJsonCount,
                                         udfContainer);
CommonExit:
    if (udfContainer != NULL) {
        memFree(udfContainer);
        udfContainer = NULL;
    }

    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

// Only 3 types of parameterization are allowed now
bool
ApiHandlerUpdateRetina::isValidParamType(XcalarApis paramType)
{
    return paramType == XcalarApiFilter || paramType == XcalarApiBulkLoad ||
           paramType == XcalarApiExport;
}

Status
ApiHandlerUpdateRetina::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->updateRetinaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->updateRetinaInput;

    status = DagLib::get()->isValidRetinaName(input_->retinaName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "\"%s\" is not a valid batch dataflow: %s,",
                input_->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
