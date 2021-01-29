// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerImportRetina.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "dag/DagLib.h"

ApiHandlerImportRetina::ApiHandlerImportRetina(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerImportRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerImportRetina::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;

    status = DagLib::get()->importRetina(input_, output, outputSize);

    return status;
}

// Be compatible with XD here in terms of what constitutes a valid name for a
// batch dataflow. Only allowed chars are alphanumeric, hyphen or underscore
bool
ApiHandlerImportRetina::isValidNameChar(char c)
{
    return isalnum(c) || c == '-' || c == '_';
}

Status
ApiHandlerImportRetina::isValidName(char *name)
{
    Status status = StatusOk;

    const size_t len = strlen(name);
    for (unsigned ii = 0; ii < len; ii++) {
        if (!isValidNameChar(name[ii])) {
            xSyslog(moduleName,
                    XlogErr,
                    "Batch Dataflow Name '%s' is invalid due to char '%c'",
                    name,
                    name[ii]);
            status = StatusRetinaNameInvalid;
            break;
        }
    }
    return status;
}

Status
ApiHandlerImportRetina::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize = 0;

    assert((uintptr_t) input == (uintptr_t) &input->importRetinaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->importRetinaInput;

    status = isValidName(input_->retinaName);
    BailIfFailedMsg(moduleName,
                    status,
                    "Batch Dataflow name '%s' is invalid",
                    input_->retinaName);

    expectedInputSize = sizeof(*input_) + input_->retinaCount;
    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes, batchdataflowCount: %lu bytes",
                inputSize,
                expectedInputSize,
                input_->retinaCount);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
ApiHandlerImportRetina::serializeOutput(XcalarApiOutput *buf, size_t bufSize)
{
    uint64_t ii;
    XcalarApiImportRetinaOutput *importRetinaOutput;

    importRetinaOutput = &buf->outputResult.importRetinaOutput;
    for (ii = 0; ii < importRetinaOutput->numUdfModules; ii++) {
        importRetinaOutput->udfModuleStatuses[ii] =
            (XcalarApiUdfAddUpdateOutput *) XcalarApiMagic;
    }
    importRetinaOutput->udfModuleStatuses =
        (XcalarApiUdfAddUpdateOutput **) XcalarApiMagic;
    return StatusOk;
}
