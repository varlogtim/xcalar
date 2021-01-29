// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerExportRetina.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "dag/DagLib.h"
#include "udf/UserDefinedFunction.h"

ApiHandlerExportRetina::ApiHandlerExportRetina(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerExportRetina::getFlags()
{
    // export retina may have to extract UDF py code into json from the retina
    // and the UDFs are in the retina sandbox; so NeedsSessionOrGraph flag is
    // unnecessary
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerExportRetina::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;

    // Export retina may have to extract UDF py code into json from the retina
    // and the UDFs are in the retina sandbox; so container must have retina.
    // This udfContainer can be derived from the retina->dag, when the latter is
    // created by getRetinaId, in exportRetinaInt() which is invoked by
    // exportRetina()

    status = DagLib::get()->exportRetina(input_, output, outputSize);
    return status;
}

Status
ApiHandlerExportRetina::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->exportRetinaInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->exportRetinaInput;

    status = DagLib::get()->isValidRetinaName(input_->retinaName);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error retrieving batch dataflow \"%s\": %s",
                input_->retinaName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
