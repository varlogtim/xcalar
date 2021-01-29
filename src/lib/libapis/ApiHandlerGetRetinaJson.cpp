// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetRetinaJson.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"

ApiHandlerGetRetinaJson::ApiHandlerGetRetinaJson(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetRetinaJson::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately | NeedsSessionOrGraph);
}

Status
ApiHandlerGetRetinaJson::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;
    DagLib *dagLib = DagLib::get();

    // There are two types of retinas.
    //
    // 1) Created from a retina file and thus will have the UDF modules taken
    //    from the retina file and populated into the /dataflows namespace
    //
    // 2) Created from json where the UDF modules are separate and are found
    //    either in the workbook namespace (or sharedUdfs namespace).
    //
    // As we don't know which type this is we'll first assume it's 1) and
    // if that fails we'll try assuming 2)

    status = dagLib->getRetinaJson(input_, NULL, output, outputSize);
    if (status == StatusAstNoSuchFunction) {
        status = dagLib->getRetinaJson(input_,
                                       dstGraph_->getUdfContainer(),
                                       output,
                                       outputSize);
    }

    return status;
}

Status
ApiHandlerGetRetinaJson::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->getRetinaJsonInput.retinaName;

    status = DagLib::get()->isValidRetinaName(input_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "\"%s\" is not a valid batch dataflow: %s",
                input_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
