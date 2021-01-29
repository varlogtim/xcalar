// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetRetina.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"

ApiHandlerGetRetina::ApiHandlerGetRetina(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetRetina::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetRetina::run(XcalarApiOutput **output, size_t *outputSize)
{
    return DagLib::get()->getRetina(input_, output, outputSize);
}

Status
ApiHandlerGetRetina::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->getRetinaInput.retInput;

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

Status
ApiHandlerGetRetina::serializeOutput(XcalarApiOutput *buf, size_t bufSize)
{
    XcalarApiGetRetinaOutput *getRetinaOutput;
    XcalarApiDagOutput *dagOutput;
    unsigned ii;

    getRetinaOutput = &buf->outputResult.getRetinaOutput;
    dagOutput = &getRetinaOutput->retina.retinaDag;

    for (ii = 0; ii < dagOutput->numNodes; ii++) {
        dagOutput->node[ii] = (XcalarApiDagNode *) XcalarApiMagic;
    }

    return StatusOk;
}
