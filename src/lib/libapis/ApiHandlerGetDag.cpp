// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetDag.h"
#include "msg/MessageTypes.h"
#include "dag/Dag.h"
#include "dag/DagTypes.h"

ApiHandlerGetDag::ApiHandlerGetDag(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetDag::getFlags()
{
    return (Flags)(NeedsAck | NeedsSessionOrGraph | NeedsToRunImmediately);
}

Status
ApiHandlerGetDag::run(XcalarApiOutput **output, size_t *outputSize)
{
    *output = NULL;
    *outputSize = 0;
    return dstGraph_->getDagByName(input_,
                                   Dag::TableScope::LocalOnly,
                                   output,
                                   outputSize);
}

Status
ApiHandlerGetDag::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = input->dagTableNameInput.tableInput;
    return StatusOk;
}

Status
ApiHandlerGetDag::serializeOutput(XcalarApiOutput *buf, size_t bufSize)
{
    XcalarApiDagOutput *dagOutput;
    unsigned ii;

    dagOutput = &buf->outputResult.dagOutput;
    for (ii = 0; ii < dagOutput->numNodes; ii++) {
        dagOutput->node[ii] = (XcalarApiDagNode *) XcalarApiMagic;
    }

    return StatusOk;
}
