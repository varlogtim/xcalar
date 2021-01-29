// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerQueryState.h"
#include "msg/MessageTypes.h"
#include "util/MemTrack.h"
#include "querymanager/QueryManager.h"
#include "sys/XLog.h"
#include "dag/DagLib.h"

ApiHandlerQueryState::ApiHandlerQueryState(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerQueryState::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerQueryState::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;

    QueryManager *qm = QueryManager::get();
    status = qm->requestQueryState(output,
                                   outputSize,
                                   input_->queryName,
                                   input_->detailedStats);

    if (*output != NULL && *outputSize == 0) {
        // The "done" handler doesn't like the case where a buffer was allocated
        // but there's nothing in it.  So get rid of the buffer.
        memFree(*output);
        *output = NULL;
    }

    return status;
}

Status
ApiHandlerQueryState::setArg(XcalarApiInput *input, size_t inputSize)
{
    assert((uintptr_t) input == (uintptr_t) &input->queryStateInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->queryStateInput;
    return StatusOk;
}

Status
ApiHandlerQueryState::serializeOutput(XcalarApiOutput *buf, size_t bufSize)
{
    XcalarApiDagOutput *dagOutput;
    unsigned ii;

    dagOutput = &buf->outputResult.queryStateOutput.queryGraph;
    for (ii = 0; ii < dagOutput->numNodes; ii++) {
        dagOutput->node[ii] = (XcalarApiDagNode *) XcalarApiMagic;
    }

    return StatusOk;
}
