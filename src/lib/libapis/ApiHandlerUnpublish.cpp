// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerUnpublish.h"
#include "msg/MessageTypes.h"
#include "dag/DagLib.h"
#include "xdb/HashTree.h"
#include "sys/XLog.h"

ApiHandlerUnpublish::ApiHandlerUnpublish(XcalarApis api) : ApiHandler(api) {}

ApiHandler::Flags
ApiHandlerUnpublish::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerUnpublish::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    Status status = StatusUnknown;
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;
    Stopwatch stopwatch;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "Failed unpublish table %s %s to allocate output "
                "(Required size: %lu bytes)",
                input_->srcTable.tableName,
                input_->inactivateOnly ? "True" : "False",
                outputSize);
        outputSize = 0;
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "Unpublish table %s %s started",
            input_->srcTable.tableName,
            input_->inactivateOnly ? "True" : "False");

    stopwatch.restart();
    status = HashTreeMgr::get()->destroyHashTree(input_->srcTable.tableName,
                                                 input_->inactivateOnly);
    stopwatch.stop();
    stopwatch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
    if (status == StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Unpublish table %s %s finished in %lu:%02lu:%02lu.%03lu",
                input_->srcTable.tableName,
                input_->inactivateOnly ? "True" : "False",
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver);
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Unpublish table %s %s failed in %lu:%02lu:%02lu.%03lu: %s",
                input_->srcTable.tableName,
                input_->inactivateOnly ? "True" : "False",
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(status == StatusOk);

CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerUnpublish::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &apiInput_->unpublishInput;

    return StatusOk;
}
