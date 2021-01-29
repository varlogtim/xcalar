// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerRuntimeGetParam.h"
#include "runtime/Runtime.h"
#include "sys/XLog.h"

ApiHandlerRuntimeGetParam::ApiHandlerRuntimeGetParam(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerRuntimeGetParam::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerRuntimeGetParam::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status = StatusOk;
    Config *config = Config::get();
    Runtime *runtime = Runtime::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned coreCount = MsgMgr::get()->numCoresOnNode(myNodeId);
    XcalarApiOutput *output = NULL;
    size_t outputSize =
        XcalarApiSizeOfOutput(output->outputResult.runtimeGetParamOutput);
    output = (XcalarApiOutput *) memAlloc(outputSize);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed runtime get params, insufficient memory to allocate "
                "output (Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    XcalarApiRuntimeGetParamOutput *paramOut;
    paramOut = &output->outputResult.runtimeGetParamOutput;
    paramOut->schedParamsCount = Runtime::TotalSdkScheds;

    for (uint8_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        unsigned threadsCount =
            runtime->getThreadsCount(static_cast<Runtime::SchedId>(ii));
        paramOut->schedParams[ii].runtimeType =
            runtime->getType(static_cast<Runtime::SchedId>(ii));
        paramOut->schedParams[ii].cpusReservedInPct =
            (threadsCount * 100) / coreCount;

        switch (static_cast<Runtime::SchedId>(ii)) {
        case Runtime::SchedId::Sched0:
            strlcpy(paramOut->schedParams[ii].schedName,
                    Runtime::NameSchedId0,
                    sizeof(paramOut->schedParams[ii].schedName));
            break;
        case Runtime::SchedId::Sched1:
            strlcpy(paramOut->schedParams[ii].schedName,
                    Runtime::NameSchedId1,
                    sizeof(paramOut->schedParams[ii].schedName));
            break;
        case Runtime::SchedId::Sched2:
            strlcpy(paramOut->schedParams[ii].schedName,
                    Runtime::NameSchedId2,
                    sizeof(paramOut->schedParams[ii].schedName));
            break;
        case Runtime::SchedId::Immediate:
            strlcpy(paramOut->schedParams[ii].schedName,
                    Runtime::NameImmediate,
                    sizeof(paramOut->schedParams[ii].schedName));
            break;

        default:
            assert(0 && "Invalid Runtime::Type");
            break;
        }
    }
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerRuntimeGetParam::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    apiInput_ = input;
    inputSize_ = inputSize;
    return status;
}
