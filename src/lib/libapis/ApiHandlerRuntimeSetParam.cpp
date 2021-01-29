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
#include "libapis/ApiHandlerRuntimeSetParam.h"
#include "runtime/Runtime.h"
#include "gvm/Gvm.h"
#include "msg/Message.h"
#include "sys/XLog.h"

ApiHandlerRuntimeSetParam::ApiHandlerRuntimeSetParam(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerRuntimeSetParam::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerRuntimeSetParam::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status = StatusOk;
    Runtime *rt = Runtime::get();

    status = rt->changeThreadsCount(input_);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to set runtime params: %s",
                    strGetFromStatus(status));

CommonExit:
    return status;
}

Status
ApiHandlerRuntimeSetParam::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;

    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->runtimeSetParamInput;
    MsgMgr *msgMgr = MsgMgr::get();
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned coreCount = MsgMgr::get()->numCoresOnNode(myNodeId);
    XcalarConfig *xc = XcalarConfig::get();
    Runtime::SchedId schedId[Runtime::TotalSdkScheds];

    for (uint8_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        schedId[ii] = Runtime::SchedId::MaxSched;
    }

    // Needs Symmetric/same core count on all nodes in the cluster.
    if (!msgMgr->symmetricalNodes()) {
        status = StatusRuntimeSetParamNotSupported;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Set runtime params unsupported, since cluster does not "
                      "have symmerical nodes: %s",
                      strGetFromStatus(status));
        goto CommonExit;
    }

    // Needs sufficient CPU resources on the host server.
    if (coreCount < xc->runtimeMixedModeMinCores_) {
        status = StatusRuntimeSetParamNotSupported;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Set runtime params unsupported, since number of CPUs %u "
                      "is less than %u: %s",
                      coreCount,
                      xc->runtimeMixedModeMinCores_,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    // Needs to set all the schedulers CPU resource partitions.
    if (input_->schedParamsCount != Runtime::TotalSdkScheds) {
        status = StatusRuntimeSetParamInvalid;
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Set runtime params schedParamsCount %u must be equal to "
                      "%u: %s",
                      input_->schedParamsCount,
                      Runtime::TotalSdkScheds,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    for (uint8_t ii = 0; ii < Runtime::TotalSdkScheds; ii++) {
        XcalarApiSchedParam *schedParam =
            (XcalarApiSchedParam *) ((uintptr_t) input_->schedParams +
                                     sizeof(XcalarApiSchedParam) * ii);
        Runtime::SchedId curSchedId = Runtime::SchedId::MaxSched;

        // Scheduler names need to match valid names and cannot repeat.
        curSchedId = Runtime::getSchedIdFromName(schedParam->schedName,
                                                 sizeof(schedParam->schedName));
        if (curSchedId == Runtime::SchedId::MaxSched) {
            status = StatusRuntimeSetParamInvalid;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to set runtime params, Scheduler name %s not "
                          "valid: %s",
                          schedParam->schedName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        uint8_t jj = 0;
        for (jj = 0; jj < Runtime::TotalSdkScheds; jj++) {
            if (schedId[jj] == curSchedId) {
                break;
            }
        }

        if (jj == Runtime::TotalSdkScheds) {
            schedId[ii] = curSchedId;
        } else {
            status = StatusRuntimeSetParamInvalid;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to set runtime params, Scheduler name %s "
                          "repeats: %s",
                          schedParam->schedName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        // Scheduler type needs to be valid.
        switch (schedId[ii]) {
        case Runtime::SchedId::Sched0:
            if (schedParam->runtimeType != RuntimeTypeThroughput) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Sched1:
            if (schedParam->runtimeType != RuntimeTypeThroughput &&
                schedParam->runtimeType != RuntimeTypeLatency) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Sched2:
            if (schedParam->runtimeType != RuntimeTypeLatency) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        case Runtime::SchedId::Immediate:
            if (schedParam->runtimeType != RuntimeTypeImmediate) {
                status = StatusRuntimeSetParamInvalid;
            }
            break;
        default:
            status = StatusRuntimeSetParamInvalid;
            break;
        }

        if (status != StatusOk) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to set runtime params, Scheduler name %s, "
                          "cannot have type %u: %s",
                          schedParam->schedName,
                          schedParam->runtimeType,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        if (schedParam->cpusReservedInPct == 0 ||
            ((schedParam->cpusReservedInPct * coreCount) / 100) == 0) {
            status = StatusRuntimeSetParamInvalid;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed to set runtime params, Scheduler name %s, "
                          "type %u, cpusReservedInPct %u not valid, since "
                          "scheduler needs to have atleast 1 thread: %s",
                          schedParam->schedName,
                          schedParam->runtimeType,
                          schedParam->cpusReservedInPct,
                          strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}
