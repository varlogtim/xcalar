// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/CgroupService.h"
#include "util/CgroupMgr.h"
#include "util/MemTrack.h"

using namespace xcalar::compute::localtypes::Cgroup;

ServiceAttributes
CgroupService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
CgroupService::process(const CgRequest *cgRequest, CgResponse *cgResponse)
{
    Status status = StatusOk;
    const char *jsonInput = cgRequest->jsoninput().c_str();
    char *jsonOutput = NULL;
    size_t jsonOutputLen = 0;

    status = CgroupMgr::get()->process(jsonInput,
                                       &jsonOutput,
                                       CgroupMgr::Type::External);
    if (status != StatusOk) {
        goto CommonExit;
    }

    jsonOutputLen = strlen(jsonOutput);

    try {
        cgResponse->set_jsonoutput(jsonOutput, jsonOutputLen);
    } catch (std::exception &e) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

CommonExit:
    if (jsonOutput != NULL) {
        memFree(jsonOutput);
        jsonOutput = NULL;
    }
    return status;
}
