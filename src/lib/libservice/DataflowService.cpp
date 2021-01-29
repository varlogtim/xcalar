// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <querymanager/QueryManager.h>
#include "primitives/Primitives.h"
#include "service/DataflowService.h"
#include "sys/XLog.h"
#include "operators/XcalarEval.h"

using namespace xcalar::compute::localtypes::Dataflow;

DataflowService::DataflowService() {}

ServiceAttributes
DataflowService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    // Just scheduling of DF is on Immediate Runtime, but the actual query
    // execution is parameterized.
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
DataflowService::execute(const ExecuteRequest *retExecuteRequest,
                         ExecuteResponse *retExecuteResponse)
{
    Status status = QueryManager::get()->processDataflow(retExecuteRequest,
                                                         retExecuteResponse);
    if (status != StatusOk) {
        goto CommonExit;
    }
CommonExit:
    return status;
}
