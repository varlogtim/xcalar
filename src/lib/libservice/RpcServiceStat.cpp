// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "service/RpcServiceStat.h"

MustCheck Status
RpcServiceStatMgr::init()
{
    Status status;
    StatsLib* statsLib = StatsLib::get();
    uint64_t sz = 2 * serviceTable.getSize();
    status = statsLib->initNewStatGroup("xcrpcApis", &apisStatGroupId, sz);
    for (auto iter = serviceTable.begin(); iter.get() != nullptr; iter.next()) {
        auto entryPtr = iter.get();
        status = statsLib->initStatHandle(&(entryPtr->incoming));
        BailIfFailed(status);
        status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                             entryPtr->getName(),
                                             entryPtr->incoming,
                                             StatUint64,
                                             StatAbsoluteWithNoRefVal,
                                             StatRefValueNotApplicable);
        BailIfFailed(status);
        status = statsLib->initStatHandle(&(entryPtr->done));
        BailIfFailed(status);
        status = statsLib->initAndMakeGlobal(apisStatGroupId,
                                             entryPtr->getName(),
                                             entryPtr->done,
                                             StatUint64,
                                             StatAbsoluteWithNoRefVal,
                                             StatRefValueNotApplicable);
        BailIfFailed(status);
    }
CommonExit:
    return status;
}

Status
RpcServiceStatMgr::incoming2Stat(const ServiceRequest* request)
{
    Status status = StatusUnknown;
    char serviceKey[MAX_SERVICE_FULL_NAME_LENGTH];

    status = getServiceKey(serviceKey, request);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed RPC incoming request: %s::%s: %s, '%s'",
                request->servicename().c_str(),
                request->methodname().c_str(),
                strGetFromStatus(status),
                request->DebugString().c_str());
        return status;
    }
    auto statEntry = serviceTable.find(serviceKey);
    if (statEntry == nullptr) {
        xSyslog(moduleName,
                XlogErr,
                "Unknown RPC incoming request: %s::%s, '%s'",
                request->servicename().c_str(),
                request->methodname().c_str(),
                serviceKey);
        return StatusInval;
    }
    StatsLib::statAtomicIncr64(statEntry->incoming);
    status = StatusOk;
    return status;
}

Status
RpcServiceStatMgr::done2Stat(const ServiceRequest* request, Status doneStatus)
{
    Status status = StatusUnknown;
    char serviceKey[MAX_SERVICE_FULL_NAME_LENGTH];
    status = getServiceKey(serviceKey, request);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed RPC incoming request: %s::%s: %s, '%s'",
                request->servicename().c_str(),
                request->methodname().c_str(),
                strGetFromStatus(status),
                request->DebugString().c_str());
        return status;
    }
    auto statEntry = serviceTable.find(serviceKey);
    if (statEntry == nullptr) {
        // This shouldn't happen since if key not exists, the incoming2Stat will
        // fail
        xSyslog(moduleName,
                XlogErr,
                "Failed RPC incoming Request: %s::%s: %s, '%s'",
                request->servicename().c_str(),
                request->methodname().c_str(),
                strGetFromStatus(status),
                request->DebugString().c_str());
        return StatusInval;
    }
    StatsLib::statAtomicIncr64(statEntry->done);
    StatsLib::statAtomicDecr64(statEntry->incoming);

    return StatusOk;
}
