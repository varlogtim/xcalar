// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RPCSERVICESTATMGR_H_
#define _RPCSERVICESTATMGR_H_
#include "primitives/Primitives.h"
#include <stat/StatisticsTypes.h>
#include <xcalar/compute/localtypes/Service.pb.h>
#include <sys/XLog.h>
#include "runtime/Runtime.h"
#include "stat/Statistics.h"
#include "strings/String.h"

class RpcServiceStatMgr
{
  public:
    static constexpr const int MAX_SERVICE_FULL_NAME_LENGTH = 256;

    RpcServiceStatMgr(){};

    ~RpcServiceStatMgr() { serviceTable.removeAll(&ServiceStatEntry::destroy); }

    MustCheck Status init();

    MustCheck Status incoming2Stat(const ServiceRequest* request);

    MustCheck Status done2Stat(const ServiceRequest* request,
                               Status doneStatus);

    static MustCheck Status getServiceKey(
        char dst[MAX_SERVICE_FULL_NAME_LENGTH], const ServiceRequest* request)
    {
        return getServiceKey(dst,
                             request->servicename().c_str(),
                             request->methodname().c_str());
    }

    static MustCheck Status
    getServiceKey(char dst[MAX_SERVICE_FULL_NAME_LENGTH],
                  const char* serviceName,
                  const char* methodName)
    {
        Status status = StatusOk;
        if (strlen(serviceName) + strlen(methodName) + strlen(DELIM) >
            MAX_SERVICE_FULL_NAME_LENGTH) {
            status = StatusNoBufs;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed get Service key name %s %s %s status : %s",
                    serviceName,
                    DELIM,
                    methodName,
                    strGetFromStatus(status));
            return status;
        }
        status = strStrlcpy(dst, serviceName, MAX_SERVICE_FULL_NAME_LENGTH);
        BailIfFailed(status);
        strcat(dst, DELIM);
        strcat(dst, methodName);
    CommonExit:
        return status;
    }

    Status insertEntry(const char* serviceName, const char* methodName)
    {
        Status status;
        auto entry = new (std::nothrow) ServiceStatEntry();
        if (entry == nullptr) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failed create stat entry %s %s %s",
                    serviceName,
                    DELIM,
                    methodName);
            goto CommonExit;
        }
        status = entry->init(serviceName, methodName);
        BailIfFailed(status);
        status = serviceTable.insert(entry);
        BailIfFailed(status);
    CommonExit:
        if (status != StatusOk) {
            if (entry != nullptr) {
                delete entry;
            }
        }
        return status;
    }

  private:
    static constexpr const int SLOT_SIZE = 128;

    struct ServiceStatEntry {
        char name[MAX_SERVICE_FULL_NAME_LENGTH];
        StringHashTableHook stringHook;
        StatHandle incoming = nullptr;
        StatHandle done = nullptr;

        const char* getName() const { return name; }

        ServiceStatEntry() {}

        MustCheck Status init(const char* serviceName, const char* methodName)
        {
            Status status = getServiceKey(name, serviceName, methodName);
            return status;
        }

        void destroy() { delete this; }
    };

    typedef StringHashTable<ServiceStatEntry,
                            &ServiceStatEntry::stringHook,
                            &ServiceStatEntry::getName,
                            SLOT_SIZE>
        ServiceStatTable;

  private:
    StatGroupId apisStatGroupId;
    ServiceStatTable serviceTable;
    static constexpr const char* DELIM = "::";
    static constexpr const char* moduleName = "RPCServiceStatMgr";
};

#endif
