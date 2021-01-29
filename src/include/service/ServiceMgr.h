// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SERVICEMGR_H_
#define _SERVICEMGR_H_

#include <stat/StatisticsTypes.h>
#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Service.pb.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "util/StringHashTable.h"
#include "runtime/Runtime.h"
#include "runtime/Txn.h"
#include "service/RpcServiceStat.h"
#include "sys/XLog.h"

struct MethodInfo;

struct ServiceAttributes {
    Runtime::SchedId schedId = Runtime::SchedId::MaxSched;
    // Syslog incoming service & method name
    bool syslogRequest = true;
    // Syslog done service & method name with Status
    bool syslogResponse = true;
};

class IService
{
  public:
    typedef Status (*invoker)(IService *service,
                              const ServiceRequest *,
                              ServiceResponse *);

    virtual ~IService() = default;

    virtual const char *name() const = 0;
    virtual const MethodInfo *methods(int *numMethods) const = 0;
    virtual ServiceAttributes getAttr(const char *methodName) = 0;
};

struct MethodInfo {
    constexpr MethodInfo(const char *name, IService::invoker invoker)
        : name_(name), invoker_(invoker)
    {
    }

    const char *name_ = NULL;
    IService::invoker invoker_ = NULL;
};

class ServiceMgr
{
  private:
    static ServiceMgr *instance_;

  public:
    static constexpr const char *ModuleName = "ServiceMgr";
    MustCheck Status handleApi(const ProtoRequestMsg *protoRequest,
                               ProtoResponseMsg *protoResponse);

    MustCheck Status registerService(IService *service);

    MustCheck Txn generateTxn(const ServiceRequest *request, Status *retStatus);

    //
    // Singleton Stuff
    //
    MustCheck static Status init()
    {
        Status status = StatusOk;
        instance_ = new (std::nothrow) ServiceMgr;
        BailIfNull(instance_);
        status = instance_->initInternal();
        BailIfFailed(status);

    CommonExit:
        if (status != StatusOk && instance_) {
            delete instance_;
            instance_ = NULL;
        }
        return status;
    }
    void destroy()
    {
        if (instance_) {
            delete instance_;
            instance_ = NULL;
        }
    }
    MustCheck static ServiceMgr *get() { return instance_; }

    MustCheck Status incoming2Stat(const ServiceRequest *request);
    MustCheck Status done2Stat(const ServiceRequest *request);

  private:
    RpcServiceStatMgr *statMgr_ = nullptr;
    struct RegisteredMethod {
        const MethodInfo *method = NULL;
        StringHashTableHook hook;

        RegisteredMethod(const MethodInfo *theMethod) : method(theMethod) {}

        void destroy() { delete this; }

        const char *getName() const { return method->name_; }
    };
    typedef StringHashTable<RegisteredMethod,
                            &RegisteredMethod::hook,
                            &RegisteredMethod::getName,
                            101>
        MethodHashTable;

    struct RegisteredService {
        IService *service = NULL;
        StringHashTableHook hook;
        MethodHashTable methods;

        RegisteredService(IService *theService) : service(theService) {}

        void destroy()
        {
            methods.removeAll(&RegisteredMethod::destroy);
            if (service) {
                delete service;
                service = NULL;
            }
            delete this;
        }

        const char *getName() const { return service->name(); }
    };
    typedef StringHashTable<RegisteredService,
                            &RegisteredService::hook,
                            &RegisteredService::getName,
                            101>
        ServiceHashTable;

    ServiceMgr() = default;
    ~ServiceMgr();

    MustCheck Status initInternal();

    ServiceHashTable services_;
    Atomic64 mergeOpRoundRobin_;
};

template <typename service_t,
          typename request_t,
          typename response_t,
          Status (service_t::*handler)(const request_t *, response_t *)>
Status
invokeService(IService *service,
              const ServiceRequest *request,
              ServiceResponse *response)
{
    Status status;
    service_t *thisService = dynamic_cast<service_t *>(service);
    request_t req;
    response_t resp;

    bool success = request->body().UnpackTo(&req);
    if (!success) {
        status = StatusProtobufDecodeError;
        xSyslog(ServiceMgr::ModuleName,
                XlogErr,
                "Invalid request for %s:%s: %s",
                request->servicename().c_str(),
                request->methodname().c_str(),
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = (thisService->*(handler))(&req, &resp);
    BailIfFailed(status);

    response->mutable_body()->PackFrom(resp);

CommonExit:
    return status;
}

#endif  // _SERVICEMGR_H_
