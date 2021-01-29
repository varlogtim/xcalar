// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "service/ServiceMgr.h"
#include "xcalar/compute/localtypes/Service.xcrpc.h"
#include "service/ServiceRegistry.h"
#include "runtime/Runtime.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "libapis/LibApisRecv.h"

ServiceMgr *ServiceMgr::instance_ = NULL;

Status
ServiceMgr::initInternal()
{
    Status status = StatusOk;
    IService *service = NULL;
    statMgr_ = new (std::nothrow) RpcServiceStatMgr();
    if (statMgr_ == nullptr) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (int ii = 0; ii < (int) ArrayLen(serviceBuilders); ii++) {
        service = serviceBuilders[ii]();
        BailIfNull(service);
        status = registerService(service);
        BailIfFailed(status);
        service = NULL;
    }

    status = statMgr_->init();
    BailIfFailed(status);

    atomicWrite64(&mergeOpRoundRobin_, 0);

CommonExit:
    if (service) {
        delete service;
        service = NULL;
    }
    return status;
}

ServiceMgr::~ServiceMgr()
{
    services_.removeAll(&RegisteredService::destroy);
    delete statMgr_;
    statMgr_ = nullptr;
}

Status
ServiceMgr::registerService(IService *service)
{
    Status status = StatusOk;
    RegisteredMethod *registeredMethod = NULL;
    RegisteredService *registerEntry = NULL;
    int numMethods;
    const MethodInfo *methInfos;

    registerEntry = new (std::nothrow) RegisteredService(service);
    BailIfNull(registerEntry);

    methInfos = registerEntry->service->methods(&numMethods);
    assert(methInfos && "this is statically allocated and should not fail");

    for (int ii = 0; ii < numMethods; ii++) {
        registeredMethod = new (std::nothrow) RegisteredMethod(&methInfos[ii]);
        BailIfNull(registeredMethod);
        status = registerEntry->methods.insert(registeredMethod);
        BailIfFailed(status);
        status =
            statMgr_->insertEntry(service->name(), registeredMethod->getName());
        BailIfFailed(status);
        registeredMethod = NULL;
    }

    status = services_.insert(registerEntry);
    BailIfFailed(status);
    registerEntry = NULL;

CommonExit:
    if (registerEntry) {
        delete registerEntry;
        registerEntry = NULL;
    }
    if (registeredMethod) {
        delete registeredMethod;
        registeredMethod = NULL;
    }

    return status;
}

Status
ServiceMgr::handleApi(const ProtoRequestMsg *protoRequest,
                      ProtoResponseMsg *protoResponse)
{
    Status status;
    RegisteredService *regService;
    RegisteredMethod *regMethod;
    const ServiceRequest *serRequest = &protoRequest->servic();
    ServiceResponse *serResponse = protoResponse->mutable_servic();
    const char *serviceName = serRequest->servicename().c_str();
    const char *methodName = serRequest->methodname().c_str();
    ServiceAttributes sattr;
    bool startTxn = false;

    status = statMgr_->incoming2Stat(serRequest);
    BailIfFailed(status);

    // Look up method handler based on service and method name
    regService = services_.find(serviceName);
    if (regService == NULL) {
        status = StatusInval;
        xSyslog(ModuleName, XlogErr, "No such service '%s'", serviceName);
        goto CommonExit;
    }
    assert(regService->service && "service should be valid");

    regMethod = regService->methods.find(methodName);
    if (regMethod == NULL) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "No such method '%s' for service '%s'",
                methodName,
                serviceName);
        goto CommonExit;
    }
    assert(regMethod->method && "service method should be valid");
    assert(regMethod->method->invoker_ && "service method should be valid");

    if (!apisRecvInitialized()) {
        status = StatusClusterNotReady;
        xSyslog(ModuleName,
                XlogErr,
                "Failed service '%s' method '%s': %s",
                methodName,
                serviceName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = MsgMgr::get()->addTxnLog(Txn::currentTxn());
    BailIfFailed(status);

    startTxn = true;
    sattr = regService->service->getAttr(methodName);

    if (sattr.syslogRequest) {
        xSyslog(ModuleName,
                XlogInfo,
                "RPC incoming request: %s::%s",
                serviceName,
                methodName);
    }

    status = regMethod->method->invoker_(regService->service,
                                         serRequest,
                                         serResponse);
    if (sattr.syslogResponse || status != StatusOk) {
        xSyslog(ModuleName,
                XlogInfo,
                "RPC request done: %s::%s: %s",
                serviceName,
                methodName,
                strGetFromStatus(status));
    }
    BailIfFailed(status);

CommonExit:
    if (startTxn) {
        MsgMgr::TxnLog *txnLog = MsgMgr::get()->getTxnLog(Txn::currentTxn());
        if (txnLog != nullptr && txnLog->numMessages > 0) {
            // XXX: only return the first message for now
            protoResponse->set_error(txnLog->messages[0]);
        } else {
            protoResponse->set_error(status.message());
        }
        MsgMgr::get()->deleteTxnLog(Txn::currentTxn());
    } else {
        protoResponse->set_error(status.message());
    }
    verifyOk(statMgr_->done2Stat(serRequest, status));
    return status;
}

Txn
ServiceMgr::generateTxn(const ServiceRequest *request, Status *retStatus)
{
    Status status = StatusOk;
    RegisteredService *regService;
    RegisteredMethod *regMethod;
    const char *serviceName = request->servicename().c_str();
    const char *methodName = request->methodname().c_str();
    ServiceAttributes sattr;
    Txn genNewTxn = Txn();

    // Look up method handler based on service and method name
    regService = services_.find(serviceName);
    if (regService == NULL) {
        status = StatusInval;
        xSyslog(ModuleName, XlogErr, "No such service '%s'", serviceName);
        goto CommonExit;
    }
    assert(regService->service && "service should be valid");

    regMethod = regService->methods.find(methodName);
    if (regMethod == NULL) {
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "No such method '%s' for service '%s'",
                methodName,
                serviceName);
        goto CommonExit;
    }
    assert(regMethod->method && "service method should be valid");
    assert(regMethod->method->invoker_ && "service method should be valid");

    sattr = regService->service->getAttr(methodName);
    genNewTxn = Txn::newTxn(Txn::Mode::NonLRQ, sattr.schedId);

#ifdef DEBUG
    if (status == StatusOk) {
        assert(genNewTxn.valid());
    }
#endif

CommonExit:
    *retStatus = status;
    return genNewTxn;
}
