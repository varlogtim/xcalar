// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "service/AppService.h"
#include "app/AppMgr.h"

using namespace xcalar::compute::localtypes::App;

AppService::AppService() {}

ServiceAttributes
AppService::getAttr(const char *methodName)
{
    ServiceAttributes sattr;
    sattr.schedId = Runtime::SchedId::Immediate;
    return sattr;
}

Status
AppService::appStatus(
    const xcalar::compute::localtypes::App::AppStatusRequest *appStatusRequest,
    xcalar::compute::localtypes::App::AppStatusResponse *appStatusResponse)
{
    Status status = StatusOk;
    bool is_alive;
    is_alive = AppMgr::get()->isAppAlive(appStatusRequest->group_id());
    appStatusResponse->set_is_alive(is_alive);
    return status;
}

Status
AppService::driver(const DriverRequest *req, DriverResponse *resp)
{
    Status status;
    const char *driverAppName = AppMgr::DriverAppName;
    ::App *driverApp = NULL;
    char *appOutBlob = NULL;
    char *appOutput = NULL;
    char *errorStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    const char *userName = "";

    driverApp = AppMgr::get()->openAppHandle(driverAppName, &handle);
    if (driverApp == NULL) {
        status = StatusNoEnt;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Driver app %s does not exist",
                      driverAppName);
        goto CommonExit;
    }

    status = AppMgr::get()->runMyApp(driverApp,
                                     AppGroup::Scope::Local,
                                     userName,
                                     0,
                                     req->input_json().c_str(),
                                     0,
                                     &appOutBlob,
                                     &errorStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Driver app %s failed with output:%s, error:%s",
                      driverAppName,
                      appOutBlob,
                      errorStr);
        goto CommonExit;
    }

    status = AppMgr::extractSingleAppOut(appOutBlob, &appOutput);
    BailIfFailed(status);

    resp->set_output_json(appOutput);

CommonExit:
    if (driverApp) {
        AppMgr::get()->closeAppHandle(driverApp, handle);
        driverApp = NULL;
    }
    if (appOutBlob) {
        memFree(appOutBlob);
        appOutBlob = NULL;
    }
    if (appOutput) {
        memFree(appOutput);
        appOutput = NULL;
    }
    if (errorStr) {
        memFree(errorStr);
        errorStr = NULL;
    }
    return status;
}
