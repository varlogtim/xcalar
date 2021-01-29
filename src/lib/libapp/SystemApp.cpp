// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "app/AppMgr.h"
#include "app/SystemApp.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "libapis/LibApisRecv.h"
#include "runtime/Txn.h"

///////////////////////// SystemApp                         ////////////////////

Status
SystemApp::completeInit(SystemApp *instance, const char *appInput)
{
    DCHECK(instance != nullptr);
    DCHECK(appInput != nullptr);

    auto trxn = Txn::currentTxn();
    Txn::setTxn(Txn::newImmediateTxn());
    Status status;

    instance->appInputBlob_ = new (std::nothrow) std::string(appInput);
    BailIfNull(instance->appInputBlob_);

    instance->task_ = new (std::nothrow) Task(instance);
    BailIfNull(instance->task_);

    status = Runtime::get()->schedule(instance->task_);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        xSyslog(mname_,
                XlogErr,
                "Failed to schedule a system app: %s, status=%s",
                instance->name(),
                strGetFromStatus(status));
        delete instance->task_;
        instance->task_ = nullptr;
    }

    Txn::setTxn(trxn);
    return status;
}

SystemApp::~SystemApp()
{
    if (task_) task_->doneSem_.semWait();
    delete appInputBlob_;
    appInputBlob_ = nullptr;
    delete task_;
    task_ = nullptr;
}

Status
SystemApp::run(AppGroup::Id *appGroupId) const
{
    LibNsTypes::NsHandle handle;
    auto app = AppMgr::get()->openAppHandle(name(), &handle);
    if (app == nullptr) {
        xSyslog(mname_, XlogErr, "The system app '%s' does not exist", name());
        return StatusAppDoesNotExist;
    }

    xSyslog(mname_, XlogInfo, "Starting a System app: %s", app->getName());

    DCHECK(appInputBlob_ != nullptr);
    char *errStr = nullptr;
    bool appInternalError = false;
    Status status = StatusUnknown;

    status = AppMgr::get()->runMyAppAsync(app,
                                          AppGroup::Scope::Global,
                                          "",
                                          0,
                                          appInputBlob_->c_str(),
                                          0 /* cookie */,
                                          appGroupId,
                                          &errStr,
                                          &appInternalError);
    if (status != StatusOk) {
        xSyslog(mname_,
                XlogErr,
                "System app '%s' failed with status : "
                "'%s', "
                "error: '%s'",
                app->getName(),
                strGetFromStatus(status),
                errStr ? errStr : "");
        goto CommonExit;
    }

    xSyslog(mname_, XlogInfo, "Launched a System App: %s", app->getName());

CommonExit:
    if (app != nullptr) AppMgr::get()->closeAppHandle(app, handle);
    if (errStr) {
        memFree(errStr);
        errStr = nullptr;
    }
    return status;
}

///////////////////////// SystemApp::Task                   ////////////////////

SystemApp::Task::Task(SystemApp *owner)
    : Schedulable(owner->name()), owner_(owner)
{
    doneSem_.init(0);
}

void
SystemApp::Task::done()
{
    doneSem_.post();
}

void
SystemApp::Task::run()
{
    // This function is run as a fiber, polling every AppCheckTimeoutUsecs
    // usecs. This function only exits when usrnode shutdown has been initiated.
    //
    // In each loop, we poll the system app to see if it is still up.
    // If the app has died because it has  been shot down for some reason, we
    // will try to re-run the app again.
    // If user wants to stop the app, they can change the config knobs related
    // to each app
    Status status;
    bool appInternalError = false;
    AppGroup::Id appGroupId = XidInvalid;
    auto *appName = owner_->name();
    Semaphore sema(0);

    while (true) {
        bool appEnabled = owner_->isAppEnabled();
        if (usrNodeNormalShutdown()) {
            // usrnode going down, check if app running already and abort before
            // we exit
            if (appGroupId != XidInvalid) {
                xSyslog(mname_,
                        XlogInfo,
                        "Shutdown initiated. Killing app %s...",
                        appName);
                status = AppMgr::get()->abortMyAppRun(appGroupId,
                                                      StatusShutdownInProgress,
                                                      &appInternalError,
                                                      true);
                if (status != StatusOk) {
                    xSyslog(mname_,
                            XlogErr,
                            "Abort System App failed with status: %s",
                            strGetFromStatus(status));
                }
            }
            return;
        } else if (appEnabled) {
            // check if app already running and healthy
            // else launch the app
            if (appGroupId == XidInvalid ||
                !AppMgr::get()->isAppAlive(appGroupId)) {
                appGroupId = XidInvalid;
                status = owner_->run(&appGroupId);
                if (status != StatusOk) {
                    DCHECK(appGroupId == XidInvalid);
                    xSyslog(mname_,
                            XlogErr,
                            "Launch of '%s' System App failed with status: %s",
                            appName,
                            strGetFromStatus(status));
                    // will retry..
                }
            }
        } else {
            // app not enabled, kill if running and loop
            DCHECK(!appEnabled);
            if (appGroupId != XidInvalid) {
                xSyslog(mname_,
                        XlogInfo,
                        "Stopping system app '%s'..",
                        appName);
                status = AppMgr::get()->abortMyAppRun(appGroupId,
                                                      StatusSystemAppDisabled,
                                                      &appInternalError,
                                                      true);
                if (status != StatusOk) {
                    xSyslog(mname_,
                            XlogErr,
                            "Abort of '%s' System App failed with status: %s",
                            appName,
                            strGetFromStatus(status));
                }
                appGroupId = XidInvalid;
            }
        }
        // sleep before loop
        sema.timedWait(AppCheckTimeoutUsecs);
    }
}
