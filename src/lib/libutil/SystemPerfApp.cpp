// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "util/SystemPerfApp.h"

SystemPerfApp *SystemPerfApp::instance_ = nullptr;

SystemApp *
SystemPerfApp::get()
{
    return instance_;
}

Status
SystemPerfApp::init()
{
    DCHECK(instance_ == nullptr);
    instance_ = new (std::nothrow) SystemPerfApp;
    if (instance_ == nullptr) return StatusNoMem;

    Status status = completeInit(instance_, "{}");
    if (status != StatusOk) {
        destroy();
    }
    return status;
}

void
SystemPerfApp::destroy()
{
    delete instance_;
    instance_ = nullptr;
}

const char *
SystemPerfApp::name() const
{
    return AppMgr::SystemRuntimePerfAppName;
}

bool
SystemPerfApp::isAppEnabled()
{
    return XcalarConfig::get()->runtimeStats_ &&
           XcalarConfig::get()->enableRuntimeStatsApp_;
}
