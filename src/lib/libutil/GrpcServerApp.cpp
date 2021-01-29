// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "util/GrpcServerApp.h"

GrpcServerApp *GrpcServerApp::instance_ = nullptr;

SystemApp *
GrpcServerApp::get()
{
    return instance_;
}

Status
GrpcServerApp::init()
{
    Status status;

    DCHECK(instance_ == nullptr);
    instance_ = new (std::nothrow) GrpcServerApp;
    if (instance_ == nullptr) {
        return StatusNoMem;
    }
    status = completeInit(instance_, "");
    if (status != StatusOk) {
        destroy();
    }
    return status;
}

void
GrpcServerApp::destroy()
{
    delete instance_;
    instance_ = nullptr;
}

const char *
GrpcServerApp::name() const
{
    return AppMgr::GrpcServerAppName;
}

bool
GrpcServerApp::isAppEnabled()
{
    return true;
}
