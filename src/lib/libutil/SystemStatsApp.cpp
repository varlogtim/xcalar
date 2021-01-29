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
#include "util/SystemStatsApp.h"

SystemStatsApp *SystemStatsApp::instance_ = nullptr;

SystemApp *
SystemStatsApp::get()
{
    return instance_;
}

Status
SystemStatsApp::init()
{
    char *jsonStr = NULL;
    Status status;
    XcalarConfig *xc = XcalarConfig::get();

    DCHECK(instance_ == nullptr);
    instance_ = new (std::nothrow) SystemStatsApp;
    if (instance_ == nullptr) return StatusNoMem;

    // set input to this app
    auto jsonObj = json_pack(
        "{"
        "s:s,"  // stats_dir_path
        "s:s,"  // logPath
        "}",
        "stats_dir_path",
        LogLib::get()->dfStatsDirPath_,
        "log_path",
        xc->xcalarLogCompletePath_);
    BailIfNull(jsonObj);
    jsonStr = json_dumps(jsonObj, 0);
    BailIfNull(jsonStr);

    status = completeInit(instance_, jsonStr);

CommonExit:
    if (status != StatusOk) {
        destroy();
    }
    if (jsonStr != NULL) {
        memFree(jsonStr);
        jsonStr = NULL;
    }
    if (jsonObj != NULL) {
        json_decref(jsonObj);
        jsonObj = NULL;
    }
    return status;
}

void
SystemStatsApp::destroy()
{
    delete instance_;
    instance_ = nullptr;
}

const char *
SystemStatsApp::name() const
{
    return AppMgr::SystemStatsAppName;
}

bool
SystemStatsApp::isAppEnabled()
{
    return XcalarConfig::get()->collectStats_;
}
