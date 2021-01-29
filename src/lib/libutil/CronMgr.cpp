// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "util/CronMgr.h"
#include "app/AppMgr.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "config/Config.h"

void
CronMgr::removeCronTab()
{
    if (Config::get()->getMyNodeId() != 0) {
        return;
    }

    char *inBlob = NULL;
    char *outBlob = NULL;
    char *errBlob = NULL;
    json_t *jObj = NULL;
    Status status = StatusOk;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;

    App *schedClearApp =
        AppMgr::get()->openAppHandle(AppMgr::ScheduleClearAppName, &handle);
    if (schedClearApp == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to open hanfle to App %s:%s",
                AppMgr::ScheduleClearAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    jObj = json_pack("{}");
    if (!jObj) {
        goto CommonExit;
    }
    inBlob = json_dumps(jObj, 0);

    status = AppMgr::get()->runMyApp(schedClearApp,
                                     AppGroup::Scope::Local,
                                     "",
                                     0,
                                     inBlob,
                                     0,
                                     &outBlob,
                                     &errBlob,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName, XlogErr, "%s: %s", outBlob, errBlob);
        goto CommonExit;
    }

CommonExit:
    if (inBlob) {
        memFree(inBlob);
        inBlob = NULL;
    }
    if (outBlob) {
        memFree(outBlob);
        outBlob = NULL;
    }
    if (errBlob) {
        memFree(errBlob);
        errBlob = NULL;
    }
    if (jObj) {
        json_decref(jObj);
        jObj = NULL;
    }
    if (schedClearApp) {
        AppMgr::get()->closeAppHandle(schedClearApp, handle);
    }
}
