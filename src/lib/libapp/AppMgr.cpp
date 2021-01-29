// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include "StrlFunc.h"
#include "app/AppMgr.h"
#include "AppGroupMgr.h"
#include "gvm/Gvm.h"
#include "AppStats.h"
#include "util/MemTrack.h"
#include "util/FileUtils.h"
#include "log/Log.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "AppGroupMgrGvm.h"
#include "AppMgrGvm.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"

// This needs a definition as well; see the .h for the values
constexpr const AppMgr::BuiltInApp AppMgr::BuiltInApps[];

//
// Singleton setup/teardown.
//
AppMgr *AppMgr::instance = NULL;

AppMgr::AppMgr() : appGroupMgr_(NULL) {}

AppMgr::~AppMgr()
{
    assert(appGroupMgr_ == NULL);
}

Status  // static
AppMgr::init()
{
    Status status;

    assert(instance == NULL);
    instance = new (std::nothrow) AppMgr;
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to init:%s",
                strGetFromStatus(status));
        return status;
    }

    status = AppMgrGvm::init();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed AppMgrGvm init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = AppGroupMgrGvm::init();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed AppGroupMgrGvm init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->stats_ = new (std::nothrow) AppStats;
    if (instance->stats_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed stats init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = instance->stats_->init();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed stats init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->appGroupMgr_ = new (std::nothrow) AppGroupMgr(instance);
    if (instance->appGroupMgr_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed AppGroupMgr init:%s",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to init:%s",
                strGetFromStatus(status));
        delete instance;
        instance = NULL;
    }
    return status;
}

Status
AppMgr::tryAbortAllApps(Status abortReason)
{
    AppGroup::Id *groupIds = NULL;
    size_t numGroupIds = 0;
    Status status = StatusUnknown;
    bool appInternalError = false;

    status = appGroupMgr_->listAllAppGroupIds(&groupIds, &numGroupIds);
    if (status != StatusOk) {
        goto CommonExit;
    }

    for (size_t ii = 0; ii < numGroupIds; ii++) {
        Status status2;
        xSyslog(ModuleName,
                XlogDebug,
                "Attempting to abort appGroup %lu",
                groupIds[ii]);
        status2 =
            abortMyAppRun(groupIds[ii], abortReason, &appInternalError, true);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogDebug,
                    "Failed to abort appGroup %lu: %s",
                    groupIds[ii],
                    strGetFromStatus(status2));
            status = status2;
        }
    }
CommonExit:
    if (groupIds != NULL) {
        memFree(groupIds);
        groupIds = NULL;
    }

    return status;
}

void
AppMgr::destroy()
{
    if (appGroupMgr_ != NULL) {
        delete appGroupMgr_;
        appGroupMgr_ = NULL;
    }

    if (stats_ != NULL) {
        delete stats_;
        stats_ = NULL;
    }

    App *app;

    appsLock_.lock();
    while ((app = apps_.begin().get()) != NULL) {
        verify(apps_.remove(app->getName()) == app);
        appsLock_.unlock();
        appsLock_.lock();
    }
    appsLock_.unlock();

    if (AppGroupMgrGvm::get()) {
        AppGroupMgrGvm::get()->destroy();
    }

    if (AppMgrGvm::get()) {
        AppMgrGvm::get()->destroy();
    }

    delete instance;
    instance = NULL;
}

Status
AppMgr::extractSingleAppOut(const char *appOutBlob, char **onlyAppOut)
{
    Status status = StatusOk;
    json_t *totalJson = NULL;
    json_t *nodeJson;
    json_t *appJson;
    json_error_t jsonError;
    *onlyAppOut = NULL;
    int appOutSize = -1;

    int numNodeOuts;

    totalJson = json_loads(appOutBlob, 0, &jsonError);
    if (totalJson == NULL) {
        // The format of this string is dictated by the AppGroup, and thus
        // can be guaranteed to be parseable. However, it's possible to
        // encounter a memory error here, so we still have to handle errors
        status = StatusFailed;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to parse app output: %s",
                      jsonError.text);
        goto CommonExit;
    }

    assert(json_typeof(totalJson) == JSON_ARRAY);
    numNodeOuts = json_array_size(totalJson);
    for (int ii = 0; *onlyAppOut == NULL && ii < numNodeOuts; ii++) {
        nodeJson = json_array_get(totalJson, ii);
        assert(nodeJson && json_typeof(totalJson) == JSON_ARRAY);
        int numAppOuts = json_array_size(nodeJson);

        for (int jj = 0; *onlyAppOut == NULL && jj < numAppOuts; jj++) {
            appJson = json_array_get(nodeJson, 0);
            assert(appJson && json_typeof(appJson) == JSON_STRING);

            const char *appOut = json_string_value(appJson);
            assert(appOut);
            appOutSize = strlen(appOut);
            if (appOutSize == 0) {
                // This was an empty app; continue looking for the output
                continue;
            }
            *onlyAppOut = (char *) memAlloc(appOutSize + 1);
            BailIfNull(*onlyAppOut);
            verify((int) strlcpy(*onlyAppOut, appOut, appOutSize + 1) ==
                   appOutSize);
        }
    }

    if (*onlyAppOut == NULL) {
        status = StatusFailed;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to find valid preview output");
        goto CommonExit;
    }

CommonExit:
    if (totalJson) {
        json_decref(totalJson);
        totalJson = NULL;
        nodeJson = NULL;
        appJson = NULL;
    }
    if (status != StatusOk) {
        if (*onlyAppOut) {
            memFree(*onlyAppOut);
            *onlyAppOut = NULL;
        }
    }
    return status;
}

//
// XXX Note that we will delete all App Source code management here and move
// this over to UDFs eventually.
// Entry points for registering/deregistering/updating Apps globally.
//

Status
AppMgr::createApp(const char *name,
                  App::HostType hostType,
                  uint32_t flags,
                  const uint8_t *exec,
                  size_t execSize)
{
    App::Packed *pack = NULL;
    size_t packSize = sizeof(App::Packed) + execSize;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    bool nsPublished = false;
    bool gvmIssued = false;
    LibNs *libNs = LibNs::get();
    App *app = NULL;
    bool handleValid = false;
    LibNsTypes::NsHandle handle;
    AppRecord appRecord(false);
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload = (Gvm::Payload *) memAlloc(packSize + sizeof(Gvm::Payload));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    pack = (App::Packed *) gPayload->buf;
    if (strlcpy(pack->name, name, sizeof(pack->name)) >= sizeof(pack->name)) {
        status = StatusAppNameInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pack->hostType = hostType;
    pack->flags = flags;
    pack->execSize = execSize;
    pack->version = (uint32_t) App::Packed::Version::Current;
    memcpy(pack->exec, exec, execSize);

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   AppPrefix,
                   name);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nsId = libNs->publish(fullyQualName, &appRecord, &status);
    if (status != StatusOk) {
        if (status == StatusExist || status == StatusPendingRemoval) {
            status = StatusAppAlreadyExists;
        }
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on NS publish: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    nsPublished = true;

    // Open in exclusive access
    handle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound) {
            status = StatusAppNotFound;
        } else if (status == StatusAccess || status == StatusPendingRemoval) {
            status = StatusAppAlreadyExists;
        }
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on open: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    gvmIssued = true;
    gPayload->init(AppMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppMgrGvm::Action::Create,
                   packSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on GVM Create:%s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    app = findApp(pack->name);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on find: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = app->save();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on save: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(appRecord.isCreated_ == false);
    appRecord.isCreated_ = true;
    handle = libNs->updateNsObject(handle, &appRecord, &status);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create App %s on update %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Apps have now been persisted and creation has succeeded!
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (gvmIssued == true) {
            removeAppInternal(name, handle);  // GVM cleanout
            handleValid = false;
        } else if (nsPublished == true) {
            Status status2 = libNs->remove(nsId, NULL);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Create App %s failed on NS remove: %s",
                        name,
                        strGetFromStatus(status2));
            }
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Create App %s failed on NS close: %s",
                    name,
                    strGetFromStatus(status2));
        }
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

Status
AppMgr::updateApp(const char *name,
                  App::HostType hostType,
                  uint32_t flags,
                  const uint8_t *exec,
                  size_t execSize)
{
    App::Packed *pack = NULL;
    size_t packSize = sizeof(App::Packed) + execSize;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    int ret;
    LibNs *libNs = LibNs::get();
    bool gvmIssued = false;
    App *app = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    assert(builtInApp(name) ==
           false);  // Don't allow built-in App to be updated

    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload = (Gvm::Payload *) memAlloc(packSize + sizeof(Gvm::Payload));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    pack = (App::Packed *) gPayload->buf;
    if (strlcpy(pack->name, name, sizeof(pack->name)) >= sizeof(pack->name)) {
        status = StatusAppNameInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pack->hostType = hostType;
    pack->flags = flags;
    pack->execSize = execSize;
    pack->version = (uint32_t) App::Packed::Version::Current;
    memcpy(pack->exec, exec, execSize);

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   AppPrefix,
                   name);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Open in exclusive access
    handle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound) {
            status = StatusAppNotFound;
        } else if (status == StatusAccess || status == StatusPendingRemoval) {
            status = StatusAppInUse;
        }
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s on open: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    gvmIssued = true;
    gPayload->init(AppMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppMgrGvm::Action::Update,
                   packSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s on GVM Update:%s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    app = findApp(pack->name);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s on find: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = app->save();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to update App %s on save: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Apps have now been persisted and update has succeeded!
    status = StatusOk;

CommonExit:
    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Update App %s failed on NS close: %s",
                    name,
                    strGetFromStatus(status2));
        }
    }

    if (status != StatusOk && gvmIssued == true) {
        removeApp(name);  // GVM cleanout
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

void
AppMgr::removeApp(const char *name)
{
    Status status;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    size_t getPathInfoSize;
    size_t prefixSize = strlen(AppPrefix);
    LibNsTypes::PathInfoOut *pathInfoOut = NULL;
    LibNs *libNs = LibNs::get();

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   AppPrefix,
                   name);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Remove App %s failed: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    getPathInfoSize = libNs->getPathInfo(fullyQualName, &pathInfoOut);
    if (getPathInfoSize == 0) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Remove App %s failed on get path info: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (size_t ii = 0; ii < getPathInfoSize; ii++) {
        char *appNameTmp = pathInfoOut[ii].pathName + prefixSize;
        LibNsTypes::NsHandle appHandle;
        appHandle.nsId = LibNsTypes::NsInvalidId;
        removeAppInternal(appNameTmp, appHandle);
    }

CommonExit:
    if (pathInfoOut != NULL) {
        memFree(pathInfoOut);
    }
}

void
AppMgr::removeAppInternal(const char *name, LibNsTypes::NsHandle appHandle)
{
    App::Packed *pack = NULL;
    size_t packSize = sizeof(App::Packed);
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    int ret;
    LibNs *libNs = LibNs::get();
    App *app = NULL;
    AppRecord *appRecord = NULL;
    bool removeApp = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload = (Gvm::Payload *) memAlloc(packSize + sizeof(Gvm::Payload));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    pack = (App::Packed *) gPayload->buf;
    if (strlcpy(pack->name, name, sizeof(pack->name)) >= sizeof(pack->name)) {
        status = StatusAppNameInvalid;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pack->hostType = App::HostType::Invalid;
    pack->flags = App::FlagInvalid;
    pack->execSize = 0;
    pack->version = (uint32_t) App::Packed::Version::Current;

    ret = snprintf(fullyQualName,
                   LibNsTypes::MaxPathNameLen,
                   "%s%s",
                   AppPrefix,
                   name);
    if (ret >= (int) sizeof(fullyQualName)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Open in exclusive access
    if (appHandle.nsId == LibNsTypes::NsInvalidId) {
        handle = libNs->open(fullyQualName,
                             LibNsTypes::WriterExcl,
                             (NsObject **) &appRecord,
                             &status);
        if (status != StatusOk) {
            if (status == StatusNsNotFound) {
                status = StatusAppNotFound;
            } else if (status == StatusAccess ||
                       status == StatusPendingRemoval) {
                status = StatusAppInUse;
            }
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to remove App %s on open: %s",
                    name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        handle = appHandle;
        appRecord = (AppRecord *) libNs->getNsObject(handle);
        if (appRecord == NULL) {
            status = StatusNoEnt;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to remove App %s on getNsObject: %s",
                    name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    handleValid = true;

    if (appRecord->isCreated_ == false) {
        status = StatusAppInUse;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s on pending create: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    removeApp = true;
    app = findApp(pack->name);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s on find: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Apps delete has succeeded!
    app->undoSave();

    gPayload->init(AppMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppMgrGvm::Action::Remove,
                   packSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to remove App %s on GVM Remove:%s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = StatusOk;

CommonExit:
    if (handleValid) {
        // Now mark the App for removal. This means all subsequent attempts
        // to open will fail.
        Status status2;
        if (removeApp) {
            status2 = libNs->remove(fullyQualName, NULL);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Remove App %s failed on NS remove: %s",
                        name,
                        strGetFromStatus(status2));
            }
        }

        // With close of handle, the App will be tossed out of the namespace.
        status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Remove App %s failed on NS close: %s",
                    name,
                    strGetFromStatus(status2));
        }
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (appRecord != NULL) {
        memFree(appRecord);
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
}

App *
AppMgr::openAppHandle(const char *name, LibNsTypes::NsHandle *retHandle)
{
    Status status;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    int ret;
    LibNs *libNs = LibNs::get();
    App *app = NULL;

    // Open in shared access
    if (builtInApp(name) == false) {
        ret = snprintf(fullyQualName,
                       LibNsTypes::MaxPathNameLen,
                       "%s%s",
                       AppPrefix,
                       name);
        if (ret >= (int) sizeof(fullyQualName)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to open handle tp App %s: %s",
                    name,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        handle = libNs->open(fullyQualName, LibNsTypes::ReaderShared, &status);
        if (status != StatusOk) {
            if (status == StatusNsNotFound) {
                status = StatusAppNotFound;
            } else if (status == StatusAccess ||
                       status == StatusPendingRemoval) {
                status = StatusAppInUse;
            }
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to open handle to App %s: %s",
                    name,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        handleValid = true;
    } else {
        handle.nsId = LibNsTypes::NsInvalidId;
    }

    app = findApp(name);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to open handle to App %s on find: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    *retHandle = handle;

CommonExit:
    if (app == NULL && handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Open handle App %s failed on NS close: %s",
                    name,
                    strGetFromStatus(status2));
        }
    }
    return app;
}

void
AppMgr::closeAppHandle(App *app, LibNsTypes::NsHandle handle)
{
    if (app->getFlags() & App::FlagBuiltIn) {
        return;
    }

    LibNs *libNs = LibNs::get();
    Status status = libNs->close(handle, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Close handle App %s failed on NS close: %s",
                app->getName(),
                strGetFromStatus(status));
    }
    // After close, App may go out of namespace. So don't expect the Apps to be
    // valid anymore.
}

Status
AppMgr::addBuildInApps()
{
    Status status = StatusOk;
    int ret;
    char *appSrc = NULL;
    int fd = -1;
    struct stat fileStat;
    int fileStrLen;

    const char *xlrDir = getenv("XLRDIR");
    if (xlrDir == NULL) {
        xSyslog(ModuleName,
                XlogCrit,
                "%s",
                "XLRDIR environment variable must point to Xcalar install");
        return StatusFailed;
    }

    for (size_t ii = 0; ii < ArrayLen(BuiltInApps); ii++) {
        const BuiltInApp *sysApp = &BuiltInApps[ii];
        char appPath[PATH_MAX];

        if (snprintf(appPath, PATH_MAX, "%s/%s", xlrDir, sysApp->appPath) >=
            PATH_MAX) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s/%s on open: %s",
                    xlrDir,
                    sysApp->appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        fd = open(appPath, O_CLOEXEC | O_RDONLY);
        if (fd == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s on open: %s",
                    appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        ret = fstat(fd, &fileStat);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s on fstat: %s",
                    appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // Add 1 to ensure null termination
        fileStrLen = fileStat.st_size + 1;
        appSrc = (char *) memAlloc(fileStrLen);
        if (appSrc == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s: %s",
                    appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // This makes the assumption that file size isn't changing
        status = FileUtils::convergentRead(fd, appSrc, fileStat.st_size, NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s on read: %s",
                    appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        appSrc[fileStrLen - 1] = '\0';

        status = createApp(sysApp->appName,
                           App::HostType::Python2,
                           sysApp->flags,
                           (uint8_t *) appSrc,
                           fileStrLen);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add built in App %s on createApp: %s",
                    appPath,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        memFree(appSrc);
        appSrc = NULL;
        FileUtils::close(fd);
        fd = -1;
    }

CommonExit:
    if (appSrc) {
        memFree(appSrc);
        appSrc = NULL;
    }
    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }
    return status;
}

void
AppMgr::removeBuiltInApps()
{
    for (unsigned ii = 0; ii < ArrayLen(BuiltInApps); ii++) {
        const BuiltInApp *sysApp = &BuiltInApps[ii];
        removeApp(sysApp->appName);
    }
}

Status
AppMgr::restoreAll()
{
    Status status = StatusOk;
    // Used to recognize logs exactly once.
    static constexpr const char *metaPattern = "*-meta";
    const size_t metaPatternLen = strlen(metaPattern);

    DIR *dirIter =
        opendir(LogLib::get()->getDirPath(LogLib::AppPersistDirIndex));
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to restore Apps: %s",
                strGetFromStatus(status));
        return status;
    }

    unsigned countFailed = 0;
    unsigned countSucceeded = 0;

    while (true) {
        struct dirent *currentFile;

        // The only error is EBADF, which should exclusively be a program error
        errno = 0;
        currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            break;  // End of directory.
        }

        // Apply pattern skip
        if (!strMatch(metaPattern, currentFile->d_name)) {
            continue;
        }

        const size_t fileNameLen =
            strnlen(currentFile->d_name, sizeof(currentFile->d_name));
        assert(fileNameLen != sizeof(currentFile->d_name));  // Via readdir_r.
        if (fileNameLen < metaPatternLen) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to restore App %s due to invalid save file",
                    currentFile->d_name);
            countFailed++;
            continue;
        }

        // Get rid of -meta.
        currentFile->d_name[fileNameLen - metaPatternLen + 1] = '\0';
        status = App::restore(currentFile->d_name);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to restore App %s: %s",
                    currentFile->d_name,
                    strGetFromStatus(status));
            countFailed++;
        } else {
            countSucceeded++;
        }
    }

    xSyslog(ModuleName,
            XlogInfo,
            "Restored %u Apps successfully. Encountered %u errors",
            countSucceeded,
            countFailed);
    status = StatusOk;

    if (dirIter != NULL) {
        verify(closedir(dirIter) == 0);
    }
    return status;
}

Status
AppMgr::createLocalHandler(const void *payload)
{
    Status status;
    bool lockHeld = false;
    bool appAlloced = false;

    App::Packed *pack = (App::Packed *) payload;

    appsLock_.lock();  // Held over creation.
    lockHeld = true;

    App *app = apps_.find(pack->name);
    if (app != NULL) {
        status = StatusAppAlreadyExists;
        xSyslog(ModuleName,
                XlogErr,
                "createLocalHandler for App %s failed: %s",
                pack->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    app = new (std::nothrow) App;  // AppMgr owns initial ref.
    if (app == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "createLocalHandler for App %s failed: %s",
                pack->name,
                strGetFromStatus(status));
        goto CommonExit;
    }
    appAlloced = true;

    status = app->init(pack->name,
                       pack->hostType,
                       pack->flags,
                       pack->exec,
                       pack->execSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "createLocalHandler for App %s failed on init: %s",
                pack->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    verifyOk(apps_.insert(app));
    stats_->incrCountApps();
    status = StatusOk;

CommonExit:
    if (lockHeld == true) {
        appsLock_.unlock();
    }

    if (status != StatusOk && appAlloced == true) {
        delete app;
        app = NULL;
    }

    return status;
}

Status
AppMgr::updateLocalHandler(const void *payload)
{
    Status status;
    App::Packed *pack = (App::Packed *) payload;

    App *app = findApp(pack->name);
    if (app == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "updateLocalHandler App %s failed to find: %s",
                pack->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        app->update(pack->hostType, pack->flags, pack->exec, pack->execSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "updateLocalHandler App %s failed to update: %s",
                pack->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
AppMgr::removeLocalHandler(const void *payload)
{
    App::Packed *pack = (App::Packed *) payload;

    appsLock_.lock();
    App *app = apps_.remove(pack->name);
    appsLock_.unlock();

    stats_->decrCountApps();
    delete app;
}

//
// To allow servicing of App messages from arbitrary XPU ports, AppMgr keeps
// ParentChild::Id -> AppInstance mapping.
//
void
AppMgr::registerAppInstance(AppInstance *instance)
{
    appInstancesLock_.lock();
    assert(appInstances_.find(instance->getMyChildId()) == NULL);
    appInstances_.insert(instance);
    instance->refGet();
    appInstancesLock_.unlock();
}

void
AppMgr::deregisterAppInstance(ParentChild::Id id)
{
    appInstancesLock_.lock();
    AppInstance *instance = appInstances_.remove(id);
    instance->refPut();
    appInstancesLock_.unlock();
}

void
AppMgr::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                     LocalConnection *connection,
                     const ProtoRequestMsg *request,
                     ProtoResponseMsg *response)
{
    // Find corresponding AppInstance.
    appInstancesLock_.lock();
    AppInstance *instance = appInstances_.find(request->childid());
    if (instance == NULL) {
        appInstancesLock_.unlock();
        response->set_status(StatusNoEnt.code());
        xSyslog(ModuleName,
                XlogErr,
                "onRequestMsg could not find App instance:%s",
                strGetFromStatus(StatusNoEnt));
        return;
    }

    instance->refGet();
    appInstancesLock_.unlock();

    // Pass message off to AppInstance.
    instance->onRequestMsg(reqHandler, connection, request, response);
    instance->refPut();
}

Status
AppMgr::waitForMyAppResult(AppGroup::Id appGroupId,
                           uint64_t usecsTimeout,
                           char **outBlobOut,
                           char **errorBlobOut,
                           bool *retAppInternalError)
{
    Status status;
    bool appHandleValid = false;
    LibNsTypes::NsHandle appHandle;
    App *myApp = NULL;
    AppGroup *appGroup = NULL;

    *retAppInternalError = false;
    *outBlobOut = NULL;
    *errorBlobOut = NULL;

    appGroup = appGroupMgr_->getThisGroup(appGroupId);
    if (appGroup == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "App waitForMyAppResult %lu failed on getThisGroup:%s",
                appGroupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    appHandle = appGroup->appHandle_;
    myApp = appGroup->app_;
    appHandleValid = true;

    status = appGroup->waitForMyGroupResult(usecsTimeout,
                                            outBlobOut,
                                            errorBlobOut,
                                            retAppInternalError);
    if (status != StatusOk && status != StatusTimedOut) {
        xSyslog(ModuleName,
                XlogErr,
                "App waitForMyAppResult %lu failed on waitForMyGroupResult:%s",
                appGroupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (appGroup != NULL) {
        appGroup->decRef();
        appGroup = NULL;
    }

    if (appHandleValid && !(*retAppInternalError)) {
        closeAppHandle(myApp, appHandle);
    }

    return status;
}

bool
AppMgr::isAppAlive(AppGroup::Id appGroupId)
{
    bool isAlive = false;
    AppGroup *appGroup = NULL;
    appGroup = appGroupMgr_->getThisGroup(appGroupId);
    if (appGroup == NULL) {
        xSyslog(ModuleName, XlogInfo, "AppGroup %lu not found", appGroupId);
        goto CommonExit;
    }

    // If any nodes are not Done state, app is still considered alive.
    for (size_t ii = 0; ii < appGroup->nodesCount_; ii++) {
        if (appGroup->nodes_[ii].state != AppGroup::State::Done) {
            isAlive = true;
            break;
        }
    }

CommonExit:

    if (appGroup != NULL) {
        appGroup->decRef();
        appGroup = NULL;
    }

    return isAlive;
}

Status
AppMgr::runMyApp(App *app,
                 AppGroup::Scope scope,
                 const char *userIdName,
                 uint64_t sessionId,
                 const char *inBlob,
                 uint64_t cookie,
                 char **outBlobOut,
                 char **errorBlobOut,
                 bool *retAppInternalError)
{
    AppGroup::Id appGroupId;
    LibNsTypes::NsHandle appHandle;
    Status status;

    *retAppInternalError = false;

    App *myApp = AppMgr::get()->openAppHandle(app->getName(), &appHandle);
    if (myApp == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "App %s runMyApp failed on openAppHandle: %s",
                app->getName(),
                strGetFromStatus(status));
        return status;
    }
    assert(myApp == app);
    myApp = NULL;

    status = appGroupMgr_->runThisGroup(app,
                                        scope,
                                        userIdName,
                                        sessionId,
                                        inBlob,
                                        cookie,
                                        &appGroupId,
                                        errorBlobOut,
                                        appHandle,
                                        retAppInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s runMyApp failed on runThisGroup: %s",
                app->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = waitForMyAppResult(appGroupId,
                                0,
                                outBlobOut,
                                errorBlobOut,
                                retAppInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s Group %lu runMyApp failed on waitForMyAppResult: %s",
                app->getName(),
                appGroupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (myApp != NULL) {
        AppMgr::get()->closeAppHandle(myApp, appHandle);
    }

    return status;
}

Status
AppMgr::runMyAppAsync(App *app,
                      AppGroup::Scope scope,
                      const char *userIdName,
                      uint64_t sessionId,
                      const char *inBlob,
                      uint64_t cookie,
                      AppGroup::Id *retAppGroupId,
                      char **errorBlobOut,
                      bool *retAppInternalError)
{
    AppGroup::Id appGroupId;
    *errorBlobOut = NULL;
    LibNsTypes::NsHandle handle;
    Status status;
    *retAppInternalError = false;

    App *myApp = AppMgr::get()->openAppHandle(app->getName(), &handle);
    if (myApp == NULL) {
        status = StatusAppNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "App %s runMyAppAsync failed on openAppHandle: %s",
                app->getName(),
                strGetFromStatus(status));
        return status;
    }
    assert(myApp == app);
    myApp = NULL;

    status = appGroupMgr_->runThisGroup(app,
                                        scope,
                                        userIdName,
                                        sessionId,
                                        inBlob,
                                        cookie,
                                        &appGroupId,
                                        errorBlobOut,
                                        handle,
                                        retAppInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s runMyAppAsync failed on runThisGroup: %s",
                app->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }
    *retAppGroupId = appGroupId;

CommonExit:
    if (myApp != NULL) {
        AppMgr::get()->closeAppHandle(myApp, handle);
    }

    return status;
}

Status
AppMgr::abortMyAppRun(AppGroup::Id appGroupId,
                      Status abortReason,
                      bool *retAppInternalError)
{
    return abortMyAppRun(appGroupId, abortReason, retAppInternalError, false);
}

Status
AppMgr::abortMyAppRun(AppGroup::Id appGroupId,
                      Status abortReason,
                      bool *retAppInternalError,
                      bool reappFlag)
{
    Status status = StatusOk;
    AppGroup::Scope scope;
    *retAppInternalError = false;

    AppGroup *appGroup = appGroupMgr_->getThisGroup(appGroupId);
    unsigned nodeCount = Config::get()->getActiveNodes();
    void *outputPerNode[nodeCount];
    size_t sizePerNode[nodeCount];

    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));
    if (appGroup == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "App waitForMyAppResult %lu failed on getThisGroup:%s",
                appGroupId,
                strGetFromStatus(status));
        return status;
    }

    scope = appGroup->getMyScope();

    if (scope == AppGroup::Scope::Global) {
        status = appGroupMgr_->abortThisGroup(appGroupId,
                                              abortReason,
                                              retAppInternalError);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu abort reason %s abortMyAppRun failed on"
                    " abortMyAppRun: %s",
                    appGroupId,
                    strGetFromStatus(abortReason),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        if (appGroup->kickLocalAbort()) {
            appGroup->localMyGroupAbort(abortReason);
        }
    }

    if (reappFlag) {
        status = appGroupMgr_->reapThisGroup(appGroupId,
                                             scope,
                                             outputPerNode,
                                             sizePerNode,
                                             retAppInternalError);
        // ok to ignore StatusCanceled as we just abort the group
        if (status == StatusCanceled) {
            status = StatusOk;
        }
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu failed reap: status=%s",
                    appGroupId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    for (size_t ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
        }
    }

    if (appGroup != NULL) {
        appGroup->decRef();
        appGroup = NULL;
    }
    return status;
}

// XXX This API was introduced to workaround the fact that Operators code
// calling Ser-Des App cannot have it's fiber suspended because it's doing
// IO holding Spinlock.
// This API is slated to be removed, so please don't use it else where.
bool
AppMgr::builtInApp(const char *appName)
{
    for (uint64_t ii = 0; ii < sizeof(BuiltInApps) / sizeof(BuiltInApp); ii++) {
        if (strcmp(appName, BuiltInApps[ii].appName) == 0) {
            return true;
        }
    }
    return false;
}

Status
AppMgr::setupMsgStreams()
{
    return appGroupMgr_->setupMsgStreams();
}

void
AppMgr::teardownMsgStreams()
{
    appGroupMgr_->teardownMsgStreams();
}
