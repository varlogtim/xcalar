// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "AppGroupMgrGvm.h"
#include "AppGroupMgr.h"
#include "app/AppMgr.h"
#include "sys/XLog.h"

AppGroupMgrGvm *AppGroupMgrGvm::instance;

AppGroupMgrGvm *
AppGroupMgrGvm::get()
{
    return instance;
}

Status
AppGroupMgrGvm::init()
{
    instance =
        (AppGroupMgrGvm *) memAllocExt(sizeof(AppGroupMgrGvm), ModuleName);
    if (instance == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Init failed:%s",
                strGetFromStatus(StatusNoMem));
        return StatusNoMem;
    }
    new (instance) AppGroupMgrGvm();
    Gvm::get()->registerTarget(instance);
    return StatusOk;
}

void
AppGroupMgrGvm::destroy()
{
    instance->~AppGroupMgrGvm();
    memFree(instance);
    instance = NULL;
}

GvmTarget::Index
AppGroupMgrGvm::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexAppGroupMgr;
}

Status
AppGroupMgrGvm::localHandler(uint32_t action,
                             void *payload,
                             size_t *outputSizeOut)
{
    Status status;
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    bool canAbort = false;
    AppGroupMgr::Args *args = (AppGroupMgr::Args *) payload;
    AppGroup *appGroup = NULL;

    switch ((AppGroupMgrGvm::Action) action) {
    case Action::InitMetadata:
        if (strnlen(args->initMetadata.inBlob, args->initMetadata.inBlobSize) !=
            args->initMetadata.inBlobSize - 1) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler action %u failed:%s",
                    action,
                    strGetFromStatus(StatusInval));
            return StatusInval;
        }

        assert(args->initMetadata.txn.valid());
        status =
            appGroupMgr
                ->localThisGroupInitMetadata(args->groupId,
                                             args->initMetadata.appName,
                                             args->initMetadata.scope,
                                             args->initMetadata.userIdName,
                                             args->initMetadata.sessionId,
                                             args->initMetadata.inBlob,
                                             args->initMetadata.cookie,
                                             args->initMetadata.txn,
                                             args->initMetadata.appHandle,
                                             args->initMetadata.xpusPerNode,
                                             args->initMetadata
                                                 .xpusPerNodeCount);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu action %u failed on"
                    " localThisGroupInitMetadata:%s",
                    args->groupId,
                    action,
                    strGetFromStatus(status));
        }
        break;

    case Action::Start:
        appGroup =
            getAppGroup(args->groupId, (AppGroupMgrGvm::Action) action, NULL);
        if (appGroup == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu action %u failed on getAppGroup:%s",
                    args->groupId,
                    action,
                    strGetFromStatus(StatusNoEnt));
            return StatusNoEnt;
        }

        status = appGroup->localMyGroupStart();
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu action %u failed on"
                    " localMyGroupStart:%s",
                    args->groupId,
                    action,
                    strGetFromStatus(status));
        }
        break;

    case Action::NodeDone:
        appGroup =
            getAppGroup(args->groupId, (AppGroupMgrGvm::Action) action, NULL);
        if (appGroup == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu action %u failed on getAppGroup:%s",
                    args->groupId,
                    action,
                    strGetFromStatus(StatusNoEnt));
            return StatusNoEnt;
        }

        appGroup->localMyGroupNodeDone(args->origin);
        status = StatusOk;
        break;

    case Action::Reap:
        status = appGroupMgr->localThisGroupReap(args->origin,
                                                 args->groupId,
                                                 payload,
                                                 outputSizeOut);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu origin %u action %u failed on"
                    " localThisGroupReap:%s",
                    args->groupId,
                    args->origin,
                    action,
                    strGetFromStatus(status));
        }
        break;

    case Action::Cleanup:
        status =
            appGroupMgr->localThisGroupCleanup(args->origin, args->groupId);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu origin %u action %u failed on"
                    " localThisGroupCleanup:%s",
                    args->groupId,
                    args->origin,
                    action,
                    strGetFromStatus(status));
        }
        break;

    case Action::Abort:
        appGroup = getAppGroup(args->groupId,
                               (AppGroupMgrGvm::Action) action,
                               &canAbort);
        if (appGroup == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "localHandler group %lu action %u failed on getAppGroup:%s",
                    args->groupId,
                    action,
                    strGetFromStatus(StatusNoEnt));
            return StatusNoEnt;
        }

        if (canAbort) {
            appGroup->localMyGroupAbort(args->abortStatus);
        }
        xSyslog(ModuleName,
                XlogDebug,
                "group %lu canAbort %s abortStatus %s",
                args->groupId,
                canAbort ? "true" : "false",
                strGetFromStatus(args->abortStatus));
        status = StatusOk;
        break;

    default:
        assert(0 && "Unknown AppGroupMgr GVM action");
        return StatusUnimpl;
        break;  // Never reached
    }

    if (appGroup != NULL) {
        appGroup->decRef();
    }

    return status;
}

AppGroup *
AppGroupMgrGvm::getAppGroup(AppGroup::Id appGroupId,
                            AppGroupMgrGvm::Action action,
                            bool *retCanAbort)
{
    AppGroup *appGroup = NULL;
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    bool firstAppGroupAbortInvoked = false;
    if (retCanAbort != NULL) {
        *retCanAbort = false;
    }

    appGroupMgr->groupsLock_.lock();

    appGroup = appGroupMgr->groups_.find(appGroupId);
    if (appGroup == NULL) {
        appGroupMgr->groupsLock_.unlock();
        return NULL;
    }
    appGroup->incRef();
    appGroupMgr->groupsLock_.unlock();

    appGroup->lock_.lock();

    if (action == Action::Abort && appGroup->abortInvoked_ == false) {
        firstAppGroupAbortInvoked = true;
        appGroup->abortInvoked_ = true;
    }

    appGroup->lock_.unlock();

    // For any AppGroup, App Abort should be invoked exactly once.
    if (action == Action::Abort && firstAppGroupAbortInvoked == true) {
        if (retCanAbort != NULL) {
            *retCanAbort = true;
        }
    }
    return appGroup;
}
