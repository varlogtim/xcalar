// Copyright 2016-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "StrlFunc.h"
#include "AppGroupMgr.h"
#include "AppStats.h"
#include "app/AppMgr.h"
#include "msg/Xid.h"
#include "gvm/Gvm.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "sys/XLog.h"
#include "parent/Parent.h"

AppGroupMgr::AppGroupMgr(AppMgr *appMgr) : appMgr_(appMgr) {}

AppGroupMgr::~AppGroupMgr()
{
    if (msgStreamIds_ != NULL) {
        memFree(msgStreamIds_);
        msgStreamIds_ = NULL;
    }
}

Status
AppGroupMgr::localThisGroupInitMetadata(AppGroup::Id groupId,
                                        const char *appName,
                                        AppGroup::Scope scope,
                                        const char *userIdName,
                                        uint64_t sessionId,
                                        const char *inBlob,
                                        uint64_t cookie,
                                        Txn txn,
                                        LibNsTypes::NsHandle appHandle,
                                        unsigned *xpusPerNode,
                                        unsigned xpusPerNodeCount)
{
    Status status;

    AppGroup *group = new (std::nothrow) AppGroup;
    if (group == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s Group %lu localThisGroupInitMetadata failed:%s",
                appName,
                groupId,
                strGetFromStatus(status));
        return status;
    }

    App *app = appMgr_->findApp(appName);
    status = group->init(groupId,
                         app,
                         scope,
                         userIdName,
                         sessionId,
                         inBlob,
                         cookie,
                         txn,
                         appHandle,
                         xpusPerNode,
                         xpusPerNodeCount);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s Group %lu localThisGroupInitMetadata failed on init:%s",
                appName,
                groupId,
                strGetFromStatus(status));
        group->decRef();
        return status;
    }

    groupsLock_.lock();
    verifyOk(groups_.insert(group));
    AppMgr::get()->getStats()->incrCountAppGroups();
    groupsLock_.unlock();

    return StatusOk;
}

Status
AppGroupMgr::localThisGroupCleanup(NodeId originNode, AppGroup::Id groupId)
{
    AppGroup *group;
    Status status = StatusOk;

    groupsLock_.lock();
    group = groups_.find(groupId);
    groupsLock_.unlock();

    if (group == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu originNode %u localThisGroupCleanup failed:%s",
                groupId,
                originNode,
                strGetFromStatus(status));
        return status;
    }

    groupsLock_.lock();
    verify(groups_.remove(groupId) == group);
    AppMgr::get()->getStats()->decrCountAppGroups();
    groupsLock_.unlock();

    assert(group->appInstancesCount_ > 0);
    AppInstance *appInst = NULL;
    while ((appInst = group->appInstances_.begin().get()) != NULL) {
        AppInstance *appInstTmp =
            group->appInstances_.remove(appInst->getMyXpuId());
        assert(appInstTmp == appInst);
        assert(group->appInstancesCount_ > 0);
        group->appInstancesCount_--;
        AppMgr::get()->getStats()->decrCountAppInstances();
        appInst->refPut();  // appInstances_ ref.
        appInst->refPut();  // original appInstance ref.
    }
    assert(group->appInstancesCount_ == 0);

    // No one should need this group anymore, so cleanout
    group->decRef();

    return status;
}

Status
AppGroupMgr::localThisGroupReap(NodeId originNode,
                                AppGroup::Id groupId,
                                void *payload,
                                size_t *outputSizeOut)
{
    Status status;
    AppGroup *group;

    groupsLock_.lock();
    group = groups_.find(groupId);
    groupsLock_.unlock();

    if (group == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu originNode %u localThisGroupReap failed:%s",
                groupId,
                originNode,
                strGetFromStatus(status));
        return status;
    }

    // To prevent race between localMyGroupNodeDone and localMyGroupReap
    group->waitForMyGroupAllNodesDone();

    // Drain any pending aborts outstanding,
    group->waitForPendingAbort();

    groupsLock_.lock();
    verify(groups_.remove(groupId) == group);
    AppMgr::get()->getStats()->decrCountAppGroups();
    groupsLock_.unlock();

    status = group->localMyGroupReap(payload, outputSizeOut);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu originNode %u localMyGroupReap failed:%s",
                groupId,
                originNode,
                strGetFromStatus(status));
    }

    // No one should need this group anymore, so cleanout
    group->decRef();

    return status;
}

Status
AppGroupMgr::runThisGroupLocal(App *app,
                               Txn appTxn,
                               AppGroup::Id groupId,
                               const char *userIdName,
                               uint64_t sessionId,
                               const char *inBlob,
                               uint64_t cookie,
                               AppGroup::Id *retAppGroupId,
                               char **errorBlobOut,
                               LibNsTypes::NsHandle appHandle,
                               bool *retAppInternalError)
{
    Status status = StatusOk;
    AppGroup::Scope scope = AppGroup::Scope::Local;
    bool initMeta = false;
    AppGroup *group = NULL;
    unsigned xpusPerNode = 1;
    unsigned xpusPerNodeCount = 1;

    status = localThisGroupInitMetadata(groupId,
                                        app->getName(),
                                        scope,
                                        userIdName,
                                        sessionId,
                                        inBlob,
                                        cookie,
                                        appTxn,
                                        appHandle,
                                        &xpusPerNode,
                                        xpusPerNodeCount);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s failed on localThisGroupInitMetadata:%s",
                app->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }
    initMeta = true;

    groupsLock_.lock();
    assert(groups_.find(groupId) != NULL);
    group = groups_.find(groupId);
    group->incRef();
    groupsLock_.unlock();

    status = group->localMyGroupStart();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s group %lu failed on localMyGroupStart:%s",
                app->getName(),
                group->getMyId(),
                strGetFromStatus(status));
        // Abort the group run.
        if (group->kickLocalAbort()) {
            group->localMyGroupAbort(status);
        }
        group->decRef();
        goto CommonExit;
    }
    group->decRef();

CommonExit:
    if (status != StatusOk) {
        if (initMeta) {
            char *outBlob = NULL;
            Status status2 = reapThisGroupToCleanout(groupId,
                                                     scope,
                                                     &outBlob,
                                                     errorBlobOut,
                                                     retAppInternalError);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Run App %s group %lu failed group reap:%s",
                        app->getName(),
                        groupId,
                        strGetFromStatus(status2));
            }

            // Reap returns the error to be presented to user.
            status = status2;
            if (outBlob != NULL) {
                memFree(outBlob);
                outBlob = NULL;
            }
        }
    }
    return status;
}

Status
AppGroupMgr::runThisGroupGlobal(App *app,
                                Txn appTxn,
                                AppGroup::Id groupId,
                                const char *userIdName,
                                uint64_t sessionId,
                                const char *inBlob,
                                uint64_t cookie,
                                AppGroup::Id *retAppGroupId,
                                char **errorBlobOut,
                                LibNsTypes::NsHandle appHandle,
                                bool *retAppInternalError)
{
    Status status = StatusOk;
    AppGroup::Scope scope = AppGroup::Scope::Global;

    const size_t inBlobSize = strlen(inBlob) + 1;
    const size_t argsSize = sizeof(Args) + inBlobSize;
    size_t userIdNameMaxSize;
    Args *args = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    bool initMeta = false;
    unsigned numXpus =
        Runtime::get()->getThreadsCount(Txn::currentTxn().rtSchedId_);
    MsgMgr *msgMgr = MsgMgr::get();

    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(argsSize + sizeof(Gvm::Payload));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s failed:%s",
                app->getName(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    args = (Args *) gPayload->buf;
    args->groupId = groupId;
    args->origin = Config::get()->getMyNodeId();
    args->initMetadata.scope = scope;
    args->initMetadata.inBlobSize = inBlobSize;
    args->initMetadata.cookie = cookie;
    args->initMetadata.txn = appTxn;
    args->initMetadata.appHandle = appHandle;

    if (app->getFlags() & App::FlagInstancePerNode) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            args->initMetadata.xpusPerNode[ii] = 1;
        }
    } else {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (!msgMgr->symmetricalNodes()) {
                args->initMetadata.xpusPerNode[ii] =
                    msgMgr->numCoresOnNode((NodeId) ii);
            } else {
                // With symmetric cores, the number of threads in the
                // runtime scheduler only depends on the Txn and is same
                // on all nodes in the cluster.
                args->initMetadata.xpusPerNode[ii] = numXpus;
            }
        }
    }
    args->initMetadata.xpusPerNodeCount = nodeCount;

    // XXX: Why is argsSize used below, and not XcalarApiMaxAppNameLen + 1??
    verify(strlcpy(args->initMetadata.appName, app->getName(), argsSize) <
           argsSize);

    userIdNameMaxSize = sizeof(args->initMetadata.userIdName);
    verify(strlcpy(args->initMetadata.userIdName,
                   userIdName,
                   userIdNameMaxSize) < userIdNameMaxSize);
    args->initMetadata.sessionId = sessionId;
    verify(strlcpy(args->initMetadata.inBlob, inBlob, inBlobSize) < inBlobSize);

    gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppGroupMgrGvm::Action::InitMetadata,
                   argsSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if ((status == StatusOk && nodeStatus[ii] != StatusOk) ||
                (status != StatusOk && nodeStatus[ii] != StatusOk &&
                 nodeStatus[ii] != StatusXpuConnAborted)) {
                status = nodeStatus[ii];
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s failed on GVM InitMetadata:%s",
                app->getName(),
                strGetFromStatus(status));
        // Even though GVM failed, InitMetadata phase of AppGroup does not
        // run the user code in XPUs yet. So there is no problem issuing
        // TXN clean out.
        goto CommonExit;
    }

    //
    // AppGroup metadata propagated to all nodes. Send command to start Apps.
    //
    initMeta = true;
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Args) + sizeof(Gvm::Payload));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s group %lu failed:%s",
                app->getName(),
                groupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    args = (Args *) gPayload->buf;
    args->groupId = groupId;
    args->origin = Config::get()->getMyNodeId();

    gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppGroupMgrGvm::Action::Start,
                   sizeof(Args));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if ((status == StatusOk && nodeStatus[ii] != StatusOk) ||
                (status != StatusOk && nodeStatus[ii] != StatusOk &&
                 nodeStatus[ii] != StatusXpuConnAborted)) {
                status = nodeStatus[ii];
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Run App %s group %lu failed on GVM Start:%s",
                app->getName(),
                groupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (status != StatusOk) {
        if (initMeta) {
            // Abort the group run
            Status status2 =
                abortThisGroup(groupId, status, retAppInternalError);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Run App %s group %lu failed group abort:%s",
                        app->getName(),
                        groupId,
                        strGetFromStatus(status2));
            }

            // Reap the group run if abort succeeded
            if (status2 == StatusOk) {
                char *outBlob = NULL;
                status2 = reapThisGroupToCleanout(groupId,
                                                  scope,
                                                  &outBlob,
                                                  errorBlobOut,
                                                  retAppInternalError);
                if (status2 != StatusOk) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Run App %s group %lu failed group reap:%s",
                            app->getName(),
                            groupId,
                            strGetFromStatus(status2));
                }

                // Reap returns the error to be presented to user.
                status = status2;
                if (outBlob != NULL) {
                    memFree(outBlob);
                    outBlob = NULL;
                }
            }
        } else {
            Status status2 = cleanupThisGroup(groupId);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Run App %s group %lu failed group cleanup:%s",
                        app->getName(),
                        groupId,
                        strGetFromStatus(status2));
            }
        }
    }
    return status;
}

//
// Methods used elsewhere in libapp that make global calls.
//

// Entry point to run a local or global App group. Runs AppGroup asynchronously
// on one or all nodes. What happens next depends on how subsequent messages
// are handled. Caller must decrement ref on group.
Status
AppGroupMgr::runThisGroup(App *app,
                          AppGroup::Scope scope,
                          const char *userIdName,
                          uint64_t sessionId,
                          const char *inBlob,
                          uint64_t cookie,
                          AppGroup::Id *retAppGroupId,
                          char **errorBlobOut,
                          LibNsTypes::NsHandle appHandle,
                          bool *retAppInternalError)
{
    Status status;
    *errorBlobOut = NULL;

    // Generate new, globally unique, AppGroup ID.
    AppGroup::Id groupId = XidMgr::get()->xidGetNext();

    // Get a new txnId for this appGroup. Note that an App always runs async and
    // the caller may choose to block sometimes based on it's policy. So App Run
    // should internally generate it's own TXN ID always.
    Txn curTxn = Txn::currentTxn();
    Txn appTxn = Txn::newTxn(curTxn.mode_, curTxn.rtSchedId_);
    assert(appTxn.valid());

    xSyslog(ModuleName,
            XlogDebug,
            "User %s starting app group %lu (%s) txn %lu,%hhu,%hhu",
            userIdName,
            groupId,
            app->getName(),
            appTxn.id_,
            (unsigned char) appTxn.rtType_,
            static_cast<uint8_t>(appTxn.rtSchedId_));

    if (scope == AppGroup::Scope::Local) {
        status = runThisGroupLocal(app,
                                   appTxn,
                                   groupId,
                                   userIdName,
                                   sessionId,
                                   inBlob,
                                   cookie,
                                   retAppGroupId,
                                   errorBlobOut,
                                   appHandle,
                                   retAppInternalError);
    } else {
        assert(scope == AppGroup::Scope::Global);
        status = runThisGroupGlobal(app,
                                    appTxn,
                                    groupId,
                                    userIdName,
                                    sessionId,
                                    inBlob,
                                    cookie,
                                    retAppGroupId,
                                    errorBlobOut,
                                    appHandle,
                                    retAppInternalError);
    }
    BailIfFailed(status);
    status = StatusOk;
    *retAppGroupId = groupId;

CommonExit:
    if (status != StatusOk) {
        AppMgr::get()->closeAppHandle(app, appHandle);
    }
    return status;
}

// Report that this node is "done" with an AppGroup.
Status
AppGroupMgr::nodeDoneThisGroup(AppGroup::Id groupId)
{
    AppGroupMgr::Args *args = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Status status = StatusOk;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Args));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App nodeDoneThisGroup group %lu failed:%s",
                groupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    args = (AppGroupMgr::Args *) gPayload->buf;
    args->groupId = groupId;
    args->origin = Config::get()->getMyNodeId();

    gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppGroupMgrGvm::Action::NodeDone,
                   sizeof(Args));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if ((status == StatusOk && nodeStatus[ii] != StatusOk) ||
                (status != StatusOk && nodeStatus[ii] != StatusOk &&
                 nodeStatus[ii] != StatusXpuConnAborted)) {
                status = nodeStatus[ii];
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App nodeDoneThisGroup group %lu failed on GVM NodeDone:%s",
                groupId,
                strGetFromStatus(status));
    }
CommonExit:
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
AppGroupMgr::reapThisGroupToCleanout(AppGroup::Id groupId,
                                     AppGroup::Scope scope,
                                     char **outBlobOut,
                                     char **errorBlobOut,
                                     bool *retAppInternalError)
{
    unsigned nodeCount = Config::get()->getActiveNodes();
    void *outputPerNode[MaxNodes];
    size_t sizePerNode[MaxNodes];

    *outBlobOut = NULL;
    *errorBlobOut = NULL;

    // Needed for local case.
    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));

    Status status = reapThisGroup(groupId,
                                  scope,
                                  outputPerNode,
                                  sizePerNode,
                                  retAppInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu reapThisGroupToCleanout failed on"
                " reapThisGroup:%s",
                groupId,
                strGetFromStatus(status));
    }

    Status statusUnpack = unpackOutput(groupId,
                                       (OutputPacked **) outputPerNode,
                                       sizePerNode,
                                       nodeCount,
                                       outBlobOut,
                                       errorBlobOut);
    if (statusUnpack != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu reapThisGroupToCleanout failed on"
                " unpackOutput:%s",
                groupId,
                strGetFromStatus(statusUnpack));
    }

    for (size_t ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
        }
    }
    return status;
}

// Gather all output and destroy all metadata on all nodes. Caller must handle
// nodes that don't return output.
Status
AppGroupMgr::reapThisGroup(AppGroup::Id groupId,
                           AppGroup::Scope scope,
                           void **outputPerNode,
                           size_t *sizePerNode,
                           bool *retAppInternalError)
{
    // XXX Get output from 2pc without a similarly sized input payload.
    // FIXME:Xc-9341 get rid of this and use streaming instead
    NodeId me = Config::get()->getMyNodeId();
    Status status;
    Gvm::Payload *gPayload = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    *retAppInternalError = false;
    for (size_t ii = 0; ii < nodeCount; ii++) {
        outputPerNode[ii] = NULL;
        sizePerNode[ii] = 0;
    }

    if (scope == AppGroup::Scope::Local) {
        size_t outputSize = AppInstance::InputOutputLimit;
        void *output = memAlloc(outputSize);
        if (output == NULL) {
            status = StatusNoMem;
            *retAppInternalError = true;
            xSyslog(ModuleName,
                    XlogErr,
                    "App reapThisGroup group %lu failed:%s",
                    groupId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = localThisGroupReap(me, groupId, output, &outputSize);
        outputPerNode[me] = output;
        if (status == StatusOk) {
            if (outputSize != 0) {
                sizePerNode[me] = outputSize - sizeof(Status);
                status = *((Status *) ((uintptr_t) outputPerNode[me] +
                                       sizePerNode[me]));
            }
        }
    } else {
        assert(scope == AppGroup::Scope::Global);
        Args *args = NULL;

        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        gPayload =
            (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Args));
        if (gPayload == NULL) {
            status = StatusNoMem;
            *retAppInternalError = true;
            xSyslog(ModuleName,
                    XlogErr,
                    "App reapThisGroup group %lu failed:%s",
                    groupId,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        args = (Args *) gPayload->buf;
        args->groupId = groupId;
        args->origin = me;

        gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                       (uint32_t) AppGroupMgrGvm::Action::Reap,
                       sizeof(Args));
        status = Gvm::get()->invokeWithOutput(gPayload,
                                              AppInstance::InputOutputLimit,
                                              outputPerNode,
                                              sizePerNode,
                                              nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    if (status == StatusOk ||
                        (status != StatusOk &&
                         nodeStatus[ii] != StatusXpuConnAborted)) {
                        status = nodeStatus[ii];
                    }
                } else {
                    if (sizePerNode[ii] != 0) {
                        sizePerNode[ii] -= sizeof(Status);
                        Status appGroupError =
                            *((Status *) ((uintptr_t) outputPerNode[ii] +
                                          sizePerNode[ii]));
                        if ((appGroupError != StatusOk && status == StatusOk) ||
                            (appGroupError != StatusOk && status != StatusOk &&
                             appGroupError != StatusXpuConnAborted)) {
                            // StatusXpuConnAborted may just be a symptom. Try
                            // to capture the first order error status.
                            status = appGroupError;
                        }
                    }
                }
            }
        } else {
            *retAppInternalError = true;
        }
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App reapThisGroup group %lu failed: %s",
                groupId,
                strGetFromStatus(status));
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
AppGroupMgr::cleanupThisGroup(AppGroup::Id groupId)
{
    Status status = StatusOk;
    AppGroupMgr::Args *args = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Args));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App cleanupThisGroup group %lu failed:%s",
                groupId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    args = (AppGroupMgr::Args *) gPayload->buf;
    args->groupId = groupId;
    args->origin = Config::get()->getMyNodeId();

    gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppGroupMgrGvm::Action::Cleanup,
                   sizeof(Args));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if ((status == StatusOk && nodeStatus[ii] != StatusOk) ||
                (status != StatusOk && nodeStatus[ii] != StatusOk &&
                 nodeStatus[ii] != StatusXpuConnAborted)) {
                status = nodeStatus[ii];
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App cleanupThisGroup group %lu failed on GVM cleanup: %s",
                groupId,
                strGetFromStatus(status));
    }
CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
AppGroupMgr::listAllAppGroupIds(AppGroup::Id **groupIdsOut,
                                size_t *numGroupIdsOut)
{
    Status status = StatusUnknown;
    size_t numGroupIds = 0;
    size_t ii = 0;
    AppGroup::Id *groupIds = NULL;
    AppGroup *group = NULL;
    bool groupsLockHeld = false;

    groupsLock_.lock();
    groupsLockHeld = true;

    numGroupIds = groups_.getSize();

    groupIds = (AppGroup::Id *) memAlloc(sizeof(*groupIds) * numGroupIds);
    if (groupIds == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (auto groupIter = groups_.begin(); (group = groupIter.get()) != NULL;
         groupIter.next()) {
        groupIds[ii++] = group->id_;
    }

    *groupIdsOut = groupIds;
    groupIds = NULL;
    *numGroupIdsOut = numGroupIds;
    status = StatusOk;
CommonExit:
    if (groupsLockHeld) {
        groupsLock_.unlock();
        groupsLockHeld = false;
    }

    if (groupIds != NULL) {
        memFree(groupIds);
        groupIds = NULL;
    }

    return status;
}

Status
AppGroupMgr::abortThisGroup(AppGroup::Id groupId,
                            Status abortStatus,
                            bool *retAppInternalError)
{
    Status status = StatusOk;
    AppGroupMgr::Args *args = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(Args));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App reapThisGroup group %lu failed:%s",
                groupId,
                strGetFromStatus(status));
        if (retAppInternalError) {
            *retAppInternalError = true;
        }
        goto CommonExit;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    args = (AppGroupMgr::Args *) gPayload->buf;
    args->groupId = groupId;
    args->origin = Config::get()->getMyNodeId();
    args->abortStatus = abortStatus;

    gPayload->init(AppGroupMgrGvm::get()->getGvmIndex(),
                   (uint32_t) AppGroupMgrGvm::Action::Abort,
                   sizeof(Args));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if ((status == StatusOk && nodeStatus[ii] != StatusOk) ||
                (status != StatusOk && nodeStatus[ii] != StatusOk &&
                 nodeStatus[ii] != StatusXpuConnAborted)) {
                status = nodeStatus[ii];
            }
        }
    } else {
        if (retAppInternalError) {
            *retAppInternalError = true;
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App abortThisGroup group %lu failed on GVM Abort: %s",
                groupId,
                strGetFromStatus(status));
    }
CommonExit:
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

// Unpack result of reap from each node and concatenate output together.
// The format is an array of node outputs where the node output is an array
// of app outputs.
// AppOut := String
// nodeOut := [AppOut]
// totalOut := [nodeOut]
Status
AppGroupMgr::unpackOutput(AppGroup::Id appGroupId,
                          OutputPacked **packs,
                          size_t *packSizes,
                          size_t packsCount,
                          char **outBlobOut,
                          char **errorBlobOut)
{
    Status status = StatusOk;
    json_t *nodeOutJson = NULL;
    json_t *nodeErrJson = NULL;
    json_t *outJsonArray = NULL;
    json_t *errJsonArray = NULL;
    char *out = NULL;
    char *err = NULL;

    outJsonArray = json_array();
    BailIfNull(outJsonArray);

    errJsonArray = json_array();
    BailIfNull(errJsonArray);

    for (size_t ii = 0; ii < packsCount; ii++) {
        int ret;
        json_error_t jsonError;
        if (packs[ii] == NULL) {
            continue;
        }
        if (packSizes[ii] < sizeof(OutputPacked)) {
            continue;
        }

        // Unpack strings
        const char *nodeErr;
        const char *nodeOut;
        if (packs[ii]->errorSize > 0) {
            nodeErr = packs[ii]->pack;
        } else {
            nodeErr = "";
        }
        if (packs[ii]->outputSize > 0) {
            nodeOut = packs[ii]->pack + packs[ii]->errorSize;
        } else {
            nodeOut = "";
        }

        // Recreate the json app out array for this node, and add it to the
        // array of nodes.
        nodeOutJson = json_loads(nodeOut, 0, &jsonError);
        if (nodeOutJson == NULL) {
            // This can also happen in the case of OOM
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu unpackOutput failed to parse app json"
                    " output: %s",
                    appGroupId,
                    jsonError.text);
            status = StatusAppFailedToGetOutput;
            goto CommonExit;
        }

        if (json_typeof(nodeOutJson) != JSON_ARRAY) {
            // This can also happen in the case of OOM
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu unpackOutput failed; App json output not"
                    " array: %s",
                    appGroupId,
                    nodeOut);
            status = StatusAppFailedToGetOutput;
            goto CommonExit;
        }

        ret = json_array_append_new(outJsonArray, nodeOutJson);
        if (ret == -1) {
            json_decref(nodeOutJson);
            nodeOutJson = NULL;
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu failed to append json output '%s'",
                    appGroupId,
                    nodeOut);
            status = StatusAppFailedToGetOutput;
            goto CommonExit;
        }

        // The reference has now been stolen
        nodeOutJson = NULL;

        // Now we do the same thing for error output
        nodeErrJson = json_loads(nodeErr, 0, &jsonError);
        if (nodeErrJson == NULL) {
            // This can also happen in the case of OOM
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu failed to parse app json error output: %s",
                    appGroupId,
                    jsonError.text);
            status = StatusAppFailedToGetError;
            goto CommonExit;
        }
        if (json_typeof(nodeErrJson) != JSON_ARRAY) {
            // This can also happen in the case of OOM
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu json error output not array: %s",
                    appGroupId,
                    nodeErr);
            status = StatusAppFailedToGetError;
            goto CommonExit;
        }
        ret = json_array_append_new(errJsonArray, nodeErrJson);
        if (ret == -1) {
            json_decref(nodeErrJson);
            nodeErrJson = NULL;
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu failed to append json error output '%s'",
                    appGroupId,
                    nodeErr);
            status = StatusAppFailedToGetError;
            goto CommonExit;
        }
        nodeErrJson = NULL;
    }

    assert(outJsonArray);
    assert(errJsonArray);

    // Now we have our json arrays for the outputs. We can render this as
    // a string to return now.

    // This is an array, so we don't need to have JSON_ENCODE_ANY
    out = json_dumps(outJsonArray, 0);
    BailIfNull(out);

    err = json_dumps(errJsonArray, 0);
    BailIfNull(err);

CommonExit:
    if (outJsonArray != NULL) {
        json_decref(outJsonArray);
        outJsonArray = NULL;
    }
    if (errJsonArray != NULL) {
        json_decref(errJsonArray);
        errJsonArray = NULL;
    }
    if (nodeOutJson != NULL) {
        json_decref(nodeOutJson);
        nodeOutJson = NULL;
    }
    if (nodeErrJson != NULL) {
        json_decref(nodeErrJson);
        nodeErrJson = NULL;
    }

    *outBlobOut = out;
    *errorBlobOut = err;
    return status;
}

Status
AppGroupMgr::setupMsgStreams()
{
    Status status = StatusOk;
    Config *config = Config::get();
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned activeNodes = config->getActiveNodes();

    msgStreamIds_ = (Xid *) memAlloc(sizeof(Xid) * activeNodes);
    if (msgStreamIds_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Setup MsgStreams failed with %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < activeNodes; ii++) {
        msgStreamIds_[ii] = XidInvalid;
    }

    for (unsigned ii = 0; ii < activeNodes; ii++) {
        if (ii == (unsigned) myNodeId) {
            // We are attempting to establish MsgStream from source node to a
            // destination node.
            continue;
        }

        // Since the stream is generic for any transaction to ride on, there is
        // no need to set up a stream cookie.
        status = msgStreamMgr
                     ->startStream(TwoPcSingleNode,
                                   ii,
                                   MsgStreamMgr::StreamObject::XpuCommunication,
                                   NULL,
                                   &msgStreamIds_[ii]);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Start MsgStreams to node %u failed with %s.",
                    ii,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

void
AppGroupMgr::teardownMsgStreams()
{
    Config *config = Config::get();
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned activeNodes = config->getActiveNodes();

    if (msgStreamIds_ != NULL) {
        for (unsigned ii = 0; ii < activeNodes; ii++) {
            if (ii == (unsigned) myNodeId) {
                // We are attempting to establish MsgStream from source node to
                // a destination node.
                continue;
            }
            if (msgStreamIds_[ii] != XidInvalid) {
                msgStreamMgr->endStream(msgStreamIds_[ii]);
            }
        }
        memFree(msgStreamIds_);
        msgStreamIds_ = NULL;
    }
}
