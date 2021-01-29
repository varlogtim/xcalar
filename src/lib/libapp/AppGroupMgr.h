// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPGROUPMGR_H
#define APPGROUPMGR_H

#include "primitives/Primitives.h"
#include "app/AppGroup.h"
#include "gvm/GvmTarget.h"
#include "runtime/Spinlock.h"
#include "libapis/LibApisConstants.h"
#include "AppGroupMgrGvm.h"
#include "msgstream/MsgStream.h"

class AppMgr;

class AppGroupMgr final
{
    friend class AppGroupMgrGvm;
    friend class AppGroup;
    friend class XpuCommObjectAction;

  public:
    AppGroupMgr(AppMgr *appMgr);
    ~AppGroupMgr();

    MustCheck AppGroup *getThisGroup(AppGroup::Id groupId)
    {
        groupsLock_.lock();
        AppGroup *group = groups_.find(groupId);
        if (group != NULL) {
            group->incRef();
        }
        groupsLock_.unlock();
        return group;
    }

    Status listAllAppGroupIds(AppGroup::Id **groupIdsOut,
                              size_t *numGroupIdsOut);

    // Run this App with global/local scope. This is the entry point for running
    // an App. An AppGroup Id is returned to the caller, which is in turn used
    // to manage the life cycle of this particular execution of App. Note that
    // the same App can be run multiple times and each will be managed with it's
    // respective AppGroup.
    MustCheck Status runThisGroup(App *app,
                                  AppGroup::Scope scope,
                                  const char *userIdName,
                                  uint64_t sessionId,
                                  const char *inBlob,
                                  uint64_t cookie,
                                  AppGroup::Id *retAppGroupId,
                                  char **errorBlobOut,
                                  LibNsTypes::NsHandle appHandle,
                                  bool *retAppInternalError);

    // Reap this AppGroup. This cleans up all the AppGroup related state cluster
    // wide and collects output of App Run kept locally on all nodes in the
    // cluster and returns to the caller.
    MustCheck Status reapThisGroup(AppGroup::Id groupId,
                                   AppGroup::Scope scope,
                                   void **outputPerNode,
                                   size_t *sizePerNode,
                                   bool *retAppInternalError);

    // Abort this AppGroup. This ensures that all AppInstance(s) (Apps are
    // executed MPP style on all nodes in the cluster) finishes executing and
    // all XPUs have been returned to the parent's free pool.
    MustCheck Status abortThisGroup(AppGroup::Id groupId,
                                    Status abortReason,
                                    bool *retAppInternalError);

    // An AppGroup is executed MPP style and each node locally tracks all it's
    // AppInstance(s). When a node locally accounts for all it's AppInstance(s)
    // completion, it updates it's state to all other nodes in the cluster.
    MustCheck Status nodeDoneThisGroup(AppGroup::Id groupId);

    // Set up message streams for XPU<->XPU communication.
    MustCheck Status setupMsgStreams();

    // Tear down message streams set up for XPU<->XPU communication.
    void teardownMsgStreams();

  private:
    static constexpr const char *ModuleName = "AppGroupMgr";

    struct OutputPacked {
        size_t errorSize;
        size_t outputSize;
        char pack[0];
    };

    struct InitMetadata {
        char appName[XcalarApiMaxAppNameLen + 1];
        AppGroup::Scope scope;
        char userIdName[LOGIN_NAME_MAX + 1];
        uint64_t sessionId;
        uint64_t cookie;
        Txn txn;
        LibNsTypes::NsHandle appHandle;
        unsigned xpusPerNodeCount;
        unsigned xpusPerNode[MaxNodes];
        size_t inBlobSize;
        char inBlob[0];
    };

    // Broadcast message formats.
    struct Args {
        Args() : initMetadata() {}
        AppGroup::Id groupId;
        NodeId origin;

        // Union must be last.
        union {
            InitMetadata initMetadata;
            Status abortStatus;
        };
    };

    Mutex groupsLock_;
    AppGroup::MgrHashTable groups_;
    AppMgr *appMgr_;
    Xid *msgStreamIds_ = NULL;

    // Handlers for AppGroup with local scope.
    MustCheck Status localThisGroupInitMetadata(AppGroup::Id groupId,
                                                const char *appName,
                                                AppGroup::Scope scope,
                                                const char *userIdName,
                                                uint64_t sessionId,
                                                const char *inBlob,
                                                uint64_t cookie,
                                                Txn txn,
                                                LibNsTypes::NsHandle appHandle,
                                                unsigned *xpusPerNode,
                                                unsigned xpusPerNodeCount);

    MustCheck Status localThisGroupReap(NodeId originNode,
                                        AppGroup::Id groupId,
                                        void *payload,
                                        size_t *outputSizeOut);

    MustCheck Status localThisGroupCleanup(NodeId originNode,
                                           AppGroup::Id groupId);

    // Global AppGroup cleanout for internal errors.
    MustCheck Status reapThisGroupToCleanout(AppGroup::Id groupId,
                                             AppGroup::Scope scope,
                                             char **outBlobOut,
                                             char **errorBlobOut,
                                             bool *retAppInternalError);

    // Create App Group namespace entry.
    MustCheck Status publishAppGroupToNs(AppGroup::Id appGroupId);

    MustCheck Status unpackOutput(AppGroup::Id appGroupId,
                                  OutputPacked **packs,
                                  size_t *packSizes,
                                  size_t packsCount,
                                  char **outBlobOut,
                                  char **errorBlobOut);

    MustCheck Status runThisGroupLocal(App *app,
                                       Txn appTxn,
                                       AppGroup::Id groupId,
                                       const char *userIdName,
                                       uint64_t sessionId,
                                       const char *inBlob,
                                       uint64_t cookie,
                                       AppGroup::Id *retAppGroupId,
                                       char **errorBlobOut,
                                       LibNsTypes::NsHandle appHandle,
                                       bool *retAppInternalError);

    MustCheck Status runThisGroupGlobal(App *app,
                                        Txn appTxn,
                                        AppGroup::Id groupId,
                                        const char *userIdName,
                                        uint64_t sessionId,
                                        const char *inBlob,
                                        uint64_t cookie,
                                        AppGroup::Id *retAppGroupId,
                                        char **errorBlobOut,
                                        LibNsTypes::NsHandle appHandle,
                                        bool *retAppInternalError);

    MustCheck Status cleanupThisGroup(AppGroup::Id groupId);

    // Disallow.
    AppGroupMgr(const AppGroupMgr &) = delete;
    AppGroupMgr &operator=(const AppGroupMgr &) = delete;
};

#endif  // APPGROUPMGR_H
