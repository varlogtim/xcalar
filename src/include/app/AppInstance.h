// Copyright 2019 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPINSTANCE_H
#define APPINSTANCE_H

#include "primitives/Primitives.h"
#include "parent/IParentNotify.h"
#include "util/IntHashTable.h"
#include "util/AtomicTypes.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "dataset/AppLoader.h"
#include "localmsg/LocalConnection.h"
#include "parent/ParentChild.h"
#include "runtime/CondVar.h"

class App;
class AppGroup;
class ParentChild;
class XpuCommObjectAction;

//
// Represents running instance of an App. Handles commands coming in from
// running App.
//

// XXX AppInstance currently is polluted with Load related state. This needs to
// be cleaned up.

class AppInstance final
    : public IParentNotify  // Receive messages from App in XPU.
{
    friend class XpuCommObjectAction;
    friend class AppGroup;

  public:
    typedef Xid Id;

    // The largest message here is the output of listing files.
    // With 30 character file names, this allows ~1.4 million files.
    static constexpr size_t InputOutputLimit = 40 * MB;

    //
    // Interface libapp exposes to Apps running in XPU.
    //

    AppInstance();

    // AppInstance is ref-counted due to concurrent access. Initial ref is given
    // to AppGroup::appInstances_. Any code tinkering with an AppInstance should
    // get a ref through AppGroup to make sure the instance still exists.
    void refGet() override;
    void refPut() override;

    MustCheck Status init(App *app,
                          AppGroup *group,
                          const char *inBlob,
                          const char *userIdName,
                          uint64_t sessionId,
                          uint64_t cookie,
                          Txn txn,
                          unsigned xpuId,
                          unsigned xpuClusterSize);
    MustCheck Status startMyInstance(LocalConnection::Response **responseOut,
                                     ParentChild *newChild);
    void abortMyInstance(Status status);
    MustCheck ParentChild::Id getMyChildId() const;
    MustCheck size_t getMyXpuId() const;

    // Sidebuffer processors call this when finished
    void bufferLoaded(const BufferResult *loadResult);

    // IParentNotify.
    void onDeath() override;

    void onRequestMsg(LocalMsgRequestHandler *reqHandler,
                      LocalConnection *connection,
                      const ProtoRequestMsg *request,
                      ProtoResponseMsg *response);

  private:
    static constexpr size_t SlotsAppGroupToAppInstance = 7;
    static constexpr size_t SlotsAppMgrToAppInstance = 127;
    static constexpr uint64_t TimeoutUSecsStart =
        USecsPerSec * SecsPerMinute * 5;
    static constexpr const char *ModuleName = "AppInstance";

    enum class State { Invalid = 10, Inited, Running, Destructing, Done };

    struct LoadResult {
        Status status;
        size_t numBuffers;
        size_t numBytes;
        int64_t numFiles;
        char errorStringBuf[5 * KB];
        char errorFileBuf[256];
    };

    // Members set at init that don't change.
    App *app_;
    AppGroup *group_;
    IntHashTableHook appGroupHook_;  // Owned by AppGroup.
    IntHashTableHook appMgrHook_;    // Owned by AppMgr.
    Atomic32 ref_;

    // Due to async access, basic monitor-like locking used. Must be
    // acquired *after* group lock.
    mutable Mutex lock_;

    CondVar startCondVar_;
    CondVar doneCondVar_;
    State state_;
    ParentChild *child_;
    ChildAppStartRequest *startArgs_ = NULL;
    uint64_t cookie_;
    Txn txn_;
    unsigned xpuId_;

    // Store PID to deregister from AppMgr::appInstances_. Not assumed
    // up-to-date.
    pid_t pid_;

    //
    // Load specific stuff
    //

    // Outstanding sidebufs for processing locally
    Atomic64 outstandingOps_;
    CondVar outstandingOpsCv_;
    Atomic64 outstandingBuffers_;
    Semaphore sideBufsLoadedSem_;
    LoadResult loadResult_;

    void doneMyInstance(Status doneStatus,
                        const char *outStr,
                        const char *errStr);
    MustCheck Status updateProgress(int64_t numFiles);
    void setTotalWork(int64_t numFiles, bool downSampled);
    MustCheck Status loadBuffers(const ParentAppLoadBufferRequest *loadRequest);
    MustCheck Status sendListToDstXpus(LocalMsgRequestHandler *reqHandler,
                                       const XpuSendListToDsts *sendListToDsts);
    MustCheck Status getLoadOutput(char **outStr);
    MustCheck Status getLoadErr(char **errStr, bool *foundError);

    struct BufDesc {
        void *buf;
        size_t bufLen;
    };
    MustCheck Status dispatchToDstXpu(BufDesc *recvBuff,
                                      unsigned recvBufCount,
                                      unsigned srcXpuId);

    Status exportRuntimeHistograms(ProtoResponseMsg *response);

    void destroy();

    ~AppInstance();

    // Disallow.
    AppInstance(const AppInstance &) = delete;
    AppInstance &operator=(const AppInstance &) = delete;

  public:
    typedef IntHashTable<Id,
                         AppInstance,
                         &AppInstance::appGroupHook_,
                         &AppInstance::getMyXpuId,
                         SlotsAppGroupToAppInstance,
                         hashIdentity>
        AppGroupHashTable;
    typedef AppGroupHashTable::iterator AppGroupHashTableIter;

    typedef IntHashTable<ParentChild::Id,
                         AppInstance,
                         &AppInstance::appMgrHook_,
                         &AppInstance::getMyChildId,
                         SlotsAppMgrToAppInstance,
                         hashIdentity>
        AppMgrHashTable;
};

#endif  // APPINSTANCE_H
