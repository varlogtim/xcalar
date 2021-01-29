// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPGROUP_H
#define APPGROUP_H

#include <jansson.h>

#include "primitives/Primitives.h"
#include "app/AppInstance.h"
#include "operators/GenericTypes.h"
#include "util/IntHashTable.h"
#include "hash/Hash.h"
#include "runtime/Mutex.h"
#include "runtime/Semaphore.h"
#include "util/AtomicTypes.h"
#include "runtime/CondVar.h"

//
// XXX Entire AppGroup should be abstracted away from the users of Apps
// infrastructure. It's too much of Apps internal implementation detail.
//

// Group of AppInstance(s) (running Apps) running across the cluster.
class AppGroup final
{
    friend class AppGroupMgr;
    friend class AppGroupMgrGvm;
    friend class AppInstance;
    friend class AppMgr;
    friend class XpuCommObjectAction;

  public:
    typedef Xid Id;

    enum class Scope { Local, Global };

    AppGroup();
    MustCheck Status init(Id id,
                          App *app,
                          Scope scope,
                          const char *userIdName,
                          uint64_t sessionId,
                          const char *inBlob,
                          uint64_t cookie,
                          Txn txn,
                          LibNsTypes::NsHandle appHandle,
                          unsigned *xpusPerNode,
                          unsigned xpusPerNodeCount);
    MustCheck NodeId getXceNodeIdFromXpuId(unsigned xpuId);
    MustCheck unsigned getNumXpusPerXceNode(NodeId nodeId);
    MustCheck Id getMyId() const;
    MustCheck Scope getMyScope() const;
    void incRef();
    void decRef();

  private:
    // A guess at the number of AppGroups in use at one time. Guess big, this
    // doesn't take up much space.
    static constexpr unsigned SlotsAppGroup = 241;
    static constexpr const char *ModuleName = "libapp";

    enum class State {
        Invalid,
        Inited,
        Running,
        Done,
    };

    struct NodeInfo {
        NodeId nodeId;
        State state;
        unsigned numXpus;
    };

    struct OutputPacked {
        size_t errorSize;
        size_t outputSize;
        char pack[0];
    };

    Id id_;
    Scope scope_;
    IntHashTableHook hook_;  // Used for hashtable in AppMgr.
    Txn txn_;
    Mutex lock_;
    AppInstance::AppGroupHashTable appInstances_;
    size_t appInstancesCount_;
    Atomic32 ref_;

    // Blocks until all the local AppInstances have been aborted.
    bool abortInvoked_ = false;
    bool abortStillPending_ = false;
    CondVar abortCompletion_;

    // nodes_ tracks what we know about other nodes. A node will only broadcast
    // its state on a "need to know" basis, so these shouldn't be considered
    // accurate.
    NodeInfo *nodes_;

    size_t nodesCount_;

    // Signalled once AppGroup is done on all nodes.
    Semaphore reapable_;
    Semaphore myGroupAllNodesDone_;
    bool reapDone_ = false;
    unsigned xpuClusterSize_;

    // App Handle
    App *app_ = NULL;
    LibNsTypes::NsHandle appHandle_;

    //
    // Results of AppInstances that are done.
    //

    // Generally reflects the first failure detected or StatusOk.
    Status overallStatus_;

    json_t *outJsonArray_;
    json_t *errorJsonArray_;

    // GVM local handlers
    MustCheck Status localMyGroupStart();
    void localMyGroupAbort(Status status);
    void localMyGroupNodeDone(NodeId nodeId);
    void localMyGroupInstanceDone(AppInstance *appInstance,
                                  Status doneStatus,
                                  const char *outStr,
                                  const char *errStr);
    MustCheck Status localMyGroupReap(void *output, size_t *outputSizeOut);
    void localRemoveAppInstanceFromGroup(AppInstance *appInstance);

    MustCheck Status appendOutput(const char *outStr, const char *errStr);

    MustCheck NodeInfo *getNodeInfo(NodeId nodeId) const;
    void waitForMyGroupAllNodesDone();
    void waitForPendingAbort();
    MustCheck bool kickLocalAbort();

    // Wait cluster-wide for reaping the AppGroup results kept around.
    MustCheck Status waitForMyGroupResult(uint64_t usecsTimeout,
                                          char **outBlobOut,
                                          char **errorBlobOut,
                                          bool *retAppInternalError);

    MustCheck bool isXpuIdValid(unsigned xpuId);

    MustCheck Status startInstances(LocalConnection::Response **responseOut,
                                    AppInstance **appInstances,
                                    int numChildren);

    class SchedulableNodeDone : public Schedulable
    {
      public:
        SchedulableNodeDone(AppGroup::Id id);

        void run() override;
        void done() override;

      private:
        AppGroup::Id id_;
    };

    static constexpr unsigned SlotsXpuCommStreamBuf = 7;
    static constexpr unsigned SlotsXpuCommStream = 7;

    Mutex xpuCommStreamHtLock_;

    struct XpuCommStreamBuf {
        IntHashTableHook hook;

        // Pieces of stream buffer
        void *payload = NULL;

        MustCheck size_t getMySeq() const;

        void doDelete();
        XpuCommStreamBuf() = default;
        ~XpuCommStreamBuf();
    };

    typedef IntHashTable<size_t,
                         XpuCommStreamBuf,
                         &XpuCommStreamBuf::hook,
                         &XpuCommStreamBuf::getMySeq,
                         SlotsXpuCommStreamBuf,
                         hashIdentity>
        XpuCommStreamBufHT;

    class XpuCommStream
    {
      public:
        static constexpr const unsigned XpuIdInvalid = (unsigned) -1;
        IntHashTableHook hook_;

        // Assume that there could be concurrent XPU<->XPU streams for the same
        // AppGroup. So commId uniquely identifies an open XPU<->XPU
        // communication stream.
        Xid commId_ = XidInvalid;

        unsigned xpuSrcId_ = XpuIdInvalid;
        unsigned xpuDstId_ = XpuIdInvalid;
        size_t totalBufs_ = 0;
        size_t streamBufsTotalSize_ = 0;

        Mutex lock_;

        MustCheck Xid getMyId() const { return commId_; }

        void doDelete();
        XpuCommStream() {}
        virtual ~XpuCommStream() {}
    };

    class SrcXpuCommStream : public XpuCommStream
    {
      public:
        size_t numStreamBufsSent_ = 0;
        size_t streamBufsSentTotalSize_ = 0;
        size_t numStreamBufsCompletions_ = 0;
        bool ackReceived_ = false;
        LocalMsgRequestHandler *reqHandler_ = NULL;

        // Manages the XPU communication stream buffers.
        XpuCommStreamBufHT srcXpuCommStreamBufHt_;

        SrcXpuCommStream() = default;
        virtual ~SrcXpuCommStream();
    };

    class DstXpuCommStream : public XpuCommStream
    {
      public:
        size_t numStreamBufsReceived_ = 0;
        size_t streamBufsRecvTotalSize_ = 0;
        bool ackSent_ = false;
        bool ackCompletion_ = false;
        AppInstance::BufDesc *recvBuf_ = NULL;
        size_t numRecvBufs_ = 0;

        DstXpuCommStream() = default;
        virtual ~DstXpuCommStream();
    };

    typedef IntHashTable<Xid,
                         XpuCommStream,
                         &XpuCommStream::hook_,
                         &XpuCommStream::getMyId,
                         SlotsXpuCommStream,
                         hashIdentity>
        XpuCommStreamHT;
    XpuCommStreamHT xpuCommStreamHt_;

    MustCheck Status dispatchToDstXpu(AppInstance::BufDesc *recvBufs,
                                      size_t numRecvBufs,
                                      unsigned srcXpuId,
                                      unsigned dstXpu);

    ~AppGroup();
    // Disallow.
    AppGroup(const AppGroup &) = delete;
    AppGroup &operator=(const AppGroup &) = delete;

  public:
    typedef IntHashTable<Id,
                         AppGroup,
                         &AppGroup::hook_,
                         &AppGroup::getMyId,
                         SlotsAppGroup,
                         hashIdentity>
        MgrHashTable;
};

#endif  // APPGROUP_H
