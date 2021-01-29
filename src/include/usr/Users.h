// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _USERS_H_
#define _USERS_H_

#include "primitives/Primitives.h"
#include "ns/LibNs.h"
#include "util/StringHashTable.h"
#include "sys/XLog.h"
#include "kvstore/KvStore.h"
#include "session/Sessions.h"
#include "xcalar/compute/localtypes/KvStore.pb.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

class UserMgr final
{
  public:
    static MustCheck Status init();
    void destroy();
    static MustCheck UserMgr *get();
    void shutdown();

    // Mainline session functions called from UI / CLI
    MustCheck Status create(XcalarApiUserId *user,
                            XcalarApiSessionNewInput *sessionInput,
                            XcalarApiSessionNewOutput *sessionOutput);

    MustCheck Status list(XcalarApiUserId *user,
                          const char *sessionListPattern,
                          XcalarApiOutput **outputOut,
                          size_t *sessionListOutputSize);

    MustCheck Status doDelete(XcalarApiUserId *user,
                              XcalarApiSessionDeleteInput *sessionInput,
                              XcalarApiSessionGenericOutput *sessionOutput);

    MustCheck Status rename(XcalarApiUserId *user,
                            XcalarApiSessionRenameInput *sessionInput,
                            XcalarApiSessionGenericOutput *sessionOutput);

    MustCheck Status inactivate(XcalarApiUserId *user,
                                XcalarApiSessionDeleteInput *sessionInput,
                                XcalarApiSessionGenericOutput *sessionOutput);

    MustCheck Status activate(XcalarApiUserId *user,
                              XcalarApiSessionActivateInput *sessionInput,
                              XcalarApiSessionGenericOutput *sessionOutput);

    MustCheck Status persist(XcalarApiUserId *user,
                             XcalarApiSessionDeleteInput *sessionInput,
                             XcalarApiOutput **outputOut,
                             size_t *sessionListOutputSize);

    MustCheck Status
    download(XcalarApiUserId *user,
             XcalarApiSessionDownloadInput *sessionDownloadInput,
             XcalarApiOutput **outputOut,
             size_t *outputSizeOut);

    // XXX: following may not be needed. See UserDefinedFunction.cpp
    MustCheck Status getSessionId(XcalarApiUserId *user,
                                  XcalarApiSessionInfoInput *sessionInput);

    // upload is modeled after ::create; both call createInternal()
    MustCheck Status upload(XcalarApiUserId *user,
                            XcalarApiSessionUploadInput *sessionInput,
                            XcalarApiSessionNewOutput *sessionOutput);

    MustCheck Status getOwnerNodeId(const char *userName, NodeId *nodeId);

    MustCheck Status getOwnerNodeIdAndFillIn(
        char *userName, XcalarApiSessionGenericOutput *sessionGenericOutput);

    // If sessionName is NULL, getDag will get the DAG of the one active
    // session belonging to the user (currently, a user can have only one
    // active session). In the future, a sessionName will be mandatory when
    // a user could have multiple active sessions.

    MustCheck Status getDag(XcalarApiUserId *userId,
                            const char *sessionName,
                            Dag **returnHandle);

    MustCheck Status
    getDag(const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
               *specifier,
           Dag **returnHandle);

    // Upgrades pre-DF2 dag in queryGraph to DF2 dataflow key/value pairs which
    // will be entered into the respective kv stores - some in global kvs and
    // some in the workbook specified by user/sessionInput.
    MustCheck Status queryToDF2Upgrade(Dag *queryGraph,
                                       XcalarApiUserId *user,
                                       XcalarApiSessionInfoInput *sessionInput);

    MustCheck Status getUdfContainer(const char *udfUserName,
                                     const char *udfSessionName,
                                     XcalarApiUdfContainer **retUdfContainer);

    MustCheck Status
    getSessionContainer(const char *udfUserName,
                        const char *udfSessionName,
                        XcalarApiUdfContainer **retSessionContainer);

    enum class OutstandOps {
        Inc,
        Dec,
    };
    MustCheck Status trackOutstandOps(XcalarApiUserId *userId,
                                      char *sessionName,
                                      OutstandOps oops);

    MustCheck Status trackOutstandOps(XcalarApiUdfContainer *udfContainer,
                                      UserMgr::OutstandOps oops);

    MustCheck Status trackOutstandOps(
        const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
            *specifier,
        OutstandOps oops);

    MustCheck Status
    getResultSetIdsByTableId(TableNsMgr::TableId tableId,
                             XcalarApiUserId *user,
                             XcalarApiSessionInfoInput *sessInput,
                             ResultSetId **rsIdsOut,
                             unsigned *numRsIdsOut);

    MustCheck Status trackResultset(
        ResultSetId rsId,
        TableNsMgr::TableId tableId,
        const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
            *specifier);

    MustCheck Status trackResultset(ResultSetId rsId,
                                    TableNsMgr::TableId tableId,
                                    XcalarApiUserId *user,
                                    XcalarApiSessionInfoInput *sessInput);

    void untrackResultset(
        ResultSetId rsId,
        TableNsMgr::TableId tableId,
        const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
            *specifier);

    void untrackResultset(ResultSetId rsId,
                          TableNsMgr::TableId tableId,
                          XcalarApiUserId *user,
                          XcalarApiSessionInfoInput *sessInput);

    void untrackResultsets(TableNsMgr::TableId tableId,
                           XcalarApiUserId *user,
                           XcalarApiSessionInfoInput *sessInput);

    MustCheck Status getKvStoreId(XcalarApiUserId *userId,
                                  XcalarApiSessionInfoInput *sessionInfo,
                                  KvStoreId *kvStoreId);

    MustCheck Status getUserAndSessInput(
        const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
            *specifier,
        XcalarApiUserId *user,
        XcalarApiSessionInfoInput *sessInput);

    MustCheck Status
    getKvStoreId(const xcalar::compute::localtypes::Workbook::WorkbookSpecifier
                     *specifier,
                 KvStoreId *kvStoreId);

    // XXX Exposed for test code
    MustCheck Status sessionGetDagById(XcalarApiUserId *userId,
                                       Dag **sessionGraphHandleOut,
                                       uint64_t sessionId);

  private:
    static constexpr const char *ModuleName = "UserMgr";
    static constexpr const char *PathPrefix = "/usr/";
    static constexpr const unsigned SlotsSessionsHt = 13;
    static constexpr const unsigned SlotsUsrsHt = 61;

    static UserMgr *instance;

    enum class SessOp : uint32_t {
        Activate = 0xd001d001,
        Delete,
        Inactivate,
        Persist,
        Rename,
        Upgrade,   // Used by upgrade tool (upgrade legacy to current version)
        ReadState  // read session state (e.g. getDag(), or getKvStoreId())
                   // Currently, concurrent ReadState ops aren't allowed - so
                   // all these ops are mutually exclusive. The OpState must be
                   // Idle for any of these ops to be able to choose a session
                   // to operate on for the op.
    };

    class UsrNsObject : public NsObject
    {
      public:
        UsrNsObject(size_t objSize, NodeId nodeId)
            : NsObject(objSize), nodeId_(nodeId)
        {
        }
        ~UsrNsObject() {}

        NodeId getNodeId() { return nodeId_; }

      private:
        // Node to which this user is tied to, i.e. where the user is logged
        // in. Note that this is transient.
        NodeId nodeId_;

        UsrNsObject(const UsrNsObject &) = delete;
        UsrNsObject &operator=(const UsrNsObject &) = delete;
    };

    class UsrPerSessInfo
    {
      public:
        StringHashTableHook hook_;
        SessionMgr::Session *session_ = NULL;

        const char *getName() const { return session_->getName(); };

        UsrPerSessInfo() {}
        ~UsrPerSessInfo() {}

        UsrPerSessInfo(const UsrPerSessInfo &) = delete;
        UsrPerSessInfo &operator=(const UsrPerSessInfo &) = delete;
    };

    struct UsrPerSessInfoListElt {
        UsrPerSessInfo *usrPerSessInfo;
        UsrPerSessInfoListElt *next;
    };

    typedef StringHashTable<UsrPerSessInfo,
                            &UsrPerSessInfo::hook_,
                            &UsrPerSessInfo::getName,
                            SlotsSessionsHt>
        SessionsHt;

    class PerUsrInfo
    {
      public:
        char usrName_[LOGIN_NAME_MAX + 1];

        // Organize all the sessions that belong to this user in the session
        // name HT.
        Mutex sessionHtLock_;
        StringHashTableHook hook_;
        SessionsHt sessionsHt_;

        // Manage state transition. We need this to,
        // * Avoid coarse grained.
        // * Avoid issuing 2PCs with locks held.
        // Protected by usrHTLock_
        enum class UsrState : uint32_t {
            Initializing = 0xc001c001,  // tracked by UsrsHt
            PublishedToNs,              // tracked by UsrsHt
            UnpublishedFromNs           // should be removed from UsrsHt
        };

        // State of all sessions: mainly to track when all sessions have been
        // loaded from disk and entered into the sessionsHt_ hash table. This is
        // also needed to avoid holding the sessionHtLock_ spinlock across disk
        // I/O to load / write sessions durable data.
        // Protected by sessionHtLock_

        enum class SessState : uint32_t {
            Init = 0xc002c002,
            Loading,
            LoadedPartial,  // some sessions failed to load
            Loaded,
            Failed,
        };

        Atomic32 usrState_;
        Atomic32 sessState_;

        // Using CondVar to serialize publishing to LibNs which involves a
        // 2PC. We want to avoid pointless 2PCs from local node for the same
        // object in the NS.
        CondVar publishCondVar_;      // assoicated lock: usrHTLock_
        CondVar usrSessLoadCondVar_;  // associated lock: sessionHtLock_

        // condVar used to wait for session: either to be found or to be idle
        CondVar usrSessBusyCondVar_;  // associated lock: sessionHtLock_

        // Use refcounting to again manage the life cycle of this instance
        // memory.
        Atomic32 ref_;

        // Update a session's state to be Idle, waking up waiters, if any
        void setSessIdle(SessionMgr::Session *thisSession)
        {
            sessionHtLock_.lock();
            thisSession->setOpState(SessionMgr::Session::OpState::Idle);
            usrSessBusyCondVar_.broadcast();
            sessionHtLock_.unlock();
        }

        MustCheck bool validateUsrState(UsrState curState)
        {
            return curState == UsrState::Initializing ||
                   curState == UsrState::PublishedToNs ||
                   curState == UsrState::UnpublishedFromNs;
        }

        MustCheck UsrState getUsrState()
        {
            return (UsrState) atomicRead32(&usrState_);
        }

        void setUsrState(UsrState newState)
        {
            assert((newState == UsrState::Initializing) ||
                   validateUsrState(getUsrState()));
            assert(validateUsrState(newState));
            atomicWrite32((Atomic32 *) &usrState_, (int32_t) newState);
        }

        MustCheck bool validateSessState(SessState curState)
        {
            return curState == SessState::Init ||
                   curState == SessState::Loading ||
                   curState == SessState::Loaded ||
                   curState == SessState::LoadedPartial ||
                   curState == SessState::Failed;
        }

        MustCheck SessState getSessState()
        {
            return (SessState) atomicRead32(&sessState_);
        }

        void setSessState(SessState newState)
        {
            assert((newState == SessState::Init) ||
                   validateSessState(getSessState()));
            assert(validateSessState(newState));
            atomicWrite32((Atomic32 *) &sessState_, (int32_t) newState);
        }

        void incRef()
        {
            assert(atomicRead32(&ref_) > 0);
            atomicInc32(&ref_);
        }

        void decRef()
        {
            assert(atomicRead32(&ref_) > 0);
            if (atomicDec32(&ref_) == 0) {
                delete this;
            }
        }

        const char *getName() const { return (const char *) usrName_; }

        PerUsrInfo(char *usrName)
        {
            atomicWrite32((Atomic32 *) &ref_, 1);
            setUsrState(UsrState::Initializing);
            setSessState(SessState::Init);
            // XXX It's best to use an explicit init method, if constructors
            // can fail. Or if this is an impossible case, just add an assert.
            Status status = strStrlcpy(usrName_, usrName, sizeof(usrName_));
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogWarn,
                        "Warning! %s was truncated to %s",
                        usrName,
                        usrName_);
            }
        }

        ~PerUsrInfo() { assert(atomicRead32((Atomic32 *) &ref_) == 0); }

        PerUsrInfo(const PerUsrInfo &) = delete;
        PerUsrInfo &operator=(const PerUsrInfo &) = delete;
    };

    typedef StringHashTable<PerUsrInfo,
                            &PerUsrInfo::hook_,
                            &PerUsrInfo::getName,
                            SlotsUsrsHt>
        UsrsHt;
    UsrsHt usrsHt_;
    Mutex usrHtLock_;

    enum GetMode {
        Get,
        GetAlloc,
    };

    static constexpr size_t StatCount = 16;
    StatGroupId usrStatGroupId_;
    struct {
        StatHandle createdCount;
        StatHandle createdFailures;
        StatHandle deletedCount;
        StatHandle deletedFailures;
        StatHandle inactedCount;
        StatHandle inactedFailures;
        StatHandle actedCount;
        StatHandle actedFailures;
        StatHandle persistCount;
        StatHandle persistFailures;
        StatHandle renameCount;
        StatHandle renameFailures;
        StatHandle downloadCount;
        StatHandle downloadFailures;
        StatHandle uploadCount;
        StatHandle uploadFailures;
    } stats;

    void abortLoadSessions(SessionListElt *sessList,
                           UsrPerSessInfoListElt *usrPerSessList,
                           PerUsrInfo *perUsrInfo);

    MustCheck Status validateUsrAndLoadSessions(XcalarApiUserId *user,
                                                PerUsrInfo **perUsrInfoRet,
                                                NodeId *ownerNodeId);

    MustCheck Status destroyUsrAndUnloadSessions(XcalarApiUserId *user);

    MustCheck Status openWorkBookKvStore(UsrPerSessInfoListElt *usrPerSessList);

    bool chooseSess(SessOp sessOp, SessionMgr::Session *thisSession);

    UsrPerSessInfo *findSess(PerUsrInfo *perUsrInfo, char *name);

    MustCheck Status addSessToList(SessionMgr::Session *thisSession,
                                   SessionListElt **sessListp,
                                   int *sCount,
                                   SessOp sessOp);

    MustCheck Status getSessList(XcalarApiUserId *user,
                                 XcalarApiSessionDeleteInput *sessionInput,
                                 SessOp sessOp,
                                 PerUsrInfo **perUsrInfoRet,
                                 int *sessCountRet,
                                 SessionListElt **sessListRet);

    MustCheck bool foundSessListBusy(XcalarApiSessionDeleteInput *sessionInput,
                                     PerUsrInfo *perUsrInfo,
                                     UserMgr::SessOp sessOp,
                                     SessionListElt **sessListp,
                                     int *sCount,
                                     Status *statusp);

    MustCheck bool foundSessBusy(XcalarApiSessionInfoInput *sessionInput,
                                 PerUsrInfo *perUsrInfo,
                                 UserMgr::SessOp sessOp,
                                 SessionMgr::Session **thisSessRet,
                                 Status *statusp);

    MustCheck Status getSess(XcalarApiUserId *user,
                             XcalarApiSessionInfoInput *sessionInput,
                             SessOp sessOp,
                             SessionMgr::Session **thisSessRet,
                             PerUsrInfo **perUsrInfoRet);

    // getUsrInfo is a wrapper (for convenience), over getOrAllocUsrInfo,
    // using "Get" mode.
    MustCheck PerUsrInfo *getUsrInfo(char *usrName,
                                     size_t usrNameLen,
                                     Status *retStatus,
                                     NodeId *retUsrNodeId);

    MustCheck PerUsrInfo *getOrAllocUsrInfo(char *usrName,
                                            size_t usrNameLen,
                                            Status *retStatus,
                                            NodeId *retUsrNodeId,
                                            GetMode getMode);

    MustCheck Status
    updateUsrOwnerNodeInfo(char *userName,
                           NodeId ownerNodeId,
                           XcalarApiSessionGenericOutput *sessionGenericOutput);

    // common routine between ::create and ::upload
    // for ::create, sessionUploadPayload is NULL
    // for ::upload, sessionUploadPayload has workbook file to be uploaded
    MustCheck Status createInternal(XcalarApiUserId *userName,
                                    XcalarApiSessionNewInput *sessionInput,
                                    SessionUploadPayload *sessionUploadPayload,
                                    XcalarApiSessionNewOutput *sessionOutput);

    void allocListOutputAndFillInOwnerNodeInfo(XcalarApiUserId *user,
                                               NodeId nodeId,
                                               XcalarApiOutput **outputOut,
                                               size_t *outputSizeOut);

    void destroyUser(PerUsrInfo *perUsrInfo, bool usrPublished);

    UserMgr() {}
    ~UserMgr() {}

    UserMgr(const UserMgr &) = delete;
    UserMgr &operator=(const UserMgr &) = delete;
};

#endif  // _USERS_H_
