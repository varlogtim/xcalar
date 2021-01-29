// Copyright 2013-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SESSIONS_H_
#define _SESSIONS_H_

#include <dirent.h>

#include "kvstore/KvStoreTypes.h"
#include "runtime/Mutex.h"
#include "DurableObject.pb.h"
#include "StrlFunc.h"
#include "util/Archive.h"
#include "table/ResultSet.h"

// Enable to debug session outstanding ops refs.
// #define DEBUG_SESSION_REFS

static constexpr uint64_t SessionMagicV2 = 0xcafe080367a3dfc8ULL;
static constexpr size_t SessionNameLen = 256;
static const char *NonPersistIMDKey = "IMD_ONLY_NP_PREFIX_";
static const char *NonPersistIMDKeyALL = "IMD_ONLY_NP_PREFIX_.*";
struct SessionListElt;
struct SessionUploadPayload;

// Hash table to keep track of KVS keys to be cleaned up on failure
struct KeyNameHtEntry {
    StringHashTableHook hook;
    char keyName[XcalarApiMaxKeyLen + 1];
    const char *getKeyName() const { return keyName; };
    void del();
};
typedef StringHashTable<KeyNameHtEntry,
                        &KeyNameHtEntry::hook,
                        &KeyNameHtEntry::getKeyName,
                        7,  // slots
                        hashStringFast>
    KeyNameHashTable;

class SessionMgr final
{
  public:
    static constexpr unsigned int InvalidUserCookie = 0;
    // DurableSessionV2 is present just to support deserialization from
    // pre-Dionysus session files during upgrade to Dionysus. It will be
    // removed in Eos (assuming upgrade from pre-Dionysus to Eos isn't
    // supported).
    struct DurableSessionV2 {
        uint64_t magic = SessionMagicV2;
        uint64_t sessionId = -1;
        struct timespec sessionCreateTime;
        struct timespec sessionLastUsedTime;
        struct timespec sessionLastChangedTime;
        // Durable Session Name. The session name scope is per User.
        char sessionName[XcalarApiSessionNameLen + 1];
        char sessionOwningUserId[LOGIN_NAME_MAX + 1];
    };
    // All sessions Dionysus onwards use the following structure (to which
    // the above structure is copied during upgrade).
    //

    struct SessionPersistedDataV2 {
        // *********************************************************************
        // NOTE * NOTE * NOTE * NOTE * NOTE * NOTE
        //
        // If you make any changes to this structure, the workbook version
        // (see enum WorkbookVersions) must be bumped up and upgrade code added
        // if needed, in:
        // SessionMgr::Session::deserialize
        //     - at workbook version check - for existing sessions
        // SessionMgr::Session::serialize
        //     - at workbook version check - for workbook upload
        //
        // NOTE * NOTE * NOTE * NOTE * NOTE * NOTE
        // *********************************************************************
        uint64_t magic = SessionMagicV2;
        uint64_t sessionId = -1;
        struct timespec sessionCreateTime;
        struct timespec sessionLastUsedTime;
        struct timespec sessionLastChangedTime;
        char sessionName[XcalarApiSessionNameLen + 1];
        char sessionOwningUserId[LOGIN_NAME_MAX + 1];
    };
    SessionMgr()
    {
        // If the following assert fails, a change was made to
        // SessionPersistedDataV2 above which changed its size - since this
        // structure is persisted, the WorkbookVersions enum must be bumped up,
        // and upgrade code from old persisted layout to the new modified
        // layout may be needed.
        //
        // Also, even if the size didn't change, but the structure was modified
        // (e.g. a field was deleted and a new field added) or the semantics of
        // a field changed), the same actions performed as stated in the big
        // block comment in the struct above.

        assertStatic((sizeof(SessionPersistedDataV2) == 584) &&
                     "Change to struct size needs version bump, upgrade!!");
    }
    // Bytes to represent ASCII Max 64-bit number, i.e.
    // "FFFFFFFFFFFFFFFF" + '\0' appended.
    static constexpr const size_t MaxTextHexIdSize = 17;
    static constexpr unsigned MaxDataflowPathLen =
        XcalarApiMaxPathLen + 1 + LOGIN_NAME_MAX + 1 + XcalarApiSessionNameLen +
        1 + sizeof(LogLib::DataflowWkBookDirName);

    class Session
    {
      public:
        // session creation state
        enum class CrState : uint32_t {
            Creating = 0xc003c003,  // in process of being created
            Created,                // session fully created
            Failed,     // failed: reason logged in sessFailedStatus_
            DelInProg,  // session delete in progress
        };

        // state of operations on a session
        enum class OpState : uint32_t {
            Idle = 0xc004c004,  // idle - no operation in progress
            Activating,         // activation in progress
            Inactivating,       // inactivation in progress
            Persisting,         // persist in progress
            Renaming,           // rename in progress
            Upgrading,          // upgrade in progress
            Reading             // reading session state (e.g. getDag)
        };

        enum class LogPrefix { Unknown, Session, Dag };

        // used by shutdownSessUtil to return info to UserMgr
        enum class ShutDown { Active, Inactive, InactiveWithDagClean };

        enum WorkbookVersions : unsigned {
            WorkbookVersion1 = 1,
            // V2 - Added workbook checksum
            WorkbookVersion2 = 2,
            // V3 - Removed persisted query graph
            WorkbookVersion3 = 3,
            // V4 - Moved session metadata to KVS
            // but retained a versioned sessionInfo section in workbook
            WorkbookVersion4 = 4
        };
        static constexpr WorkbookVersions WorkbookVersionCur = WorkbookVersion4;

        MustCheck Status create(XcalarApiUserId *sessionUser,
                                XcalarApiSessionNewInput *sessionInput,
                                SessionUploadPayload *sessionUploadPayload,
                                XcalarApiSessionNewOutput *sessionOutput,
                                SessionMgr::Session *forkedSession);
        MustCheck Status activate(XcalarApiUserId *sessionUser,
                                  XcalarApiSessionGenericOutput *sessionOutput);
        MustCheck Status updateName(XcalarApiUserId *sessionUser,
                                    XcalarApiSessionRenameInput *sessionInput);
        MustCheck Status persist(XcalarApiUserId *sessionUser,
                                 XcalarApiOutput **outputOut,
                                 size_t *sessionListOutputSize);
        MustCheck Status
        download(XcalarApiUserId *sessionUser,
                 XcalarApiSessionDownloadInput *sessionDownloadInput,
                 XcalarApiOutput **outputOut,
                 size_t *outputSizeOut);
        MustCheck Status upload(XcalarApiUserId *sessionUser,
                                SessionUploadPayload *sessionUploadPayload);
        MustCheck Status getDag(Dag **sessionGraphHandleOut);

        // Called during UserMgr::destroy to close out all users' sessions'
        // log files
        void destroySessUtil();
        void shutdownSessUtil(char *userName, ShutDown *sdType);

        // Called during UserMgr::validateUsrAndLoadSessions to open up kvstore
        // for each workbook, as soon as it's loaded successfully

        MustCheck Status sessionKvsCreateOrOpen();
        MustCheck Status addVersionKeyToKvs(char *wbVerKeyName);
        MustCheck Status validateKvsVerAndUpgrade(char *wbVerKeyValue);

        static MustCheck Status addKeyNameForCleanup(
            const char *keyName, SessionMgr::Session *targetSess);

        static void doKeyCleanup(Status status,
                                 SessionMgr::Session *targetSess);

        // Prepare the query string OR Dag for upgrade:
        //
        // The caller supplies either a queryStr or a Dag (not both).
        // A queryStr is supplied for workbook upload/upgrade.
        // A Dag is supplied for stand-alone tool's workbook upgrade (which
        // must not call reverse-parse to generate the json, since embedded
        // exec-retina nodes in a Dag are expanded in reverse-parse, losing
        // the UDFs in the process).
        //
        // The Dag (supplied OR parsed from the queryStr) is fixed as:
        //
        // 0. fix it for any dataset names if needed (for workbook queries)
        // 1. extract any UDFs from embedded exec-retina nodes in the Dag,
        //    prefix them with the retina-name and add them to the workbook
        //    (expServer will do the same for UDF references in the query).
        //    This is only for workbook queries (since retina queries don't
        //    have embedded exec-retina nodes)
        // And then the fixed/processed Dag is reverse-parsed into JSON, with
        // an appropriate header added to the query using version, etc. This
        // reverseParse will also expand the query inside the compressed tarball
        // in the embedded exec-retina nodes into JSON in the retinaBuf.
        //
        // Return the final query in 'hdrWqueryOut' which can be sent to
        // expserver for upgrade
        //
        // NOTE:
        // For workbooks, wbVersion is non-zero but retQueryJsonObj is.
        // For retina queries, wbVersion is 0, but retQueryJsonObj isn't.
        //
        MustCheck Status prepQueryForUpgrade(char *queryStr,
                                             Dag *qGraph,
                                             int wbVersion,
                                             json_t *retQueryJsonObj,
                                             char **hdrWqueryOut);

        // Core utility function to convert a legacy pre-DF2.0 query string in
        // "queryStr" into a series of key-value pairs representing DF2.0
        // dataflows which are stored in the workbook represented by
        // "targetSess"
        MustCheck Status queryToDF2Upgrade(char *queryStr);

        // Legacy pre-DF2 retina is upgraded to DF2 using following routine.
        // Payload is assumed to be the retina's ".tar.gz" file
        MustCheck Status tryUpgradeRetinaToDF2(void *buf, size_t bufSize);

        // XXX: Move to instance oriented i/f rather than taking a sessList
        // Iteration can be in UserMgr, with invocation via session instance
        // Do this for both bulk inactivate and bulk delete
        static MustCheck Status inactivate(XcalarApiUserId *sessionUser,
                                           SessionListElt *sessList,
                                           int sessCount);

        // Inactivation only used by workbook download.  The difference from a
        // "normal" inactivate is that the workbook is not cleaned (previously
        // known as paused).
        static MustCheck Status inactivateNoClean(XcalarApiUserId *sessionUser,
                                                  SessionListElt *sessList,
                                                  int sessCount);

        static MustCheck Status doDelete(XcalarApiUserId *sessionUser,
                                         SessionListElt *sessList,
                                         int sessCount);

        static MustCheck Status list(XcalarApiUserId *sessionUser,
                                     SessionListElt *sessList,
                                     const char *sessionListPattern,
                                     XcalarApiOutput **outputOut,
                                     size_t *sessionListOutputSize);

        // readSessionsProtobuf used only when upgrading to Dionysus to
        // deserialize pre-Dionysus sessions metadata in protobuf/liblog
        static MustCheck Status
        readSessionsProtobuf(char *userName, SessionListElt **sessListRet);

        // readSessionsKvs used to deserialize sessions metadata from KVS where
        // it's stored from Dionysus onwards
        static MustCheck Status readSessionsKvs(char *userName,
                                                SessionListElt **sessListRet);
        // readSessions is the generic routine which calls readSessionsProtobuf
        // or readSessionsKvs depending on whether it's upgrade or not
        static MustCheck Status readSessions(char *userName,
                                             SessionListElt **sessListRet);

        MustCheck bool validateCrState(CrState curState)
        {
            return curState == CrState::Creating ||
                   curState == CrState::Created ||
                   curState == CrState::Failed ||
                   curState == CrState::DelInProg;
        }
        MustCheck CrState getCrState()
        {
            return (CrState) atomicRead32(&sessCrState_);
        }
        void setCrState(CrState newState)
        {
            assert((newState == CrState::Creating) ||
                   validateCrState(getCrState()));
            assert(validateCrState(newState));
            atomicWrite32((Atomic32 *) &sessCrState_, (int32_t) newState);
        }
        void setCrFailedStatus(Status status) { sessFailedStatus_ = status; }
        MustCheck bool validateOpState(OpState curState)
        {
            return curState == OpState::Idle ||
                   curState == OpState::Activating ||
                   curState == OpState::Inactivating ||
                   curState == OpState::Persisting ||
                   curState == OpState::Upgrading ||
                   curState == OpState::Renaming ||
                   curState == OpState::Reading;
        }
        MustCheck OpState getOpState()
        {
            return (OpState) atomicRead32(&sessOpState_);
        }
        void setOpState(OpState newState)
        {
            assert((newState == OpState::Idle) ||
                   validateOpState(getOpState()));
            assert(validateOpState(newState));
            atomicWrite32((Atomic32 *) &sessOpState_, (int32_t) newState);
        }

        // XXX: Check this / it may not be needed given states above
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

        uint64_t incOutstandOps();

        uint64_t decOutstandOps();

        uint64_t getOutstandOps() { return atomicRead64(&outstandingOps_); }

        Session(char *sessName)
        {
            atomicWrite32((Atomic32 *) &ref_, 1);
            atomicWrite64((Atomic64 *) &outstandingOps_, 0);
            setName(sessName);
            setCrState(CrState::Creating);
            setOpState(OpState::Idle);
        }

        ~Session()
        {
            assert(atomicRead64((Atomic64 *) &outstandingOps_) == 0);
            assert(atomicRead32((Atomic32 *) &ref_) == 0);
            if (udfContainer_ != NULL) {
                memFree(udfContainer_);
                udfContainer_ = NULL;
            }
            tableRsIdHt_.removeAll(&TableRsIdTrack::destroy);
        }

        MustCheck char *getName() const
        {
            return (char *) sessionPersistedData_.sessionName;
        }
        MustCheck uint64_t getSessId() const
        {
            return (uint64_t) sessionPersistedData_.sessionId;
        }
        MustCheck KvStoreId getKvStoreId()
        {
            return (KvStoreId) sessionKvStoreId_;
        }
        MustCheck bool isActive() { return sessionActive_; }
        MustCheck bool isPersisted() { return sessionDag_ == NULL; }

        MustCheck Status getResultsetIdsByTableId(TableNsMgr::TableId tableId,
                                                  ResultSetId **rsIdsOut,
                                                  unsigned *numRsIdsOut);

        MustCheck Status trackResultset(ResultSetId rsId,
                                        TableNsMgr::TableId tableId);

        void untrackResultset(ResultSetId rsId, TableNsMgr::TableId tableId);

        void untrackResultsets(TableNsMgr::TableId tableId);

        void untrackResultsets();

      private:
        enum class DagReadType { DagRead, DagUpgrade };
        // This version is used only for upgrade to DF2 - not useful beyond
        // that. See notes in the code using the version.
        enum DataflowVersions : unsigned {
            DataflowVersion1 = 1  // pre-DF2 version
        };
        // XXX: delete following field in Eos
        DurableSessionV2 sessionDurableData_;          // pre-Dionysus
        SessionPersistedDataV2 sessionPersistedData_;  // Dionysus onwards

        // Keys and packing format used to store sessions metadata in KVS
        static constexpr const char *WorkbookVersionInfoKey = "versionInfo";
        static constexpr const char *XcVersionKey = "xcalarVersion";
        static constexpr const char *WkbookVersionKey = "workbookVersion";
        static constexpr const char *MagicKey = "magic";
        static constexpr const char *SessionIdKey = "sessionId";
        static constexpr const char *SessionUserKey = "sessionOwningUserId";
        static constexpr const char *SessionCrTimeKey = "sessionCreateTime";
        static constexpr const char *SessionModTimeKey =
            "sessionLastChangedTime";
        static constexpr const char *SessionRefTimeKey = "sessionLastUsedTime";
        static constexpr const char *SessionNameKey = "sessionName";
        static constexpr const char *TvSecKey = "tvSec";
        static constexpr const char *TvNsecKey = "tvNsec";

        const char *JsonPackFormatString =
            "{"
            "s:s"  // Xcalar version
            "s:s"  // workbook version
            "s:s"  // magic
            "s:s"  // sessionId
            "s:s"  // sessionOwningUserId
            "s:o"  // sessionCreateTime
            "s:o"  // sessionLastChangedTime
            "s:o"  // sessionLastUsedTime
            "s:s"  // sessionName
            "}";

        unsigned int sessionUserIdUnique_ = 0;
        Dag *sessionDag_ = NULL;
        XcalarApiUdfContainer *udfContainer_ = NULL;
        LogLib::Handle *sessionLogHandle_ = NULL;
        char *sessionLogFilePrefix_ = NULL;
        Xid sessionKvStoreId_ = 0;
        bool sessionActive_ = false;
        KeyNameHashTable keyTrackHt_;  // track kvs keys for cleanup @ failure
        Atomic64 outstandingOps_;
        Atomic32 ref_;
        Atomic32 sessCrState_;
        Atomic32 sessOpState_;
        // if sessCrState_ == Failed, the following records why
        Status sessFailedStatus_ = StatusOk;

        void setName(char *sessName);
        struct RsIdTrack {
            ResultSetId rsId = XidInvalid;
            IntHashTableHook intHook;
            RsIdTrack(ResultSetId rsIdIn) : rsId(rsIdIn) {}

            ResultSetId getRsId() const { return rsId; }

            void destroy()
            {
                ResultSetMgr::get()->freeResultSet(rsId, false);
                delete this;
            }
        };

        static constexpr unsigned RsIdHtSlots = 5;
        typedef IntHashTable<ResultSetId,
                             RsIdTrack,
                             &RsIdTrack::intHook,
                             &RsIdTrack::getRsId,
                             RsIdHtSlots,
                             hashIdentity>
            RsIdHashTable;

        struct TableRsIdTrack {
            TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
            IntHashTableHook intHook;
            RsIdHashTable rsIdHt_;

            TableRsIdTrack(TableNsMgr::TableId tableIdIn) : tableId(tableIdIn)
            {
            }

            TableNsMgr::TableId getTableId() const { return tableId; }

            void destroy()
            {
                rsIdHt_.removeAll(&RsIdTrack::destroy);
                delete this;
            }
        };

        static constexpr unsigned TableRsIdHtSlots = 11;
        typedef IntHashTable<TableNsMgr::TableId,
                             TableRsIdTrack,
                             &TableRsIdTrack::intHook,
                             &TableRsIdTrack::getTableId,
                             TableRsIdHtSlots,
                             hashIdentity>
            TableRsIdHashTable;
        TableRsIdHashTable tableRsIdHt_;
        Mutex tableRsLock_;

        void setName(const char *sessName);
        MustCheck Status writeSession(bool newSession);
        void cleanQgraph();

        MustCheck Status deserialize(size_t userLen,
                                     char *gKvsKey,
                                     char **jsonStrOut);
        MustCheck Status serialize(char *uploadStr);
        MustCheck Status initUdfContainer();
        MustCheck Status updateWbHealth(json_t *archiveHeader,
                                        const char *hkey,
                                        const char *hvalue);
        void updateNameUtil(char *sesionName);
        MustCheck Status isErrorFixable(const Status &status,
                                        Status *badStatus);
        MustCheck Status ignoreDownloadError(const Status &status,
                                             json_t *archiveHeader,
                                             json_t *missingFilesArray,
                                             const char *missingFile,
                                             bool *badWorkbook);
        static MustCheck Status inactivateInternal(XcalarApiUserId *sessionUser,
                                                   bool cleanup,
                                                   SessionListElt *sessList,
                                                   int sessCount);

        // Workbook related functions
        MustCheck Status addSessionKvStoreToManifest(
            json_t *archiveChecksum, ArchiveManifest *archiveManifest);
        MustCheck Status getSessionKvStoreFromManifest(
            ArchiveManifest *archiveManifest, const json_t *archiveChecksum);
        MustCheck Status addSessionToManifest(json_t *archiveChecksum,
                                              ArchiveManifest *archiveManifest,
                                              char *userName);
        MustCheck Status getSessionFromManifest(
            ArchiveManifest *archiveManifest, const json_t *archiveChecksum);
        MustCheck Status addQGraphToManifest(json_t *archiveChecksum,
                                             ArchiveManifest *archiveManifest,
                                             char *userName);
        // getQGraphFromManifest returns a string (which has json
        // representation of the query graph pulled from the manifest) in
        // queryRet.
        MustCheck Status getQGraphFromManifest(ArchiveManifest *archiveManifest,
                                               const json_t *archiveChecksum,
                                               char **queryRet);
        MustCheck Status
        addSessionUDFsToManifest(json_t *archiveHeader,
                                 json_t *archiveChecksum,
                                 ArchiveManifest *archiveManifest);
        MustCheck Status
        getSessionUDFsFromManifest(json_t *archiveHeader,
                                   ArchiveManifest *archiveManifest,
                                   const json_t *archiveChecksum);
        MustCheck Status initArchiveHeader(json_t *archiveHeader);

        // Init the header for the upgrade payload to be sent to the expserver
        // for upgrade. The header contains context information for the payload
        // so the upgrade can be done correctly for the context (e.g. whether
        // the payload represents a workbook or batch dataflow).
        MustCheck Status initUpgrHeader(json_t *upgrHeader,
                                        int workbookVersion);

        MustCheck Status
        addArchiveHeaderToManifest(json_t *archiveHeader,
                                   json_t *archiveChecksum,
                                   ArchiveManifest *archiveManifest,
                                   size_t *archiveHeaderSizeOut);
        MustCheck Status removeArchiveHeaderFromManifest(
            size_t archiveHeaderSize, ArchiveManifest *archiveManifest);
        MustCheck Status addArchiveChecksumToManifest(
            json_t *archiveChecksum, ArchiveManifest *archiveManifest);
        MustCheck Status getArchiveChecksumFromManifest(
            json_t **archiveChecksum, ArchiveManifest *archiveManifest);
        MustCheck Status
        getArchiveHeaderFromManifest(json_t **archiveHeader,
                                     ArchiveManifest *archiveManifest,
                                     const json_t *archiveChecksum);
        MustCheck Status
        addSessionJupyterNBsToManifest(json_t *archiveChecksum,
                                       const char *pathToAdditionalFiles,
                                       ArchiveManifest *archiveManifest);
        MustCheck Status
        getSessionJupyterNBsFromManifest(const char *pathToAdditionalFiles,
                                         ArchiveManifest *archiveManifest,
                                         const json_t *archiveChecksum);
        MustCheck Status
        deleteWorkbookUdfs(XcalarApiUdfContainer *udfContainer);

        // Core upgrade function to upgrade a workbook at version "version",
        // to the current version, if needed. Called during workbook upload,
        // to upgrade a workbook on-the-fly during workbook upload
        MustCheck Status wbUploadUpgrade(unsigned version,
                                         ArchiveManifest *archiveManifest,
                                         const json_t *archiveChecksum);

        // This is a service function for prepQueryForUpgrade. See block
        // comment for latter function in the header file, for a full
        // description. Essentially, prepQueryForUpgrade() uses
        // fixQueryGraphForUpgrade() to do the enumerated fixup of the Dag (
        // either supplied or generated from queryStr), and reverse-parsing
        // the fixed Dag into JSON, finally adding the header to this JSON.
        //
        // If a workbook is being upgraded, the "isWorkbook" param must be
        // set to true.
        //
        // Caller is responsible for freeing memory in first param and
        // dec-ref'ing the returned json object.
        //
        // The username which appears in dataset names in the query graph, must
        // be fixed to reflect the username of the user doing the upload - this
        // is for workbook upgrade only (not strictly necessary). And any UDFs
        // in embedded exec-retina nodes in the Dag are extracted, prefixed with
        // the retina name and added to the workbook.
        //

        MustCheck Status fixQueryGraphForUpgrade(char *queryStr,
                                                 Dag *queryGraph,
                                                 bool isWorkbook,
                                                 json_t **fixedQueryJsonOut);

        // Generate the name of global KVS key for a session given user, sessId
        MustCheck Status sessionMetaKvsName(const char *userName,
                                            uint64_t sessionId,
                                            char *sessMetaKvsName,
                                            size_t maxSessMetaKvsName);

#ifdef DEBUG_SESSION_REFS
        void printBtsForOutstandOps(const char *tag);
#endif  // DEBUG_SESSION_REFS

        // XXX Deleting the non-persisted IMD related keys here, this is a
        // workaround for non-persisted keys to get deleted upon inactivation
        // will be handled properly in this
        // https://xcalar.atlassian.net/browse/XD-1986
        static MustCheck Status deleteNPPrefixedImdKeys(KvStoreId id);

        Session(const Session &) = delete;
        Session &operator=(const Session &) = delete;
    };

    enum class WorkbookDirType {
        Udfs,
        Dataflows,
    };
    MustCheck Status processWorkbookDirTree(WorkbookDirType dirType,
                                            DIR *dirIter,
                                            char *dirPath,
                                            size_t maxDirNameLen,
                                            XcalarApiUdfContainer *udfContainer,
                                            bool upgradeToDionysus,
                                            int *treeDepth);

    DIR *getNextDirAndName(DIR *dirIter,
                           char *dirPath,
                           char *nextDirName,
                           size_t maxDirNameLen);

    static MustCheck Status init();
    void destroy();
    static MustCheck SessionMgr *get();

    // Following are public APIs for sessionMgr, no need to be in Session class
    // XXX Exposed for test code
    static void parseSessionIdFromPrefix(const char *sessionLogPrefix,
                                         uint64_t *sessionId);

    MustCheck bool checkIfUpgradeNeeded();

  private:
    // Following fields should be used with direct prefix as globals,
    // by nested Session class

    static constexpr size_t MaxSessionKvStoreNameLen = 255;

    // The default log file size is some number of megabytes (it's variously
    // been 50MB, 4MB, 16MB).  This is far larger than we need for the saved
    // session structs, so we set a smaller default for the session log
    // files.  The query graph log files stay with the default because a
    // query graph can grow large.
    static constexpr uint64_t SessionLogFileSize = 256 * KB;
    static constexpr const uint64_t NumSlotsForCreateNewDag = 127;
    static constexpr const char *SesPrefix = "Xcalar.session.";
    static constexpr const char *DagPrefix = "Xcalar.qgraph.";
    static constexpr const char *MetaSuffix = "-meta";

    // Sections of the workbook file
    static constexpr const char *WorkbookDirName = "workbook/";
    static constexpr const char *UdfWkbkDirName = "udfs/";
    // XXX: Should we add a dataflows/ section? In the future, dataflows can
    // be imported into a workbook - and udfs for these dataflows will be in
    // a sub-dir under workbook - so:
    //         workbook/dataflows/dataflowInfo.json
    //         workbook/dataflows/udfs/*.py
    //
    // So, the layout would be like:
    //
    //         workbook/qgraphInfo.json
    //         workbook/udfs/*.py
    //         workbook/dataflows/dataflowInfo.json (future)
    //         workbook/dataflows/udfs/*.py (future)
    //
    // Note that qgraphInfo.json is the workbook's query graph portion dumped
    // into json. The dataflows/dataflowInfo.json has more information (see
    // exportRetinaInt()) - it add a tables[] array to the dataflowInfo.json).
    // So naming it differently reduces confusion.
    //
    // static constexpr const char *DataFlowDirName = "dataflows/";
    static constexpr const char *WorkbookHeaderName = "workbookHeader.json";
    static constexpr const char *WorkbookChecksumName = "workbookChecksum.json";
    static constexpr const char *SessionFileName = "sessionInfo.json";
    static constexpr const char *QGraphFileName = "qgraphInfo.json";
    static constexpr const char *KvStoreFileName = "kvstoreInfo.json";
    static constexpr const char *JupyterNBDirName = "jupyterNotebooks/";
    static constexpr const char *UDFCommonFileName = "udfs";
    static constexpr const char *JupyterNBCommonFileName = "jupyterNotebooks";
    // key value string pairs for workbook health
    static constexpr const char *workbookHealth = "workBookHealth";
    static constexpr const char *workbookHealthGood = "Good";
    static constexpr const char *workbookHealthBad = "Bad";
    // key for missing files when workbook health is bad
    static constexpr const char *missingFiles = "missingFiles";

    // prefixes for KVS (global or workbook, etc.) - used for DF2.0 upgrade
    // These prefixes are used by expServer which does the upgrade, to demarcate
    // keys meant for global vs. workbook kvs, and those needed for datasets, to
    // XCE, which needs to enter these keys correctly
    //
    static constexpr const char *GlobalKeyPrefix = "/globalKvs/";
    static constexpr const char *GlobalKeyDsPrefix = "/globalKvsDataset/";
    static constexpr const char *WkbookKeyPrefix = "/workbookKvs/";

    // The special key specified below is used by XD to store table metadata.  A
    // workbook being upgraded is expected to have this key (but its absence
    // shouldn't be fatal to upgrade), and its value is sent to expServer during
    // upgrade, so expServer can upgrade this metadata into DF2.0 layout.

    static constexpr const char *TableDescKeyForXD = "gInfo-1";

    // XXX: The following special key is needed to implement a temporary fix for
    // Xc-14132. It is used by XD to store a description string for a workbook
    // in the workbook's KVS. XCE retrieves the description using the same key
    // when sending back information for the workbook in the sessionList output.
    // This is done so that XD doesn't need to lookup this key for each workbook
    // when displaying the list of workbooks along with their information when a
    // user logs in - more importantly, providing this as part of list output is
    // the current design (e.g. workbook name, state, info, etc. are also
    // provided in list output, so it'd make sense for description to also be
    // part of list output). In the future, the description should be stored
    // along with other workbook metadata, in the workbook metadata store (which
    // will move from protobuf/liblog to KVS), and a new API created to set and
    // get a workbook's description. Note that upgrade could be handled lazily -
    // the list code could lookup the key, write it out to the new store, and
    // delete the key, while returning the info in list output.

    static constexpr const char *WorkbookDescrKey = "workBookDesc-1";

    // XXX Math is a bit off here, though would not cause buffer overrun.
    // The fixed overhead of the log file name =
    //   char hex representation of the session ID +
    //   the .*-meta suffix on the file name + null terminator = 24.
    static constexpr size_t LogFileOverhead = 24;
    // The path used to save session data.This is needed to find saved
    // sessions.
    char sessionPath_[XcalarApiMaxPathLen + 1];
    static SessionMgr *instance;

    static constexpr size_t StatCount = 2;
    StatGroupId sessionStatGroupId_;
    struct {
        StatHandle writtenCount;
        StatHandle writtenFailures;
    } stats;

    ~SessionMgr() {}

    SessionMgr(const SessionMgr &) = delete;
    SessionMgr &operator=(const SessionMgr &) = delete;
};

// Used to pass session list between User and Session modules
struct SessionListElt {
    SessionMgr::Session *session;
    struct SessionListElt *next;
};

struct SessionUploadPayload {
    char pathToAdditionalFiles[XcalarApiMaxPathLen + 1];
    ssize_t sessionContentCount;  // size of buffer at sessionContentP
    uint8_t *sessionContentP;
};

#endif  // _SESSIONS_H_
