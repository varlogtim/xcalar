// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _HASHTREE_H_
#define _HASHTREE_H_

#include "primitives/Primitives.h"
#include "xdb/Xdb.h"
#include "table/Table.h"
#include "xdb/LibHashTreeGvm.h"
#include "libapis/LibApisCommon.h"
#include "operators/OperatorsTypes.h"
#include "operators/OperatorsEvalTypes.h"
#include "operators/XcalarEval.h"
#include "runtime/Async.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "kvstore/KvStoreTypes.h"
#include "service/PublishedTableService.h"

// a hash tree is a collection of xdbs stored in a range partitioned array
// each hash tree has a primary key that it is range partitioned on
// this determines an row ordering for all xdbs in this hash tree
class HashTree
{
    friend class InsertWork;
    friend class SelectWork;
    friend class PopulateIndexWork;
    friend class HashTreeMgr;

  public:
    HashTree() = default;
    ~HashTree() = default;

    static constexpr const int64_t InvalidBatchId = -1;

    struct UpdateStats {
        size_t numInserts = 0;
        size_t numUpdates = 0;
        size_t numDeletes = 0;
    };

    struct UpdateMeta {
        UpdateMeta(XcalarApiTableInput *srcTableIn,
                   time_t unixTSIn,
                   int64_t batchIdIn,
                   size_t sizeIn,
                   size_t numRowsIn,
                   UpdateStats *statsIn)
            : srcTable(*srcTableIn),
              unixTS(unixTSIn),
              batchId(batchIdIn),
              size(sizeIn),
              numRows(numRowsIn)
        {
            stats = *statsIn;
        }

        XcalarApiTableInput srcTable;
        time_t unixTS;
        int64_t batchId;
        size_t size;
        size_t numRows;
        UpdateStats stats;

        struct UpdateMeta *next = NULL;
    };

    struct SelectMeta {
        SelectMeta(XcalarApiTableInput *dstTableIn,
                   int64_t minBatchIdIn,
                   int64_t maxBatchIdIn,
                   struct SelectMeta *nextIn)
            : dstTable(*dstTableIn),
              minBatchId(minBatchIdIn),
              maxBatchId(maxBatchIdIn),
              next(nextIn)
        {
        }

        XcalarApiTableInput dstTable;
        int64_t minBatchId;
        int64_t maxBatchId;

        struct SelectMeta *next = NULL;
    };

    struct Index {
      public:
        // breakdown of an IndexKvEntry
        enum IndexEntryIdxs {
            KeyIdx,
            TupleOffsetIdx,
            TupleIdxIdx,
            EndBufOffsetIdx,
            XdbPagePtrIdx,
            LastIdx,  // must be last
        };

        IntHashTableHook hook;

        int keyIdxInHashTree;

        uint64_t getKeyIdx() const { return keyIdxInHashTree; }

        void destroy() { delete this; }

        XdbId xdbId;
        Xdb *indexXdb;
        Stopwatch timer;
        unsigned numBatches = 0;
        XdbPageBatch *batches = NULL;

        Index(Xdb *xdb, int keyIdx)
        {
            indexXdb = xdb;
            keyIdxInHashTree = keyIdx;
            xdbId = XdbMgr::xdbGetXdbId(xdb);
        }

        ~Index()
        {
            XdbMgr::get()->xdbDropLocalInternal(xdbId);

            if (batches) {
                delete[] batches;
            }
        }

        static void saveCursorInIndexKv(TableCursor *cur,
                                        NewKeyValueEntry *indexKv);

        static MustCheck Status
        restoreCursorFromIndexKv(NewKeyValueEntry *indexKv, TableCursor *cur);
    };

    MustCheck Status init(const char *pubTableName,
                          Xid hashTreeId,
                          TableMgr::ColumnInfoTable *columnsIn,
                          unsigned numKeys,
                          DfFieldAttrHeader *keyAttr,
                          XcalarApiTableInput *srcTable);

    // creates all xdbs and sets up column info according to a srcXdb
    MustCheck Status initSchema(const char *pubTableName,
                                TableMgr::ColumnInfoTable *columnsIn);

    // Process an xdb for insertion or update into the hash tree
    MustCheck Status insertXdb(Xdb *srcXdb,
                               XcalarApiTableInput *srcTable,
                               time_t unixTS,
                               size_t size,
                               size_t numRows,
                               int flags,
                               int64_t batchId);

    MustCheck Status select(Xdb *dstXdb,
                            struct LibHashTreeGvm::GvmSelectInput *selectInput);

    MustCheck Status coalesce();

    MustCheck Status addIndex(const char *keyName);
    void removeIndex(const char *keyName);
    MustCheck Index *getIndex(const char *keyName);

    void destroy();

    void computeLocalSize(size_t *sizeOut, size_t *rowsOut);
    void fixupXdbSlotAugs();

    unsigned getNumFields() { return tupMeta_.getNumFields(); }
    uint64_t getNumSlots() { return hashSlots_; }
    Xid getXid() const { return hashTreeId_; }

    XdbMeta *getXdbMeta() { return xdbMeta_; }

    NewTupleMeta *getTupMeta() { return &tupMeta_; }

    int64_t getCurrentBatchId() { return currentBatchId_; }

    int getColumnIdx(const char *columnName)
    {
        for (unsigned ii = 0; ii < tupMeta_.getNumFields(); ii++) {
            if (strcmp(xdbMeta_->kvNamedMeta.valueNames_[ii], columnName) ==
                0) {
                return ii;
            }
        }

        return InvalidIdx;
    }

    uint64_t numUpdates_ = 0;
    UpdateMeta *updateHead_ = NULL;

    uint64_t numSelects_ = 0;
    SelectMeta *selectHead_ = NULL;

    // Total sizes across the cluster, updated during update and coalesce
    bool needsSizeRefresh_ = true;
    size_t sizeTotal_ = 0;
    size_t numRowsTotal_ = 0;

    static constexpr size_t indexHashTableSlots = 7;

    IntHashTable<uint64_t,
                 Index,
                 &Index::hook,
                 &Index::getKeyIdx,
                 indexHashTableSlots,
                 hashIdentity>
        indexHashTable_;
    Mutex indexHashTableLock_;

  private:
    static constexpr int NoFlags = 0;
    static constexpr int AddOpCodeFlag = 1;
    static constexpr int AddRankOverFlag = 1 << 1;
    static constexpr int DropSrcSlotsFlag = 1 << 2;

    // this is the latest batch id in the main xdb
    int64_t mainBatchId_ = 0;
    int64_t currentBatchId_ = -1;

    XcalarApiTableInput sourceTable_;

    uint64_t hashSlots_;
    Xdb *mainXdb_ = NULL;
    Xdb *updateXdb_ = NULL;

    bool rangeInited = false;
    DfFieldValue min_;
    DfFieldValue max_;

    NewTupleMeta tupMeta_;
    XdbMeta *xdbMeta_ = NULL;

    int64_t batchIdx_;
    int opCodeIdx_ = NewTupleMeta::DfInvalidIdx;
    int rankOverIdx_ = NewTupleMeta::DfInvalidIdx;

    unsigned numKeys_ = 0;
    DfFieldAttrHeader *keys_ = NULL;
    Xid hashTreeId_ = XidInvalid;

    TableMgr::ColumnInfoTable columns_;

    XdbMeta *setupXdbMeta();
    Status initXdb(Xdb *xdb, const char *pubTableName);

    void updateSlotXdbMinMax(Xdb *slotXdb, XdbHashSlotAug *srcSlotAug);

    MustCheck Status setupForInsert(Xdb *srcXdb);

    Status processSelectedKv(NewKeyValueEntry *selectKv,
                             NewTupleMeta *selectTupMeta,
                             int64_t minBatchId,
                             int64_t maxBatchId,
                             OpKvEntryCopyMapping *mapping,
                             EvalContext *filterCtx,
                             unsigned numMaps,
                             EvalContext *mapCtxs,
                             XdbPage **coalescedPages,
                             uint64_t *selectedRowCount,
                             OpInsertHandle *insertHandle);

    void cleanOutBatchId(int64_t batchIdToClean);

    // copies over data and history
    MustCheck Status copyHashTreeData(HashTree *hashTreeIn);

    // Index management functions
    MustCheck Status createIndex(const char *keyName, Index **indexOut);
    MustCheck Status populateIndexXdb(Index *index);
    MustCheck Status setupIndexXdbPageBatches(Index *index);

    void freeXdbs();

    void pushUpdateMeta(UpdateMeta *update);
    UpdateMeta *popUpdateMeta();
};

struct HashTreeRefHandle {
    Xid hashTreeId;
    bool active;
    bool restoring;
    char name[XcalarApiMaxTableNameLen + 1];
    LibNsTypes::NsHandle nsHandle;
    int64_t currentBatchId;
    int64_t oldestBatchId;
    XcalarApiUdfContainer sessionContainer;
};

class HashTreeMgr final
{
    friend class LibHashTreeGvm;
    friend class ApiHandlerUpdate;
    friend class ApiHandlerCoalesce;
    friend class TwoPcMsg2pcPubTableDlm1;

  public:
    static constexpr NodeId DlmNodeId = 0;

    static MustCheck HashTreeMgr *get();
    static MustCheck Status init();
    void destroy();
    MustCheck Status publishNamesToNs();
    void unpublishNamesFromNs();
    void cancelRestore(const char *name);

    MustCheck Status initState(Xid hashTreeId, const char *name);

    MustCheck Status createHashTree(XcalarApiTableInput srcTable,
                                    Xid hashTreeId,
                                    const char *name,
                                    time_t unixTS,
                                    bool dropSrc,
                                    size_t size,
                                    size_t numRows,
                                    Dag *dag);

    enum class CreateReason {
        Invalid,
        Publish,
        RestoreFromSnap,
        RestoreFromXcRoot,
    };

    static constexpr const char *PublishDependencyKey = "/sys/imd_dependencies";

    struct RestoreInfo {
        static constexpr int TableSlots = 11;

        struct DependentTable {
            StringHashTableHook stringHook;
            char name[XcalarApiMaxTableNameLen + 1];

            DependentTable(const char *nameIn)
            {
                strlcpy(name, nameIn, sizeof(name));
            }

            const char *getName() const { return name; }

            void del() { delete this; }
        };

        StringHashTableHook stringHook;
        char pubTableName[XcalarApiMaxTableNameLen + 1];
        char snapshotRetinaName[XcalarApiMaxTableNameLen + 1];

        StringHashTable<DependentTable,
                        &DependentTable::stringHook,
                        &DependentTable::getName,
                        TableSlots>
            dependencies;
        Mutex lock;

        RestoreInfo(const char *pubTableNameIn)
        {
            strlcpy(pubTableName, pubTableNameIn, sizeof(pubTableName));
            snapshotRetinaName[0] = '\0';
            atomicWrite32(&cancelIssued, false);
        }
        ~RestoreInfo() { dependencies.removeAll(&DependentTable::del); }

        const char *getName() const { return pubTableName; }

        void destroy() { delete this; }

        bool checkCancelled() { return atomicRead32(&cancelIssued); }

        void markCancelled() { atomicWrite32(&cancelIssued, 1); }

      private:
        Atomic32 cancelIssued;
    };

    MustCheck Status createHashTreeLocal(XcalarApiTableInput srcTable,
                                         Xid hashTreeId,
                                         CreateReason createReason,
                                         const char *pubTableName);

    MustCheck Status changeOwnership(const char *publishedTableName,
                                     const char *userIdName,
                                     const char *sessionName);

    enum class UpdateReason {
        Invalid,
        RegularUpdate,
        RestoreFromSnap,
        RestoreFromXcRoot,
    };

    MustCheck Status updateCommit(unsigned numUpdates,
                                  HashTreeRefHandle **refHandles,
                                  int64_t *commitBatchIds);

    MustCheck Status updateInMemHashTree(XcalarApiTableInput srcTable,
                                         Xid hashTreeId,
                                         const char *name,
                                         time_t unixTS,
                                         bool dropSrc,
                                         size_t size,
                                         size_t numRows,
                                         int64_t newBatchId,
                                         UpdateReason updateReason);

    MustCheck Status
    updateInMemHashTreeLocal(XcalarApiTableInput srcTable,
                             time_t unixTS,
                             bool dropSrc,
                             size_t size,
                             size_t numRows,
                             int64_t newBatchId,
                             Xid hashTreeId,
                             UpdateReason updateReason,
                             HashTree::UpdateStats *updateOutput);

    // reverts hash tree to batchId
    MustCheck Status revertHashTree(int64_t batchId,
                                    Xid hashTreeId,
                                    const char *name,
                                    time_t unixTS);

    void revertHashTreeLocal(int64_t batchId, Xid hashTreeId);

    MustCheck Status selectHashTree(XcalarApiTableInput dstTable,
                                    XcalarApiTableInput joinTable,
                                    Xid hashTreeId,
                                    const char *publishTableName,
                                    int64_t minBatchId,
                                    int64_t maxBatchId,
                                    const char *filterString,
                                    unsigned numColumns,
                                    XcalarApiRenameMap *columns,
                                    size_t evalInputLen,
                                    const char *evalInput,
                                    uint64_t limitRows,
                                    DagTypes::DagId dagId);

    MustCheck Status
    selectHashTreeLocal(struct LibHashTreeGvm::GvmSelectInput *selectInput);

    MustCheck Status coalesceHashTree(HashTreeRefHandle *refHandle,
                                      const char *publishTableName,
                                      Xid hashTreeId);

    MustCheck Status coalesceHashTreeLocal(Xid hashTreeId,
                                           int64_t coalesceBatchId);

    MustCheck Status addIndexToHashTree(const char *publishTableName,
                                        Xid hashTreeId,
                                        const char *keyName);

    MustCheck Status addIndexToHashTreeLocal(Xid hashTreeId,
                                             const char *keyName);

    MustCheck Status removeIndexFromHashTree(const char *publishTableName,
                                             Xid hashTreeId,
                                             const char *keyName);

    void removeIndexFromHashTreeLocal(Xid hashTreeId, const char *keyName);

    MustCheck Status destroyHashTree(const char *name, bool inactivateOnly);

    void destroyHashTreeLocal(Xid hashTreeId, const char *name);

    void inactivateHashTreeLocal(Xid hashTreeId, const char *name);

    MustCheck HashTree *getHashTreeById(Xid hashTreeId);

    enum class OpenRetry { True, False };
    MustCheck Status openHandleToHashTree(const char *name,
                                          LibNsTypes::NsOpenFlags openFlag,
                                          HashTreeRefHandle *handleOut,
                                          OpenRetry retry);

    void closeHandleToHashTree(HashTreeRefHandle *handle,
                               Xid hashTreeId,
                               const char *name);

    // grabs handles on a set of published tables before returning,
    // fails when acquiring any individual handle fails
    MustCheck Status openHandles(const char **publishedTableNames,
                                 unsigned numTables,
                                 LibNsTypes::NsOpenFlags openFlags,
                                 HashTreeRefHandle *refHandles);

    void closeHandles(const char **publishedTableNames,
                      unsigned numHandles,
                      HashTreeRefHandle *refHandles);

    MustCheck Status listHashTrees(
        const char *namePattern,
        uint32_t maxUpdateCount,
        int64_t updateStartBatchId,
        uint32_t maxSelectCount,
        xcalar::compute::localtypes::PublishedTable::ListTablesResponse
            *response);

    // XXX: These are published table specific functions, eventually we'll want
    // to seperate this out into a dedicated Table module

    MustCheck Status restorePublishedTable(const char *publishedTableName,
                                           const XcalarApiUserId *userId,
                                           RestoreInfo **restoreInfoOut,
                                           Dag *sessionDag);

    MustCheck Status persistUpdate(XcalarApiTableInput srcTable,
                                   Dag *dag,
                                   time_t unixTS,
                                   int64_t batchId,
                                   const char *publishedTableName,
                                   Xid hashTreeId);

    uint64_t getTotalUsedBytes();

    MustCheck bool runSnapshotApp(LibNsTypes::NsHandle *retNsHandle);

    void snapshotAppDone(LibNsTypes::NsHandle nsHandle);

  private:
    static constexpr const char *KvStoreKeyPrefix = "/sys/pt";
    static constexpr const char *PublishTableNsPrefix = "/publishTable";
    static constexpr const char *PubTableSnapshotNs = "/pubTableSnapshot";
    static constexpr const char *UpdateFilePrefix = "update_";
    static constexpr const char *TmpUpdateRetinaNamePrefix = "#Xc.tmp.update.";
    static constexpr const char *TmpUpdateRetinaNamePrefixRegex =
        "#Xc.tmp.update.*";
    static constexpr const char *TmpTableName = "Xc.tmp.table";

    // recovery logic constants
    static constexpr const char *SnapshotUserName = "admin";
    static constexpr const char *SnapshotWorkbookName =
        "PublishTableSnapshotWorkbook";
    static constexpr const char *SnapshotRestoreDFSuffix = "SnapshotRestoreDF";
    static constexpr const char *SnapshotResultsSuffix = "SnapshotResults";
    static constexpr const char *SnapshotInfoSuffix = "SnapshotInfo";
    static constexpr const char *LatestSnapshotKey = "latest";
    static constexpr const char *SnapshotNameKey = "snapshotName";
    static constexpr const char *SnapshotRetinaNameKey = "dataflow";
    static constexpr const char *SnapshotBatchIdKey = "batchId";
    static constexpr const char *SnapshotUnixTSKey = "unixTS";
    static constexpr const char *SnapshotRetinaParamsKey = "parameters";

    static constexpr const char *SnapInfoJsonUnpackFormatStr =
        "{"
        "s:s"  // snapshotName
        "s:s"  // dataflow
        "s:I"  // batchId (should be int64_t)
        "s:I"  // unixTS (should be uint64_t)
        "s:o"  // parameters
        "}";

    // Coalesce persistence
    static constexpr const char *CoalesceSuffix = "Coalesce";
    static constexpr const char *CoalesceBatchIdKey = "batchId";
    static constexpr const char *CoalesceJsonPackFormatStr =
        "{"
        "s:I"  // batchId (should be int64_t)
        "}";

#define UpdateFilePattern "update_%ld_%lu.tar.gz"

    // Schema persistence
    static constexpr const char *SchemaSuffix = "Schema";
    static constexpr const char *SchemaValues = "values";
    static constexpr const char *SchemaKeys = "keys";
    static constexpr const char *SchemaColumnName = "columnName";
    static constexpr const char *SchemaColumnType = "columnType";

    bool init_ = false;
    static HashTreeMgr *instance;
    static constexpr uint64_t NumHashSlots = 31;

    enum class ActiveState {
        Invalid,
        Active,
        Inactive,
    };

    enum class RestoreState {
        Invalid,
        RestoreInProgress,
        NoRestoreInProgress,
    };

    struct HashTreeEntry {
        IntHashTableHook intHook;
        StringHashTableHook stringHook;
        HashTree *hashTree = NULL;
        Xid hashTreeId = XidInvalid;
        char pubTableName[XcalarApiMaxTableNameLen + 1];
        bool schemaAvailable = false;
        XcalarApiColumnInfo *values = NULL;
        size_t numValues = 0;
        XcalarApiColumnInfo *keys = NULL;
        size_t numKeys = 0;

        HashTreeEntry(const char *pubTableNameIn,
                      Xid hashTreeIdIn,
                      HashTree *hashTreeIn);
        ~HashTreeEntry();

        Xid getId() const { return hashTreeId; }

        const char *getName() const { return pubTableName; }

        void destroy()
        {
            if (hashTree) {
                hashTree->destroy();
            }
            delete this;
        }
    };

    Mutex lock_;
    typedef IntHashTable<Xid,
                         HashTreeEntry,
                         &HashTreeEntry::intHook,
                         &HashTreeEntry::getId,
                         NumHashSlots,
                         hashIdentity>
        HashTreeIdTable;
    HashTreeIdTable hashTreeIdTable_;

    typedef StringHashTable<HashTreeEntry,
                            &HashTreeEntry::stringHook,
                            &HashTreeEntry::getName,
                            NumHashSlots>
        HashTreeNameTable;
    HashTreeNameTable hashTreeNameTable_;

    typedef StringHashTable<RestoreInfo,
                            &RestoreInfo::stringHook,
                            &RestoreInfo::getName,
                            3>
        RestoreInfoTable;
    RestoreInfoTable restoresInProgress_;

    struct PersistedInfo {
        PersistedInfo(char *nameIn, int64_t totalUpdatesIn)
        {
            strlcpy(name, nameIn, sizeof(name));
            totalUpdates = totalUpdatesIn;
        }

        char name[XcalarApiMaxTableNameLen + 1];
        int64_t totalUpdates;

        StringHashTableHook hook;

        const char *getName() const { return name; }

        void destroy() { delete this; }
    };

    struct RestoreUpdateOutput {
        Status status;
        XcalarApiTableInput srcTable;
        time_t unixTS;
        size_t size;
        size_t numRows;
        Dag *outputDag = NULL;

        int64_t batchId;
    };

    struct SizeAndRows {
        size_t size;
        size_t rows;
    };

    typedef StringHashTable<PersistedInfo,
                            &PersistedInfo::hook,
                            &PersistedInfo::getName,
                            NumHashSlots>
        PersistedInfoHashTable;

    MustCheck Status initInternal();

    MustCheck Status createHashTreeInt(XcalarApiTableInput srcTable,
                                       const char *publishedTableName,
                                       Xid hashTreeId,
                                       CreateReason createReason);

    MustCheck Status getPersistedTables(const char *namePattern,
                                        PersistedInfoHashTable *ht);

    MustCheck Status restoreUpdatesFromDir(const char *dirName,
                                           const char *publishedTableName,
                                           RestoreInfo *restoreInfo,
                                           Xid hashTreeId,
                                           const XcalarApiUserId *userId,
                                           int64_t batchIdStart,
                                           int64_t *restoreBatchId,
                                           Dag *sessionDag);

    // Figures out total size from all nodes and broadcasts the result
    Status refreshTotalSize(Xid hashTreeId);

    void refreshTotalSizeLocal(Xid hashTreeId, size_t size, size_t numRows);

    void getSizeLocal(Xid hashTreeId, void *outPayload, size_t *outputSizeOut);

    MustCheck Status recoverFromSnapshot(const char *publishedTableName,
                                         RestoreInfo *restoreInfo,
                                         Xid hashTreeId,
                                         const XcalarApiUserId *userId,
                                         int64_t *snapshotBatchId,
                                         Dag *sessionDag);

    MustCheck RestoreUpdateOutput restoreUpdate(const char *dirName,
                                                const char *publishedTableName,
                                                const XcalarApiUserId *userId,
                                                Xid hashTreeId,
                                                int64_t batchId,
                                                time_t unixTS,
                                                bool avoidSelfSelects,
                                                Atomic64 *outstandingUpdates,
                                                RestoreInfo *restoreInfo,
                                                Dag *sessionDag);

    MustCheck Status applyUpdateFuture(Xid hashTreeId,
                                       const char *dirName,
                                       const char *publishedTableName,
                                       RestoreInfo *restoreInfo,
                                       const XcalarApiUserId *userId,
                                       int64_t batchId,
                                       Dag *sessionDag,
                                       Future<RestoreUpdateOutput> *future);

    MustCheck Status unpersistUpdate(const char *publishedTableName,
                                     Xid hashTreeId,
                                     int64_t batchId,
                                     time_t unixTS);

    MustCheck HashTreeEntry *getHashTreeEntryById(Xid hashTreeId);

    MustCheck Status persistCoalesceBatchId(const char *publishTableName,
                                            Xid hashTreeId,
                                            int64_t coalesceBatchId);

    MustCheck Status dispatchToDlmNode(void *payload,
                                       size_t payloadSize,
                                       void *ephemeral,
                                       size_t ackSize);

    MustCheck Status coalesceHashTreeInternal(const char *publishTableName,
                                              Xid hashTreeId,
                                              int64_t coalesceBatchId);

    MustCheck Status getPersistentCoalesceBatchId(const char *publishTableName,
                                                  Xid hashTreeId,
                                                  int64_t *retCoalesceBatchId);

    MustCheck Status persistSchema(const char *publishTableName,
                                   Xid hashTreeId,
                                   XdbMeta *xdbMeta);

    MustCheck Status persistDependency(const char *publishTableName,
                                       XcalarApiTableInput srcTable,
                                       Dag *dag);

    MustCheck Status getPersistedSchemaHelper(const char *publishTableName,
                                              Xid hashTreeId,
                                              json_t *jsonSchema,
                                              XcalarApiColumnInfo *colInfo,
                                              size_t numCols);
    MustCheck Status getPersistedSchema(const char *publishTableName,
                                        Xid hashTreeId,
                                        XcalarApiColumnInfo **retKeys,
                                        size_t *retNumKeys,
                                        XcalarApiColumnInfo **retValues,
                                        size_t *retNumValues);

    MustCheck Status destroyHashTreeInt(Xid hashTreeId,
                                        const char *name,
                                        bool inactivateOnly);

    MustCheck Status getFQN(char *fqn, size_t fqnLen, const char *ptrName);

    MustCheck Status executeRestoreRetina(const char *retName,
                                          unsigned numParams,
                                          XcalarApiParameter *params,
                                          Dag **outputDag);

    MustCheck Status executeUpdateRetina(const char *retinaPath,
                                         const char *retinaName,
                                         const char *publishedTableName,
                                         const XcalarApiUserId *userId,
                                         bool avoidSelfSelects,
                                         Dag **outputDag,
                                         RestoreInfo *restoreInfo,
                                         Dag *sessionDag);

    MustCheck Status persistPublishedTable(XcalarApiTableInput firstUpdate,
                                           Dag *dag,
                                           time_t unixTS,
                                           const char *publishedTableName,
                                           Xid hashTreeId);

    MustCheck Status unpersistPublishedTable(const char *publishedTableName,
                                             Xid hashTreeId);

    // Keep this private, use init instead
    HashTreeMgr() {}

    // Keep this private, use destroy instead
    ~HashTreeMgr() {}

    HashTreeMgr(const HashTreeMgr &) = delete;
    HashTreeMgr &operator=(const HashTreeMgr &) = delete;
};

class TwoPcMsg2pcPubTableDlm1 : public TwoPcAction
{
  public:
    TwoPcMsg2pcPubTableDlm1() {}
    virtual ~TwoPcMsg2pcPubTableDlm1() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    TwoPcMsg2pcPubTableDlm1(const TwoPcMsg2pcPubTableDlm1 &) = delete;
    TwoPcMsg2pcPubTableDlm1(const TwoPcMsg2pcPubTableDlm1 &&) = delete;
    TwoPcMsg2pcPubTableDlm1 &operator=(const TwoPcMsg2pcPubTableDlm1 &) =
        delete;
    TwoPcMsg2pcPubTableDlm1 &operator=(const TwoPcMsg2pcPubTableDlm1 &&) =
        delete;
};

#endif  // _HASHTREE_H_
