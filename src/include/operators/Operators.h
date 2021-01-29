// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORS_H_
#define _OPERATORS_H_

#include "primitives/Primitives.h"
#include "xdb/TableTypes.h"
#include "libapis/ApiHandler.h"
#include "df/DataFormatTypes.h"
#include "msg/Message.h"
#include "scalars/Scalars.h"
#include "operators/OperatorsTypes.h"
#include "operators/GenericTypes.h"
#include "dag/DagTypes.h"
#include "common/InitLevel.h"
#include "runtime/Semaphore.h"
#include "operators/OperatorsApiWrappers.h"
#include "xdb/Xdb.h"
#include "xcalar/compute/localtypes/ParentChild.pb.h"
#include "cursor/Cursor.h"
#include "stat/Statistics.h"
#include "ns/LibNsTypes.h"
#include "util/MemoryPile.h"
#include "table/Table.h"
#include "table/TableNs.h"

static constexpr const uint64_t OpMinRowCountMult = 1 * KB;

struct OperatorsShipWork;
struct OpHandle;
struct OpChildShmContext;
struct OpDemystifyLocalArgs;
struct OpIndexDemystifyLocalArgs;

class Operators final
{
    friend class FilterAndMapDemystifySend;
    friend class GroupByDemystifySend;
    friend class CreateScalarTableDemystifySend;
    friend class IndexDemystifySend;
    friend class OperatorsDemystifyRecv;
    friend class FilterAndMapDemystifyRecv;
    friend class GroupByDemystifyRecv;
    friend class CreateScalarTableDemystifyRecv;
    friend class IndexDemystifyRecv;
    friend class OperatorHandler;
    friend class OperatorHandlerProject;
    friend class OperatorHandlerJoin;
    friend class HashTree;
    friend class OperatorsGvm;

  public:
    enum { LocalOperatorsNotUnique = false, LocalOperatorsUnique = true };

    static bool operatorsInited;

    static constexpr const uint64_t StatsCount = 6;
    static StatGroupId statsGrpId;
    static StatHandle indexDsHandle;
    static StatHandle operationsSucceeded;
    static StatHandle operationsFailed;
    static StatHandle operationsOutstanding;
    static StatHandle indexSetKeyTypeQueued;
    static StatHandle indexSetKeyTypeAborted;

    struct PerTxnInfo {
        IntHashTableHook hook;
        OpTxnCookie *remoteCookies = NULL;
        Txn txn;
        PerOpGlobalInput perOpGlobIp;
        DsDemystifyTableInput *demystifyInput = NULL;
        Xdb *dstXdb = NULL;
        XdbMeta *dstMeta = NULL;
        Xdb *srcXdb = NULL;
        XdbMeta *srcMeta = NULL;
        MustCheck Xid getMyId() const { return txn.id_; }
        void del();

        PerTxnInfo() = default;
        ~PerTxnInfo() = default;
    };

    MustCheck Status index(Dag *dag,
                           XcalarApiIndexInput *indexInput,
                           void *optimizerContext,
                           XcalarApiUserId *user);

    static unsigned getNumWorkers(uint64_t rows);

    MustCheck Status loadDataset(Dag *dag,
                                 XcalarApiBulkLoadInput *bulkLoadInput,
                                 void *optimizerContext,
                                 XcalarApiBulkLoadOutput *bulkLoadOutput,
                                 DsDataset **datasetOut,
                                 DatasetRefHandle *dsRefHandleOut,
                                 XdbId *xdbIdOut,
                                 XcalarApiUserId *user);

    MustCheck Status getRowNum(Dag *srcDag,
                               Dag *dstDag,
                               XcalarApiGetRowNumInput *getRowNumInput,
                               void *optimizerContext,
                               XcalarApiUserId *user);

    MustCheck Status project(Dag *dag,
                             XcalarApiProjectInput *projectInput,
                             void *optimizerContext,
                             XcalarApiUserId *user);

    MustCheck Status filter(Dag *dag,
                            XcalarApiFilterInput *filterInput,
                            void *optimizerContext,
                            XcalarApiUserId *user);

    MustCheck Status filterAndMapGlobal(Dag *dag,
                                        const XdbId srcXdbId,
                                        const XdbId dstXdbId,
                                        unsigned numEvals,
                                        char **evalStrs,
                                        OperatorFlag opFlag);

    MustCheck Status groupBy(Dag *dag,
                             XcalarApiGroupByInput *groupByInput,
                             size_t inputSize,
                             void *optimizerContext,
                             XcalarApiUserId *user);

    MustCheck Status join(Dag *dag,
                          XcalarApiJoinInput *joinInput,
                          void *optimizerContext,
                          XcalarApiUserId *user);

    MustCheck Status unionOp(Dag *dag,
                             XcalarApiUnionInput *unionInput,
                             size_t inputSize,
                             void *optimizerContext,
                             XcalarApiUserId *user);

    MustCheck Status buildScalarTable(Dag *dag,
                                      ExExportBuildScalarTableInput *buildInput,
                                      XdbId *tempXdbId,
                                      XcalarApiUserId *user);

    MustCheck Status map(Dag *dag,
                         XcalarApiMapInput *mapInput,
                         size_t inputSize,
                         void *optimizerContext,
                         XcalarApiUserId *user);

    MustCheck Status synthesize(Dag *dag,
                                XcalarApiSynthesizeInput *synthesizeInput,
                                size_t inputSize,
                                void *optimizerContext);

    MustCheck Status aggregate(Dag *dag,
                               XcalarApiAggregateInput *aggregateInput,
                               Scalar *scalarOut,
                               bool shouldDestroyOp,
                               XcalarApiUserId *user);

    MustCheck Status getRowCount(DagTypes::NamedInput *tableNameInput,
                                 DagTypes::DagId dagId,
                                 uint64_t *rowCountOut,
                                 XcalarApiUserId *user);

    enum MergeTableType : unsigned {
        MergeDeltaTable,
        MergeTargetTable,
        MergeNumTables,
    };
    MustCheck Status merge(const char *tableName[MergeNumTables],
                           XdbId xdbId[MergeNumTables],
                           TableNsMgr::IdHandle *idHandle[MergeNumTables]);

    MustCheck Status mergeInit(const char *tableName[MergeNumTables],
                               XdbId xdbId[MergeNumTables],
                               TableNsMgr::IdHandle *idHandle[MergeNumTables]);

    MustCheck Status
    mergePrepare(const char *tableName[MergeNumTables],
                 XdbId xdbId[MergeNumTables],
                 TableNsMgr::IdHandle *idHandle[MergeNumTables],
                 TableMgr::ColumnInfoTable *columns[MergeNumTables]);

    MustCheck Status
    mergeCommit(const char *tableName[MergeNumTables],
                TableNsMgr::IdHandle *idHandle[MergeNumTables]);

    enum class PostCommitType {
        Inline,
        Deferred,
    };
    Status mergePostCommit(TableNsMgr::IdHandle *idHandle,
                           PostCommitType pcType);

    void mergeAbort(const char *tableName[MergeNumTables],
                    XdbId xdbId[MergeNumTables],
                    TableNsMgr::IdHandle *idHandle[MergeNumTables]);

    MustCheck Status mergeSetupColumns(const char *tableName,
                                       XdbId xdbId,
                                       XdbMeta **retXdbMeta,
                                       MergeTableType mergeTableType,
                                       TableMgr::ColumnInfoTable *columns);

    MustCheck Status
    mergeInitLocal(const char *tableName[MergeNumTables],
                   TableNsMgr::IdHandle *idHandle[MergeNumTables],
                   XdbId xdbId[MergeNumTables]);

    MustCheck Status
    mergePrepareLocal(const char *tableName[MergeNumTables],
                      TableNsMgr::IdHandle *idHandle[MergeNumTables],
                      XdbId xdbId[MergeNumTables]);

    MustCheck Status mergePostCommitLocal(TableNsMgr::IdHandle *idHandle);

    MustCheck Status
    mergeAbortLocal(const char *tableName[MergeNumTables],
                    TableNsMgr::IdHandle *idHandle[MergeNumTables],
                    XdbId xdbId[MergeNumTables]);

    MustCheck Status
    getTableMeta(XcalarApiGetTableMetaInput *getTableMetaInputIn,
                 XcalarApiOutput **output,
                 size_t *outputSizeOut,
                 DagTypes::DagId dagId,
                 XcalarApiUserId *user);

    void operatorsDestroy();

    MustCheck Status hashAndSendKv(TransportPageHandle *scalarPagesHandle,
                                   TransportPage *scalarPages[],
                                   TransportPageHandle *demystifyPagesHandle,
                                   TransportPage *demystifyPages[],
                                   unsigned numKeys,
                                   const DfFieldType *keyType,
                                   const DfFieldValue *keyValue,
                                   bool *keyValid,
                                   NewKeyValueEntry *dstKvEntry,
                                   OpInsertHandle *insertHandle,
                                   Dht *dht,
                                   TransportPageType bufType,
                                   DemystifyMgr::Op op);

    MustCheck Status indexProcessDemystifiedKeys(
        OpIndexDemystifyLocalArgs *localArgs, NewKeyValueEntry *scalarValues);

    static MustCheck Status processIndexPage(TransportPage *scalarPage);

    // Remains a static as childnode calls this after calling a subset of
    // initTeardown::init because of it's initLevel.
    static Status operatorsChildEval(const ChildEvalRequest &request,
                                     ProtoResponseMsg *response);

    static Status operatorsChildEvalHelper(OpChildShmInput *input,
                                           OpChildShmOutput *output,
                                           bool icvMode,
                                           uint64_t *numRowsFailedTotal);

    Status indexRow(OpMeta *opMeta,
                    TransportPage **scalarPages,
                    TransportPage **demystifyPages,
                    TransportPageType bufType,
                    NewKeyValueEntry *srcKvEntry,
                    DfFieldValue *keys,
                    bool *keyValid,
                    const DfFieldType *keyTypesIn,
                    OpInsertHandle *insertHandle);

    void updateEvalXdfErrorStats(Status status, OpEvalErrorStats *errorStats);
    void updateOpStatusForEval(OpEvalErrorStats *errorStats,
                               OpEvalErrorStats *evalErrorStats);

    enum OperationStage {
        Begin = 0,
        End = 1,
    };
    void logOperationStage(const char *opName,
                           const Status &status,
                           Stopwatch &watch,
                           OperationStage opStage);

    // ========================= Usrnode Handlers =======================
    void operatorsProjectLocal(MsgEphemeral *eph, void *payload);
    void operatorsGetRowNumLocal(MsgEphemeral *eph, void *payload);
    void operatorsFilterLocal(MsgEphemeral *eph, void *payload);
    void operatorsGroupByLocal(MsgEphemeral *eph, void *payload);
    void operatorsJoinLocal(MsgEphemeral *eph, void *payload);
    void operatorsMapLocal(MsgEphemeral *eph, void *payload);
    void operatorsUnionLocal(MsgEphemeral *eph, void *payload);
    void operatorsAggregateLocal(MsgEphemeral *eph, void *payload);
    void operatorsExportLocal(MsgEphemeral *eph, void *payload);
    void operatorsIndexLocal(MsgEphemeral *eph, void *payload);
    void operatorsSynthesizeLocal(MsgEphemeral *eph, void *payload);
    MustCheck Status operatorsCountLocal(XdbId xdbId,
                                         bool unique,
                                         uint64_t *count);
    void operatorsGetTableMetaMsgLocal(MsgEphemeral *eph, void *payload);

    MustCheck Status getRowNumHelper(OperatorsApiInput *apiInput,
                                     Xdb *srcXdb,
                                     XdbMeta *srcMeta,
                                     Xdb *dstXdb,
                                     XdbMeta *dstMeta);

    MustCheck Status projectHelper(OperatorsApiInput *apiInput,
                                   Xdb *srcXdb,
                                   XdbMeta *srcXdbMeta,
                                   Xdb *dstXdb,
                                   XdbMeta *dstXdbMeta);

    MustCheck Status joinHelper(OperatorsApiInput *apiInput,
                                Xdb *leftXdb,
                                XdbMeta *leftMeta,
                                Xdb *rightXdb,
                                XdbMeta *rightMeta,
                                Xdb *dstXdb,
                                XdbMeta *dstMeta);

    static void combineValueArray(NewKeyValueEntry *leftKvEntry,
                                  NewKeyValueEntry *rightKvEntry,
                                  unsigned numLeftEntries,
                                  unsigned numRightEntries,
                                  OpKvEntryCopyMapping *opKvEntryCopyMapping,
                                  NewKeyValueEntry *dstKvEntry);

    MustCheck Status initChildShm(OpMeta *opMeta,
                                  unsigned numVariables,
                                  XdbId srcXdbId,
                                  unsigned numEvals,
                                  char **evalStrings,
                                  XcalarEvalClass1Ast *asts,
                                  OpChildShmContext *context);

    static void deserializeChildShmInput(OpChildShmInput *input);

    void initDemystifyLocalArgs(OpDemystifyLocalArgs *localArgs,
                                Scalar **scratchPadScalars,
                                DemystifyVariable *demystifyVariables,
                                Xdb *scratchPadXdb,
                                void *ast,
                                MemoryPile **scalarPile,
                                Scalar **results,
                                bool containsUdf,
                                OpChildShmContext *childContext,
                                OpInsertHandle *insertHandle,
                                OpMeta *opMeta);

    MustCheck Status getRowMeta(OpRowMeta **rowMetaOut,
                                uint64_t rowNum,
                                MemoryPile **rowMetaPileIn,
                                size_t rowMetaSize,
                                Xdb *srcXdb,
                                TableCursor *cur,
                                unsigned numVars);

    void putRowMeta(OpRowMeta *rowMeta, unsigned numScalars);

    MustCheck Status fillScratchPadWithImmediates(Scalar **scratchPadScalars,
                                                  unsigned numVariables,
                                                  OpMeta *opMeta,
                                                  NewKeyValueEntry *kvEntry);

    MustCheck Status
    populateRowMetaWithScalarsFromArray(ScalarPtr dest[],
                                        unsigned numValues,
                                        ScalarPtr source[],
                                        MemoryPile **scalarPile);

    MustCheck Status udfExecFilterOrMap(OpMeta *opMeta,
                                        OpRowMeta *rowMeta,
                                        OpChildShmContext *childContext,
                                        Xdb *dstXdb);

    MustCheck Status executeFilterOrMap(XcalarEvalClass1Ast *ast,
                                        Xdb *dstXdb,
                                        NewKeyValueEntry *srcKvEntry,
                                        Scalar **scalars,
                                        OpInsertHandle *insertHandle,
                                        OpMeta *opMeta);

    MustCheck Status udfSendToChildForEval(OpChildShmInput *input,
                                           OpChildShmContext *childContext,
                                           OpMeta *opMeta,
                                           Xdb *dstXdb);

    void substituteAggregateResultsIntoAst2(
        XcalarEvalClass2Ast *ast,
        OperatorsAggregateResult aggregateResults[],
        unsigned numAggregateResults);

    template <typename OperatorsDemystifySend>
    MustCheck Status processGroup(OperatorsDemystifySend *demystifyHandle,
                                  OpDemystifyLocalArgs *localArgs,
                                  uint64_t keyCount,
                                  int64_t *rowNum,
                                  MemoryPile **rowMetaPile,
                                  MemoryPile **groupMetaPile,
                                  OpMeta *opMeta,
                                  Xdb *srcXdb,
                                  TableCursor *keyCursor,
                                  DfFieldValue key);

    MustCheck Status insertIntoScalarTable(Scalar **scalars,
                                           DfFieldValue key,
                                           bool keyValid,
                                           OpMeta *opMeta,
                                           NewKeyValueEntry *dstKvEntry,
                                           OpInsertHandle *insertHandle,
                                           XcalarEvalClass1Ast *ast);

    MustCheck Status getScalarTableRowMeta(OpScalarTableRowMeta **rowMetaOut,
                                           DfFieldValue key,
                                           bool keyValid,
                                           MemoryPile **rowMetaPileIn,
                                           size_t rowMetaSize,
                                           unsigned numVars);

    MustCheck Status operatorsInitOpStatus(OpStatus *opStatus,
                                           XcalarApis api,
                                           XcalarApiInput *apiInput);

    static void shallowCopyKvEntry(NewKeyValueEntry *dstKvEntry,
                                   uint64_t maxNumValuesInDstKvEntry,
                                   NewKeyValueEntry *srcKvEntry,
                                   OpKvEntryCopyMapping *opKvEntryCopyMapping,
                                   unsigned dstStartIdx,
                                   unsigned numEntriesToCopy);

    static void pruneSlot(bool dropSrcSlots,
                          bool serializeSlots,
                          XdbInsertKvState insertState,
                          Xdb *srcXdb,
                          Xdb *dstXdb,
                          uint64_t slotId);

    static bool isDemystificationRequired(char **variableNames,
                                          unsigned numVariables,
                                          const XdbMeta *srcMeta);

    static bool isDemystificationRequired(const XdbMeta *srcMeta);

    static Status convertLoadUdfToAbsolutePath(
        DfLoadArgs *loadArgs, XcalarApiUdfContainer *udfContainer);

    void freeChildShm(OpChildShmContext *context);

    bool orderingIsJoinCompatible(Ordering leftOrdering,
                                  Ordering rightOrdering);
    // routines to invoke API style operators but for internal use only

    MustCheck Status indexTableApiInternal(
        // so called because it's for Tables, not datasets
        XcalarApiUserId *userId,
        const char *sessionName,
        Dag *sessionGraph,
        const char *srcTableName,
        const char *dstTableName,
        bool broadcastDstTable,
        DagTypes::NodeId *dstNodeIdOut,
        unsigned numKeys,
        const char **keyNames,
        Ordering *orderings,
        bool delaySort,  // sort per 'orderings' (delayed until table accessed)
        bool keepTxn,    // if true, continue using caller's transaction
        char *dstTableNameOut);

    MustCheck Status mapApiInternal(
        XcalarApiUserId *userId,
        const char *sessionName,
        Dag *sessionGraph,
        const char *srcTableName,
        unsigned numEvals,
        const char *evalStrs[TupleMaxNumValuesPerRecord],
        const char *newFieldNames[TupleMaxNumValuesPerRecord],
        bool icvMode,
        bool keepTxn,  // if true, continue using caller's transaction
        DagTypes::NodeId *dstNodeIdOut,
        char *dstTableNameOut);

    MustCheck Status groupByApiInternal(
        XcalarApiUserId *userId,
        const char *sessionName,
        Dag *sessionGraph,
        const char *srcTableName,
        unsigned numKeys,
        const char **keyNames,
        unsigned numEvals,
        const char *evalStrs[TupleMaxNumValuesPerRecord],
        const char *newFieldNames[TupleMaxNumValuesPerRecord],
        bool includeSample,
        bool icvMode,
        bool groupAll,
        bool keepTxn,  // if true, continue using caller's transaction
        char *dstTableNameOut);

    MustCheck Status filterTableApiInternal(
        XcalarApiUserId *userId,
        const char *sessionName,
        Dag *sessionGraph,
        const char *srcTableName,
        const char *evalStr,
        bool keepTxn,  // if true, continue using caller's transaction
        DagTypes::NodeId *dstNodeIdOut,
        char *dstTableNameOut);

    // Generates a summary of failures the map/filter encountered, in a table.
    // The table schema is a two-column schema: {numRowsFailed,
    // failureDescString}
    MustCheck Status
    genSummaryFailureOneTab(char *failMapTableName,
                            uint32_t evalNum,
                            DagTypes::NodeId *failureTableIdOut,
                            char *newFieldColName,
                            char *sessionName,
                            XcalarApiUserId *userId,
                            Dag *dstGraph);

    MustCheck Status broadcastTable(Dag *dag,
                                    XcalarApiUserId *userId,
                                    const char *srcTableName,
                                    const char *dstTableName,
                                    DagTypes::NodeId *dstNodeIdOut,
                                    void *optimizerContext);

    MustCheck Status createRangeHashDht(Dag *dag,
                                        DagTypes::NodeId srcNodeId,
                                        XdbId srcXdbId,
                                        DagTypes::NodeId dstNodeId,
                                        XdbId dstXdbId,
                                        const char *keyName,
                                        const char *dhtName,
                                        Ordering ordering,
                                        XcalarApiUserId *user,
                                        DhtId *dhtId);

    static MustCheck OpKvEntryCopyMapping *getOpKvEntryCopyMapping(
        unsigned numValueArrayEntriesInDst);

    static void initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                       XdbMeta *srcMeta,
                                       XdbMeta *dstMeta,
                                       uint64_t startDstIdx,
                                       uint64_t endDstIdx,
                                       bool sameOrder,
                                       XcalarApiRenameMap renameMap[],
                                       unsigned numRenameEntries,
                                       bool keepColsInRenameMapOnly = false);

    static void initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                       const NewTupleMeta *srcTupMeta,
                                       const char **srcFieldNames,
                                       XdbMeta *dstMeta,
                                       uint64_t startDstIdx,
                                       uint64_t endDstIdx,
                                       bool sameOrder,
                                       XcalarApiRenameMap renameMap[],
                                       unsigned numRenameEntries,
                                       bool keepColsInRenameMapOnly = false);

    static void initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                       const NewTupleMeta *srcTupMeta,
                                       const char **srcFieldNames,
                                       const NewTupleMeta *dstTupMeta,
                                       const char **dstFieldNames,
                                       uint64_t startDstIdx,
                                       uint64_t endDstIdx,
                                       bool sameOrder,
                                       XcalarApiRenameMap renameMap[],
                                       unsigned numRenameEntries,
                                       bool keepColsInRenameMapOnly = false);

    static const char *columnNewName(XcalarApiRenameMap renameMap[],
                                     unsigned numRenameEntries,
                                     const char *srcName,
                                     DfFieldType srcType);
    static TransportPage **allocSourceTransPages();
    static MustCheck Status init();
    void destroy();
    static Operators *get();

    // static as it is called by init to create the Operators singleton
    static MustCheck Status operatorsInit(InitLevel initLevel);

  private:
    static constexpr const char *moduleName = "Operators";
    static constexpr const char *failureCountColName = "FailureCount";

    static constexpr const size_t MinRecordsPerIndexWorkItem = 4 * KB;

    static Operators *instance;

    // Keep these private, use init() and destroy() instead
    Operators() {}
    ~Operators() {}

    Operators(const Operators &) = delete;
    Operators &operator=(const Operators &) = delete;

    static constexpr const size_t SlotsPerTxnTable = 11;

    typedef IntHashTable<Xid,
                         PerTxnInfo,
                         &PerTxnInfo::hook,
                         &PerTxnInfo::getMyId,
                         SlotsPerTxnTable,
                         hashIdentity>
        PerTxnTable;
    PerTxnTable perTxnTable_;
    Mutex txnTableLock_;

    MustCheck Status addPerTxnInfo(void *payload, size_t *outputSizeOut);
    void removePerTxnInfo(void *payload);
    MustCheck Status updatePerTxnInfo(void *payload);
    MustCheck PerTxnInfo *getPerTxnInfo();

    MustCheck static Status fixupKeyIndexes(unsigned numKeys,
                                            int *keyIndexes,
                                            const char **keyNames,
                                            DfFieldType *keyTypes,
                                            NewTupleMeta *tupMeta,
                                            unsigned &numImmediates,
                                            const char *immediateNames[]);

    MustCheck static Status populateJoinTupMeta(
        bool useLeftKey,
        const XdbMeta *leftXdbMeta,
        const XdbMeta *rightXdbMeta,
        unsigned numLeftColumns,
        XcalarApiRenameMap *leftRenameMap,
        unsigned numRightColumns,
        XcalarApiRenameMap *rightRenameMap,
        const char *immediateNames[],
        const char *fatptrPrefixNames[],
        JoinOperator joinType,
        unsigned numKeys,
        const char **keyNames,
        int *keyIndexes,
        DfFieldType *keyTypes,
        bool collisionCheck,
        NewTupleMeta *joinTupMeta,
        unsigned *numImmediatesOut,
        unsigned *numFatptrsOut);

    MustCheck static Status populateValuesDescWithProjectedFields(
        unsigned numKeys,
        int *keyIndexes,
        const NewTupleMeta *srcTupMeta,
        NewTupleMeta *dstTupMeta,
        XdbMeta *xdbMeta,
        char (*projectedFieldNames)[DfMaxFieldNameLen + 1],
        unsigned numProjectedFieldNames,
        const char *immediateNames[],
        unsigned *numImmediatesOut,
        const char *fatptrPrefixNames[],
        unsigned *numFatptrsOut,
        bool keepKey,
        bool *keyKept);

    MustCheck Status issueTwoPcForOp(void *payloadToDistribute,
                                     size_t payloadLength,
                                     MsgTypeId msgTypeId,
                                     TwoPcCallId twoPcCallId,
                                     Dag *dag,
                                     XcalarApiUserId *user,
                                     OperatorFlag opFlag);

    MustCheck Status populateRowMetaWithScalars(ScalarPtr scalarPtrs[],
                                                unsigned numVariables,
                                                NewKeyValueEntry *kvEntry,
                                                MemoryPile **scalarPile);

    MustCheck Status getTempScalar(Scalar **scalarOut,
                                   size_t bufSize,
                                   MemoryPile **scalarPileIn);

    void putTempScalar(Scalar *scalar);

    MustCheck Status getGroupByRowMeta(OpGroupByRowMeta **rowMetaOut,
                                       MemoryPile **rowMetaPileIn,
                                       size_t rowMetaSize,
                                       unsigned numVars,
                                       OpGroupMeta *groupMeta);

    void putGroupByRowMeta(OpGroupByRowMeta *rowMeta, unsigned numScalars);

    void putScalarTableRowMeta(OpScalarTableRowMeta *rowMeta,
                               unsigned numScalars);

    MustCheck Status getGroupMeta(OpGroupMeta **groupMetaOut,
                                  MemoryPile **groupMetaPileIn,
                                  Xdb *srcXdb,
                                  Xdb *dstXdb,
                                  TableCursor *cur,
                                  DfFieldValue key,
                                  uint64_t numRows);

    void putGroupMeta(OpGroupMeta *groupMeta);

    MustCheck Status addRowToGroup(Scalar **scratchPadScalars,
                                   OpGroupMeta *groupMeta,
                                   DfFieldType keyType,
                                   unsigned numVariables,
                                   OpMeta *opMeta,
                                   uint64_t *numRowsMissing,
                                   NewTupleMeta *tupMeta,
                                   Xdb *scratchXdb,
                                   bool lock);

    MustCheck Status executeGroupBy(XcalarEvalClass2Ast *asts,
                                    Xdb *dstXdb,
                                    XdbMeta *dstMeta,
                                    Scalar **results,
                                    OpGroupMeta *groupMeta,
                                    XdbMeta *srcMeta,
                                    Xdb *scratchPadXdb,
                                    NewTupleMeta *tupMeta,
                                    bool includeSrcTableSample,
                                    OpMeta *opMeta,
                                    OpInsertHandle *insertHandle);

    MustCheck Status operatorsGroupByWork(XcalarEvalClass2Ast *asts,
                                          Xdb *dstXdb,
                                          XdbMeta *dstMeta,
                                          Xdb *scratchPadXdb,
                                          XdbMeta *srcMeta,
                                          unsigned numVariables,
                                          NewKeyValueEntry *kvEntry,
                                          OpMeta *opMeta,
                                          MemoryPile **scalarPile,
                                          bool includeSrcTableSample,
                                          Scalar **results,
                                          OpInsertHandle *insertHandle);

    MustCheck Status processFilterAndMapResultRow(OpMeta *opMeta,
                                                  Xdb *dstXdb,
                                                  NewKeyValueEntry *srcKvEntry,
                                                  NewKeyValueEntry *dstKvEntry,
                                                  unsigned numEvals,
                                                  DfFieldValue *evalResults,
                                                  DfFieldType *evalTypes,
                                                  Status *evalStatuses,
                                                  OpInsertHandle *insertHandle);

    MustCheck Status processChildShmResponse(OpMeta *opMeta,
                                             OpChildShmContext *childContext,
                                             Xdb *dstXdb);

    MustCheck Status operatorsFilterAndMapWork(XcalarEvalClass1Ast *ast,
                                               Xdb *dstXdb,
                                               unsigned numVariables,
                                               NewKeyValueEntry *kvEntry,
                                               OpMeta *opMeta,
                                               MemoryPile **scalarPile,
                                               bool containsUdf,
                                               OpChildShmContext *childContext,
                                               OpEvalErrorStats *errorStats,
                                               OpInsertHandle *insertHandle);

    MustCheck Status
    operatorsCreateScalarTableWork(Xdb *dstXdb,
                                   unsigned numVariables,
                                   NewKeyValueEntry *kvEntry,
                                   OpMeta *opMeta,
                                   MemoryPile **scalarPile,
                                   XcalarEvalClass1Ast *ast,
                                   OpInsertHandle *insertHandle);

    MustCheck Status
    opMetaInit(OpMeta *opMeta,
               Dag *dag,
               int *newFieldIdxs,
               int numVariables,
               char **variableNames,
               unsigned numEvals,
               char **evalStrings,
               int **evalArgIndices,
               XdbId srcXdbId,
               Xdb *srcXdb,
               XdbMeta *srcMeta,
               XdbId dstXdbId,
               Xdb *dstXdb,
               XdbMeta *dstMeta,
               OpStatus *opStatus,
               OperatorsEnum op,
               bool fatptrDemystRequired,
               XdbInsertKvState insertState,
               OpPossibleImmediateIndices *possibleImmediateIndices,
               bool **validIndicesMap,
               bool *validIndices,
               OperatorsAggregateResult *aggregateResults,
               unsigned numAggregateResults,
               ChildPool *childPool,
               TransportPageHandle *fatptrTransPageHandle,
               TransportPageHandle *indexHandle,
               OperatorFlag flags);

    void opMetaDestroy(OpMeta *opMeta);

    static bool columnMatch(XcalarApiRenameMap renameMap[],
                            unsigned numRenameEntries,
                            const char *srcName,
                            DfFieldType srcType,
                            const char *dstName,
                            DfFieldType dstType,
                            bool matchColsInRenameMapOnly);

    MustCheck Status issueWork(Xdb *srcXdb,
                               XdbMeta *srcMeta,
                               Xdb *dstXdb,
                               XdbMeta *dstMeta,
                               OperatorsEnum operators,
                               OpHandle *opHandle,
                               OpMeta *opMeta,
                               unsigned numWorkers);

    MustCheck Status initiateOp(PerTxnInfo *perTxnInfo,
                                unsigned numEvals,
                                char **newFieldNames,
                                char **evalStrings,
                                int **evalArgIndices,
                                OperatorsEnum operators,
                                uint64_t numFunc,
                                EvalUdfModuleSet *udfModules,
                                const char *userIdName,
                                uint64_t sessionId);

    MustCheck Status operatorsInt(XdbId srcXdbId,
                                  XdbId dstXdbId,
                                  unsigned numVariables,
                                  char **variableNames,
                                  char *evalString,
                                  OperatorsEnum operators,
                                  DagTypes::DagId dagId,
                                  EvalUdfModuleSet *udfModules,
                                  OperatorFlag flags);

    MustCheck Status issueWorkIndexDataset(uint64_t numRecords,
                                           OperatorsIndexHandle *indexHandle,
                                           DagTypes::DagId dagId);

    MustCheck Status
    synthesizeDataset(XcalarApiSynthesizeInput *synthesizeInput,
                      DagTypes::DagId dagId,
                      Xdb *dstXdb,
                      XdbMeta *dstMeta);

    MustCheck Status operatorsFilterAndMapLocal(OperatorsEnum operators,
                                                OperatorsApiInput *apiInput,
                                                const XdbId srcXdbId,
                                                const XdbId dstXdbId,
                                                unsigned numEvals,
                                                char **evalStrs);

    MustCheck Status initIndexHandle(OperatorsIndexHandle *indexHandle,
                                     Xid srcXid,
                                     bool isTable,
                                     XdbId dstXdbId,
                                     XcalarApiIndexInput *indexInput,
                                     DagTypes::DagId dagId);

    void destroyIndexHandle(OperatorsIndexHandle *indexHandle);

    MustCheck Status issueGvm(XdbId srcXdbId,
                              XdbId dstXdbId,
                              unsigned numVariables,
                              char **variableNames,
                              DagTypes::DagId dagId,
                              OperatorFlag opFlag);

    void cleanoutGvm(XdbId srcXdbId,
                     XdbId dstXdbId,
                     DagTypes::DagId dagId,
                     Status cleanoutStatus);
};

#endif  // _OPERATORS_H_
