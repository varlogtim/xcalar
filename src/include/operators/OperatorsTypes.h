// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORSTYPES_H
#define _OPERATORSTYPES_H

#include "primitives/Primitives.h"
#include "xdb/TableTypes.h"
#include "xdb/DataModelTypes.h"
#include "operators/XcalarEvalTypes.h"
#include "runtime/Semaphore.h"
#include "xcalar/compute/localtypes/ParentChild.pb.h"
#include "cursor/Cursor.h"
#include "libapis/LibApisEnums.h"
#include "childpool/ChildPool.h"
#include "demystify/Demystify.h"
#include "operators/GenericTypes.h"
#include "newtupbuf/NewTupleTypes.h"
#include "util/MemoryPile.h"
#include "util/TrackHelpers.h"
#include "util/AccumulatorHashTable.h"
#include "JoinOpEnums.h"
#include "UnionOpEnums.h"
#include "operators/OperatorsEvalTypes.h"
#include "xdb/Xdb.h"

class ParentChild;
class TransportPageHandle;

enum OperatorsConstants {
    OperatorsAggregateTag = '^',
};

enum OperatorsEnum {
    OperatorsEvalFn = 0,
    OperatorsAggFn = 1,
    OperatorsIndexDataset = 2,
    OperatorsIndexTable = 3,
    OperatorsSynthesizeDataset = 4,
    OperatorsSynthesizeTable = 5,

    OperatorsMap = XcalarApiMap,
    OperatorsFilter = XcalarApiFilter,
    OperatorsGroupBy = XcalarApiGroupBy,
    OperatorsAggregate = XcalarApiAggregate,
    OperatorsExport = XcalarApiExport,
    OperatorsJoin = XcalarApiJoin,
    OperatorsUnion = XcalarApiUnion,
    OperatorsProject = XcalarApiProject,
    OperatorsGetRowNum = XcalarApiGetRowNum,
};

enum OperatorFlag {
    OperatorFlagNone = 0,
    OperatorFlagIcvMode = 1,
    OperatorFlagIncludeSrcTableInSample = 1 << 2,
    OperatorFlagChildProcessing = 1 << 3,
    OperatorFlagDropSrcSlots = 1 << 4,
    OperatorFlagSerializeSlots = 1 << 5
};

enum OperatorsFreeScalarValuesMode {
    OperatorsFreeWholeValueArray,
    OperatorsFreeLastEntry,
};

struct OperatorsAsyncContext {
    Semaphore *sem;
    uint64_t numNodes;
    Status statusArray[0];
};

typedef Scalar *ScalarPtr;

struct OpRowMeta {
    Atomic32 numIssued;
    TableCursor opCursor;
    int64_t rowNum;  // XXX: for debugging
    ScalarPtr scalars[0];
};

struct PerOpGlobalInput {
    XdbId srcXdbId;
    XdbId dstXdbId;
    DagTypes::DagId dagId;
    NodeId srcNodeId;
    OperatorFlag opFlag;
};

struct OpGroupMeta {
    uint64_t numRowsMissing;
    DfFieldValue key;
    TableCursor opCursor;
    Mutex lock;
    XdbPage *firstPage;
};

struct OpGroupByRowMeta {
    Atomic32 numIssued;
    OpGroupMeta *groupMeta;
    ScalarPtr scalars[0];
};

struct OpScalarTableRowMeta {
    Atomic32 numIssued;
    DfFieldValue key;
    bool keyValid;
    ScalarPtr scalars[0];
};

struct OpPossibleImmediateIndices {
    unsigned numPossibleImmediateIndices;
    unsigned possibleImmediateIndices[TupleMaxNumValuesPerRecord];
};

struct OpKvEntryCopyMapping {
    bool isReplica;
    uint64_t maxNumEntries;
    uint64_t numEntries;
    int srcIndices[0];
};

struct OperatorsAggregateResult {
    DagTypes::NodeId aggId;
    char aggName[XcalarApiMaxTableNameLen + 1];
    unsigned variableIdx;
    Scalar *result;
};

struct OpMeta {
    Atomic64 status;

    int *newFieldIdxs;
    Dag *dag;

    unsigned numVariables;
    char **variableNames;
    unsigned numEvals;
    char **evalStrings;
    int **evalArgIndices;

    XdbId srcXdbId;
    Xdb *srcXdb;
    XdbMeta *srcMeta;
    XdbId dstXdbId;
    Xdb *dstXdb;
    XdbMeta *dstMeta;

    Txn txn;

    struct OpStatus *opStatus;
    OperatorsEnum op;
    bool includeSrcTableSample;
    bool fatptrDemystRequired;
    XdbInsertKvState insertState;

    bool indexOnRowNum;
    Dht *dht;

    OpPossibleImmediateIndices *possibleImmediateIndices;
    bool **validIndicesMap;
    // flattened validIndicesMap
    bool *validIndices;
    ChildPool *childPool;

    // malloced array of scalars to store any aggregate results needed
    // for evaluation
    OperatorsAggregateResult *aggregateResults;
    unsigned numAggregateResults;

    TransportPageHandle *fatptrTransPageHandle;
    TransportPageHandle *indexHandle;

    // extracted from OperatorFlag
    bool icvMode;
    bool dropSrcSlots;
    bool serializeSlots;

    OpKvEntryCopyMapping *opKvEntryCopyMapping;
    Semaphore *sem;
    size_t scalarsSize;
};

struct OperatorsIndexHandle {
    Xid srcXid;
    XdbId dstXdbId;
    char *keyName;
    struct XcalarApiIndexInput *indexInput;

    DsDataset *dataset;
    Status indexStatus;
    Semaphore sem;
    Xdb *dstXdb;
    XdbMeta *dstMeta;
    Dht *dht;
    OpStatus *opStatus;
    uint64_t numRecords;

    TransportPageHandle *fatptrHandle;  // send fatptrs for demystification
    TransportPageHandle *scalarHandle;  // send scalars to dht destination
};

struct OpIndexErrorStats {
    union {
        uint64_t numParseError;
        Atomic64 numParseErrorAtomic;
    };

    union {
        uint64_t numFieldNoExist;
        Atomic64 numFieldNoExistAtomic;
    };

    union {
        uint64_t numTypeMismatch;
        Atomic64 numTypeMismatchAtomic;
    };

    union {
        uint64_t numOtherError;
        Atomic64 numOtherErrorAtomic;
    };
};

struct OpLoadErrorStats {
    union {
        uint64_t numFileOpenFailure;
        Atomic64 numFileOpenFailureAtomic;
    };
    union {
        uint64_t numDirOpenFailure;
        Atomic64 numDirOpenFailureAtomic;
    };
};

struct OpEvalXdfErrorStats {
    union {
        uint64_t numTotal;  // grand total
        Atomic64 numTotalAtomic;
    };
    // break-down of grand total below
    union {
        uint64_t numUnsubstituted;
        Atomic64 numUnsubstitutedAtomic;
    };
    union {
        uint64_t numUnspportedTypes;
        Atomic64 numUnspportedTypesAtomic;
    };
    union {
        uint64_t numMixedTypeNotSupported;
        Atomic64 numMixedTypeNotSupportedAtomic;
    };
    union {
        uint64_t numEvalCastError;
        Atomic64 numEvalCastErrorAtomic;
    };
    union {
        uint64_t numDivByZero;
        Atomic64 numDivByZeroAtomic;
    };
    union {
        uint64_t numMiscError;
        Atomic64 numMiscErrorAtomic;
    };
};

struct FailureDesc {
    uint64_t numRowsFailed;
    char failureDesc[XcalarApiMaxFailureDescLen + 1];
};

struct FailureSummary {
    char failureSummName[XcalarApiMaxTableNameLen + 1];
    ssize_t failureSummInfoCount;
    FailureDesc failureSummInfo[XcalarApiMaxFailures];
};

struct OpEvalUdfErrorStats {
    union {
        uint64_t numEvalUdfError;
        Atomic64 numEvalUdfErrorAtomic;
    };
    // break-down of numEvalUdfError errors:
    //
    // For each of XcalarApiMaxFailureEvals evals that may have failed, a
    // failure summary across all rows, is made available in opFailureSummary[]
    //
    // For each eval x, and unique failureDesc string y,
    // opFailureSummary[x].failureSummInfo[y].numRowsFailed reports the number
    // of rows that have failed with that unique failureDesc string. Upto
    // XcalarApiMaxFailures unique failureDesc strings are reported - see
    // "struct FailureSummary". The name for each eval's FailureSummary is
    // made available in FailureSummary.failureSummName -> this is typically
    // the name of destination column name for that eval function.
    //
    ssize_t opFailureSummaryCount;
    FailureSummary opFailureSummary[XcalarApiMaxFailureEvals];
};

struct OpEvalErrorStats {
    struct OpEvalXdfErrorStats evalXdfErrorStats;
    struct OpEvalUdfErrorStats evalUdfErrorStats;
};

// This struct need to be kept in sync with libapisCommon
// XXX: shouldn't this struct be a union?
struct OpErrorStats {
    OpLoadErrorStats loadErrorStats;
    OpIndexErrorStats indexErrorStats;
    OpEvalErrorStats evalErrorStats;
};

// This struct need to be kept in sync with libapisCommon
// Needs PageSize alignment for Sparse copy.
struct __attribute__((aligned(PageSize))) OpDetails {
    union {
        uint64_t numWorkCompleted;
        Atomic64 numWorkCompletedAtomic;
    };
    struct PerNode {
        uint64_t numRows;
        int8_t hashSlotSkew;
        size_t sizeBytes;
        uint64_t numTransPagesReceived;
    } perNode[MaxNodes];
    volatile bool cancelled;
    uint64_t numWorkTotal;
    uint64_t numRowsTotal;
    size_t sizeTotal;
    uint64_t xdbBytesConsumed;
    uint64_t xdbBytesRequired;
    OpErrorStats errorStats;
};

// Needs PageSize alignment for Sparse copy.
struct __attribute__((aligned(PageSize))) OpStatus {
    OpDetails atomicOpDetails;
    OpMeta *opMeta;
};

// Input for childnode eval. Buffer containing fields used in eval.
struct OpChildShmInput {
    NewTupleMeta tupleMeta;
    unsigned numEvals;

    // Cursor is only used in childnode. Must persist across subsequent calls
    // into childnode, so put it here.
    NewTuplesCursor tupCursor;

    // these are pointers into the variable length buf at the bottom
    char **evalStrings;
    int **evalArgIndices;
    NewTuplesBuffer *tupBuf;  // key is rowMeta, values are scalarObjs

    // Below is the order in which buf is populated.
    // - evalStrings
    // - evalArgIndices
    // - Tuple buffer
    uint8_t buf[0];
};

// Output for childnode eval. Buffer containing evaled field. Eval will
// result in multiple calls into childnode if result overflows this buffer.
struct OpChildShmOutput {
    XdbId srcXdbId;
    NewTupleMeta tupleMeta;
    NewTuplesBuffer tupBuf;
};

// Used to request that child performs eval given the 2 buffers.
struct OpChildEvalRequest {
    ParentChildShmPtr ptrInput;
    ParentChildShmPtr ptrOutput;
};

struct GetTableMetaInput {
    bool isTable;
    bool isPrecise;
    DagTypes::NodeId nodeId;
    Xid xid;
};

class FilterAndMapDemystifySend
    : public DemystifySend<::FilterAndMapDemystifySend>
{
  public:
    FilterAndMapDemystifySend(void *localCookie,
                              TransportPage **demystifyPages,
                              XdbId dstXdbId,
                              bool *validIndices,
                              TransportPageHandle *transPageHandle,
                              TransportPage **additionalPages,
                              TransportPageHandle *additionalPagesHandle)
        : DemystifySend<::FilterAndMapDemystifySend>(
              DemystifyMgr::Op::FilterAndMap,
              localCookie,
              DemystifySend<::FilterAndMapDemystifySend>::SendFatptrsOnly,
              demystifyPages,
              dstXdbId,
              DfOpRowMetaPtr,
              NewTupleMeta::DfInvalidIdx,
              validIndices,
              transPageHandle,
              additionalPages,
              additionalPagesHandle)
    {
    }

    MustCheck Status processDemystifiedRow(void *localCookie,
                                           NewKeyValueEntry *srcKvEntry,
                                           DfFieldValue rowMetaField,
                                           Atomic32 *countToDecrement);
};

class GroupByDemystifySend : public DemystifySend<::GroupByDemystifySend>
{
  public:
    GroupByDemystifySend(void *localCookie,
                         TransportPage **demystifyPages,
                         XdbId dstXdbId,
                         bool *validIndices,
                         TransportPageHandle *transPageHandle,
                         TransportPage **additionalPages,
                         TransportPageHandle *additionalPagesHandle)
        : DemystifySend<
              ::GroupByDemystifySend>(DemystifyMgr::Op::GroupBy,
                                      localCookie,
                                      DemystifySend<::GroupByDemystifySend>::
                                          SendFatptrsOnly,
                                      demystifyPages,
                                      dstXdbId,
                                      DfOpRowMetaPtr,
                                      NewTupleMeta::DfInvalidIdx,
                                      validIndices,
                                      transPageHandle,
                                      additionalPages,
                                      additionalPagesHandle)
    {
    }

    MustCheck Status processDemystifiedRow(void *localCookie,
                                           NewKeyValueEntry *srcKvEntry,
                                           DfFieldValue rowMetaField,
                                           Atomic32 *countToDecrement);
};

class CreateScalarTableDemystifySend
    : public DemystifySend<::CreateScalarTableDemystifySend>
{
  public:
    CreateScalarTableDemystifySend(void *localCookie,
                                   TransportPage **demystifyPages,
                                   XdbId dstXdbId,
                                   bool *validIndices,
                                   TransportPageHandle *transPageHandle,
                                   TransportPage **additionalPages,
                                   TransportPageHandle *additionalPagesHandle)
        : DemystifySend<::CreateScalarTableDemystifySend>(
              DemystifyMgr::Op::CreateScalarTable,
              localCookie,
              DemystifySend<::CreateScalarTableDemystifySend>::SendFatptrsOnly,
              demystifyPages,
              dstXdbId,
              DfOpRowMetaPtr,
              NewTupleMeta::DfInvalidIdx,
              validIndices,
              transPageHandle,
              additionalPages,
              additionalPagesHandle)
    {
    }

    MustCheck Status processDemystifiedRow(void *localCookie,
                                           NewKeyValueEntry *srcKvEntry,
                                           DfFieldValue rowMetaField,
                                           Atomic32 *countToDecrement);
};

class IndexDemystifySend : public DemystifySend<::IndexDemystifySend>
{
  public:
    IndexDemystifySend(DemystifyMgr::Op op,
                       void *localCookie,
                       TransportPage **demystifyPages,
                       XdbId dstXdbId,
                       bool *validIndices,
                       TransportPageHandle *transPageHandle,
                       TransportPage **additionalPages,
                       TransportPageHandle *additionalPagesHandle)
        : DemystifySend<
              ::IndexDemystifySend>(DemystifyMgr::Op::Index,
                                    localCookie,
                                    DemystifySend<
                                        ::IndexDemystifySend>::SendFatptrsOnly,
                                    demystifyPages,
                                    dstXdbId,
                                    DfOpRowMetaPtr,
                                    NewTupleMeta::DfInvalidIdx,
                                    validIndices,
                                    transPageHandle,
                                    additionalPages,
                                    additionalPagesHandle)
    {
    }

    MustCheck Status processDemystifiedRow(void *localCookie,
                                           NewKeyValueEntry *srcKvEntry,
                                           DfFieldValue rowMetaField,
                                           Atomic32 *countToDecrement);
};

class OperatorsDemystifyRecv : public DemystifyRecv
{
  public:
    OperatorsDemystifyRecv(){};
    virtual ~OperatorsDemystifyRecv() {}

    virtual MustCheck Status demystifySourcePage(
        TransportPage *transPage, TransportPageHandle *scalarHandle);
    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context) = 0;
};

class FilterAndMapDemystifyRecv : public OperatorsDemystifyRecv
{
  public:
    FilterAndMapDemystifyRecv(){};
    virtual ~FilterAndMapDemystifyRecv() {}

    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context);
};

class GroupByDemystifyRecv : public OperatorsDemystifyRecv
{
  public:
    GroupByDemystifyRecv(){};
    virtual ~GroupByDemystifyRecv() {}

    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context);
};

class CreateScalarTableDemystifyRecv : public OperatorsDemystifyRecv
{
  public:
    CreateScalarTableDemystifyRecv(){};
    virtual ~CreateScalarTableDemystifyRecv() {}

    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context);
};

class IndexDemystifyRecv : public OperatorsDemystifyRecv
{
  public:
    IndexDemystifyRecv(){};
    virtual ~IndexDemystifyRecv() {}

    virtual MustCheck Status processScalarPage(TransportPage *transPage,
                                               void *context);
};

struct OpHandle {
    uint64_t rowsToProcess;
    uint64_t numSlots;

    char **evalStrings;
    TransportPageHandle fatptrHandle;
    TransportPageHandle indexHandle;
};

struct OpChildShmContext {
    OpChildShmInput *input;
    OpChildShmOutput *output;
};

struct OpDemystifyLocalArgs {
    OpDemystifyLocalArgs(Scalar **scratchPadScalarsIn,
                         DemystifyVariable *demystifyVariablesIn,
                         Xdb *scratchPadXdbIn,
                         void *astsIn,
                         MemoryPile **scalarPileIn,
                         Scalar **resultsIn,
                         bool containsUdfIn,
                         OpChildShmContext *childContextIn,
                         OpInsertHandle *insertHandleIn,
                         OpMeta *opMetaIn)
        : scratchPadScalars(scratchPadScalarsIn),
          demystifyVariables(demystifyVariablesIn),
          scratchPadXdb(scratchPadXdbIn),
          asts(astsIn),
          scalarPile(scalarPileIn),
          results(resultsIn),
          containsUdf(containsUdfIn),
          childContext(childContextIn),
          insertHandle(insertHandleIn),
          opMeta(opMetaIn){};

    Scalar **scratchPadScalars;
    DemystifyVariable *demystifyVariables;
    Xdb *scratchPadXdb;
    void *asts;
    MemoryPile **scalarPile;
    Scalar **results;
    bool containsUdf;
    OpChildShmContext *childContext;
    OpInsertHandle *insertHandle;
    OpMeta *opMeta;
};

struct OpIndexDemystifyLocalArgs {
    OpIndexDemystifyLocalArgs(OpMeta *opMetaIn,
                              TransportPage **scalarPagesIn,
                              TransportPage **demystifyPagesIn,
                              TransportPageType bufTypeIn,
                              Scalar **scratchPadScalarsIn,
                              DemystifyVariable *demystifyVariablesIn,
                              MemoryPile **scalarPileIn,
                              OpInsertHandle *insertHandleIn)
        : opMeta(opMetaIn),
          scalarPages(scalarPagesIn),
          demystifyPages(demystifyPagesIn),
          bufType(bufTypeIn),
          scratchPadScalars(scratchPadScalarsIn),
          demystifyVariables(demystifyVariablesIn),
          scalarPile(scalarPileIn),
          insertHandle(insertHandleIn){};

    OpMeta *opMeta;

    TransportPage **scalarPages;
    TransportPage **demystifyPages;
    TransportPageType bufType;

    Scalar **scratchPadScalars;
    DemystifyVariable *demystifyVariables;
    MemoryPile **scalarPile;

    OpInsertHandle *insertHandle;
};

class OperatorsWorkBase : public Schedulable
{
  public:
    OperatorsWorkBase(const char *name,
                      TrackHelpers::WorkerType masterWorkerIn,
                      unsigned workerIdIn)
        : Schedulable(name), masterWorker(masterWorkerIn), workerId(workerIdIn)
    {
    }

    void run() override;
    void done() override;

    virtual Status setUp() = 0;
    virtual Status doWork() = 0;
    virtual void tearDown() = 0;

    int64_t slotId;
    int64_t startRecord;
    int64_t numRecords;
    char *evalString;
    XdbId srcXdbId = XidInvalid;
    XdbId dstXdbId = XidInvalid;

    TrackHelpers *trackHelpers = NULL;
    Xdb *srcXdb = NULL;
    Xdb *dstXdb = NULL;
    OperatorsEnum op;
    OpInsertHandle insertHandle;
    void *localContext = NULL;

    // extracted from OperatorFlag
    bool dropSrcSlots = false;
    bool serializeSlots = false;

    MustCheck Status checkLoopStatus(OpStatus *opStatus,
                                     size_t *loopCounter,
                                     size_t *workCounter);

  private:
    TrackHelpers::WorkerType masterWorker;
    unsigned workerId;
};

class OperatorsIndexDatasetWork : public OperatorsWorkBase
{
  public:
    OperatorsIndexDatasetWork(TrackHelpers::WorkerType masterWorkerIn,
                              unsigned workerIdIn)
        : OperatorsWorkBase("IndexDatasetWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    unsigned numKeys;
    Accessor *keyAccessors = NULL;
    uint64_t recordsPerChunk;
    uint64_t lastChunkLen;
    TransportPage **scalarPages = NULL;
    OperatorsIndexHandle *indexHandle = NULL;
};

class OperatorsDemystifyWork : public OperatorsWorkBase
{
  public:
    OperatorsDemystifyWork(const char *name,
                           TrackHelpers::WorkerType masterWorkerIn,
                           unsigned workerIdIn)
        : OperatorsWorkBase(name, masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp() = 0;
    virtual Status doWork() = 0;
    virtual void tearDown() = 0;

    Scalar **scratchPadScalars = NULL;
    DemystifyVariable *demystifyVariables = NULL;
    TransportPage **demystifyPages = NULL;

    MemoryPile *scalarPile = NULL;
    MemoryPile *rowMetaPile = NULL;

    OpMeta *opMeta = NULL;

    OpDemystifyLocalArgs *localArgs = NULL;
};

class OperatorsIndexTableWork : public OperatorsDemystifyWork
{
  public:
    OperatorsIndexTableWork(TrackHelpers::WorkerType masterWorkerIn,
                            unsigned workerIdIn,
                            TransportPageHandle *indexHandleIn)
        : OperatorsDemystifyWork("IndexWork", masterWorkerIn, workerIdIn),
          indexHandle(indexHandleIn)
    {
    }
    virtual ~OperatorsIndexTableWork() = default;

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    TransportPage **scalarPages = NULL;
    TransportPageHandle *indexHandle;
};

class OperatorsFilterAndMapWork : public OperatorsDemystifyWork
{
  public:
    OperatorsFilterAndMapWork(TrackHelpers::WorkerType masterWorkerIn,
                              unsigned workerIdIn)
        : OperatorsDemystifyWork("FilterAndMapWork", masterWorkerIn, workerIdIn)
    {
    }
    virtual ~OperatorsFilterAndMapWork() = default;

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    XcalarEvalClass1Ast *asts = NULL;
    bool containsUdf = false;
    OpChildShmContext childContext;

    FilterAndMapDemystifySend *demystifyHandle = NULL;
};

class OperatorsGroupByWork : public OperatorsDemystifyWork
{
  public:
    OperatorsGroupByWork(TrackHelpers::WorkerType masterWorkerIn,
                         unsigned workerIdIn)
        : OperatorsDemystifyWork("GroupByWork", masterWorkerIn, workerIdIn)
    {
    }
    virtual ~OperatorsGroupByWork() = default;

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    MemoryPile *groupMetaPile = NULL;
    XcalarEvalClass2Ast *asts = NULL;
    Xdb *scratchPadXdb = NULL;
    Scalar **results = NULL;

    GroupByDemystifySend *demystifyHandle = NULL;
    bool evalContextsValid = false;
    GroupEvalContext *evalContexts = NULL;
};

class OperatorsCreateScalarTableWork : public OperatorsDemystifyWork
{
  public:
    OperatorsCreateScalarTableWork(TrackHelpers::WorkerType masterWorkerIn,
                                   unsigned workerIdIn,
                                   bool useSrcKeyIn)
        : OperatorsDemystifyWork("CreateScalarTableWork",
                                 masterWorkerIn,
                                 workerIdIn)
    {
        useSrcKey = useSrcKeyIn;
    }
    virtual ~OperatorsCreateScalarTableWork() = default;

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    CreateScalarTableDemystifySend *demystifyHandle = NULL;
    bool useSrcKey;
    XcalarEvalClass1Ast *asts = NULL;
    TransportPage **scalarPages = NULL;
    TransportPageHandle *indexHandle;
};

class OperatorsProjectWork : public OperatorsWorkBase
{
  public:
    OperatorsProjectWork(TrackHelpers::WorkerType masterWorkerIn,
                         unsigned workerIdIn)
        : OperatorsWorkBase("ProjectWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    OpKvEntryCopyMapping *kvEntryCopyMapping;
    OpStatus *opStatus;
    XdbInsertKvState insertState;
};

class OperatorsSynthesizeDatasetWork : public OperatorsWorkBase
{
  public:
    OperatorsSynthesizeDatasetWork(TrackHelpers::WorkerType masterWorkerIn,
                                   unsigned workerIdIn)
        : OperatorsWorkBase("SynthesizeDatasetWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp() { return StatusOk; }
    virtual Status doWork();
    virtual void tearDown() {}

    DsDataset *dataset;
    OpStatus *opStatus;
    XdbLoadArgs *xdbLoadArgs;
    ParseArgs *parseArgs;

    int64_t startPageIdx;
    int64_t numPages;

    int64_t pagesPerWorker;
    int64_t lastChunkLen;
};

class OperatorsGetRowNumWork : public OperatorsWorkBase
{
  public:
    OperatorsGetRowNumWork(TrackHelpers::WorkerType masterWorkerIn,
                           unsigned workerIdIn)
        : OperatorsWorkBase("GetRowNumWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    // the starting row of the xdb on this node from
    // a global viewpoint
    uint64_t nodeRowStart;
    size_t rowNumIdx;
    OpStatus *opStatus;
    OpKvEntryCopyMapping *opKvEntryCopyMapping;
    XdbInsertKvState insertState;
};

class OperatorsEvalFnWork : public OperatorsWorkBase
{
  public:
    OperatorsEvalFnWork(TrackHelpers::WorkerType masterWorkerIn,
                        unsigned workerIdIn)
        : OperatorsWorkBase("EvalFnWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp() { return StatusOk; }
    virtual Status doWork();
    virtual void tearDown() {}

    ScalarGroupIter *groupIter;
    XcalarEvalRegisteredFn *registeredFn;
    OpStatus *opStatus;
};

class OperatorsAggFnWork : public OperatorsWorkBase
{
  public:
    OperatorsAggFnWork(TrackHelpers::WorkerType masterWorkerIn,
                       unsigned workerIdIn)
        : OperatorsWorkBase("AggFnWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp() { return StatusOk; }
    virtual Status doWork();
    virtual void tearDown() {}

    XdfAggregateAccumulators *aggAccs;
    bool *aggAccsValid;
    bool *accValid;
    XdfAggregateAccumulators *acc;
    XdfAggregateHandlers aggHandler;
    ScalarGroupIter *groupIter;
    OpStatus *opStatus;
};

class OperatorsJoinWork : public OperatorsWorkBase
{
  public:
    OperatorsJoinWork(TrackHelpers::WorkerType masterWorkerIn,
                      unsigned workerIdIn)
        : OperatorsWorkBase("JoinWork", masterWorkerIn, workerIdIn)
    {
    }
    virtual ~OperatorsJoinWork() = default;

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    MustCheck Status combineValueArrayAndInsert(NewKeyValueEntry *leftKvEntry,
                                                NewKeyValueEntry *rightKvEntry,
                                                NewKeyValueEntry *dstKvEntry);

    MustCheck int compareValueArray(XdbMeta *leftMeta,
                                    XdbMeta *rightMeta,
                                    NewKeyValueEntry *leftKvEntry,
                                    NewKeyValueEntry *rightKvEntry);

    MustCheck Status evalFilter(XdbMeta *leftMeta,
                                NewKeyValueEntry *leftKvEntry,
                                NewKeyValueEntry *rightKvEntry,
                                bool *filterResult);

    MustCheck Status initCursor(TableCursor *cursor,
                                bool *cursorExhausted,
                                bool *cursorInited,
                                const bool left);

    MustCheck Status checkLoopStatusAndUpdate(size_t *loopCounter,
                                              size_t *workCounter);

    MustCheck Status processEqual(TableCursor *leftCursor,
                                  TableCursor *rightCursor,
                                  XdbMeta *leftMeta,
                                  XdbMeta *rightMeta,
                                  NewKeyValueEntry *leftKvEntry,
                                  NewKeyValueEntry *rightKvEntry,
                                  NewKeyValueEntry *dstKvEntry,
                                  NewKeyValueEntry *leftStartKvEntry,
                                  TableCursor::Position *&rightStartPosition,
                                  TableCursor::Position *&rightTempPosition,
                                  bool *leftExhausted,
                                  bool *rightExhausted,
                                  AccumulatorHashTable *accHashTable,
                                  size_t *loopCounter,
                                  size_t *workCounter);

    XdbId leftXdbId;
    XdbId rightXdbId;

    int64_t numRecsLeft;
    int64_t numRecsRight;

    int64_t startRecordLeft;
    int64_t startRecordRight;

    JoinOperator joinType;
    bool nullSafe;

    OpStatus *opStatus;
    Xdb *leftXdb;
    Xdb *rightXdb;

    OpKvEntryCopyMapping *opKvEntryCopyMapping;
    unsigned numLeftCopyMappingEntries;
    unsigned numRightCopyMappingEntries;
    XdbInsertKvState insertState;

    EvalContext filterCtx;
};

class OperatorsCrossJoinWork : public OperatorsJoinWork
{
  public:
    OperatorsCrossJoinWork(TrackHelpers::WorkerType masterWorkerIn,
                           unsigned workerIdIn)
        : OperatorsJoinWork(masterWorkerIn, workerIdIn)
    {
    }
    virtual ~OperatorsCrossJoinWork() = default;

    Status doWork() override;

    MustCheck Status evalFilter(XdbMeta *dstMeta,
                                NewKeyValueEntry *dstKvEntry,
                                bool *filterResult);

    MustCheck Status workHelper(Xdb *broadcastXdb,
                                XdbMeta *broadcastMeta,
                                NewKeyValueEntry *broadcastKvEntry,
                                TableCursor *broadcastCursor,
                                Xdb *srcXdb,
                                XdbMeta *srcMeta,
                                NewKeyValueEntry *srcKvEntry,
                                TableCursor *srcCursor,
                                uint64_t startRecord,
                                uint64_t totalRecords,
                                NewKeyValueEntry *leftKvEntry,
                                NewKeyValueEntry *rightKvEntry);
};

class OperatorsUnionWork : public OperatorsWorkBase
{
  public:
    OperatorsUnionWork(TrackHelpers::WorkerType masterWorkerIn,
                       unsigned workerIdIn)
        : OperatorsWorkBase("UnionWork", masterWorkerIn, workerIdIn)
    {
    }

    virtual Status setUp();
    virtual Status doWork();
    virtual void tearDown();

    unsigned numSrcXdbs;
    Xdb **xdbs;
    XdbMeta **xdbMetas;

    XdbMeta *dstMeta;

    int64_t *numRecs = NULL;
    int64_t *startRecs = NULL;

    int **argMap;
    bool dedup;
    UnionOperator unionType;
    OpStatus *opStatus;

  private:
    Status doUnionWork();
    Status doUnionAllWork();
    Status applyArgMap(NewKeyValueEntry *srcKvEntry,
                       NewKeyValueEntry *dstKvEntry,
                       unsigned srcXdbIdx);

    MustCheck Status getNumInserts(uint64_t *numMin, uint64_t *inserts) const;
};

#endif  // _OPERATORSTYPES_H
