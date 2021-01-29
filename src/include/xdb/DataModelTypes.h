// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAMODELTYPES_H_
#define _DATAMODELTYPES_H_

#include "xdb/TableTypes.h"
#include "dataset/DatasetTypes.h"
#include "df/DataFormatTypes.h"
#include "operators/Dht.h"
#include "runtime/Mutex.h"
#include "util/System.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "util/Atomics.h"
#include "bc/BufferCache.h"

// XDB provides the data model for Operators and supports below Hashing
// Functions.
// [1] Random Hashing:
// During KV insert stage from operators, the hashing is just random. Here the
// idea is that until the inserts are done, we need to be efficient in the
// organizing incoming KVs. Note that for filters and aggregrates, this is
// sufficient and there is no need to re-hash after KV inserts are done, since
// these require complete scan of the hash table anyway.
// [2] Standard Hashing:
// This is CRC based hashing. This is used during re-hash stage only. Note that
// operators like groupby and join require this partial sort order to prevent
// complete scan of XDB.
// [3] Range Hashing:
// This is used duing re-hash stage only. Note that operators like sort require
// complete sort order.
// [4] Slot Hash:
// The key is an int64 value that hashes to key % numSlots
enum XdbInsertKvState : uint8_t {
    XdbInsertRandomHash = 1,
    XdbInsertCrcHash,
    XdbInsertRangeHash,
    XdbInsertSlotHash
};

// Local Xdbs are used as temporary scratchpads
enum XdbGlobalStateMask : uint8_t {
    XdbLocal = 0x0,
    XdbGlobal = 0x1,
};

enum { XdbPgCursorFirstNonEmptySlot = -1 };

struct XdbPage;
struct TransportPage;

struct OpPagePoolNode {
    bool inUse;
    XdbPage *page;
    TransportPage **transPages;
    DfFieldType keyType;
    bool keyRangeValid;
    DfFieldValue minKey;
    DfFieldValue maxKey;

    OpPagePoolNode *next;
    OpPagePoolNode *listNext;
};

class TransportPageHandle;
struct Xdb;

struct OpLoadInfo {
    bool loadWasComplete;
    Xdb *dstXdb;
    Mutex lock;
    unsigned poolSize;
    unsigned totalAllocs;

    TransportPageHandle *transPageHandle;
    OpPagePoolNode *xdbPagePool;
    OpPagePoolNode *allNodesList;
};

struct OpInsertHandle {
    OpPagePoolNode *node;
    OpLoadInfo *loadInfo;

    // used for slot hash
    int slotId = -1;
    XdbInsertKvState insertState;
};

class SchedFsmTransportPage;

// XXX Need to do per-tuple-field structure indead of several
// TupleMaxNumValuesPerRecord members in this struct to get better
// memory savings from Sparse allocation.
struct XdbMeta {
    XdbMeta()
        : xdbId(XdbIdInvalid),
          dhtId(DhtInvalidDhtId),
          isScratchPadXdb(false),
          prehashed(false),
          numImmediates(0),
          numFatptrs(0)
    {
        atomicWrite32(&setKeyTypeInProgress, 0);
    }

    unsigned getNumFields() const
    {
        return kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
    }

    DfFieldType getFieldType(const char *name)
    {
        for (unsigned ii = 0; ii < getNumFields(); ii++) {
            if (strcmp(name, kvNamedMeta.valueNames_[ii]) == 0) {
                return kvNamedMeta.kvMeta_.tupMeta_->getFieldType(ii);
            }
        }

        return DfUnknown;
    }

    XdbId xdbId = XdbIdInvalid;
    DhtId dhtId = DhtInvalidDhtId;
    DhtHandle dhtHandle;

    unsigned numKeys;

    DfFieldAttrHeader keyAttr[TupleMaxNumValuesPerRecord];
    // this array is just the valueArrayIdx portion of the keyAttrs,
    // used as an argument into DataFormat::fieldCompare
    int keyIdxOrder[TupleMaxNumValuesPerRecord];
    // this array is just the ordering portion of the keyAttrs,
    // used as an argument into DataFormat::fieldCompare
    Ordering keyOrderings[TupleMaxNumValuesPerRecord];
    bool keyTypeSet[TupleMaxNumValuesPerRecord];

    bool isScratchPadXdb = false;
    bool prehashed = false;
    OpLoadInfo loadInfo;
    unsigned numImmediates = 0;
    unsigned numFatptrs = 0;

    Mutex setKeyMutex;
    Mutex setKeyTypeLock;
    Atomic32 setKeyTypeInProgress;

    // XXX Hack so transport page processing fibers don't get suspended on
    // setKeyType. Needs to be reworked.
    SchedFsmTransportPage *schedFsmList = NULL;
    uint64_t schedFsmListSize = 0;
    uint64_t schedFsmPendingCount = 0;

    void processQueuedSchedFsmTp();

    NewKeyValueNamedMeta kvNamedMeta;

    unsigned numDatasets = 0;
    DsDataset *datasets[TupleMaxNumValuesPerRecord];
};  // this augments & replaces existing OperatorsMeta

// Don't use the first bit since that's reserved for sortedness
// XdbPageIsTransport denotes pages that comes from the transport buf$.
// XdbPageIsNormal denotes pages that come from the Xdb buf$.
// XdbPageEmbedded denotes pages that are not explicitly allocated
enum XdbPageFlags : uint8_t {
    XdbPageNormalFlag = 0x2,
    XdbPageEmbeddedFlag = 0x4,
};

// SortedFlag is 0x1, UnsortedFlag is 0x0
enum XdbPageType : uint8_t {
    XdbAllocedNormal = XdbPageNormalFlag,

    XdbUnsortedNormal = XdbPageNormalFlag | UnsortedFlag,
    XdbSortedNormal = XdbPageNormalFlag | SortedFlag,

    XdbSortedEmbedded = XdbPageEmbeddedFlag | SortedFlag,
};

struct XdbPageHdr {
    XdbPage *nextPage;
    XdbPage *prevPage;
    Atomic64 refCount;
    size_t pageSize;
    Atomic32 pageState;
    uint32_t numRows = 0;
    XdbPageType xdbPageType;
    bool isCompressed = false;
#ifdef DEBUG
    bool isInit = false;
#endif
};

class NewTuplesBuffer;

struct XdbPage {
    enum PageState : uint8_t {
        Resident,
        Serializable,
        Serializing,
        Serialized,
        Deserializing,
        Dropped,
    };

    XdbPageHdr hdr;

    // fields needed for demand paging and compression
    XdbPage *pageListNext = NULL;
    XdbPage *pageListPrev = NULL;
    Mutex pageRefLock;

    // fields needed for bcScanCleanout(pageHdr)
    Xid txnId = XidInvalid;
    BcHandle::BcScanCleanoutMode cleanoutMode =
        BcHandle::BcScanCleanoutNotToFree;

    XdbId xdbId = XdbIdInvalid;
    int64_t slotId = -1;

    union {
        NewTuplesBuffer *tupBuf;
        Xid serializedXid;
    };

    MustCheck Status getRef(Xdb *xdb);
    void dropKvBuf();

    Status insertKv(const NewKeyValueMeta *kvMeta, NewTupleValues *valueArray);
    Status getFirstKey(Xdb *xdb, DfFieldValue *keyOut, bool *keyValid);
    Status getLastKey(Xdb *xdb, DfFieldValue *keyOut, bool *keyValid);
};

struct XdbPageBatch {
    DfFieldValue min;
    bool minValid = false;
    DfFieldValue max;
    bool maxValid = false;

    unsigned numPages = 0;
    XdbPage *start = NULL;
    XdbPage *end = NULL;

    Xdb *xdb = NULL;
    int startSlot = -1;

    void init(XdbPage *startPage,
              XdbPage *endPage,
              unsigned numPagesIn,
              Xdb *xdbIn,
              unsigned startSlotIn)
    {
        start = startPage;
        end = endPage;

        numPages = numPagesIn;
        startSlot = startSlotIn;
        xdb = xdbIn;

        Status status = startPage->getFirstKey(xdb, &min, &minValid);
        assert(status == StatusOk);

        status = endPage->getLastKey(xdb, &max, &maxValid);
        assert(status == StatusOk);
    }
};

struct XdbItemSerializedHdr {
    // Serilaization is not allowed across instances, so this pointer should
    // always remain valid
    XdbPage *xdbPage;
    size_t bufferStartOffset;
    size_t bufferEndOffset;
};

struct XdbBatchSerializedHdr {
    uint64_t magic;
    uint64_t cksum;
    uint64_t randIdent;
    Xid batchId;
    XdbId xdbId;
    uint64_t numPages;
    XdbItemSerializedHdr itemHdrs[0];
};

class XdbPgCursor
{
  public:
    typedef XdbPage XdbPageType;

    Xdb *xdb_ = NULL;
    int64_t slotId_ = 0;
    XdbPage *xdbPage_ = NULL;

    enum Flags {
        None = 0,
        Sorted = 1,
        CanCrossSlotBoundary = 1 << 1,
        NoLocking = 1 << 2
    };

    uint32_t flags_ = 0;

    XdbPgCursor() {}
    ~XdbPgCursor();

    XdbPgCursor(Xdb *xdb, int64_t slotId, XdbPage *xdbPage, uint32_t flags)
    {
        xdb_ = xdb;
        slotId_ = slotId;
        xdbPage_ = xdbPage;
        flags_ = flags;
    }

    Status init(XdbId xdbId,
                Ordering ordering = Unordered,
                int64_t slotId = XdbPgCursorFirstNonEmptySlot);

    Status init(Xdb *xdb,
                Ordering ordering = Unordered,
                int64_t slotId = XdbPgCursorFirstNonEmptySlot,
                bool noSlotLock = false);

    Status getNextPage(XdbPage **xdbPageOut);
    Status getNextTupBuf(NewTuplesBuffer **tupBufOut);

    Status seekToRow(NewTuplesBuffer **tupBufOut,
                     uint64_t *startRowOut,
                     uint64_t finalRow);

    Status cleanupUnorderedPages();
};

struct XdbTableAndKey {
    XdbId xdbId;
    unsigned keyNum;

    DfFieldType newKeyType;
    Status status;
};

#endif  // _DATAMODELTYPES_H_
