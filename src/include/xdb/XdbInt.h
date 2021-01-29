// Copyright 2013 = 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDBINT_H_
#define _XDBINT_H_

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "bc/BufferCache.h"
#include "df/DataFormatTypes.h"
#include "operators/Dht.h"
#include "xdb/DataModelTypes.h"
#include "operators/OperatorsTypes.h"
#include "util/IntHashTable.h"
#include "dag/DagTypes.h"
#include "runtime/Schedulable.h"
#include "util/TrackHelpers.h"
#include "util/MemoryPile.h"

enum { InvalidXdbPage = -1 };

// Bit field enums
enum SlotLock {
    SlotUnlocked = 0x0,
    SlotLocked = 0x1,
};

enum FreedPagesInSlotStatus {
    CannotFreePagesDueToActiveCursor = 0xffffffff,
};

union XdbAtomicHashSlot {
    // We reserve XdbHashSlotFlagNumBits lsb bits for flags by enforcing that
    // any xdb page size is aligned on a minimum 2^XdbHashSlotFlagNumBits
    // boundary
    XdbPage *nextPageUseInlinesToModify;
    struct {
        volatile uintptr_t casLock : 1;
        uintptr_t sortFlag : 1;
        uintptr_t serialized : 1;
        // no more room for any more bits
        uintptr_t nextPageMsbDoNotTouch : 64 - XdbMgr::XdbHashSlotFlagNumBits;
    };
} Aligned(8);

// XdbHashSlotAug augments XdbAtomicHashSlot with additional information for
// interactive work.
struct XdbHashSlotAug {
    uint64_t numRows = 0;
    uint64_t numPages = 0;
    uint64_t startRecord = 0;
    DfFieldValue minKey;
    DfFieldValue maxKey;
    bool keyRangeValid = false;
    Semaphore sem;
    Spinlock lock;

    XdbHashSlotAug() : sem(0)
    {
        memZero(&this->minKey, sizeof(DfFieldValue));
        memZero(&this->maxKey, sizeof(DfFieldValue));
    }
    ~XdbHashSlotAug() {}
};

struct XdbHashSlotInfo {
    uint64_t hashSlots = 0;
    XdbAtomicHashSlot *hashBase = NULL;
    XdbHashSlotAug *hashBaseAug = NULL;
};

struct ScratchPadShared {
    struct ScratchPad {
        Xdb *scratchXdb;
        bool used;
    };
    ScratchPad *scratchPads;
    unsigned numScratchPads;
};

struct ScratchPadInfo {
    unsigned scratchIdx;
    static constexpr unsigned ScratchPadOwnerIdx = 0xc001cafe;
    ScratchPadShared *scatchPadShared;
};

enum XdbDensityState {
    XdbDensityNotInit,
    XdbDensityInited,
    XdbDensityNeedUpdate,
};

struct XdbPageHdrPile {
    uint64_t count_ = 1;
    uint64_t nextOffset_ = 0;
    uint8_t buf_[0];
};

// Please note that total size of XdbPageHdrBackingPage must be a multiple of
// XdbMinPageAlignment
typedef struct XdbPageHdrBackingPage {
    Atomic64 count;
    size_t nextIdx;
    size_t maxElems;
    XdbPage *pageHdrs;
    uint64_t unused;
    uint8_t buf[0];  // Must be last
} XdbPageHdrBackingPage;

struct Xdb {
    Xdb(XdbId xdbIdIn, XdbGlobalStateMask globalStateIn)
    {
        xdbId = xdbIdIn;
        globalState = globalStateIn;
        atomicWrite64(&numTransPageSent, 0);
        atomicWrite64(&numTransPageRecv, 0);
        atomicWrite64(&refCnt, 0);

        keyRangePreset = (globalState == XdbLocal);

        maxKey.uint64Val = 0;
        minKey.uint64Val = UINT64_MAX;
        hashDivForRangeBasedHash.float64Val = 0.f;
    };

    XdbId xdbId;
    uint64_t numRows = 0;  // Number of tuples/records on this node
    uint64_t numPages = 0;

    XdbGlobalStateMask globalState;

    IntHashTableHook hook;
    XdbId getXdbId() const { return xdbId; };

    XdbMeta *meta = NULL;
    NewTupleMeta tupMeta;

    bool loadDone = false;
    bool isResident = true;

    Atomic64 numTransPageSent;
    Atomic64 numTransPageRecv;

    XdbDensityState densityState = XdbDensityNotInit;
    uint64_t bytesConsumed = 0;
    uint64_t bytesAllocated = 0;

    bool keyRangeValid = false;
    bool keyRangePreset;
    DfFieldValue maxKey;
    DfFieldValue minKey;
    DfFieldValue hashDivForRangeBasedHash;

    Mutex lock;
    Atomic64 refCnt;

    XdbHashSlotInfo hashSlotInfo;
    ScratchPadInfo *scratchPadInfo = NULL;

    // pageHdrBackingPage actually might out-live the Xdb.
    XdbPageHdrBackingPage *pageHdrBackingPage = NULL;
    Mutex pageHdrLock;
};

struct XdbAllocLocalParams {
    XdbId xdbId;
    DhtId dhtId;
    DhtHandle dhtHandle;

    XdbGlobalStateMask globalState;
    int64_t hashSlots;
    Ordering ordering;

    unsigned numKeys;
    DfFieldAttrHeader keyAttr[TupleMaxNumValuesPerRecord];
    NewTupleMeta tupMeta;
    unsigned numDatasetIds;
    DsDatasetId datasetIds[TupleMaxNumValuesPerRecord];
    unsigned numImmediates;
    char immediateNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
    unsigned numFatptrs;
    char fatptrPrefixNames[TupleMaxNumValuesPerRecord][DfMaxFieldNameLen + 1];
};

struct XdbLoadDoneInput {
    XdbId xdbId;
    bool trackProgress;
    bool delaySort;
    DagTypes::DagId dagId;
};

class RehashHelper : public Schedulable
{
  public:
    Xdb *srcXdb = NULL;
    Xdb *dstXdb = NULL;
    XdbInsertKvState xdbInsertKvState = XdbInsertRandomHash;
    static constexpr uint64_t WorkerIdInvalid = 0xbaadcafec001c0de;
    uint64_t workerId = WorkerIdInvalid;
    bool masterWorker = false;
    TrackHelpers *trackHelpers = NULL;
    NewKeyValueMeta kvMeta;

    virtual void run();
    virtual void done();

    static RehashHelper *setUp(Xdb *srcXdb,
                               Xdb *dstXdb,
                               XdbInsertKvState xdbInsertKvState,
                               uint64_t workerId,
                               TrackHelpers *trackHelpers);
    static void tearDown(RehashHelper **rehashHelper);

  private:
    // Keep this private. Use setUp instead.
    RehashHelper() : Schedulable("RehashHelper") {}

    // Keep this private. Use tearDown instead.
    virtual ~RehashHelper() {}

    struct XdbPageCacheEntry {
        XdbPage *xdbPage;
        uint64_t hashSlot;
        DfFieldValue minKey;
        DfFieldValue maxKey;
        bool keyRangeValid;
        XdbPageCacheEntry *next;
    };

    // The pageCache is a hashTable indexed by hashSlot % pageCacheSlots.
    // There can be at most pageCacheSize entries in the pageCache
    uint64_t maxPageCacheSize_ = 0;
    uint64_t pageCacheSize_ = 0;
    uint64_t pageCacheSlots_ = 0;
    XdbPageCacheEntry **pageCache_ = NULL;
    XdbMgr *xdbMgr_ = NULL;

    Status rehashOneXdbPage(XdbPage *xdbPage);

    Status allocPageCache(uint64_t slots, uint64_t size);
    void freePageCache();

    Status processPageCache(uint64_t hashSlot, NewKeyValueEntry *kvEntry);

    Status flushPageCache();

    // Sorry folks! No operator overloading allowed here.
    RehashHelper(const RehashHelper &) = delete;
    RehashHelper &operator=(const RehashHelper &) = delete;
};

#endif  // _XDBINT_H_
