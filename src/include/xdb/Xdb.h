// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDB_H_
#define _XDB_H_

#include "primitives/Primitives.h"
#include "xdb/DataModelTypes.h"
#include "msg/MessageTypes.h"
#include "bc/BufferCache.h"
#include "operators/Dht.h"
#include "dag/DagTypes.h"
#include "util/IntHashTable.h"
#include "constants/XcalarConfig.h"
#include "util/SchedulableFsm.h"

// From LibApisCommon.h.
typedef struct XcalarApiTableMeta XcalarApiTableMeta;
typedef struct XcalarApiGetTableMetaOutput XcalarApiGetTableMetaOutput;

struct XdbLoadDoneInput;
union XdbAtomicHashSlot;
struct XdbHashSlotAug;
struct iovec;
struct XdbHashSlotInfo;

// The serialization/deserialization of entire tables is not currently
// being used except by the xdbStress func test.  In the future the
// infrastructure will allow use cases such as: serializing a table
// while modeling instead of dropping it in order to free memory.
//
// The current implementation only serializes/deserializes Xdb pages
// (aka demand paging) for batch dataflows.  All other uses, e.g.
// modeling, will get an out-of-resources error if Xdb pages are not
// allocatable.

class XdbMgr final
{
    friend class LibXdbGvm;
    friend class HashTree;
    friend class MergeMgr;
    friend class HashTreeMgr;
    friend class RehashHelper;
    friend class SortXdbHelper;
    friend class HashMergeSort;
    friend class XdbPgCursor;
    friend struct XdbPage;

  public:
    static constexpr unsigned BatchAllocSize = 32;
    static constexpr unsigned BatchFreeSize = 128;

#ifdef XDBPAGEHDRMAP
    struct BufCacheDbgToXdbHdrItem {
        IntHashTableHook hook;

        void *getKey() const { return keyPtr; }

        static uint64_t hashKey(void *k) { return (uint64_t) k; }

        void del() { delete this; }

        void *keyPtr;
        void *valPtr;
    };

    typedef IntHashTable<void *,
                         BufCacheDbgToXdbHdrItem,
                         &BufCacheDbgToXdbHdrItem::hook,
                         &BufCacheDbgToXdbHdrItem::getKey,
                         2049,
                         BufCacheDbgToXdbHdrItem::hashKey>
        BufCacheDbgToXdbHdrMap;

    BufCacheDbgToXdbHdrMap *bufCacheDbgToXdbHdrMap = NULL;
    Spinlock bufCacheDbgToXdbHdrMapLock;
#endif

    static constexpr uint64_t XdbHashSlotFlagNumBits = 4;
    static constexpr uint64_t XdbMinPageAlignment = 1ULL
                                                    << XdbHashSlotFlagNumBits;
    // XXX Number of XDB hash slots is bound by MsgMaxPayloadSize.
    // 2PC Msg2pcXcalarApiGetTableMeta stuffs all the XDB meta for all the slots
    // in a single payload.
    static constexpr uint64_t XdbHashSlotsDefault = 1021;
    static constexpr uint64_t XdbHashSlotsMin = 1;
    static constexpr uint64_t XdbHashSlotsMax = 1000000;
    static constexpr uint64_t MaxXdbs = 1024;

    static uint64_t xdbHashSlots;

    static MustCheck XdbMgr *get();

    static MustCheck Status init();

    void destroy();

    MustCheck Status xdbLoadDoneScratchXdb(Xdb *scratchXdb);

    static void xdbGetSlotMinMax(Xdb *xdb,
                                 uint64_t slotId,
                                 DfFieldType *type,
                                 DfFieldValue *min,
                                 DfFieldValue *max,
                                 bool *valid);

    // this copies over all min/max values, including the slot level
    void xdbCopyMinMax(Xdb *srcXdb, Xdb *dstXdb);

    void xdbResetDensityStats(Xdb *xdb);

    void xdbIncRecvTransPage(Xdb *xdb);

    MustCheck Status xdbGetPageDensity(Xdb *xdb,
                                       uint64_t *allocated,
                                       uint64_t *consumed,
                                       bool isPrecise);

    MustCheck Status xdbGetNumTransPages(Xdb *xdb,
                                         uint64_t *sent,
                                         uint64_t *received);
    static MustCheck XdbId xdbGetXdbId(Xdb *xdb);
    static SortedFlags xdbGetSortedFlags(Xdb *xdb, uint64_t slotId);
    static void xdbCopySlotSortState(Xdb *srcXdb, Xdb *dstXdb, uint64_t slotId);
    static void xdbSetSlotSortState(Xdb *xdb,
                                    uint64_t slotId,
                                    SortedFlags sortFlag);
    MustCheck size_t xdbGetSize(XdbId xdbId);

    static MustCheck XdbMeta *xdbGetMeta(Xdb *xdb);
    static unsigned getNumFields(XdbMeta *xdbMeta);
    static uint64_t getSlotStartRecord(Xdb *xdb, uint64_t slotId);

    static MustCheck size_t bcSize();
    enum class SlabHint {
        Default,
        OsPageable,
    };
    MustCheck void *bcAlloc(
        Xid txnId,
        Status *statusOut,
        SlabHint slabHint,
        BcHandle::BcScanCleanoutMode cleanoutMode =
            BcHandle::BcScanCleanoutNotToFree,
        BcHandle::BcPageType pageType = BcHandle::BcPageTypeBody);
    void bcFree(void *buf);
    void bcMarkCleanoutNotToFree(void *buf);
    Status bcScanCleanout(Xid txnId);
    MustCheck uint64_t bcBatchAlloc(void **bufs,
                                    uint64_t numBufs,
                                    size_t offset,
                                    Xid txnId,
                                    SlabHint slabHint,
                                    BcHandle::BcScanCleanoutMode cleanoutMode =
                                        BcHandle::BcScanCleanoutNotToFree);
    void bcBatchFree(void **bufs, uint64_t numBufs, size_t offset);

    MustCheck bool inRange(void *buf);

    // xdbCreate(), xdbDrop(), xdbGet(), and xdbInsertKey() are the most
    // commonly invoked functions.
    // if no fatptr prefixes are specified, dataset names will be used by
    // default.
    MustCheck Status xdbCreate(XdbId xdbId,
                               const char *keyName,
                               DfFieldType keyType,
                               int keyIndex,
                               const NewTupleMeta *tupMeta,
                               DsDatasetId datasetIds[],
                               unsigned numDatasetIds,
                               const char *immediateNames[],
                               unsigned numImmediates,
                               const char *fatptrPrefixNames[],
                               unsigned numFatptrs,
                               Ordering ordering,
                               XdbGlobalStateMask globalState,
                               DhtId dhtId);

    MustCheck Status xdbCreate(XdbId xdbId,
                               unsigned numKeys,
                               const char **keyName,
                               DfFieldType *keyType,
                               int *keyIndex,
                               Ordering *keyOrdering,
                               const NewTupleMeta *tupMeta,
                               DsDatasetId datasetIds[],
                               unsigned numDatasetIds,
                               const char *immediateNames[],
                               unsigned numImmediates,
                               const char *fatptrPrefixNames[],
                               unsigned numFatptrs,
                               XdbGlobalStateMask globalState,
                               DhtId dhtId);

    MustCheck Status xdbCreateScratchPadXdbs(Xdb *dstXdb,
                                             const char *keyName,
                                             DfFieldType keyType,
                                             const NewTupleMeta *tupMeta,
                                             char *immediateNames[],
                                             unsigned numImmediates);

    void xdbDrop(XdbId xdbId);

    void xdbDropSlot(Xdb *xdb, uint64_t slotId, bool locked);

    void pagePutRef(XdbId xdbId, int64_t slotId, XdbPage *page);
    void pagePutRef(Xdb *xdb, int64_t slotId, XdbPage *page);

    // StatusOk: when ptr to xdb is returned in xdbPtrReturned.
    // StatusXdbNotFound: Xdb not found. xdbPtrReturned is meaningless.
    // Note: This function is kept separate for a reason - once we get the xdb
    // we can repeatedly call xdbInsertKey() which is a workhorse function. We
    // cannot pay the price of getting the xdb from the xdb handle on every key
    // insert.
    MustCheck Status xdbGet(XdbId xdbId,
                            Xdb **xdbPtrReturned,
                            XdbMeta **xdbMeta);

    MustCheck Status xdbGetWithRef(XdbId xdbId,
                                   Xdb **xdbPtrReturned,
                                   XdbMeta **xdbMeta);
    static void xdbPutRef(Xdb *xdb);

    // Note: This is the only call that passes in xdb instead of xdbId
    // for speed of insertion. The xdb is guaranteed to not disappear
    // during load/insert because it is protected by the xdb->loadDone flag
    // which is set by the caller who must explicitly call xdbLoadDone()
    // after the last insert.
    MustCheck Status xdbInsertKv(Xdb *xdb,
                                 DfFieldValue *key,
                                 NewTupleValues *xdbValueArray,
                                 XdbInsertKvState xdbInsertKvState);

    MustCheck Status xdbInsertKvNoLock(Xdb *xdb,
                                       int slotId,
                                       DfFieldValue *key,
                                       NewTupleValues *xdbValueArray,
                                       XdbInsertKvState xdbInsertKvState);

    // Insert a given XDB page into XDB. Also given are the min key and max key
    // in the XDB page. Note that this provides random hashing of the Keys in
    // the XDB pge into XDB.
    MustCheck Status xdbInsertOnePage(Xdb *xdb,
                                      XdbPage *xdbPage,
                                      bool keyRangeValid,
                                      DfFieldValue minKeyInXdbPage,
                                      DfFieldValue maxKeyInXdbPage,
                                      bool takeRef = true);

    MustCheck Status xdbInsertKvIntoXdbPage(XdbPage *xdbPage,
                                            bool keyRangePreset,
                                            bool *keyRangeValid,
                                            DfFieldValue *minKey,
                                            DfFieldValue *maxKey,
                                            DfFieldValue *key,
                                            uint64_t *numTuples,
                                            NewTupleValues *xdbValueArray,
                                            NewKeyValueMeta *keyValueMeta,
                                            DfFieldType type);

    MustCheck Status xdbRehashXdbIfNeeded(Xdb *xdb,
                                          XdbInsertKvState xdbInsertKvState);

    // This function must be called before the xdb is used.
    // StatusOk: When numRows is returned.
    // StatusXdbNotFound: Unconcatenate to demystify.
    MustCheck Status xdbLoadDone(XdbId xdbId);

    MustCheck Status xdbLoadDoneSort(XdbId xdbId, Dag *dag, bool delaySort);

    void xdbLoadDoneLocal(MsgEphemeral *eph, void *payload);

    void xdbFreeScratchPadXdbs(Xdb *xdb);
    MustCheck Xdb *xdbGetScratchPadXdb(Xdb *xdb);

    void xdbReleaseScratchPadXdb(Xdb *scratchPadXdb);

    void xdbReinitScratchPadXdb(Xdb *scratchPadXdb);

    // Return count of total num of rows in a Xdb.
    // StatusOk: When numRows is returned.
    // StatusXdbNotFound: Xdb not found. numRows is 0.
    // StatusBusy: numRows is 0.
    Status xdbGetNumLocalRowsFromXdbId(XdbId xdbId, uint64_t *numRows);

    // Return size in bytes consumed by Xdb pages in a Xdb.
    // StatusOk: When size is returned.
    // StatusXdbNotFound: Xdb not found. size is 0.
    // StatusBusy: size is 0.
    Status xdbGetSizeInBytesFromXdbId(XdbId xdbId, uint64_t *size);

    // Return total count of numTransPageReceived
    // StatusOk: When numTransPageReceived is right
    // StatusXdbNotFound: Xdb not found. numTransPageReceived is 0
    Status xdbGetNumTransPagesReceived(XdbId xdbId, uint64_t *received);

    Status xdbGetHashSlotSkew(XdbId xdbId, int8_t *hashSlotSkew);

    static MustCheck uint64_t xdbGetNumLocalRows(Xdb *xdb);

    // XXX - Need clarification if done UI?
    MustCheck Status xdbGetXcalarApiTableMetaLocal(XdbId xdbId,
                                                   XcalarApiTableMeta *outMeta,
                                                   bool isPrecise);

    // XXX - Need clarification if done UI?
    MustCheck Status xdbGetXcalarApiTableMetaGlobal(
        XdbId xdbId, XcalarApiGetTableMetaOutput *outMeta);

    static MustCheck uint64_t xdbGetNumHashSlots(Xdb *xdb);
    static MustCheck uint64_t xdbGetNumRowsInHashSlot(Xdb *xdb,
                                                      uint64_t hashSlotNum);
    static MustCheck uint64_t xdbGetHashSlotStartRecord(Xdb *xdb,
                                                        uint64_t hashSlotNum);

    // Attempt to set key type. The true key type will be determined and set on
    // the Dlm node first, then returned to all other nodes.
    MustCheck Status xdbSetKeyType(XdbMeta *xdbMeta,
                                   DfFieldType newKeyType,
                                   unsigned keyNum,
                                   bool tryLock = false);

    // Add a page to the XDB chain of transport pages.  After all the pages
    // have been received, the KV entries will be hashed.
    // XXX - Important XDB interfaces and need to be read carefully. There
    // should be no need to block here.
    void xdbIncTransportPageCounter(Xdb *xdb);

    // These are microkernel private interfaces. Use xdbSetKeyType() instead.
    void xdbResolveKeyTypeDLM(MsgEphemeral *eph, void *payload);
    void xdbResolveKeyTypeDLMCompletion(MsgEphemeral *eph, void *payload);

    // This are uK private functions used primarily by sched.
    void xdbDropLocal(MsgEphemeral *eph, void *ignore);
    Status xdbDropLocalInternal(XdbId xdbId);

    MustCheck XdbPage *xdbAllocXdbPage(
        Xid txnId,
        Xdb *xdb,
        BcHandle::BcScanCleanoutMode cleanoutMode =
            BcHandle::BcScanCleanoutNotToFree);

    MustCheck uint64_t
    xdbAllocXdbPageBatch(XdbPage **xdbPages,
                         const uint64_t numBufs,
                         Xid txnId,
                         Xdb *xdb,
                         SlabHint slabHint,
                         BcHandle::BcScanCleanoutMode cleanoutMode =
                             BcHandle::BcScanCleanoutNotToFree);

    void xdbInitXdbPage(Xdb *xdb,
                        XdbPage *xdbPage,
                        XdbPage *nextXdbPage,
                        size_t pageSize,
                        XdbPageType pageType);

    void xdbInitXdbPage(NewKeyValueMeta *kvMeta,
                        XdbPage *xdbPage,
                        XdbPage *nextXdbPage,
                        size_t pageSize,
                        XdbPageType pageType);

    void xdbFreeXdbPage(XdbPage *xdbPage);

    void xdbFreeXdbPageBatch(XdbPage **xdbPages, const uint64_t numBufs);

    void xdbAppendPageChain(Xdb *xdb, uint64_t slot, XdbPage *firstPage);

    MustCheck static uint64_t getBcXdbLocalBufSize();

    static MustCheck XdbPage *getXdbHashSlotNextPage(
        XdbAtomicHashSlot *xdbHashSlot);

    static void lockSlot(XdbHashSlotInfo *slotInfo, uint64_t slotId);

    static void unlockSlot(XdbHashSlotInfo *slotInfo, uint64_t slotId);

    void setXdbHashSlotNextPage(XdbAtomicHashSlot *xdbHashSlot,
                                XdbId xdbId,
                                uint64_t slotId,
                                XdbPage *nextPage);

    static void clearSlot(XdbAtomicHashSlot *xdbHashSlot);

    MustCheck Status sortHashSlotEx(Xdb *xdb,
                                    uint64_t hashSlot,
                                    XdbPage **firstPageOut);

    MustCheck Status createCursorFast(Xdb *xdb,
                                      int64_t slotId,
                                      TableCursor *cur);

    static MustCheck DfFieldValue getHashDiv(DfFieldType keyType,
                                             const DfFieldValue *minKey,
                                             const DfFieldValue *maxKey,
                                             bool valid,
                                             uint64_t hashSlots);

    static MustCheck Status hashXdbFixedRange(DfFieldValue fieldVal,
                                              DfFieldType fieldType,
                                              uint64_t hashSlots,
                                              const DfFieldValue *minKey,
                                              DfFieldValue divisor,
                                              uint64_t *hashSlotOut,
                                              Ordering ordering);

    // Serialize/Deserialize entire XDB table
    MustCheck Status distributeSerDesWork(Xdb *xdb, bool serialize);

    MustCheck Status distributeSerDesWorkInt(Xdb *xdb, bool serialize);

    MustCheck Status xdbSerializeLocal(const XdbId xdbId);
    MustCheck Status xdbDeserializeLocal(const XdbId xdbId);
    MustCheck Status xdbSerDes(const XdbId xdbId, bool serialize);

    MustCheck Status xdbDeserialize(Xdb *xdb);

    MustCheck Status xdbSerialize(Xdb *xdb);

    // Serialize/Deserialize array of XDB pages.  Caller is responsible for
    // locking.
    MustCheck Status xdbSerializeVec(XdbId xdbId,
                                     const Xid batchId,
                                     XdbPage *xdbPages[],
                                     const size_t numPages);

    MustCheck Status xdbSerializeVec(Xdb *xdb,
                                     const Xid batchId,
                                     XdbPage *xdbPages[],
                                     const size_t numPages);

    MustCheck Status xdbDeserializeVec(XdbId xdbId,
                                       const Xid batchId,
                                       XdbPage *xdbPages[],
                                       const size_t numPages);

    MustCheck Status xdbDeserializeVec(Xdb *xdb,
                                       const Xid batchId,
                                       XdbPage *xdbPages[],
                                       const size_t numPages);

    MustCheck Status xdbGetSerDesFname(Xid batchId, char *buf, size_t sz);

    void xdbSerDesDropVec(Xid batchId,
                          XdbPage *xdbPages[],
                          const size_t numPages);

    MustCheck Status xdbDeserializeAllPages(XdbId xdbId);
    void xdbDeserializeAllPagesLocal(MsgEphemeral *eph, void *payload);

    MustCheck Status xdbDeserializeAllPages(Xdb *xdb);

    MustCheck static uint64_t hashXdbUniform(DfFieldValue fieldVal,
                                             DfFieldType fieldType,
                                             uint64_t hashSlots);

    MustCheck Status xdbInsertKvCommon(Xdb *xdb,
                                       DfFieldValue *key,
                                       NewTupleValues *xdbValueArray,
                                       XdbAtomicHashSlot *xdbHashSlot,
                                       XdbHashSlotAug *xdbHashSlotAug,
                                       uint64_t slotId);

    MustCheck Status xdbIoVecWork(Xid &batchId,
                                  struct iovec *iov,
                                  ssize_t iovcnt,
                                  bool isSer);

#ifdef BUFCACHESHADOW
    MustCheck void *xdbGetBackingBcAddr(void *buf);
#endif
    static void freeXdbMeta(XdbMeta *meta);

  private:
    static constexpr uint64_t SerializationMinSize = 256 * MB;
    static constexpr uint64_t XdbToSlotRatio = 32;
    static constexpr uint64_t XdbMinHashSlots = 1;
    static constexpr uint64_t serializedMagic = 0xfeedd27f838117d3ULL;
    static constexpr uint64_t serializedItemMagic = 0xdeaf9295e8e0f7c1ULL;
    static constexpr uint32_t maxPagingFileCount_ = 10000;
    // Used in hash slot skew calculation
    static const unsigned MultFactorForPrecision = 1000;
    static XdbMgr *instance;
    // Used for sanity checking xdbpages don't cross cluster generations
    static uint64_t defaultPagingIdent_;
    static char xdbSerPath_[XcalarApiMaxPathLen];

    StatGroupId statsGrpId_;
    StatHandle numSerializationFailures_;
    StatHandle numDeserializationFailures_;
    StatHandle numSerializedPages_;
    StatHandle numSerializedPagesHWM_;
    StatHandle numSerializedBatches_;
    StatHandle numSerializedBytes_;
    StatHandle numSerializedBytesHWM_;
    StatHandle numDeserializedPages_;
    StatHandle numDeserializedBatches_;
    StatHandle numDeserializedBytes_;
    StatHandle numSerDesDroppedPages_;
    StatHandle numSerDesDroppedBytes_;
    StatHandle numSerDesDroppedBatches_;
    StatHandle numSerDesDropFailures_;
    StatHandle numSerializationLockRetries_;
    StatHandle numXdbPageableSlabOom_;
    StatHandle numOsPageableSlabOom_;

    MustCheck Status xdbSerDesInit(void);
    MustCheck Status xdbSerDesInitSparse();

    static size_t getListNum(XdbPage *page)
    {
        return (((uint64_t) page) / sizeof(*page)) %
               XcalarConfig::get()->xdbSerDesParallelism_;
    }

    bool init_ = false;
    BcHandle *bcXdbPage_ = NULL;
    BcHandle *bcOsPageableXdbPage_ = NULL;
    BcHandle *bcXdbLocal_ = NULL;
    uint64_t totalXdbPages_ = 0;
    Mutex xdbRefCountLock_;

    class PageList
    {
      public:
        void lock(uint32_t lockNum)
        {
            assert(lockNum < XcalarConfig::XdbSerDesParallelismMax);
            listLock_[lockNum].lock();
        }
        void lock(XdbPage *page) { lock(XdbMgr::getListNum(page)); }

        bool trylock(uint32_t lockNum) { return listLock_[lockNum].tryLock(); }
        bool trylock(XdbPage *page)
        {
            return trylock(XdbMgr::getListNum(page));
        }

        void unlock(uint32_t lockNum)
        {
            assert(lockNum < XcalarConfig::XdbSerDesParallelismMax);
            listLock_[lockNum].unlock();
        }
        void unlock(XdbPage *page) { unlock(XdbMgr::getListNum(page)); }

        XdbPage *getHeadLocked(XdbPage *page)
        {
            return getHeadLocked(XdbMgr::getListNum(page));
        }
        XdbPage *getHeadLocked(uint32_t listNum)
        {
            assert(!listLock_[listNum].tryLock());
            return head_[listNum];
        }

        void add(XdbPage *page)
        {
            uint32_t listNum = XdbMgr::getListNum(page);
            lock(listNum);
            addLocked(listNum, page);
            unlock(listNum);
        }
        void addLocked(XdbPage *page)
        {
            addLocked(XdbMgr::getListNum(page), page);
        }
        void addLocked(uint32_t listNum, XdbPage *page);

        void remove(XdbPage *page)
        {
            uint32_t listNum = XdbMgr::getListNum(page);
            lock(listNum);
            removeLocked(listNum, page);
            unlock(listNum);
        }
        void removeLocked(XdbPage *page)
        {
            removeLocked(XdbMgr::getListNum(page), page);
        }
        void removeLocked(uint32_t listNum, XdbPage *page);

        uint64_t getListsSize();

      private:
        Mutex listLock_[XcalarConfig::XdbSerDesParallelismMax];
        XdbPage *head_[XcalarConfig::XdbSerDesParallelismMax] = {NULL};
        XdbPage *tail_[XcalarConfig::XdbSerDesParallelismMax] = {NULL};
        uint64_t size_[XcalarConfig::XdbSerDesParallelismMax] = {0};
    };

    PageList serializationList_;

    Mutex serializationListLock_[XcalarConfig::XdbSerDesParallelismMax];
    XdbPage *serializationListHead_[XcalarConfig::XdbSerDesParallelismMax] = {
        NULL};
    XdbPage *serializationListTail_[XcalarConfig::XdbSerDesParallelismMax] = {
        NULL};
    uint64_t serializationListSize_[XcalarConfig::XdbSerDesParallelismMax] = {
        0};
    size_t currSerializationListNum = 0;  // Racy

    Mutex pagingFileLock_;
    uint32_t pagingFileCount_ = 0;
    uint64_t nextPagingFileOffset_ = 0;
    uint32_t pagingFileBlockSize_ = 0;
    int pagingFileFds_[maxPagingFileCount_];
    uint64_t maxPagingFileSize_;

    MustCheck Status initInternal();

    void addSerializationElem(XdbPage *page);
    void removeSerializationElem(XdbPage *page);
    void removeSerializationElemLocked(XdbPage *page);
    uint64_t getSerializationListsSize();

    MustCheck Status serializeNext();

    Status pageToOrFromLocalFile(Xid &batchId,
                                 struct iovec *iov,
                                 ssize_t iovcnt,
                                 bool writingPage);
    Status openPagingFile(uint32_t fileNum);
    Status openNextPagingFile();
    Status batchIdToPageInfo(uint64_t batchId,
                             uint64_t &fileNum,
                             uint64_t &blockNum,
                             uint32_t &blockCount);
    MustCheck Status pageInfoToBatchId(uint64_t &batchId,
                                       uint64_t fileNum,
                                       uint64_t blockNum,
                                       uint32_t blockCount);
    Status deallocateBlock(int fd, uint64_t fileOffset, uint32_t blockSize);

    // XXX This needs to be a runtime reader/writer lock.
    pthread_rwlock_t xdbSlowHashTableLock_;
    unsigned xdbRefCount_ = 0;

    MustCheck Status xdbLoadDoneByXdbInt(Xdb *xdb, XdbLoadDoneInput *input);

    void xdbDropLocalActual(Xdb *xdb);

    MustCheck Status sortXdb(Xdb *xdb, XdbLoadDoneInput *input);

    MustCheck Status rehashXdb(Xdb *xdb, XdbInsertKvState xdbInsertKvState);

    static void xdbUpdateCounters(Xdb *xdb);

    void freeXdbPages(Xdb *xdb);

    void printArrayForDebug(void *arrIn, uint64_t count);

    void xdbInsertOnePageIntoHashSlot(Xdb *xdb,
                                      XdbPage *xdbPage,
                                      uint64_t hashSlot,
                                      bool keyRangeValid,
                                      DfFieldValue *minKeyInXdbPage,
                                      DfFieldValue *maxKeyInXdbPage);

    MustCheck Status xdbLoadDoneInt(XdbLoadDoneInput *xdbLoadDoneInput);

    static MustCheck Status cursorGetFirstPageInSlot(Xdb *xdb,
                                                     bool sort,
                                                     uint64_t slotId,
                                                     XdbPage **pageOut,
                                                     bool noLock,
                                                     bool noPageIn = false);

    MustCheck int tryFreeUnorderedPages(XdbAtomicHashSlot *xdbHashSlot,
                                        Xdb *xdb);

    static MustCheck bool onOrderingBoundary(XdbPage *xdbPage);

    MustCheck Status replicateXdbMetaData(Xdb *xdbSrc, Xdb *xdbDst);

    MustCheck Status xdbAllocLocal(void *payload);
    MustCheck XdbMeta *xdbAllocMeta(XdbId xdbId,
                                    DhtId dhtId,
                                    DhtHandle *dhtHandle,
                                    unsigned numKeys,
                                    const DfFieldAttrHeader *keyAttr,
                                    unsigned numDatasetIds,
                                    const DsDatasetId *datasetIds,
                                    unsigned numImmediates,
                                    const char **immediateNames,
                                    unsigned numFatptrs,
                                    const char **fatptrPrefixNames,
                                    const NewTupleMeta *tupMeta);

    void xdbFreeTupBuf(XdbPage *xdbPage, bool changeState = true);

    XdbPage *xdbAllocXdbPageHdr(Xid txnId,
                                Xdb *xdb,
                                BcHandle::BcScanCleanoutMode cleanoutMode);

    typedef struct XdbPageHdrBackingPage XdbPageHdrBackingPage;
    void xdbPutXdbPageHdr(XdbPageHdrBackingPage *backingPage);
    void xdbPutXdbPageHdr(XdbPage *pageHdr);

    Status xdbAllocTupBuf(XdbPage *xdbPage,
                          Xid txnId,
                          BcHandle::BcScanCleanoutMode cleanoutMode,
                          XdbPage::PageState pageState = XdbPage::Resident);

    void lockAllSlots(Xdb *xdb);
    void unlockAllSlots(Xdb *xdb);

    MustCheck uint64_t xdbCksumVec(const struct iovec *iov,
                                   const ssize_t count) const;
    MustCheck Status xdbWriteVec(Xid &batchId,
                                 struct iovec *iov,
                                 ssize_t iovcnt);
    MustCheck Status xdbReadVec(Xid &batchId,
                                struct iovec *iov,
                                ssize_t iovcnt);
    MustCheck Status xdbIoVec(Xid &batchId,
                              struct iovec *iov,
                              ssize_t iovcnt,
                              bool isSer);

    void reversePages(Xdb *xdb, uint64_t slotId);

    void xdbSetKeyTypeHelper(DfFieldType newKeyType, unsigned keyNum, Xdb *xdb);
    MustCheck void *bcAllocInternal(
        Xid txnId,
        Status *statusOut,
        BcHandle::BcScanCleanoutMode cleanoutMode =
            BcHandle::BcScanCleanoutNotToFree,
        BcHandle::BcPageType pageType = BcHandle::BcPageTypeBody);

    Status getRefHelper(XdbPage *page, XdbPage::PageState *pageState);

    static MustCheck uint64_t xdbGetHashSlotToInsertPage(Xdb *xdb);

    void xdbSetDensityStats(Xdb *xdb);

    MustCheck static XdbMeta *allocXdbMeta();

    void kickPagingOnThreshold();

    // Keep this private, use init instead
    XdbMgr() {}

    // Keep this private, use destroy instead
    ~XdbMgr() {}

    XdbMgr(const XdbMgr &) = delete;
    XdbMgr &operator=(const XdbMgr &) = delete;
};

#endif  // _XDB_H_
