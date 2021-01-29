// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BUFFERCACHE_H_
#define _BUFFERCACHE_H_

#include "stat/StatisticsTypes.h"
#include "constants/XcalarConfig.h"
#include "util/XcalarSysHelper.h"
#include "util/IntHashTableHook.h"
#include "util/IntHashTable.h"

#ifdef BUFCACHESHADOW
#include "hash/Hash.h"
#endif

enum class BufferCacheObjects : uint32_t {
    XcalarApis = 0,
    CalloutDispatch,
    TransPageSource,
    TransPageSerializationBuf,
    UkMsgNormal,
    UkMsgAlt,
    UkMsgAtomic64,
    UkMsgTwoPcPages,
    UkSchedObject,
    Dht,
    XcalarEvalOperatorNodes,
    XcalarEvalRegularNodes,
    XcalarEvalRegisteredFns,
    QueryEvalRunningElems,
    QueryEvalRunnableElems,
    QueryEvalCompletedElems,
    OptimizerQueryAnnotations,
    OptimizerQueryFieldsElt,
    ArchiveManifest,
    ArchiveFile,
    LocalMsgSmallMsg,
    LocalMsgRequestHandler,
    LocalMsgResponse,
    XdbLocal,
    XdbPage,
    XdbPageOsPageable,
    // Keep this as the last entry
    ObjectMax,
    ObjectIdUnknown = 0xfee1b01d
};

class BcObject;

// Buffer Cache Object Handle
class BcHandle
{
  public:
    // Does the user want bcScanCleanout services? We're using 1 bit to store
    // this in BcHdr bcScanCleanoutMode_, so this should not exceed 2 values
    typedef enum {
        BcScanCleanoutNotToFree = 0,
        BcScanCleanoutToFree = 1,
    } BcScanCleanoutMode;

    // We're using 1 bit to store this in BcHdr bcPageType_, so this should not
    // exceed 2 values
    typedef enum { BcPageTypeHdr = 0, BcPageTypeBody = 1 } BcPageType;

  public:
    MustCheck uint64_t getBufSize();

    MustCheck uint64_t getTotalSizeInBytes();

    MustCheck uint64_t getTotalBufs();

    // XXX Need to use templates here for the argument.
    static MustCheck BcHandle *create(BufferCacheObjects objectId);

    // XXX Used from unit tests. Need a way to assert this.
    // XXX May be there is a better way of exposing this to unit tests, but
    // that's for later.
    static MustCheck BcHandle *create(BcObject *object);

    static void destroy(BcHandle **bcHandle);

    void markCleanoutNotToFree(void *buf);

    // Allocate buffer size buffer stamped with an txnXid,
    // use XidInvalid if the buffer should not be cleaned up by txn
    MustCheck void *allocBuf(
        Xid txnXid,
        Status *statusOut = nullptr,
        BcScanCleanoutMode cleanoutMode = BcScanCleanoutNotToFree,
        BcPageType pageType = BcPageTypeBody);

    // Free/reuse this buffer
    void freeBuf(void *buf);

    MustCheck bool inRange(void *buf);

    // Allocate numBufs, storing them in array bufs where the pointer to the
    // allocated buffer is stored at (bufs[ii] + offset)
    // returns total number of allocations succeeded
    MustCheck uint64_t
    batchAlloc(void **bufs,
               uint64_t numBufs,
               size_t offset,
               Xid txnXid,
               BcScanCleanoutMode cleanoutMode = BcScanCleanoutNotToFree);

    // Free all buffers in array bufs
    void batchFree(void **bufs, uint64_t numBufs, size_t offset);

    // free all buffers associated with a txnXid
    void scan(Xid txnXidToFree, BcPageType pageType);

    MustCheck uint64_t getBufsUsed();
    MustCheck uint64_t getMemoryUsed();
    MustCheck uint64_t getMemAvailable();
    MustCheck uint64_t getBufsAvailable();

    // Init stats.
    MustCheck Status statsToInit();

#ifdef BUFCACHESHADOW
    MustCheck void *getBackingAddr(void *buf);
#endif

// To enable BUFCACHEPOISON, export BUFCACHEPOISON=true; build config && build
#ifdef BUFCACHEPOISON
    static uint32_t constexpr AllocPattern = 0xab;
    static uint32_t constexpr FreePattern = 0xef;
#endif

  private:
#ifdef BUFCACHETRACE
    static constexpr const uint32_t TraceSize = 16;
    static constexpr const uint32_t NumTraces = 4;
    static constexpr const uint32_t BtBuffer = 4096;
#endif

    // Manage individual BufferSize buffers.
    class BcHdr
    {
      public:
        enum {
            BcStateFree = 0,   // Buffer in free list
            BcStateAlloc = 1,  // Buffer is alloced
        };

        void *buf_ = NULL;  // BufSize buffer
        BcHdr *next_ = NULL;
        uint64_t bufState_ : 1;
        uint64_t bcScanCleanoutMode_ : 1;
        uint64_t bcPageType_ : 1;
        uint64_t reserved_ : 61;
        Xid txnXid = XidInvalid;

#ifdef BUFCACHETRACE
        struct Trace {
            bool isAlloc;
            int traceSize;
            void *trace[TraceSize];
        };

        Trace traces_[NumTraces];
        int idx_ = 0;
#endif
    };

#ifdef BUFCACHESHADOW
    struct BufCacheDbgVaItem {
        IntHashTableHook hook;

        void *getKey() const { return keyPtr; }

        static uint64_t hashKey(void *k) { return (uint64_t) k; }

        void del()
        {
            // XXX: Only used with XDB pages for now
            // munmap(keyPtr, XcalarConfig::get()->xdbPageSize_);
            delete this;
        }

        void *keyPtr;
        void *valPtr;
    };

    typedef IntHashTable<void *,
                         BufCacheDbgVaItem,
                         &BufCacheDbgVaItem::hook,
                         &BufCacheDbgVaItem::getKey,
                         2049,
                         BufCacheDbgVaItem::hashKey>
        BufCacheDbgVaMap;

    // Not all hash manipulations are protected by lock_ (eg calls to inRange
    // and/or getOrdinal from Xdb)
    Spinlock dbgHashLock_;

    // Map shadow addresses to the backing b$ addresses
    BufCacheDbgVaMap *shadowToBc_ = NULL;
    // Map backing b$ addresses to shadow addresses
    BufCacheDbgVaMap *bcToShadow_ = NULL;

#ifdef BUFCACHESHADOWRETIRED
    BufCacheDbgVaMap *retiredShadowToBc_ = NULL;
#endif  // BUFCACHESHADOWRETIRED

    static Status shadowInit(BcHandle *bcHandle);
    static void destroyShadow(BcHandle *bcHandle);
    MustCheck void *remapAddr(void *buf);
    MustCheck void *retireAddr(void *buf);
#endif  // BUFCACHESHADOW

    static constexpr const uint32_t InvalidMlockPct = (uint32_t)(-3);
    static constexpr const uint32_t InvalidMlockChunkPct = (uint32_t)(-4);
    unsigned mlockChunkPct_ = 0;
    unsigned mlockPct_ = 0;

    // Range of contiguous buffer cache memory from baseBuf to topBuf.
    void *baseBuf_ = NULL;
    void *topBuf_ = NULL;

    // Manage the free buffers.
    BcHdr *freeList_ = NULL;  // Singly linked-list of BcHdrs

    BcHdr *freeListSaved_ = NULL;  // Array of BcHdrs
    Spinlock lock_;

    // Remember if Slow warning was emitted. This happens when Malloc was done
    // for the first time.
    bool didSlowWarning_ = false;

    // Stats
    StatHandle elemSizeBytes_;      // Size of each allocation in bytes.
    StatHandle fastAllocs_;         // Fast alloc count.
    StatHandle fastFrees_;          // Fast free count.
    StatHandle fastHWM_;            // Fast allocs high water mark.
    StatHandle slowAllocs_;         // Slow alloc count.
    StatHandle slowFrees_;          // Slow free count.
    StatHandle slowHWM_;            // Slow allocs high water mark.
    StatHandle totMemBytes_;        // Total allocation excluding slow allocs.
    StatHandle mlockChunk_;         // mlock additional chunk
    StatHandle mlockChunkFailure_;  // mlock chunk failure

    // Opaque object related information
    BcObject *object_ = NULL;

    MustCheck static BcHandle *createInternal(BcObject *object);

    static void destroyInternal(BcHandle **bcHandle);

    MustCheck void *fastAlloc(Xid txnXid,
                              BcScanCleanoutMode cleanoutMode,
                              BcPageType pageType = BcPageTypeBody);
    void fastFree(void *buf);

    void mlockAdditionalChunk();
    void addElemsToFreeList(uint64_t startElem, uint64_t numElems);

    MustCheck uint64_t getOrdinal(void *buf);

    // Keep this private. Use bcCreate instead
    BcHandle() {}

    // Keep this private. Use bcDestroy instead
    virtual ~BcHandle() {}

    BcHandle(const BcHandle &) = delete;
    BcHandle &operator=(const BcHandle &) = delete;
};

// Manages the entire Buffer Cache module. There is just one instance of this
// in the entire system.
class BufferCacheMgr final
{
  public:
    // Minimum total memory size. We will expect this to be the minimum
    // supported configuration.
    // XXX Need a better default?
    static constexpr uint64_t MinTotalMem = 32 * GB;
    static constexpr const char *FileNamePattern = "%s-%x-bufCache";

    enum Type : uint8_t {
        TypeMin = 0,
        // XXX Place holder until all the users of Buf$ start using it's own
        // table.
        TypeNone,
        TypeUsrnode,    // usrnode
        TypeChildnode,  // childnode
        TypeXccli,      // xccli
    };

    static BufferCacheMgr *get();

    static MustCheck Status init(Type type, uint64_t bufCacheSize);

    void destroy();

    MustCheck Type getType();

    MustCheck Status initObjects(size_t objectCount,
                                 const BufferCacheObjects *bufferCacheObjects);

    void destroyObjects();

    MustCheck BcObject *getObject(BufferCacheObjects objectId);

    MustCheck size_t getObjectCount();

    void addHandle(BcHandle *handle, BufferCacheObjects objectId);
    void removeHandle(BufferCacheObjects objectId);

    // numLocalNodes: Number of instances of usrnode processes on this host
    // (physical hardware).
    // return Buffer cache memory size in Bytes (PageSize aligned).
    static constexpr const uint64_t InvalidSize = (uint64_t)(-1);
    static MustCheck uint64_t computeSize(unsigned numLocalNodes);

    static constexpr const uint64_t InvalidAddr = (uint64_t)(-7);
    MustCheck uint64_t getOffsetFromAddr(void *addr);

  private:
    static constexpr BufferCacheObjects bcUsrNode[] = {
        BufferCacheObjects::XcalarApis,
        BufferCacheObjects::CalloutDispatch,
        BufferCacheObjects::TransPageSource,
        BufferCacheObjects::TransPageSerializationBuf,
        BufferCacheObjects::UkMsgNormal,
        BufferCacheObjects::UkMsgAlt,
        BufferCacheObjects::UkMsgAtomic64,
        BufferCacheObjects::UkMsgTwoPcPages,
        BufferCacheObjects::UkSchedObject,
        BufferCacheObjects::Dht,
        BufferCacheObjects::QueryEvalRunningElems,
        BufferCacheObjects::QueryEvalRunnableElems,
        BufferCacheObjects::QueryEvalCompletedElems,
        BufferCacheObjects::OptimizerQueryAnnotations,
        BufferCacheObjects::OptimizerQueryFieldsElt,
        BufferCacheObjects::ArchiveManifest,
        BufferCacheObjects::ArchiveFile,
        BufferCacheObjects::LocalMsgSmallMsg,
        BufferCacheObjects::LocalMsgRequestHandler,
        BufferCacheObjects::LocalMsgResponse,
        BufferCacheObjects::XdbLocal,
        BufferCacheObjects::XdbPage,
        // XDB page pagable kvbuf should always be the last valid entry
        BufferCacheObjects::XdbPageOsPageable,
    };

    // "XdbPageOsPageable" Type should always be the last valid entry. This
    // allows us to use the BCMM optimally, since there is no upper bound on
    // how many elements XdbPage object can use. Note that this invariant
    // should not be broken unless there is a justification!
    static_assert(bcUsrNode[ArrayLen(bcUsrNode) - 1] ==
                      BufferCacheObjects::XdbPageOsPageable,
                  "bcUsrNode[ArrayLen(bcUsrNode) - 1] == "
                  "BufferCacheObjects::XdbPageOsPageable");

    static constexpr BufferCacheObjects bcChildNode[] = {
        BufferCacheObjects::CalloutDispatch,
        BufferCacheObjects::LocalMsgSmallMsg,
        BufferCacheObjects::LocalMsgRequestHandler,
        BufferCacheObjects::LocalMsgResponse,
    };

    static constexpr BufferCacheObjects bcXccli[] = {
        BufferCacheObjects::ArchiveManifest,
        BufferCacheObjects::ArchiveFile,
    };

    BcObject *objects_ = NULL;  // Collection of Buffer Cache objects.
    const BufferCacheObjects *bufferCacheObjectIds_ = NULL;
    size_t objectCount_ = 0;
    bool init_ = false;
    static BufferCacheMgr *instance;
    Type type_ = TypeMin;  // Buffer Cache type
    uint32_t numNodes_ = 0;

    // Manage deferred BC stats init.
    struct BcDeferredStats {
      public:
        BcHandle *bcHandle = NULL;
        struct BcDeferredStats *prev;
        struct BcDeferredStats *next;
    };
    BcHandle **bcHandle_ = NULL;

    MustCheck size_t getObjectIdx(BufferCacheObjects objectId);

    MustCheck Status initObjects();

    MustCheck Status initObject(BufferCacheObjects objectId);

    static MustCheck unsigned mlockedPercentOfMemory();

    static size_t getBufCachePct();

    BufferCacheMgr() {}

    ~BufferCacheMgr() {}

    BufferCacheMgr(const BufferCacheMgr &) = delete;
    BufferCacheMgr &operator=(const BufferCacheMgr &) = delete;
};

// XXX May be there is a better place to stick these in. But let's not waste
// our time over this. I will let evolution take it's course.
// XXX: Waiting for libapis c++ conversion
uint64_t getBcXcalarApisBufSize();

// XXX: Need Achive c++ conversion
uint64_t getBcArchiveManifestNumElems();
uint64_t getBcArchiveFileNumElems();

#endif  // _BUFFERCACHE_H_
