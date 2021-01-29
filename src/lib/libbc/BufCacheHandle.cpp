// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#ifdef XLR_VALGRIND
#include <valgrind/valgrind.h>
#endif

#include "StrlFunc.h"        // strlcat, strlcpy
#include "strings/String.h"  // strMatch

#include "bc/BufCacheMemMgr.h"
#include "bc/BufCacheObjectMgr.h"
#include "bc/BufferCache.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "xdb/Xdb.h"
#include "xdb/XdbInt.h"  // Joys of bad abstraction...
#include "util/SaveTrace.h"

#ifdef BUFCACHESHADOW
#include <sys/mman.h>
#endif

#ifdef BUFCACHETRACE
#include <execinfo.h>
#endif

static constexpr const char *moduleName = "libbc";

uint64_t
BcHandle::getBufSize()
{
    return object_->getBufferSize();
}

uint64_t
BcHandle::getOrdinal(void *buf)
{
#ifdef BUFCACHESHADOW
    buf = getBackingAddr(buf);
#endif

#ifdef XLR_VALGRIND
    uint64_t ordinal = (uint64_t)(
        (((uintptr_t) buf - object_->getBufferSize()) - (uintptr_t) baseBuf_) /
        object_->vgBufferSize());
#else   // XLR_VALGRIND
    uint64_t ordinal = (uint64_t)(((uintptr_t) buf - (uintptr_t) baseBuf_) /
                                  object_->getBufferSize());
#endif  // XLR_VALGRIND
    return ordinal;
}

bool
BcHandle::inRange(void *buf)
{
    uint64_t ordinal = getOrdinal(buf);
    if (ordinal < object_->getNumElements()) {
        return true;
    } else {
        return false;
    }
}

uint64_t
BcHandle::getTotalSizeInBytes()
{
    return object_->getObjectSize();
}

uint64_t
BcHandle::getTotalBufs()
{
    return object_->getNumElements();
}

BcHandle *
BcHandle::create(BufferCacheObjects objectId)
{
    assert(objectId < BufferCacheObjects::ObjectMax);
    BcObject *object = BufferCacheMgr::get()->getObject(objectId);
    BcHandle *handle = createInternal(object);
    if (handle != NULL) {
        BufferCacheMgr::get()->addHandle(handle, objectId);
    }
    return handle;
}

BcHandle *
BcHandle::create(BcObject *object)
{
    assert(((BcObject *) object)->getObjectId() ==
           BufferCacheObjects::ObjectIdUnknown);
    // XXX - We don't track unregistered or unknown object handles yet.
    return createInternal(object);
}

void
BcHandle::destroy(BcHandle **bcHandle)
{
    BufferCacheObjects objectId =
        ((BcObject *) (*bcHandle)->object_)->getObjectId();
    if (objectId != BufferCacheObjects::ObjectIdUnknown) {
        BufferCacheMgr::get()->removeHandle(objectId);
    }
    destroyInternal(bcHandle);
}

uint64_t
BcHandle::getBufsUsed()
{
    uint64_t numBufs;
    lock_.lock();
    numBufs = this->fastAllocs_ - this->fastFrees_;
    lock_.unlock();
    return numBufs;
}

uint64_t
BcHandle::getMemoryUsed()
{
    return getBufsUsed() * getBufSize();
}

uint64_t
BcHandle::getBufsAvailable()
{
    return getTotalBufs() - getBufsUsed();
}

uint64_t
BcHandle::getMemAvailable()
{
    return getBufsAvailable() * getBufSize();
}

void *
BcHandle::fastAlloc(Xid txnXid,
                    BcScanCleanoutMode cleanoutMode,
                    BcPageType pageType)
{
    assert(!lock_.tryLock());

    if (unlikely(!freeList_)) {
        if (unlikely(mlockPct_ < 100)) {
            mlockAdditionalChunk();
        }

        if (unlikely(!freeList_)) {
            return NULL;
        }
    }

    assert(freeList_ != NULL);
    void *retBuf;

    BcHdr *allocHdr = freeList_;
    freeList_ = freeList_->next_;

    assert(allocHdr->bufState_ == BcHdr::BcStateFree);
    allocHdr->bufState_ = BcHdr::BcStateAlloc;
    allocHdr->txnXid = txnXid;
    allocHdr->bcScanCleanoutMode_ = cleanoutMode;
    allocHdr->bcPageType_ = pageType;
    retBuf = allocHdr->buf_;

#ifdef BUFCACHEPOISON
    memset(retBuf, AllocPattern, this->object_->getBufferSize());
#endif  // BUFCACHEPOISON

#ifdef XLR_VALGRIND
    VALGRIND_MALLOCLIKE_BLOCK(retBuf,
                              this->object->getBufferSize(),
                              this->object->getBufferSize(),
                              0);
#endif

#ifdef BUFCACHETRACE
    int size;
    size = getBacktrace(allocHdr->traces_[allocHdr->idx_].trace, TraceSize);
    allocHdr->traces_[allocHdr->idx_].isAlloc = true;
    allocHdr->traces_[allocHdr->idx_].traceSize = size;

    allocHdr->idx_++;
    if (allocHdr->idx_ == NumTraces) {
        allocHdr->idx_ = 0;
    }
#endif  // BUFCACHETRACE

    StatsLib::statNonAtomicIncr(this->fastAllocs_);

#ifdef BUFCACHESHADOW
    if (pageType != BcHandle::BcPageTypeHdr) {
        retBuf = remapAddr(retBuf);
    }
#endif

    if (XcalarConfig::get()->bufCacheHdrDumpInCores_ &&
        pageType == BcPageTypeHdr &&
        (object_->getInitFlag() & BcObject::InitDontCoreDump)) {
        // By right this doesn't core-dump. But we want to make an exception for
        // PageHdrs
        verifyOk(memMarkDoCoreDump(retBuf, object_->getBufferSize()));
    }

    if (likely(retBuf)) {
        if (XcalarConfig::get()->ctxTracesMode_ &&
            (object_->getObjectId() == BufferCacheObjects::XdbPageOsPageable ||
             object_->getObjectId() == BufferCacheObjects::XdbPage)) {
            saveTrace(TraceOpts::MemInc, retBuf);
        }
    }
    return retBuf;
}

void
BcHandle::fastFree(void *buf)
{
    assert(!lock_.tryLock());

#ifdef XLR_VALGRIND
    VALGRIND_FREELIKE_BLOCK(buf, this->object->getBufferSize());
#endif  // XLR_VALGRIND

    uint64_t ordinal = getOrdinal(buf);
    assert(ordinal < object_->getNumElements());

#ifdef BUFCACHEPOISON
    memset(buf, FreePattern, object_->getBufferSize());
#endif  // BUFCACHEPOISON

    BcHdr *savedFreeList = freeList_;
    assert(savedFreeList == NULL ||
           savedFreeList->bufState_ == BcHdr::BcStateFree);
    assert(freeListSaved_[ordinal].bufState_ == BcHdr::BcStateAlloc);

    freeList_ = &freeListSaved_[ordinal];
    assert(freeList_->buf_ == buf);
    freeList_->bufState_ = BcHdr::BcStateFree;
    freeList_->next_ = savedFreeList;

    if (XcalarConfig::get()->bufCacheHdrDumpInCores_ &&
        (object_->getInitFlag() & BcObject::InitDontCoreDump) &&
        freeList_->bcPageType_ == BcPageTypeHdr) {
        // This has been marked to core dump in fastAlloc, so we need to re-mark
        // it not to core dump since minidump is turned on
        verifyOk(memMarkDontCoreDump(buf, object_->getBufferSize()));
    }

#ifdef BUFCACHETRACE
    int size;
    BcHdr *freeHdr = &this->freeListSaved_[ordinal];
    size = getBacktrace(freeHdr->traces_[freeHdr->idx_].trace, TraceSize);
    freeHdr->traces_[freeHdr->idx_].isAlloc = false;
    freeHdr->traces_[freeHdr->idx_].traceSize = size;

    freeHdr->idx_++;
    if (freeHdr->idx_ == NumTraces) {
        freeHdr->idx_ = 0;
    }
#endif  // BUFCACHETRACE

    StatsLib::statNonAtomicIncr(this->fastFrees_);
    if (XcalarConfig::get()->ctxTracesMode_ &&
        (object_->getObjectId() == BufferCacheObjects::XdbPageOsPageable ||
         object_->getObjectId() == BufferCacheObjects::XdbPage)) {
        saveTrace(TraceOpts::MemDec, buf);
    }
}

// returns number of bufs allocated
uint64_t
BcHandle::batchAlloc(void **bufs,
                     uint64_t numBufs,
                     size_t offset,
                     Xid txnXid,
                     BcScanCleanoutMode cleanoutMode)
{
    uint64_t numAllocs;
    lock_.lock();

    for (numAllocs = 0; numAllocs < numBufs; numAllocs++) {
        void *buf = fastAlloc(txnXid, cleanoutMode);
        if (unlikely(buf == NULL)) {
            goto CommonExit;
        }

        if (offset == 0) {
            bufs[numAllocs] = buf;
        } else {
            assert(bufs[numAllocs] != NULL);
            *(void **) ((size_t) bufs[numAllocs] + offset) = buf;
        }
    }

CommonExit:
    lock_.unlock();
    return numAllocs;
}

void
BcHandle::batchFree(void **bufs, uint64_t numBufs, size_t offset)
{
    lock_.lock();
    for (uint64_t ii = 0; ii < numBufs; ii++) {
        void *buf;
        if (offset == 0) {
            buf = bufs[ii];
        } else {
            buf = *(void **) ((size_t) bufs[ii] + offset);
        }
        assert(buf != NULL);
        fastFree(buf);
    }

    lock_.unlock();
}

void
BcHandle::markCleanoutNotToFree(void *buf)
{
    BcHdr *hdr;

    lock_.lock();

    uint64_t ordinal = getOrdinal(buf);
    assert(ordinal < object_->getNumElements());
    hdr = &freeListSaved_[ordinal];
    assert(hdr->txnXid != XidInvalid);
    assert(hdr->bcScanCleanoutMode_ == BcScanCleanoutToFree);
    assert(hdr->bufState_ == BcHdr::BcStateAlloc);
    hdr->bcScanCleanoutMode_ = BcScanCleanoutNotToFree;
    lock_.unlock();
}

void
BcHandle::addElemsToFreeList(uint64_t startElem, uint64_t numElems)
{
    BcHdr *bcHdr = freeListSaved_;
    BcHdr *freeListTmp = NULL;
    uint64_t ii;
    freeListTmp = &bcHdr[startElem];

    for (ii = startElem; ii < (startElem + numElems) - 1; ++ii) {
#ifdef BUFCACHETRACE
        freeListTmp->idx_ = 0;
#endif

#ifdef XLR_VALGRIND
        // Split each buffer into thirds.  The first third is a redzone, the
        // middle third is the usable memory (the part we're interested in
        // for this initialization), and the final third is another redzone.
        freeListTmp->buf =
            (void *) ((uintptr_t) baseBuf_ + (object_->vgBufferSize() * ii +
                                              object_->getBufferSize()));
#else
        freeListTmp->buf_ =
            (void *) ((uintptr_t) baseBuf_ + (object_->getBufferSize() * ii));
#endif
        freeListTmp->next_ = &bcHdr[ii + 1];
        freeListTmp->txnXid = XidInvalid;
        freeListTmp->bufState_ = BcHdr::BcStateFree;
        freeListTmp = freeListTmp->next_;
    }
#ifdef XLR_VALGRIND
    freeListTmp->buf =
        (void *) ((uintptr_t) baseBuf_ +
                  (object_->vgBufferSize() * ii + object_->getBufferSize()));
#else
    freeListTmp->buf_ =
        (void *) ((uintptr_t) baseBuf_ + (object_->getBufferSize() * ii));
#endif
    freeListTmp->next_ = freeList_;
    freeListTmp->txnXid = XidInvalid;
    freeListTmp->bufState_ = BcHdr::BcStateFree;

    freeList_ = &bcHdr[startElem];
}

void
BcHandle::mlockAdditionalChunk()
{
    StatsLib::statNonAtomicIncr(this->mlockChunk_);

    assert(mlockPct_ != InvalidMlockPct);

    Status status;
    uintptr_t baseAddr = (uintptr_t) baseBuf_;
    uint64_t elemsAdded = (object_->getNumElements() * mlockPct_) / 100;
    uintptr_t bytesLocked = elemsAdded * object_->getBufferSize();
    uint64_t elemsToAdd;
    if (mlockPct_ + mlockChunkPct_ >= 100) {
        // lock the rest
        elemsToAdd = object_->getNumElements() - elemsAdded;
    } else {
        elemsToAdd = (object_->getNumElements() * mlockChunkPct_) / 100;
    }

    uintptr_t bytesToLock = elemsToAdd * object_->getBufferSize();

    void *mapAddr = (void *) (baseAddr + bytesLocked);

    if (BufferCacheMgr::get()->getType() == BufferCacheMgr::TypeUsrnode) {
        int ret = mlock(mapAddr, bytesToLock);
        if (ret != 0) {
            StatsLib::statNonAtomicIncr(this->mlockChunkFailure_);
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "SHM mlock bc %s failed: %s",
                    object_->getObjectName(),
                    strGetFromStatus(status));
        } else {
            addElemsToFreeList(elemsAdded, elemsToAdd);
            mlockPct_ += mlockChunkPct_;
        }
    } else {
        // if we aren't usrnode, don't bother mlocking
        addElemsToFreeList(elemsAdded, elemsToAdd);
        mlockPct_ += mlockChunkPct_;
    }
}

void *
BcHandle::allocBuf(Xid txnXid,
                   Status *statusOut,
                   BcScanCleanoutMode cleanoutMode,
                   BcPageType pageType)
{
    Status status = StatusOk;

    void *retBuf = NULL;
    bool emitSlowAllocWarning = false;
    bool includeInCoreDump = false;

    lock_.lock();

    // First attempt fast alloc.
    retBuf = fastAlloc(txnXid, cleanoutMode, pageType);
    if (retBuf != NULL) {
        this->fastHWM_->statUint64 =
            MAX(this->fastHWM_->statUint64,
                this->fastAllocs_->statUint64 - this->fastFrees_->statUint64);
        lock_.unlock();
        goto CommonExit;
    }

    if (object_->getInitFlag() & BcObject::InitFastAllocsOnly) {
        lock_.unlock();
        assert(retBuf == NULL);
        // Slow allocs are not allowed
        status = StatusNoXdbPageBcMem;
        goto CommonExit;
    }

    // Fast alloc fails! Fall back to slow alloc.
    assert(object_->getInitFlag() & BcObject::InitSlowAllocsOk);
    StatsLib::statNonAtomicIncr(this->slowAllocs_);
    this->slowHWM_->statUint64 =
        MAX(this->slowHWM_->statUint64,
            this->slowAllocs_->statUint64 - this->slowFrees_->statUint64);
    if (!this->didSlowWarning_ &&
        StatsLib::statReadUint64(this->slowAllocs_) == 1 &&
        !(object_->getInitFlag() & BcObject::InitFastAllocsOnly)) {
        emitSlowAllocWarning = true;
        this->didSlowWarning_ = true;
    }

    lock_.unlock();

    if (emitSlowAllocWarning) {
        xSyslog(moduleName,
                XlogWarn,
                "%s degraded to slow "
                "allocations",
                object_->getObjectName());
    }

    if (object_->alignedAllocs()) {
        retBuf = memAllocAlignedExt(object_->getAlignment(),
                                    object_->getBufferSize(),
                                    moduleName);
    } else {
        retBuf = memAllocExt(object_->getBufferSize(), moduleName);
    }

    if (XcalarConfig::get()->bufCacheHdrDumpInCores_ &&
        pageType == BcPageTypeHdr) {
        includeInCoreDump = true;
    }

    if (retBuf == NULL) {
        lock_.lock();
        StatsLib::statNonAtomicDecr(this->slowAllocs_);
        lock_.unlock();
        status = StatusNoMem;
    } else if ((object_->getInitFlag() & BcObject::InitDontCoreDump) &&
               !includeInCoreDump) {
        verifyOk(memMarkDontCoreDump(retBuf, object_->getBufferSize()));
    }

#ifdef BUFCACHEPOISON
    if (retBuf) {
        // We don't discriminate fast or slow paths here. The intention is
        // to catch Buf$ use after free culprits. Init buffer with alloc
        // pattern.
        memset(retBuf, AllocPattern, object_->getBufferSize());
    }
#endif  // BUFCACHEPOISON
CommonExit:
    if (statusOut)
        *statusOut = status;
    return retBuf;
}

void
BcHandle::freeBuf(void *buf)
{
    if (buf == NULL) {
        return;
    }

    lock_.lock();

#ifdef BUFCACHESHADOW
    buf = retireAddr(buf);
#endif

    if ((buf >= baseBuf_) && (buf < topBuf_)) {
        // Fast path
        fastFree(buf);
        lock_.unlock();
    } else {
        assert(object_->getInitFlag() & BcObject::InitSlowAllocsOk);
        // Slow path, call the system free as this was malloc-ed buf.
        StatsLib::statNonAtomicIncr(this->slowFrees_);
        lock_.unlock();

#ifdef BUFCACHEPOISON
        if (buf) {
            // We don't discriminate fast or slow paths here. The intention is
            // to catch Buf$ use after free culprits.
            memset(buf, FreePattern, object_->getBufferSize());
        }
#endif  // BUFCACHEPOISON

        if (object_->getInitFlag() & BcObject::InitDontCoreDump) {
            verifyOk(memMarkDoCoreDump(buf, object_->getBufferSize()));
        }
        if (object_->alignedAllocs()) {
            memAlignedFree(buf);
        } else {
            memFree(buf);
        }
    }
}

void
BcHandle::scan(Xid txnXidToFree, BcPageType pageType)
{
    bool hasLock = false;
    const uint64_t batchSize = XcalarConfig::get()->bcScanBatchSize_;

    for (uint64_t ii = 0; ii < this->object_->getNumElements(); ii++) {
        if (!hasLock) {
            lock_.lock();
            hasLock = true;
        }

        // Periodically drop lock in loop to allow others in
        const bool dropLock = (batchSize != 0) && (((ii + 1) % batchSize) == 0);
        if (freeListSaved_[ii].txnXid != txnXidToFree ||
            freeListSaved_[ii].bufState_ == BcHdr::BcStateFree ||
            freeListSaved_[ii].bcScanCleanoutMode_ == BcScanCleanoutNotToFree ||
            freeListSaved_[ii].bcPageType_ != pageType) {
            if (dropLock) {
                lock_.unlock();
                hasLock = false;
            }
            continue;
        }

        if (freeListSaved_[ii].bcPageType_ == BcPageTypeHdr) {
            // Unfortunately, we have to break abstraction now and poke into
            // Xdb layer to free these pages, because demand-paging is
            // managed at Xdb layer. This should be fixed when we move
            // demand-paging into b$
            XdbPageHdrBackingPage *backingPage =
                (XdbPageHdrBackingPage *) freeListSaved_[ii].buf_;
            size_t pageHdrsToDrop = 0;
            for (size_t ii = 0; ii < backingPage->nextIdx; ii++) {
                XdbPage *pageHdr = &backingPage->pageHdrs[ii];

                // This is a key assumption -- very subtle -- when we are
                // doing the scan, if the page is not already in a dropped
                // state, there should be no other racing threads that will
                // turn this into a dropped state.
                if (atomicRead32(&pageHdr->hdr.pageState) != XdbPage::Dropped) {
                    pageHdrsToDrop++;
                }
            }

            if (pageHdrsToDrop > 0) {
                size_t pageHdrsDropped = 0;

                // We can't hold the bcLock across a potential wait on
                // pageRef lock, which might be held while a serialization
                // is in progress. However, as soon as we release the
                // bcLock, and we free all pageHdrs within this page, this
                // page might get freed, and so we need to carefully count
                // the number of pageHdrs we need to free and stop looking at
                // the page as soon as we've freed all of the pageHdrs
                lock_.unlock();
                hasLock = false;

                // Now we perform the actual drop
                for (size_t ii = 0; ii < backingPage->nextIdx; ii++) {
                    XdbPage *pageHdr = &backingPage->pageHdrs[ii];
                    if (atomicRead32(&pageHdr->hdr.pageState) !=
                        XdbPage::Dropped) {
                        XdbMgr::get()->xdbFreeXdbPage(pageHdr);
                        pageHdrsDropped++;
                        if (pageHdrsDropped >= pageHdrsToDrop) {
                            assert(pageHdrsDropped == pageHdrsToDrop);
                            // This page might have been freed. No longer
                            // safe to look at this page
                            backingPage = NULL;
                            break;
                        }
                    }
                }
                assert(pageHdrsDropped == pageHdrsToDrop);
                lock_.lock();
                hasLock = true;
            }
        } else {
#ifdef BUFCACHESHADOW
            void *buf = retireAddr(freeListSaved_[ii].buf_);
            assert(buf == freeListSaved_[ii].buf_);
#endif
            fastFree(freeListSaved_[ii].buf_);
        }

        if (dropLock) {
            lock_.unlock();
            hasLock = false;
        }
    }

    if (hasLock) {
        lock_.unlock();
        hasLock = false;
    }
}
Status
BcHandle::statsToInit()
{
    Status status = StatusOk;
    StatGroupId statsGrpId;
    StatsLib *statsLib = StatsLib::get();

    status =
        statsLib->initNewStatGroup(object_->getObjectName(), &statsGrpId, 10);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->elemSizeBytes_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "elemSizeBytes",
                                         this->elemSizeBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->fastAllocs_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "fastAllocs",
                                         this->fastAllocs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->fastFrees_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "fastFrees",
                                         this->fastFrees_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->fastHWM_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "fastHWM",
                                         this->fastHWM_,
                                         StatUint64,
                                         StatHWM,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->slowAllocs_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "slowAllocs",
                                         this->slowAllocs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->slowFrees_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "slowFrees",
                                         this->slowFrees_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->slowHWM_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "slowHWM",
                                         this->slowHWM_,
                                         StatUint64,
                                         StatHWM,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->totMemBytes_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "totMemBytes",
                                         this->totMemBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->mlockChunk_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "mlockChunk",
                                         this->mlockChunk_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->mlockChunkFailure_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "mlockChunkFailure",
                                         this->mlockChunkFailure_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "BcHandle::statsToInit %s failed: %s",
                object_->getObjectName(),
                strGetFromStatus(status));
    }
    return status;
}

BcHandle *
BcHandle::createInternal(BcObject *bcObject)
{
    Status status = StatusOk;
    BcHandle *bcHandle = NULL;
    BcHdr *bcHdr = NULL;
    XcalarConfig *xc = XcalarConfig::get();

    bcHandle = new (std::nothrow) BcHandle();
    BailIfNull(bcHandle);

    bcHandle->baseBuf_ = bcObject->getBaseAddr();
    bcHandle->topBuf_ = bcObject->getEndAddr();
    bcHandle->object_ = bcObject;
    bcHandle->didSlowWarning_ = false;
    bcHandle->mlockPct_ = InvalidMlockPct;
    bcHandle->mlockChunkPct_ = InvalidMlockChunkPct;

    status = bcHandle->statsToInit();
    if (status != StatusOk) {
        goto CommonExit;
    }

    StatsLib::statNonAtomicAdd64(bcHandle->elemSizeBytes_,
                                 bcObject->getBufferSize());
    StatsLib::statNonAtomicAdd64(bcHandle->totMemBytes_,
                                 bcObject->getObjectSize());

    if (!bcObject->getNumElements()) {
        goto CommonExit;
    }

    assert(bcHandle->topBuf_ == (void *) ((uintptr_t) bcHandle->baseBuf_ +
                                          bcObject->getObjectSize()));

    // Organize hdrs into free list
    bcHdr = new (std::nothrow) BcHdr[bcObject->getNumElements()];
    BailIfNull(bcHdr);

    bcHandle->freeListSaved_ = &bcHdr[0];

    if (!xc->bufCacheMlock_) {
        // Buf$ with Mlocking OFF.
        bcHandle->mlockPct_ = InvalidMlockPct;
        bcHandle->mlockChunkPct_ = InvalidMlockChunkPct;
        bcHandle->addElemsToFreeList(0, bcObject->getNumElements());
    } else if (!xc->bufferCacheLazyMemLocking_) {
        // Buf$ with Mlocking ON, Lazy Mlocking OFF.
        // If we aren't lazy, just add everything to freeList.
        bcHandle->mlockPct_ = 100;
        bcHandle->mlockChunkPct_ = InvalidMlockChunkPct;
        bcHandle->addElemsToFreeList(0, bcObject->getNumElements());
    } else {
        // Buf$ with Mlocking ON, Lazy Mlocking ON.
        if (bcObject->getObjectId() == BufferCacheObjects::XdbPage) {
            // For XdbPages, Lazy Mlock in Chunks.
            bcHandle->mlockPct_ = 0;
            bcHandle->mlockChunkPct_ =
                XcalarConfig::get()->xdbPageMlockChunkPct_;
            bcHandle->freeList_ = NULL;
        } else if (bcObject->getObjectId() ==
                   BufferCacheObjects::XdbPageOsPageable) {
            // Don't attempt mlock for XdbPageOsPageable slab
            bcHandle->mlockPct_ = InvalidMlockPct;
            bcHandle->mlockChunkPct_ = InvalidMlockChunkPct;
            bcHandle->addElemsToFreeList(0, bcObject->getNumElements());
        } else {
            // For the rest of slabs, Mlock everything, i.e. ignore
            // lazy mlocking option.
            bcHandle->mlockPct_ = 0;
            bcHandle->mlockChunkPct_ = 100;
            bcHandle->mlockAdditionalChunk();
        }
    }

#ifdef BUFCACHESHADOW
    status = shadowInit(bcHandle);
    BailIfFailed(status);
#endif

CommonExit:
    if (status != StatusOk) {
        if (bcHdr != NULL) {
            delete[] bcHdr;
            bcHdr = NULL;
        }
        if (bcHandle != NULL) {
            delete bcHandle;
            bcHandle = NULL;
        }
        xSyslog(moduleName,
                XlogErr,
                "bcCreate(%s) failed: %s",
                bcObject->getObjectName(),
                strGetFromStatus(status));
    } else {
        xSyslog(moduleName,
                XlogDebug,
                "bcCreate(%s) succeeded %luMB",
                bcObject->getObjectName(),
                bcObject->getObjectSize() / MB);
    }

    return bcHandle;
}

// baseBuf is freed outside this function
void
BcHandle::destroyInternal(BcHandle **bcHandle)
{
    uint64_t fastAllocs;
    uint64_t fastFrees;
    uint64_t slowAllocs;
    uint64_t slowFrees;

    (*bcHandle)->lock_.lock();
    fastAllocs = (*bcHandle)->fastAllocs_->statUint64;
    fastFrees = (*bcHandle)->fastFrees_->statUint64;
    slowAllocs = (*bcHandle)->slowAllocs_->statUint64;
    slowFrees = (*bcHandle)->slowFrees_->statUint64;
    (*bcHandle)->lock_.unlock();

    bool fastLeak = fastAllocs != fastFrees;
    bool slowLeak = slowAllocs != slowFrees;

    if (fastLeak) {
        xSyslog(moduleName,
                XlogErr,
                "bcDestroy(%s)::fast allocs %lu frees %lu",
                (*bcHandle)->object_->getObjectName(),
                fastAllocs,
                fastFrees);

#ifdef BUFCACHETRACE
        for (uint64_t ii = 0; ii < (*bcHandle)->object_->getNumElements();
             ii++) {
            BcHdr *hdr = &(*bcHandle)->freeListSaved_[ii];

            if (hdr->bufState_ == BcHdr::BcStateAlloc) {
                int ret;
                char btBuffer[BtBuffer];
                char grTrace[BtBuffer];
                size_t currPos = 0;
                int leakIdx = hdr->idx_;
                if (leakIdx == 0) {
                    leakIdx = NumTraces - 1;
                } else {
                    leakIdx = leakIdx - 1;
                }
                printBackTrace(btBuffer,
                               sizeof(btBuffer),
                               hdr->traces_[leakIdx].traceSize,
                               hdr->traces_[leakIdx].trace);
                xSyslog(moduleName,
                        XlogErr,
                        "Txn %lu, hdr: %p, buf: %p\n%s",
                        hdr->txnXid,
                        hdr,
                        hdr->buf_,
                        btBuffer);

                // Dump trace in format consumable by guardrails grdump.py
                ret = snprintf(&grTrace[currPos],
                               BtBuffer,
                               "%lu,",
                               (*bcHandle)->object_->getBufferSize());
                for (ssize_t frame = 0; frame < TraceSize; frame++) {
                    if (ret < 0) {
                        assert(0);
                        break;
                    }
                    currPos += ret;
                    if (currPos >= BtBuffer) {
                        assert(0);
                        break;
                    }

                    if (frame < hdr->traces_[leakIdx].traceSize) {
                        ret = snprintf(&grTrace[currPos],
                                       BtBuffer - currPos,
                                       "%p,",
                                       hdr->traces_[leakIdx].trace[frame]);
                    } else {
                        // GuardRails format requires even empty fields be
                        // present
                        ret = snprintf(&grTrace[currPos],
                                       BtBuffer - currPos,
                                       ",");
                    }
                }

                xSyslog(moduleName,
                        XlogErr,
                        "Txn %lu\ngrtrace: %s",
                        hdr->txnXid,
                        grTrace);
            }
        }
#endif  // BUFCACHETRACE
    }

    if (slowLeak) {
        xSyslog(moduleName,
                XlogErr,
                "bcDestroy(%s)::slow allocs %lu frees %lu",
                (*bcHandle)->object_->getObjectName(),
                slowAllocs,
                slowFrees);
    }

    if (fastLeak || slowLeak) {
        // The file name used below is also used in jenkinsUtils.sh by
        // genBuildArtifacts() so if you change it here, change in both places!
        dumpAllTraces("bcleak-traces");
    }

#ifdef BUFCACHETRACE
    if (fastLeak || slowLeak) {
        assert(0 && "Leaks in Buffer Cache");
    }
#endif  // BUFCACHETRACE

#ifdef BUFCACHESHADOW
    destroyShadow(*bcHandle);
#endif  // BUFCACHESHADOW

    delete[](*bcHandle)->freeListSaved_;

    delete *bcHandle;
    *bcHandle = NULL;
}

#ifdef BUFCACHESHADOW

Status
BcHandle::shadowInit(BcHandle *bcHandle)
{
    Status status = StatusOk;

    if (bcHandle->object_->getObjectId() == BufferCacheObjects::XdbPage) {
        bcHandle->shadowToBc_ =
            (BufCacheDbgVaMap *) memAllocExt(sizeof(BufCacheDbgVaMap),
                                             moduleName);
        BailIfNull(bcHandle->shadowToBc_);
        bcHandle->shadowToBc_ = new (bcHandle->shadowToBc_) BufCacheDbgVaMap();
        BailIfNull(bcHandle->shadowToBc_);

        bcHandle->bcToShadow_ =
            (BufCacheDbgVaMap *) memAllocExt(sizeof(BufCacheDbgVaMap),
                                             moduleName);
        BailIfNull(bcHandle->bcToShadow_);
        bcHandle->bcToShadow_ = new (bcHandle->bcToShadow_) BufCacheDbgVaMap();
        BailIfNull(bcHandle->bcToShadow_);

#ifdef BUFCACHESHADOWRETIRED
        bcHandle->retiredShadowToBc_ =
            (BufCacheDbgVaMap *) memAllocExt(sizeof(BufCacheDbgVaMap),
                                             moduleName);
        BailIfNull(bcHandle->retiredShadowToBc_);
        bcHandle->retiredShadowToBc_ =
            new (bcHandle->retiredShadowToBc_) BufCacheDbgVaMap();
        BailIfNull(bcHandle->retiredShadowToBc_);
#endif  // BUFCACHESHADOWRETIRED
    }

CommonExit:
    return status;
}

void *
BcHandle::getBackingAddr(void *buf)
{
    if (object_->getObjectId() == BufferCacheObjects::XdbPage) {
        dbgHashLock_.lock();
        BufCacheDbgVaItem *item = shadowToBc_->find(buf);
        dbgHashLock_.unlock();
        if (item) {
            // Original address was shadow address
            return item->valPtr;
        }
    }

    return buf;
}

void *
BcHandle::remapAddr(void *buf)
{
    assert(!lock_.tryLock());

    if (object_->getObjectId() == BufferCacheObjects::XdbPage) {
        dbgHashLock_.lock();
        assert(bcToShadow_->find(buf) == NULL);  // No existing b$ mapping

        void *shadowPtr =
            mremap(buf, 0, object_->getBufferSize(), MREMAP_MAYMOVE);
        assert(shadowPtr);
        assert(shadowPtr != ((void *) 0xffffffffffffffff));
        assert(shadowToBc_->find(shadowPtr) ==
               NULL);  // New VA should be unique

        // XXX: Slow path allocation for hash table item
        BufCacheDbgVaItem *bcItem =
            (BufCacheDbgVaItem *) memAllocExt(sizeof(*bcItem), moduleName);
        bcItem->valPtr = shadowPtr;
        bcItem->keyPtr = buf;
        bcToShadow_->insert(bcItem);

        BufCacheDbgVaItem *shadowItem =
            (BufCacheDbgVaItem *) memAllocExt(sizeof(*shadowItem), moduleName);
        shadowItem->valPtr = buf;
        shadowItem->keyPtr = shadowPtr;
        shadowToBc_->insert(shadowItem);
        dbgHashLock_.unlock();
        return shadowPtr;
    } else {
        return buf;
    }
}

void *
BcHandle::retireAddr(void *buf)
{
    void *bcPtr = buf;

    assert(!lock_.tryLock());

    if (object_->getObjectId() == BufferCacheObjects::XdbPage) {
        dbgHashLock_.lock();
        BufCacheDbgVaItem *shadowItem = shadowToBc_->remove(buf);
        BufCacheDbgVaItem *bcItem;
        if (shadowItem == NULL) {
            bcItem = bcToShadow_->remove(buf);
            if (bcItem == NULL) {
                dbgHashLock_.unlock();
                // This is probably XdbPageHdr which was not remaped
                uint64_t ordinal = getOrdinal(buf);
                assert(ordinal < object_->getNumElements());
                assert(freeListSaved_[ordinal].bcPageType_ ==
                       BcHandle::BcPageTypeHdr);
                return bcPtr;
            }
            shadowItem = shadowToBc_->remove(bcItem->valPtr);
        } else {
            bcItem = bcToShadow_->remove(shadowItem->valPtr);
            bcPtr = shadowItem->valPtr;
        }
        assert(shadowItem);
        assert(bcItem);

        int ret =
            mprotect(shadowItem->keyPtr, object_->getBufferSize(), PROT_NONE);
        assert(ret == 0);
        ret = madvise(shadowItem->keyPtr,
                      object_->getBufferSize(),
                      MADV_DONTDUMP);
        assert(ret == 0);
#ifdef BUFCACHESHADOWRETIRED
        // Save all retired addresses for debugging purposes XXX: RHEL7 has
        // 128TB of VA per process which allows a fairly long run time even
        // though addresses are permanently retired.  If we want to enable this
        // for long-running tests this should probably turn into a delay list
        // so we can reclaim VA after some time.
        assert(retiredShadowToBc_->find(shadowItem->keyPtr) == NULL);
        retiredShadowToBc_->insert(shadowItem);
#else
        memFree(shadowItem);
#endif  // BUFCACHESHADOWRETIRED
        memFree(bcItem);
        dbgHashLock_.unlock();
    }

    return bcPtr;
}

void
BcHandle::destroyShadow(BcHandle *bcHandle)
{
    if (bcHandle->object_->getObjectId() == BufferCacheObjects::XdbPage) {
#ifdef BUFCACHESHADOWRETIRED
        BufCacheDbgVaItem *item;
        for (BufCacheDbgVaMap::iterator it =
                 bcHandle->retiredShadowToBc_->begin();
             (item = it.get()) != NULL;
             it.next()) {
            munmap(item->keyPtr, bcHandle->object_->getBufferSize());
            bcHandle->retiredShadowToBc_->remove(item);
        }
        bcHandle->retiredShadowToBc_->~BufCacheDbgVaMap();
        memFree(bcHandle->retiredShadowToBc_);
        bcHandle->retiredShadowToBc_ = NULL;
#endif  // BUFCACHESHADOWRETIRED
        bcHandle->bcToShadow_->~BufCacheDbgVaMap();
        memFree(bcHandle->bcToShadow_);
        bcHandle->bcToShadow_ = NULL;
        bcHandle->shadowToBc_->~BufCacheDbgVaMap();
        memFree(bcHandle->shadowToBc_);
        bcHandle->shadowToBc_ = NULL;
    }
}

#endif  // BUFCACHESHADOW
