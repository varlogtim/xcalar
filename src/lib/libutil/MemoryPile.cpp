// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "util/MemoryPile.h"
#include "xdb/Xdb.h"
#include "stat/Statistics.h"
#include "sys/XLog.h"
#include <sys/mman.h>

StatGroupId MemoryPile::statsGrpId_;
StatHandle MemoryPile::numPilePagesAlloced_;
StatHandle MemoryPile::numPilePagesFreed_;

static constexpr const char *moduleName = "memoryPile";

Status
MemoryPile::init()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("MemoryPile", &statsGrpId_, 2);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numPilePagesAlloced_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numPilePagesAlloced",
                                         numPilePagesAlloced_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numPilePagesFreed_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numPilePagesFreed",
                                         numPilePagesFreed_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}

MemoryPile *
MemoryPile::allocPile(Xid txnId,
                      Status *statusOut,
                      size_t alignment,
                      BcHandle::BcScanCleanoutMode cleanoutMode)
{
    void *bufs[BatchAllocSize];
    uint64_t numBufsAlloced =
        XdbMgr::get()->bcBatchAlloc(bufs,
                                    BatchAllocSize,
                                    0,
                                    txnId,
                                    XdbMgr::SlabHint::Default,
                                    cleanoutMode);
    if (numBufsAlloced == 0) {
        if (statusOut != NULL) {
            *statusOut = StatusNoXdbPageBcMem;
        }
        return NULL;
    }

    for (uint64_t ii = 0; ii < numBufsAlloced; ii++) {
        MemoryPile *memoryPile;
#ifdef BUFCACHESHADOW
        memoryPile =
            (MemoryPile *) XdbMgr::get()->xdbGetBackingBcAddr(bufs[ii]);
#else   // BUFCACHESHADOW
        memoryPile = (MemoryPile *) bufs[ii];
#endif  // BUFCACHESHADOW
        new (memoryPile) MemoryPile();

        // make sure we are aligned to pile size
        assert(((uintptr_t) memoryPile & ~(XdbMgr::bcSize() - 1)) ==
               (uintptr_t) memoryPile);

        // will dec once we initiate all memory in this pile
        atomicWrite32(&memoryPile->count_, 1);
        memoryPile->txnId_ = txnId;
        memoryPile->alignment_ = alignment;
        memoryPile->nextOffset_ = 0;
        memoryPile->cleanoutMode_ = cleanoutMode;
        StatsLib::statAtomicIncr64(numPilePagesAlloced_);

        memoryPile->maxMemory_ = XdbMgr::bcSize() - sizeof(*memoryPile);
        if (ii < numBufsAlloced - 1) {
            memoryPile->batchMemPileNext_ =
#ifdef BUFCACHESHADOW
                (MemoryPile *) XdbMgr::get()->xdbGetBackingBcAddr(bufs[ii + 1]);
#else   // BUFCACHESHADOW
                (MemoryPile *) bufs[ii + 1];
#endif  // BUFCACHESHADOW
        }

#ifdef MEMORY_PILE_DEBUG
        xSyslog(moduleName, XlogInfo,
              "MemoryPiles alloc %lu: %p", ii, memoryPile);
#endif // MEMORY_PILE_DEBUG
    }

    if (statusOut != NULL) {
        *statusOut = StatusOk;
    }
    return (MemoryPile *) bufs[0];
}

MemoryPile *
MemoryPile::allocPile(size_t elemSize,
                      Xid txnId,
                      Status *statusOut,
                      size_t alignment,
                      BcHandle::BcScanCleanoutMode cleanoutMode)
{
    MemoryPile *memoryPile =
        allocPile(txnId, statusOut, alignment, cleanoutMode);
    if (memoryPile == NULL) {
        return NULL;
    }

    MemoryPile *cur = memoryPile;
    while (cur != NULL) {
        cur->setSize(elemSize);
        cur = cur->batchMemPileNext_;
    }

    return memoryPile;
}

void
MemoryPile::freeMemoryPile()
{
    MemoryPile *cur = this;
    void *bufs[BatchAllocSize];
    unsigned numToFree = 0;
    for (uint64_t ii = 0; ii < BatchAllocSize; ii++) {
        bufs[ii] = cur;
        MemoryPile *prev = cur;
        if (cur != NULL) {
            cur = cur->batchMemPileNext_;
            numToFree++;
        }
        if (prev != NULL) {
            prev->batchMemPileNext_ = NULL;
            prev->~MemoryPile();
            StatsLib::statAtomicIncr64(numPilePagesFreed_);
#ifdef MEMORY_PILE_DEBUG
            xSyslog(moduleName, XlogInfo,
                    "MemoryPiles free %lu : %p", ii, prev);
#endif // MEMORY_PILE_DEBUG
        }
        if (cur == NULL) {
            break;
        }
    }
    XdbMgr::get()->bcBatchFree(bufs, numToFree, 0);
}

void
MemoryPile::putMemoryPile()
{
    if (unlikely(atomicDec32(&count_) == 0)) {
        freeMemoryPile();
    }
}

void
MemoryPile::markPileAllocsDone()
{
    // we initialized count to 1, do the final put here
    putMemoryPile();
}

void *
MemoryPile::getElem(MemoryPile **memoryPileIn, size_t size, Status *statusOut)
{
    MemoryPile *memoryPile = *memoryPileIn;
    void *elem = NULL;
    Status status = StatusUnknown;

    if (unlikely(memoryPile == NULL)) {
        status = StatusInval;
        goto CommonExit;
    }

    uint32_t curOffset;
    curOffset =
        roundUp(memoryPile->nextOffset_, memoryPile->alignment_);
    uint32_t baseOffset;
    baseOffset =
        roundUp(offsetof(MemoryPile, baseBuf_), memoryPile->alignment_);

    // Support for variable-sized elements only if elemSize_ is not set
    assert(memoryPile->elemSize_ == 0 || memoryPile->elemSize_ == size);

    if (unlikely(baseOffset + size > memoryPile->maxMemory_)) {
        // For the alignment chosen, there is no way anything can fit.
        status = StatusMaxFieldSizeExceeded;
        goto CommonExit;
    }

    if (unlikely(curOffset + size > memoryPile->maxMemory_)) {
        if (memoryPile->batchMemPileNext_) {
            *memoryPileIn = memoryPile->batchMemPileNext_;
            memoryPile->batchMemPileNext_ = NULL;
#ifdef MEMORY_PILE_DEBUG
            xSyslog(moduleName, XlogInfo,
                    "MemoryPiles cur %p next %p", memoryPile, *memoryPileIn);
#endif // MEMORY_PILE_DEBUG
        } else {
            *memoryPileIn = allocPile(memoryPile->elemSize_,
                                      memoryPile->txnId_,
                                      statusOut,
                                      memoryPile->alignment_,
                                      memoryPile->cleanoutMode_);
        }
        memoryPile->markPileAllocsDone();
        memoryPile = *memoryPileIn;
        if (unlikely(memoryPile == NULL)) {
            status = StatusNoXdbPageBcMem;
            goto CommonExit;
        }
    }

    curOffset = roundUp(memoryPile->nextOffset_, memoryPile->alignment_);
    elem = (void *) ((size_t) memoryPile->baseBuf_ + curOffset);
    memoryPile->nextOffset_ = curOffset + size;
    atomicInc32(&memoryPile->count_);
#ifdef MEMORY_PILE_POISON
    memset(elem, 0xbd, size);
#endif  // MEMORY_PILE_POISON
    status = StatusOk;

CommonExit:
    *statusOut = status;
    return elem;
}

// assumes that pile allocations are pileSize aligned
MemoryPile *
MemoryPile::getSourcePile(void *elem, size_t pileSize)
{
    size_t elemAddress = (size_t) elem;
    size_t offset = (size_t) elemAddress % pileSize;
    return (MemoryPile *) (elemAddress - offset);
}

// assumes that pile allocations are pileSize aligned
void
MemoryPile::putElem(void *elem, size_t pileSize)
{
    MemoryPile *memoryPile = getSourcePile(elem, pileSize);
#ifdef MEMORY_PILE_POISON
    memset(elem, 0xac, memoryPile->elemSize_);
#endif  // MEMORY_PILE_POISON
    memoryPile->putMemoryPile();
}
