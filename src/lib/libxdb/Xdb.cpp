// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <math.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "stat/Statistics.h"
#include "msg/Message.h"
#include "xdb/Xdb.h"
#include "xdb/XdbInt.h"
#include "bc/BufCacheMemMgr.h"
#include "bc/BufCacheObjectMgr.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "util/Math.h"
#include "dataset/Dataset.h"
#include "df/DataFormat.h"
#include "operators/OperatorsHash.h"
#include "operators/OperatorsXdbPageOps.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "util/WorkQueue.h"
#include "operators/XcalarEval.h"
#include "operators/Operators.h"
#include "util/TrackHelpers.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "msg/Xid.h"
#include "transport/TransportPage.h"
#include "gvm/Gvm.h"
#include "LibXdbGvm.h"
#include "ns/LibNs.h"
#include "cursor/Cursor.h"
#include "xdb/HashMergeSort.h"
#include "datapage/DataPageIndex.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "durable/Durable.h"
#include "util/DFPUtils.h"
#include "xdb/Merge.h"
#include "strings/String.h"
#include "util/SaveTrace.h"

static constexpr const char *moduleName = "libxdb";
XdbMgr *XdbMgr::instance = NULL;
uint64_t XdbMgr::xdbHashSlots = XdbMgr::XdbHashSlotsDefault;

// XXX Need to scope this, but first need better abstraction for
// struct Xdb.
// In case you are curious what slow hash is...it is the name given to
// separate the generic hash from the impl of xdb which you can call the
// hash from hell, and make it easier to read xdb code. Else you risk
// namespace pollution.
static constexpr uint64_t SlowHashTableSlotCount = 32749;
typedef IntHashTable<XdbId,
                     Xdb,
                     &Xdb::hook,
                     &Xdb::getXdbId,
                     SlowHashTableSlotCount,
                     hashIdentity>
    XdbHashTable;
XdbHashTable slowHashTable;

XdbMgr *
XdbMgr::get()
{
    return instance;
}

void
XdbMgr::xdbFreeTupBuf(XdbPage *xdbPage, bool changeState)
{
    if (!xdbPage) {
        return;
    }

#ifdef XDBPAGEHDRMAP
    BufCacheDbgToXdbHdrItem *bcXdbItem;
    bufCacheDbgToXdbHdrMapLock.lock();
    bcXdbItem = bufCacheDbgToXdbHdrMap->remove(xdbPage->tupBuf);
    assert(bcXdbItem);
    memFree(bcXdbItem);
    bufCacheDbgToXdbHdrMapLock.unlock();
#endif

    if (xdbPage->tupBuf) {
        // In order to free a tupBuf it must be resident in memory.  Note: this
        // includes the Serializing state as we free the tupBuf along that path
        assert(atomicRead32(&xdbPage->hdr.pageState) == XdbPage::Resident ||
               atomicRead32(&xdbPage->hdr.pageState) == XdbPage::Serializable ||
               atomicRead32(&xdbPage->hdr.pageState) == XdbPage::Serializing ||
               atomicRead32(&xdbPage->hdr.pageState) ==
                   XdbPage::Deserializing ||
               atomicRead32(&xdbPage->hdr.pageState) == XdbPage::Dropped);
        bcFree(xdbPage->tupBuf);
        xdbPage->tupBuf = NULL;
    }

    if (changeState) {
        atomicWrite32(&xdbPage->hdr.pageState, XdbPage::Dropped);
    }
}

void
XdbMgr::xdbFreeXdbPage(XdbPage *xdbPage)
{
    if (!xdbPage) {
        return;
    }

    // Treat defferred as freed refcount for debug/tracking purposes
    saveTrace(TraceOpts::RefDel, xdbPage, xdbPage->hdr.refCount.val);

    if (xdbPage->hdr.xdbPageType & XdbPageNormalFlag) {
        xdbPage->dropKvBuf();
        xdbPutXdbPageHdr(xdbPage);
    } else if (xdbPage->hdr.xdbPageType & XdbPageEmbeddedFlag) {
        // We are not responsible for freeing this page. NOOP!
    } else {
        assert(0 && "Unknown XDB page type");
    }
}

void
XdbMgr::xdbInitXdbPage(NewKeyValueMeta *kvMeta,
                       XdbPage *xdbPage,
                       XdbPage *nextXdbPage,
                       size_t pageSize,
                       XdbPageType pageType)
{
#ifdef DEBUG
    // Catch double init, which aside from being wrong breaks refcounts for oom
    // debug code
    assert(!xdbPage->hdr.isInit);
    xdbPage->hdr.isInit = true;
#endif
    xdbPage->hdr.nextPage = nextXdbPage;
    xdbPage->hdr.prevPage = NULL;
    xdbPage->hdr.xdbPageType = pageType;
    xdbPage->hdr.pageSize = pageSize;
    xdbPage->hdr.numRows = 0;
    saveTrace(TraceOpts::RefInc, xdbPage);
    atomicWrite32(&xdbPage->hdr.pageState, XdbPage::Resident);
    atomicWrite64(&xdbPage->hdr.refCount, 1);

    new (xdbPage->tupBuf) NewTuplesBuffer(xdbPage->tupBuf, pageSize);
}

void
XdbMgr::xdbInitXdbPage(Xdb *xdb,
                       XdbPage *xdbPage,
                       XdbPage *nextXdbPage,
                       size_t pageSize,
                       XdbPageType pageType)
{
    xdbInitXdbPage((NewKeyValueMeta *) &xdb->meta->kvNamedMeta.kvMeta_,
                   xdbPage,
                   nextXdbPage,
                   pageSize,
                   pageType);
}

Status
XdbMgr::xdbAllocTupBuf(XdbPage *xdbPage,
                       Xid txnId,
                       BcHandle::BcScanCleanoutMode cleanoutMode,
                       XdbPage::PageState pageState)
{
    Status status;
    NewTuplesBuffer *tupBuf = NULL;
    tupBuf = (NewTuplesBuffer *) bcAllocInternal(txnId, &status, cleanoutMode);
    if (tupBuf == NULL) {
        assert(status != StatusOk);
        return status;
    }

    new (tupBuf) NewTuplesBuffer(tupBuf, bcXdbPage_->getBufSize());
    xdbPage->tupBuf = tupBuf;

#ifdef XDBPAGEHDRMAP
    BufCacheDbgToXdbHdrItem *bcXdbItem =
        (BufCacheDbgToXdbHdrItem *) memAllocExt(sizeof(*bcXdbItem), moduleName);
    bufCacheDbgToXdbHdrMapLock.lock();
    assert(xdbPage->tupBuf);
    bcXdbItem->keyPtr = xdbPage->tupBuf;
    bcXdbItem->valPtr = xdbPage;
    bufCacheDbgToXdbHdrMap->insert(bcXdbItem);
    bufCacheDbgToXdbHdrMapLock.unlock();
#endif

    atomicWrite32(&xdbPage->hdr.pageState, pageState);

    return StatusOk;
}

void
XdbMgr::xdbPutXdbPageHdr(XdbPageHdrBackingPage *backingPage)
{
    if (atomicDec64(&backingPage->count) == 0) {
        bcFree(backingPage);
    }
}

void
XdbMgr::xdbPutXdbPageHdr(XdbPage *pageHdr)
{
    size_t offset = (uintptr_t) pageHdr % bcXdbPage_->getBufSize();
    XdbPageHdrBackingPage *backingPage =
        (XdbPageHdrBackingPage *) ((uintptr_t) pageHdr - offset);
    // Note that setting state of Dropped is needed for correctness
    // because bcScanCleanout relies on pageHdr to decide if a page needs
    // to be cleaned out
    atomicWrite32(&pageHdr->hdr.pageState, XdbPage::Dropped);
    xdbPutXdbPageHdr(backingPage);
}

XdbPage *
XdbMgr::xdbAllocXdbPageHdr(Xid txnId,
                           Xdb *xdb,
                           BcHandle::BcScanCleanoutMode cleanoutMode)
{
    Status status = StatusUnknown;
    XdbPage *pageHdr = NULL;
    XdbPageHdrBackingPage *newBackingPage = NULL;
    XdbPageHdrBackingPage *backingPage = NULL;
    size_t backingPageSize = 0;
    size_t bufSize = 0;
    uintptr_t offset;

    xdb->pageHdrLock.lock();

    static_assert(sizeof(*pageHdr) % XdbMinPageAlignment == 0,
                  "Need XdbPage size to be a multiple of XdbMinPageAlignment");
    assert(sizeof(*pageHdr) % XdbMinPageAlignment == 0);
    assert(mathIsPowerOfTwo(XdbMinPageAlignment));

    backingPage = xdb->pageHdrBackingPage;
    if (backingPage == NULL || backingPage->nextIdx >= backingPage->maxElems) {
        // Need a new backing page
        newBackingPage =
            (XdbPageHdrBackingPage *) bcAlloc(txnId,
                                              &status,
                                              SlabHint::Default,
                                              cleanoutMode,
                                              BcHandle::BcPageTypeHdr);
        if (newBackingPage == NULL) {
            goto CommonExit;
        }

        backingPageSize = bcXdbPage_->getBufSize();

        assert(mathIsAligned(newBackingPage, backingPageSize));

        bufSize = backingPageSize - sizeof(*newBackingPage);
        assert(backingPageSize > sizeof(*newBackingPage));
        offset = (uintptr_t) newBackingPage->buf % XdbMinPageAlignment;
        assert(
            mathIsAligned(&newBackingPage->buf[offset], XdbMinPageAlignment));
        newBackingPage->pageHdrs = (XdbPage *) &newBackingPage->buf[offset];

        atomicWrite64(&newBackingPage->count, 1);
        newBackingPage->nextIdx = 0;
        newBackingPage->maxElems = (bufSize - offset) / sizeof(*pageHdr);

        if (xdb->pageHdrBackingPage != NULL) {
            xdbPutXdbPageHdr(xdb->pageHdrBackingPage);
        }
        xdb->pageHdrBackingPage = backingPage = newBackingPage;
        newBackingPage = NULL;
    }

    assert(backingPage != NULL);
    atomicAdd64(&backingPage->count, 1);
    pageHdr = &backingPage->pageHdrs[backingPage->nextIdx++];
CommonExit:
    if (newBackingPage != NULL) {
        bcFree(newBackingPage);
        newBackingPage = NULL;
    }
    xdb->pageHdrLock.unlock();

    return (pageHdr);
}

// XXX need statistics to track xdbAllocXdbPage(XidInvalid)
XdbPage *
XdbMgr::xdbAllocXdbPage(Xid txnId,
                        Xdb *xdb,
                        BcHandle::BcScanCleanoutMode cleanoutMode)
{
    Status status = StatusUnknown;

    XdbPage *xdbPage = NULL;
    xdbPage = xdbAllocXdbPageHdr(txnId, xdb, cleanoutMode);
    if (unlikely(xdbPage == NULL)) {
        return NULL;
    }

    new (xdbPage) XdbPage();
    xdbPage->hdr.xdbPageType = XdbAllocedNormal;
    xdbPage->txnId = txnId;
    xdbPage->cleanoutMode = cleanoutMode;

    status = xdbAllocTupBuf(xdbPage, txnId, cleanoutMode);
    if (unlikely(status != StatusOk)) {
        xdbPutXdbPageHdr(xdbPage);
        return NULL;
    }

    assert(mathIsAligned(xdbPage, XdbMinPageAlignment));

#ifdef DEBUG
    // Flip after XdbMgr::xdbInitXdbPage
    // TODO: Just call XdbMgr::xdbInitXdbPage here...
    xdbPage->hdr.isInit = false;
#endif

    return xdbPage;
}

uint64_t
XdbMgr::xdbAllocXdbPageBatch(XdbPage **xdbPages,
                             const uint64_t numBufs,
                             Xid txnId,
                             Xdb *xdb,
                             SlabHint slabHint,
                             BcHandle::BcScanCleanoutMode cleanoutMode)
{
    uint64_t hdrs = 0;
    Status status;

    for (hdrs = 0; hdrs < numBufs; hdrs++) {
        xdbPages[hdrs] = xdbAllocXdbPageHdr(txnId, xdb, cleanoutMode);
        if (unlikely(xdbPages[hdrs] == NULL)) {
            break;
        }

        new (xdbPages[hdrs]) XdbPage();
        xdbPages[hdrs]->txnId = txnId;
        xdbPages[hdrs]->cleanoutMode = cleanoutMode;
    }

    if (unlikely(hdrs == 0)) {
        return 0;
    }

    size_t offset = (uintptr_t) & ((XdbPage *) 0x0)->tupBuf;
    uint64_t numTupBufs = bcBatchAlloc((void **) xdbPages,
                                       hdrs,
                                       offset,
                                       txnId,
                                       slabHint,
                                       cleanoutMode);
    if (unlikely(numTupBufs < hdrs)) {
        for (uint64_t ii = numTupBufs; ii < hdrs; ii++) {
            xdbPutXdbPageHdr(xdbPages[ii]);
        }
    }

    for (uint64_t ii = 0; ii < numTupBufs; ii++) {
        xdbInitXdbPage(xdb,
                       xdbPages[ii],
                       NULL,
                       bcXdbPage_->getBufSize(),
                       XdbUnsortedNormal);
    }

#ifdef XDBPAGEHDRMAP
    for (size_t ii = 0; ii < MIN(numTupBufs, hdrs); ii++) {
        BufCacheDbgToXdbHdrItem *bcXdbItem =
            (BufCacheDbgToXdbHdrItem *) memAllocExt(sizeof(*bcXdbItem),
                                                    moduleName);
        bufCacheDbgToXdbHdrMapLock.lock();
        assert(xdbPages[ii]->tupBuf);
        bcXdbItem->keyPtr = xdbPages[ii]->tupBuf;
        bcXdbItem->valPtr = xdbPages[ii];
        bufCacheDbgToXdbHdrMap->insert(bcXdbItem);
        bufCacheDbgToXdbHdrMapLock.unlock();
    }
#endif

    return numTupBufs;
}

// XXX TODO Need to figure out batching here.
void
XdbMgr::xdbFreeXdbPageBatch(XdbPage **xdbPages, const uint64_t numBufs)
{
    if (numBufs == 0) {
        return;
    }

    for (uint64_t ii = 0; ii < numBufs; ii++) {
        xdbFreeXdbPage(xdbPages[ii]);
    }
}

uint64_t
XdbMgr::getBcXdbLocalBufSize()
{
    return sizeof(Xdb);
}

Status
XdbMgr::init()
{
    assert(instance == NULL);
    instance = (XdbMgr *) memAllocExt(sizeof(XdbMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) XdbMgr();

    return instance->initInternal();
}

// Needs to be called once at startup before creating the very first Xdb
Status
XdbMgr::initInternal()
{
    StatsLib *statsLib = StatsLib::get();

    assert(log2(XdbMinPageAlignment) == XdbHashSlotFlagNumBits);

    // We reserve XdbHashSlotFlagNumBits lsb bits for flags by enforcing that
    // any xdb page size is aligned on a minimum 2^XdbHashSlotFlagNumBits
    // boundary
    assert(bcSize() % XdbMinPageAlignment == 0);

    // XdbAtomicHashSlot must be 64 bits for all the crafty bit field locking
    // and pointer manipulation it uses.
    assertStatic(sizeof(XdbAtomicHashSlot) == sizeof(uintptr_t));

    Status status;

    instance->bcXdbLocal_ = BcHandle::create(BufferCacheObjects::XdbLocal);
    BailIfNull(instance->bcXdbLocal_);

    // Buf$ for the pages that comprise XDB.
    instance->bcXdbPage_ = BcHandle::create(BufferCacheObjects::XdbPage);
    if (instance->bcXdbPage_ == NULL) {
        xSyslog(moduleName, XlogErr, "Failed to allocate XDB pages");
    }
    BailIfNull(instance->bcXdbPage_);
    totalXdbPages_ = instance->bcXdbPage_->getTotalBufs();

    instance->bcOsPageableXdbPage_ =
        BcHandle::create(BufferCacheObjects::XdbPageOsPageable);
    if (instance->bcOsPageableXdbPage_ == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate OS pageable XDB pages");
    }
    BailIfNull(instance->bcOsPageableXdbPage_);

    int ret;
    ret = pthread_rwlock_init(&instance->xdbSlowHashTableLock_, NULL);
    assert(ret == 0);

    status = LibXdbGvm::init();
    BailIfFailed(status);

    status = MergeMgr::init();
    BailIfFailed(status);

    if (XcalarConfig::get()->xdbSerDesMode_ !=
        (uint32_t) XcalarConfig::SerDesMode::Disabled) {
        status = xdbSerDesInit();
        if (status == StatusNoSerDesPath) {
            // Non-fatal error: keep going without serialization
            xSyslog(moduleName,
                    XlogWarn,
                    "Disabling serialization module: %s",
                    strGetFromStatus(status));
            XcalarConfig::get()->xdbSerDesMode_ =
                (uint32_t) XcalarConfig::SerDesMode::Disabled;
        } else {
            // Fatal error: bail...
            BailIfFailed(status);
        }
    }

#ifdef XDBPAGEHDRMAP
    bufCacheDbgToXdbHdrMap =
        (BufCacheDbgToXdbHdrMap *) memAllocExt(sizeof(BufCacheDbgToXdbHdrMap),
                                               moduleName);
    BailIfNull(bufCacheDbgToXdbHdrMap);
    bufCacheDbgToXdbHdrMap =
        new (bufCacheDbgToXdbHdrMap) BufCacheDbgToXdbHdrMap();
    BailIfNull(bufCacheDbgToXdbHdrMap);
#endif

    status = statsLib->initNewStatGroup("XdbMgr", &statsGrpId_, 17);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializationFailures_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumSerializationFailures",
                                         numSerializationFailures_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerDesDropFailures_);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDeserializationFailures_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumDeserializationFailures",
                                         numDeserializationFailures_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumSerDesDropFailures",
                                         numSerDesDropFailures_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializedPages_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializedPages",
                                         numSerializedPages_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializedPagesHWM_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializedPagesHWM",
                                         numSerializedPagesHWM_,
                                         StatUint64,
                                         StatHWM,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDeserializedPages_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numDeserializedPages",
                                         numDeserializedPages_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerDesDroppedPages_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerDesDroppedPages",
                                         numSerDesDroppedPages_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerDesDroppedBytes_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerDesDroppedBytes",
                                         numSerDesDroppedBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializedBatches_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializedBatches",
                                         numSerializedBatches_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDeserializedBatches_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numDeserializedBatches",
                                         numDeserializedBatches_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializedBytes_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializedBytes",
                                         numSerializedBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializedBytesHWM_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializedBytesHWM",
                                         numSerializedBytesHWM_,
                                         StatUint64,
                                         StatHWM,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDeserializedBytes_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numDeserializedBytes",
                                         numDeserializedBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);

    status = statsLib->initStatHandle(&numSerDesDroppedBatches_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerDesDroppedBatches",
                                         numSerDesDroppedBatches_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numSerializationLockRetries_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSerializationLockRetries",
                                         numSerializationLockRetries_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numXdbPageableSlabOom_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numXdbPageableSlabOom",
                                         numXdbPageableSlabOom_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numOsPageableSlabOom_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numOsPageableSlabOom",
                                         numOsPageableSlabOom_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    instance->init_ = true;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (instance->bcXdbPage_ != NULL) {
            BcHandle::destroy(&instance->bcXdbPage_);
            instance->bcXdbPage_ = NULL;
        }
        if (instance->bcOsPageableXdbPage_ != NULL) {
            BcHandle::destroy(&instance->bcOsPageableXdbPage_);
            instance->bcOsPageableXdbPage_ = NULL;
        }
        if (instance->bcXdbLocal_ != NULL) {
            BcHandle::destroy(&instance->bcXdbLocal_);
            instance->bcXdbLocal_ = NULL;
        }
    }

    return status;
}

// Called once at server shutdown. This function waits for all Xdbs
// to be dropped before it frees up Xdb meta data.
void
XdbMgr::destroy()
{
    if (MergeMgr::get()) {
        MergeMgr::get()->destroy();
    }

    if (LibXdbGvm::get()) {
        LibXdbGvm::get()->destroy();
    }

    // Don't need xdbRefCountLock_ as xdbDelete() is guaranteed to be called
    // just once.
    if (xdbRefCount_ != 0) {
        xSyslog(moduleName,
                XlogErr,
                "XdbMgr::destroy found %u XDBs leaked."
                " Just forcefully clean up",
                xdbRefCount_);
        Xdb *xdb = NULL;
        do {
            pthread_rwlock_wrlock(&xdbSlowHashTableLock_);
            XdbHashTable::iterator it = slowHashTable.begin();
            xdb = it.get();
            if (xdb != NULL) {
                XdbId xdbId = xdb->getXdbId();
                pthread_rwlock_unlock(&xdbSlowHashTableLock_);
                xdbDropLocalInternal(xdbId);
            } else {
                pthread_rwlock_unlock(&xdbSlowHashTableLock_);
            }
        } while (xdb != NULL);
    }

    if (bcXdbLocal_ != NULL) {
        BcHandle::destroy(&bcXdbLocal_);
        bcXdbLocal_ = NULL;
    }

    if (bcXdbPage_ != NULL) {
        BcHandle::destroy(&bcXdbPage_);
        bcXdbPage_ = NULL;
    }

    if (bcOsPageableXdbPage_ != NULL) {
        BcHandle::destroy(&bcOsPageableXdbPage_);
        bcOsPageableXdbPage_ = NULL;
    }

    if (init_ == true) {
        pthread_rwlock_destroy(&xdbSlowHashTableLock_);
    }

    init_ = false;
    instance->~XdbMgr();
    memFree(instance);
    instance = NULL;
}

size_t
XdbMgr::bcSize()
{
    return XcalarConfig::get()->xdbPageSize_;
}

void *
XdbMgr::bcAlloc(Xid txnId,
                Status *statusOut,
                SlabHint slabHint,
                BcHandle::BcScanCleanoutMode cleanoutMode,
                BcHandle::BcPageType pageType)
{
    Status status = StatusOk;
    void *buf = NULL;

    if (slabHint == SlabHint::OsPageable &&
        bcOsPageableXdbPage_->getTotalSizeInBytes()) {
        buf = bcOsPageableXdbPage_->allocBuf(
            txnId, &status, cleanoutMode, pageType);
    } else {
        buf = bcXdbPage_->allocBuf(txnId, &status, cleanoutMode, pageType);
    }

    if (buf) {
        goto CommonExit;
    } else {
        buf = bcAllocInternal(txnId, &status, cleanoutMode, pageType);
    }

CommonExit:
    *statusOut = status;
    if (!buf) {
        assert(status != StatusOk);
    }
    return buf;
}

void
XdbMgr::kickPagingOnThreshold()
{
    XcalarConfig *xc = XcalarConfig::get();
    Status status;

    // Check for threshold of pages in the paging list at which it needs to be
    // drained.
    if (xc->xcalarPagingThresholdPct_) {
        uint64_t serialListSize = getSerializationListsSize();
        uint64_t pctVal =
            percent(totalXdbPages_, xc->xcalarPagingThresholdPct_);
        uint64_t numCandidatePages =
            (serialListSize < pctVal) ? serialListSize : pctVal;
        while (numCandidatePages && status == StatusOk) {
            numCandidatePages--;
            status = serializeNext();
        }
    }
}

void *
XdbMgr::bcAllocInternal(Xid txnId,
                        Status *statusOut,
                        BcHandle::BcScanCleanoutMode cleanoutMode,
                        BcHandle::BcPageType pageType)
{
    void *buf = NULL;
    Status status = StatusOk;

    kickPagingOnThreshold();

    // Attempt to allocate from XdbPage buffer cache slab. Note that XdbPage
    // slab may have Xcalar demand paging turned ON.
    do {
        static int64_t traceCount = 0;
        buf = bcXdbPage_->allocBuf(txnId, &status, cleanoutMode, pageType);
        if (unlikely(buf == NULL)) {
            if (XcalarConfig::get()->ctxTracesMode_ &
                (int32_t) XcalarConfig::CtxTraces::DumpOnPagingCounterArg) {
                if (XcalarConfig::get()->ctxTracesArg_ > traceCount++) {
                    dumpAllTraces("trace-counter-arg");
                }
            }

            if (XcalarConfig::get()->ctxTracesMode_ &
                (int32_t) XcalarConfig::CtxTraces::DumpOnPagingModuloArg) {
                if (XcalarConfig::get()->ctxTracesArg_ &&
                    !(traceCount++ % XcalarConfig::get()->ctxTracesArg_)) {
                    dumpAllTraces("trace-moduluo-arg");
                }
            }

            if (XcalarConfig::get()->xdbSerDesMode_ ==
                (uint32_t) XcalarConfig::SerDesMode::Disabled) {
                // DemandPaging is disabled or this allocation isn't on
                // behalf of a batch dataflow.
                status = StatusNoXdbPageBcMem;
                StatsLib::statAtomicIncr64(numXdbPageableSlabOom_);
            } else {
                status = serializeNext();
                if (status == StatusSerializationListEmpty) {
                    status = StatusNoXdbPageBcMem;
                    StatsLib::statAtomicIncr64(numXdbPageableSlabOom_);
#ifdef BUFCACHETRACE
                    if (XcalarConfig::get()->abortOnOom_ == 1) {
                        xSyslog(moduleName,
                                XlogErr,
                                "DEBUG: Aborting on XDB pageable slab OOM");
                        abort();
                    }
#endif
                    if (XcalarConfig::get()->ctxTracesMode_ &
                        (int32_t) XcalarConfig::CtxTraces::DumpOnOom) {
                        dumpAllTraces("trace-main");
                    }
                } else if (status != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Serialization failed for txn %lu : %s",
                            txnId,
                            strGetFromStatus(status));
                }
            }
        }
    } while (buf == NULL && status == StatusOk);

    // If allocation attempt from XdbPage buffer cache slab failed, try
    // allocating from XdbPageOsPageable slab.
    if (buf == NULL) {
        assert(status != StatusOk);
        buf = bcOsPageableXdbPage_->allocBuf(
            txnId, &status, cleanoutMode, pageType);

        if (unlikely(buf == NULL)) {
            StatsLib::statAtomicIncr64(numOsPageableSlabOom_);
#ifdef BUFCACHETRACE
            if (XcalarConfig::get()->abortOnOom_ == 2) {
                xSyslog(moduleName,
                        XlogErr,
                        "DEBUG: Aborting on OS pageable slab OOM");
                abort();
            }
#endif
            if (XcalarConfig::get()->ctxTracesMode_ &
                (int32_t) XcalarConfig::CtxTraces::DumpOnOsPagableOom) {
                dumpAllTraces("trace-ospagable");
            }
        }
    }

    if (!buf) {
        assert(status != StatusOk);
    } else {
    }

    *statusOut = status;
    return buf;
}

void
XdbMgr::bcFree(void *buf)
{
    // Figure which Xdb page slab the buf was allocated from.
    if (bcXdbPage_->inRange(buf)) {
        bcXdbPage_->freeBuf(buf);
    } else {
        assert(bcOsPageableXdbPage_->inRange(buf) &&
               "buf needs to in XdbPage or XdbPageOsPageable slab");
        bcOsPageableXdbPage_->freeBuf(buf);
    }
}

void
XdbMgr::bcMarkCleanoutNotToFree(void *buf)
{
    if (bcXdbPage_->inRange(buf)) {
        bcXdbPage_->markCleanoutNotToFree(buf);
    } else {
        assert(bcOsPageableXdbPage_->inRange(buf) &&
               "buf needs to in XdbPage or XdbPageOsPageable slab");
        bcOsPageableXdbPage_->markCleanoutNotToFree(buf);
    }
}

Status
XdbMgr::bcScanCleanout(Xid txnId)
{
    xSyslog(moduleName, XlogInfo, "Starting bcScanCleanout on txn %lu", txnId);
    bcXdbPage_->scan(txnId, BcHandle::BcPageTypeHdr);
    bcOsPageableXdbPage_->scan(txnId, BcHandle::BcPageTypeHdr);
    bcXdbPage_->scan(txnId, BcHandle::BcPageTypeBody);
    bcOsPageableXdbPage_->scan(txnId, BcHandle::BcPageTypeBody);
    xSyslog(moduleName, XlogInfo, "Completed bcScanCleanout on txn %lu", txnId);
    return StatusOk;
}

uint64_t
XdbMgr::bcBatchAlloc(void **bufs,
                     uint64_t numBufs,
                     size_t offset,
                     Xid txnId,
                     SlabHint slabHint,
                     BcHandle::BcScanCleanoutMode cleanoutMode)
{
    uint64_t allocs = 0;

    if (numBufs == 0) {
        return allocs;
    }

    if (slabHint == SlabHint::OsPageable &&
        bcOsPageableXdbPage_->getTotalSizeInBytes()) {
        allocs = bcOsPageableXdbPage_->batchAlloc(bufs,
                                                  numBufs,
                                                  offset,
                                                  txnId,
                                                  cleanoutMode);
    } else {
        allocs =
            bcXdbPage_->batchAlloc(bufs, numBufs, offset, txnId, cleanoutMode);
    }

    if (allocs) {
        return allocs;
    } else {
        // try invoking serialization using bcAlloc
        Status status;
        void *buf = bcAllocInternal(txnId, &status, cleanoutMode);
        if (status == StatusOk) {
            allocs = 1;
            if (offset == 0) {
                bufs[0] = buf;
            } else {
                assert(bufs[0] != NULL);
                *(void **) ((size_t) bufs[0] + offset) = buf;
            }
        }
    }

    return allocs;
}

void
XdbMgr::bcBatchFree(void **bufs, uint64_t numBufs, size_t offset)
{
    if (numBufs == 0) {
        return;
    }

    void *xdbPool[XdbMgr::BatchFreeSize];
    unsigned xdbPoolNum = 0;
    memZero(xdbPool, sizeof(xdbPool));
    void *osPagePool[XdbMgr::BatchFreeSize];
    unsigned osPagePoolNum = 0;
    memZero(osPagePool, sizeof(osPagePool));

    for (uint64_t ii = 0; ii < numBufs; ii++) {
        void *buf;
        if (offset == 0) {
            buf = bufs[ii];
        } else {
            buf = *(void **) ((size_t) bufs[ii] + offset);
        }
        assert(buf != NULL);
        if (bcXdbPage_->inRange(buf)) {
            xdbPool[xdbPoolNum++] = buf;
        } else {
            assert(bcOsPageableXdbPage_->inRange(buf) &&
                   "buf needs to in XdbPage or XdbPageOsPageable slab");
            osPagePool[osPagePoolNum++] = buf;
        }

        if (xdbPoolNum == XdbMgr::BatchFreeSize) {
            bcXdbPage_->batchFree(xdbPool, xdbPoolNum, 0);
            memZero(xdbPool, sizeof(xdbPool));
            xdbPoolNum = 0;
        }

        if (osPagePoolNum == XdbMgr::BatchFreeSize) {
            bcOsPageableXdbPage_->batchFree(osPagePool, osPagePoolNum, 0);
            memZero(osPagePool, sizeof(osPagePool));
            osPagePoolNum = 0;
        }
    }

    if (xdbPoolNum) {
        bcXdbPage_->batchFree(xdbPool, xdbPoolNum, 0);
    }
    if (osPagePoolNum) {
        bcOsPageableXdbPage_->batchFree(osPagePool, osPagePoolNum, 0);
    }
}

bool
XdbMgr::inRange(void *buf)
{
    return bcXdbPage_->inRange(buf) || bcOsPageableXdbPage_->inRange(buf);
}

uint64_t
XdbMgr::xdbGetNumLocalRows(Xdb *xdb)
{
    uint64_t numRows;
    xdb->lock.lock();
    numRows = xdb->numRows;
    xdb->lock.unlock();
    return numRows;
}

Status
XdbMgr::xdbGetXcalarApiTableMetaGlobal(XdbId xdbId,
                                       XcalarApiGetTableMetaOutput *outMeta)
{
    Xdb *xdb;
    XdbMeta *xdbMeta;
    assert(outMeta != NULL);
    memZero(outMeta, sizeof(*outMeta));
    Status status = StatusUnknown;

    status = xdbGet(xdbId, &xdb, &xdbMeta);
    if (status != StatusOk) {
        return status;
    }

    const NewTupleMeta *tupMeta = xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
    outMeta->numMetas = Config::get()->getActiveNodes();
    outMeta->numValues = tupMeta->getNumFields();
    outMeta->numImmediates = xdbMeta->numImmediates;
    outMeta->numDatasets = xdbMeta->numDatasets;
    outMeta->ordering = xdbMeta->keyOrderings[0];
    outMeta->numKeys = xdbMeta->numKeys;
    for (unsigned ii = 0; ii < xdbMeta->numKeys; ii++) {
        outMeta->keyAttr[ii] = xdbMeta->keyAttr[ii];
    }

    for (unsigned ii = 0; ii < xdbMeta->numDatasets; ii++) {
        verifyOk(strStrlcpy(outMeta->datasets[ii],
                            xdbMeta->datasets[ii]->getDatasetName(),
                            sizeof(outMeta->datasets[ii])));
    }

    size_t numFields = tupMeta->getNumFields();
    for (unsigned ii = 0; ii < numFields; ii++) {
        verifyOk(strStrlcpy(outMeta->valueAttrs[ii].name,
                            xdbMeta->kvNamedMeta.valueNames_[ii],
                            sizeof(outMeta->valueAttrs[ii].name)));
        outMeta->valueAttrs[ii].type = tupMeta->getFieldType(ii);
    }

    return StatusOk;
}

Status
XdbMgr::xdbGetPageDensity(Xdb *xdb,
                          uint64_t *allocated,
                          uint64_t *consumed,
                          bool isPrecise)
{
    if (XcalarConfig::get()->enablePageDensityStats_) {
        xdb->lock.lock();

        if ((xdb->densityState == XdbDensityNotInit) ||
            ((xdb->densityState == XdbDensityNeedUpdate) && isPrecise)) {
            xdbSetDensityStats(xdb);
        }
        xdb->lock.unlock();

        *allocated = xdb->bytesAllocated;
        *consumed = xdb->bytesConsumed;
    } else {
        *allocated = 0;
        *consumed = 0;
    }

    return StatusOk;
}

Status
XdbMgr::xdbGetNumTransPagesReceived(XdbId xdbId, uint64_t *received)
{
    Status status = StatusOk;
    Xdb *xdb;
    *received = 0;

    status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        return status;
    }

    *received = atomicRead64(&xdb->numTransPageRecv);

    return StatusOk;
}

Status
XdbMgr::xdbGetNumTransPages(Xdb *xdb, uint64_t *sent, uint64_t *received)
{
    *sent = atomicRead64(&xdb->numTransPageSent);
    *received = atomicRead64(&xdb->numTransPageRecv);

    return StatusOk;
}

SortedFlags
XdbMgr::xdbGetSortedFlags(Xdb *xdb, uint64_t slotId)
{
    return static_cast<SortedFlags>(
        xdb->hashSlotInfo.hashBase[slotId].sortFlag);
}

void
XdbMgr::xdbCopySlotSortState(Xdb *srcXdb, Xdb *dstXdb, uint64_t slotId)
{
    dstXdb->hashSlotInfo.hashBase[slotId].sortFlag =
        srcXdb->hashSlotInfo.hashBase[slotId].sortFlag;
}

void
XdbMgr::xdbSetSlotSortState(Xdb *xdb, uint64_t slotId, SortedFlags sortFlag)
{
    xdb->hashSlotInfo.hashBase[slotId].sortFlag = sortFlag;
}

Status
XdbMgr::xdbGetHashSlotSkew(XdbId xdbId, int8_t *hashSlotSkew)
{
    Status status = StatusOk;
    Xdb *xdb;
    unsigned numCores = XcSysHelper::get()->getNumOnlineCores();
    *hashSlotSkew = -1;
    unsigned *workerArray = NULL;
    unsigned wLoop = 0;
    unsigned minVal = 0;
    unsigned nextLoopMinVal = UINT_MAX;
    unsigned slotId = 0;
    unsigned tmpSlotId = 0;
    XdbHashSlotInfo *hashSlotInfo;
    unsigned curNumRows;
    unsigned numSlots;
    int64_t sumRows = 0;
    int64_t even = 0;
    int64_t bestPossbleRowsPerCore = 0;
    int64_t bestAchievableSkew = 0;
    int64_t observedSkew = 0;
    int64_t bestCaseRunningTotal = 0;
    int64_t idealRowsForThisCore = 0;
    workerArray = new (std::nothrow) unsigned[numCores];
    BailIfNull(workerArray);

    // Initialize workerArray
    for (unsigned ii = 0; ii < numCores; ii++) {
        workerArray[ii] = 0;
    }

    status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }
    // Dirty read of loadDone is fine
    if (!xdb->loadDone) {
        status = StatusBusy;
        goto CommonExit;
    }

    assert(numCores > 0);
    if (numCores == 1) {
        *hashSlotSkew = 0;
        goto CommonExit;
    }

    xdb->lock.lock();
    numSlots = xdb->hashSlotInfo.hashSlots;
    hashSlotInfo = &xdb->hashSlotInfo;

    // first fill in all elements of workerArray
    // indicating all cores doing work
    while (slotId < numSlots && wLoop < numCores) {
        lockSlot(hashSlotInfo, slotId);
        curNumRows = hashSlotInfo->hashBaseAug[slotId].numRows;
        unlockSlot(hashSlotInfo, slotId);
        // skip any empty slots
        if (curNumRows == 0) {
            slotId++;
            continue;
        }
        workerArray[wLoop] = curNumRows;
        nextLoopMinVal = std::min(nextLoopMinVal, workerArray[wLoop]);
        wLoop++;
        slotId++;
    }

    while (slotId < numSlots) {
        wLoop = 0;
        minVal = nextLoopMinVal;
        nextLoopMinVal = UINT_MAX;
        tmpSlotId = slotId;

        // Do work, clear out minVal amount of
        // pages from each element of workerArray
        while (wLoop < numCores) {
            workerArray[wLoop] -= minVal;
            if (workerArray[wLoop] != 0) {
                nextLoopMinVal = std::min(nextLoopMinVal, workerArray[wLoop]);
            }
            wLoop++;
        }

        wLoop = 0;
        // replace all the zero elements of workerArray
        while (wLoop < numCores) {
            if (workerArray[wLoop] != 0) {
                wLoop += 1;
                continue;
            }

            // drain any empty slots here
            while (slotId < numSlots) {
                lockSlot(hashSlotInfo, slotId);
                curNumRows = hashSlotInfo->hashBaseAug[slotId].numRows;
                unlockSlot(hashSlotInfo, slotId);
                if (curNumRows == 0) {
                    slotId++;
                } else {
                    break;
                }
            }
            if (slotId == numSlots) {
                break;
            }
            // this slot has some records
            workerArray[wLoop] = curNumRows;
            nextLoopMinVal = std::min(nextLoopMinVal, workerArray[wLoop]);
            wLoop++;
            slotId++;
        }
        if (slotId == tmpSlotId) {
            // This should never happen. If this condition is ever met, there is
            // a bug in the above algorithm
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to compute hash slot skew for Xdb %lu, slotId %u, "
                    "tmpSlotId %u, minVal %u",
                    xdbId,
                    slotId,
                    tmpSlotId,
                    minVal);
            xdb->lock.unlock();
            assert(false);
            goto CommonExit;
        }
    }
    xdb->lock.unlock();

    // now just calculate how skewed the resulting workerArray is relative to
    // the best case distribution
    for (uint64_t ii = 0; ii < numCores; ii++) {
        sumRows += workerArray[ii];
    }
    if (sumRows <= 1) {
        *hashSlotSkew = 0;
        goto CommonExit;
    }
    // since we are using integers, we are multiplying by MultFactorForPrecision
    // to keep some precision
    even = MultFactorForPrecision / (int64_t) numCores;

    // We can find the best case theoretical distribution that can be achieved
    // across all cores (bestAchievableSkew) for the remaining work, and then
    // calculate skew based on the deviation from the best case skew.
    // E.g:
    // If workArray = [1, 1, 0, 0, 0, 0, 0, 0], then the observed and best case
    // skew are both ~85. But since this is the best distribution possible, the
    // reported skew is 0 (since relative to best case skew, observed skew is
    // the same).
    bestPossbleRowsPerCore = sumRows / (int64_t) numCores;
    ;
    bestCaseRunningTotal = bestPossbleRowsPerCore * (int64_t) numCores;

    for (uint64_t ii = 0; ii < numCores; ii++) {
        idealRowsForThisCore = bestPossbleRowsPerCore;
        if (bestCaseRunningTotal < sumRows) {
            idealRowsForThisCore += 1;
        }
        bestCaseRunningTotal += idealRowsForThisCore;
        bestAchievableSkew += std::abs(
            ((idealRowsForThisCore * MultFactorForPrecision) / sumRows) - even);

        observedSkew += std::abs(
            ((workerArray[ii] * MultFactorForPrecision) / sumRows) - even);
    }

    if (bestAchievableSkew > observedSkew || observedSkew == 0) {
        // This condition could be possible due to integer rounding
        *hashSlotSkew = 0;
    } else {
        *hashSlotSkew = (int8_t)(((observedSkew - bestAchievableSkew) * 100) /
                                 observedSkew);
    }
CommonExit:
    if (workerArray != NULL) {
        delete[] workerArray;
        workerArray = NULL;
    }
    return status;
}

Status
XdbMgr::xdbGetXcalarApiTableMetaLocal(XdbId xdbId,
                                      XcalarApiTableMeta *outMeta,
                                      bool isPrecise)
{
    Xdb *xdb;
    assert(outMeta != NULL);
    memZero(outMeta, sizeof(*outMeta));
    Status status = StatusUnknown;

    status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        return status;
    }

    // Dirty read of loadDone is fine
    if (!xdb->loadDone) {
        return StatusBusy;
    }

    xdb->lock.lock();

    outMeta->numRows = xdb->numRows;
    outMeta->numPages = xdb->numPages;
    outMeta->numSlots = xdb->hashSlotInfo.hashSlots;
    outMeta->size = outMeta->numPages * bcSize();

    // XXX Note that Hash Aug structures are use and throw during KV insert.
    // Once XDB load is done, these should be destroyed, So this is violation
    // of abstraction.
    for (uint64_t ii = 0; ii < XdbMgr::XdbHashSlotsDefault; ii++) {
        XdbHashSlotAug *hashBaseAug = &xdb->hashSlotInfo.hashBaseAug[ii];
        lockSlot(&xdb->hashSlotInfo, ii);
        outMeta->numRowsPerSlot[ii] = hashBaseAug->numRows;
        outMeta->numPagesPerSlot[ii] = hashBaseAug->numPages;
        unlockSlot(&xdb->hashSlotInfo, ii);
    }

    if ((xdb->densityState == XdbDensityNotInit) ||
        ((xdb->densityState == XdbDensityNeedUpdate) && isPrecise)) {
        xdbSetDensityStats(xdb);
    }

    outMeta->xdbPageAllocatedInBytes = xdb->bytesAllocated;
    outMeta->xdbPageConsumedInBytes = xdb->bytesConsumed;
    outMeta->numTransPageSent = atomicRead64(&xdb->numTransPageSent);
    outMeta->numTransPageRecv = atomicRead64(&xdb->numTransPageRecv);

    xdb->lock.unlock();

    return StatusOk;
}

size_t
XdbMgr::xdbGetSize(XdbId xdbId)
{
    Xdb *xdb = NULL;
    verifyOk(xdbGet(xdbId, &xdb, NULL));
    uint64_t numPages = 0;

    xdb->lock.lock();
    numPages = xdb->numPages;
    xdb->lock.unlock();

    return numPages * bcSize();
}

Status
XdbMgr::xdbGetNumLocalRowsFromXdbId(XdbId xdbId, uint64_t *numRows)
{
    Xdb *xdb;
    assert(numRows != NULL);
    *numRows = 0;

    Status status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        return status;
    }

    if (!xdb->loadDone) {
        return StatusBusy;
    }

    *numRows = xdbGetNumLocalRows(xdb);
    return StatusOk;
}

Status
XdbMgr::xdbGetSizeInBytesFromXdbId(XdbId xdbId, uint64_t *size)
{
    Xdb *xdb;
    assert(size != NULL);
    *size = 0;

    Status status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        return status;
    }

    if (!xdb->loadDone) {
        return StatusBusy;
    }

    xdb->lock.lock();
    *size = xdb->numPages * bcSize();
    xdb->lock.unlock();
    return StatusOk;
}

uint64_t
XdbMgr::xdbGetNumHashSlots(Xdb *xdb)
{
    assert(xdb != NULL);
    assert(xdb->loadDone);

    return xdb->hashSlotInfo.hashSlots;
}

uint64_t
XdbMgr::xdbGetNumRowsInHashSlot(Xdb *xdb, uint64_t hashSlotNum)
{
    assert(xdb != NULL);
    assert(hashSlotNum < xdb->hashSlotInfo.hashSlots);
    // XXX Note that Hash Aug structures are use and throw during KV insert.
    // Once XDB load is done, these should be destroyed, So this is violation
    // of abstraction.
    return xdb->hashSlotInfo.hashBaseAug[hashSlotNum].numRows;
}

uint64_t
XdbMgr::xdbGetHashSlotStartRecord(Xdb *xdb, uint64_t hashSlotNum)
{
    assert(xdb != NULL);
    assert(xdb->loadDone);
    assert(hashSlotNum < xdb->hashSlotInfo.hashSlots);
    // XXX Note that Hash Aug structures are use and throw during KV insert.
    // Once XDB load is done, these should be destroyed, So this is violation
    // of abstraction.
    return xdb->hashSlotInfo.hashBaseAug[hashSlotNum].startRecord;
}

void
XdbMgr::xdbAppendPageChain(Xdb *xdb, uint64_t slot, XdbPage *firstPage)
{
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[slot];
    setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, slot, firstPage);
}

Status
XdbMgr::xdbLoadDoneInt(XdbLoadDoneInput *xdbLoadDoneInput)
{
    Xdb *xdb = NULL;
    XdbMeta *xdbMeta = NULL;
    XdbId xdbId = xdbLoadDoneInput->xdbId;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status status = xdbGet(xdbId, &xdb, &xdbMeta);

    // creation should always be done before loadDone
    assert(status == StatusOk);

    Status statusArray[numNodes];
    MsgEphemeral eph;
    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      xdbLoadDoneInput,
                                      sizeof(*xdbLoadDoneInput),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcXdbLoadDone1,
                                      statusArray,
                                      TwoPcNoInputOrOutput);

    TwoPcNodeProperty twoPcNodeProperty;
    NodeId remoteNodeId;
    if (xdb->globalState & XdbGlobal) {
        // mark load done globally
        twoPcNodeProperty = TwoPcAllNodes;
        remoteNodeId = TwoPcIgnoreNodeId;

        xSyslog(moduleName,
                XlogDebug,
                "XDB %lu load done starting",
                xdb->meta->xdbId);

        TwoPcHandle twoPcHandle;
        status = MsgMgr::get()->twoPc(&twoPcHandle,
                                      MsgTypeId::Msg2pcXdbLoadDone,
                                      TwoPcDoNotReturnHandle,
                                      &eph,
                                      (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                         MsgRecvHdrOnly),
                                      TwoPcSyncCmd,
                                      twoPcNodeProperty,
                                      remoteNodeId,
                                      TwoPcClassNonNested);
        if (status != StatusOk) {
            return status;
        }
        assert(!twoPcHandle.twoPcHandle);

        for (unsigned ii = 0; ii < numNodes; ii++) {
            if (statusArray[ii] != StatusOk) {
                status = statusArray[ii];
                break;
            }
        }

        xSyslog(moduleName,
                XlogDebug,
                "XDB %lu load done complete",
                xdb->meta->xdbId);
    } else {
        status = xdbLoadDoneByXdbInt(xdb, xdbLoadDoneInput);
    }

    return status;
}

Status
XdbMgr::xdbLoadDoneSort(XdbId xdbId, Dag *dag, bool delaySort)
{
    XdbLoadDoneInput xdbLoadDoneInput;

    xdbLoadDoneInput.xdbId = xdbId;
    xdbLoadDoneInput.trackProgress = true;
    xdbLoadDoneInput.delaySort = delaySort;
    xdbLoadDoneInput.dagId = dag->getId();

    return xdbLoadDoneInt(&xdbLoadDoneInput);
}

// This function must be called before the xdb is used. We do not allow
// cursoring through the xdb while load is in progress. Once loaded we
// cannot add any more new rows currently given the complexity this adds
// to cursoring through sorted and unsorted slots at the same time.
Status
XdbMgr::xdbLoadDone(XdbId xdbId)
{
    XdbLoadDoneInput xdbLoadDoneInput;

    xdbLoadDoneInput.xdbId = xdbId;
    xdbLoadDoneInput.trackProgress = false;
    xdbLoadDoneInput.delaySort = false;
    xdbLoadDoneInput.dagId = XidInvalid;

    return xdbLoadDoneInt(&xdbLoadDoneInput);
}

void
XdbMgr::reversePages(Xdb *xdb, uint64_t slotId)
{
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[slotId];
    XdbPage *page = getXdbHashSlotNextPage(xdbHashSlot);
    XdbPage *lastPage = page;

    while (page != NULL) {
        XdbPage *tmpPage = page->hdr.nextPage;

        page->hdr.nextPage = page->hdr.prevPage;
        page->hdr.prevPage = tmpPage;

        lastPage = page;
        page = tmpPage;
    }

    setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, slotId, lastPage);
}

// No locking required, exclusive access is ensured before this function
Status
XdbMgr::xdbLoadDoneByXdbInt(Xdb *xdb, XdbLoadDoneInput *input)
{
    Status status = StatusOk;
    DfFieldValueRangeState range = DfFieldValueRangeInvalid;
    bool emptyXdb = true;

    if (xdb->loadDone) {
        return StatusAlreadyLoadDone;
    }

    status = opProcessLoadInfo(xdb, &xdb->meta->loadInfo);
    BailIfFailed(status);

    for (uint64_t ii = 0; ii < xdb->hashSlotInfo.hashSlots; ++ii) {
        XdbHashSlotAug *xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[ii];
        if (!xdb->keyRangePreset && xdbHashSlotAug->keyRangeValid) {
            // Set Min Key
            DataFormat::updateFieldValueRange(xdb->meta->keyAttr[0].type,
                                              xdbHashSlotAug->minKey,
                                              &xdb->minKey,
                                              &xdb->maxKey,
                                              DontHashString,
                                              range);
            // Set Max Key
            range = DfFieldValueRangeValid;
            DataFormat::updateFieldValueRange(xdb->meta->keyAttr[0].type,
                                              xdbHashSlotAug->maxKey,
                                              &xdb->minKey,
                                              &xdb->maxKey,
                                              DontHashString,
                                              range);
        }

        if (emptyXdb && xdbHashSlotAug->numRows) {
            emptyXdb = false;
        }

        if (xdb->globalState == XdbGlobal) {
            // we want a FIFO ordering, but xdbInsert is LIFO.
            // Reverse pages to get the desired behavior
            reversePages(xdb, ii);
        }
    }

    xdb->densityState = XdbDensityNeedUpdate;

    if (range == DfFieldValueRangeValid) {
        xdb->keyRangeValid = true;
    }

    if (!emptyXdb) {
        XdbInsertKvState xdbInsertKvState;

        if (xdb->meta->keyOrderings[0] == Random) {
            xdbInsertKvState = XdbInsertRandomHash;
        } else if (xdb->meta->keyOrderings[0] == Unordered ||
                   xdb->meta->keyOrderings[0] & PartiallyOrdered) {
            xdbInsertKvState = XdbInsertCrcHash;
        } else {
            xdbInsertKvState = XdbInsertRangeHash;
        }

        if (xdbInsertKvState != XdbInsertRandomHash &&
            xdb->meta->prehashed == false) {
            status = rehashXdb(xdb, xdbInsertKvState);
            BailIfFailed(status);
        }

        if (xdb->meta->keyOrderings[0] & Ordered) {
            status = sortXdb(xdb, input);
            BailIfFailed(status);
        }
    }

    xdbUpdateCounters(xdb);

#ifdef DEBUG
    // verify that all keys have the right index and type
    for (unsigned ii = 0; ii < xdb->meta->numKeys; ii++) {
        int keyIndex = xdb->meta->keyAttr[ii].valueArrayIndex;
        if (keyIndex != NewTupleMeta::DfInvalidIdx) {
            assert(xdb->meta->keyAttr[ii].type ==
                   xdb->tupMeta.getFieldType(keyIndex));
        }
    }
#endif  // DEBUG

CommonExit:
    xdbFreeScratchPadXdbs(xdb);

    if (status == StatusOk) {
        xdb->loadDone = true;
    }

    return status;
}

Status
XdbMgr::xdbLoadDoneScratchXdb(Xdb *scratchXdb)
{
    XdbLoadDoneInput xdbLoadDoneInput;

    xdbLoadDoneInput.xdbId = XdbIdInvalid;
    xdbLoadDoneInput.trackProgress = false;

    return xdbLoadDoneByXdbInt(scratchXdb, &xdbLoadDoneInput);
}

void
XdbMgr::xdbLoadDoneLocal(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    struct XdbLoadDoneInput *loadDoneInput =
        (struct XdbLoadDoneInput *) payload;
    XdbId xdbId = loadDoneInput->xdbId;

    Xdb *xdb;
    status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < xdb->meta->numKeys; ii++) {
        if (!xdb->meta->keyTypeSet[ii]) {
            // this case occurs when we don't have any rows on this node.
            // setKeyType will grab the correct keyType from Dlm node
            status = xdbSetKeyType(xdb->meta, DfUnknown, ii);
            BailIfFailed(status);

            assert(xdb->meta->keyTypeSet[ii]);
        }
    }

    status = xdbLoadDoneByXdbInt(xdb, loadDoneInput);
    BailIfFailed(status);

    if (xdb->numRows == 0) {
        xSyslog(moduleName, XlogDebug, "Xdb %lu has 0 rows", xdb->xdbId);
    }

CommonExit:
    eph->setAckInfo(status, 0);
}

Status
XdbMgr::xdbCreate(XdbId xdbId,
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
                  DhtId dhtId)
{
    return xdbCreate(xdbId,
                     1,
                     &keyName,
                     &keyType,
                     &keyIndex,
                     &ordering,
                     tupMeta,
                     datasetIds,
                     numDatasetIds,
                     immediateNames,
                     numImmediates,
                     fatptrPrefixNames,
                     numFatptrs,
                     globalState,
                     dhtId);
}

// Create a single xdb
Status
XdbMgr::xdbCreate(XdbId xdbId,
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
                  DhtId dhtId)
{
    Status status = StatusUnknown;
    Gvm::Payload *gPayload = NULL;
    XdbAllocLocalParams *xdbAllocLocalParams = NULL;
    unsigned ii;
    uint64_t derivedNumImmediates = 0;
    DhtHandle dhtHandle;
    DhtMgr *dhtMgr = DhtMgr::get();
    bool dhtIdOpened = false;
    bool fieldsPackingSet = false;
    size_t numFields = tupMeta->getNumFields();
    NewTupleMeta::FieldsPacking fieldsPacking =
        NewTupleMeta::FieldsPacking::Unknown;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;

    assert(init_ == true);

    if (numImmediates + numFatptrs > TupleMaxNumValuesPerRecord) {
        return StatusFieldLimitExceeded;
    }

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(XdbAllocLocalParams));
    BailIfNull(gPayload);

    gPayload->init(LibXdbGvm::get()->getGvmIndex(),
                   (uint32_t) LibXdbGvm::Action::Create,
                   sizeof(XdbAllocLocalParams));

    xdbAllocLocalParams = (XdbAllocLocalParams *) gPayload->buf;
    memZero(xdbAllocLocalParams, sizeof(*xdbAllocLocalParams));
    new (xdbAllocLocalParams) XdbAllocLocalParams();

    // If Xdb is local, definitely no associated Dht.
    // But if Xdb is global, might not have a Dht. (e.g. when
    // doing an aggregate, we create a global temporary Xdb)
    // Therefore !globalState => dhtId == DhtInvalidDhtId
    assert((globalState & XdbGlobal) || dhtId == DhtInvalidDhtId);

    xdbAllocLocalParams->xdbId = xdbId;
    xdbAllocLocalParams->hashSlots = xdbHashSlots;

    assert(xcalar::internal::durable::dag::Ordering_IsValid(keyOrdering[0]));
    xdbAllocLocalParams->ordering = keyOrdering[0];
    xdbAllocLocalParams->globalState = globalState;

    if (dhtId != DhtInvalidDhtId) {
        // Protect our access to the DHT.  It is opened here and either
        // closed below if an error occurs or is closed when the Xdb
        // is dropped.
        dhtHandle = dhtMgr->dhtOpen(dhtId, &status);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to open '%lu': %s",
                    dhtId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        dhtIdOpened = true;
        // Save the handle so that it can be closed by xdbDrop.  It gets passed
        // to all the nodes but only one of them will do the drop.
        xdbAllocLocalParams->dhtHandle = dhtHandle;
    }

    xdbAllocLocalParams->dhtId = dhtId;
    xdbAllocLocalParams->numKeys = numKeys;
    for (ii = 0; ii < numKeys; ii++) {
        verifyOk(strStrlcpy(xdbAllocLocalParams->keyAttr[ii].name,
                            keyName[ii],
                            sizeof(xdbAllocLocalParams->keyAttr[ii].name)));
        assert(keyType[ii] != DfScalarObj);

        xdbAllocLocalParams->keyAttr[ii].type = keyType[ii];
        xdbAllocLocalParams->keyAttr[ii].valueArrayIndex = keyIndex[ii];
        assert(
            xcalar::internal::durable::dag::Ordering_IsValid(keyOrdering[ii]));
        xdbAllocLocalParams->keyAttr[ii].ordering = keyOrdering[ii];

#ifdef DEBUG
        // verify that all keys have the right index
        if (keyIndex[ii] != NewTupleMeta::DfInvalidIdx) {
            unsigned immSeen = 0;
            for (int jj = 0; jj < keyIndex[ii]; jj++) {
                if (tupMeta->getFieldType(jj) != DfFatptr) {
                    immSeen++;
                }
            }

            assert(strcmp(keyName[ii], immediateNames[immSeen]) == 0);
        }
#endif  // DEBUG
    }

    assert(numFields <= TupleMaxNumValuesPerRecord);
    xdbAllocLocalParams->tupMeta = *tupMeta;

    for (ii = 0; ii < numFields; ++ii) {
        DfFieldType typeTmp = tupMeta->getFieldType(ii);
        if (typeTmp != DfFatptr) {
            derivedNumImmediates++;
        }
        if (typeTmp == DfUnknown ||
            (fieldsPackingSet &&
             fieldsPacking == NewTupleMeta::FieldsPacking::Unknown)) {
            fieldsPacking = NewTupleMeta::FieldsPacking::Unknown;
        } else if (!DataFormat::fieldTypeIsFixed(typeTmp)) {
            fieldsPacking = NewTupleMeta::FieldsPacking::Variable;
        } else if (fieldsPacking == NewTupleMeta::FieldsPacking::Unknown) {
            assert(!fieldsPackingSet);
            fieldsPacking = NewTupleMeta::FieldsPacking::Fixed;
        }
        if (!fieldsPackingSet) {
            fieldsPackingSet = true;
        }
    }

    // For now, when the field type is DfUnknown, always pick Variable packing
    // style.
    if (fieldsPacking == NewTupleMeta::FieldsPacking::Unknown) {
        fieldsPacking = NewTupleMeta::FieldsPacking::Variable;
    }
    xdbAllocLocalParams->tupMeta.setFieldsPacking(fieldsPacking);

    assert(derivedNumImmediates == numImmediates);

    assert(numDatasetIds <= TupleMaxNumValuesPerRecord);

    xdbAllocLocalParams->numDatasetIds = numDatasetIds;

    for (ii = 0; ii < numDatasetIds; ii++) {
        assert(datasetIds[ii] != DsDatasetIdInvalid);

        xdbAllocLocalParams->datasetIds[ii] = datasetIds[ii];
    }

    xdbAllocLocalParams->numImmediates = numImmediates;

    for (ii = 0; ii < numImmediates; ii++) {
        verifyOk(strStrlcpy(xdbAllocLocalParams->immediateNames[ii],
                            immediateNames[ii],
                            sizeof(xdbAllocLocalParams->immediateNames[ii])));
    }

    xdbAllocLocalParams->numFatptrs = numFatptrs;

    for (ii = 0; ii < numFatptrs; ii++) {
        verifyOk(
            strStrlcpy(xdbAllocLocalParams->fatptrPrefixNames[ii],
                       fatptrPrefixNames[ii],
                       sizeof(xdbAllocLocalParams->fatptrPrefixNames[ii])));
    }

    if (globalState & XdbGlobal) {
        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }

        // If we had a GVM failure, the XDB does not exist on any node.
        // Before exiting, we need to free the DHT reference if we stole it.
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Global XDB creation failed for %lu, reason: %s",
                    xdbId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        // This path allows creation of temporary, local-only XDBs.
        assert(dhtId == DhtInvalidDhtId);
        status = xdbAllocLocal(xdbAllocLocalParams);
    }

CommonExit:
    if (status != StatusOk) {
        if (dhtIdOpened) {
            // Release out protection as an error occurred.
            Status status2 = dhtMgr->dhtClose(dhtHandle, dhtId);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to close ID '%lu': %s",
                        dhtId,
                        strGetFromStatus(status2));
            }
        }
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (gPayload) {
        memFree(gPayload);
        gPayload = NULL;
    }

    return status;
}

void
XdbMgr::xdbGetSlotMinMax(Xdb *xdb,
                         uint64_t slotId,
                         DfFieldType *type,
                         DfFieldValue *min,
                         DfFieldValue *max,
                         bool *valid)
{
    *max = xdb->hashSlotInfo.hashBaseAug[slotId].maxKey;
    *min = xdb->hashSlotInfo.hashBaseAug[slotId].minKey;
    *valid = xdb->hashSlotInfo.hashBaseAug[slotId].keyRangeValid;
    *type = xdb->meta->keyAttr[0].type;
}

void
XdbMgr::xdbCopyMinMax(Xdb *srcXdb, Xdb *dstXdb)
{
    // Update slots min/max
    assert(srcXdb->hashSlotInfo.hashSlots == dstXdb->hashSlotInfo.hashSlots);

    for (uint64_t ii = 0; ii < srcXdb->hashSlotInfo.hashSlots; ++ii) {
        XdbAtomicHashSlot *xdbHashSlot = &srcXdb->hashSlotInfo.hashBase[ii];
        XdbHashSlotAug *srcAug = &srcXdb->hashSlotInfo.hashBaseAug[ii];
        XdbHashSlotAug *dstAug = &dstXdb->hashSlotInfo.hashBaseAug[ii];
        XdbPage *xdbPage = getXdbHashSlotNextPage(xdbHashSlot);
        if (xdbPage != NULL) {
            dstAug->minKey = srcAug->minKey;
            dstAug->maxKey = srcAug->maxKey;
            dstAug->keyRangeValid = srcAug->keyRangeValid;
        }
    }

    dstXdb->minKey = srcXdb->minKey;
    dstXdb->maxKey = srcXdb->maxKey;

    dstXdb->keyRangePreset = true;
    dstXdb->keyRangeValid = true;

    dstXdb->hashDivForRangeBasedHash =
        getHashDiv(dstXdb->meta->keyAttr[0].type,
                   &dstXdb->minKey,
                   &dstXdb->maxKey,
                   dstXdb->keyRangeValid,
                   dstXdb->hashSlotInfo.hashSlots);
}

void
XdbMgr::xdbIncRecvTransPage(Xdb *xdb)
{
    atomicInc64(&xdb->numTransPageRecv);
}

void
XdbMgr::xdbResetDensityStats(Xdb *xdb)
{
    xdb->lock.lock();
    xdb->densityState = XdbDensityNeedUpdate;
    xdb->bytesAllocated = 0;
    xdb->bytesConsumed = 0;
    xdb->lock.unlock();
}

void
XdbMgr::xdbSetDensityStats(Xdb *xdb)
{
    XdbAtomicHashSlot *xdbHashSlot;
    XdbPage *xdbPage;

    assert(!xdb->lock.tryLock());

    xdb->bytesAllocated = 0;
    xdb->bytesConsumed = 0;

    for (uint64_t ii = 0; ii < xdb->hashSlotInfo.hashSlots; ++ii) {
        xdbHashSlot = &xdb->hashSlotInfo.hashBase[ii];

        lockSlot(&xdb->hashSlotInfo, ii);
        if (xdbHashSlot->serialized) {
            unlockSlot(&xdb->hashSlotInfo, ii);
            continue;
        }

        xdbPage = getXdbHashSlotNextPage(xdbHashSlot);

        // XXX: Fix linear scan through XDB pages list
        // XXX2: ...while holding slot lock...
        // XXX3: ...while paging in data...
        while (xdbPage) {
            Status status = xdbPage->getRef(xdb);
            if (status == StatusOk) {
                xdb->bytesAllocated += xdbPage->tupBuf->getBufSize();
                xdb->bytesConsumed += (xdbPage->tupBuf->getBufSize() -
                                       xdbPage->tupBuf->getBytesRemaining());
                pagePutRef(xdb, ii, xdbPage);
            }
            xdbPage = (XdbPage *) xdbPage->hdr.nextPage;
        }
        unlockSlot(&xdb->hashSlotInfo, ii);
    }

    xdb->densityState = XdbDensityInited;
}

XdbMeta *
XdbMgr::allocXdbMeta()
{
    XdbMeta *meta = (XdbMeta *) memAllocAlignedExt(CpuCacheLineSize,
                                                   sizeof(XdbMeta),
                                                   moduleName);
    if (!meta) {
        return NULL;
    }
    memZero(meta, sizeof(XdbMeta));

    new (meta) XdbMeta();
    return meta;
}

void
XdbMgr::freeXdbMeta(XdbMeta *meta)
{
    meta->~XdbMeta();
    memAlignedFree(meta);
}

XdbMeta *
XdbMgr::xdbAllocMeta(XdbId xdbId,
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
                     const NewTupleMeta *tupMeta)
{
    uint64_t immediatesSeen = 0, fatptrsSeen = 0;

    XdbMeta *meta = allocXdbMeta();
    if (!meta) {
        return NULL;
    }

    meta->xdbId = xdbId;
    meta->dhtId = dhtId;
    // invalid primary key or random unordered xdb means we don't need a rehash
    if (keyAttr[0].valueArrayIndex == NewTupleMeta::DfInvalidIdx ||
        (meta->dhtId == XidMgr::XidSystemRandomDht &&
         keyAttr[0].ordering == Unordered)) {
        meta->prehashed = true;
    }

    if (dhtHandle) {
        meta->dhtHandle = *dhtHandle;
    }
    meta->numKeys = numKeys;
    memcpy(meta->keyAttr, keyAttr, numKeys * sizeof(*keyAttr));

    for (unsigned ii = 0; ii < meta->numKeys; ii++) {
        meta->keyIdxOrder[ii] = keyAttr[ii].valueArrayIndex;
        meta->keyTypeSet[ii] = (keyAttr[ii].type != DfUnknown);
        assert(xcalar::internal::durable::dag::Ordering_IsValid(
            keyAttr[ii].ordering));
        meta->keyOrderings[ii] = keyAttr[ii].ordering;
    }
    meta->keyIdxOrder[meta->numKeys] = NewTupleMeta::DfInvalidIdx;

    meta->numDatasets = numDatasetIds;

    for (unsigned ii = 0; ii < numDatasetIds; ii++) {
        // only 1 node needs to grab a global reference on the datasets; the
        // others can just get a local *
        Status status;
        meta->datasets[ii] =
            Dataset::get()->getDatasetFromId(datasetIds[ii], &status);
        assert(meta->datasets[ii] != NULL && status == StatusOk &&
               "guaranteed by contract");
    }

    size_t numFields;

    numFields = tupMeta->getNumFields();
    for (unsigned ii = 0; ii < numFields; ii++) {
        if (tupMeta->getFieldType(ii) != DfFatptr) {
            assert(numImmediates > immediatesSeen);
            verifyOk(strStrlcpy(meta->kvNamedMeta.valueNames_[ii],
                                immediateNames[immediatesSeen],
                                sizeof(meta->kvNamedMeta.valueNames_[ii])));
            immediatesSeen++;
        } else {
            assert(numFatptrs > fatptrsSeen);
            verifyOk(strStrlcpy(meta->kvNamedMeta.valueNames_[ii],
                                fatptrPrefixNames[fatptrsSeen],
                                sizeof(meta->kvNamedMeta.valueNames_[ii])));
            fatptrsSeen++;
        }
    }

    new (&meta->kvNamedMeta.kvMeta_)
        NewKeyValueMeta(tupMeta, meta->keyAttr[0].valueArrayIndex);

    meta->numImmediates = immediatesSeen;
    meta->numFatptrs = fatptrsSeen;

    return meta;
}

// Alloc an xdb on each node
Status
XdbMgr::xdbAllocLocal(void *payload)
{
    Status status = StatusOk;
    Xdb *xdb = NULL;
    bool refUpdated = false;

    assert(init_ == true);

    XdbAllocLocalParams *xdbAllocLocalParams = (XdbAllocLocalParams *) payload;
    const char *immediateNames[xdbAllocLocalParams->numImmediates];
    const char *fatptrPrefixNames[xdbAllocLocalParams->numFatptrs];

    for (unsigned ii = 0; ii < xdbAllocLocalParams->numImmediates; ii++) {
        immediateNames[ii] = xdbAllocLocalParams->immediateNames[ii];
    }

    for (unsigned ii = 0; ii < xdbAllocLocalParams->numFatptrs; ii++) {
        fatptrPrefixNames[ii] = xdbAllocLocalParams->fatptrPrefixNames[ii];
    }

    assert(xdbGet(xdbAllocLocalParams->xdbId, &xdb, NULL) != StatusOk);

    xdb = (Xdb *) bcXdbLocal_->allocBuf(XidInvalid, &status);
    BailIfFailed(status);

    new (xdb) Xdb(xdbAllocLocalParams->xdbId, xdbAllocLocalParams->globalState);

    xdbRefCountLock_.lock();
    ++xdbRefCount_;
    xdbRefCountLock_.unlock();
    refUpdated = true;

    xdb->tupMeta = xdbAllocLocalParams->tupMeta;

    xdb->meta = xdbAllocMeta(xdbAllocLocalParams->xdbId,
                             xdbAllocLocalParams->dhtId,
                             &xdbAllocLocalParams->dhtHandle,
                             xdbAllocLocalParams->numKeys,
                             xdbAllocLocalParams->keyAttr,
                             xdbAllocLocalParams->numDatasetIds,
                             xdbAllocLocalParams->datasetIds,
                             xdbAllocLocalParams->numImmediates,
                             immediateNames,
                             xdbAllocLocalParams->numFatptrs,
                             fatptrPrefixNames,
                             &xdb->tupMeta);
    BailIfNull(xdb->meta);

    if (xdbAllocLocalParams->globalState == XdbGlobal) {
        status = opInitLoadInfo(&xdb->meta->loadInfo,
                                xdb,
                                3 * Runtime::get()->getThreadsCount(
                                        Txn::currentTxn().rtSchedId_));
        BailIfFailed(status);
    } else {
        assert(xdbAllocLocalParams->globalState == XdbLocal);

        // local xdbs do not need additional load info for xdb page pool
        status = opInitLoadInfo(&xdb->meta->loadInfo, xdb, 0);
        BailIfFailed(status);
    }

    // Hash table to implement a range based xdb.
    xdb->hashSlotInfo.hashSlots = xdbAllocLocalParams->hashSlots;

    xdb->hashSlotInfo.hashBase = (XdbAtomicHashSlot *)
        memAllocAlignedExt(XdbMinPageAlignment,
                           sizeof(XdbAtomicHashSlot) *
                               xdbAllocLocalParams->hashSlots,
                           moduleName);
    BailIfNull(xdb->hashSlotInfo.hashBase);

    memZero(xdb->hashSlotInfo.hashBase,
            sizeof(XdbAtomicHashSlot) * xdbAllocLocalParams->hashSlots);

    xdb->hashSlotInfo.hashBaseAug = (XdbHashSlotAug *)
        memAllocAlignedExt(XdbMinPageAlignment,
                           sizeof(XdbHashSlotAug) *
                               xdbAllocLocalParams->hashSlots,
                           moduleName);
    BailIfNull(xdb->hashSlotInfo.hashBaseAug);

    for (int64_t ii = 0; ii < xdbAllocLocalParams->hashSlots; ii++) {
        new (&xdb->hashSlotInfo.hashBaseAug[ii]) XdbHashSlotAug();
    }

    pthread_rwlock_wrlock(&xdbSlowHashTableLock_);
    verifyOk(slowHashTable.insert(xdb));
    pthread_rwlock_unlock(&xdbSlowHashTableLock_);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (xdb != NULL) {
            if (xdb->hashSlotInfo.hashBaseAug != NULL) {
                for (int64_t ii = 0; ii < xdbAllocLocalParams->hashSlots;
                     ii++) {
                    xdb->hashSlotInfo.hashBaseAug[ii].~XdbHashSlotAug();
                }
                memAlignedFree(xdb->hashSlotInfo.hashBaseAug);
                xdb->hashSlotInfo.hashBaseAug = NULL;
            }
            if (xdb->hashSlotInfo.hashBase != NULL) {
                memAlignedFree(xdb->hashSlotInfo.hashBase);
                xdb->hashSlotInfo.hashBase = NULL;
            }
            if (xdb->meta != NULL) {
                opDestroyLoadInfo(&xdb->meta->loadInfo);
                freeXdbMeta(xdb->meta);
                xdb->meta = NULL;
            }
            xdb->~Xdb();
            bcXdbLocal_->freeBuf(xdb);
        }

        if (refUpdated) {
            xdbRefCountLock_.lock();
            assert(xdbRefCount_ > 0);
            --xdbRefCount_;
            xdbRefCountLock_.unlock();
        }
    }

    return status;
}

Status
XdbMgr::xdbCreateScratchPadXdbs(Xdb *dstXdb,
                                const char *keyName,
                                DfFieldType keyType,
                                const NewTupleMeta *tupMeta,
                                char *immediateNames[],
                                unsigned numImmediates)
{
    Status status = StatusOk;
    ScratchPadShared *scratchPadShared = NULL;
    unsigned ii = 0;

    //
    // Scratchpad XDBs should be 2X the number of CPUs. This is needed because
    // the Fiber holding a Scratchpad XDB can be suspended  and we provision for
    // as many CPUs.
    //
    // XXX TODO
    // Xc-13400 Need to rework operators that use scratchPad XDBs especially
    // groupBy to not suspend the fiber holding onto scratchpad XDBs.
    //
    // Also below workaround is only valid for the groupBy case operators
    // execute work in a throughput manner and does not straddle schedulers.
    //
    MsgMgr *msgMgr = MsgMgr::get();
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();
    unsigned numCores = msgMgr->numCoresOnNode(myNodeId);
    static constexpr const unsigned NumCoreMultiplier = 2;
    unsigned numScratchPads = NumCoreMultiplier * numCores;

    ScratchPadInfo *scratchPadInfo =
        (ScratchPadInfo *) memAllocAlignedExt(CpuCacheLineSize,
                                              sizeof(ScratchPadInfo),
                                              moduleName);
    if (scratchPadInfo == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // XXX Use placement new to construct.
    scratchPadInfo->scratchIdx = ScratchPadInfo::ScratchPadOwnerIdx;
    scratchPadInfo->scatchPadShared = NULL;

    scratchPadShared =
        (ScratchPadShared *) memAllocAlignedExt(CpuCacheLineSize,
                                                sizeof(ScratchPadShared),
                                                moduleName);
    if (scratchPadShared == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // XXX Use placement new to construct.
    scratchPadShared->scratchPads = NULL;
    scratchPadShared->numScratchPads = numScratchPads;

    scratchPadShared->scratchPads = (ScratchPadShared::ScratchPad *)
        memAllocAlignedExt(CpuCacheLineSize,
                           sizeof(ScratchPadShared::ScratchPad) *
                               scratchPadShared->numScratchPads,
                           moduleName);
    if (scratchPadShared->scratchPads == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // XXX Use placement new to construct this instead of memZero here.
    memZero(scratchPadShared->scratchPads,
            sizeof(ScratchPadShared::ScratchPad) *
                scratchPadShared->numScratchPads);

    for (; ii < scratchPadShared->numScratchPads; ii++) {
        // XXX Scratch pad XDBs consuming Table ID from XID pool. Seems quite
        // wasteful?
        XdbId scratchPadXdbId = XidMgr::get()->xidGetNext();
        Xdb *scratchXdb = NULL;

        status = xdbCreate(scratchPadXdbId,
                           keyName,
                           keyType,
                           NewTupleMeta::DfInvalidIdx,
                           tupMeta,
                           NULL,
                           0,
                           (const char **) immediateNames,
                           numImmediates,
                           NULL,
                           0,
                           Random,
                           XdbLocal,
                           DhtInvalidDhtId);
        BailIfFailed(status);

        status = xdbGet(scratchPadXdbId, &scratchXdb, NULL);
        assert(status == StatusOk);

        scratchXdb->meta->isScratchPadXdb = true;
        scratchXdb->hashSlotInfo.hashSlots = 1;

        scratchPadShared->scratchPads[ii].used = false;
        scratchPadShared->scratchPads[ii].scratchXdb = scratchXdb;
        scratchXdb->scratchPadInfo =
            (ScratchPadInfo *) memAllocAlignedExt(CpuCacheLineSize,
                                                  sizeof(ScratchPadInfo),
                                                  moduleName);
        if (scratchXdb->scratchPadInfo == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        // XXX Use placement new to construct.
        scratchXdb->scratchPadInfo->scratchIdx = ii;
        scratchXdb->scratchPadInfo->scatchPadShared = scratchPadShared;
    }
    scratchPadInfo->scatchPadShared = scratchPadShared;
    dstXdb->scratchPadInfo = scratchPadInfo;

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (scratchPadShared != NULL) {
            for (unsigned jj = 0; jj < ii; jj++) {
                MsgEphemeral eph;
                Xdb *scratchXdb = scratchPadShared->scratchPads[jj].scratchXdb;

                if (scratchXdb->scratchPadInfo != NULL) {
                    memAlignedFree(scratchXdb->scratchPadInfo);
                    scratchXdb->scratchPadInfo = NULL;
                }
                eph.ephemeral = (void *) scratchXdb->meta->xdbId;
                xdbDropLocal(&eph, NULL);
            }
            memAlignedFree(scratchPadShared->scratchPads);
            memAlignedFree(scratchPadShared);
        }
        if (scratchPadInfo != NULL) {
            memAlignedFree(scratchPadInfo);
        }
        assert(dstXdb->scratchPadInfo == NULL);
    }
    return status;
}

// Drop xdb globally
void
XdbMgr::xdbDrop(XdbId xdbId)
{
    Xdb *xdb;
    XdbMeta *xdbMeta;
    LibNs *libNs = LibNs::get();
    DhtMgr *dhtMgr = DhtMgr::get();

    Status status = xdbGet(xdbId, &xdb, &xdbMeta);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "XDB drop failed for table ID %lu with %s",
                xdbId,
                strGetFromStatus(status));
        return;
    }

    if (xdbMeta->dhtId != DhtInvalidDhtId) {
        assert(xdb->globalState & XdbGlobal);
        status = dhtMgr->dhtClose(xdbMeta->dhtHandle, xdbMeta->dhtId);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to close ID '%lu': %s",
                    xdbMeta->dhtId,
                    strGetFromStatus(status));
        }
        libNs->setHandleInvalid(&xdbMeta->dhtHandle);
        xdbMeta->dhtId = DhtInvalidDhtId;
    }

    MsgEphemeral eph;
    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      NULL,
                                      0,
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcDropXdb1,
                                      (void *) xdbId,
                                      TwoPcNoInputOrOutput);

    if (xdb->globalState & XdbGlobal) {
        TwoPcHandle twoPcHandle;
        status = MsgMgr::get()->twoPc(&twoPcHandle,
                                      MsgTypeId::Msg2pcDropXdb,
                                      TwoPcDoNotReturnHandle,
                                      &eph,
                                      (MsgSendRecvFlags)(MsgSendHdrOnly |
                                                         MsgRecvHdrOnly),
                                      TwoPcSyncCmd,
                                      TwoPcAllNodes,
                                      TwoPcIgnoreNodeId,
                                      TwoPcClassNonNested);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "XDB drop failed for table Id %lu  with %s",
                    xdbId,
                    strGetFromStatus(status));
        } else {
            assert(!twoPcHandle.twoPcHandle);
            xSyslog(moduleName, XlogDebug, "XDB %lu drop complete", xdbId);
        }
    } else {
        xdbDropLocal(&eph, NULL);
    }
}

void
XdbMgr::xdbDropLocalActual(Xdb *xdb)
{
    xdb->lock.lock();
    xdbFreeScratchPadXdbs(xdb);

    CursorManager::get()->removeTableTracking(xdb->xdbId);

    freeXdbPages(xdb);

    if (xdb->hashSlotInfo.hashBase != NULL) {
        memAlignedFree(xdb->hashSlotInfo.hashBase);
        xdb->hashSlotInfo.hashBase = NULL;
    }

    if (xdb->hashSlotInfo.hashBaseAug != NULL) {
        memAlignedFree(xdb->hashSlotInfo.hashBaseAug);
        xdb->hashSlotInfo.hashBaseAug = NULL;
    }

    if (xdb->meta != NULL) {
        opDestroyLoadInfo(&xdb->meta->loadInfo);
        freeXdbMeta(xdb->meta);
        xdb->meta = NULL;
    }

    if (xdb->pageHdrBackingPage != NULL) {
        xdbPutXdbPageHdr(xdb->pageHdrBackingPage);
        xdb->pageHdrBackingPage = NULL;
    }

    xdb->lock.unlock();
    xdb->~Xdb();
    bcXdbLocal_->freeBuf(xdb);
}

void
XdbMgr::xdbDropLocal(MsgEphemeral *eph, void *ignore)
{
    eph->status = xdbDropLocalInternal((XdbId) eph->ephemeral);
}

// Drop xdb locally from gvm
Status
XdbMgr::xdbDropLocalInternal(XdbId xdbId)
{
    Status status = StatusOk;
    assert(init_ == true);

    pthread_rwlock_wrlock(&xdbSlowHashTableLock_);
    Xdb *xdb = slowHashTable.find(xdbId);
    if (xdb == NULL) {
        pthread_rwlock_unlock(&xdbSlowHashTableLock_);
        status = StatusXdbNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Table %lu could not be found. Drop failed.",
                xdbId);
        return status;
    }

    slowHashTable.remove(xdb->xdbId);
    pthread_rwlock_unlock(&xdbSlowHashTableLock_);

    while (atomicRead64(&xdb->refCnt) > 0) {
        // wait for all refs to be dropped
        sysUSleep(100);
    }

    xdbDropLocalActual(xdb);

    xdbRefCountLock_.lock();
    assert(xdbRefCount_ > 0);
    --xdbRefCount_;
    xdbRefCountLock_.unlock();

    return status;
}

uint64_t
XdbMgr::xdbGetHashSlotToInsertPage(Xdb *xdb)
{
    // Here we don't give a f$%^ about the XDB hash function. All that we do
    // here is to take the xdbPage coming in and stick it into a random slot
    // in the XDB table in the cheapest possible manner.
    // XXX Using rand() here for distributing the XDB page between hash slots.
    // May be round robin works better, but we need to lug around yet another
    // state. So let's decide this later.
    return rand() % xdb->hashSlotInfo.hashSlots;
}

// Insert a given XDB page populated with a set of Key-Value pairs into XDB
// table.
// [1] Note that XDB engine provides the data model (optimized for thoughput
// across operations) for us to build FASJ operations.
// [2] Note that a given XDB page could have random Key-Value pairs. Here we
// just do random hashing to stick the XDB page to any hash slot.
//
MustCheck Status
XdbMgr::xdbInsertOnePage(Xdb *xdb,
                         XdbPage *xdbPage,
                         bool keyRangeValid,
                         DfFieldValue minKeyInXdbPage,
                         DfFieldValue maxKeyInXdbPage,
                         bool takeRef)
{
    Status status = StatusOk;
    uint64_t hashSlot = 0;
    XdbHashSlotAug *xdbHashSlotAug = NULL;

    hashSlot = xdbGetHashSlotToInsertPage(xdb);
    xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[hashSlot];
    assert(xdbHashSlotAug != NULL);

    if (takeRef) {
        // We take the ref on insert because eventually setXdbHashSlotNextPage
        // will drop the refcount.
        status = xdbPage->getRef(xdb);
        if (status != StatusOk) {
            return status;
        }
    }

    lockSlot(&xdb->hashSlotInfo, hashSlot);  // Grab XDB slot lock.

    xdbInsertOnePageIntoHashSlot(xdb,
                                 xdbPage,
                                 hashSlot,
                                 keyRangeValid,
                                 &minKeyInXdbPage,
                                 &maxKeyInXdbPage);

    unlockSlot(&xdb->hashSlotInfo, hashSlot);  // Relinquish XDB slot lock.

    return status;
}

// Insert a given XDB page populated with a set of Key-Value pairs into XDB
// table directly into the hash slot. Assume that the caller already holds the
// slot lock.
void
XdbMgr::xdbInsertOnePageIntoHashSlot(Xdb *xdb,
                                     XdbPage *xdbPage,
                                     uint64_t hashSlot,
                                     bool keyRangeValid,
                                     DfFieldValue *minKeyInXdbPage,
                                     DfFieldValue *maxKeyInXdbPage)
{
    XdbPage *xdbPageTemp = NULL;
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[hashSlot];
    XdbHashSlotAug *xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[hashSlot];
    assert(xdbHashSlotAug != NULL);
    uint64_t numRows = xdbPage->tupBuf->getNumTuples();

    assert(xdbHashSlot->casLock);

    xdbPageTemp = getXdbHashSlotNextPage(xdbHashSlot);
    if (xdbPageTemp == NULL) {
        xdbHashSlotAug->numRows = 0;
        xdbHashSlotAug->numPages = 1;
    } else {
        xdbPageTemp->hdr.prevPage = xdbPage;
        xdbHashSlotAug->numPages++;
    }
    xdbHashSlotAug->numRows += numRows;
    xdbPage->hdr.prevPage = NULL;
    xdbPage->hdr.nextPage = xdbPageTemp;
    setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, hashSlot, xdbPage);

    if (xdb->keyRangePreset == false && keyRangeValid) {
        DfFieldValueRangeState range;
        if (xdbHashSlotAug->keyRangeValid == false) {
            range = DfFieldValueRangeInvalid;
            xdbHashSlotAug->keyRangeValid = true;
        } else {
            range = DfFieldValueRangeValid;
        }

        // Update the XDB per slot MinKey with XDB page MinKey.
        DataFormat::updateFieldValueRange(xdb->meta->keyAttr[0].type,
                                          *minKeyInXdbPage,
                                          &xdbHashSlotAug->minKey,
                                          &xdbHashSlotAug->maxKey,
                                          DontHashString,
                                          range);

        // Update the XDB per slot MaxKey with XDB page MaxKey.
        range = DfFieldValueRangeValid;
        DataFormat::updateFieldValueRange(xdb->meta->keyAttr[0].type,
                                          *maxKeyInXdbPage,
                                          &xdbHashSlotAug->minKey,
                                          &xdbHashSlotAug->maxKey,
                                          DontHashString,
                                          range);
    }
}

// Insert a given Key into XDB page. Assume that the caller already holds the
// slot lock.
Status
XdbMgr::xdbInsertKvIntoXdbPage(XdbPage *xdbPage,
                               bool keyRangePreset,
                               bool *keyRangeValid,
                               DfFieldValue *minKey,
                               DfFieldValue *maxKey,
                               DfFieldValue *key,
                               uint64_t *numTuples,
                               NewTupleValues *xdbValueArray,
                               NewKeyValueMeta *keyValueMeta,
                               DfFieldType type)
{
    Status status = StatusOk;

    // Append KV into the XDB page
    status = xdbPage->insertKv(keyValueMeta, xdbValueArray);
    if (status != StatusOk) {
        return status;
    }

    if (numTuples != NULL) {
        (*numTuples)++;
    }

    bool doUpdate = !keyRangePreset;
    if (xdbValueArray->isKeyFNF(keyValueMeta->keyIdx_)) {
        doUpdate = false;
    }

    if (doUpdate) {
        // Update Min & Max per XDB hash slot.
        DfFieldValueRangeState range;
        if ((*keyRangeValid) == false) {
            range = DfFieldValueRangeInvalid;
            (*keyRangeValid) = true;
        } else {
            range = DfFieldValueRangeValid;
        }
        DataFormat::updateFieldValueRange(type,
                                          *key,
                                          minKey,
                                          maxKey,
                                          DoHashString,
                                          range);
    }
    return status;
}

Status
XdbMgr::xdbInsertKvCommon(Xdb *xdb,
                          DfFieldValue *key,
                          NewTupleValues *xdbValueArray,
                          XdbAtomicHashSlot *xdbHashSlot,
                          XdbHashSlotAug *xdbHashSlotAug,
                          uint64_t slotId)
{
    XdbPage *xdbPage = NULL;
    Status status = StatusOk;

    xdbPage = getXdbHashSlotNextPage(xdbHashSlot);
    if (!xdbPage) {
        // First page to be slotted
        xdbHashSlotAug->numRows = 0;

        xdbPage = xdbAllocXdbPage(XidInvalid, xdb);
        if (xdbPage == NULL) {
            return StatusNoXdbPageBcMem;
        }

        xdbInitXdbPage(xdb, xdbPage, NULL, bcSize(), XdbUnsortedNormal);

        status = xdbInsertKvIntoXdbPage(xdbPage,
                                        xdb->keyRangePreset,
                                        &xdbHashSlotAug->keyRangeValid,
                                        &xdbHashSlotAug->minKey,
                                        &xdbHashSlotAug->maxKey,
                                        key,
                                        &xdbHashSlotAug->numRows,
                                        xdbValueArray,
                                        &xdb->meta->kvNamedMeta.kvMeta_,
                                        xdb->meta->keyAttr[0].type);
        if (status != StatusOk) {
            // We should be able to insert a single record into an empty
            // xdb page
            assert(status == StatusNoData);
            status = StatusMaxRowSizeExceeded;

            xdbFreeXdbPage(xdbPage);
            xdbPage = NULL;
            return status;
        }

        setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, slotId, xdbPage);
        xdbHashSlotAug->numPages = 1;
    } else {
        // Insert into existing page
        status = xdbInsertKvIntoXdbPage(xdbPage,
                                        xdb->keyRangePreset,
                                        &xdbHashSlotAug->keyRangeValid,
                                        &xdbHashSlotAug->minKey,
                                        &xdbHashSlotAug->maxKey,
                                        key,
                                        &xdbHashSlotAug->numRows,
                                        xdbValueArray,
                                        &xdb->meta->kvNamedMeta.kvMeta_,
                                        xdb->meta->keyAttr[0].type);

        // If exisitng page is full, alloc a new page
        if (status == StatusNoData) {
            XdbPage *xdbPageTemp = xdbPage;
            xdbPage = xdbAllocXdbPage(XidInvalid, xdb);
            if (xdbPage == NULL) {
                return StatusNoXdbPageBcMem;
            }
            xdbInitXdbPage(xdb,
                           xdbPage,
                           xdbPageTemp,
                           bcSize(),
                           XdbUnsortedNormal);
            xdbPage->xdbId = xdb->xdbId;

            status = xdbInsertKvIntoXdbPage(xdbPage,
                                            xdb->keyRangePreset,
                                            &xdbHashSlotAug->keyRangeValid,
                                            &xdbHashSlotAug->minKey,
                                            &xdbHashSlotAug->maxKey,
                                            key,
                                            &xdbHashSlotAug->numRows,
                                            xdbValueArray,
                                            &xdb->meta->kvNamedMeta.kvMeta_,
                                            xdb->meta->keyAttr[0].type);
            if (status != StatusOk) {
                // We should be able to insert a single record into an empty
                // xdb page
                assert(status == StatusNoData);
                status = StatusMaxRowSizeExceeded;
                xdbFreeXdbPage(xdbPage);
                xdbPage = NULL;
                return status;
            }

            xdbPageTemp->hdr.prevPage = xdbPage;
            setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, slotId, xdbPage);
            xdbHashSlotAug->numPages++;
        } else {
            if (status != StatusOk) {
                return status;
            }
        }
    }

    return status;
}

Status
XdbMgr::xdbInsertKvNoLock(Xdb *xdb,
                          int slotId,
                          DfFieldValue *key,
                          NewTupleValues *xdbValueArray,
                          XdbInsertKvState xdbInsertKvState)
{
    Status status;
    uint64_t hashSlot = 0;
    XdbAtomicHashSlot *xdbHashSlot = NULL;
    XdbHashSlotAug *xdbHashSlotAug = NULL;

    const ssize_t keyIdx = xdb->meta->kvNamedMeta.kvMeta_.keyIdx_;
    if (xdbValueArray->isKeyFNF(keyIdx) &&
        xdbInsertKvState != XdbInsertSlotHash) {
        // if we want descending, send all FNFs to the last slot
        if (xdb->meta->keyOrderings[0] & DescendingFlag) {
            hashSlot = xdb->hashSlotInfo.hashSlots - 1;
        } else {
            hashSlot = 0;
        }
    } else {
        switch (xdbInsertKvState) {
        case XdbInsertCrcHash:
            hashSlot = hashXdbUniform(*key,
                                      xdb->meta->keyAttr[0].type,
                                      xdb->hashSlotInfo.hashSlots);
            break;
        case XdbInsertRangeHash:
            status = hashXdbFixedRange(*key,
                                       xdb->meta->keyAttr[0].type,
                                       xdb->hashSlotInfo.hashSlots,
                                       &xdb->minKey,
                                       xdb->hashDivForRangeBasedHash,
                                       &hashSlot,
                                       xdb->meta->keyOrderings[0]);
            BailIfFailed(status);
            break;
        case XdbInsertSlotHash:
            assert(slotId != -1);
            hashSlot = (uint64_t) slotId;
            break;
        default:
            assert(0);
            status = StatusUnimpl;
            break;
        }
    }

    xdbHashSlot = &xdb->hashSlotInfo.hashBase[hashSlot];
    xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[hashSlot];

    status = xdbInsertKvCommon(xdb,
                               key,
                               xdbValueArray,
                               xdbHashSlot,
                               xdbHashSlotAug,
                               hashSlot);

CommonExit:
    return status;
}

// Insert key and val into xdb.
// No cursor is needed.
// Note: This code needs to be kept super fast as it is the single most
// used workhorse in Xcalar. No other logic is pushed to the limit
// as much as this.
// Loading a xdb is kept single threaded for performance. Doing an atomic op
// for every insert will likely hurt.
// XXX - fix, needs much optimization.
Status
XdbMgr::xdbInsertKv(Xdb *xdb,
                    DfFieldValue *key,
                    NewTupleValues *xdbValueArray,
                    XdbInsertKvState xdbInsertKvState)
{
    uint64_t hashSlot = 0;
    XdbAtomicHashSlot *xdbHashSlot = NULL;
    XdbHashSlotAug *xdbHashSlotAug = NULL;
    Status status = StatusOk;

    const ssize_t keyIdx = xdb->meta->kvNamedMeta.kvMeta_.keyIdx_;
    if (xdbValueArray->isKeyFNF(keyIdx)) {
        // if we want descending, send all FNFs to the last slot
        if (xdb->meta->keyOrderings[0] & DescendingFlag) {
            hashSlot = xdb->hashSlotInfo.hashSlots - 1;
        } else {
            hashSlot = 0;
        }
    } else if (xdbInsertKvState == XdbInsertCrcHash) {
        hashSlot = hashXdbUniform(*key,
                                  xdb->meta->keyAttr[0].type,
                                  xdb->hashSlotInfo.hashSlots);
    } else if (xdbInsertKvState == XdbInsertRangeHash) {
        status = hashXdbFixedRange(*key,
                                   xdb->meta->keyAttr[0].type,
                                   xdb->hashSlotInfo.hashSlots,
                                   &xdb->minKey,
                                   xdb->hashDivForRangeBasedHash,
                                   &hashSlot,
                                   xdb->meta->keyOrderings[0]);
        if (status != StatusOk) {
            return status;
        }
    } else {
        assert(xdbInsertKvState == XdbInsertRandomHash);
        assert(xdb->loadDone == false);
        hashSlot = xdbGetHashSlotToInsertPage(xdb);
    }

    xdbHashSlot = &xdb->hashSlotInfo.hashBase[hashSlot];
    xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[hashSlot];

    lockSlot(&xdb->hashSlotInfo, hashSlot);  // Grab XDB slot lock.
    status = xdbInsertKvCommon(xdb,
                               key,
                               xdbValueArray,
                               xdbHashSlot,
                               xdbHashSlotAug,
                               hashSlot);
    unlockSlot(&xdb->hashSlotInfo, hashSlot);

    return status;
}

Status
XdbMgr::xdbGet(XdbId xdbId, Xdb **xdbPtrReturned, XdbMeta **meta)
{
    Xdb *xdb;
    Status status = StatusXdbNotFound;

    pthread_rwlock_rdlock(&xdbSlowHashTableLock_);
    xdb = slowHashTable.find(xdbId);

    if (xdb) {
        // Check if the world has changed underneath us.
        if (xdbId != xdb->meta->xdbId) {
            status = StatusXdbNotFound;
        } else {
            if (xdbPtrReturned) {
                *xdbPtrReturned = xdb;
            }

            if (meta) {
                *meta = xdb->meta;
            }

            status = StatusOk;
        }
    }
    pthread_rwlock_unlock(&xdbSlowHashTableLock_);

    return status;
}

Status
XdbMgr::xdbGetWithRef(XdbId xdbId, Xdb **xdbPtrReturned, XdbMeta **meta)
{
    Xdb *xdb;
    Status status = StatusXdbNotFound;

    pthread_rwlock_rdlock(&xdbSlowHashTableLock_);
    xdb = slowHashTable.find(xdbId);

    if (xdb) {
        // Check if the world has changed underneath us.
        if (xdbId != xdb->meta->xdbId) {
            status = StatusXdbNotFound;
        } else {
            if (xdbPtrReturned) {
                *xdbPtrReturned = xdb;
            }

            if (meta) {
                *meta = xdb->meta;
            }

            atomicInc64(&xdb->refCnt);
            status = StatusOk;
        }
    }
    pthread_rwlock_unlock(&xdbSlowHashTableLock_);

    return status;
}

void
XdbMgr::xdbPutRef(Xdb *xdb)
{
    atomicDec64(&xdb->refCnt);
}

Status
XdbMgr::cursorGetFirstPageInSlot(Xdb *xdb,
                                 bool sort,
                                 uint64_t slotId,
                                 XdbPage **pageOut,
                                 bool noLock,
                                 bool noPageIn)
{
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[slotId];
    Status status = StatusOk;
    bool slotLocked = false;
    XdbPage *xdbPage = NULL;

    if (!noLock) {
        lockSlot(&xdb->hashSlotInfo, slotId);
        slotLocked = true;
    }

    xdbPage = getXdbHashSlotNextPage(xdbHashSlot);
    if (xdbPage == NULL) {
        // should still mark slot as sorted, even if it's empty
        if (sort && xdbHashSlot->sortFlag == UnsortedFlag) {
            xdbHashSlot->sortFlag = SortedFlag;
        }
        status = StatusNoData;
        goto CommonExit;
    }

    if (!noPageIn) {
        status = xdbPage->getRef(xdb);
        BailIfFailed(status);
    }

    if (sort && xdbHashSlot->sortFlag == UnsortedFlag) {
        status = XdbMgr::get()->sortHashSlotEx(xdb, slotId, &xdbPage);
        BailIfFailed(status);
    }

CommonExit:
    if (slotLocked) {
        unlockSlot(&xdb->hashSlotInfo, slotId);
        slotLocked = false;
    }

    *pageOut = xdbPage;
    return status;
}

// Point the user's cursor to the first xdbPage in the first populated slot.
// Start searching for the first populated slot from slot 0. Since the user
// does not have any control over xdb hashing, we will end up with a "random"
// row. There ought to be no expectation that this is the first logical
// row of the table; this would not be possible without a key.
// Note: xdbPgCursor() returns the first valid page in the xdb.
// Subsequent calls to xdbCursorPageGetNext() give successive xdb pages.
// Note: The cursor is valid only if StatusOk is returned.
// Note: If you want to init the cursor on a specific slot, specify a positive
// slotId. Also, the slot must be populated. This cursor is limited to values
// in this slot
Status
XdbPgCursor::init(XdbId xdbId, Ordering ordering, int64_t slotId)
{
    Xdb *xdb;
    Status status = XdbMgr::get()->xdbGet(xdbId, &xdb, NULL);

    // Ensure that the Xdb has not been dropped.
    if (status != StatusOk) {
        return StatusXdbNotFound;
    }

    return init(xdb, ordering, slotId);
}

Status
XdbPgCursor::init(Xdb *xdb, Ordering ordering, int64_t slotId, bool noSlotLock)
{
    Status status = StatusOk;

    if (!xdb->isResident) {
        return StatusXdbNotResident;
    }

    if (ordering & Ordered && xdb->meta->keyOrderings[0] == Unordered) {
        // We cannot give the caller any more than partial sorting if our
        // xdb is unordered
        if (ordering == Ascending || ordering == Descending) {
            assert(0);
            return StatusOrderingNotSupported;
        } else {
            // If we are a sorted cursor on an unsorted xdb, we will make the
            // xdb partially sorted. Currently only support Ascending
            for (int ii = xdb->meta->numKeys - 1; ii >= 0; ii--) {
                xdb->meta->keyOrderings[ii] = PartialAscending;
            }
            memBarrier();
        }
    }

    XdbPage *xdbPage = NULL;
    uint64_t ii;

    // setup flags
    if (ordering & Ordered) {
        flags_ |= Sorted;
    }

    // not specifying a slot means you can cursor the entire table
    if (slotId == -1) {
        flags_ |= CanCrossSlotBoundary;
    }

    if (noSlotLock) {
        flags_ |= NoLocking;
    }

    if (slotId >= 0) {
        ii = slotId;

        status = XdbMgr::cursorGetFirstPageInSlot(xdb,
                                                  flags_ & Sorted,
                                                  slotId,
                                                  &xdbPage,
                                                  flags_ & NoLocking);
        BailIfFailed(status);
    } else {
        assert(slotId == XdbPgCursorFirstNonEmptySlot);

        for (ii = 0; ii < xdb->hashSlotInfo.hashSlots; ++ii) {
            if (xdb->hashSlotInfo.hashBaseAug[ii].numRows > 0) {
                status = XdbMgr::cursorGetFirstPageInSlot(xdb,
                                                          flags_ & Sorted,
                                                          ii,
                                                          &xdbPage,
                                                          flags_ & NoLocking);
                if (status != StatusOk) {
                    return status;
                }

                break;
            }
        }

        if (ii == xdb->hashSlotInfo.hashSlots) {
            // didn't find a valid slot
            status = StatusNoData;
            goto CommonExit;
        }
    }

    xdb_ = xdb;
    slotId_ = ii;
    xdbPage_ = xdbPage;

CommonExit:
    return status;
}

XdbPgCursor::~XdbPgCursor()
{
    if (xdbPage_ != NULL) {
        XdbMgr::get()->pagePutRef(xdb_, 0, xdbPage_);
        xdbPage_ = NULL;
    }
}

// the hash slot must be locked before calling this function
// returns number of pages freed (-1) if there are active cursors
MustCheck int
XdbMgr::tryFreeUnorderedPages(XdbAtomicHashSlot *xdbHashSlot, Xdb *xdb)
{
    XdbPage *firstPage = getXdbHashSlotNextPage(xdbHashSlot);
    int freedPages = 0;

    if (firstPage == NULL) {
        return freedPages;
    }

    // we set the prev pointer of the first sorted page to the first
    // unsorted page
    XdbPage *firstUnorderedPage = (XdbPage *) firstPage->hdr.prevPage;
    if (unlikely(firstUnorderedPage != NULL)) {
        assert(firstPage->hdr.xdbPageType & SortedFlag);

        XdbPage *tmpPage = firstUnorderedPage;

        // check if there are any active unsorted cursors
        // XXX: we may be safe to delete unordered pages if the unsorted
        // cursors are not referencing any of those pages
        uint64_t refs = CursorManager::get()->getUnsortedRefs(xdb->xdbId);
        if (refs > 0) {
            return -1;
        }

        // No unsorted cursors on the slot, we can free the pages
        // nullify pointers to freed pages
        firstPage->hdr.prevPage = NULL;

        while (firstUnorderedPage) {
            tmpPage = firstUnorderedPage;
            firstUnorderedPage = (XdbPage *) firstUnorderedPage->hdr.nextPage;

            xdbFreeXdbPage(tmpPage);
            freedPages++;
            tmpPage = NULL;
        }
    }

    return freedPages;
}

void
XdbMgr::addSerializationElem(XdbPage *page)
{
    assert(!page->pageRefLock.tryLock());
    assert(!page->hdr.isCompressed);
    serializationList_.add(page);
}

void
XdbMgr::removeSerializationElem(XdbPage *page)
{
    serializationList_.lock(page);
    removeSerializationElemLocked(page);
    serializationList_.unlock(page);
}

void
XdbMgr::removeSerializationElemLocked(XdbPage *page)
{
    assert(!page->pageRefLock.tryLock());
    assert(!page->hdr.isCompressed);
    assert(page == serializationList_.getHeadLocked(page) ||
           (page->pageListPrev != nullptr || page->pageListNext != nullptr));
    serializationList_.removeLocked(page);
}

uint64_t
XdbMgr::getSerializationListsSize()
{
    return serializationList_.getListsSize();
}

Status
XdbMgr::getRefHelper(XdbPage *xdbPageIn, XdbPage::PageState *pageState)
{
    *pageState = XdbPage::Resident;
    if (XcalarConfig::get()->xdbSerDesMode_ ==
        (uint32_t) XcalarConfig::SerDesMode::Disabled) {
        return StatusOk;
    }

    assert(!xdbPageIn->pageRefLock.tryLock());
    assert(atomicRead32(&xdbPageIn->hdr.pageState) != XdbPage::Deserializing);

    int64_t refCount = atomicAdd64(&xdbPageIn->hdr.refCount, 1);
    assert(refCount >= 1);
    if (unlikely(refCount < 1)) {
        // Until refcount return status is fully wired in force an abort (even
        // in prod builds) on refcount error to avoid corruption.
        xSyslog(moduleName,
                XlogErr,
                "Refcount error: xdbpage: %p, refCount: %ld",
                xdbPageIn,
                refCount);
        assert(false);
        return StatusXdbRefCountError;
    }

    saveTrace(TraceOpts::RefInc, xdbPageIn);

    XdbPage::PageState cur;

    if (refCount == 1) {
        // Racing here because page is resident
        cur = (XdbPage::PageState) atomicCmpXchg32(&xdbPageIn->hdr.pageState,
                                                   XdbPage::Serializable,
                                                   XdbPage::Resident);
        if (cur == XdbPage::Serializable) {
            removeSerializationElem(xdbPageIn);
        }
    } else {
        assert(xdbPageIn->pageListPrev == NULL);
        assert(xdbPageIn->pageListNext == NULL);
        cur = (XdbPage::PageState) atomicRead32(&xdbPageIn->hdr.pageState);
    }

    *pageState = cur;
    return StatusOk;
}

void
XdbMgr::pagePutRef(Xdb *xdb, int64_t slotId, XdbPage *page)
{
    pagePutRef(xdb->xdbId, slotId, page);
}

void
XdbMgr::pagePutRef(XdbId xdbId, int64_t slotId, XdbPage *page)
{
    if (XcalarConfig::get()->xdbSerDesMode_ ==
        (uint32_t) XcalarConfig::SerDesMode::Disabled) {
        return;
    }

    page->pageRefLock.lock();

    int64_t refCount = atomicSub64(&page->hdr.refCount, 1);
    assert(refCount >= 0);
    if (unlikely(refCount < 0)) {
        // No need to error out here as we will catch the refcount error and
        // return bad status when/if we ever attempt to get a ref from a bad
        // refcount
        xSyslog(moduleName,
                XlogErr,
                "Refcount error: xdbpage: %p, refCount: %ld",
                page,
                refCount);
    }

    saveTrace(TraceOpts::RefDec, page);

    if (refCount == 0) {
        page->xdbId = xdbId;
        page->slotId = slotId;

        XdbPage::PageState cur =
            (XdbPage::PageState) atomicCmpXchg32(&page->hdr.pageState,
                                                 XdbPage::Resident,
                                                 XdbPage::Serializable);

        if (cur == XdbPage::Resident) {
            addSerializationElem(page);
        }
    }

    page->pageRefLock.unlock();

    kickPagingOnThreshold();
}

Status
XdbMgr::serializeNext()
{
    Status status = StatusOk;
    XdbPage *page;
    size_t numLocks = XcalarConfig::get()->xdbSerDesParallelism_;

    // This is allowed to be racy; we just want to make some effort to
    // round-robin here
    size_t listNum = currSerializationListNum++ % numLocks;
    static constexpr const uint64_t SuspendInterval = 1 << 10;
    uint64_t retryCount = 0;
    Semaphore sem(0);

retry:
    // Ugh...need to respect pageRefLock/serializationList_[listNum] order
    for (size_t idx = 0; idx < numLocks; idx++) {
        serializationList_.lock(listNum);
        page = serializationList_.getHeadLocked(listNum);
        if (page == NULL) {
            serializationList_.unlock(listNum);
            listNum = (++listNum) % numLocks;
        } else {
            break;
        }
    }

    if (page == NULL) {
        return StatusSerializationListEmpty;
    }

    // Since we cannot know the serializationList_[listNum] until after
    // taking the serializationListLock_, we need to violate lock ordering here.
    // So if the lock is already held retry to avoid deadlock
    if (!page->pageRefLock.tryLock()) {
        serializationList_.unlock(listNum);
        StatsLib::statAtomicIncr64(numSerializationLockRetries_);
        if (!(++retryCount % SuspendInterval)) {
            sem.timedWait(USecsPerMSec);
        }
        goto retry;
    }

    assert(page == serializationList_.getHeadLocked(listNum));

    removeSerializationElemLocked(page);

    assert(atomicRead32(&page->hdr.pageState) == XdbPage::Serializable);
    atomicWrite32(&page->hdr.pageState, XdbPage::Serializing);

    serializationList_.unlock(listNum);

    Xid serXid = XidMgr::get()->xidGetNext();
    assert(atomicRead32(&page->hdr.pageState) == XdbPage::Serializing);

    status = xdbSerializeVec(page->xdbId, serXid, &page, 1);

    if (unlikely(status != StatusOk)) {
        // add it back to the serialization list for retry
        assert(atomicRead32(&page->hdr.pageState) == XdbPage::Serializing);
        assert(!page->hdr.isCompressed);
        atomicWrite32(&page->hdr.pageState, XdbPage::Serializable);

        addSerializationElem(page);
    }

    page->pageRefLock.unlock();
    return status;
}

void
XdbMgr::xdbDropSlot(Xdb *xdb, uint64_t slotId, bool locked)
{
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[slotId];
    new (&xdb->hashSlotInfo.hashBaseAug[slotId]) XdbHashSlotAug();

    if (!locked) {
        lockSlot(&xdb->hashSlotInfo, slotId);
    }
    XdbPage *xdbPage = getXdbHashSlotNextPage(xdbHashSlot);
    XdbPage *tmpPage;

    while (xdbPage) {
        tmpPage = xdbPage;
        xdbPage = (XdbPage *) xdbPage->hdr.nextPage;

        xdbFreeXdbPage(tmpPage);
    }

    clearSlot(xdbHashSlot);

    if (!locked) {
        unlockSlot(&xdb->hashSlotInfo, slotId);
    }
}

Status
XdbPgCursor::cleanupUnorderedPages()
{
    Status status = StatusOk;

    // Ensure that the Xdb has not been dropped.
    if (xdb_->meta == NULL) {
        return StatusXdbNotFound;
    }

    XdbAtomicHashSlot *xdbHashSlot = &xdb_->hashSlotInfo.hashBase[slotId_];

    unsigned int ret;

    XdbMgr::lockSlot(&xdb_->hashSlotInfo, slotId_);

    if (xdbHashSlot->sortFlag == SortedFlag) {
        ret = XdbMgr::get()->tryFreeUnorderedPages(xdbHashSlot, xdb_);
        if (ret == CannotFreePagesDueToActiveCursor) {
            status = StatusXdbSlotHasActiveCursor;
        }
    }

    XdbMgr::unlockSlot(&xdb_->hashSlotInfo, slotId_);

    return status;
}

// checks whether or not the next page changes ordering,
// returns true for last page in slot
bool
XdbMgr::onOrderingBoundary(XdbPage *xdbPage)
{
    XdbPage *nextPage = (XdbPage *) xdbPage->hdr.nextPage;
    if (nextPage == NULL) {
        return true;
    }

    return (nextPage->hdr.xdbPageType & SortedFlag) !=
           (xdbPage->hdr.xdbPageType & SortedFlag);
}

Status
XdbPgCursor::seekToRow(NewTuplesBuffer **tupBufOut,
                       uint64_t *startRowOut,
                       uint64_t finalRow)
{
    if (finalRow >= xdb_->numRows) {
        *tupBufOut = NULL;
        return StatusNoData;
    }

    Status status;
    uint64_t startRow = 0;
    uint64_t hashSlot;

    for (hashSlot = 0; hashSlot < xdb_->hashSlotInfo.hashSlots; hashSlot++) {
        XdbHashSlotAug *hashAug = &xdb_->hashSlotInfo.hashBaseAug[hashSlot];

        uint64_t nextRecord;
        if (hashSlot == xdb_->hashSlotInfo.hashSlots - 1) {
            nextRecord = xdb_->numRows;
        } else {
            nextRecord =
                xdb_->hashSlotInfo.hashBaseAug[hashSlot + 1].startRecord;
        }

        if (finalRow >= hashAug->startRecord && finalRow < nextRecord) {
            startRow = hashAug->startRecord;
            break;
        }
    }

    XdbPage *seekPage;
    status = XdbMgr::cursorGetFirstPageInSlot(xdb_,
                                              flags_ & Sorted,
                                              hashSlot,
                                              &seekPage,
                                              flags_ & NoLocking);
    BailIfFailed(status);

    while (status == StatusOk && finalRow >= startRow + seekPage->hdr.numRows) {
        startRow += seekPage->hdr.numRows;

        seekPage = (XdbPage *) seekPage->hdr.nextPage;

        if (unlikely(seekPage == NULL)) {
            status = StatusNoData;
        }
    }

    if (status == StatusOk) {
        slotId_ = hashSlot;
        status = seekPage->getRef(xdb_);
        BailIfFailed(status);
        if (xdbPage_ != NULL) {
            XdbMgr::get()->pagePutRef(xdb_, 0, xdbPage_);
        }
        xdbPage_ = seekPage;

        assert(seekPage->tupBuf != NULL);
        *tupBufOut = seekPage->tupBuf;
        *startRowOut = startRow;
    }

CommonExit:
    return status;
}

// Given a xdbPgCursor return a pointer to the tupBuf within the next xdbPage.
// Note: A XdbPgCursor cursors through xdb one xdbPage at a time. This is
// not the same as a XdbCursor that cursors at key granulatiry (which is
// slow and should not be used). When the cursor hits the last slot it does
// not wrap. We start at the first valid slot on xdbPgCursor() and
// go down the slots until we hit the last slot.
Status
XdbPgCursor::getNextPage(XdbPage **xdbPageOut)
{
    Status status = StatusOk;

    assert(xdb_->meta != NULL);

    bool found = false;
    XdbPage *nextPage;
    uint64_t nextSlot;

    // refCnt on the page pointed to by the cursor can only be dropped if
    // a new page is found. This unfortunately results in 2 unlockSlot()
    // operations when jumping slots.
    // The last condition addresses bug 2371. If we are a cursor
    // on a ordered page and the ordered cursor has not freed the unordered
    // pages, we end up jumping to an unordered page, leading to dup values
    if ((nextPage = (XdbPage *) xdbPage_->hdr.nextPage) &&
        !XdbMgr::onOrderingBoundary(xdbPage_)) {
        // Same slot, next page.
        found = true;
        nextSlot = slotId_;
    } else {
        if (!(flags_ & CanCrossSlotBoundary)) {
            goto CommonExit;
        }

        // Need to go down slots starting from the next slot to find a
        // valid page.
        for (nextSlot = slotId_ + 1; nextSlot < xdb_->hashSlotInfo.hashSlots;
             ++nextSlot) {
            if (xdb_->hashSlotInfo.hashBaseAug[nextSlot].numRows > 0) {
                // Call with noPageIn=true to defer the getRef until after
                // we've put the current ref in CommonExit
                status = XdbMgr::cursorGetFirstPageInSlot(xdb_,
                                                          flags_ & Sorted,
                                                          nextSlot,
                                                          &nextPage,
                                                          flags_ & NoLocking,
                                                          true);
                BailIfFailed(status);

                found = true;
                break;
            }
        }
    }

CommonExit:
    if (found) {
        if (xdbPage_) {
            assert(!xdbPage_->hdr.isCompressed);
            XdbMgr::get()->pagePutRef(xdb_->xdbId, 0, xdbPage_);
            xdbPage_ = NULL;  // Ref given up. No longer safe to refer
        }
        assert(status == StatusOk);

        status = nextPage->getRef(xdb_);
        if (likely(status == StatusOk)) {
            slotId_ = nextSlot;
            xdbPage_ = nextPage;

            assert(atomicRead32(&nextPage->hdr.pageState) == XdbPage::Resident);
            assert(!nextPage->hdr.isCompressed);
            assert(nextPage->tupBuf != NULL);

            *xdbPageOut = nextPage;
        } else {
            *xdbPageOut = NULL;
        }
    } else {
        // Note: slotIndex and xdbPage within xdbPgCursor are unchanged.
        *xdbPageOut = NULL;
        status = StatusNoData;
    }

    return status;
}

Status
XdbPgCursor::getNextTupBuf(NewTuplesBuffer **tupBufOut)
{
    XdbPage *xdbPage;
    Status status = getNextPage(&xdbPage);
    if (status == StatusOk) {
        *tupBufOut = xdbPage->tupBuf;
    } else {
        *tupBufOut = NULL;
    }

    return status;
}

void
XdbMgr::xdbSetKeyTypeHelper(DfFieldType newKeyType, unsigned keyNum, Xdb *xdb)
{
    XdbMeta *meta = xdb->meta;
    int keyIdx = meta->keyAttr[keyNum].valueArrayIndex;
    bool fixedPacking = true;

    meta->keyTypeSet[keyNum] = true;
    assert(keyIdx != NewTupleMeta::DfInvalidIdx);
    assert(meta->keyAttr[keyNum].valueArrayIndex != NewTupleMeta::DfInvalidIdx);
    xdb->tupMeta.setFieldType(newKeyType,
                              meta->keyAttr[keyNum].valueArrayIndex);

    const NewTupleMeta *tupMeta = &xdb->tupMeta;
    new (&meta->kvNamedMeta.kvMeta_)
        NewKeyValueMeta(tupMeta, meta->keyAttr[0].valueArrayIndex);

    // Check if we do know if all the keys now are fixed size, so the XBD
    // packing style can be updated as Fixed.
    for (unsigned ii = 0; ii < meta->numKeys; ii++) {
        if (meta->keyAttr[ii].valueArrayIndex == NewTupleMeta::DfInvalidIdx) {
            continue;
        }
        if (!DataFormat::fieldTypeIsFixed(meta->keyAttr[ii].type)) {
            fixedPacking = false;
            break;
        }
    }

    assert(xdb->tupMeta.getFieldsPacking() ==
           NewTupleMeta::FieldsPacking::Variable);
    if (fixedPacking) {
        // Since default is Variable packing if types are Unknown, change to
        // Fixed packing now.
        xdb->tupMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);
    }

    // make sure all of the above gets commited before we mark the key type
    // as resolved
    memBarrier();

    meta->keyAttr[keyNum].type = newKeyType;
}

Status
XdbMgr::xdbSetKeyType(XdbMeta *xdbMeta,
                      DfFieldType newKeyType,
                      unsigned keyNum,
                      bool tryLock)
{
    XdbTableAndKey xdbTableAndKey;
    xdbTableAndKey.xdbId = xdbMeta->xdbId;
    xdbTableAndKey.keyNum = keyNum;
    xdbTableAndKey.newKeyType = newKeyType;
    xdbTableAndKey.status = StatusUnknown;
    Status status;
    Config *config = Config::get();
    MsgEphemeral eph;
    Xdb *xdb = NULL;
    assert(newKeyType != DfScalarObj);

    if (tryLock && !xdbMeta->setKeyMutex.tryLock()) {
        return StatusAgain;
    } else if (!tryLock) {
        xdbMeta->setKeyMutex.lock();
    }

    atomicWrite32(&xdbMeta->setKeyTypeInProgress, 1);

    // once we get the lock, check again as another thread on this node may
    // have just set it ahead of us
    if (xdbMeta->keyAttr[keyNum].type != DfUnknown) {
        // another thread set it, so we're all set
        status = StatusOk;
        goto CommonExit;
    }

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      &xdbTableAndKey,
                                      sizeof(xdbTableAndKey),
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::Msg2pcDlmResolveKeyType1,
                                      &xdbTableAndKey,
                                      TwoPcMemCopyInput);

    // Set Key type is called already from the context of a 2PC and is hence
    // of nested type.
    TwoPcHandle twoPcHandle;
    status =
        MsgMgr::get()->twoPc(&twoPcHandle,
                             MsgTypeId::Msg2pcDlmResolveKeyType,
                             TwoPcDoNotReturnHandle,
                             &eph,
                             (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                MsgRecvHdrPlusPayload),
                             TwoPcSyncCmd,
                             TwoPcSingleNode,
                             config->getMyDlmNode((MsgTypeId) xdbMeta->xdbId),
                             TwoPcClassNested);
    BailIfFailed(status);

    assert(xdbTableAndKey.status != StatusUnknown);

    if ((xdbTableAndKey.status != StatusOk) &&
        (xdbTableAndKey.status != StatusXdbKeyTypeAlreadySet)) {
        status = xdbTableAndKey.status;
        goto CommonExit;
    }

    if (xdbTableAndKey.status == StatusXdbKeyTypeAlreadySet) {
        // treat this as success locally
        xdbTableAndKey.status = StatusOk;
    }

    status = xdbGet(xdbMeta->xdbId, &xdb, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // key type already set for dlm node
    if (config->getMyNodeId() !=
        config->getMyDlmNode((MsgTypeId) xdbMeta->xdbId)) {
        assert(xdb->meta->keyAttr[keyNum].type == DfUnknown);
        xdbSetKeyTypeHelper(xdbTableAndKey.newKeyType, keyNum, xdb);
    }

CommonExit:
    xdbMeta->processQueuedSchedFsmTp();
    atomicWrite32(&xdbMeta->setKeyTypeInProgress, 0);
    xdbMeta->setKeyMutex.unlock();

    return status;
}

void
XdbMgr::xdbResolveKeyTypeDLMCompletion(MsgEphemeral *eph, void *payload)
{
    XdbTableAndKey *out = (XdbTableAndKey *) eph->ephemeral;

    if ((eph->status == StatusOk) ||
        (eph->status == StatusXdbKeyTypeAlreadySet)) {
        memcpy(out, payload, sizeof(XdbTableAndKey));
    }

    out->status = eph->status;
}

void
XdbMgr::xdbResolveKeyTypeDLM(MsgEphemeral *eph, void *payload)
{
    assert(payload);

    XdbTableAndKey *xdbTableAndKey = (XdbTableAndKey *) payload;
    XdbId xdbId = xdbTableAndKey->xdbId;
    DfFieldType newKeyType = xdbTableAndKey->newKeyType;
    unsigned keyNum = xdbTableAndKey->keyNum;
    size_t payloadLength = 0;

    Status status = StatusOk;
    Xdb *xdb = NULL;
    status = xdbGet(xdbId, &xdb, NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    xdb->lock.lock();
    if (!xdb->meta->keyTypeSet[keyNum]) {
        assert(xdb->loadDone == false);
        xdbSetKeyTypeHelper(newKeyType, keyNum, xdb);
    } else {
        status = StatusXdbKeyTypeAlreadySet;
        xdbTableAndKey->newKeyType = xdb->meta->keyAttr[keyNum].type;
    }
    xdb->lock.unlock();

    payloadLength = sizeof(XdbTableAndKey);

CommonExit:
    xcAssertIf(((status != StatusOk) && (status != StatusXdbKeyTypeAlreadySet)),
               (payloadLength == 0));

    eph->setAckInfo(status, payloadLength);
}

// this function can only be called when there are no more outstanding cursors
// don't need to lock
void
XdbMgr::freeXdbPages(Xdb *xdb)
{
    XdbAtomicHashSlot *xdbHashSlot;
    XdbPage *xdbPage, *xdbPageTemp;
    unsigned int freedPages = 0;
    unsigned int ret;

    assert(!CursorManager::get()->getTotalRefs(xdb->xdbId));

    for (uint64_t ii = 0;
         ii < xdb->hashSlotInfo.hashSlots && xdb->hashSlotInfo.hashBase != NULL;
         ++ii) {
        xdbHashSlot = &xdb->hashSlotInfo.hashBase[ii];

        ret = tryFreeUnorderedPages(xdbHashSlot, xdb);
        assert(ret != CannotFreePagesDueToActiveCursor);

        xdbPage = getXdbHashSlotNextPage(xdbHashSlot);

        while (xdbPage) {
            // Save next page state before deleting page
            xdbPageTemp = (XdbPage *) xdbPage->hdr.nextPage;
            xdbFreeXdbPage(xdbPage);
            freedPages++;

            xdbPage = xdbPageTemp;
        }

        clearSlot(xdbHashSlot);
    }

    xdb->numRows = 0;
    xdb->numPages = 0;
}

void
XdbMgr::xdbReinitScratchPadXdb(Xdb *scratchPadXdb)
{
    freeXdbPages(scratchPadXdb);
    scratchPadXdb->loadDone = false;
}

XdbId
XdbMgr::xdbGetXdbId(Xdb *xdb)
{
    return xdb->meta->xdbId;
}

XdbMeta *
XdbMgr::xdbGetMeta(Xdb *xdb)
{
    return xdb->meta;
}

unsigned
XdbMgr::getNumFields(XdbMeta *xdbMeta)
{
    return xdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
}

uint64_t
XdbMgr::getSlotStartRecord(Xdb *xdb, uint64_t slotId)
{
    return xdb->hashSlotInfo.hashBaseAug[slotId].startRecord;
}

void
XdbMgr::xdbReleaseScratchPadXdb(Xdb *scratchPadXdb)
{
    if (scratchPadXdb == NULL) {
        return;
    }

    ScratchPadInfo *scratchPadInfo = scratchPadXdb->scratchPadInfo;
    unsigned scatchIdx = scratchPadInfo->scratchIdx;
    ScratchPadShared *scratchPadShared = scratchPadInfo->scatchPadShared;

    assert(scatchIdx != ScratchPadInfo::ScratchPadOwnerIdx);
    assert(scatchIdx < scratchPadShared->numScratchPads);
    assert(scratchPadShared->scratchPads[scatchIdx].used == true);

    scratchPadShared->scratchPads[scatchIdx].used = false;
    memBarrier();
}

Xdb *
XdbMgr::xdbGetScratchPadXdb(Xdb *xdb)
{
    Xdb *scratchPadXdb = NULL;
    unsigned ii;
    if (xdb->scratchPadInfo == NULL) {
        return NULL;
    }

    ScratchPadInfo *scratchPadInfo = xdb->scratchPadInfo;
    unsigned scatchIdx = scratchPadInfo->scratchIdx;
    ScratchPadShared *scratchPadShared = scratchPadInfo->scatchPadShared;

    assert(scatchIdx == ScratchPadInfo::ScratchPadOwnerIdx);

    xdb->lock.lock();

    // XXX With new run time changes, we can reduce the scratch pad XDB count.
    // find an unused scratch xdb. Note that there will always be an unused
    // scratch xdb as the number of scratch xdbs is equal to the number of
    // threads in this wq.

    ii = rand() % scratchPadShared->numScratchPads;
    while (true) {
        if (scratchPadShared->scratchPads[ii].used == false) {
            scratchPadXdb = scratchPadShared->scratchPads[ii].scratchXdb;
            break;
        }
        ii++;
        if (ii == scratchPadShared->numScratchPads) {
            ii = 0;
        }
    }

    assert(scratchPadShared->scratchPads[ii].used == false);
    scratchPadShared->scratchPads[ii].used = true;
    assert(scratchPadXdb->scratchPadInfo->scratchIdx == ii);

    xdb->lock.unlock();

    return scratchPadXdb;
}

// Sort all pages in a hash slot.
Status
XdbMgr::sortHashSlotEx(Xdb *xdb, uint64_t hashSlot, XdbPage **firstPageOut)
{
    XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[hashSlot];
    XdbHashSlotAug *xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[hashSlot];

    XdbPage *sortedPage = NULL;
    XdbPage *firstPage = NULL;
    HashMergeSort hashMergeSort;
    Status status = StatusOk;
    int ret;

    firstPage = getXdbHashSlotNextPage(xdbHashSlot);
    BailIfNullWith(firstPage, StatusNoData);

    status =
        hashMergeSort.sort(firstPage,
                           (NewKeyValueMeta *) &xdb->meta->kvNamedMeta.kvMeta_,
                           xdbHashSlotAug->minKey,
                           xdbHashSlotAug->maxKey,
                           xdbHashSlotAug->numRows,
                           hashSlot,
                           xdb,
                           &sortedPage,
                           &xdbHashSlotAug->numPages);
    BailIfFailed(status);

    // set first sorted page's prev page pointer to first unordered page
    sortedPage->hdr.prevPage = firstPage;

    clearSlot(xdbHashSlot);
    setXdbHashSlotNextPage(xdbHashSlot, xdb->xdbId, hashSlot, sortedPage);

    xdbHashSlot->sortFlag = SortedFlag;

    // Unsotted pages can have dangling cursors. Do not check return
    // value of tryFreeUnorderedPages() as it can return
    // CannotFreePagesDueToActiveCursor which is permitted.
    ret = tryFreeUnorderedPages(xdbHashSlot, xdb);
    (void) (ret);

    status = sortedPage->getRef(xdb);
    BailIfFailed(status);

CommonExit:
    *firstPageOut = sortedPage;
    return status;
}

void
XdbMgr::printArrayForDebug(void *arrIn, uint64_t count)
{
    uint64_t ii;
    uint64_t *arr = (uint64_t *) arrIn;

    for (ii = 0; ii < count; ++ii) {
        xSyslog(moduleName,
                XlogDebug,
                "%llu ",
                (long long unsigned int) *((uint64_t *) arr[ii]));
    }

    xSyslog(moduleName, XlogDebug, "\n");
    xSyslog(moduleName,
            XlogDebug,
            "printArrayForDebug %llu",
            (long long unsigned int) ii);
}

DfFieldValue
XdbMgr::getHashDiv(DfFieldType keyType,
                   const DfFieldValue *minKey,
                   const DfFieldValue *maxKey,
                   bool valid,
                   uint64_t hashSlots)
{
    DFPUtils *dfp = DFPUtils::get();
    DfFieldValue hashDiv;

    if (!valid) {
        if (keyType != DfMoney) {
            hashDiv.float64Val = (float64_t) UINT64_MAX / hashSlots;
        } else {
            Status status;
            XlrDfp dfpHashSlots;

            status = dfp->xlrDfpUInt64ToNumeric(&dfpHashSlots, hashSlots);
            // We should never have that many slots that overflow the string
            // representation.
            assert(status == StatusOk);

            dfp->xlrDfpUInt32ToNumeric(&hashDiv.numericVal, UINT32_MAX);
            dfp->xlrDfpDiv(&hashDiv.numericVal,
                           &hashDiv.numericVal,
                           &dfpHashSlots);
        }

        return hashDiv;
    }

    switch (keyType) {
    case DfFloat32:
        hashDiv.float64Val =
            (float64_t)(maxKey->float32Val - minKey->float32Val) / hashSlots;
        break;
    case DfInt32:
        hashDiv.float64Val =
            (float64_t)(maxKey->int32Val - minKey->int32Val) / hashSlots;
        break;
    case DfUInt32:
        hashDiv.float64Val =
            (float64_t)(maxKey->uint32Val - minKey->uint32Val) / hashSlots;
        break;
    case DfFloat64:
        hashDiv.float64Val =
            maxKey->float64Val / hashSlots - minKey->float64Val / hashSlots;
        break;
    case DfInt64:
        hashDiv.float64Val = (float64_t) maxKey->int64Val / hashSlots -
                             (float64_t) minKey->int64Val / hashSlots;
        break;
    case DfTimespec:
        hashDiv.float64Val = (float64_t) maxKey->timeVal.ms / hashSlots -
                             (float64_t) minKey->timeVal.ms / hashSlots;
        break;
    case DfUInt64:
        hashDiv.float64Val =
            (float64_t)(maxKey->uint64Val - minKey->uint64Val) / hashSlots;
        break;
    case DfMoney: {
        Status status;
        XlrDfp dfpHashSlots;

        status = dfp->xlrDfpUInt64ToNumeric(&dfpHashSlots, hashSlots);
        // We should never have that many slots that overflow the string
        // representation.
        assert(status == StatusOk);

        dfp->xlrDfpSub(&hashDiv.numericVal,
                       &maxKey->numericVal,
                       &minKey->numericVal);
        dfp->xlrDfpDiv(&hashDiv.numericVal, &hashDiv.numericVal, &dfpHashSlots);
    } break;
    case DfUnknown:
    case DfBoolean:
    case DfNull:
        hashDiv.float64Val = 1.f;
        break;
    case DfString:
        // XXX relies on string hack in xdbInsertKv().  maxKey and minKey
        // store the hash of the string, not the string itself
        hashDiv.float64Val =
            (float64_t)(maxKey->uint64Val - minKey->uint64Val) / hashSlots;
        break;
    case DfBlob:
    case DfMixed:
    case DfFatptr:
    default:
        assert(0);
        hashDiv.float64Val = (float64_t) UINT64_MAX / hashSlots;
    }

    if (keyType != DfMoney && hashDiv.float64Val == 0) {
        hashDiv.float64Val = 1.f;
    } else if (keyType == DfMoney && dfp->xlrDfpIsZero(&hashDiv.numericVal)) {
        dfp->xlrDfpInt32ToNumeric(&hashDiv.numericVal, 1);
    }

    // Hash div will be NaN when minKey or maxKey is NaN.
    if (keyType != DfMoney && hashDiv.float64Val != hashDiv.float64Val) {
        hashDiv.float64Val = 1.f;
    } else if (keyType == DfMoney && dfp->xlrDfpIsNan(&hashDiv.numericVal)) {
        dfp->xlrDfpInt32ToNumeric(&hashDiv.numericVal, 1);
    }

    return hashDiv;
}

Status
XdbMgr::replicateXdbMetaData(Xdb *xdbSrc, Xdb *xdbDst)
{
    Status status = StatusOk;

    xdbDst->xdbId = xdbSrc->xdbId;
    xdbDst->meta = xdbSrc->meta;
    xdbDst->tupMeta = xdbSrc->tupMeta;
    xdbDst->numRows = xdbSrc->numRows;
    xdbDst->loadDone = xdbSrc->loadDone;
    memcpy(xdbDst->meta->keyTypeSet,
           xdbSrc->meta->keyTypeSet,
           sizeof(xdbDst->meta->keyTypeSet));

    xdbDst->keyRangePreset = false;
    xdbDst->keyRangeValid = true;
    xdbDst->maxKey = xdbSrc->maxKey;
    xdbDst->minKey = xdbSrc->minKey;

    // XXX: assign hashslots to max for now
    xdbDst->hashSlotInfo.hashSlots = xdbHashSlots;

    // Calculate a new xdbDst->hashSlotInfo.hashSlots based on range. This will
    // allow for a better hash distribution hopefully, haha. Ensure that at the
    // minimum we have two hash slots to deal with trivial case of hashing
    // booleans in two separate slots.
    xdbDst->hashDivForRangeBasedHash =
        getHashDiv(xdbDst->meta->keyAttr[0].type,
                   &xdbDst->minKey,
                   &xdbDst->maxKey,
                   xdbDst->keyRangeValid,
                   xdbDst->hashSlotInfo.hashSlots);
    assert(xdbDst->hashSlotInfo.hashSlots <= xdbHashSlots + 2);

    xdbDst->hashSlotInfo.hashBase = NULL;
    xdbDst->hashSlotInfo.hashBaseAug = NULL;

    xdbDst->hashSlotInfo.hashBase = (XdbAtomicHashSlot *)
        memAllocAlignedExt(XdbMinPageAlignment,
                           sizeof(XdbAtomicHashSlot) *
                               xdbDst->hashSlotInfo.hashSlots,
                           moduleName);
    if (xdbDst->hashSlotInfo.hashBase == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(xdbDst->hashSlotInfo.hashBase,
            sizeof(XdbAtomicHashSlot) * xdbDst->hashSlotInfo.hashSlots);

    xdbDst->hashSlotInfo.hashBaseAug = (XdbHashSlotAug *)
        memAllocAlignedExt(XdbMinPageAlignment,
                           sizeof(XdbHashSlotAug) *
                               xdbDst->hashSlotInfo.hashSlots,
                           moduleName);
    if (xdbDst->hashSlotInfo.hashBaseAug == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    for (uint64_t ii = 0; ii < xdbDst->hashSlotInfo.hashSlots; ii++) {
        new (&xdbDst->hashSlotInfo.hashBaseAug[ii]) XdbHashSlotAug();
    }

CommonExit:
    if (status != StatusOk) {
        if (xdbDst->hashSlotInfo.hashBase != NULL) {
            memAlignedFree(xdbDst->hashSlotInfo.hashBase);
            xdbDst->hashSlotInfo.hashBase = NULL;
        }
        if (xdbDst->hashSlotInfo.hashBaseAug != NULL) {
            for (uint64_t ii = 0; ii < xdbDst->hashSlotInfo.hashSlots; ii++) {
                xdbDst->hashSlotInfo.hashBaseAug[ii].~XdbHashSlotAug();
            }
            memAlignedFree(xdbDst->hashSlotInfo.hashBaseAug);
            xdbDst->hashSlotInfo.hashBaseAug = NULL;
        }
    }
    return status;
}

void
XdbMgr::xdbUpdateCounters(Xdb *xdb)
{
    xdb->numPages = 0;
    xdb->numRows = 0;
    for (uint64_t ii = 0; ii < xdb->hashSlotInfo.hashSlots; ++ii) {
        XdbHashSlotAug *xdbHashSlotAug = &xdb->hashSlotInfo.hashBaseAug[ii];
        xdbHashSlotAug->startRecord = xdb->numRows;
        xdb->numPages += xdbHashSlotAug->numPages;
        xdb->numRows += xdbHashSlotAug->numRows;
    }
}

Status
RehashHelper::allocPageCache(uint64_t slots, uint64_t size)
{
    Status status = StatusOk;
    pageCache_ = (XdbPageCacheEntry **) memAllocExt(slots * sizeof(*pageCache_),
                                                    moduleName);
    BailIfNull(pageCache_);
    memZero(pageCache_, slots * sizeof(*pageCache_));

    pageCacheSlots_ = slots;
    maxPageCacheSize_ = size;
    pageCacheSize_ = 0;

CommonExit:
    return status;
}

void
RehashHelper::freePageCache()
{
    XdbPageCacheEntry *entry;
    XdbPageCacheEntry *entryTmp;

    for (unsigned ii = 0; ii < pageCacheSlots_; ii++) {
        entry = pageCache_[ii];

        while (entry != NULL) {
            entryTmp = entry->next;

            if (entry->xdbPage != NULL) {
                xdbMgr_->xdbFreeXdbPage(entry->xdbPage);
            }
            memFree(entry);

            entry = entryTmp;
        }

        pageCache_[ii] = NULL;
    }
    memFree(pageCache_);
}

Status
RehashHelper::flushPageCache()
{
    Status status = StatusOk;
    NewKeyValueMeta *kvMeta = &this->kvMeta;
    NewKeyValueEntry kvEntry(kvMeta);
    XdbPageCacheEntry *entry;
    XdbPageCacheEntry *entryTmp;
    bool retIsValid;

    // for each cache entry, insert each row into dstXdb
    for (unsigned ii = 0; ii < pageCacheSlots_; ii++) {
        entry = pageCache_[ii];

        while (entry != NULL) {
            entryTmp = entry->next;

            if (entry->xdbPage != NULL) {
                NewTuplesCursor tupCursor(entry->xdbPage->tupBuf);

                while (tupCursor.getNext(kvMeta->tupMeta_, &kvEntry.tuple_) ==
                           StatusOk &&
                       status == StatusOk) {
                    DfFieldValue key = kvEntry.getKey(&retIsValid);
                    status = xdbMgr_->xdbInsertKv(this->dstXdb,
                                                  &key,
                                                  &kvEntry.tuple_,
                                                  this->xdbInsertKvState);
                    // don't bail here, we want to finish processing all
                    // entries in the page cache
                }

                xdbMgr_->xdbFreeXdbPage(entry->xdbPage);
            }

            memFree(entry);
            pageCacheSize_--;

            entry = entryTmp;
        }

        pageCache_[ii] = NULL;
    }

    return status;
}

Status
RehashHelper::processPageCache(uint64_t hashSlot, NewKeyValueEntry *kvEntry)
{
    // check if we have a page cache entry for this slot
    Status status;
    uint64_t cacheSlot = hashSlot % pageCacheSlots_;
    XdbPageCacheEntry *cacheEntry = pageCache_[cacheSlot];
    bool found = false;
    DfFieldValue key;
    bool retIsValid;

    while (cacheEntry != NULL) {
        if (cacheEntry->hashSlot == hashSlot) {
            found = true;
            break;
        }
        cacheEntry = cacheEntry->next;
    }

    if (found) {
        // Insert into page cache
        key = kvEntry->getKey(&retIsValid);
        status = xdbMgr_->xdbInsertKvIntoXdbPage(cacheEntry->xdbPage,
                                                 false,
                                                 &cacheEntry->keyRangeValid,
                                                 &cacheEntry->minKey,
                                                 &cacheEntry->maxKey,
                                                 &key,
                                                 NULL,
                                                 &kvEntry->tuple_,
                                                 &this->kvMeta,
                                                 this->srcXdb->meta->keyAttr[0]
                                                     .type);
        if (status == StatusNoData) {
            // Page is full, insert it into dstXdb and alloc a new page
            xdbMgr_->lockSlot(&this->dstXdb->hashSlotInfo,
                              hashSlot);  // Grab XDB slot lock.
            xdbMgr_->xdbInsertOnePageIntoHashSlot(this->dstXdb,
                                                  cacheEntry->xdbPage,
                                                  hashSlot,
                                                  cacheEntry->keyRangeValid,
                                                  &cacheEntry->minKey,
                                                  &cacheEntry->maxKey);
            xdbMgr_->unlockSlot(&this->dstXdb->hashSlotInfo, hashSlot);

            cacheEntry->xdbPage =
                xdbMgr_->xdbAllocXdbPage(XidInvalid, this->dstXdb);
            BailIfNullWith(cacheEntry->xdbPage, StatusNoXdbPageBcMem);

            xdbMgr_->xdbInitXdbPage(this->dstXdb,
                                    cacheEntry->xdbPage,
                                    NULL,
                                    XdbMgr::bcSize(),
                                    XdbUnsortedNormal);

            key = kvEntry->getKey(&retIsValid);
            status =
                xdbMgr_->xdbInsertKvIntoXdbPage(cacheEntry->xdbPage,
                                                false,
                                                &cacheEntry->keyRangeValid,
                                                &cacheEntry->minKey,
                                                &cacheEntry->maxKey,
                                                &key,
                                                NULL,
                                                &kvEntry->tuple_,
                                                &this->kvMeta,
                                                this->srcXdb->meta->keyAttr[0]
                                                    .type);
            assert(status == StatusOk);  // just alloced new xdbPage
        }
    } else {
        if (pageCacheSize_ == maxPageCacheSize_) {
            status = StatusPageCacheFull;
            goto CommonExit;
        }

        // Create a new page cache entry for this slot
        XdbPageCacheEntry *newCacheEntry =
            (XdbPageCacheEntry *) memAllocExt(sizeof(*newCacheEntry),
                                              moduleName);
        BailIfNull(newCacheEntry);

        newCacheEntry->xdbPage =
            xdbMgr_->xdbAllocXdbPage(XidInvalid, this->dstXdb);
        if (newCacheEntry->xdbPage == NULL) {
            memFree(newCacheEntry);
            newCacheEntry = NULL;
            status = StatusNoXdbPageBcMem;
            goto CommonExit;
        }

        newCacheEntry->keyRangeValid = false;
        newCacheEntry->next = pageCache_[cacheSlot];
        newCacheEntry->hashSlot = hashSlot;

        pageCache_[cacheSlot] = newCacheEntry;
        pageCacheSize_++;

        xdbMgr_->xdbInitXdbPage(this->dstXdb,
                                newCacheEntry->xdbPage,
                                NULL,
                                XdbMgr::bcSize(),
                                XdbUnsortedNormal);

        key = kvEntry->getKey(&retIsValid);
        status = xdbMgr_->xdbInsertKvIntoXdbPage(newCacheEntry->xdbPage,
                                                 false,
                                                 &newCacheEntry->keyRangeValid,
                                                 &newCacheEntry->minKey,
                                                 &newCacheEntry->maxKey,
                                                 &key,
                                                 NULL,
                                                 &kvEntry->tuple_,
                                                 &this->kvMeta,
                                                 this->srcXdb->meta->keyAttr[0]
                                                     .type);
        assert(status == StatusOk);  // just alloced new page
    }

CommonExit:
    return status;
}

Status
RehashHelper::rehashOneXdbPage(XdbPage *xdbPage)
{
    Status status = StatusOk;
    DfFieldType keyType = this->dstXdb->meta->keyAttr[0].type;
    uint64_t slots = this->dstXdb->hashSlotInfo.hashSlots;
    uint64_t hashSlot = 0;
    NewKeyValueMeta *kvMeta = &this->kvMeta;
    NewKeyValueEntry kvEntry(kvMeta);
    NewTuplesCursor tupCursor(xdbPage->tupBuf);
    DfFieldValue key;
    bool retIsValid;

    while ((status = tupCursor.getNext(kvMeta->tupMeta_, &kvEntry.tuple_)) ==
           StatusOk) {
        key = kvEntry.getKey(&retIsValid);
        if (kvEntry.tuple_.isKeyFNF(this->kvMeta.keyIdx_)) {
            // if we want descending, send all FNFs to the last slot
            if (this->dstXdb->meta->keyOrderings[0] & DescendingFlag) {
                hashSlot = this->dstXdb->hashSlotInfo.hashSlots - 1;
            } else {
                hashSlot = 0;
            }
        } else {
            switch (this->xdbInsertKvState) {
            case XdbInsertCrcHash:
                hashSlot = xdbMgr_->hashXdbUniform(key, keyType, slots);
                break;
            case XdbInsertRangeHash:
                status =
                    xdbMgr_->hashXdbFixedRange(key,
                                               keyType,
                                               slots,
                                               &this->dstXdb->minKey,
                                               this->dstXdb
                                                   ->hashDivForRangeBasedHash,
                                               &hashSlot,
                                               this->dstXdb->meta
                                                   ->keyOrderings[0]);
                BailIfFailed(status);
                break;
            default:
                assert(0);
                status = StatusUnimpl;
                break;
            }
        }

        if (XcalarConfig::get()->enablePageCache_) {
            status = processPageCache(hashSlot, &kvEntry);
            if (status == StatusOk) {
                continue;
            }

            if (status != StatusPageCacheFull) {
                goto CommonExit;
            }
        }

        // If the page cache is full, grab the slot lock on the dstXdb and
        // do normal kvInsert
        status = xdbMgr_->xdbInsertKv(this->dstXdb,
                                      &key,
                                      &kvEntry.tuple_,
                                      this->xdbInsertKvState);
        BailIfFailed(status);
    }

    assert(status == StatusNoData);
    status = StatusOk;

CommonExit:
    xdbMgr_->xdbFreeXdbPage(xdbPage);
    xdbPage = NULL;

    return status;
}

void
RehashHelper::run()
{
    Status status = StatusOk;
    status = this->trackHelpers->helperStart();
    uint64_t beginWorkItem, totalWork;

    if (status == StatusAllWorkDone) {
        // Nothing to do
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    // Stagger starting point of workers, but don't let them complete until all
    // the work is done. This should hopefully make the workers compete less and
    // cooperate more.
    totalWork = this->trackHelpers->getWorkUnitsTotal();
    beginWorkItem =
        (totalWork / this->trackHelpers->getHelpersTotal()) * this->workerId;
    for (uint64_t ii = beginWorkItem, workDoneCount = 0;
         workDoneCount < totalWork;
         workDoneCount++, ii = (beginWorkItem + workDoneCount) % totalWork) {
        XdbAtomicHashSlot *xdbHashSlot = NULL;
        XdbPage *xdbPage = NULL;
        XdbPage *xdbPageTemp = NULL;

        if (false == this->trackHelpers->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            this->trackHelpers->workUnitComplete(ii);
            continue;
        }

        xdbHashSlot = &this->srcXdb->hashSlotInfo.hashBase[ii];

        xdbMgr_->lockSlot(&this->srcXdb->hashSlotInfo, ii);

        xdbPage = xdbMgr_->getXdbHashSlotNextPage(xdbHashSlot);
        xdbPageTemp = NULL;

        while (xdbPage != NULL) {
            // Save next page state before enqueuing work
            xdbPageTemp = (XdbPage *) xdbPage->hdr.nextPage;

            status = xdbPage->getRef(this->srcXdb);

            // Whether or not we successfully grab a ref, we assume
            // after this step, the xdbPage is freed
            if (status == StatusOk) {
                status = rehashOneXdbPage(xdbPage);
            } else {
                xdbMgr_->xdbFreeXdbPage(xdbPage);
            }
            xdbPage = NULL;

            if (status != StatusOk) {
                if (xdbPageTemp) {
                    xdbPageTemp->hdr.prevPage = NULL;
                }
                xdbMgr_->clearSlot(xdbHashSlot);
                xdbMgr_->setXdbHashSlotNextPage(xdbHashSlot,
                                                this->srcXdb->xdbId,
                                                ii,
                                                xdbPageTemp);
                this->trackHelpers->workUnitComplete(ii);
                break;
            }
            xdbPage = xdbPageTemp;
        }
        if (status == StatusOk) {
            xdbMgr_->clearSlot(xdbHashSlot);
            this->trackHelpers->workUnitComplete(ii);
        }
        xdbMgr_->unlockSlot(&this->srcXdb->hashSlotInfo, ii);
    }

    if (status == StatusOk && XcalarConfig::get()->enablePageCache_) {
        status = flushPageCache();
    }
CommonExit:
    this->trackHelpers->helperDone(status);
}

void
RehashHelper::done()
{
    freePageCache();
    this->~RehashHelper();
    memFree(this);
}

RehashHelper *
RehashHelper::setUp(Xdb *srcXdb,
                    Xdb *dstXdb,
                    XdbInsertKvState xdbInsertKvState,
                    uint64_t workerId,
                    TrackHelpers *trackHelpers)
{
    XdbMgr *xdbMgr = XdbMgr::get();
    RehashHelper *rehashHelper =
        (RehashHelper *) memAllocExt(sizeof(RehashHelper), moduleName);
    if (rehashHelper == NULL) {
        return NULL;
    }
    rehashHelper = new (rehashHelper) RehashHelper();
    Status status = rehashHelper->allocPageCache(XdbMgr::xdbHashSlots,
                                                 XdbMgr::xdbHashSlots);
    if (status == StatusNoMem) {
        return NULL;
    }
    assert(status == StatusOk);

    rehashHelper->srcXdb = srcXdb;
    rehashHelper->dstXdb = dstXdb;
    assert(xdbInsertKvState != XdbInsertRandomHash);
    rehashHelper->xdbInsertKvState = xdbInsertKvState;
    assert(workerId < trackHelpers->getHelpersTotal());
    rehashHelper->masterWorker = (workerId == 0);
    rehashHelper->workerId = workerId;
    rehashHelper->trackHelpers = trackHelpers;
    new (&rehashHelper->kvMeta)
        NewKeyValueMeta(&rehashHelper->srcXdb->tupMeta,
                        rehashHelper->srcXdb->meta->keyAttr[0].valueArrayIndex);
    rehashHelper->xdbMgr_ = xdbMgr;
    return rehashHelper;
}

void
RehashHelper::tearDown(RehashHelper **rehashHelper)
{
    assert((*rehashHelper)->trackHelpers == NULL);
    (*rehashHelper)->~RehashHelper();
    memFree((*rehashHelper));
    (*rehashHelper) = NULL;
}

// This function takes the original xdb and rehashes it into a new xdb. It
// frees memory from the old xdb, create a new xdbDst which has its own
// meta data. Then finally it assigns xdbDst's meta data back to the
// original xdb.
Status
XdbMgr::rehashXdb(Xdb *xdb, XdbInsertKvState xdbInsertKvState)
{
    Xdb xdbDst(xdb->xdbId, xdb->globalState);
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    RehashHelper **rehashHelpers = NULL;
    TrackHelpers *trackHelpers = NULL;
    unsigned numScheds =
        Runtime::get()->getThreadsCount(Txn::currentTxn().rtSchedId_);

    assert(xdbInsertKvState != XdbInsertRandomHash);

    status = replicateXdbMetaData(xdb, &xdbDst);
    if (status != StatusOk) {
        goto CommonExit;
    }

    trackHelpers = TrackHelpers::setUp(&workerStatus,
                                       numScheds,
                                       xdb->hashSlotInfo.hashSlots);
    BailIfNull(trackHelpers);

    rehashHelpers = new (std::nothrow) RehashHelper *[numScheds];
    BailIfNull(rehashHelpers);
    for (uint64_t ii = 0; ii < numScheds; ii++) {
        rehashHelpers[ii] = NULL;
    }

    for (uint64_t ii = 0; ii < numScheds; ii++) {
        rehashHelpers[ii] = RehashHelper::setUp(xdb,
                                                &xdbDst,
                                                xdbInsertKvState,
                                                ii,
                                                trackHelpers);
        BailIfNull(rehashHelpers[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) rehashHelpers,
                                           numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (rehashHelpers != NULL) {
        for (uint64_t ii = 0; ii < numScheds; ii++) {
            if (rehashHelpers[ii] != NULL) {
                memFree(rehashHelpers[ii]);
                rehashHelpers[ii] = NULL;
            }
        }
        delete[] rehashHelpers;
        rehashHelpers = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Xdb Id: %lu Failing rehash: %s",
                xdb->xdbId,
                strGetFromStatus(status));

        freeXdbPages(xdb);
        freeXdbPages(&xdbDst);
    }

    // Free hash table and hashBaseAug from original xdb.
    memAlignedFree(xdb->hashSlotInfo.hashBase);
    memAlignedFree(xdb->hashSlotInfo.hashBaseAug);

    // Update original xdb with new allocations that were made for xdbDst
    xdb->hashSlotInfo.hashBase = xdbDst.hashSlotInfo.hashBase;
    xdb->hashSlotInfo.hashBaseAug = xdbDst.hashSlotInfo.hashBaseAug;
    xdb->hashSlotInfo.hashSlots = xdbDst.hashSlotInfo.hashSlots;
    if (xdb->pageHdrBackingPage != NULL) {
        xdbPutXdbPageHdr(xdb->pageHdrBackingPage);
        xdb->pageHdrBackingPage = xdbDst.pageHdrBackingPage;
    }

    // Update original xdb with computed values needed for hashXdbFixedRange()
    // Implicit copy assignment operator is used and arrays are handled
    // correctly.
    xdb->hashDivForRangeBasedHash = xdbDst.hashDivForRangeBasedHash;

    return status;
}

class SortXdbHelper : public Schedulable
{
  public:
    Xdb *xdb = NULL;
    static constexpr uint64_t WorkerIdInvalid = 0xbaadcafec001c0de;
    uint64_t workerId = WorkerIdInvalid;
    bool masterWorker = false;
    TrackHelpers *trackHelpers = NULL;
    OpStatus *opStatus = NULL;

    void func();
    static SortXdbHelper *setUp(Xdb *xdb,
                                uint64_t workerId,
                                TrackHelpers *trackHelpers,
                                OpStatus *opStatus);
    static void tearDown(SortXdbHelper **sortXdbHelper);
    virtual void run();
    virtual void done();

  private:
    // Keep this private. Use setUp instead.
    SortXdbHelper() : Schedulable("SortXdbHelper") {}

    // Keep this private. Use tearDown instead.
    virtual ~SortXdbHelper() {}

    // Sorry folks! No operator overloading allowed here.
    SortXdbHelper(const SortXdbHelper &) = delete;
    SortXdbHelper &operator=(const SortXdbHelper &) = delete;
};

SortXdbHelper *
SortXdbHelper::setUp(Xdb *xdb,
                     uint64_t workerId,
                     TrackHelpers *trackHelpers,
                     OpStatus *opStatus)
{
    SortXdbHelper *sortXdbHelper =
        (SortXdbHelper *) memAllocExt(sizeof(SortXdbHelper), moduleName);
    if (sortXdbHelper == NULL) {
        return NULL;
    }
    sortXdbHelper = new (sortXdbHelper) SortXdbHelper();
    sortXdbHelper->xdb = xdb;
    assert(workerId < trackHelpers->getHelpersTotal());
    sortXdbHelper->workerId = workerId;
    sortXdbHelper->masterWorker = (workerId == 0);
    sortXdbHelper->trackHelpers = trackHelpers;
    sortXdbHelper->opStatus = opStatus;
    return sortXdbHelper;
}

void
SortXdbHelper::tearDown(SortXdbHelper **sortXdbHelper)
{
    assert((*sortXdbHelper)->trackHelpers == NULL);
    (*sortXdbHelper)->~SortXdbHelper();
    memFree((*sortXdbHelper));
    (*sortXdbHelper) = NULL;
}

void
SortXdbHelper::run()
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t beginWorkItem, totalWork;

    status = this->trackHelpers->helperStart();
    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    // Stagger starting point of workers, but don't let them complete until all
    // the work is done. This should hopefully make the workers compete less and
    // cooperate more.
    totalWork = this->trackHelpers->getWorkUnitsTotal();
    beginWorkItem =
        (totalWork / this->trackHelpers->getHelpersTotal()) * this->workerId;
    for (uint64_t ii = beginWorkItem, workDoneCount = 0;
         workDoneCount < totalWork;
         workDoneCount++, ii = (beginWorkItem + workDoneCount) % totalWork) {
        if (false == this->trackHelpers->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            this->trackHelpers->workUnitComplete(ii);
            continue;
        }

        XdbAtomicHashSlot *xdbHashSlot = &xdb->hashSlotInfo.hashBase[ii];
        XdbPage *xdbPage = xdbMgr->getXdbHashSlotNextPage(xdbHashSlot);
        if (xdbPage == NULL) {
            // StatusNoData
            this->trackHelpers->workUnitComplete(ii);
            continue;
        }

        xdbMgr->lockSlot(&xdb->hashSlotInfo, ii);
        if (xdbHashSlot->sortFlag == UnsortedFlag) {
            Status status2 = xdbMgr->sortHashSlotEx(xdb, ii, &xdbPage);
            xdbMgr->unlockSlot(&xdb->hashSlotInfo, ii);

            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Sort Xdb %lu slot %lu failed: %s",
                        xdb->getXdbId(),
                        ii,
                        strGetFromStatus(status));
                status = status2;
                this->trackHelpers->workUnitComplete(ii);
                continue;
            }
        } else {
            xdbMgr->unlockSlot(&xdb->hashSlotInfo, ii);
        }

        if (opStatus != NULL) {
            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        xdb->hashSlotInfo.hashBaseAug[ii].numRows);
        }

        this->trackHelpers->workUnitComplete(ii);
    }

CommonExit:
    this->trackHelpers->helperDone(status);
}

void
SortXdbHelper::done()
{
    this->~SortXdbHelper();
    memFree(this);  // Must be just done before return
}

Status
XdbMgr::sortXdb(Xdb *xdb, XdbLoadDoneInput *input)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    SortXdbHelper **sortXdbHelpers = NULL;
    OpStatus *opStatus;
    DagLib *dagLib = DagLib::get();
    uint64_t numScheds =
        Runtime::get()->getThreadsCount(Txn::currentTxn().rtSchedId_);

    TrackHelpers *trackHelpers =
        TrackHelpers::setUp(&workerStatus,
                            numScheds,
                            xdb->hashSlotInfo.hashSlots);
    BailIfNull(trackHelpers);

    if (input && input->trackProgress) {
        DagTypes::DagId dagId = input->dagId;
        status = dagLib->getDagLocal(dagId)->getOpStatusFromXdbId(xdb->xdbId,
                                                                  &opStatus);
        if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        opStatus = NULL;
    }

    sortXdbHelpers =
        (SortXdbHelper **) new (std::nothrow) SortXdbHelper *[numScheds];
    BailIfNull(sortXdbHelpers);

    for (uint64_t ii = 0; ii < numScheds; ii++) {
        sortXdbHelpers[ii] = NULL;
    }

    for (uint64_t ii = 0; ii < numScheds; ii++) {
        sortXdbHelpers[ii] =
            SortXdbHelper::setUp(xdb, ii, trackHelpers, opStatus);
        BailIfNull(sortXdbHelpers[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) sortXdbHelpers,
                                           numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (sortXdbHelpers != NULL) {
        for (uint64_t ii = 0; ii < numScheds; ii++) {
            if (sortXdbHelpers[ii]) {
                memFree(sortXdbHelpers[ii]);
                sortXdbHelpers[ii] = NULL;
            }
        }
        delete[] sortXdbHelpers;
        sortXdbHelpers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }
    return status;
}

// XXX - needs much work. This is probably doing some terrible hashing.
uint64_t
XdbMgr::hashXdbUniform(DfFieldValue fieldVal,
                       DfFieldType fieldType,
                       uint64_t hashSlots)
{
    uint64_t hashSlot = 0;

    switch (fieldType) {
    case DfString:
        hashSlot = hashStringFast(fieldVal.stringVal.strActual) % hashSlots;
        break;

    case DfInt32:
        hashSlot =
            hashCrc32c(0, &fieldVal.int32Val, sizeof(fieldVal.int32Val)) %
            hashSlots;
        break;

    case DfUInt32:
        hashSlot =
            hashCrc32c(0, &fieldVal.uint32Val, sizeof(fieldVal.int32Val)) %
            hashSlots;
        break;

    case DfInt64:
        hashSlot =
            hashCrc32c(0, &fieldVal.int64Val, sizeof(fieldVal.int64Val)) %
            hashSlots;
        break;

    case DfUInt64:
        hashSlot =
            hashCrc32c(0, &fieldVal.uint64Val, sizeof(fieldVal.uint64Val)) %
            hashSlots;
        break;

    case DfFloat32:
        hashSlot =
            hashCrc32c(0, &fieldVal.float32Val, sizeof(fieldVal.float32Val)) %
            hashSlots;
        break;

    case DfFloat64:
        hashSlot =
            hashCrc32c(0, &fieldVal.float64Val, sizeof(fieldVal.float64Val)) %
            hashSlots;
        break;

    case DfMoney:
        hashSlot = hashCrc32c(0,
                              fieldVal.numericVal.ieee,
                              sizeof(fieldVal.numericVal.ieee)) %
                   hashSlots;
        break;

    case DfBoolean:
        hashSlot = (uint32_t) fieldVal.boolVal;
        break;

    case DfTimespec:
        hashSlot =
            hashCrc32c(0, &fieldVal.timeVal.ms, sizeof(fieldVal.timeVal.ms)) %
            hashSlots;
        break;

    case DfBlob:
        hashSlot = hashStringFast(fieldVal.stringVal.strActual) % hashSlots;
        assert(0);
        break;

    case DfUnknown:
    case DfNull:
        hashSlot = 0;
        break;

    case DfMixed:
    default:
        assert(0);
        break;
    }

    assert(hashSlot < hashSlots);
    return hashSlot;
}

// XXX - needs much work. This is probably doing some terrible hashing.
Status
XdbMgr::hashXdbFixedRange(DfFieldValue fieldVal,
                          DfFieldType fieldType,
                          uint64_t hashSlots,
                          const DfFieldValue *minKey,
                          DfFieldValue divisor,
                          uint64_t *hashSlotOut,
                          Ordering ordering)
{
    Status status = StatusOk;
    DFPUtils *dfp = DFPUtils::get();
    uint64_t hashSlot = 0;

    if (fieldType != DfMoney && divisor.float64Val == 0.f) {
        divisor.float64Val = 1.0;
    } else if (fieldType == DfMoney && dfp->xlrDfpIsZero(&divisor.numericVal)) {
        dfp->xlrDfpInt32ToNumeric(&divisor.numericVal, 1);
    }

    switch (fieldType) {
    case DfString:
        // XXX relies on string hack in xdbInsertKv().  maxKey and minKey
        // store the hash of the string, not the string itself
        fieldVal.uint64Val =
            operatorsHashByString(fieldVal.stringVal.strActual);
        if (fieldVal.uint64Val < minKey->uint64Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.uint64Val -= minKey->uint64Val;
        hashSlot =
            (uint64_t)((float64_t) fieldVal.uint64Val / divisor.float64Val);
        break;

    case DfInt32:
        if (fieldVal.int32Val < minKey->int32Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.int32Val -= minKey->int32Val;
        hashSlot =
            (uint64_t)((float64_t) fieldVal.int32Val / divisor.float64Val);
        break;

    case DfUInt32:
        if (fieldVal.uint32Val < minKey->uint32Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.uint32Val -= minKey->uint32Val;
        hashSlot =
            (uint64_t)((float64_t) fieldVal.uint32Val / divisor.float64Val);
        break;

    case DfInt64:
        if (fieldVal.int64Val < minKey->int64Val) {
            hashSlot = 0;
            break;
        }

        hashSlot =
            (uint64_t)((float64_t) fieldVal.int64Val / divisor.float64Val -
                       (float64_t) minKey->int64Val / divisor.float64Val);
        break;

    case DfUInt64:
        if (fieldVal.uint64Val < minKey->uint64Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.uint64Val -= minKey->uint64Val;
        hashSlot =
            (uint64_t)((float64_t) fieldVal.uint64Val / divisor.float64Val);
        break;

    case DfMoney: {
        if (DataFormat::fieldCompare(fieldType, fieldVal, *minKey) < 0) {
            hashSlot = 0;
            break;
        }

        dfp->xlrDfpSub(&fieldVal.numericVal,
                       &fieldVal.numericVal,
                       &minKey->numericVal);
        dfp->xlrDfpDiv(&fieldVal.numericVal,
                       &fieldVal.numericVal,
                       &divisor.numericVal);
        hashSlot = (uint64_t) dfp->xlrDfpNumericToUInt32(&fieldVal.numericVal);
    } break;

    case DfTimespec:
        if (fieldVal.timeVal.ms < minKey->timeVal.ms) {
            hashSlot = 0;
            break;
        }

        fieldVal.timeVal.ms -= minKey->timeVal.ms;
        hashSlot =
            (uint64_t)((float64_t) fieldVal.timeVal.ms / divisor.float64Val);
        break;

    case DfFloat32:
        if (fieldVal.float32Val < minKey->float32Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.float32Val -= minKey->float32Val;
        hashSlot = (uint64_t)(fieldVal.float32Val / divisor.float64Val);
        break;

    case DfFloat64:
        if (fieldVal.float64Val < minKey->float64Val) {
            hashSlot = 0;
            break;
        }

        fieldVal.float64Val -= minKey->float64Val;
        hashSlot = (uint64_t)(fieldVal.float64Val / divisor.float64Val);
        break;

    case DfBoolean:
        hashSlot = fieldVal.boolVal;
        break;

    case DfUnknown:
    case DfNull:
        hashSlot = 0;
        break;

    case DfBlob:
    case DfMixed:
    default:
        assert(0);
        status = StatusUnimpl;
        goto CommonExit;
    }

    if (hashSlot >= hashSlots) {
        // deal with max key off by 1 error
        hashSlot = hashSlots - 1;
    }

    if (ordering & AscendingFlag) {
        *hashSlotOut = hashSlot;
    } else {
        assert(ordering & DescendingFlag);
        *hashSlotOut = hashSlots - 1 - hashSlot;
    }

CommonExit:
    return status;
}

XdbPage *
XdbMgr::getXdbHashSlotNextPage(XdbAtomicHashSlot *xdbHashSlot)
{
    uintptr_t xdbPage = (uintptr_t) xdbHashSlot->nextPageUseInlinesToModify;

    xdbPage &= ~(XdbMinPageAlignment - 1);
    return (XdbPage *) xdbPage;
}

void
XdbMgr::clearSlot(XdbAtomicHashSlot *xdbHashSlot)
{
    XdbAtomicHashSlot temp;

    temp.nextPageUseInlinesToModify = NULL;
    temp.casLock = xdbHashSlot->casLock;
    temp.sortFlag = xdbHashSlot->sortFlag;
    temp.serialized = xdbHashSlot->serialized;

    xdbHashSlot->nextPageUseInlinesToModify = temp.nextPageUseInlinesToModify;
}

void
XdbMgr::setXdbHashSlotNextPage(XdbAtomicHashSlot *xdbHashSlot,
                               XdbId xdbId,
                               uint64_t slotId,
                               XdbPage *nextPage)
{
    XdbAtomicHashSlot temp;

    // Mark the previous page as serializable
    XdbPage *page = getXdbHashSlotNextPage(xdbHashSlot);
    if (page != NULL && (page->hdr.xdbPageType & XdbPageNormalFlag)) {
        pagePutRef(xdbId, slotId, page);
    }

    // Check alignment
    assert(((uintptr_t) nextPage & ~(XdbMinPageAlignment - 1)) ==
           (uintptr_t) nextPage);

    temp.nextPageUseInlinesToModify = nextPage;
    temp.casLock = xdbHashSlot->casLock;
    temp.sortFlag = xdbHashSlot->sortFlag;
    temp.serialized = xdbHashSlot->serialized;

    xdbHashSlot->nextPageUseInlinesToModify = temp.nextPageUseInlinesToModify;
}

// Lock slot and busy wait with sleep until slot is unlocked.
void
XdbMgr::lockSlot(XdbHashSlotInfo *slotInfo, uint64_t slotId)
{
    // Only LSB in hashSlot list is used to manage lock state
    XdbAtomicHashSlot *xdbHashSlot = &slotInfo->hashBase[slotId];
    XdbHashSlotAug *xdbAugSlot = &slotInfo->hashBaseAug[slotId];
    XdbAtomicHashSlot oldValueDirtyRead, newValue, oldValueCas;
    bool done = false;
    uint64_t loopCount;
    static const uint64_t LoopCountThresh = 1 << (17);

    while (!done) {
        // Dirty bit field read.
        loopCount = 0;
        while (xdbHashSlot->casLock == SlotLocked &&
               ++loopCount < LoopCountThresh) {
            // Empty loop body intentional
        }

        oldValueDirtyRead.nextPageUseInlinesToModify =
            xdbHashSlot->nextPageUseInlinesToModify;
        newValue.nextPageUseInlinesToModify =
            xdbHashSlot->nextPageUseInlinesToModify;

        // Pretend slot was unlocked to save an extra assignment in the
        // while() above which is non-atomic by design.
        oldValueDirtyRead.casLock = SlotUnlocked;
        newValue.casLock = SlotLocked;

        xdbAugSlot->lock.lock();
        oldValueCas.nextPageUseInlinesToModify = (XdbPage *)
            atomicCmpXchg64((Atomic64 *) &xdbHashSlot
                                ->nextPageUseInlinesToModify,
                            (int64_t)
                                oldValueDirtyRead.nextPageUseInlinesToModify,
                            (int64_t) newValue.nextPageUseInlinesToModify);
        xdbAugSlot->lock.unlock();

        if (oldValueDirtyRead.nextPageUseInlinesToModify ==
            oldValueCas.nextPageUseInlinesToModify) {
            done = true;
        } else {
            // Spun for too long; Time to sleep!
            xdbAugSlot->sem.semWait();
        }
    }
}

void
XdbMgr::lockAllSlots(Xdb *xdb)
{
    for (uint64_t slot = 0; slot < xdb->hashSlotInfo.hashSlots; slot++) {
        lockSlot(&xdb->hashSlotInfo, slot);
    }
}

void
XdbMgr::unlockSlot(XdbHashSlotInfo *slotInfo, uint64_t slotId)
{
    // Only LSB in hashSlot list is used to manage lock state
    XdbAtomicHashSlot *xdbHashSlot = &slotInfo->hashBase[slotId];
    XdbHashSlotAug *xdbAugSlot = &slotInfo->hashBaseAug[slotId];
    XdbAtomicHashSlot oldValueDirtyRead, newValue, oldValueCas;

    // Dirty bit field read
    assert(xdbHashSlot->casLock == SlotLocked);

    oldValueDirtyRead.nextPageUseInlinesToModify =
        xdbHashSlot->nextPageUseInlinesToModify;
    newValue.nextPageUseInlinesToModify =
        xdbHashSlot->nextPageUseInlinesToModify;

    newValue.casLock = SlotUnlocked;

    xdbAugSlot->lock.lock();
    oldValueCas.nextPageUseInlinesToModify = (XdbPage *)
        atomicCmpXchg64((Atomic64 *) &xdbHashSlot->nextPageUseInlinesToModify,
                        (int64_t) oldValueDirtyRead.nextPageUseInlinesToModify,
                        (int64_t) newValue.nextPageUseInlinesToModify);

    assert(oldValueDirtyRead.nextPageUseInlinesToModify ==
           oldValueCas.nextPageUseInlinesToModify);

    // Kick up the waiters.
    xdbAugSlot->sem.post();
    xdbAugSlot->lock.unlock();

    memBarrier();
}

void
XdbMgr::unlockAllSlots(Xdb *xdb)
{
    for (ssize_t slot = xdb->hashSlotInfo.hashSlots - 1; slot >= 0; slot--) {
        unlockSlot(&xdb->hashSlotInfo, slot);
    }
}

Status
XdbMgr::xdbRehashXdbIfNeeded(Xdb *xdb, XdbInsertKvState xdbInsertKvState)
{
    // XXX This is a NOOP now, since we always rehash for the unordered XDB case
    // during load done. However if we choose to not rehash the XDB for
    // unordered case during load done, we will need a hook to manage stale
    // cursors.
    // For cursor management, we will use a separate look aside table outside
    // XDB to lookup the cursors based on table ID. This can be done using GVMs.
    return StatusOk;
}

Status
XdbMgr::createCursorFast(Xdb *xdb, int64_t slotId, TableCursor *cur)

{
    Status status;
    XdbPgCursor *xdbPgCursor = &cur->xdbPgCursor;

    cur->xdbId = xdb->meta->xdbId;
    cur->ordering_ = Unordered;
    cur->trackOption_ = Cursor::Untracked;
    cur->backing_ = Cursor::BackingTable;

    status = xdbPgCursor->init(xdb, Unordered, slotId, true);
    BailIfFailed(status);

    if (slotId == -1) {
        // Negative slotId was used by init to set appropriate flags.
        slotId = 0;
    }

    cur->xdbPgTupBuf = xdbPgCursor->xdbPage_->tupBuf;

    new (&cur->xdbPgTupCursor) NewTuplesCursor(cur->xdbPgTupBuf);
    cur->startRecordNum = xdb->hashSlotInfo.hashBaseAug[slotId].startRecord;
    cur->eofWasReached = false;
    cur->needDestroy_ = false;
    cur->kvMeta_ = (NewKeyValueMeta *) &xdb->meta->kvNamedMeta.kvMeta_;
    cur->tupleMeta_ = cur->kvMeta_->tupMeta_;

CommonExit:
    return status;
}

void
XdbMgr::xdbFreeScratchPadXdbs(Xdb *xdb)
{
    if (xdb->scratchPadInfo != NULL &&
        xdb->scratchPadInfo->scratchIdx == ScratchPadInfo::ScratchPadOwnerIdx) {
        ScratchPadInfo *scratchPadInfo = xdb->scratchPadInfo;
        ScratchPadShared *scratchPadShared = scratchPadInfo->scatchPadShared;
        for (unsigned ii = 0; ii < scratchPadShared->numScratchPads; ii++) {
            MsgEphemeral eph;
            Xdb *scratchXdb = scratchPadShared->scratchPads[ii].scratchXdb;

            if (scratchXdb->scratchPadInfo != NULL) {
                assert(scratchXdb->scratchPadInfo->scratchIdx == ii);
                assert(scratchXdb->scratchPadInfo->scatchPadShared ==
                       scratchPadShared);
                memAlignedFree(scratchXdb->scratchPadInfo);
                scratchXdb->scratchPadInfo = NULL;
            }

            eph.ephemeral = (void *) scratchXdb->meta->xdbId;
            xdbDropLocal(&eph, NULL);
            assert(eph.status == StatusOk);
        }
        memAlignedFree(scratchPadShared->scratchPads);
        memAlignedFree(scratchPadShared);
        memAlignedFree(xdb->scratchPadInfo);
        xdb->scratchPadInfo = NULL;
    }
}

MustCheck Status
XdbPage::getRef(Xdb *xdb)
{
    Status status = StatusOk;
    PageState cur;

    pageRefLock.lock();
    status = XdbMgr::get()->getRefHelper(this, &cur);
    BailIfFailed(status);

    if (cur == Dropped) {
        assert(this->tupBuf == NULL);
        status = StatusXdbDesError;
        goto CommonExit;
    }

    if (cur == Serialized) {
        XdbPage *page = this;
        status = XdbMgr::get()->xdbDeserializeVec(xdb,
                                                  this->serializedXid,
                                                  &page,
                                                  1);
        BailIfFailed(status);
    }

    assert(atomicRead32(&this->hdr.pageState) == XdbPage::Resident);

CommonExit:
    pageRefLock.unlock();
    return status;
}

void
XdbPage::dropKvBuf()
{
    PageState cur;
    XdbMgr *xdbMgr = XdbMgr::get();

    this->pageRefLock.lock();
    cur = (XdbPage::PageState) atomicRead32(&this->hdr.pageState);

    if (unlikely(cur == Dropped)) {
        assert(this->tupBuf == NULL && "XdbPage payload has been dropped");
        goto CommonExit;
    }

    assert(cur != Serializing);

    if (unlikely(cur == Serialized)) {
        XdbPage *page = this;
        xdbMgr->xdbSerDesDropVec(this->serializedXid, &page, 1);
    } else {
        xdbMgr->xdbFreeTupBuf(this);
        if (cur == Serializable) {
            xdbMgr->removeSerializationElem(this);
        }
    }

    assert(atomicRead32(&this->hdr.pageState) == Dropped);

CommonExit:
    this->pageRefLock.unlock();
}

Status
XdbPage::insertKv(const NewKeyValueMeta *kvMeta, NewTupleValues *valueArray)
{
    assert(atomicRead32(&this->hdr.pageState) == Resident);
    assert(!this->hdr.isCompressed);

    Status status = this->tupBuf->append(kvMeta->tupMeta_, valueArray);
    if (likely(status == StatusOk)) {
        this->hdr.numRows++;
    }

    assert(this->hdr.numRows == this->tupBuf->getNumTuples());

    return status;
}

#ifdef BUFCACHESHADOW
MustCheck void *
XdbMgr::xdbGetBackingBcAddr(void *buf)
{
    if (bcXdbPage_->inRange(buf)) {
        return bcXdbPage_->getBackingAddr(buf);
    } else {
        return bcOsPageableXdbPage_->getBackingAddr(buf);
    }
}
#endif

Status
XdbPage::getFirstKey(Xdb *xdb, DfFieldValue *keyOut, bool *keyValid)
{
    NewTupleValues tuple;
    NewTuplesCursor cursor(this->tupBuf);
    Status status;

    status = cursor.getNext(&xdb->tupMeta, &tuple);
    if (status != StatusOk) {
        return status;
    }

    *keyOut = tuple.get(xdb->meta->keyAttr[0].valueArrayIndex, keyValid);
    return StatusOk;
}

Status
XdbPage::getLastKey(Xdb *xdb, DfFieldValue *keyOut, bool *keyValid)
{
    NewTupleValues tuple;
    NewTuplesCursor cursor(this->tupBuf);
    Status status;

    status = cursor.seek(&xdb->tupMeta, -1, NewTuplesCursor::SeekOpt::End);
    if (status != StatusOk) {
        return status;
    }

    status = cursor.getNext(&xdb->tupMeta, &tuple);
    if (status != StatusOk) {
        return status;
    }

    *keyOut = tuple.get(xdb->meta->keyAttr[0].valueArrayIndex, keyValid);
    return StatusOk;
}

void
XdbMgr::PageList::addLocked(uint32_t listNum, XdbPage *page)
{
    assert(!listLock_[listNum].tryLock());
    assert(page->pageListPrev == NULL);
    assert(page->pageListNext == NULL);
    if (head_[listNum] == NULL) {
        head_[listNum] = page;
        tail_[listNum] = page;
    } else {
        page->pageListNext = head_[listNum];
        head_[listNum]->pageListPrev = page;
        head_[listNum] = page;
    }
    size_[listNum]++;

#ifdef PAGE_LIST_DEBUG
    uint64_t count = 0;
    XdbPage *tmpPage = head_[listNum];
    while (tmpPage) {
        tmpPage = tmpPage->pageListNext;
        count++;
    }
    assert(count == size_[listNum]);
#endif  // PAGE_LIST_DEBUG
}

void
XdbMgr::PageList::removeLocked(uint32_t listNum, XdbPage *page)
{
    assert(!listLock_[listNum].tryLock());
    if (page == head_[listNum] && page == tail_[listNum]) {
        head_[listNum] = NULL;
        tail_[listNum] = NULL;
    } else if (page == head_[listNum]) {
        head_[listNum] = page->pageListNext;
        head_[listNum]->pageListPrev = NULL;
    } else if (page == tail_[listNum]) {
        tail_[listNum] = page->pageListPrev;
        tail_[listNum]->pageListNext = NULL;
    } else {
        page->pageListPrev->pageListNext = page->pageListNext;
        page->pageListNext->pageListPrev = page->pageListPrev;
    }

    assert(size_[listNum] > 0);
    size_[listNum]--;
    page->pageListNext = NULL;
    page->pageListPrev = NULL;

#ifdef PAGE_LIST_DEBUG
    uint64_t count = 0;
    XdbPage *tmpPage = head_[listNum];
    while (tmpPage) {
        tmpPage = tmpPage->pageListNext;
        count++;
    }
    assert(count == size_[listNum]);
#endif  // PAGE_LIST_DEBUG
}

uint64_t
XdbMgr::PageList::getListsSize()
{
    uint64_t listSize = 0;
    size_t numLocks = XcalarConfig::get()->xdbSerDesParallelism_;
    for (size_t idx = 0; idx < numLocks; idx++) {
        lock(idx);
        listSize += size_[idx];
        unlock(idx);
    }
    return listSize;
}
