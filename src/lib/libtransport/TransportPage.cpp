// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "StrlFunc.h"
#include "primitives/Primitives.h"

#include "config/Config.h"
#include "df/DataFormat.h"
#include "operators/Operators.h"
#include "usrnode/UsrNode.h"
#include "operators/OperatorsXdbPageOps.h"
#include "bc/BufferCache.h"
#include "operators/OperatorsHash.h"
#include "sys/XLog.h"
#include "msg/Message.h"
#include "xdb/DataModelTypes.h"
#include "util/MemTrack.h"
#include "operators/OperatorsTypes.h"
#include "transport/TransportPage.h"
#include "runtime/Runtime.h"
#include "stat/Statistics.h"
#include "newtupbuf/NewTuplesBuffer.h"
#include "usrnode/UsrNode.h"
#include "libapis/LibApisRecv.h"

static constexpr const char *moduleName = "TransportPage";

TransportPageMgr *TransportPageMgr::instance = NULL;
bool TransportPageMgr::doNetworkCompression = true;

TransportPageMgr::TransportPageMgr() : tpShipFsm_(this) {}

TransportPageMgr::~TransportPageMgr() {}

Status  // static
TransportPageMgr::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) TransportPageMgr;
    if (instance == NULL) {
        return StatusNoMem;
    }
    Status status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }

    return status;
}

void  // static
TransportPageMgr::destroy()
{
    if (instance == NULL) {
        return;
    }
    instance->destroyInternal();
    delete instance;
    instance = NULL;
}

TransportPageMgr *
TransportPageMgr::get()
{
    return instance;
}

Status
TransportPageMgr::initInternal()
{
    Status status = StatusOk;
    Config *config = Config::get();
    StatsLib *statsLib = StatsLib::get();

    assert((transportPageSize() % sizeof(uint64_t)) == 0 &&
           "Invalid transportPageSize");
    bcHandleSourcePages_ =
        BcHandle::create(BufferCacheObjects::TransPageSource);
    if (bcHandleSourcePages_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    bcHandleSerializationBuf_ =
        BcHandle::create(BufferCacheObjects::TransPageSerializationBuf);
    if (bcHandleSerializationBuf_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    usrEphPool_ = new (std::nothrow) MemoryPool();
    BailIfNull(usrEphPool_);
    assert(sizeof(UsrEph) <= usrEphPool_->getMaxPoolSize());

    status = statsLib->initNewStatGroup("TransPageMgr", &statsGrpId_, 11);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize statsGroup: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numFailedToAllocSerialBufferStat_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize numFailedToAllocSerialBufferStat_: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumCompressBufferAllocFailed",
                                         numFailedToAllocSerialBufferStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make numFailedToAllocSerialBufferStat_ global: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&cumlTransPagesShipped_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize cumlTransPagesShipped_: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumTransPagesShipped",
                                         cumlTransPagesShipped_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make cumlTransPagesShipped_ global: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&cumlTransPagesReceived_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize cumlTransPagesReceived_: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumTransPagesReceived",
                                         cumlTransPagesReceived_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make cumlTransPagesReceived_ global: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&shipCompletionFailure_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize shipCompletionFailure_: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "statNumShipCompletionFailure",
                                         shipCompletionFailure_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make shipCompletionFailure_ global: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&sourcePagesEnqueued_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize sourcePagesEnqueued_: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "sourceTransPagesQueueCount",
                                         sourcePagesEnqueued_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to make sourcePagesEnqueued_ global: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&numSourceTransPagesAllocRetries_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numSourceTransPagesAllocRetries",
                                         numSourceTransPagesAllocRetries_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numTransPagesAllocsXdbPool_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numTransPagesAllocsXdbPool",
                                         numTransPagesAllocsXdbPool_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numTransPagesFreesXdbPool_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numTransPagesFreesXdbPool",
                                         numTransPagesFreesXdbPool_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&cumShipperCleanout_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "cumShipperCleanout",
                                         cumShipperCleanout_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numUsrEphAllocs_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numUsrEphAllocs",
                                         numUsrEphAllocs_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numUsrEphFrees_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numUsrEphFrees",
                                         numUsrEphFrees_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    outstandingPagesCounter_ =
        (Atomic32 *) memAllocExt(sizeof(*outstandingPagesCounter_) *
                                     config->getActiveNodes(),
                                 moduleName);
    BailIfNull(outstandingPagesCounter_);

    memZero(statusAgainCounter_, sizeof(statusAgainCounter_));
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        atomicWrite32(&outstandingPagesCounter_[ii], 0);
        atomicWrite64(&statusAgainCounter_[ii], 0);
        atomicWrite32(&shipperActive_[ii], 0);
    }

    atomicWrite64(&sourcePagesEnqueued_->statUint64Atomic, 0);

    sourcePageSem_.init(0);
    tmpSourcePages_ =
        new (std::nothrow) TransportPage *[config->getActiveNodes()];
    BailIfNull(tmpSourcePages_);

CommonExit:
    return status;
}

Status
TransportPageMgr::setupShipper()
{
    Txn curTxn = Txn::currentTxn();
    Status status;
    unsigned shippers = XcalarConfig::get()->tpShippersCount_;
    Txn::setTxn(Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::SendMsgNw));

    for (unsigned ii = 0; ii < shippers; ii++) {
        status = Runtime::get()->schedule(&instance->tpShipFsm_);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed scheduling TP shipper FSM: %s",
                        strGetFromStatus(status));
        instance->tpShipFsm_.incActiveShippers();
    }
CommonExit:
    Txn::setTxn(curTxn);
    return status;
}

void
TransportPageMgr::teardownShipper()
{
    if (instance->tpShipFsm_.getActiveShippers()) {
        tpShipFsm_.waitUntilDone();
    }
}

void
TransportPageMgr::destroyInternal()
{
    if (outstandingPagesCounter_) {
        memFree(outstandingPagesCounter_);
        outstandingPagesCounter_ = NULL;
    }
    if (bcHandleSourcePages_ != NULL) {
        BcHandle::destroy(&bcHandleSourcePages_);
        bcHandleSourcePages_ = NULL;
    }
    if (bcHandleSerializationBuf_ != NULL) {
        BcHandle::destroy(&bcHandleSerializationBuf_);
        bcHandleSerializationBuf_ = NULL;
    }
    delete usrEphPool_;
    usrEphPool_ = NULL;
    delete[] tmpSourcePages_;
    tmpSourcePages_ = NULL;
}

TransportPageMgr::UsrEph *
TransportPageMgr::allocUsrEph()
{
    auto guard = usrEphPoolLock_.take();
    auto *usrEphElem = (UsrEph *) usrEphPool_->getElem(sizeof(UsrEph));
    if (unlikely(usrEphElem == NULL)) {
        return usrEphElem;
    }
    StatsLib::statNonAtomicIncr(numUsrEphAllocs_);
    return usrEphElem;
}

void
TransportPageMgr::freeUsrEph(TransportPageMgr::UsrEph *usrEph)
{
    auto guard = usrEphPoolLock_.take();
    usrEphPool_->putElem((void *) usrEph);
    StatsLib::statNonAtomicIncr(numUsrEphFrees_);
}

void
TransportPageMgr::ship(TransportPage *shipStackIn)
{
    Status status = StatusOk;
    TransportPage *transPageNext;
    NodeId dstNodeId;
    TransportPageHandle *transPageHandle;
    TransportPage *transPage = shipStackIn;

    while (transPage) {
        transPageHandle = transPage->usrEph->transPageHandle;
        transPageNext = transPage->next;
        dstNodeId = transPage->dstNodeId;

        // We always free the transPage
        switch (transPage->pageType) {
        case TransportPageType::SendSourcePage:
            if (transPageHandle->cmpStatus_ != StatusOk) {
                // If the operator already failed, no point trying to ship it.
                // Instead just cleanout inline.
                freeUsrEph(transPage->usrEph);
                TransportPageMgr::get()->freeTransportPage(transPage);
                transPage = transPageNext;
                StatsLib::statAtomicDecr64(sourcePagesEnqueued_);
                StatsLib::statNonAtomicIncr(cumShipperCleanout_);
                transPageHandle->lock_.lock();
                if (!atomicDec32(
                        &transPageHandle->outstandingTransportPageMsgs_)) {
                    transPageHandle->wakeupCv_.signal();
                }
                transPageHandle->lock_.unlock();
                continue;
            } else {
                // Let's ship this TP now.
                StatsLib::statAtomicDecr64(sourcePagesEnqueued_);
            }
            break;
        default:
            break;
        }

        atomicInc32(&outstandingPagesCounter_[dstNodeId]);

        status = transPageHandle->shipPage(transPage);
        if (status == StatusOk) {
            StatsLib::statNonAtomicIncr(cumlTransPagesShipped_);
        } else if (status == StatusAgain) {
            atomicInc64(&statusAgainCounter_[dstNodeId]);
            // We have reached the limit for this node, add to holding stack.
            shipStackLock_.lock();
            while (transPage) {
                transPageNext = transPage->next;
                transPage->next = shipStackOnHold_[dstNodeId];
                shipStackOnHold_[dstNodeId] = transPage;
                transPage = transPageNext;
            }
            shipStackLock_.unlock();
            continue;
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to shipPage: %s",
                    strGetFromStatus(status));
        }
        transPage = transPageNext;
    }
}

void
TransportPageMgr::processPageForShipping(TransportPage *transPage)
{
    assert(transPage != NULL);
    NodeId dstNodeId = transPage->dstNodeId;
    switch (transPage->pageType) {
    case TransportPageType::SendSourcePage:
        StatsLib::statAtomicIncr64(sourcePagesEnqueued_);
        break;
    default:
        break;
    }
    shipStackLock_.lock();
    transPage->next = shipStackOnHold_[dstNodeId];
    shipStackOnHold_[dstNodeId] = transPage;
    shipStackLock_.unlock();
    tpShipFsm_.wake();
}

void
TransportPageMgr::shipDriver()
{
    unsigned numNodes = Config::get()->getActiveNodes();
    while (true) {
        unsigned perNodeShipping = 0;
        for (unsigned curNodeId = 0; curNodeId < numNodes; curNodeId++) {
            TransportPage *curTp = NULL;
            if (!atomicCmpXchg32(&shipperActive_[curNodeId], 0, 1)) {
                shipStackLock_.lock();
                // Let's prevent concurrent shippers to same dstNode.
                curTp = shipStackOnHold_[curNodeId];
                shipStackOnHold_[curNodeId] = NULL;
                shipStackLock_.unlock();
                if (curTp) {
                    perNodeShipping++;
                    ship(curTp);
                }
                atomicWrite32(&shipperActive_[curNodeId], 0);
            }
        }
        if (!perNodeShipping) {
            break;
        }
    }
}

Status
TransportPageHandle::initHandle(MsgTypeId msgTypeId,
                                TwoPcCallId msgCallId,
                                OpTxnCookie *perNodeTxnCookies,
                                XdbId dstXdbId,
                                SchedFsmTransportPage *schedFsm)
{
    // Do an extra increment in case the increment in the loop races with the
    // completion post before the whole loop has had a chance to finish.
    atomicWrite32(&outstandingTransportPageMsgs_, 1);

    msgTypeId_ = msgTypeId;
    msgCallId_ = msgCallId;
    perNodeTxnCookies_ = perNodeTxnCookies;
    cmpStatus_ = StatusOk;
    dstXdbId_ = dstXdbId;
    schedFsm_ = schedFsm;

    // Supplying a schedFsm implies Async shipping, this means that we dont
    // need to waitForCompletions
    if (schedFsm) {
        type_ = Async;
    } else {
        type_ = Sync;
    }

    tpMgr_ = TransportPageMgr::get();

    return StatusOk;
}

MustCheck Status
TransportPageHandle::allocQuotaTransportPage(TransportPageType type,
                                             TransportPage **transPages,
                                             TransportPage **additionalTps,
                                             TransportPageHandle *additionalTph,
                                             unsigned numTransPages,
                                             unsigned dstId)
{
    Status status;
    assert(transPages[dstId] == NULL);

    if (type == TransportPageType::RecvPage) {
        transPages[dstId] =
            (TransportPage *) XdbMgr::get()->bcAlloc(XidInvalid,
                                                     &status,
                                                     XdbMgr::SlabHint::Default);
        if (transPages[dstId] == NULL) {
            goto CommonExit;
        }
        new (transPages[dstId]) TransportPage(type);
        goto CommonExit;
    }

    assert(type == TransportPageType::SendSourcePage);

    status = tpMgr_->allocMissingSourcePages(transPages,
                                             numTransPages,
                                             this,
                                             additionalTps,
                                             additionalTph);
    BailIfFailed(status);

CommonExit:
    return status;
}

TransportPage *
TransportPageMgr::allocRecvTransportPage(Status *statusOut)
{
    TransportPage *transPage = NULL;
    *statusOut = StatusOk;

    transPage =
        (TransportPage *) XdbMgr::get()->bcAlloc(XidInvalid,
                                                 statusOut,
                                                 XdbMgr::SlabHint::Default);
    if (transPage == NULL) {
        return NULL;
    }

    assert(transPage != NULL);
    new (transPage) TransportPage(TransportPageType::RecvPage);

    return transPage;
}

Status
TransportPageMgr::sendAllPages(TransportPage **pages,
                               unsigned numPages,
                               TransportPageHandle *tpHandle)
{
    Status status;

    // send out partially filled pages and release empty pages
    for (unsigned ii = 0; ii < numPages; ii++) {
        if (pages[ii] != NULL) {
            status = tpHandle->enqueuePage(pages[ii]);
            if (status != StatusOk) {
                freeTransportPage(pages[ii]);
                pages[ii] = NULL;
                goto CommonExit;
            }
            pages[ii] = NULL;
        }
    }

CommonExit:
    return status;
}

bool
TransportPageMgr::allocSourcePagesInternal(TransportPage **pages,
                                           unsigned numPages)
{
    unsigned numXdbPagesAllocs = 0, numTransPagesAllocs = 0,
             numPagesRequired = 0;
    unsigned ii = 0, jj = 0;

    // check how many pages are needed
    for (ii = 0; ii < numPages; ii++) {
        if (ii == Config::get()->getMyNodeId() || pages[ii] != NULL) {
            continue;
        }
        numPagesRequired++;
    }
    if (numPagesRequired == 0) {
        return true;
    }

    // only 1 thread is allowed to allocate at a time to get most to him.
    auto guard = sourcePageAllocationLock_.take();

    // allocate from transport page pool first
    numTransPagesAllocs =
        bcHandleSourcePages_->batchAlloc((void **) tmpSourcePages_,
                                         numPagesRequired,
                                         0,
                                         XidInvalid);
    // then try from xdb page pool
    numXdbPagesAllocs =
        XdbMgr::get()->bcBatchAlloc((void **) (tmpSourcePages_ +
                                               numTransPagesAllocs),
                                    numPagesRequired - numTransPagesAllocs,
                                    0,
                                    XidInvalid,
                                    XdbMgr::SlabHint::Default);

    if (numTransPagesAllocs + numXdbPagesAllocs < numPagesRequired) {
        // free pages allocated as we didn't get all we need
        bcHandleSourcePages_->batchFree((void **) tmpSourcePages_,
                                        numTransPagesAllocs,
                                        0);
        XdbMgr::get()->bcBatchFree((void **) (tmpSourcePages_ +
                                              numTransPagesAllocs),
                                   numXdbPagesAllocs,
                                   0);
        return false;
    }
    StatsLib::statNonAtomicAdd(numTransPagesAllocsXdbPool_, numXdbPagesAllocs);
    assert(numTransPagesAllocs + numXdbPagesAllocs == numPagesRequired);

    // all allocations succeeded, assign it to pages from tmpSourcePages_
    for (ii = 0; ii < numPages; ii++) {
        if (ii == Config::get()->getMyNodeId() || pages[ii] != NULL) {
            continue;
        }
        pages[ii] = tmpSourcePages_[jj];
        new (pages[ii]) TransportPage(TransportPageType::SendSourcePage);
        tmpSourcePages_[jj++] = NULL;
    }
    assert(jj == numPagesRequired);

    return true;
}

Status
TransportPageMgr::allocMissingSourcePages(TransportPage **pages,
                                          unsigned numPages,
                                          TransportPageHandle *tpHandle,
                                          TransportPage **additionalTps,
                                          TransportPageHandle *additionalTph)
{
    assert(numPages == Config::get()->getActiveNodes());

    Status status;
    bool drainFilledPages = true;
    bool allocsSuccess = false;

    while (!allocsSuccess) {
        allocsSuccess = allocSourcePagesInternal(pages, numPages);
        if (allocsSuccess) {
            break;
        }

        if (drainFilledPages) {
            // allocations failed, send out partially filled pages to make
            // some room.
            if (tpHandle) {
                status = sendAllPages(pages, numPages, tpHandle);
                BailIfFailed(status);
            }

            if (additionalTps) {
                status = sendAllPages(additionalTps, numPages, additionalTph);
                BailIfFailed(status);
            }
            drainFilledPages = false;
        }
        // wait for a free page
        StatsLib::statNonAtomicIncr(numSourceTransPagesAllocRetries_);
        sourcePageSem_.semWait();
    }

CommonExit:
    return status;
}

void
TransportPageMgr::freeTransportPage(TransportPage *transPage)
{
    assert(transPage != NULL);

    switch (transPage->pageType) {
    case TransportPageType::RecvPage:
        XdbMgr::get()->bcFree(transPage);
        break;
    case TransportPageType::SendSourcePage:
        if (bcHandleSourcePages_->inRange(transPage)) {
            bcHandleSourcePages_->freeBuf(transPage);
        } else {
            assert(XdbMgr::get()->inRange(transPage) &&
                   "transPage needs to in TransSourcePages or XdbPage slab");
            XdbMgr::get()->bcFree(transPage);
            StatsLib::statNonAtomicIncr(numTransPagesFreesXdbPool_);
        }
        sourcePageSem_.post();
        break;
    default:
        assert(0);
        break;
    }
}

void
TransportPageHandle::initPage(TransportPage *transPage,
                              NodeId dstNodeId,
                              const NewTupleMeta *tupleMeta,
                              uint8_t op)
{
    assertStatic((offsetof(TransportPage, buf) % sizeof(uint64_t)) == 0 &&
                 "Invalid Transport page alignment");

    transPage->headerSize = sizeof(*transPage);
    assert(transPage->headerSize < TransportPageMgr::transportPageSize());

    // This is recomputed after compression.
    transPage->payloadSize =
        TransportPageMgr::transportPageSize() - transPage->headerSize;

    transPage->demystifyOp = op;
    transPage->usrEph = NULL;
    transPage->next = NULL;
    transPage->srcNodeId = Config::get()->getMyNodeId();
    transPage->dstNodeId = dstNodeId;
    transPage->dstXdbId = dstXdbId_;
    transPage->txn = Txn::currentTxn();
    new (&transPage->tupMeta) NewTupleMeta();
    size_t numFields = tupleMeta->getNumFields();
    transPage->tupMeta.setNumFields(numFields);
    transPage->tupMeta.setFieldsPacking(tupleMeta->getFieldsPacking());
    for (size_t ii = 0; ii < numFields; ii++) {
        transPage->tupMeta.setFieldType(tupleMeta->getFieldType(ii), ii);
    }

    transPage->compressed = false;
    if (perNodeTxnCookies_ != NULL) {
        transPage->srcNodeTxnCookie = perNodeTxnCookies_[transPage->srcNodeId];
        transPage->dstNodeTxnCookie = perNodeTxnCookies_[transPage->dstNodeId];
        assert(transPage->srcNodeTxnCookie != OpTxnCookieInvalid);
        assert(transPage->dstNodeTxnCookie != OpTxnCookieInvalid);
    } else {
        transPage->srcNodeTxnCookie = OpTxnCookieInvalid;
        transPage->dstNodeTxnCookie = OpTxnCookieInvalid;
    }

    new (&transPage->kvMeta)
        NewKeyValueMeta(&transPage->tupMeta, NewTupleMeta::DfInvalidIdx);
    transPage->tupBuf = (NewTuplesBuffer *) transPage->buf;
    new (transPage->tupBuf)
        NewTuplesBuffer(transPage->tupBuf, transPage->payloadSize);
}

// Get ready to send it over the wire
void
TransportPage::serialize()
{
    TransportPageMgr *tpMgr = TransportPageMgr::get();
    this->tupBuf->serialize();
    this->payloadSize = this->tupBuf->getTotalBytesUsed();
    this->compressed = false;

    if (tpMgr->doNetworkCompression) {
        void *serializationBuffer =
            tpMgr->bcHandleSerializationBuf_->allocBuf(XidInvalid);
        if (serializationBuffer == NULL) {
            // Not end of the world
            StatsLib::statNonAtomicIncr(
                tpMgr->numFailedToAllocSerialBufferStat_);
        } else {
            size_t compressedSize = 0;
            Status status = XcSnappy::compress(this->tupBuf,
                                               this->payloadSize,
                                               serializationBuffer,
                                               &compressedSize);
            if (status == StatusOk) {
                assert(XcSnappy::getMaxCompressedLength(this->payloadSize) >=
                       compressedSize);
                if (compressedSize < this->payloadSize) {
                    memcpy(this->tupBuf, serializationBuffer, compressedSize);
                    this->payloadSize = compressedSize;
                    this->compressed = true;
                }
            } else {
                // Not end of the world
                StatsLib::statNonAtomicIncr(
                    tpMgr->numFailedToAllocSerialBufferStat_);
            }
            tpMgr->bcHandleSerializationBuf_->freeBuf(serializationBuffer);
            serializationBuffer = NULL;
        }
    }

    this->tupBuf = (NewTuplesBuffer *) XcalarApiMagic;
}

Status
TransportPage::deserialize()
{
    TransportPageMgr *tpMgr = TransportPageMgr::get();
    Status status = StatusOk;
    void *serializationBuffer = NULL;

    assert(this->tupBuf == (NewTuplesBuffer *) XcalarApiMagic);
    this->tupBuf = (NewTuplesBuffer *) this->buf;
    new (&this->kvMeta)
        NewKeyValueMeta(&this->tupMeta, NewTupleMeta::DfInvalidIdx);

    if (this->compressed) {
        serializationBuffer =
            tpMgr->bcHandleSerializationBuf_->allocBuf(XidInvalid);
        if (serializationBuffer == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to allocate serialization buffer during "
                    "decompression");
            return StatusNoMem;
        }

        memcpy(serializationBuffer, this->tupBuf, this->payloadSize);

        status = XcSnappy::deCompress(this->tupBuf,
                                      serializationBuffer,
                                      this->payloadSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error deserializing transportPage for table %lu: %s",
                    this->dstXdbId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    this->tupBuf->deserialize();

CommonExit:
    if (serializationBuffer != NULL) {
        tpMgr->bcHandleSerializationBuf_->freeBuf(serializationBuffer);
        serializationBuffer = NULL;
    }
    return status;
}

Status
TransportPageHandle::shipPage(TransportPage *transPage)
{
    Status status = StatusOk;
    TransportPageMgr::UsrEph *usrEph = transPage->usrEph;
    MsgEphemeral eph;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      transPage,
                                      transPage->headerSize +
                                          transPage->payloadSize,
                                      0,
                                      TwoPcFastPath,
                                      msgCallId_,
                                      usrEph,
                                      (TwoPcBufLife)(TwoPcZeroCopyInput |
                                                     TwoPcZeroCopyOutput));

    status = MsgMgr::get()->twoPcAlt(msgTypeId_,
                                     &eph,
                                     (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                                        MsgRecvHdrOnly),
                                     TwoPcAltCmd,
                                     TransportPagesForPayload,
                                     TwoPcSingleNode,
                                     transPage->dstNodeId,
                                     transPage->txn);

    // transPage will be invalid after freeTransportPage: stash pointer
    TransportPageHandle *transPageHandle = transPage->usrEph->transPageHandle;

    if (status != StatusAgain) {
        // free the local copy of the transport page we just sent
        TransportPageMgr::get()->freeTransportPage(transPage);
    }

    // As soon as we send this out, the ack could come back and call
    // transportPageShipComplete, which will free this handle (aka this)
    // so it's no longer safe to reference member variables
    if (status != StatusOk && status != StatusAgain) {
        // Set the error status in the handle so it will be sent back to the
        // sender by SchedFsmTransportPage::done.
        if (transPageHandle->cmpStatus_ == StatusOk) {
            transPageHandle->cmpStatus_ = status;
            memBarrier();
        }

        transPageHandle->lock_.lock();
        if (!atomicDec32(&outstandingTransportPageMsgs_)) {
            transPageHandle->wakeupCv_.signal();
        }
        transPageHandle->lock_.unlock();
    }

    return status;
}

Status
TransportPageHandle::waitForCompletions()
{
    Status status = StatusOk;
    assert(schedFsm_ == NULL);

    atomicDec32(&outstandingTransportPageMsgs_);

    lock_.lock();
    while (atomicRead32(&outstandingTransportPageMsgs_)) {
        wakeupCv_.wait(&lock_);
    }
    lock_.unlock();

    if (cmpStatus_ != StatusOk) {
        status = cmpStatus_;

        xSyslog(moduleName,
                XlogErr,
                "error(s) sending transport pages "
                "for table %lu status: %s",
                dstXdbId_,
                strGetFromStatus(status));
    }

    return status;
}

// Add a transport page to the list of pages to be shipped.  completionStatus
// is the address of a variable that will be set if the remote node returns a
// failing status.  It is assumed that the caller has initialized this to be
// StatusOk.
MustCheck Status
TransportPageHandle::enqueuePage(TransportPage *transPage)
{
    Status status;
    TransportPageMgr::UsrEph *usrEph = NULL;

    Config *config = Config::get();
    if (transPage->dstXdbId == XidInvalid) {
        // page wasn't initialized, just free it and return
        tpMgr_->freeTransportPage(transPage);
        return StatusOk;
    }

    // We should only enqueue transport pages for remote nodes
    assert(transPage->dstNodeId != config->getMyNodeId());

    // Obtain the usrEph here so that we can nicely fail the enqueue
    // of the transport page if we can't get it instead of a more
    // difficult to handle error when shipping the transport page.
    transPage->usrEph = tpMgr_->allocUsrEph();
    if (transPage->usrEph == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate transPage->usrEph");
        status = StatusNoMem;
        goto CommonExit;
    }

    usrEph = transPage->usrEph;

    usrEph->srcNodeId = config->getMyNodeId();
    usrEph->dstNodeId = transPage->dstNodeId;
    usrEph->tableId = transPage->dstXdbId;
    usrEph->transPage = static_cast<TransportPage *>(transPage);

    usrEph->transPageHandle = this;

    transPage->serialize();

    // pass off to the transport page mgr for shipping
    atomicInc32(&outstandingTransportPageMsgs_);
    tpMgr_->processPageForShipping(transPage);
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (usrEph != NULL) {
            tpMgr_->freeUsrEph(usrEph);
            usrEph = NULL;
            transPage->usrEph = NULL;
        }
    }

    return status;
}

MustCheck Status
TransportPageHandle::insertKv(TransportPage **transPageOut,
                              const NewTupleMeta *tupleMeta,
                              NewTupleValues *tuple)
{
    Status status;
    Status enqStatus;

    assert(transPageOut != NULL);
    TransportPage *transPage = *transPageOut;
    NewTuplesBuffer *tupBuf = transPage->tupBuf;
    NewTupleMeta *curTupMeta = &transPage->tupMeta;
    size_t numFields = tupleMeta->getNumFields();

    // XXX: Optimize by tracking whether each trans page contains any FNFs or
    // not.  Skip if not.
    for (uint64_t ii = 0; ii < numFields; ii++) {
        if (curTupMeta->getFieldType(ii) != tupleMeta->getFieldType(ii)) {
            // First tuples added to the page contained FNFs but then a
            // non-FNF was added, so set the value type in the
            // transPage->kvMeta accordingly
            assert(curTupMeta->getFieldType(ii) == DfUnknown ||
                   curTupMeta->getFieldType(ii) == DfScalarObj);
            curTupMeta->setFieldType(tupleMeta->getFieldType(ii), ii);
        }
    }

    status = tupBuf->append(curTupMeta, tuple);
    if (status == StatusNoData) {
        // Don't enqueue a newly allocated page that isn't big enough.  Let
        // the caller decide what to do with it.
        if (tupBuf->getNumTuples() > 0) {
            enqStatus = enqueuePage(transPage);
            if (enqStatus == StatusOk) {
                *transPageOut = transPage = NULL;
            } else {
                status = enqStatus;
            }
        }
    } else {
        assert(status == StatusOk);
    }

    return status;
}

void
TransportPageHandle::destroy()
{
}

uint32_t
TransportPageMgr::transportSourcePagesCount(uint32_t numNodes)
{
    // for source producer to make forward progress, he needs exactly N-1 pages
    // We want numCores concurrent source producers to make progress at once
    uint32_t pageCount =
        XcSysHelper::get()->getNumOnlineCores() * (numNodes - 1);
    if (pageCount == 0) {
        pageCount = 1;
    }

    return pageCount * XcalarConfig::get()->sourcePagesScaleFactor_;
}

uint32_t
TransportPageMgr::getMaxSerializationBuffers()
{
    uint32_t bufCount = XcSysHelper::get()->getNumOnlineCores();

    return bufCount;
}

FsmState::TraverseState
TransportPageMgr::TpShipStart::doWork()
{
    SchedFsmShipTransPage *fsm =
        dynamic_cast<SchedFsmShipTransPage *>(getSchedFsm());

    if (usrNodeNormalShutdown()) {
        fsm->setNextState(NULL);
    } else if (usrNodeForceShutdown()) {
        fsm->setNextState(NULL);
    } else {
        fsm->waitSem_.timedWait(USecsPerSec);
        fsm->tpMgr_->shipDriver();
    }
    return TraverseState::TraverseNext;
}

void
TransportPageMgr::SchedFsmShipTransPage::done()
{
    if (atomicInc32(&shippersDone_) == atomicRead32(&activeShippers_)) {
        doneSem_.post();
    }
}
