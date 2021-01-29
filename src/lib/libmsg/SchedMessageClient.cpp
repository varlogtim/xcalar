// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "msg/Message.h"
#include "msg/MessageTypes.h"
#include "MessageInt.h"
#include "SchedMessageClient.h"
#include "stat/Statistics.h"
#include "sys/XLog.h"
#include "SchedInt.h"
#include "TwoPcInt.h"
#include "runtime/Runtime.h"
#include "operators/Operators.h"

SchedAckAndFree::SchedAckAndFree(MsgMessage *msg)
    : SchedulableFsm("SchedAckAndFree", NULL)
{
    memcpy(&msgCopy_, msg, sizeof(MsgMessage));
    msgCopy_.msgHdr.msgBufType = NormalCopyMsg;
}

SchedAckAndFree::~SchedAckAndFree() {}

void
SchedAckAndFree::run()
{
    assert(Txn::currentTxn().valid());
    assert(msgCopy_.msgHdr.eph.txn == Txn::currentTxn());
    MsgMgr::get()->ackAndFree(&msgCopy_);
}

bool
SchedAckAndFree::isNonBlocking() const
{
    return true;
}

bool
SchedAckAndFree::isPriority() const
{
    return true;
}

void
SchedAckAndFree::done()
{
    SchedMgr *schedMgr = SchedMgr::get();
    Txn::setTxn(msgCopy_.msgHdr.eph.txn);
    this->~SchedAckAndFree();
    StatsLib::statAtomicDecr64(schedMgr->schedObjectsActive_);
    schedMgr->bcHandleSchedObject_->freeBuf(this);
}

SchedMessageObject::SchedMessageObject(const char *name, MsgMessage *msg)
    : SchedulableFsm(name, NULL)
{
    memcpy(&msgCopy_, msg, sizeof(MsgMessage));
    msgCopy_.msgHdr.msgBufType = (MsgBufType)(
        (uint64_t) msgCopy_.msgHdr.msgBufType | (uint64_t) CopyMsg);
}

void
SchedMessageObject::run()
{
    assert(Txn::currentTxn().valid());
    assert(msgCopy_.msgHdr.eph.txn == Txn::currentTxn());
    MsgMgr::get()->doTwoPcWorkForMsg(&this->msgCopy_);
}

void
SchedMessageObject::done()
{
    SchedMgr *schedMgr = SchedMgr::get();
    Txn::setTxn(msgCopy_.msgHdr.eph.txn);
    MsgMessage *msg = &this->msgCopy_;

    NodeId myNodeId = Config::get()->getMyNodeId();
    if (myNodeId != msg->msgHdr.srcNodeId) {
        bool inlineAck = true;
        SchedAckAndFree *ackAndFreeObj =
            (SchedAckAndFree *) schedMgr->bcHandleSchedObject_->allocBuf(
                XidInvalid);
        if (ackAndFreeObj != NULL) {
            StatsLib::statAtomicIncr64(schedMgr->schedObjectsActive_);
            new (ackAndFreeObj) SchedAckAndFree(msg);
            Status status = Runtime::get()->schedule(ackAndFreeObj);
            if (status != StatusOk) {
                ackAndFreeObj->run();
                ackAndFreeObj->done();
                ackAndFreeObj = NULL;
            }
            inlineAck = false;
        }
        if (inlineAck) {
            MsgMgr::get()->ackAndFree(msg);  // ACK inline
        }
    }
    StatsLib::statAtomicDecr64(schedMgr->schedObjectsActive_);
    this->~SchedMessageObject();
    schedMgr->bcHandleSchedObject_->freeBuf(this);
}

SchedFsmTransportPage::SchedFsmTransportPage(const char *name, MsgMessage *msg)
    : SchedulableFsm(name, NULL),
      demystifySourcePage_(this),
      enqueueScalarPage_(this),
      dequeueScalarPage_(this),
      processScalarPage_(this),
      indexPage_(this)
{
    TransportPageMgr::get()->incTransPagesReceived();
    memcpy(&msgCopy_, msg, sizeof(MsgMessage));
    msgCopy_.msgHdr.msgBufType = (MsgBufType)(
        (uint64_t) msgCopy_.msgHdr.msgBufType | (uint64_t) CopyMsg);

    // this transport page was alloced by recvThread, but the bufType was
    // overwritten when the payload was copied over. Reset it here
    transPage_ = (TransportPage *) msgCopy_.payload;
    transPage_->pageType = TransportPageType::RecvPage;
    msgCopy_.payload = NULL;

    demystifyMgr_ = DemystifyMgr::get();
    msgCopy_.msgHdr.eph.status = StatusOk;

    switch (msgCopy_.msgHdr.typeId) {
    case MsgTypeId::Msg2pcDemystifySourcePage:
        this->setNextState(&demystifySourcePage_);
        break;
    case MsgTypeId::Msg2pcProcessScalarPage:
        this->setNextState(&enqueueScalarPage_);
        break;
    case MsgTypeId::Msg2pcIndexPage:
        this->setNextState(&indexPage_);
        break;
    default:
        assert(0);
        break;
    }
    atomicWrite64(&indexPageRef_, 1);
}

SchedFsmTransportPage::~SchedFsmTransportPage() {}

bool
SchedFsmTransportPage::isPriority() const
{
    return true;
}

Status
SchedFsmTransportPage::allocContext(DemystifyMgr::Op op)
{
    Status status = StatusOk;
    op_ = op;

    switch (op) {
    case DemystifyMgr::Op::Index: {
        indexPage_.numPages_ = Config::get()->getActiveNodes();
        indexPage_.indexPages_ = (TransportPage **) memAlloc(
            indexPage_.numPages_ * sizeof(*indexPage_.indexPages_));
        BailIfNull(indexPage_.indexPages_);

        memZero(indexPage_.indexPages_,
                indexPage_.numPages_ * sizeof(*indexPage_.indexPages_));

        Operators::PerTxnInfo *perTxnInfo =
            (Operators::PerTxnInfo *) transPage_->dstNodeTxnCookie;
        DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
        indexPage_.opMeta_ = (OpMeta *) demystifyInput->opMeta;
        break;
    }
    default:
        assert(0);
        break;
    }

    contextAlloced_ = true;
CommonExit:
    return status;
}

void
SchedFsmTransportPage::freeContext()
{
    TransportPageMgr *tpMgr = TransportPageMgr::get();
    MsgMessage *msg = &this->msgCopy_;

    switch (op_) {
    case DemystifyMgr::Op::Index: {
        for (unsigned ii = 0; ii < indexPage_.numPages_; ii++) {
            if (msg->msgHdr.eph.status == StatusOk) {
                if (indexPage_.indexPages_[ii]) {
                    msg->msgHdr.eph.status =
                        indexPage_.opMeta_->indexHandle->enqueuePage(
                            indexPage_.indexPages_[ii]);
                    if (msg->msgHdr.eph.status != StatusOk) {
                        tpMgr->freeTransportPage(indexPage_.indexPages_[ii]);
                    }

                    indexPage_.indexPages_[ii] = NULL;
                }
            }
        }

        memFree(indexPage_.indexPages_);
        break;
    }
    default:
        assert(0);
        break;
    }

    contextAlloced_ = false;
}

void
SchedFsmTransportPage::done()
{
    if (getCurState() == &indexPage_ && atomicDec64(&indexPageRef_)) {
        return;
    }
    SchedMgr *schedMgr = SchedMgr::get();
    MsgMgr *msgMgr = MsgMgr::get();
    Txn::setTxn(msgCopy_.msgHdr.eph.txn);
    if (contextAlloced_) {
        freeContext();
    }

    if (handle_ == NULL ||
        atomicDec32(&handle_->outstandingTransportPageMsgs_) == 0) {
        MsgMessage *msg = &this->msgCopy_;
        NodeId myNodeId = Config::get()->getMyNodeId();
        assert(myNodeId != msg->msgHdr.srcNodeId);
        if (handle_ != NULL) {
            if (msg->msgHdr.eph.status == StatusOk &&
                handle_->cmpStatus_ != StatusOk) {
                msg->msgHdr.eph.status = handle_->cmpStatus_;
            }
            delete handle_;
            handle_ = NULL;
        }

        bool inlineAck = true;
        if (!isMarkedToAbort()) {
            SchedAckAndFree *ackAndFreeObj =
                (SchedAckAndFree *) schedMgr->bcHandleSchedObject_->allocBuf(
                    XidInvalid);
            if (ackAndFreeObj != NULL) {
                StatsLib::statAtomicIncr64(schedMgr->schedObjectsActive_);
                new (ackAndFreeObj) SchedAckAndFree(msg);
                Status status = Runtime::get()->schedule(ackAndFreeObj);
                if (status != StatusOk) {
                    ackAndFreeObj->run();
                    ackAndFreeObj->done();
                    ackAndFreeObj = NULL;
                }
                inlineAck = false;
            }
        }
        if (inlineAck) {
            msgMgr->ackAndFree(msg);  // ACK inline
        }
        StatsLib::statAtomicDecr64(schedMgr->schedObjectsActive_);
        this->~SchedFsmTransportPage();
        schedMgr->bcHandleSchedObject_->freeBuf(this);
    }
}

void
DemystifyMgr::demystifySourcePageWork(MsgEphemeral *eph,
                                      TransportPage *transPage)
{
    Status status = StatusOk;
    TransportPageHandle *scalarHandle = NULL;
    bool scalarHandleInit = false;
    XdbMgr *xdbMgr = XdbMgr::get();
    SchedFsmTransportPage *schedFsm =
        (SchedFsmTransportPage *) Runtime::get()->getRunningSchedulable();
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) transPage->dstNodeTxnCookie;
    Xdb *xdbDst = perTxnInfo->dstXdb;
    xdbMgr->xdbIncRecvTransPage(xdbDst);

    status = transPage->deserialize();
    BailIfFailed(status);

    scalarHandle = new (std::nothrow) TransportPageHandle();
    BailIfNull(scalarHandle);

    status = scalarHandle->initHandle(MsgTypeId::Msg2pcProcessScalarPage,
                                      TwoPcCallId::Msg2pcProcessScalarPage1,
                                      perTxnInfo->remoteCookies,
                                      transPage->dstXdbId,
                                      schedFsm);
    BailIfFailed(status);
    scalarHandleInit = true;
    schedFsm->handle_ = scalarHandle;

    assert(transPage->demystifyOp < DemystifyMgr::Op::Last);

    status = demystifyRecvHandlers[transPage->demystifyOp]
                 ->demystifySourcePage(transPage, scalarHandle);

CommonExit:
    if (scalarHandle != NULL) {
        if (scalarHandleInit) {
            scalarHandle->destroy();
        }
    }

    TransportPageMgr::get()->freeTransportPage(transPage);

    eph->status = status;
}

FsmState::TraverseState
StateDemystifySourcePage::doWork()
{
    assert(Txn::currentTxn().valid());
    SchedFsmTransportPage *schedFsm =
        dynamic_cast<SchedFsmTransportPage *>(getSchedFsm());
    assert(schedFsm->msgCopy_.msgHdr.eph.txn == Txn::currentTxn());

    schedFsm->setNextState(NULL);

    // For now, the incoming source TPs that need to be demystified are always
    // packed with fixed style fields. So we can make a hard assumption here
    // about the type of TP.
    schedFsm->demystifyMgr_
        ->demystifySourcePageWork(&schedFsm->msgCopy_.msgHdr.eph,
                                  (schedFsm->transPage_));
    schedFsm->transPage_ = NULL;

    return TraverseState::TraverseStop;
}

FsmState::TraverseState
StateEnqueueScalarPage::doWork()
{
    assert(Txn::currentTxn().valid());
    SchedFsmTransportPage *schedFsm =
        dynamic_cast<SchedFsmTransportPage *>(getSchedFsm());
    bool enqueued;
    assert(schedFsm->msgCopy_.msgHdr.eph.txn == Txn::currentTxn());

    // Try to enqueue the page, enqueue will fail if there aren't enough
    // scalarPageWorkers
    // if our page was not enqueued, we need to process the page ourself
    // this puts us in the processPage loop where we begin servicing the
    // scalarPage queue. We become a scalarPageWorker

    // For now, the incoming Scalar Page TPs are always packed with variable
    // field type DfScalar. So we can make a hard assumption here about
    // the type of TP.
    enqueued = schedFsm->demystifyMgr_
                   ->enqueueScalarPage(&schedFsm->msgCopy_.msgHdr.eph,
                                       schedFsm->transPage_);

    if (schedFsm->msgCopy_.msgHdr.eph.status != StatusOk || enqueued) {
        // if our page was enqueued or there was an error, we are done
        schedFsm->transPage_ = NULL;
        schedFsm->setNextState(NULL);
        return TraverseState::TraverseStop;
    } else {
        schedFsm->setNextState(&schedFsm->processScalarPage_);
        return TraverseState::TraverseNext;
    }
}

FsmState::TraverseState
StateDequeueScalarPage::doWork()
{
    assert(Txn::currentTxn().valid());

    SchedFsmTransportPage *schedFsm =
        dynamic_cast<SchedFsmTransportPage *>(getSchedFsm());
    // we shouldn't have a transPage if we are dequeueing
    assert(schedFsm->transPage_ == NULL);
    assert(schedFsm->msgCopy_.msgHdr.eph.txn == Txn::currentTxn());

    schedFsm->transPage_ = schedFsm->demystifyMgr_->dequeueScalarPage(
        schedFsm->msgCopy_.msgHdr.eph.txn);

    if (!schedFsm->transPage_) {
        // if we didn't get a transPage, we are done
        schedFsm->setNextState(NULL);
        return TraverseState::TraverseStop;
    } else {
        // if we got a transPage, process it
        schedFsm->setNextState(&schedFsm->processScalarPage_);
        return TraverseState::TraverseNext;
    }
}

FsmState::TraverseState
StateProcessScalarPage::doWork()
{
    assert(Txn::currentTxn().valid());

    SchedFsmTransportPage *schedFsm =
        dynamic_cast<SchedFsmTransportPage *>(getSchedFsm());

    assert(schedFsm->transPage_ != NULL);
    assert(schedFsm->msgCopy_.msgHdr.eph.txn == Txn::currentTxn());

    DemystifyMgr::Op demystifyOp =
        (DemystifyMgr::Op) schedFsm->transPage_->demystifyOp;
    XdbMgr *xdbMgr = XdbMgr::get();
    Xdb *xdbDst = NULL;
    TransportPage *vtp = schedFsm->transPage_;
    Status status = schedFsm->msgCopy_.msgHdr.eph.status;
    BailIfFailed(status);

    status = vtp->deserialize();
    BailIfFailed(status);

    Operators::PerTxnInfo *perTxnInfo;
    perTxnInfo = (Operators::PerTxnInfo *) vtp->dstNodeTxnCookie;
    xdbDst = perTxnInfo->dstXdb;

    xdbMgr->xdbIncRecvTransPage(xdbDst);

    if (demystifyOp == DemystifyMgr::Op::Index &&
        schedFsm->indexPage_.indexPages_ == NULL) {
        status = schedFsm->allocContext(demystifyOp);
        BailIfFailed(status);
    }

    status = demystifyRecvHandlers[demystifyOp]
                 ->processScalarPage(vtp, schedFsm->indexPage_.indexPages_);
    BailIfFailed(status);

CommonExit:
    schedFsm->msgCopy_.msgHdr.eph.status = status;

    TransportPageMgr::get()->freeTransportPage(vtp);
    schedFsm->transPage_ = NULL;

    // continue processing incoming scalar pages
    schedFsm->setNextState(&schedFsm->dequeueScalarPage_);
    return TraverseState::TraverseNext;
}

void
XdbMeta::processQueuedSchedFsmTp()
{
    Status status = StatusOk;

    setKeyTypeLock.lock();

#ifdef DEBUG
    // If there is any pending SchedFsm held hostage for SetKeyType,
    // then below invariant must hold true.
    if (this->schedFsmPendingCount) {
        assert(this->schedFsmPendingCount == this->schedFsmListSize + 1);
    }
#endif

    // Drain queued Schedulables and add to Runtime Scheduler for processing.
    while (this->schedFsmList != NULL) {
        SchedFsmTransportPage *schedFsm = this->schedFsmList;
        this->schedFsmList = schedFsmList->setKeyTypeListLink_;

        status = Runtime::get()->schedule(schedFsm);
        if (status != StatusOk) {
            // Looks like we failed to Schedule pending Schedulables onto
            // Runtime. Add this SchedFsm back to the pending list, so
            // this Txn can be aborted cleanly.
            if (this->schedFsmList == NULL) {
                this->schedFsmList = schedFsm;
                schedFsm->setKeyTypeListLink_ = NULL;
                assert(this->schedFsmListSize == 1);
            } else {
                assert(this->schedFsmListSize > 1);
                schedFsm->setKeyTypeListLink_ = this->schedFsmList;
                this->schedFsmList = schedFsm;
            }
            goto CommonExit;
        }
        assert(this->schedFsmListSize >= 0 &&
               this->schedFsmPendingCount == this->schedFsmListSize + 1);
        this->schedFsmListSize--;
        this->schedFsmPendingCount--;
    }

CommonExit:
    if (status != StatusOk) {
        // Our attempt to drain pending SchedFsm failed, so just abort Txn.
        while (this->schedFsmList != NULL) {
            SchedFsmTransportPage *schedFsm = this->schedFsmList;
            this->schedFsmList = schedFsmList->setKeyTypeListLink_;
            schedFsm->msgCopy_.msgHdr.eph.status = status;
            schedFsm->markToAbort();
            schedFsm->done();
            StatsLib::statAtomicIncr64(Operators::indexSetKeyTypeAborted);
            assert(this->schedFsmListSize >= 0 &&
                   this->schedFsmPendingCount == this->schedFsmListSize + 1);
            this->schedFsmListSize--;
            this->schedFsmPendingCount--;
        }
    }

    // Account for the SchedFsm Fiber held hostage to drain all Schedulables.
    if (this->schedFsmPendingCount == 1) {
        this->schedFsmPendingCount--;
    }
    assert(this->schedFsmListSize == 0 && this->schedFsmPendingCount == 0);

    setKeyTypeLock.unlock();
}

FsmState::TraverseState
StateIndexPage::doWork()
{
    assert(Txn::currentTxn().valid());

    SchedFsmTransportPage *schedFsm =
        dynamic_cast<SchedFsmTransportPage *>(getSchedFsm());
    assert(schedFsm->transPage_ != NULL);
    assert(schedFsm->msgCopy_.msgHdr.eph.txn == Txn::currentTxn());
    Status status;
    TransportPage *tp = schedFsm->transPage_;
    NewTupleMeta *tupleMeta = &tp->tupMeta;
    XdbMgr *xdbMgr = XdbMgr::get();

    // Ensure that if this SchedFsm is rescheduled, setUp happens exactly once,
    // since setUp is not idempotent.
    if (init_ == false) {
        XdbMgr *xdbMgr = XdbMgr::get();
        if (tp->dstNodeTxnCookie != OpTxnCookieInvalid) {
            Operators::PerTxnInfo *perTxnInfo =
                (Operators::PerTxnInfo *) tp->dstNodeTxnCookie;
            dstXdb_ = perTxnInfo->dstXdb;
            dstMeta_ = perTxnInfo->dstMeta;
        } else {
            status = xdbMgr->xdbGet(tp->dstXdbId, &dstXdb_, &dstMeta_);
            assert(status == StatusOk);
        }
        status = tp->deserialize();
        BailIfFailed(status);
        init_ = true;
    }

TryAgain:
    // Check if key type is already set, or the burden is on us to set it.
    // Note that below code follows the principle of holding one Fiber
    // hostable by suspending it on timed Semaphore while the rest of
    // Schedulables are queued.
    //
    // Note the xdbSetKeyType() is called from other code paths that are
    // ok with suspended Fibers, b/c number of suspended Fibers is bounded.
    // But this code path can have a suspended Fiber per TP which is unbounded.
    for (unsigned ii = 0; ii < dstMeta_->numKeys; ii++) {
        if (unlikely(dstMeta_->keyAttr[ii].type == DfUnknown)) {
            int keyIndex = dstMeta_->keyAttr[ii].valueArrayIndex;
            DfFieldType keyType = tupleMeta->getFieldType(keyIndex);

            if (keyType != DfUnknown && keyType != DfScalarObj) {
                status = xdbMgr->xdbSetKeyType(dstMeta_, keyType, ii, true);
                if (status == StatusAgain) {
                    // Set key type attempt failed with tryLock.
                    StatsLib::statAtomicIncr64(
                        Operators::indexSetKeyTypeQueued);

                    // Here the first guy that stumbles on SetKeyType tryLock
                    // is held hostage by suspending his fiber on Semaphore
                    // timedWait. This is to ensure the queued Schedulables
                    // on SetKeyType are guaranteed to get drained.
                    dstMeta_->setKeyTypeLock.lock();

                    if (!setKeyTypeDrainer_ &&
                        dstMeta_->schedFsmPendingCount == 0) {
                        setKeyTypeDrainer_ = true;
                        dstMeta_->schedFsmPendingCount++;
                    }

                    if (setKeyTypeDrainer_) {
                        dstMeta_->setKeyTypeLock.unlock();
                        // First guy is held hostage and suspended.
                        setKeyTypeSem_.timedWait(SetKeyTypeSemTimeOutUsecs);
                        goto TryAgain;
                    }

                    dstMeta_->schedFsmPendingCount++;

                    // I am not the first guy to deal with setKeyType tryLock
                    // failure. So my Schedulable can be queued and am
                    // guaranteed to be Scheduled.
                    if (dstMeta_->schedFsmList == NULL) {
                        dstMeta_->schedFsmList = schedFsm;
                        schedFsm->setKeyTypeListLink_ = NULL;
                        assert(dstMeta_->schedFsmListSize == 0);
                    } else {
                        assert(dstMeta_->schedFsmListSize > 0);
                        schedFsm->setKeyTypeListLink_ = dstMeta_->schedFsmList;
                        dstMeta_->schedFsmList = schedFsm;
                    }

                    dstMeta_->schedFsmListSize++;
                    assert(dstMeta_->schedFsmListSize + 1 ==
                           dstMeta_->schedFsmPendingCount);
                    dstMeta_->setKeyTypeLock.unlock();

                    atomicInc64(&schedFsm->indexPageRef_);

                    // Now that the SchedFsm has been queued, FSM can be
                    // stopped. FSM will be restarted when the Schedulable
                    // is re-scheduled on the Runtime explicitly.
                    return TraverseState::TraverseStop;
                } else if (status != StatusOk) {
                    goto CommonExit;
                } else {
                    continue;
                }
            }
        }
    }

    if (setKeyTypeDrainer_) {
        // I am the anointed guy to drain any pending Schedulables on
        // SetKeyType.
        dstMeta_->processQueuedSchedFsmTp();
    }

    status = Operators::processIndexPage(tp);
    BailIfFailed(status);

CommonExit:
    TransportPageMgr::get()->freeTransportPage(tp);
    schedFsm->transPage_ = NULL;

    schedFsm->msgCopy_.msgHdr.eph.status = status;

    schedFsm->setNextState(NULL);
    return TraverseState::TraverseStop;
}

void
TransportPageMgr::transportPageShipComplete(MsgEphemeral *eph, void *payload)
{
    UsrEph *usrEph = (UsrEph *) eph->ephemeral;
    TransportPageHandle *transPageHandle = usrEph->transPageHandle;
    verify(atomicDec32(&outstandingPagesCounter_[usrEph->dstNodeId]) >= 0);

    if (eph->status != StatusOk) {
        // XXX Consider returning per-node status
        transPageHandle->cmpStatus_ = eph->status;
        memBarrier();
        StatsLib::statAtomicIncr64(shipCompletionFailure_);
    }

    // if schedFsm_ is set, then we want to use Fsm calls to track completions
    // otherwise use the semaphore
    if (transPageHandle->schedFsm_ != NULL) {
        transPageHandle->schedFsm_->done();
    } else {
        transPageHandle->lock_.lock();
        if (!atomicDec32(&transPageHandle->outstandingTransportPageMsgs_)) {
            transPageHandle->wakeupCv_.signal();
        }
        transPageHandle->lock_.unlock();
    }

    freeUsrEph(usrEph);
}
