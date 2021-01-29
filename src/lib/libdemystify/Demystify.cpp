// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "operators/XcalarEval.h"
#include "df/DataFormat.h"
#include "util/Math.h"
#include "operators/Xdf.h"
#include "dataset/Dataset.h"
#include "usrnode/UsrNode.h"
#include "util/MemTrack.h"
#include "stat/Statistics.h"
#include "sys/XLog.h"
#include "transport/TransportPage.h"
#include "libapis/LibApisCommon.h"
#include "demystify/Demystify.h"
#include "runtime/Runtime.h"

DemystifyRecv *demystifyRecvHandlers[DemystifyMgr::Op::Last];
DemystifyMgr *DemystifyMgr::instance = NULL;

DemystifyMgr::DemystifyMgr() {}

DemystifyMgr::~DemystifyMgr() {}

Status  // static
DemystifyMgr::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) DemystifyMgr;
    if (instance == NULL) {
        return StatusNoMem;
    }

    return StatusOk;
}

void  // static
DemystifyMgr::destroy()
{
    if (instance == NULL) {
        return;
    }

    delete instance;
    instance = NULL;
}

DemystifyMgr *
DemystifyMgr::get()
{
    return instance;
}

template <typename OpDemystifySend>
DemystifySend<OpDemystifySend>::DemystifySend(
    DemystifyMgr::Op op,
    void *localCookie,
    DemystifySend<OpDemystifySend>::SendOption sendOption,
    TransportPage **demystifyPages,
    XdbId dstXdbId,
    DfFieldType keyType,
    int keyIndex,
    bool *validIndices,
    TransportPageHandle *demystifyPagesHandle,
    TransportPage **additionalPages,
    TransportPageHandle *additionalPagesHandle)
{
    op_ = op;
    localCookie_ = localCookie;
    sendOption_ = sendOption;
    demystifyPages_ = demystifyPages;
    dstXdbId_ = dstXdbId;
    keyType_ = keyType;
    keyIndex_ = keyIndex;
    validIndices_ = validIndices;
    demystifyPagesHandle_ = demystifyPagesHandle;

    additionalPages_ = additionalPages;
    additionalPagesHandle_ = additionalPagesHandle;

    // private
    myNode_ = Config::get()->getMyNodeId();
    activeNodes_ = Config::get()->getActiveNodes();
    tpMgr_ = TransportPageMgr::get();
    df_ = DataFormat::get();
}

template DemystifySend<FilterAndMapDemystifySend>::DemystifySend(
    DemystifyMgr::Op op,
    void *localCookie,
    DemystifySend<FilterAndMapDemystifySend>::SendOption sendOption,
    TransportPage **demystifyPages,
    XdbId dstXdbId,
    DfFieldType keyType,
    int keyIndex,
    bool *validIndices,
    TransportPageHandle *demystifyPagesHandle,
    TransportPage **additionalPages,
    TransportPageHandle *additionalPagesHandle);
template DemystifySend<IndexDemystifySend>::DemystifySend(
    DemystifyMgr::Op op,
    void *localCookie,
    DemystifySend<IndexDemystifySend>::SendOption sendOption,
    TransportPage **demystifyPages,
    XdbId dstXdbId,
    DfFieldType keyType,
    int keyIndex,
    bool *validIndices,
    TransportPageHandle *demystifyPagesHandle,
    TransportPage **additionalPages,
    TransportPageHandle *additionalPagesHandle);
template DemystifySend<GroupByDemystifySend>::DemystifySend(
    DemystifyMgr::Op op,
    void *localCookie,
    DemystifySend<GroupByDemystifySend>::SendOption sendOption,
    TransportPage **demystifyPages,
    XdbId dstXdbId,
    DfFieldType keyType,
    int keyIndex,
    bool *validIndices,
    TransportPageHandle *demystifyPagesHandle,
    TransportPage **additionalPages,
    TransportPageHandle *additionalPagesHandle);
template DemystifySend<CreateScalarTableDemystifySend>::DemystifySend(
    DemystifyMgr::Op op,
    void *localCookie,
    DemystifySend<CreateScalarTableDemystifySend>::SendOption sendOption,
    TransportPage **demystifyPages,
    XdbId dstXdbId,
    DfFieldType keyType,
    int keyIndex,
    bool *validIndices,
    TransportPageHandle *demystifyPagesHandle,
    TransportPage **additionalPages,
    TransportPageHandle *additionalPagesHandle);

template <typename OpDemystifySend>
Status
DemystifySend<OpDemystifySend>::sendValueArray(unsigned dstNodeId,
                                               const NewTupleMeta *tupleMeta,
                                               NewTupleValues *tupleValues)
{
    Status status;
    bool pageAlloced = false;

    do {
        if (unlikely(demystifyPages_[dstNodeId] == NULL)) {
            status =
                demystifyPagesHandle_
                    ->allocQuotaTransportPage(TransportPageType::SendSourcePage,
                                              demystifyPages_,
                                              (TransportPage **)
                                                  additionalPages_,
                                              additionalPagesHandle_,
                                              activeNodes_,
                                              dstNodeId);
            if (status != StatusOk) {
                break;
            }

            assert(demystifyPages_[dstNodeId] != NULL);

            pageAlloced = true;
        }

        if (unlikely(demystifyPages_[dstNodeId]->dstXdbId == XidInvalid)) {
            demystifyPagesHandle_->initPage(demystifyPages_[dstNodeId],
                                            dstNodeId,
                                            tupleMeta,
                                            op_);
        }

        status = demystifyPagesHandle_->insertKv(&demystifyPages_[dstNodeId],
                                                 tupleMeta,
                                                 tupleValues);
        if (status != StatusOk && pageAlloced) {
            // this means we allocated a new page and still couldn't fit
            // the row
            status = StatusMaxRowSizeExceeded;
            tpMgr_->freeTransportPage(demystifyPages_[dstNodeId]);
            demystifyPages_[dstNodeId] = NULL;
            break;
        }
    } while (status == StatusNoData);

    return status;
}

template <typename OpDemystifySend>
Status
DemystifySend<OpDemystifySend>::demystifyRow(NewKeyValueEntry *srcKvEntry,
                                             DfFieldValue rowMetaField,
                                             Atomic32 *countToIncrement)
{
    NodeId nodeId;
    Status status = StatusOk, failStatus = StatusOk;
    unsigned ii;
    uint64_t numValsPerPair = srcKvEntry->kvMeta_->tupMeta_->getNumFields();
    bool rowIssued = false;

    for (ii = 0; ii < activeNodes_; ii++) {
        PerNodeState *perNodeState = &perNodeState_[ii];
        perNodeState->issuedToNode = false;

        perNodeState->dstTupleMeta.setNumFields(numValsPerPair + 1);
        perNodeState->dstTuple.setInvalid(0, numValsPerPair + 1);

        for (unsigned jj = 0; jj < numValsPerPair; jj++) {
            perNodeState->dstTupleMeta.setFieldType(DfFatptr, jj);
        }

        perNodeState->dstTupleMeta.setFieldType(DfOpRowMetaPtr, numValsPerPair);

        // This is always fixed size packing since this is always FatPtrs +
        // RowMetaPtr.
        perNodeState->dstTupleMeta.setFieldsPacking(
            NewTupleMeta::FieldsPacking::Fixed);
    }

    for (ii = 0; ii < numValsPerPair; ii++) {
        if (!validIndices_[ii]) {
            continue;
        }

        bool retIsValid;
        DfFieldType typeTmp = srcKvEntry->kvMeta_->tupMeta_->getFieldType(ii);
        DfFieldValue valueTmp =
            srcKvEntry->tuple_.get(ii, numValsPerPair, typeTmp, &retIsValid);
        if (!retIsValid) {
            // this field doesn't exist, the srcNode will handle the FNF
            nodeId = myNode_;
        } else if (typeTmp == DfFatptr) {
            nodeId = df_->fatptrGetNodeId(valueTmp.fatptrVal);
        } else {
            // It's the responsibility of the srcNode (aka me) to
            // demystify immediates. Remote nodes will pretend
            // immediates in a valueArray doesn't exist
            nodeId = myNode_;
        }

        // for remote fatPtrs, append to value array
        if (nodeId != myNode_) {
            DfFieldValue dstValueTmp;
            dstValueTmp.fatptrVal = valueTmp.fatptrVal;
            perNodeState_[nodeId].dstTuple.set(ii, dstValueTmp, DfFatptr);
        }

        if (perNodeState_[nodeId].issuedToNode) {
            continue;
        }

        perNodeState_[nodeId].issuedToNode = true;
        rowIssued = true;
        atomicInc32(countToIncrement);
    }

    // if row wasn't sent out to anyone, I'll be the one to process it
    if (!rowIssued) {
        perNodeState_[myNode_].issuedToNode = true;
        atomicInc32(countToIncrement);
    }

    for (nodeId = 0; nodeId < activeNodes_; nodeId++) {
        PerNodeState *perNodeState = &perNodeState_[nodeId];
        if (perNodeState->issuedToNode == false) {
            continue;
        }

        if (nodeId == myNode_) {
            status = processDemystifiedRow(localCookie_,
                                           srcKvEntry,
                                           rowMetaField,
                                           countToIncrement);
            if (status != StatusOk && failStatus == StatusOk) {
                failStatus = status;
            }
            continue;
        }

        // copy over rowMeta ptr
        perNodeState->dstTuple.set(numValsPerPair,
                                   rowMetaField,
                                   DfOpRowMetaPtr);

        status = sendValueArray(nodeId,
                                &perNodeState->dstTupleMeta,
                                &perNodeState->dstTuple);
        if (status != StatusOk && failStatus == StatusOk) {
            failStatus = status;
        }
    }

    return failStatus;
}

template Status DemystifySend<FilterAndMapDemystifySend>::demystifyRow(
    NewKeyValueEntry *srcKvEntry,
    DfFieldValue rowMetaField,
    Atomic32 *countToIncrement);
template Status DemystifySend<IndexDemystifySend>::demystifyRow(
    NewKeyValueEntry *srcKvEntry,
    DfFieldValue rowMetaField,
    Atomic32 *countToIncrement);
template Status DemystifySend<CreateScalarTableDemystifySend>::demystifyRow(
    NewKeyValueEntry *srcKvEntry,
    DfFieldValue rowMetaField,
    Atomic32 *countToIncrement);
template Status DemystifySend<GroupByDemystifySend>::demystifyRow(
    NewKeyValueEntry *srcKvEntry,
    DfFieldValue rowMetaField,
    Atomic32 *countToIncrement);

bool
DemystifyMgr::enqueueScalarPage(MsgEphemeral *eph, TransportPage *transPage)
{
    Status status = StatusOk;
    Txn txn = eph->txn;
    DemystifyContext *context;
    bool enqueued = false;
    bool locked = false;
    assert(eph->status == StatusOk);

    // XXX Lock wait time may be high because of number of incoming Scalar TPs
    // which has been demystified. Needs to move towards a per slot lock
    // instead.
    lock_.lock();
    locked = true;

    context = contextTable_.find(txn.id_);
    if (unlikely(context == NULL)) {
        context = new (std::nothrow) DemystifyContext(txn);
        BailIfNull(context);

        status = contextTable_.insert(context);
        BailIfFailed(status);
    }

    if (context->numScalarPageWorkers_ >= context->maxScalarPageWorkers_) {
        transPage->next = context->listHead_;
        context->listHead_ = transPage;
        context->listSize_++;
        enqueued = true;
    } else {
        // we have become a worker and will continue processing pages until the
        // queue is empty
        context->numScalarPageWorkers_++;
    }

CommonExit:
    if (status != StatusOk) {
        TransportPageMgr::get()->freeTransportPage(transPage);
    }

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    eph->status = status;

    return enqueued;
}

TransportPage *
DemystifyMgr::dequeueScalarPage(Txn txn)
{
    DemystifyContext *context;
    TransportPage *transPage;

    lock_.lock();

    context = contextTable_.find(txn.id_);
    assert(context != NULL);

    transPage = context->listHead_;

    if (transPage != NULL) {
        context->listHead_ = transPage->next;
        context->listSize_--;
    } else {
        // if there are no more pages left, we have served our term as a worker
        context->numScalarPageWorkers_--;
    }

    lock_.unlock();

    return transPage;
}

void
DemystifyMgr::removeDemystifyContext(Txn txn)
{
    DemystifyContext *context;

    lock_.lock();
    context = contextTable_.remove(txn.id_);
    if (context != NULL) {
        assert(context->numScalarPageWorkers_ == 0);
        assert(context->listSize_ == 0);
        assert(context->listHead_ == NULL);
        delete context;
    }
    lock_.unlock();
}
