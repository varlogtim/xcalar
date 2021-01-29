// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "xdb/HashTree.h"
#include "operators/OperatorsXdbPageOps.h"
#include "xdb/XdbInt.h"
#include "msg/Xid.h"

class PopulateIndexWork : public Schedulable
{
  public:
    PopulateIndexWork(TrackHelpers::WorkerType workerTypeIn,
                      unsigned workerIdIn,
                      TrackHelpers *trackHelpers,
                      HashTree::Index *index,
                      HashTree *hashTree)
        : Schedulable("PopulateIndexWork"),
          trackHelpers_(trackHelpers),
          hashTree_(hashTree),
          index_(index),
          workerType_(workerTypeIn),
          workerId_(workerIdIn)
    {
        xdbMgr = XdbMgr::get();
        txn = Txn::currentTxn();
    }

    virtual void run();
    virtual void done() { delete this; }

    TrackHelpers *trackHelpers_;
    HashTree *hashTree_;
    HashTree::Index *index_;

    TrackHelpers::WorkerType workerType_;
    unsigned workerId_;

    XdbMgr *xdbMgr;
    Txn txn;

    bool insertHandleInit_ = false;
    OpInsertHandle insertHandle_;

  private:
    Status setup();
    void tearDown();

    Status processSlot(unsigned slot);
};

void
HashTree::removeIndex(const char *keyName)
{
    if (strcmp(keyName, "*") == 0) {
        // remove all indexes
        indexHashTableLock_.lock();
        indexHashTable_.removeAll(&Index::destroy);
        indexHashTableLock_.unlock();
    } else {
        Index *index = NULL;

        int keyIdx = getColumnIdx(keyName);

        indexHashTableLock_.lock();
        index = indexHashTable_.remove(keyIdx);
        indexHashTableLock_.unlock();

        if (index) {
            delete index;
        }
    }
}

Status
HashTree::addIndex(const char *keyName)
{
    Status status;
    Index *index = NULL;

    int keyIdx = getColumnIdx(keyName);

    indexHashTableLock_.lock();
    index = indexHashTable_.find(keyIdx);
    indexHashTableLock_.unlock();

    if (index) {
        // index has already been created
        return StatusOk;
    }

    status = createIndex(keyName, &index);
    BailIfFailed(status);

    status = populateIndexXdb(index);
    BailIfFailed(status);

    status = setupIndexXdbPageBatches(index);
    BailIfFailed(status);

    indexHashTableLock_.lock();
    indexHashTable_.insert(index);
    indexHashTableLock_.unlock();

CommonExit:
    if (status != StatusOk) {
        if (index) {
            delete index;
        }
    }

    return status;
}

Status
HashTree::createIndex(const char *keyName, Index **indexOut)
{
    Status status;
    Xid indexXdbId = XidMgr::get()->xidGetNext();
    DfFieldType keyType = DfUnknown;
    int srcKeyIdx = InvalidIdx;
    NewTupleMeta tupMeta;
    unsigned numImmediates = Index::LastIdx;
    const char *immediateNames[numImmediates];
    bool xdbCreated = false;
    Index *index = NULL;

    srcKeyIdx = getColumnIdx(keyName);
    if (srcKeyIdx == InvalidIdx) {
        status = StatusFieldNotFound;
        goto CommonExit;
    }

    keyType = tupMeta_.getFieldType(srcKeyIdx);

    // schema of the index is the key followed by a rowNum
    tupMeta.setNumFields(numImmediates);

    immediateNames[Index::KeyIdx] = keyName;
    tupMeta.setFieldType(keyType, Index::KeyIdx);

    immediateNames[Index::TupleOffsetIdx] = "nextTupleOffset";
    tupMeta.setFieldType(DfInt64, Index::TupleOffsetIdx);

    immediateNames[Index::TupleIdxIdx] = "nextTupleIdx";
    tupMeta.setFieldType(DfInt64, Index::TupleIdxIdx);

    immediateNames[Index::EndBufOffsetIdx] = "bufOffsetFromEnd";
    tupMeta.setFieldType(DfInt64, Index::EndBufOffsetIdx);

    immediateNames[Index::XdbPagePtrIdx] = "xdbPagePtr";
    tupMeta.setFieldType(DfInt64, Index::XdbPagePtrIdx);

    status = XdbMgr::get()->xdbCreate(indexXdbId,
                                      keyName,
                                      keyType,
                                      0,
                                      &tupMeta,
                                      NULL,
                                      0,
                                      immediateNames,
                                      numImmediates,
                                      NULL,
                                      0,
                                      Ascending,
                                      XdbLocal,
                                      DhtInvalidDhtId);
    BailIfFailed(status);
    xdbCreated = true;

    Xdb *indexXdb;
    status = XdbMgr::get()->xdbGet(indexXdbId, &indexXdb, NULL);
    assert(status == StatusOk);

    indexXdb->keyRangePreset = false;

    index = new (std::nothrow) Index(indexXdb, srcKeyIdx);
    BailIfNull(index);

CommonExit:
    if (status != StatusOk) {
        if (xdbCreated) {
            XdbMgr::get()->xdbDropLocalInternal(indexXdbId);
            xdbCreated = false;
        }

        if (index) {
            delete index;
        }
    } else {
        *indexOut = index;
    }

    return status;
}

Status
HashTree::setupIndexXdbPageBatches(HashTree::Index *index)
{
    Status status;

    // create exactly numSlots equally sized batches that can span
    // multiple slots
    unsigned numSlots = index->indexXdb->hashSlotInfo.hashSlots;
    unsigned numBatchesTot = numSlots;
    unsigned numPages = index->indexXdb->numPages;
    unsigned pagesPerBatch = numPages / numBatchesTot + 1;

    unsigned numBatches = 0;
    unsigned count = 0;

    unsigned startSlot;
    XdbPage *startPage = NULL;

    XdbPage **pages = NULL;
    unsigned *slots = NULL;

    pages = (XdbPage **) memAlloc(numPages * sizeof(*pages));
    BailIfNull(pages);

    slots = (unsigned *) memAlloc(numPages * sizeof(*slots));
    BailIfNull(slots);

    index->batches = new (std::nothrow) XdbPageBatch[numBatchesTot];
    BailIfNull(index->batches);

    // populate pages array in order
    for (unsigned ii = 0; ii < numSlots; ii++) {
        XdbPage *page = XdbMgr::getXdbHashSlotNextPage(
            &index->indexXdb->hashSlotInfo.hashBase[ii]);

        while (page) {
            slots[count] = ii;
            pages[count++] = page;
            page = page->hdr.nextPage;
        }
    }

    assert(count == numPages);

    count = 0;
    unsigned ii;
    for (ii = 0; ii < numPages; ii++) {
        if (!startPage) {
            startPage = pages[ii];
            startSlot = slots[ii];
        }

        count++;
        if (count == pagesPerBatch) {
            index->batches[numBatches++].init(startPage,
                                              pages[ii],
                                              count,
                                              index->indexXdb,
                                              startSlot);
            startPage = NULL;
            count = 0;
        }
    }

    if (count > 0) {
        index->batches[numBatches++].init(startPage,
                                          pages[ii - 1],
                                          count,
                                          index->indexXdb,
                                          startSlot);
    }

    assert(numBatches <= numBatchesTot);
    index->numBatches = numBatches;

CommonExit:
    if (pages) {
        memFree(pages);
    }

    if (slots) {
        memFree(slots);
    }

    return status;
}

Status
HashTree::populateIndexXdb(Index *index)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    PopulateIndexWork **scheds = NULL;
    unsigned numScheds = Operators::getNumWorkers(mainXdb_->numRows);

    TrackHelpers *trackHelpers = NULL;
    // each slot is a chunk
    uint64_t numChunks = hashSlots_;

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    scheds = new (std::nothrow) PopulateIndexWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            PopulateIndexWork(ii == 0 ? TrackHelpers::Master
                                      : TrackHelpers::NonMaster,
                              ii,
                              trackHelpers,
                              index,
                              this);
        BailIfNull(scheds[ii]);
    }

    status = trackHelpers->schedThroughput((Schedulable **) scheds, numScheds);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

    status = XdbMgr::get()->xdbLoadDoneByXdbInt(index->indexXdb, NULL);
    BailIfFailed(status);
CommonExit:

    if (scheds) {
        for (unsigned jj = 0; jj < numScheds; jj++) {
            if (scheds[jj]) {
                delete scheds[jj];
                scheds[jj] = NULL;
            }
        }
        delete[] scheds;
        scheds = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    return status;
}

Status
PopulateIndexWork::setup()
{
    Status status;

    status = opGetInsertHandle(&insertHandle_,
                               &index_->indexXdb->meta->loadInfo,
                               XdbInsertRandomHash);
    BailIfFailed(status);
    insertHandleInit_ = true;

CommonExit:
    return status;
}

void
PopulateIndexWork::tearDown()
{
    if (insertHandleInit_) {
        opPutInsertHandle(&insertHandle_);
        insertHandleInit_ = false;
    }
}
void
PopulateIndexWork::run()
{
    Status status = StatusOk;
    status = trackHelpers_->helperStart();

    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = setup();
    // if setup fails we want to go ahead and mark all work items as complete
    // then report a bad status at the end

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        status = processSlot(ii);

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    tearDown();
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
PopulateIndexWork::processSlot(unsigned slot)
{
    Status status;

    TableCursor cur;
    bool srcCursorInited = false;

    XdbMeta *srcMeta = XdbMgr::xdbGetMeta(hashTree_->mainXdb_);
    XdbMeta *dstMeta = XdbMgr::xdbGetMeta(index_->indexXdb);

    NewKeyValueMeta *srcKvMeta = &srcMeta->kvNamedMeta.kvMeta_;
    NewKeyValueMeta *dstKvMeta = &dstMeta->kvNamedMeta.kvMeta_;

    NewKeyValueEntry srcKv(srcKvMeta);
    NewKeyValueEntry dstKv(dstKvMeta);

    bool cursorExhausted = false;
    status = XdbMgr::get()->createCursorFast(hashTree_->mainXdb_, slot, &cur);
    if (status == StatusNoData) {
        cursorExhausted = true;
        status = StatusOk;
        goto CommonExit;
    }
    BailIfFailed(status);

    srcCursorInited = true;

    DfFieldType keyType;
    keyType = srcKvMeta->tupMeta_->getFieldType(index_->keyIdxInHashTree);

    while (!cursorExhausted) {
        // save position of the row we are about to getNext
        dstKv.init();
        HashTree::Index::saveCursorInIndexKv(&cur, &dstKv);

        status = cur.getNext(&srcKv);
        if (status == StatusNoData) {
            cursorExhausted = true;
            break;
        }
        BailIfFailed(status);

        // setup key val
        bool valid = false;
        DfFieldValue keyVal =
            srcKv.tuple_.get(index_->keyIdxInHashTree, &valid);
        if (valid) {
            dstKv.tuple_.set(HashTree::Index::KeyIdx, keyVal, keyType);
        } else {
            assert(!dstKv.tuple_.isValid(HashTree::Index::KeyIdx));
        }

        status = opPopulateInsertHandle(&insertHandle_, &dstKv);
        BailIfFailed(status);
    }

    status = StatusOk;
CommonExit:
    if (srcCursorInited) {
        CursorManager::get()->destroy(&cur);
        srcCursorInited = false;
    }

    return status;
}

HashTree::Index *
HashTree::getIndex(const char *keyName)
{
    Index *index;
    int keyIdx = getColumnIdx(keyName);

    if (keyIdx == InvalidIdx) {
        return NULL;
    }

    indexHashTableLock_.lock();
    index = indexHashTable_.find(keyIdx);
    indexHashTableLock_.unlock();

    return index;
}

void
HashTree::Index::saveCursorInIndexKv(TableCursor *cur,
                                     NewKeyValueEntry *indexKv)
{
    NewTuplesCursor::Position pos = cur->xdbPgTupCursor.getPosition();
    DfFieldValue tupOffset, tupIdx, bufOffset, xdbPagePtr;

    tupOffset.int64Val = pos.nextTupleOffset;
    tupIdx.int64Val = pos.nextTupleIdx;
    bufOffset.int64Val = pos.bufOffsetFromEnd;
    xdbPagePtr.int64Val = (int64_t) cur->xdbPgCursor.xdbPage_;

    indexKv->tuple_.set(TupleOffsetIdx, tupOffset, DfInt64);
    indexKv->tuple_.set(TupleIdxIdx, tupIdx, DfInt64);
    indexKv->tuple_.set(EndBufOffsetIdx, bufOffset, DfInt64);
    indexKv->tuple_.set(XdbPagePtrIdx, xdbPagePtr, DfInt64);
}

Status
HashTree::Index::restoreCursorFromIndexKv(NewKeyValueEntry *indexKv,
                                          TableCursor *cur)
{
    bool valid;
    Status status = StatusUnknown;
    NewTuplesCursor::Position pos;
    XdbPage *xdbPage = NULL;
    DfFieldValue tupOffset, tupIdx, bufOffset, xdbPagePtr;
    tupOffset = indexKv->tuple_.get(TupleOffsetIdx, &valid);
    tupIdx = indexKv->tuple_.get(TupleIdxIdx, &valid);
    bufOffset = indexKv->tuple_.get(EndBufOffsetIdx, &valid);
    xdbPagePtr = indexKv->tuple_.get(XdbPagePtrIdx, &valid);

    pos.nextTupleOffset = tupOffset.int64Val;
    pos.nextTupleIdx = tupIdx.int64Val;
    pos.bufOffsetFromEnd = bufOffset.int64Val;

    xdbPage = (XdbPage *) xdbPagePtr.int64Val;
    status = xdbPage->getRef(cur->xdbPgCursor.xdb_);
    if (status != StatusOk) {
        goto CommonExit;
    }

    cur->xdbPgCursor.slotId_ = 0;
    if (cur->xdbPgCursor.xdbPage_ != NULL) {
        XdbMgr::get()->pagePutRef(cur->xdbPgCursor.xdb_,
                                  0,
                                  cur->xdbPgCursor.xdbPage_);
        cur->xdbPgCursor.xdbPage_ = NULL;
    }
    cur->xdbPgCursor.xdbPage_ = xdbPage;
    xdbPage = NULL;

    new (&cur->xdbPgTupCursor)
        NewTuplesCursor(cur->xdbPgCursor.xdbPage_->tupBuf);
    cur->xdbPgTupCursor.setPosition(pos);
    status = StatusOk;

CommonExit:
    if (xdbPage != NULL) {
        assert(status != StatusOk);
        XdbMgr::get()->pagePutRef(cur->xdbPgCursor.xdb_,
                                  0,
                                  cur->xdbPgCursor.xdbPage_);
        xdbPage = NULL;
    }

    return (status);
}
