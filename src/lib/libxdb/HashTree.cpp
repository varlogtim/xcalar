// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <math.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "table/Table.h"
#include "xdb/HashTree.h"
#include "msg/Message.h"
#include "xdb/Xdb.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "util/Math.h"
#include "df/DataFormat.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "util/TrackHelpers.h"
#include "runtime/Runtime.h"
#include "runtime/Schedulable.h"
#include "msg/Xid.h"
#include "transport/TransportPage.h"
#include "gvm/Gvm.h"
#include "ns/LibNs.h"
#include "cursor/Cursor.h"
#include "xdb/XdbInt.h"
#include "operators/OperatorsXdbPageOps.h"
#include "operators/OperatorsTypes.h"
#include "operators/Operators.h"
#include "dag/DagLib.h"
#include "operators/XcalarEval.h"
#include "operators/OperatorsEvalTypes.h"
#include "queryparser/QueryParser.h"

static constexpr char moduleName[10] = "HashTree";

class InsertWork : public Schedulable
{
  public:
    InsertWork(TrackHelpers::WorkerType workerTypeIn,
               unsigned workerIdIn,
               TrackHelpers *trackHelpers,
               Xdb *xdb,
               XdbMeta *xdbMeta,
               int flags,
               OpKvEntryCopyMapping *mapping,
               int64_t batchIdIn,
               HashTree::UpdateStats *statsIn,
               HashTree *hashTree)
        : Schedulable("InsertWork"),
          trackHelpers_(trackHelpers),
          xdb_(xdb),
          xdbMeta_(xdbMeta),
          mapping_(mapping),
          batchId_(batchIdIn),
          stats_(statsIn),
          hashTree_(hashTree),
          workerType_(workerTypeIn),
          workerId_(workerIdIn),
          flags_(flags)
    {
        xdbMgr = XdbMgr::get();
        txn = Txn::currentTxn();
    }

    virtual ~InsertWork() = default;

    virtual void run();
    virtual void done() { delete this; }

    TrackHelpers *trackHelpers_;

    Xdb *xdb_ = NULL;

    XdbMeta *xdbMeta_;
    OpKvEntryCopyMapping *mapping_;
    int64_t batchId_;

    HashTree::UpdateStats *stats_;
    HashTree *hashTree_;
    TrackHelpers::WorkerType workerType_;
    unsigned workerId_;

    int flags_;

    XdbMgr *xdbMgr;
    Txn txn;

  private:
    Status processSlotForInsert(unsigned srcSlotId, unsigned dstSlotId);
    Status insertOnePage(XdbPage *xdbPage, unsigned dstSlot);
};

void
HashTree::freeXdbs()
{
    // iterate for both main and update xdb
    for (unsigned ii = 0; ii < 2; ii++) {
        Xdb *xdb;
        if (ii == 0) {
            xdb = mainXdb_;
        } else {
            xdb = updateXdb_;
        }

        if (xdb == NULL) {
            continue;
        }

        XdbMgr::get()->freeXdbPages(xdb);

        memAlignedFree(xdb->hashSlotInfo.hashBase);
        memAlignedFree(xdb->hashSlotInfo.hashBaseAug);

        if (xdb->pageHdrBackingPage != NULL) {
            XdbMgr::get()->xdbPutXdbPageHdr(xdb->pageHdrBackingPage);
            xdb->pageHdrBackingPage = NULL;
        }

        delete xdb;
    }
}

void
HashTree::destroy()
{
    freeXdbs();

    if (xdbMeta_) {
        XdbMgr::freeXdbMeta(xdbMeta_);
        xdbMeta_ = NULL;
    }

    if (keys_) {
        delete[] keys_;
    }

    while (updateHead_) {
        UpdateMeta *tmp = updateHead_->next;
        delete updateHead_;

        updateHead_ = tmp;
    }

    while (selectHead_) {
        SelectMeta *tmp = selectHead_->next;
        delete selectHead_;

        selectHead_ = tmp;
    }

    columns_.removeAll(&TableMgr::ColumnInfo::destroy);
    indexHashTable_.removeAll(&Index::destroy);

    delete this;
}

XdbMeta *
HashTree::setupXdbMeta()
{
    // add one for batch id
    unsigned numDstCols = columns_.getSize();

    XdbMeta *xdbMeta;

    XdbId xdbId = XidMgr::get()->xidGetNext();

    const char *immediateNames[TupleMaxNumValuesPerRecord];

    tupMeta_.setNumFields(numDstCols);
    tupMeta_.setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    TableMgr::ColumnInfo *columnInfo;

    for (TableMgr::ColumnInfoTable::iterator it = columns_.begin();
         (columnInfo = it.get()) != NULL;
         it.next()) {
        assert(columnInfo->getIdxInTable() < TupleMaxNumValuesPerRecord);
        immediateNames[columnInfo->getIdxInTable()] = columnInfo->getName();
        tupMeta_.setFieldType(columnInfo->getType(), columnInfo->getIdxInTable());
    }

    xdbMeta = XdbMgr::get()->xdbAllocMeta(xdbId,
                                          XidMgr::XidSystemUnorderedDht,
                                          NULL,
                                          numKeys_,
                                          keys_,
                                          0,
                                          NULL,
                                          numDstCols,
                                          immediateNames,
                                          0,
                                          NULL,
                                          &tupMeta_);

    return xdbMeta;
}

Status
HashTree::initSchema(const char *pubTableName,
                     TableMgr::ColumnInfoTable *columnsIn)
{
    Status status = StatusOk;
    unsigned numCols = columnsIn->getSize();
    TableMgr::ColumnInfo *columnInfo;

    // copy over input columns
    for (TableMgr::ColumnInfoTable::iterator it = columnsIn->begin();
         (columnInfo = it.get()) != NULL;
         it.next()) {
        TableMgr::ColumnInfo *col =
            new (std::nothrow) TableMgr::ColumnInfo(columnInfo->getIdxInTable(),
                                                    columnInfo->getName(),
                                                    columnInfo->getType());
        BailIfNullMsg(col,
                      StatusNoMem,
                      moduleName,
                      "Failed initSchema for publish table %s %lu: %s",
                      pubTableName,
                      hashTreeId_,
                      strGetFromStatus(status));

        columns_.insert(col);
    }

    // check for batchId
    columnInfo = columns_.find(XcalarBatchIdColumnName);
    if (columnInfo) {
        if (columnInfo->getType() != DfInt64) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed initSchema for publish table %s %lu, "
                          "%s must have type DfInt64: %s",
                          pubTableName,
                          hashTreeId_,
                          XcalarBatchIdColumnName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        batchIdx_ = columnInfo->getIdxInTable();
    } else {
        columnInfo = new (std::nothrow)
            TableMgr::ColumnInfo(numCols, XcalarBatchIdColumnName, DfInt64);
        BailIfNullMsg(columnInfo,
                      StatusNoMem,
                      moduleName,
                      "Failed initSchema for publish table %s %lu: %s",
                      pubTableName,
                      hashTreeId_,
                      strGetFromStatus(status));

        verifyOk(columns_.insert(columnInfo));
        batchIdx_ = numCols++;
    }

    // check for opCode
    columnInfo = columns_.find(XcalarOpCodeColumnName);
    if (columnInfo) {
        if (columnInfo->getType() != DfInt64) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed initSchema for publish table %s %lu, "
                          "%s must have type DfInt64: %s",
                          pubTableName,
                          hashTreeId_,
                          XcalarOpCodeColumnName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        opCodeIdx_ = columnInfo->getIdxInTable();
    } else {
        columnInfo = new (std::nothrow)
            TableMgr::ColumnInfo(numCols, XcalarOpCodeColumnName, DfInt64);
        BailIfNullMsg(columnInfo,
                      StatusNoMem,
                      moduleName,
                      "Failed initSchema for publish table %s %lu: %s",
                      pubTableName,
                      hashTreeId_,
                      strGetFromStatus(status));

        verifyOk(columns_.insert(columnInfo));
        opCodeIdx_ = numCols++;
    }

    // check for rankOver
    columnInfo = columns_.find(XcalarRankOverColumnName);
    if (columnInfo) {
        if (columnInfo->getType() != DfInt64) {
            status = StatusInval;
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "Failed initSchema for publish table %s %lu, "
                          "%s must have type DfInt64: %s",
                          pubTableName,
                          hashTreeId_,
                          XcalarRankOverColumnName,
                          strGetFromStatus(status));
            goto CommonExit;
        }

        rankOverIdx_ = columnInfo->getIdxInTable();
    } else {
        columnInfo = new (std::nothrow)
            TableMgr::ColumnInfo(numCols, XcalarRankOverColumnName, DfInt64);
        BailIfNullMsg(columnInfo,
                      StatusNoMem,
                      moduleName,
                      "Failed initSchema for publish table %s %lu: %s",
                      pubTableName,
                      hashTreeId_,
                      strGetFromStatus(status));

        verifyOk(columns_.insert(columnInfo));
        rankOverIdx_ = numCols++;
    }

    xdbMeta_ = setupXdbMeta();
    BailIfNull(xdbMeta_);

    updateXdb_ = new (std::nothrow) Xdb(XidInvalid, XdbLocal);
    BailIfNull(updateXdb_);

    mainXdb_ = new (std::nothrow) Xdb(XidInvalid, XdbLocal);
    BailIfNull(mainXdb_);

    status = initXdb(updateXdb_, pubTableName);
    BailIfFailed(status);

    status = initXdb(mainXdb_, pubTableName);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
HashTree::initXdb(Xdb *xdb, const char *pubTableName)
{
    Status status = StatusOk;

    xdb->meta = xdbMeta_;
    xdb->tupMeta = tupMeta_;

    // Hash table to implement a range based xdb.
    xdb->hashSlotInfo.hashSlots = hashSlots_;

    xdb->hashSlotInfo.hashBase =
        (XdbAtomicHashSlot *) memAllocAlignedExt(XdbMgr::XdbMinPageAlignment,
                                                 sizeof(XdbAtomicHashSlot) *
                                                     hashSlots_,
                                                 moduleName);
    BailIfNullMsg(xdb->hashSlotInfo.hashBase,
                  StatusNoMem,
                  moduleName,
                  "Failed initSchema for publish table %s %lu: %s",
                  pubTableName,
                  hashTreeId_,
                  strGetFromStatus(status));

    memZero(xdb->hashSlotInfo.hashBase, sizeof(XdbAtomicHashSlot) * hashSlots_);

    xdb->hashSlotInfo.hashBaseAug =
        (XdbHashSlotAug *) memAllocAlignedExt(XdbMgr::XdbMinPageAlignment,
                                              sizeof(XdbHashSlotAug) *
                                                  hashSlots_,
                                              moduleName);
    BailIfNullMsg(xdb->hashSlotInfo.hashBaseAug,
                  StatusNoMem,
                  moduleName,
                  "Failed initSchema for publish table %s %lu: %s",
                  pubTableName,
                  hashTreeId_,
                  strGetFromStatus(status));

    for (uint64_t ii = 0; ii < hashSlots_; ii++) {
        new (&xdb->hashSlotInfo.hashBaseAug[ii]) XdbHashSlotAug();

        if (xdb == mainXdb_) {
            // main xdb is always sorted
            xdb->hashSlotInfo.hashBase[ii].sortFlag = true;
        }
    }

CommonExit:
    return status;
}

Status
HashTree::init(const char *pubTableName,
               Xid hashTreeId,
               TableMgr::ColumnInfoTable *columnsIn,
               unsigned numKeys,
               DfFieldAttrHeader *keyAttr,
               XcalarApiTableInput *srcTable)
{
    Status status = StatusOk;

    hashTreeId_ = hashTreeId;
    numKeys_ = numKeys;
    keys_ = new (std::nothrow) DfFieldAttrHeader[numKeys];
    BailIfNull(keys_);

    for (unsigned ii = 0; ii < numKeys; ii++) {
        keys_[ii] = keyAttr[ii];
    }

    sourceTable_ = *srcTable;
    hashSlots_ = XdbMgr::xdbHashSlots;
    keys_[0].ordering = PartialAscending;

    status = initSchema(pubTableName, columnsIn);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed HashTree::init %s %lu: %s",
                    pubTableName,
                    hashTreeId,
                    strGetFromStatus(status));

CommonExit:
    return status;
}

void
HashTree::updateSlotXdbMinMax(Xdb *slotXdb, XdbHashSlotAug *srcSlotAug)
{
    if (!slotXdb->keyRangeValid) {
        slotXdb->minKey = srcSlotAug->minKey;
        slotXdb->maxKey = srcSlotAug->maxKey;

        slotXdb->keyRangeValid = true;
    } else {
        DataFormat::updateFieldValueRange(keys_[0].type,
                                          srcSlotAug->minKey,
                                          &slotXdb->minKey,
                                          &slotXdb->maxKey,
                                          DontHashString,
                                          DfFieldValueRangeValid);

        DataFormat::updateFieldValueRange(keys_[0].type,
                                          srcSlotAug->maxKey,
                                          &slotXdb->minKey,
                                          &slotXdb->maxKey,
                                          DontHashString,
                                          DfFieldValueRangeValid);
    }

    slotXdb->hashDivForRangeBasedHash =
        XdbMgr::getHashDiv(keys_[0].type,
                           &slotXdb->minKey,
                           &slotXdb->maxKey,
                           slotXdb->keyRangeValid,
                           slotXdb->hashSlotInfo.hashSlots);
}

Status
HashTree::setupForInsert(Xdb *srcXdb)
{
    Status status = StatusOk;

    // make sure the srcXdb's keys match with what we expect
    assert(srcXdb->meta->numKeys == numKeys_);
    assert(strcmp(keys_[0].name, srcXdb->meta->keyAttr[0].name) == 0);

    if (srcXdb->meta->dhtId != XidMgr::XidSystemUnorderedDht) {
        status = XdbMgr::get()->rehashXdb(srcXdb, XdbInsertCrcHash);
        BailIfFailed(status);

        XdbMgr::get()->xdbUpdateCounters(srcXdb);
    }

    if (!rangeInited) {
        min_ = srcXdb->minKey;
        max_ = srcXdb->maxKey;
        rangeInited = true;
    } else {
        DataFormat::updateFieldValueRange(keys_[0].type,
                                          srcXdb->minKey,
                                          &min_,
                                          &max_,
                                          DontHashString,
                                          DfFieldValueRangeValid);

        DataFormat::updateFieldValueRange(keys_[0].type,
                                          srcXdb->maxKey,
                                          &min_,
                                          &max_,
                                          DontHashString,
                                          DfFieldValueRangeValid);
    }

    for (uint64_t ii = 0; ii < hashSlots_; ii++) {
        assert(ii < srcXdb->hashSlotInfo.hashSlots);

        XdbHashSlotAug *srcSlotAug = &srcXdb->hashSlotInfo.hashBaseAug[ii];

        if (srcSlotAug->keyRangeValid) {
            updateSlotXdbMinMax(updateXdb_, srcSlotAug);
        }
    }

CommonExit:
    return status;
}

Status
HashTree::insertXdb(Xdb *srcXdb,
                    XcalarApiTableInput *srcTable,
                    time_t unixTS,
                    size_t size,
                    size_t numRows,
                    int flags,
                    int64_t batchId)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    TrackHelpers *trackHelpers = NULL;
    unsigned numScheds = Operators::getNumWorkers(srcXdb->numRows);
    InsertWork **scheds = NULL;

    int64_t numChunks = srcXdb->hashSlotInfo.hashSlots;
    XdbMeta *srcMeta = srcXdb->meta;
    OpKvEntryCopyMapping *mapping = NULL;

    unsigned dstNumFields = updateXdb_->tupMeta.getNumFields();
    UpdateStats stats[numScheds];
    needsSizeRefresh_ = true;

    // range hash srcXdb and populate min/max info on all nodes
    status = setupForInsert(srcXdb);
    BailIfFailed(status);

    mapping = Operators::getOpKvEntryCopyMapping(dstNumFields);
    BailIfNull(mapping);

    Operators::initKvEntryCopyMapping(mapping,
                                      srcMeta,
                                      xdbMeta_,
                                      0,
                                      dstNumFields,
                                      false,
                                      NULL,
                                      0);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numScheds, numChunks);
    BailIfNull(trackHelpers);

    scheds = new (std::nothrow) InsertWork *[numScheds];
    BailIfNull(scheds);

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numScheds; ii++) {
        scheds[ii] = new (std::nothrow)
            InsertWork(ii == 0 ? TrackHelpers::Master : TrackHelpers::NonMaster,
                       ii,
                       trackHelpers,
                       srcXdb,
                       srcMeta,
                       flags,
                       mapping,
                       batchId,
                       &stats[ii],
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

    // only increment currentBatchId on success and for new updates
    {
        UpdateStats statsTotal;
        for (unsigned ii = 0; ii < numScheds; ii++) {
            statsTotal.numInserts += stats[ii].numInserts;
            statsTotal.numDeletes += stats[ii].numDeletes;
            statsTotal.numUpdates += stats[ii].numUpdates;
        }

        UpdateMeta *update = new (std::nothrow)
            UpdateMeta(srcTable, unixTS, batchId, size, numRows, &statsTotal);
        BailIfNull(update);

        pushUpdateMeta(update);
    }

CommonExit:
    if (mapping) {
        memFree(mapping);
        mapping = NULL;
    }

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
        trackHelpers->tearDown(&trackHelpers);
    }

    XdbMgr::xdbUpdateCounters(updateXdb_);

    return status;
}

void
InsertWork::run()
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

    for (uint64_t ii = 0; ii < trackHelpers_->getWorkUnitsTotal(); ii++) {
        if (!trackHelpers_->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            trackHelpers_->workUnitComplete(ii);
            continue;
        }

        status = processSlotForInsert(ii, ii);

        trackHelpers_->workUnitComplete(ii);
    }

CommonExit:
    // status is managed outside of trackHelpers
    trackHelpers_->helperDone(status);
}

Status
InsertWork::processSlotForInsert(unsigned srcSlotId, unsigned dstSlotId)
{
    Status status = StatusOk;
    XdbAtomicHashSlot *hashSlot = &xdb_->hashSlotInfo.hashBase[srcSlotId];
    XdbPage *xdbPage = XdbMgr::getXdbHashSlotNextPage(hashSlot);

    while (xdbPage) {
        status = insertOnePage(xdbPage, dstSlotId);
        BailIfFailed(status);

        xdbPage = (XdbPage *) xdbPage->hdr.nextPage;
    }

    if (flags_ & HashTree::DropSrcSlotsFlag) {
        XdbMgr::get()->xdbDropSlot(xdb_, srcSlotId, false);
    }

CommonExit:
    return status;
}

Status
InsertWork::insertOnePage(XdbPage *xdbPage, unsigned dstSlot)
{
    Status status = StatusOk;
    unsigned dstNumFields = hashTree_->updateXdb_->tupMeta.getNumFields();
    XdbMgr *xdbMgr = XdbMgr::get();
    NewKeyValueMeta *kvMeta = &xdbMeta_->kvNamedMeta.kvMeta_;

    NewKeyValueEntry kvEntry(kvMeta);
    NewKeyValueEntry dstKvEntry(&hashTree_->xdbMeta_->kvNamedMeta.kvMeta_);
    DfFieldValue key;
    bool isKeyValid = false;
    bool refGrabbed = false;
    DfFieldValue batchIdValue;
    batchIdValue.int64Val = batchId_;

    XdbAtomicHashSlot *hashSlot =
        &hashTree_->updateXdb_->hashSlotInfo.hashBase[dstSlot];
    XdbHashSlotAug *xdbHashSlotAug =
        &hashTree_->updateXdb_->hashSlotInfo.hashBaseAug[dstSlot];

    status = xdbPage->getRef(xdb_);
    if (status != StatusOk) {
        return status;
    }
    refGrabbed = true;

    NewTuplesCursor tupCursor(xdbPage->tupBuf);

    while ((status = tupCursor.getNext(kvMeta->tupMeta_, &kvEntry.tuple_)) ==
           StatusOk) {
        key = kvEntry.getKey(&isKeyValid);
        if (!isKeyValid) {
            // skip all FNF keys
            continue;
        }

        dstKvEntry.init();
        Operators::shallowCopyKvEntry(&dstKvEntry,
                                      dstNumFields,
                                      &kvEntry,
                                      mapping_,
                                      0,
                                      mapping_->numEntries);

        if (batchId_ != HashTree::InvalidBatchId) {
            dstKvEntry.tuple_.set(hashTree_->batchIdx_, batchIdValue, DfInt64);
        }

        if (flags_ & HashTree::AddOpCodeFlag) {
            dstKvEntry.tuple_.set(hashTree_->opCodeIdx_, InsertOpCode, DfInt64);
            stats_->numInserts++;
        } else {
            bool retIsValid;
            DfFieldValue op = dstKvEntry.tuple_.get(hashTree_->opCodeIdx_,
                                                    dstNumFields,
                                                    DfInt64,
                                                    &retIsValid);
            (void) op;

            if (unlikely(!retIsValid)) {
                status = StatusInvalidXcalarOpCode;
                goto CommonExit;
            } else {
                if (op.int64Val == InsertOpCode.int64Val) {
                    stats_->numInserts++;
                } else if (op.int64Val == DeleteOpCode.int64Val) {
                    stats_->numDeletes++;
                } else {
                    stats_->numUpdates++;
                }
            }
        }

        if (flags_ & HashTree::AddRankOverFlag) {
            dstKvEntry.tuple_.set(hashTree_->rankOverIdx_,
                                  InitialRank,
                                  DfInt64);
        } else {
            bool retIsValid;
            DfFieldValue rank = dstKvEntry.tuple_.get(hashTree_->rankOverIdx_,
                                                      dstNumFields,
                                                      DfInt64,
                                                      &retIsValid);
            (void) rank;

            if (unlikely(!retIsValid)) {
                status = StatusInvalidXcalarRankOver;
                goto CommonExit;
            }
        }

        status = xdbMgr->xdbInsertKvCommon(hashTree_->updateXdb_,
                                           &key,
                                           &dstKvEntry.tuple_,
                                           hashSlot,
                                           xdbHashSlotAug,
                                           dstSlot);
        BailIfFailed(status);

        if (unlikely(hashSlot->sortFlag)) {
            // we've made this slot unsorted again
            hashSlot->sortFlag = false;
        }
    }

    assert(status == StatusNoData);
    status = StatusOk;

CommonExit:
    if (refGrabbed) {
        XdbMgr::get()->pagePutRef(xdb_, 0, xdbPage);
        refGrabbed = false;
    }
    return status;
}

void
HashTree::cleanOutBatchId(int64_t batchIdToClean)
{
    Status status = StatusOk;
    NewKeyValueMeta *kvMeta = &xdbMeta_->kvNamedMeta.kvMeta_;
    NewKeyValueEntry kvEntry(kvMeta);
    unsigned numFields = kvMeta->tupMeta_->getNumFields();

    // only works on the latest update
    assert(currentBatchId_ == batchIdToClean ||
           currentBatchId_ + 1 == batchIdToClean);

    for (uint64_t slot = 0; slot < hashSlots_; slot++) {
        XdbAtomicHashSlot *hashSlot = &updateXdb_->hashSlotInfo.hashBase[slot];
        XdbHashSlotAug *hashSlotAug =
            &updateXdb_->hashSlotInfo.hashBaseAug[slot];
        XdbPage *xdbPage = XdbMgr::getXdbHashSlotNextPage(hashSlot);
        XdbPage *prevXdbPage = NULL;

        while (xdbPage) {
            // get batchId of first row in the page
            NewTuplesCursor tupCursor((NewTuplesBuffer *) xdbPage->tupBuf);
            status = tupCursor.getNext(kvMeta->tupMeta_, &kvEntry.tuple_);
            assert(status == StatusOk);

            bool retIsValid;
            DfFieldValue batchId =
                kvEntry.tuple_.get(batchIdx_, numFields, DfInt64, &retIsValid);
            assert(retIsValid);

            XdbPage *nextXdbPage = (XdbPage *) xdbPage->hdr.nextPage;
            prevXdbPage = xdbPage;
            assert(batchId.int64Val <= batchIdToClean);

            if (batchId.int64Val == batchIdToClean) {
                // because batchIds are monotonically increasing, the
                // remaining rows also have the currentBatchId. Clean
                // out the whole page
                hashSlotAug->numPages--;
                hashSlotAug->numRows -= xdbPage->hdr.numRows;

                XdbMgr::get()->xdbFreeXdbPage(xdbPage);
                prevXdbPage->hdr.nextPage = nextXdbPage;
                if (nextXdbPage) {
                    nextXdbPage->hdr.prevPage = prevXdbPage;
                } else {
                    // this was the only page in the slot
                    XdbMgr::clearSlot(hashSlot);
                }
            } else {
                assert(batchId.int64Val < batchIdToClean);
                // We've found the first page in the chain that contains
                // previous batch data. Clean up any lingering data from
                // the current batch from the end of the page

                unsigned count = 1;
                while ((status = tupCursor.getNext(kvMeta->tupMeta_,
                                                   &kvEntry.tuple_)) ==
                       StatusOk) {
                    bool retIsValid;
                    DfFieldValue batchId = kvEntry.tuple_.get(batchIdx_,
                                                              numFields,
                                                              DfInt64,
                                                              &retIsValid);
                    assert(retIsValid);

                    if (batchId.int64Val == batchIdToClean) {
                        break;
                    }

                    count++;
                }

                unsigned numToRemove = xdbPage->tupBuf->getNumTuples() - count;

                xdbPage->hdr.numRows -= numToRemove;
                hashSlotAug->numRows -= numToRemove;

                xdbPage->tupBuf->removeFromEnd(kvMeta->tupMeta_, numToRemove);
            }
            xdbPage = nextXdbPage;
        }
    }
}

void
HashTree::pushUpdateMeta(UpdateMeta *update)
{
    update->next = updateHead_;
    updateHead_ = update;

    numUpdates_++;
    currentBatchId_ = update->batchId;
}

HashTree::UpdateMeta *
HashTree::popUpdateMeta()
{
    UpdateMeta *update = updateHead_;
    if (!update) {
        return NULL;
    }

    updateHead_ = update->next;
    numUpdates_--;

    if (updateHead_) {
        assert(updateHead_->batchId + 1 == currentBatchId_);
        currentBatchId_ = updateHead_->batchId;
    } else {
        currentBatchId_ = HashTree::InvalidBatchId;
    }

    return update;
}

void
HashTree::fixupXdbSlotAugs()
{
    for (unsigned ii = 0; ii < 2; ii++) {
        Xdb *xdb;
        size_t totalRows = 0;

        if (ii == 0) {
            xdb = mainXdb_;
        } else {
            xdb = updateXdb_;
        }

        for (uint64_t jj = 0; jj < xdb->hashSlotInfo.hashSlots; ++jj) {
            XdbAtomicHashSlot *hashSlot = &xdb->hashSlotInfo.hashBase[jj];
            XdbHashSlotAug *slotAug = &xdb->hashSlotInfo.hashBaseAug[jj];
            size_t rows = 0, pages = 0;

            XdbPage *page = XdbMgr::get()->getXdbHashSlotNextPage(hashSlot);

            while (page != NULL) {
                rows += page->hdr.numRows;
                pages++;

                page = page->hdr.nextPage;
            }

            slotAug->numRows = rows;
            slotAug->numPages = pages;
            slotAug->startRecord = totalRows;

            totalRows += rows;
        }
    }
}

void
HashTree::computeLocalSize(size_t *sizeOut, size_t *rowsOut)
{
    *sizeOut = mainXdb_->numPages * XdbMgr::bcSize() +
               updateXdb_->numPages * XdbMgr::bcSize();

    *rowsOut = mainXdb_->numRows + updateXdb_->numRows;
}
