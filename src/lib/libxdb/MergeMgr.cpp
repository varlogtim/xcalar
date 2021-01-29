// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "xdb/Merge.h"
#include "xdb/MergeGvm.h"
#include "xdb/XdbInt.h"
#include "xdb/DataModelTypes.h"
#include "msg/Xid.h"
#include "strings/String.h"
#include "table/Table.h"

static constexpr const char *moduleName = "mergeMgr";

MergeMgr *MergeMgr::instance = NULL;

MergeMgr *
MergeMgr::get()
{
    return instance;
}

Status
MergeMgr::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) MergeMgr();
    if (instance == NULL) {
        return StatusNoMem;
    }
    return instance->initInternal();
}

Status
MergeMgr::initInternal()
{
    StatsLib *statsLib = StatsLib::get();
    Status status;

    status = statsLib->initNewStatGroup("merge", &statsGrpId_, StatsCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.init);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "init",
                                         stats_.init,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.initFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "initFailure",
                                         stats_.initFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.initLocal);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "initLocal",
                                         stats_.initLocal,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.initLocalFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "initLocalFailure",
                                         stats_.initLocalFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.prepare);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "prepare",
                                         stats_.prepare,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.prepareFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "prepareFailure",
                                         stats_.prepareFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.prepareLocal);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "prepareLocal",
                                         stats_.prepareLocal,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.prepareLocalFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "prepareLocalFailure",
                                         stats_.prepareLocalFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.commit);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "commit",
                                         stats_.commit,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.commitFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "commitFailure",
                                         stats_.commitFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.postCommit);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "postCommit",
                                         stats_.postCommit,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.postCommitFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "postCommitFailure",
                                         stats_.postCommitFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.postCommitLocal);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "postCommitLocal",
                                         stats_.postCommitLocal,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.postCommitLocalFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "postCommitLocalFailure",
                                         stats_.postCommitLocalFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.abort);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "abort",
                                         stats_.abort,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.abortFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "abortFailure",
                                         stats_.abortFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.abortLocal);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "abortLocal",
                                         stats_.abortLocal,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&stats_.abortLocalFailure);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "abortLocalFailure",
                                         stats_.abortLocalFailure,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = MergeGvm::init();
    BailIfFailed(status);

CommonExit:
    return status;
}

void
MergeMgr::destroy()
{
    MergeGvm::get()->destroy();

    delete instance;
    instance = NULL;
}

Status
MergeMgr::MergeInfo::init(Xdb *deltaXdb, Xdb *targetXdb)
{
    Status status;
    xdbMeta_ = setupXdbMeta(deltaXdb->meta, targetXdb->meta);
    BailIfNullMsg(xdbMeta_,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::init deltaXdb %ld targetXdb %ld: %s",
                  deltaXdb->xdbId,
                  targetXdb->xdbId,
                  strGetFromStatus(status));

    imdPendingXdb_ = new (std::nothrow) Xdb(XidInvalid, XdbLocal);
    BailIfNullMsg(imdPendingXdb_,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::init deltaXdb %ld targetXdb %ld: %s",
                  deltaXdb->xdbId,
                  targetXdb->xdbId,
                  strGetFromStatus(status));

    imdCommittedXdb_ = new (std::nothrow) Xdb(XidInvalid, XdbLocal);
    BailIfNullMsg(imdCommittedXdb_,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::init deltaXdb %ld targetXdb %ld: %s",
                  deltaXdb->xdbId,
                  targetXdb->xdbId,
                  strGetFromStatus(status));

    status = initXdb(imdPendingXdb_, deltaXdb->meta, targetXdb->meta);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed MergeInfo::init deltaXdb %ld targetXdb %ld: %s",
                    deltaXdb->xdbId,
                    targetXdb->xdbId,
                    strGetFromStatus(status));

    status = initXdb(imdCommittedXdb_, deltaXdb->meta, targetXdb->meta);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed MergeInfo::init deltaXdb %ld targetXdb %ld: %s",
                    deltaXdb->xdbId,
                    targetXdb->xdbId,
                    strGetFromStatus(status));

    targetXdb_ = targetXdb;

CommonExit:
    return status;
}

Status
MergeMgr::MergeInfo::initXdb(Xdb *xdb,
                             XdbMeta *deltaXdbMeta,
                             XdbMeta *targetXdbMeta)
{
    Status status;

    xdb->meta = xdbMeta_;
    xdb->tupMeta = *xdbMeta_->kvNamedMeta.kvMeta_.tupMeta_;
    xdb->hashSlotInfo.hashSlots = XdbMgr::xdbHashSlots;
    xdb->hashSlotInfo.hashBase =
        (XdbAtomicHashSlot *) memAllocAlignedExt(XdbMgr::XdbMinPageAlignment,
                                                 sizeof(XdbAtomicHashSlot) *
                                                     XdbMgr::xdbHashSlots,
                                                 moduleName);
    BailIfNullMsg(xdb->hashSlotInfo.hashBase,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::initXdb deltaXdb %ld targetXdb %ld: %s",
                  deltaXdbMeta->xdbId,
                  targetXdbMeta->xdbId,
                  strGetFromStatus(status));

    memZero(xdb->hashSlotInfo.hashBase,
            sizeof(XdbAtomicHashSlot) * xdb->hashSlotInfo.hashSlots);

    xdb->hashSlotInfo.hashBaseAug =
        (XdbHashSlotAug *) memAllocAlignedExt(XdbMgr::XdbMinPageAlignment,
                                              sizeof(XdbHashSlotAug) *
                                                  xdb->hashSlotInfo.hashSlots,
                                              moduleName);
    BailIfNullMsg(xdb->hashSlotInfo.hashBaseAug,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::initXdb deltaXdb %ld targetXdb %ld: %s",
                  deltaXdbMeta->xdbId,
                  targetXdbMeta->xdbId,
                  strGetFromStatus(status));

    for (uint64_t ii = 0; ii < xdb->hashSlotInfo.hashSlots; ii++) {
        new (&xdb->hashSlotInfo.hashBaseAug[ii]) XdbHashSlotAug();
    }

    xdb->keyRangePreset = false;

CommonExit:
    return status;
}

XdbMeta *
MergeMgr::MergeInfo::setupXdbMeta(XdbMeta *deltaXdbMeta, XdbMeta *targetXdbMeta)
{
    Status status;
    XdbMeta *xdbMeta = NULL;
    XdbId xdbId = XidMgr::get()->xidGetNext();
    TableMgr::ColumnInfoTable deltaColInfoTable;
    TableMgr::ColumnInfo *rankCol = NULL;

    // Accomodate XcalarBatchIdColumnName into Schema. This is needed to
    // preserve ordering of IMDs updates accross different versions.
    assert(deltaXdbMeta->numImmediates + 1 <= TupleMaxNumValuesPerRecord);
    const char *immediateNames[deltaXdbMeta->numImmediates + 1];

    size_t numFields =
        deltaXdbMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields() + 1;
    assert(numFields <= TupleMaxNumValuesPerRecord);

    // Accomodate XcalarBatchIdColumnName and XcalarRankOverColumnName
    // column as keys.
    unsigned numKeys = deltaXdbMeta->numKeys + 2;
    DfFieldAttrHeader *keys = NULL;
    assert(numKeys <= TupleMaxNumValuesPerRecord);

    status = TableMgr::createColumnInfoTable(&deltaXdbMeta->kvNamedMeta,
                                             &deltaColInfoTable);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed MergeInfo::setupXdbMeta deltaXdb %ld targetXdb "
                    "%ld: "
                    "%s",
                    deltaXdbMeta->xdbId,
                    targetXdbMeta->xdbId,
                    strGetFromStatus(status));

    rankCol = deltaColInfoTable.find(XcalarRankOverColumnName);
    assert(rankCol != NULL &&
           "XcalarRankOverColumnName must be part of schema");

    keys = new (std::nothrow) DfFieldAttrHeader[numKeys];
    BailIfNullMsg(keys,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::setupXdbMeta deltaXdb %ld targetXdb %ld: "
                  "%s",
                  deltaXdbMeta->xdbId,
                  targetXdbMeta->xdbId,
                  strGetFromStatus(status));

    for (unsigned ii = 0; ii < numKeys - 2; ii++) {
        keys[ii] = deltaXdbMeta->keyAttr[ii];
    }

    // Add XcalarRankOverColumnName to keys
    verifyOk(strStrlcpy(keys[numKeys - 2].name,
                        XcalarRankOverColumnName,
                        sizeof(keys[numKeys - 2].name)));
    keys[numKeys - 2].ordering = Unordered;
    keys[numKeys - 2].type = DfInt64;
    keys[numKeys - 2].valueArrayIndex = rankCol->getIdxInTable();

    // Add XcalarBatchIdColumnName to keys
    verifyOk(strStrlcpy(keys[numKeys - 1].name,
                        XcalarBatchIdColumnName,
                        sizeof(keys[numKeys - 1].name)));
    keys[numKeys - 1].ordering = Unordered;
    keys[numKeys - 1].type = DfInt64;
    keys[numKeys - 1].valueArrayIndex = numFields - 1;

    // Accomodate XcalarBatchIdColumnName into Schema
    memcpy(&tupMeta_,
           deltaXdbMeta->kvNamedMeta.kvMeta_.tupMeta_,
           sizeof(NewTupleMeta));
    tupMeta_.setNumFields(numFields);
    tupMeta_.setFieldType(DfInt64, numFields - 1);

    for (unsigned ii = 0; ii < deltaXdbMeta->numImmediates; ii++) {
        immediateNames[ii] = deltaXdbMeta->kvNamedMeta.valueNames_[ii];
    }
    immediateNames[deltaXdbMeta->numImmediates] = XcalarBatchIdColumnName;

    xdbMeta = XdbMgr::get()->xdbAllocMeta(xdbId,
                                          XidMgr::XidSystemUnorderedDht,
                                          NULL,
                                          numKeys,
                                          keys,
                                          0,
                                          NULL,
                                          tupMeta_.getNumFields(),
                                          immediateNames,
                                          0,
                                          NULL,
                                          &tupMeta_);
    BailIfNullMsg(xdbMeta,
                  StatusNoMem,
                  moduleName,
                  "Failed MergeInfo::setupXdbMeta deltaXdb %ld targetXdb %ld: "
                  "%s",
                  deltaXdbMeta->xdbId,
                  targetXdbMeta->xdbId,
                  strGetFromStatus(status));

    // Skip rehash
    xdbMeta->prehashed = true;

CommonExit:
    if (keys != NULL) {
        delete[] keys;
    }
    TableMgr::destroyColumnInfoTable(&deltaColInfoTable);
    return xdbMeta;
}

Xdb *
MergeMgr::MergeInfo::getImdPendingXdb()
{
    return imdPendingXdb_;
}

Xdb *
MergeMgr::MergeInfo::getImdCommittedXdb()
{
    return imdCommittedXdb_;
}

Xdb *
MergeMgr::MergeInfo::getTargetXdb()
{
    return targetXdb_;
}

XdbMeta *
MergeMgr::MergeInfo::getImdPendingXdbMeta()
{
    return imdPendingXdb_->meta;
}

XdbMeta *
MergeMgr::MergeInfo::getImdCommittedXdbMeta()
{
    return imdCommittedXdb_->meta;
}

XdbMeta *
MergeMgr::MergeInfo::getTargetXdbMeta()
{
    return targetXdb_->meta;
}

VersionId
MergeMgr::MergeInfo::getImdPendingVersionNum()
{
    return imdPendingVersionNum_;
}

void
MergeMgr::MergeInfo::setImdPendingVersionNum(VersionId versionNum)
{
    imdPendingVersionNum_ = versionNum;
}

void
MergeMgr::MergeInfo::resetImdXdbForLoadDone(Xdb *imdXdb)
{
    imdXdb->loadDone = false;
    imdXdb->keyRangePreset = false;
}

void
MergeMgr::MergeInfo::setupPostMerge()
{
    imdPostMergeLock_.lock();
    atomicWrite64(&imdSlotsProcessedCounter_, 0);
    atomicWrite64(&imdPendingMergeRowCount_, 0);
}

void
MergeMgr::MergeInfo::teardownPostMerge(Status mergeStatus)
{
    if (mergeStatus == StatusOk) {
        fixupTargetXdbSlotAugs();
    }
    atomicWrite64(&imdSlotsProcessedCounter_, 0);
    atomicWrite64(&imdPendingMergeRowCount_, 0);
    imdPostMergeLock_.unlock();
}

void
MergeMgr::MergeInfo::incImdSlotsProcessedCounter()
{
    atomicInc64(&imdSlotsProcessedCounter_);
}

int64_t
MergeMgr::MergeInfo::getImdSlotsProcessedCounter()
{
    return atomicRead64(&imdSlotsProcessedCounter_);
}

void
MergeMgr::MergeInfo::updateImdPendingMergeRowCount(int64_t rowCount)
{
    atomicAdd64(&imdPendingMergeRowCount_, rowCount);
}

int64_t
MergeMgr::MergeInfo::getImdPendingMergeRowCount()
{
    return atomicRead64(&imdPendingMergeRowCount_);
}
void
MergeMgr::MergeInfo::fixupTargetXdbSlotAugs()
{
    uint64_t totalRows = 0;
    uint64_t totalPages = 0;
    XdbMgr *xdbMgr = XdbMgr::get();

    targetXdb_->lock.lock();
    for (uint64_t jj = 0; jj < targetXdb_->hashSlotInfo.hashSlots; ++jj) {
        XdbHashSlotAug *slotAug = &targetXdb_->hashSlotInfo.hashBaseAug[jj];
#ifdef DEBUG
        xdbMgr->lockSlot(&targetXdb_->hashSlotInfo, jj);
        XdbAtomicHashSlot *hashSlot = &targetXdb_->hashSlotInfo.hashBase[jj];
        uint64_t rows = 0, pages = 0;
        XdbPage *page = xdbMgr->getXdbHashSlotNextPage(hashSlot);
        while (page != NULL) {
            rows += page->hdr.numRows;
            pages++;
            page = page->hdr.nextPage;
        }
        assert(slotAug->numRows == rows);
        assert(slotAug->numPages == pages);
        xdbMgr->unlockSlot(&targetXdb_->hashSlotInfo, jj);
#endif  // DEBUG
        slotAug->startRecord = totalRows;
        totalRows += slotAug->numRows;
        totalPages += slotAug->numPages;
    }
    targetXdb_->numRows = totalRows;
    targetXdb_->numPages = totalPages;
    targetXdb_->densityState = XdbDensityNeedUpdate;
    targetXdb_->bytesAllocated = 0;
    targetXdb_->bytesConsumed = 0;
    targetXdb_->lock.unlock();
}

void
MergeMgr::MergeInfo::freeXdb(Xdb *fXdb)
{
    XdbMgr::get()->freeXdbPages(fXdb);

    memAlignedFree(fXdb->hashSlotInfo.hashBase);
    memAlignedFree(fXdb->hashSlotInfo.hashBaseAug);

    if (fXdb->pageHdrBackingPage != NULL) {
        XdbMgr::get()->xdbPutXdbPageHdr(fXdb->pageHdrBackingPage);
        fXdb->pageHdrBackingPage = NULL;
    }

    delete fXdb;
}

MergeMgr::MergeInfo::~MergeInfo()
{
    if (imdPendingXdb_) {
        freeXdb(imdPendingXdb_);
        imdPendingXdb_ = NULL;
    }

    if (imdCommittedXdb_) {
        freeXdb(imdCommittedXdb_);
        imdCommittedXdb_ = NULL;
    }

    if (xdbMeta_) {
        XdbMgr::freeXdbMeta(xdbMeta_);
        xdbMeta_ = NULL;
    }
}
