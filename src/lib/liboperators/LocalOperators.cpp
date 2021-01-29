// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <cstdlib>
#include <sys/resource.h>
#include <math.h>
#include <algorithm>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisRecv.h"
#include "operators/OperatorsTypes.h"
#include "operators/GenericTypes.h"
#include "msg/Message.h"
#include "df/DataFormatTypes.h"
#include "operators/OperatorsHash.h"
#include "operators/Operators.h"
#include "operators/OperatorsEvalTypes.h"
#include "util/AtomicTypes.h"
#include "df/DataFormat.h"
#include "dataset/Dataset.h"
#include "xdb/TableTypes.h"
#include "scalars/Scalars.h"
#include "operators/Xdf.h"
#include "operators/XcalarEval.h"
#include "XcalarEvalInt.h"
#include "bc/BufferCache.h"
#include "xdb/Xdb.h"
#include "operators/OperatorsXdbPageOps.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "operators/OperatorsApiWrappers.h"
#include "parent/Parent.h"
#include "ParentChild.h"
#include "util/TrackHelpers.h"
#include "runtime/Spinlock.h"
#include "udf/UserDefinedFunction.h"
#include "runtime/Runtime.h"
#include "runtime/Semaphore.h"
#include "msg/Xid.h"
#include "newtupbuf/NewTupleTypes.h"
#include "newtupbuf/NewTuplesBuffer.h"
#include "newtupbuf/NewTuplesCursor.h"
#include "transport/TransportPage.h"
#include "runtime/Schedulable.h"
#include "childpool/ChildPool.h"
#include "demystify/Demystify.h"
#include "child/Child.h"
#include "bc/BufCacheMemMgr.h"
#include "newtupbuf/NewTuplesCursor.h"
#include "datapage/DataPageIndex.h"
#include "xdb/HashTree.h"
#include "OperatorsGvm.h"

static constexpr const char *moduleName = "liboperators";

bool Operators::operatorsInited = false;

StatGroupId Operators::statsGrpId;
StatHandle Operators::indexDsHandle;
StatHandle Operators::operationsSucceeded;
StatHandle Operators::operationsFailed;
StatHandle Operators::operationsOutstanding;
StatHandle Operators::indexSetKeyTypeQueued;
StatHandle Operators::indexSetKeyTypeAborted;

unsigned
Operators::getNumWorkers(uint64_t rows)
{
    uint64_t threads = (uint64_t) Runtime::get()->getThreadsCount(
        Txn::currentTxn().rtSchedId_);

    if (rows < OpMinRowCountMult) {
        return 1;
    } else {
        return xcMin(rows / OpMinRowCountMult, threads);
    }
}

Status
Operators::getTempScalar(Scalar **scalarOut,
                         size_t bufSize,
                         MemoryPile **scalarPileIn)
{
    Scalar *scalar = NULL;
    Status status = StatusUnknown;
    size_t size = sizeof(*scalar) + bufSize;

    scalar = (Scalar *) MemoryPile::getElem(scalarPileIn, size, &status);
    if (status != StatusOk) {
        assert(scalar == NULL);
        return status;
    }

    scalar->fieldUsedSize = 0;
    scalar->fieldAllocedSize = bufSize;
    *scalarOut = scalar;

    return status;
}

void
Operators::putTempScalar(Scalar *scalar)
{
    MemoryPile::putElem(scalar, XdbMgr::bcSize());
}

Status
Operators::getRowMeta(OpRowMeta **rowMetaOut,
                      uint64_t rowNum,
                      MemoryPile **rowMetaPileIn,
                      size_t rowMetaSize,
                      Xdb *srcXdb,
                      TableCursor *cur,
                      unsigned numVars)
{
    Status status = StatusUnknown;
    assert(numVars <= TupleMaxNumValuesPerRecord);
    OpRowMeta *rowMeta = NULL;

    rowMeta = (OpRowMeta *) MemoryPile::getElem(rowMetaPileIn,
                                                (*rowMetaPileIn)->getSize(),
                                                &status);
    if (unlikely(status != StatusOk)) {
        assert(rowMeta == NULL);
        goto CommonExit;
    }
    new (rowMeta) OpRowMeta;

    for (unsigned ii = 0; ii < numVars; ii++) {
        rowMeta->scalars[ii] = NULL;
    }

    atomicWrite32(&rowMeta->numIssued, 0);
    status = cur->duplicate(&rowMeta->opCursor);
    BailIfFailed(status);

    rowMeta->rowNum = rowNum;

    *rowMetaOut = rowMeta;

CommonExit:
    return status;
}

void
Operators::putRowMeta(OpRowMeta *rowMeta, unsigned numScalars)
{
    for (unsigned ii = 0; ii < numScalars; ii++) {
        if (rowMeta->scalars[ii] != NULL) {
            putTempScalar(rowMeta->scalars[ii]);
        }
    }

    rowMeta->~OpRowMeta();
    MemoryPile::putElem(rowMeta, XdbMgr::bcSize());
}

Status
Operators::getGroupByRowMeta(OpGroupByRowMeta **rowMetaOut,
                             MemoryPile **rowMetaPileIn,
                             size_t rowMetaSize,
                             unsigned numVars,
                             OpGroupMeta *groupMeta)
{
    Status status = StatusUnknown;
    assert(numVars <= TupleMaxNumValuesPerRecord);
    OpGroupByRowMeta *rowMeta = NULL;

    rowMeta =
        (OpGroupByRowMeta *) MemoryPile::getElem(rowMetaPileIn,
                                                 (*rowMetaPileIn)->getSize(),
                                                 &status);
    if (unlikely(status != StatusOk)) {
        assert(rowMeta == NULL);
        return status;
    }

    for (unsigned ii = 0; ii < numVars; ii++) {
        rowMeta->scalars[ii] = NULL;
    }

    atomicWrite32(&rowMeta->numIssued, 0);
    rowMeta->groupMeta = groupMeta;

    *rowMetaOut = rowMeta;

    return status;
}

void
Operators::putGroupByRowMeta(OpGroupByRowMeta *rowMeta, unsigned numScalars)
{
    for (unsigned ii = 0; ii < numScalars; ii++) {
        if (rowMeta->scalars[ii] != NULL) {
            putTempScalar(rowMeta->scalars[ii]);
        }
    }
    MemoryPile::putElem(rowMeta, XdbMgr::bcSize());
}

Status
Operators::getScalarTableRowMeta(OpScalarTableRowMeta **rowMetaOut,
                                 DfFieldValue key,
                                 bool keyValid,
                                 MemoryPile **rowMetaPileIn,
                                 size_t rowMetaSize,
                                 unsigned numVars)
{
    Status status = StatusUnknown;
    assert(numVars <= TupleMaxNumValuesPerRecord);
    OpScalarTableRowMeta *rowMeta = NULL;

    rowMeta = (OpScalarTableRowMeta *) MemoryPile::getElem(rowMetaPileIn,
                                                           rowMetaSize,
                                                           &status);
    if (unlikely(status != StatusOk)) {
        assert(rowMeta == NULL);
        return status;
    }

    for (unsigned ii = 0; ii < numVars; ii++) {
        rowMeta->scalars[ii] = NULL;
    }

    atomicWrite32(&rowMeta->numIssued, 0);
    rowMeta->key = key;
    rowMeta->keyValid = keyValid,

    *rowMetaOut = rowMeta;

    return status;
}

void
Operators::putScalarTableRowMeta(OpScalarTableRowMeta *rowMeta,
                                 unsigned numScalars)
{
    for (unsigned ii = 0; ii < numScalars; ii++) {
        if (rowMeta->scalars[ii] != NULL) {
            putTempScalar(rowMeta->scalars[ii]);
        }
    }

    MemoryPile::putElem(rowMeta, XdbMgr::bcSize());
}

Status
Operators::getGroupMeta(OpGroupMeta **groupMetaOut,
                        MemoryPile **groupMetaPileIn,
                        Xdb *srcXdb,
                        Xdb *dstXdb,
                        TableCursor *cur,
                        DfFieldValue key,
                        uint64_t numRows)
{
    Status status = StatusUnknown;
    OpGroupMeta *groupMeta = NULL;

    groupMeta = (OpGroupMeta *)
        MemoryPile::getElem(groupMetaPileIn,
                            roundUp(sizeof(OpGroupMeta),
                                    XdbMgr::XdbMinPageAlignment),
                            &status);
    if (unlikely(status != StatusOk)) {
        assert(groupMeta == NULL);
        goto CommonExit;
    }
    new (groupMeta) OpGroupMeta;

    groupMeta->firstPage = NULL;
    groupMeta->numRowsMissing = numRows;
    status = cur->duplicate(&groupMeta->opCursor);
    BailIfFailed(status);
    groupMeta->key = key;
    *groupMetaOut = groupMeta;

CommonExit:
    return status;
}

void
Operators::putGroupMeta(OpGroupMeta *groupMeta)
{
    assert(groupMeta->numRowsMissing == 0);
    XdbPage *page = groupMeta->firstPage;
    XdbPage *tmpPage;
    XdbMgr *xdbMgr = XdbMgr::get();
    while (page) {
        tmpPage = page;
        page = (XdbPage *) page->hdr.nextPage;

        // passing in NULL here for the xdb pointer is ok since these pages
        // can never be serialized
        xdbMgr->xdbFreeXdbPage(tmpPage);
        tmpPage = NULL;
    }
    groupMeta->firstPage = NULL;

    groupMeta->~OpGroupMeta();
    MemoryPile::putElem(groupMeta, XdbMgr::bcSize());
}

Status  // static
Operators::operatorsInit(InitLevel initLevel)
{
    Status status = StatusUnknown;
    StatsLib *statsLib = StatsLib::get();

    int logicalProcCount = (unsigned) XcSysHelper::get()->getNumOnlineCores();
    assert(logicalProcCount > 0);

    status =
        statsLib->initNewStatGroup("liboperators", &statsGrpId, StatsCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&indexDsHandle);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "indexDs",
                                         indexDsHandle,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&operationsSucceeded);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "OperationsSucceeded",
                                         operationsSucceeded,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&operationsFailed);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "OperationsFailed",
                                         operationsFailed,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&operationsOutstanding);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "OperationsOutstanding",
                                         operationsOutstanding,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&indexSetKeyTypeQueued);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "indexSetKeyTypeQueued",
                                         indexSetKeyTypeQueued,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&indexSetKeyTypeAborted);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId,
                                         "indexSetKeyTypeAborted",
                                         indexSetKeyTypeAborted,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    // register recv handlers for demystification
    demystifyRecvHandlers[DemystifyMgr::Op::FilterAndMap] =
        new (std::nothrow) FilterAndMapDemystifyRecv();
    BailIfNull(demystifyRecvHandlers[DemystifyMgr::Op::FilterAndMap]);

    demystifyRecvHandlers[DemystifyMgr::Op::GroupBy] =
        new (std::nothrow) GroupByDemystifyRecv();
    BailIfNull(demystifyRecvHandlers[DemystifyMgr::Op::GroupBy]);

    demystifyRecvHandlers[DemystifyMgr::Op::CreateScalarTable] =
        new (std::nothrow) CreateScalarTableDemystifyRecv();
    BailIfNull(demystifyRecvHandlers[DemystifyMgr::Op::CreateScalarTable]);

    demystifyRecvHandlers[DemystifyMgr::Op::Index] =
        new (std::nothrow) IndexDemystifyRecv();
    BailIfNull(demystifyRecvHandlers[DemystifyMgr::Op::Index]);

    status = Operators::init();
    BailIfFailed(status);

    status = OperatorsGvm::init();
    BailIfFailed(status);

    operatorsInited = true;
    status = StatusOk;

CommonExit:
    return status;
}

void
Operators::operatorsDestroy()
{
    if (!operatorsInited) {
        return;
    }

    Operators::get()->destroy();
    operatorsInited = false;

    delete demystifyRecvHandlers[DemystifyMgr::Op::ResultSet];
    demystifyRecvHandlers[DemystifyMgr::Op::ResultSet] = NULL;
    delete demystifyRecvHandlers[DemystifyMgr::Op::FilterAndMap];
    demystifyRecvHandlers[DemystifyMgr::Op::FilterAndMap] = NULL;
    delete demystifyRecvHandlers[DemystifyMgr::Op::GroupBy];
    demystifyRecvHandlers[DemystifyMgr::Op::GroupBy] = NULL;
    delete demystifyRecvHandlers[DemystifyMgr::Op::CreateScalarTable];
    demystifyRecvHandlers[DemystifyMgr::Op::CreateScalarTable] = NULL;
    delete demystifyRecvHandlers[DemystifyMgr::Op::Index];
    demystifyRecvHandlers[DemystifyMgr::Op::Index] = NULL;
}

void
Operators::freeChildShm(OpChildShmContext *context)
{
    XdbMgr *xdbMgr = XdbMgr::get();
    if (context->input != NULL) {
        xdbMgr->bcFree(context->input);
        context->input = NULL;
    }

    if (context->output != NULL) {
        xdbMgr->bcFree(context->output);
        context->output = NULL;
    }
}

// Prepare to do eval in childnode. Should not be called unless there is actual
// work to do (e.g. don't hold onto these if not needed).
// XXX These are held onto longer than strictly needed.
Status
Operators::initChildShm(OpMeta *opMeta,
                        unsigned numVariables,
                        XdbId srcXdbId,
                        unsigned numEvals,
                        char **evalStrings,
                        XcalarEvalClass1Ast *asts,
                        OpChildShmContext *context)
{
    Status status = StatusOk;
    uint8_t *cursor, *cursorStart;
    OpChildShmInput *input;
    OpChildShmOutput *output;
    NewTupleMeta *tupleMeta;
    NewTuplesBuffer *tupBuf;
    NewTuplesCursor *tupCursor;
    XdbMgr *xdbMgr = XdbMgr::get();

    // Set default values.
    memZero(context, sizeof(*context));

    // Grab 2 SHM regions (input and output) from xdb page bc
    context->input =
        (OpChildShmInput *) xdbMgr->bcAlloc(Txn::currentTxn().id_,
                                            &status,
                                            XdbMgr::SlabHint::Default,
                                            BcHandle::BcScanCleanoutToFree);
    BailIfNullWith(context->input, status);

    context->output =
        (OpChildShmOutput *) xdbMgr->bcAlloc(Txn::currentTxn().id_,
                                             &status,
                                             XdbMgr::SlabHint::Default,
                                             BcHandle::BcScanCleanoutToFree);
    BailIfNullWith(context->output, status);

    // This Tuple Buffer will store temporary scalars allocated in rowMeta.
    // Key is a rowMeta pointer, values are embedded scalar objs.
    // Once filled, it will be sent to the child node for eval.
    input = context->input;
    tupleMeta = &input->tupleMeta;
    new (tupleMeta) NewTupleMeta();
    tupleMeta->setNumFields(numVariables + 1);
    for (unsigned ii = 0; ii < numVariables; ii++) {
        tupleMeta->setFieldType(DfScalarObj, ii);
    }

    // Last value is the rowMeta ptr
    tupleMeta->setFieldType(DfOpRowMetaPtr, numVariables);
    tupleMeta->setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    input->numEvals = numEvals;

    cursor = cursorStart = input->buf;

    // Populate evalStrings
    input->evalStrings = (char **) cursor;

    cursor += sizeof(*input->evalStrings) * numEvals;

    for (unsigned ii = 0; ii < numEvals; ii++) {
        input->evalStrings[ii] = (char *) cursor;
        int ret = strlcpy(input->evalStrings[ii],
                          evalStrings[ii],
                          XcalarApiMaxEvalStringLen);
        cursor += ret + 1;
    }

    // Populate evalArgIndices
    input->evalArgIndices = (int **) cursor;
    cursor += sizeof(*input->evalArgIndices) * numEvals;

    for (unsigned ii = 0; ii < numEvals; ii++) {
        // Write the size of the array first
        unsigned *numArgs = (unsigned *) cursor;
        *numArgs = asts[ii].astCommon.numScalarVariables;
        cursor += sizeof(*numArgs);

        // Copy over the arg indices array from opMeta
        input->evalArgIndices[ii] = (int *) cursor;
        size_t argSize = (asts[ii].astCommon.numScalarVariables) *
                         sizeof(*opMeta->evalArgIndices[ii]);

        memcpy(input->evalArgIndices[ii], opMeta->evalArgIndices[ii], argSize);

        cursor += argSize;
    }

    tupBuf = (NewTuplesBuffer *) cursor;
    new (tupBuf)
        NewTuplesBuffer(tupBuf,
                        XdbMgr::bcSize() - sizeof(OpChildShmInput) -
                            ((uintptr_t) cursor - (uintptr_t) cursorStart));
    input->tupBuf = tupBuf;
    tupCursor = &input->tupCursor;
    new (tupCursor) NewTuplesCursor(tupBuf);

    output = context->output;
    output->srcXdbId = srcXdbId;
    tupleMeta = &output->tupleMeta;
    new (tupleMeta) NewTupleMeta();
    tupleMeta->setNumFields(numEvals + 1);

    for (unsigned ii = 0; ii < numEvals; ii++) {
        if (opMeta->icvMode) {
            // In ICV mode, the error strings are copied to ouput so field type
            // must be DfString.
            tupleMeta->setFieldType(DfString, ii);
        } else {
            if (XcalarEval::isSingletonOutputType(&asts[ii].astCommon)) {
                tupleMeta->setFieldType(XcalarEval::getOutputType(
                                            &asts[ii].astCommon),
                                        ii);
            } else {
                // store array outputs as ScalarObjs
                tupleMeta->setFieldType(DfScalarObj, ii);
            }
        }
    }

    // last index is the rowMeta ptr
    tupleMeta->setFieldType(DfOpRowMetaPtr, numEvals);
    tupleMeta->setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

CommonExit:
    if (status != StatusOk) {
        freeChildShm(context);
    }
    return status;
}

// Note: This only copies the pointers held by valueArray.value[ii] in kvEntry
// It doesn't actually move the actual data from srcKvEntry's kvBuf
// into dstKvEntry's kvBuf!
void
Operators::shallowCopyKvEntry(NewKeyValueEntry *dstKvEntry,
                              uint64_t maxNumValuesInDstKvEntry,
                              NewKeyValueEntry *srcKvEntry,
                              OpKvEntryCopyMapping *opKvEntryCopyMapping,
                              unsigned dstStartIdx,
                              unsigned numEntriesToCopy)
{
    assert(opKvEntryCopyMapping->numEntries <= maxNumValuesInDstKvEntry);
    assert(opKvEntryCopyMapping->numEntries >= dstStartIdx + numEntriesToCopy);

    const NewKeyValueMeta *srcMeta = srcKvEntry->kvMeta_;
    size_t srcNumFields = srcMeta->tupMeta_->getNumFields();
    const NewKeyValueMeta *dstMeta = dstKvEntry->kvMeta_;

    if (opKvEntryCopyMapping->isReplica && numEntriesToCopy > 0) {
        assert(srcNumFields <= maxNumValuesInDstKvEntry);
        for (size_t jj = 0, kk = dstStartIdx; jj < numEntriesToCopy;
             jj++, kk++) {
            // XXX TODO Need to optimize this.
            DfFieldType typeTmp = srcMeta->tupMeta_->getFieldType(jj);
            bool retIsValid;
            DfFieldValue valueTmp =
                srcKvEntry->tuple_.get(jj, srcNumFields, typeTmp, &retIsValid);
            if (retIsValid) {
                dstKvEntry->tuple_.set(kk,
                                       valueTmp,
                                       dstMeta->tupMeta_->getFieldType(kk));
            } else {
                dstKvEntry->tuple_.setInvalid(kk);
            }
        }
    } else {
        for (size_t ii = dstStartIdx; ii < dstStartIdx + numEntriesToCopy;
             ii++) {
            size_t idx = opKvEntryCopyMapping->srcIndices[ii];
            if (unlikely(idx == (size_t) NewTupleMeta::DfInvalidIdx)) {
                // this means we won't be getting this field from src
                continue;
            }

            DfFieldType typeTmp = srcMeta->tupMeta_->getFieldType(idx);
            bool retIsValid;
            DfFieldValue valueTmp =
                srcKvEntry->tuple_.get(idx, srcNumFields, typeTmp, &retIsValid);
            if (retIsValid) {
                dstKvEntry->tuple_.set(ii,
                                       valueTmp,
                                       dstMeta->tupMeta_->getFieldType(ii));
            } else {
                dstKvEntry->tuple_.setInvalid(ii);
            }
        }
    }
}

// status stored in opMeta->status
Status
Operators::addRowToGroup(Scalar **scratchPadScalars,
                         OpGroupMeta *groupMeta,
                         DfFieldType keyType,
                         unsigned numVariables,
                         OpMeta *opMeta,
                         uint64_t *numRowsMissing,
                         NewTupleMeta *tupMeta,
                         Xdb *scratchXdb,
                         bool lock)
{
    Status status = StatusUnknown;
    NewKeyValueMeta kvMeta(tupMeta, 0);
    NewTupleValues valueArray;
    unsigned ii;
    bool groupMetaLockAcquired = false;

    valueArray.setInvalid(0, numVariables);
    tupMeta->setNumFields(numVariables);
    tupMeta->setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    for (ii = 0; ii < numVariables; ii++) {
        tupMeta->setFieldType(DfScalarObj, ii);
        DfFieldValue valueTmp;

        if (scratchPadScalars[ii] == NULL ||
            scratchPadScalars[ii]->fieldUsedSize == 0) {
            continue;
        }
        valueTmp.scalarVal = scratchPadScalars[ii];
        valueArray.set(ii, valueTmp, DfScalarObj);
    }

    assert(opMeta->txn == Txn::currentTxn());

    if (lock) {
        groupMeta->lock.lock();
        groupMetaLockAcquired = true;
    }

    // scalars are embedded into xdbPages, we can free the pointers
    status = opInsertPageChain(&groupMeta->firstPage,
                               NULL,
                               &kvMeta,
                               &valueArray,
                               scratchXdb);
    BailIfFailed(status);

CommonExit:
    if (!groupMetaLockAcquired && lock) {
        groupMeta->lock.lock();
        groupMetaLockAcquired = true;
    }

    groupMeta->numRowsMissing--;
    *numRowsMissing = groupMeta->numRowsMissing;

    if (lock) {
        groupMeta->lock.unlock();
        groupMetaLockAcquired = false;
    }

    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    return status;
}

Status
Operators::executeGroupBy(XcalarEvalClass2Ast *asts,
                          Xdb *dstXdb,
                          XdbMeta *dstMeta,
                          Scalar **results,
                          OpGroupMeta *groupMeta,
                          XdbMeta *srcMeta,
                          Xdb *scratchPadXdb,
                          NewTupleMeta *tupMeta,
                          bool includeSrcTableSample,
                          OpMeta *opMeta,
                          OpInsertHandle *insertHandle)
{
    Status status = StatusUnknown;
    Status evalStatuses[opMeta->numEvals];
    bool errRowLevel = false;
    bool validInsert = false;
    XdbId scratchPadXdbId = XdbIdInvalid;
    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);
    NewKeyValueEntry dstKvEntry(&dstMeta->kvNamedMeta.kvMeta_);

    XdbMgr *xdbMgr = XdbMgr::get();
    XcalarEval *xcalarEval = XcalarEval::get();

    scratchPadXdbId = xdbMgr->xdbGetXdbId(scratchPadXdb);

    xdbMgr->xdbAppendPageChain(scratchPadXdb, 0, groupMeta->firstPage);
    groupMeta->firstPage = NULL;
    status = xdbMgr->xdbLoadDoneScratchXdb(scratchPadXdb);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        asts[ii].scalarXdbId = scratchPadXdbId;
        asts[ii].scalarTableGlobalState = XdbLocal;

        evalStatuses[ii] = XcalarEval::get()->aggregateEval(&asts[ii],
                                                            results[ii],
                                                            opMeta->opStatus,
                                                            scratchPadXdb);

        if (xcalarEval->isFatalError(evalStatuses[ii])) {
            status = evalStatuses[ii];
            goto CommonExit;
        }

        // Make a note if there is an icv error with atleast one of the evals
        // in the case of multi-eval groupby
        if (unlikely(opMeta->icvMode && evalStatuses[ii] != StatusOk)) {
            errRowLevel = true;
        }
    }

    DfFieldValue fieldVal;
    const NewTupleMeta *srcTupMeta;
    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields;
    srcNumFields = srcTupMeta->getNumFields();
    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields;
    dstNumFields = dstTupMeta->getNumFields();

    status = groupMeta->opCursor.getNext(&srcKvEntry);
    BailIfFailed(status);

    if (includeSrcTableSample) {
        assert(opMeta->opKvEntryCopyMapping->numEntries <= dstNumFields);
        shallowCopyKvEntry(&dstKvEntry,
                           dstNumFields,
                           &srcKvEntry,
                           opMeta->opKvEntryCopyMapping,
                           0,
                           opMeta->opKvEntryCopyMapping->numEntries);
    } else {
        // keys are at the front of dstEntry
        for (unsigned ii = 0; ii < opMeta->dstMeta->numKeys; ii++) {
            int keyIndex = srcMeta->keyAttr[ii].valueArrayIndex;

            DfFieldType typeTmp = srcTupMeta->getFieldType(keyIndex);
            bool retIsValid;
            DfFieldValue valueTmp = srcKvEntry.tuple_.get(keyIndex,
                                                          srcNumFields,
                                                          typeTmp,
                                                          &retIsValid);
            if (retIsValid) {
                dstKvEntry.tuple_.set(ii,
                                      valueTmp,
                                      dstTupMeta->getFieldType(ii));
            }
        }
    }

    // Now add the groupBy results
    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        // We insert valid field entries into kv only when: 1. status is ok
        // when icv is false and 2. status is not ok when icv is true
        validInsert = false;
        if (unlikely(opMeta->icvMode && evalStatuses[ii] != StatusOk)) {
            // return value is status string
            const char *statusStr = strGetFromStatus(evalStatuses[ii]);
            fieldVal.stringVal.strActual = statusStr;
            fieldVal.stringVal.strSize = strlen(statusStr) + 1;
            validInsert = true;
        } else if (evalStatuses[ii] == StatusOk && !opMeta->icvMode) {
            if (XcalarEval::getOutputType(&asts[ii].astCommon) == DfScalarObj) {
                fieldVal.scalarVal = results[ii];
            } else {
                status = results[ii]->getValue(&fieldVal);
                assert(status == StatusOk);
            }
            validInsert = true;
        }

        if (validInsert) {
            dstKvEntry.tuple_.set(opMeta->newFieldIdxs[ii],
                                  fieldVal,
                                  dstTupMeta->getFieldType(
                                      opMeta->newFieldIdxs[ii]));
        } else {
            dstKvEntry.tuple_.setInvalid(opMeta->newFieldIdxs[ii]);
        }
    }

    if (!opMeta->icvMode || errRowLevel) {
        status = opPopulateInsertHandle(insertHandle, &dstKvEntry);
        BailIfFailed(status);
    }

CommonExit:
    if (scratchPadXdb != NULL) {
        for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
            if (xcalarEval->isComplex(asts[ii].astCommon.rootNode)) {
                // drops the scratchXdb
                xcalarEval->aggregateDeleteVariables(&asts[ii]);
            }
        }

        // removes the pages from scratchXdb
        xdbMgr->xdbReinitScratchPadXdb(scratchPadXdb);
    }

    return status;
}

Status
Operators::operatorsGroupByWork(XcalarEvalClass2Ast *asts,
                                Xdb *dstXdb,
                                XdbMeta *dstMeta,
                                Xdb *scratchPadXdb,
                                XdbMeta *srcMeta,
                                unsigned numVariables,
                                NewKeyValueEntry *kvEntry,
                                OpMeta *opMeta,
                                MemoryPile **scalarPile,
                                bool includeSrcTableSample,
                                Scalar **results,
                                OpInsertHandle *insertHandle)
{
    Status status = StatusUnknown;
    uint64_t numRowsMissing;
    NewTupleMeta tupMeta;
    uint32_t numIssuedRemaining;
    bool retIsValid;
    DfFieldValue key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    OpGroupByRowMeta *rowMeta = (OpGroupByRowMeta *) key.opRowMetaVal;
    OpGroupMeta *groupMeta = rowMeta->groupMeta;

    status = populateRowMetaWithScalars(rowMeta->scalars,
                                        numVariables,
                                        kvEntry,
                                        scalarPile);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to populate row meta with scalars: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    numIssuedRemaining = atomicDec32(&rowMeta->numIssued);

    if (numIssuedRemaining > 0) {
        // We'll free the rowMeta when all the scalars arrive for the row
        assert(status == StatusOk);
        goto CommonExit;
    }

    // I am the last guy for my row, but not necessarily the last row
    // in my group
    assert(numIssuedRemaining == 0);

    status = addRowToGroup(rowMeta->scalars,
                           groupMeta,
                           srcMeta->keyAttr[0].type,
                           numVariables,
                           opMeta,
                           &numRowsMissing,
                           &tupMeta,
                           scratchPadXdb,
                           true);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to add row to group: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // All the scalars for my row has arrived. Get rid of this row
    putGroupByRowMeta(rowMeta, numVariables);

    if (numRowsMissing > 0) {
        // We'll free the group when all of the rows arrive for this group
        assert(status == StatusOk);
        goto CommonExit;
    }

    // Last guy in the group
    assert(numRowsMissing == 0);
    status = executeGroupBy(asts,
                            dstXdb,
                            dstMeta,
                            results,
                            groupMeta,
                            srcMeta,
                            scratchPadXdb,
                            &tupMeta,
                            includeSrcTableSample,
                            opMeta,
                            insertHandle);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to execute group by: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    groupMeta->firstPage = NULL;

    // All the rows for my group has arrived. Get rid of this group
    putGroupMeta(groupMeta);

CommonExit:
    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    return status;
}

// Deals with a single eval result during filter/map.
Status
Operators::processFilterAndMapResultRow(OpMeta *opMeta,
                                        Xdb *dstXdb,
                                        NewKeyValueEntry *srcKvEntry,
                                        NewKeyValueEntry *dstKvEntry,
                                        unsigned numEvals,
                                        DfFieldValue *evalResults,
                                        DfFieldType *evalResultTypes,
                                        Status *evalStatuses,
                                        OpInsertHandle *insertHandle)

{
    Status status = StatusOk;
    bool error = false;
    const NewTupleMeta *dstTupMeta = dstKvEntry->kvMeta_->tupMeta_;
    size_t dstNumFields = dstTupMeta->getNumFields();
    dstKvEntry->init();

    if (opMeta->op == OperatorsMap) {
        // This is a map. Just insert directly into dest XDB.

        assert(opMeta->opKvEntryCopyMapping->numEntries <= dstNumFields);

        shallowCopyKvEntry(dstKvEntry,
                           dstNumFields,
                           srcKvEntry,
                           opMeta->opKvEntryCopyMapping,
                           0,
                           opMeta->opKvEntryCopyMapping->numEntries);

        // first process non-array outputs, filling up dstKvEntry appropriately
        for (unsigned ii = 0; ii < numEvals; ii++) {
            if (evalResultTypes[ii] == DfScalarObj) {
                continue;
            }

            size_t newIdx = opMeta->newFieldIdxs[ii];

            if (unlikely(opMeta->icvMode)) {
                if (evalStatuses[ii] != StatusOk) {
                    error = true;
                    assert(evalResultTypes[ii] != DfNull);
                    if (evalResultTypes[ii] != DfNull) {
                        dstKvEntry->tuple_.set(newIdx,
                                               evalResults[ii],
                                               dstTupMeta->getFieldType(
                                                   newIdx));
                    } else {
                        dstKvEntry->tuple_.setInvalid(newIdx);
                    }
                } else {
                    // icvMode && evalStatuses[ii] == StatusOk
                    //
                    // In ICV mode, OK statuses imply FNF / empty results; so
                    // set the valid bit appropriately
                    //
                    // XXX: should be able to assert that evalResultTypes[ii]
                    // is always DfNull here
                    dstKvEntry->tuple_.setInvalid(newIdx);
                }
            } else {
                if (evalStatuses[ii] == StatusOk &&
                    evalResultTypes[ii] != DfNull) {
                    dstKvEntry->tuple_.set(newIdx,
                                           evalResults[ii],
                                           dstTupMeta->getFieldType(newIdx));
                } else {
                    dstKvEntry->tuple_.setInvalid(newIdx);
                }
            }
        }

        // process array results, add a row to dstTable for each element in the
        // array. Multiple arrays are zipped together using FNF as placeholder
        int arrayIndexesRemaining;
        int curArrayIdx = 0;
        do {
            arrayIndexesRemaining = 0;
            for (unsigned ii = 0; ii < numEvals; ii++) {
                if (evalResultTypes[ii] != DfScalarObj) {
                    continue;
                }

                if (evalStatuses[ii] == StatusOk) {
                    arrayIndexesRemaining =
                        fmax(arrayIndexesRemaining,
                             evalResults[ii].scalarVal->fieldNumValues -
                                 curArrayIdx - 1);
                }

                size_t newIdx = opMeta->newFieldIdxs[ii];

                if (unlikely(opMeta->icvMode)) {
                    if (evalStatuses[ii] != StatusOk) {
                        // XXX: check this scenario
                        // return value is status string
                        const char *statusStr =
                            strGetFromStatus(evalStatuses[ii]);
                        DfFieldValue valueTmp;
                        valueTmp.stringVal.strActual = statusStr;
                        valueTmp.stringVal.strSize = strlen(statusStr) + 1;
                        dstKvEntry->tuple_.set(newIdx,
                                               valueTmp,
                                               dstTupMeta->getFieldType(
                                                   newIdx));
                        error = true;
                    }
                } else {
                    if (evalStatuses[ii] == StatusOk &&
                        curArrayIdx <
                            evalResults[ii].scalarVal->fieldNumValues) {
                        DfFieldValue value;
                        status =
                            evalResults[ii].scalarVal->getValue(&value,
                                                                curArrayIdx);
                        if (status != StatusOk) {
                            return status;
                        }

                        dstKvEntry->tuple_.set(newIdx,
                                               value,
                                               dstTupMeta->getFieldType(
                                                   newIdx));
                    } else {
                        dstKvEntry->tuple_.setInvalid(newIdx);
                    }
                }
            }

            if (!opMeta->icvMode || error) {
                status = opPopulateInsertHandle(insertHandle, dstKvEntry);
                if (status != StatusOk) {
                    return status;
                }
            }

            curArrayIdx++;
        } while (arrayIndexesRemaining);

    } else if (opMeta->op == OperatorsFilter && evalStatuses[0] == StatusOk &&
               XcalarEval::isEvalResultTrue(evalResults[0],
                                            evalResultTypes[0])) {
        // Filter. Copy src to dst if condition evaluated to true.
        if (opMeta->opKvEntryCopyMapping->isReplica) {
            status = opPopulateInsertHandle(insertHandle, srcKvEntry);
            if (status != StatusOk) {
                return status;
            }
        } else {
            shallowCopyKvEntry(dstKvEntry,
                               dstNumFields,
                               srcKvEntry,
                               opMeta->opKvEntryCopyMapping,
                               0,
                               opMeta->opKvEntryCopyMapping->numEntries);

            status = opPopulateInsertHandle(insertHandle, dstKvEntry);
            if (status != StatusOk) {
                return status;
            }
        }
    } else {
        assert(opMeta->op == OperatorsFilter);
    }

    return status;
}

void
Operators::updateEvalXdfErrorStats(Status status, OpEvalErrorStats *errorStats)
{
    if (errorStats != NULL && status != StatusOk) {
        switch (status.code()) {
        case StatusCodeEvalUnsubstitutedVariables:
            errorStats->evalXdfErrorStats.numUnsubstituted++;
            break;
        case StatusCodeXdfTypeUnsupported:
            errorStats->evalXdfErrorStats.numUnspportedTypes++;
            break;
        case StatusCodeXdfMixedTypeNotSupported:
            errorStats->evalXdfErrorStats.numMixedTypeNotSupported++;
            break;
        case StatusCodeEvalCastError:
            errorStats->evalXdfErrorStats.numEvalCastError++;
            break;
        case StatusCodeXdfDivByZero:
            errorStats->evalXdfErrorStats.numDivByZero++;
            break;
        default:
            errorStats->evalXdfErrorStats.numMiscError++;
        }
        errorStats->evalXdfErrorStats.numTotal++;
    }
}

// Called every time the child fills the output buffer (and on completion).
Status
Operators::processChildShmResponse(OpMeta *opMeta,
                                   OpChildShmContext *childContext,
                                   Xdb *dstXdb)
{
    Status status;
    NewKeyValueEntry srcKvEntry;
    NewKeyValueEntry dstKvEntry;
    NewKeyValueMeta *srcMeta = NULL;
    XdbMeta *srcXdbMeta = NULL;
    Xdb *srcXdb;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbMeta *dstXdbMeta = xdbMgr->xdbGetMeta(dstXdb);
    OpRowMeta *rowMeta = NULL;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    unsigned numEvals = opMeta->numEvals;
    bool icvMode = opMeta->icvMode;
    DfFieldValue evalResults[numEvals];
    DfFieldType evalTypes[numEvals];
    Status evalStatuses[numEvals];

    OpChildShmOutput *output = childContext->output;
    assert(output != NULL);
    output->tupBuf.deserialize();
    NewTuplesCursor newCursor(&output->tupBuf);
    NewTupleMeta *tupleMeta = &output->tupleMeta;
    NewTupleValues tuplePtr;
    size_t numFields = tupleMeta->getNumFields();
    size_t rowMetaFieldsIdx = numFields - 1;

    status = opGetInsertHandle(&insertHandle,
                               &dstXdbMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    // output has a result for each eval and a rowMeta at the end,
    // treat rowMeta as the key

    // XXX Xc-12250 Consider using PerTxnInfo instead of doing xdbGet each time.
    status = xdbMgr->xdbGet(output->srcXdbId, &srcXdb, &srcXdbMeta);
    BailIfFailed(status);

    srcMeta = &srcXdbMeta->kvNamedMeta.kvMeta_;
    new (&srcKvEntry) NewKeyValueEntry(srcMeta);
    new (&dstKvEntry) NewKeyValueEntry(&dstXdbMeta->kvNamedMeta.kvMeta_);

    // Child eval always uses variable size packing.
    while ((status = newCursor.getNextVar(tupleMeta, &tuplePtr)) == StatusOk) {
        bool retIsValid;
        DfFieldValue val =
            tuplePtr.get(rowMetaFieldsIdx,
                         numFields,
                         tupleMeta->getFieldType(rowMetaFieldsIdx),
                         &retIsValid);
        assert(retIsValid);
        rowMeta = (OpRowMeta *) val.opRowMetaVal;

        // this is needed for SlotHash
        insertHandle.slotId = rowMeta->opCursor.xdbPgCursor.slotId_;

        status = rowMeta->opCursor.getNext(&srcKvEntry);
        BailIfFailed(status);

        for (unsigned ii = 0; ii < numEvals; ii++) {
            evalTypes[ii] = tupleMeta->getFieldType(ii);
            bool retIsValid;
            evalResults[ii] =
                tuplePtr.get(ii, numFields, evalTypes[ii], &retIsValid);
            if (icvMode) {
                // In icvMode, the sense of 'retIsValid' has been flipped:
                // - if it's true, then it's a failure description string for
                //   a UDF or XDF
                // - if it's false, then the result was left empty in child for
                //   this row, since: either the eval PASSED OR the eval is a
                //   XDF- see Operators::operatorsChildEvalHelper() - so
                //   retIsValid being false implies statusOk for UDFs, and for
                //   XDFs, we use the statusOk code as a way to skip NULL
                //   values for them (in ICV mode, passes yield NULL, and for
                //   failures, only UDFs yield non-NULL values) in
                //   processFilterAndMapResultRow
                if (retIsValid) {
                    // A failure description string is generated in failure
                    // mode (status != StatusOk) - see
                    // Operators::operatorsChildEvalHelper() (although it is
                    // generated deep down in UdfPyFunction::evalMap()).  In
                    // the case of certain failures like
                    // StatusUdfExecuteFailed, the failure description string
                    // is typically a Python traceback, but for other failures,
                    // it'd be a string returned by strGetFromStatus(status)).
                    // So, any non-OK status can be set in evalStatuses[ii]
                    // here since processFilterAndMapResultRow() checks for
                    // this code - whatever it is...to set the failure
                    // description (which will be customized to the error and
                    // so the status code itself wouldn't matter)
                    // We use a non-ambiguous term StatusUdfIcvModeFailure to
                    // be used for this purpose - as a protocol b/w this routine
                    // and processFilterAndMapResultRow()
                    evalStatuses[ii] = StatusUdfIcvModeFailure;
                } else {
                    evalStatuses[ii] = StatusOk;
                }
            } else {
                if (retIsValid) {
                    evalStatuses[ii] = StatusOk;
                } else {
                    evalStatuses[ii] = StatusUnknown;
                }
            }
        }

        status = processFilterAndMapResultRow(opMeta,
                                              dstXdb,
                                              &srcKvEntry,
                                              &dstKvEntry,
                                              numEvals,
                                              evalResults,
                                              evalTypes,
                                              evalStatuses,
                                              &insertHandle);

        BailIfFailed(status);

        // scalars have already been freed in udfExecFilterOrMap
        putRowMeta(rowMeta, 0);
        rowMeta = NULL;
    }

    assert(status == StatusNoData);
    status = StatusOk;

CommonExit:
    if (!status.ok()) {
        Status status2;
        // free if rowMeta exists first for any pending row
        if (rowMeta) {
            putRowMeta(rowMeta, 0);
            rowMeta = NULL;
        }
        // check if there are any rows which are not processed due to some
        // failures and free rowMeta so underlying xdb page can dec ref count.
        while ((status2 = newCursor.getNextVar(tupleMeta, &tuplePtr)) ==
               StatusOk) {
            bool retIsValid;
            DfFieldValue val =
                tuplePtr.get(rowMetaFieldsIdx,
                             numFields,
                             tupleMeta->getFieldType(rowMetaFieldsIdx),
                             &retIsValid);
            assert(retIsValid);
            rowMeta = (OpRowMeta *) val.opRowMetaVal;
            putRowMeta(rowMeta, 0);
            rowMeta = NULL;
        }
        assert(status2 == StatusNoData);
    }
    assert(rowMeta == NULL);

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    output->tupBuf.reset();
    return status;
}

void
Operators::deserializeChildShmInput(OpChildShmInput *input)
{
    uint8_t *cursor = input->buf;
    input->evalStrings = (char **) cursor;
    cursor += sizeof(*input->evalStrings) * input->numEvals;

    for (unsigned ii = 0; ii < input->numEvals; ii++) {
        input->evalStrings[ii] = (char *) cursor;
        cursor += strlen(input->evalStrings[ii]) + 1;
    }

    input->evalArgIndices = (int **) cursor;
    cursor += sizeof(*input->evalArgIndices) * input->numEvals;

    for (unsigned ii = 0; ii < input->numEvals; ii++) {
        unsigned numArgs = *(unsigned *) cursor;
        cursor += sizeof(numArgs);

        input->evalArgIndices[ii] = (int *) cursor;
        cursor += numArgs * sizeof(*input->evalArgIndices[ii]);
    }

    NewTuplesBuffer *tupBuf = (NewTuplesBuffer *) cursor;
    tupBuf->deserialize();
    input->tupBuf = tupBuf;

    // deserializing Tuple Cursor
    input->tupCursor.deserialize(tupBuf);
}

// Executed in childnode to perform eval.
Status  // static
Operators::operatorsChildEval(const ChildEvalRequest &request,
                              ProtoResponseMsg *response)
{
    Status status = StatusOk;
    Child *child = Child::get();
    uint64_t numRowsFailedTotal = 0;

    OpChildShmInput *input = (OpChildShmInput *) child->getBufFromOffset(
        request.ptrinput().bufcacheoffset());
    OpChildShmOutput *output = (OpChildShmOutput *) child->getBufFromOffset(
        request.ptroutput().bufcacheoffset());
    status = operatorsChildEvalHelper(input,
                                      output,
                                      request.icvmode(),
                                      &numRowsFailedTotal);

    // NOTE: status != StatusOk here could be:
    //
    // (A) StatusNoData: last row didn't fit and so parent needs to re-send
    //     beginning from last row.
    //
    // (B) status for which isFatalError() is true
    //
    // In either case, the numRowsFailedTotal count still needs to be returned
    // since the failed status is for the last row - so the number of failures
    // when processing the preceding rows shouldn't be lost

    // setting a single uint64 field doesn't throw in protobuf..so no need for
    // try/catch around the call to set_num_rows_failed_total()
    response->mutable_parentchild()->set_num_rows_failed_total(
        numRowsFailedTotal);

    return status;
}

Status  // static
Operators::operatorsChildEvalHelper(OpChildShmInput *input,
                                    OpChildShmOutput *output,
                                    bool icvMode,
                                    uint64_t *numRowsFailedTotal)
{
    size_t sizeOut = XdbMgr::bcSize();
    XcalarEval *xcalarEval = XcalarEval::get();
    Status status = StatusOk;
    int64_t numRowsScalarFuncFailedCount = 0;

    // Non-intuitively, XDFs may be executed inside the child, despite the
    // fact that they don't need to invoke the Python interpreter - generally,
    // the child is needed only for python execution - XDFs are in C++ so why
    // would they ever execute in the child? Well, this is when a single map
    // with multiple evals, has a mixture of XDFs and UDFs - in thie case, the
    // entire set of evals - both XDFs, and UDFs are executed in the child!

    // Keep track of previous so we can revert cursor to beginning of row that
    // didn't fit.
    NewTuplesCursor::Position prevPosition;

    deserializeChildShmInput(input);

    NewTuplesCursor *tupCursorSrc = &input->tupCursor;
    NewTuplesBuffer *tupBufSrc = input->tupBuf;
    NewTupleValues tuplePtrSrc;
    NewTupleMeta *tupleMetaSrc = &input->tupleMeta;
    NewTupleMeta *tupleMetaDst = &output->tupleMeta;
    size_t numFieldsDst = tupleMetaDst->getNumFields();
    NewTuplesBuffer *tupBufDst;

    XcalarEvalClass1Ast *asts = NULL;
    bool astsGenerated[input->numEvals];
    for (unsigned ii = 0; ii < input->numEvals; ii++) {
        astsGenerated[ii] = false;
    }

    tupBufDst = (NewTuplesBuffer *) (&output->tupBuf);
    new (tupBufDst) NewTuplesBuffer(tupBufDst, sizeOut - sizeof(*output));

    // rowMeta is the last value in the tuple
    size_t srcNumFields = tupleMetaSrc->getNumFields();
    size_t rowMetaIndex = srcNumFields - 1;
    asts = (XcalarEvalClass1Ast *) memAlloc(sizeof(*asts) * input->numEvals);
    BailIfNull(asts);

    for (unsigned ii = 0; ii < input->numEvals; ii++) {
        status =
            xcalarEval->generateClass1Ast(input->evalStrings[ii], &asts[ii]);
        BailIfFailed(status);
        astsGenerated[ii] = true;
    }

    // Child eval always uses variable size packing.
    while ((status = tupCursorSrc->getNextVar(tupleMetaSrc, &tuplePtrSrc)) ==
           StatusOk) {
        DfFieldValue evalResult;
        DfFieldType evalResultType = DfUnknown;
        bool evalResultIsValid;
        bool thisRowHasFailures = false;
        int64_t numEvalFailuresInThisRow = 0;
        NewTupleValues tupleValuesDst;
        tupleValuesDst.setInvalid(0, numFieldsDst);

        for (unsigned ii = 0; ii < input->numEvals; ii++) {
            for (unsigned jj = 0; jj < asts[ii].astCommon.numScalarVariables;
                 jj++) {
                int idx = input->evalArgIndices[ii][jj];
                bool retIsValid = false;
                DfFieldValue valueTmp =
                    tuplePtrSrc.get(idx,
                                    srcNumFields,
                                    tupleMetaSrc->getFieldType(idx),
                                    &retIsValid);

                if (!retIsValid) {
                    asts[ii].astCommon.scalarVariables[jj].content = NULL;
                } else {
                    asts[ii].astCommon.scalarVariables[jj].content =
                        valueTmp.scalarVal;
                }
            }

            status = xcalarEval->eval(&asts[ii],
                                      &evalResult,
                                      &evalResultType,
                                      icvMode);
            evalResultIsValid = (status == StatusOk);

            if (unlikely(xcalarEval->isFatalError(status))) {
                numRowsScalarFuncFailedCount++;
                // NOTE: this would not appear as a failed row in the table,
                // since this is a fatal error and so tupBufDst->appendVar()
                // isn't being called. So the numRowsScalarFuncFailedCount
                // counter may be one larger than the number of failed rows
                // which actually appear in the table
                goto CommonExit;
            }
            if (icvMode) {
                // Note: Even though we handle both XDF and UDF failures,
                // the failure descriptions for the two are going to be
                // different (python stack trace for UDFs, but some canned
                // description for XDFs).
                //
                if (!evalResultIsValid) {
                    // all failures including UdfExecuteFailed handled here.
                    // xcalarEval->eval() returns evalResult/Type, for all
                    // non-OK status codes (including
                    // StatusUdfExecuteFailed).
                    // The evalResult will typically be the traceback
                    // string for UDFs, but for XDF failures it'll be the status
                    // string from strGetFromStatus(status) so it's very
                    // clear what kind of failure was hit
                    assert(status != StatusOk);
                    assert(tupleMetaDst->getFieldType(ii) == evalResultType);
                    tupleValuesDst.set(ii, evalResult, evalResultType);

                    numRowsScalarFuncFailedCount++;

                    // If this row has eval failures, count them and set
                    // the boolean "thisRowHasFailures" below - just in case the
                    // destination buffer is full (see StatusNoData below),
                    // and we need to back-track the cursor - in which
                    // case, the failure counter numRowsScalarFuncFailedCount
                    // must be reduced by the number of failures in this row.
                    //
                    // Note that this is needed in either mode (ICV or not),
                    // since there can be multiple evals - some may succeed,
                    // and some may fail - and can produce data to be stored in
                    // the tupleValuesDst (causing a possible StatusNoData).

                    numEvalFailuresInThisRow++;
                    thisRowHasFailures = true;
                }
            } else {
                // normal mode:
                //
                // - report valid results (a valid result could be DfNull)
                // - invalid results are left invalid and empty
                //
                // XXX: NOTE that the eval could return a valid but DfNull type
                // ('return None' in python). Currently, in the tuples buffer,
                // it's not possible to distinguish between valid/empty and
                // invalid/empty fields. If a field is empty, it's assumed to
                // be invalid.
                //
                // Until this is fixed, one can use the following technique to
                // distinguish between the two (valid/empty or invalid/empty):
                //
                // Use the failure counter - if the failure counter for the
                // entire eval operation is 0, all empty fields must be valid.
                //
                // If the failure counter is non-0, use icv mode to debug and
                // fix all the failures.

                if (evalResultIsValid) {
                    // result type should always be the same except if a NULL
                    // is returned legitimately
                    assert(evalResultType == DfNull ||
                           tupleMetaDst->getFieldType(ii) == evalResultType);
                    if (evalResultType != DfNull) {
                        tupleValuesDst.set(ii, evalResult, evalResultType);
                    }
                    // XXX:
                    //
                    // else do nothing - this is equivalent to doing:
                    //     tupleValuesDst.setInvalid(ii);
                    // since tupleValuesDst was initialized to Invalid for all
                    // fields; since it's NOT being set here, it'll remain
                    // invalid; eventually, when a valid but empty/NULL field
                    // is supported, the if check above can be removed
                } else {
                    numRowsScalarFuncFailedCount++;
                    numEvalFailuresInThisRow++;  // see comment for icvMode
                                                 // above
                    thisRowHasFailures = true;
                    // NOTE: count all failures across all eval strings
                    // i.e.  don't stop at first eval to fail - in this
                    // case, the numRowsScalarFuncFailedCount could exceed total
                    // rows in table the max is N * rows, where N is number
                    // of eval strings
                }
            }
        }

        // pass along rowMeta pointer
        bool retIsValid;
        DfFieldValue val =
            tuplePtrSrc.get(rowMetaIndex,
                            srcNumFields,
                            tupleMetaSrc->getFieldType(rowMetaIndex),
                            &retIsValid);
        assert(retIsValid);
        tupleValuesDst.set(input->numEvals, val, DfOpRowMetaPtr);

        // Child eval always uses variable size packing.
        status = tupBufDst->appendVar(tupleMetaDst, &tupleValuesDst);
        if (unlikely(status == StatusNoData)) {
            // couldn't fit this row, reset cursor
            tupCursorSrc->setPosition(prevPosition);

            // The combination of StatusNoData and thisRowHasFailures == true
            // can occur in either mode (icvMode or not) since in either mode
            // data may have been added to dest buffer (resulting in
            // StatusNoData):
            //
            // - for icvMode off: if there are multiple evals, some may succeed,
            //   resulting in data adds to dest buffer, and some may fail. If
            //   all fail, there's still the rowMeta pointer add above that may
            //   result in StatusNoData...
            // - icvMode is on: if there are multiple evals, some may fail,
            //   resulting in data adds to dest buffer, and some may pass. If
            //   all pass, there's still the rowMeta pointer add above..
            //
            // Also, see note above where numEvalFailuresInThisRow is
            // incremented about the need to reduce
            // numRowsScalarFuncFailedCount by numEvalFailuresInThisRow

            if (thisRowHasFailures) {
                assert(numRowsScalarFuncFailedCount >=
                       numEvalFailuresInThisRow);
                numRowsScalarFuncFailedCount -= numEvalFailuresInThisRow;
            }
            goto CommonExit;
        }
        assert(status == StatusOk);

        prevPosition = tupCursorSrc->getPosition();
        numEvalFailuresInThisRow = 0;
        thisRowHasFailures = false;
    }

    assert(status == StatusNoData);
    status = StatusOk;

CommonExit:
    tupBufSrc->serialize();
    tupBufDst->serialize();

    if (asts != NULL) {
        for (unsigned ii = 0; ii < input->numEvals; ii++) {
            if (astsGenerated[ii]) {
                // scratchPadScalars have already been freed
                xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                xcalarEval->destroyClass1Ast(&asts[ii]);
                astsGenerated[ii] = false;
            }
        }

        memFree(asts);
        asts = NULL;
    }
    *numRowsFailedTotal = numRowsScalarFuncFailedCount;
    return status;
}

Status
Operators::udfSendToChildForEval(OpChildShmInput *input,
                                 OpChildShmContext *childContext,
                                 OpMeta *opMeta,
                                 Xdb *dstXdb)
{
    Status childStatus, status;
    ParentChildShmPtr ptrIn;
    ParentChildShmPtr ptrOut;
    ChildEvalRequest evalRequest;
    ProtoChildRequest childRequest;
    bool childInUse = false;
    const ProtoResponseMsg *responseMsg;
    uint64_t numRowsFailedTotal;

    input->tupBuf->serialize();
    input->tupCursor.serialize();
    ChildPool::PooledChild *pooledChild = NULL;

    // This will block until a child is available
    pooledChild = opMeta->childPool->getChild();
    BailIfNull(pooledChild);

    // Construct request message.
    ptrIn.set_bufcacheoffset(
        BufCacheMemMgr::get()->getBufCacheOffset(childContext->input));
    ptrIn.set_offset(0);

    ptrOut.set_bufcacheoffset(
        BufCacheMemMgr::get()->getBufCacheOffset(childContext->output));
    ptrOut.set_offset(0);

    evalRequest.set_allocated_ptrinput(&ptrIn);
    evalRequest.set_allocated_ptroutput(&ptrOut);
    evalRequest.set_icvmode(opMeta->icvMode);

    childRequest.set_func(ChildFuncUdfEval);
    childRequest.set_allocated_childeval(&evalRequest);

    // This is a little hard to understand. The child is cursoring through
    // input and inserting into output, both of which are fixed size bufs
    // If output becomes full before processing all of input then the
    // cursor state is stored in input->kvCursor and StatusNoData is
    // returned. Otherwise StatusOk is returned
    do {
        LocalConnection::Response *response;
        childInUse = true;
        status = pooledChild->child->sendAsync(&childRequest, &response);
        BailIfFailed(status);

        do {
            status = response->wait(USecsPerSec);
            if (status != StatusOk) {
                if (status == StatusTimedOut) {
                    if (opMeta->opStatus->atomicOpDetails.cancelled) {
                        status = StatusCanceled;
                        response->refPut();
                        goto CommonExit;
                    }
                } else {
                    response->refPut();
                    goto CommonExit;
                }
            }
        } while (status == StatusTimedOut);
        childInUse = false;

        responseMsg = response->get();
        childStatus.fromStatusCode((StatusCode) responseMsg->status());
        if (childStatus != StatusOk && childStatus != StatusNoData) {
            status = childStatus;
            goto CommonExit;
        }

        {
            const ProtoParentChildResponse &parentChildResponse =
                responseMsg->parentchild();
            numRowsFailedTotal = parentChildResponse.num_rows_failed_total();
        }

        response->refPut();

        if (numRowsFailedTotal != 0) {
            atomicAdd64(&opMeta->opStatus->atomicOpDetails.errorStats
                             .evalErrorStats.evalUdfErrorStats
                             .numEvalUdfErrorAtomic,
                        numRowsFailedTotal);
        }
        // Child filled output buffer. Process it.
        status = processChildShmResponse(opMeta, childContext, dstXdb);
        BailIfFailed(status);
    } while (childStatus == StatusNoData);

CommonExit:
    deserializeChildShmInput(input);

    // Reinit the Tuple Buffer and Cursor
    input->tupBuf->reset();

    new (&input->tupCursor) NewTuplesCursor(input->tupBuf);

    if (pooledChild) {
        opMeta->childPool->putChild(pooledChild, childInUse);
    }
    childRequest.release_childeval();
    evalRequest.release_ptrinput();
    evalRequest.release_ptroutput();
    return status;
}

// "Last guy" code. Send all remaining rows over to the child for eval.
Status
Operators::udfExecFilterOrMap(OpMeta *opMeta,
                              OpRowMeta *rowMeta,
                              OpChildShmContext *childContext,
                              Xdb *dstXdb)
{
    Status status;

    OpChildShmInput *input = childContext->input;
    NewTupleMeta *tupleMeta = &input->tupleMeta;
    NewTuplesBuffer *tupBuf = input->tupBuf;

    // Only the last guy should run this
    assert(atomicRead32(&rowMeta->numIssued) == 0);

    NewTupleValues tupleValues;
    DfFieldValue valueTmp;

    tupleValues.setInvalid(0, tupleMeta->getNumFields());

    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        if (rowMeta->scalars[ii]) {
            valueTmp.scalarVal = rowMeta->scalars[ii];
            tupleValues.set(ii, valueTmp, DfScalarObj);
        }
    }

    // Then we overwrite with the aggregateResult. This is because
    // aggregateVariables has higher precedence. We can just overwrite
    // becuase these scalars are not owned by us (we don't have to free them,
    // so we don't have to worry about losing them)
    for (unsigned ii = 0; ii < opMeta->numAggregateResults; ii++) {
        unsigned index = opMeta->aggregateResults[ii].variableIdx;
        valueTmp.scalarVal = opMeta->aggregateResults[ii].result;
        assert(valueTmp.scalarVal != NULL);
        tupleValues.set(index, valueTmp, DfScalarObj);
    }

    // key is rowMeta, this is a pointer to memory in parent node since the
    // child doesn't access it, but passes it to completion path in parent
    // key is the last value in the valueArray
    valueTmp.opRowMetaVal = (uintptr_t) rowMeta;
    tupleValues.set(opMeta->numVariables, valueTmp, DfOpRowMetaPtr);

    // embed scalars
    // Child eval always uses variable size packing.
    status = tupBuf->appendVar(tupleMeta, &tupleValues);
    if (status == StatusNoData) {
        // our page is full, send to child
        status = udfSendToChildForEval(input, childContext, opMeta, dstXdb);
        BailIfFailed(status);

        // Not safe to refer to rowMeta as soon as we send it off to the child
        // because we free rowMeta in processChildShmResponse

        status = tupBuf->append(tupleMeta, &tupleValues);
        // we just reset, this should fit
        assert(status == StatusOk);
    }

    // we no longer need these scalars after inserting into kvBuf
    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        if (rowMeta->scalars[ii] != NULL) {
            putTempScalar(rowMeta->scalars[ii]);
            rowMeta->scalars[ii] = NULL;
        }
    }

CommonExit:
    return status;
}

Status
Operators::executeFilterOrMap(XcalarEvalClass1Ast *asts,
                              Xdb *dstXdb,
                              NewKeyValueEntry *srcKvEntry,
                              Scalar **scalars,
                              OpInsertHandle *insertHandle,
                              OpMeta *opMeta)
{
    unsigned ii, jj;
    XcalarEval *xcalarEval = XcalarEval::get();
    Status evalStatuses[opMeta->numEvals];
    DfFieldValue evalResults[opMeta->numEvals];
    DfFieldType evalResultTypes[opMeta->numEvals];
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbMeta *dstXdbMeta = xdbMgr->xdbGetMeta(dstXdb);
    uint64_t numXdfFailures = 0;

    NewKeyValueEntry dstKvEntry(&dstXdbMeta->kvNamedMeta.kvMeta_);

    for (ii = 0; ii < opMeta->numAggregateResults; ii++) {
        if (scalars[opMeta->aggregateResults[ii].variableIdx] == NULL) {
            scalars[opMeta->aggregateResults[ii].variableIdx] =
                opMeta->aggregateResults[ii].result;
        }
    }

    for (ii = 0; ii < opMeta->numEvals; ii++) {
        for (jj = 0; jj < asts[ii].astCommon.numScalarVariables; jj++) {
            int idx = opMeta->evalArgIndices[ii][jj];

            if (scalars[idx] == NULL || scalars[idx]->fieldUsedSize == 0) {
                asts[ii].astCommon.scalarVariables[jj].content = NULL;
            } else {
                asts[ii].astCommon.scalarVariables[jj].content = scalars[idx];
            }
        }

        evalStatuses[ii] = xcalarEval->eval(&asts[ii],
                                            &evalResults[ii],
                                            &evalResultTypes[ii],
                                            opMeta->icvMode);
        if (xcalarEval->isFatalError(evalStatuses[ii])) {
            return evalStatuses[ii];
        }
        if (evalStatuses[ii] != StatusOk) {
            numXdfFailures++;
        }
    }
    if (numXdfFailures != 0) {
        atomicAdd64(&opMeta->opStatus->atomicOpDetails.errorStats.evalErrorStats
                         .evalXdfErrorStats.numTotalAtomic,
                    numXdfFailures);
    }
    return processFilterAndMapResultRow(opMeta,
                                        dstXdb,
                                        srcKvEntry,
                                        &dstKvEntry,
                                        opMeta->numEvals,
                                        evalResults,
                                        evalResultTypes,
                                        evalStatuses,
                                        insertHandle);
}

// Used to copy scalars from scratch pad to rowMeta.
Status
Operators::populateRowMetaWithScalarsFromArray(ScalarPtr dest[],
                                               unsigned numValues,
                                               ScalarPtr source[],
                                               MemoryPile **scalarPile)
{
    Status status = StatusOk;
    Scalar *scalarIn;
    Scalar *scalar;
    unsigned ii;

    for (ii = 0; ii < numValues; ii++) {
        scalarIn = source[ii];
        if (scalarIn == NULL || scalarIn->fieldUsedSize == 0) {
            dest[ii] = NULL;
            continue;
        }

        status = getTempScalar(&scalar, scalarIn->fieldUsedSize, scalarPile);
        if (status != StatusOk) {
            return status;
        }

        // Already alloced - won't fail.
        verifyOk(scalar->copyFrom(scalarIn));
        dest[ii] = scalar;
    }

    // Don't have to clean up because last guy populating the rowMeta
    // will do the cleanup
    return status;
}

Status
Operators::populateRowMetaWithScalars(ScalarPtr scalarPtrs[],
                                      unsigned numVariables,
                                      NewKeyValueEntry *kvEntry,
                                      MemoryPile **scalarPile)
{
    Status status = StatusUnknown;
    Scalar *scalarIn;
    Scalar *scalarOut;
    unsigned ii;
    size_t numFields = kvEntry->kvMeta_->tupMeta_->getNumFields();

    assert(scalarPtrs != NULL);

    for (ii = 0; ii < numVariables; ii++) {
        DfFieldValue valueTmp;
        bool retIsValid;
        DfFieldType typeTmp = DfScalarObj;

        valueTmp = kvEntry->tuple_.get(ii, numFields, typeTmp, &retIsValid);
        if (!retIsValid) {
            continue;
        }

        if (scalarPtrs[ii] != NULL) {
            continue;
        }

        scalarIn = valueTmp.scalarVal;

        status = getTempScalar(&scalarOut, scalarIn->fieldUsedSize, scalarPile);
        BailIfFailed(status);

        // Will never fail by design because scalar is alloc'ed
        // specially to fit scalarIn (always returns StatusOk)
        status = scalarOut->copyFrom(scalarIn);
        assert(status == StatusOk);

        if (atomicCmpXchg64((Atomic64 *) &scalarPtrs[ii],
                            0,
                            (int64_t) scalarOut) != 0) {
            putTempScalar(scalarOut);
            continue;
        }
    }

    status = StatusOk;

CommonExit:
    // Don't have to clean up because last guy populating the rowMeta
    // will do the cleanup
    return status;
}

Status
Operators::hashAndSendKv(TransportPageHandle *scalarPagesHandle,
                         TransportPage *scalarPages[],
                         TransportPageHandle *demystifyPagesHandle,
                         TransportPage *demystifyPages[],
                         unsigned numKeys,
                         const DfFieldType *keyType,
                         const DfFieldValue *keyValue,
                         bool *keyValid,
                         NewKeyValueEntry *dstKvEntry,
                         OpInsertHandle *insertHandle,
                         Dht *dht,
                         TransportPageType bufType,
                         DemystifyMgr::Op op)
{
    Status status = StatusOk;
    bool pageAllocated = false;
    Config *config = Config::get();

    const unsigned myNodeId = config->getMyNodeId();
    const unsigned numNodes = config->getActiveNodes();
    NodeId dstNodeIds[numNodes];
    unsigned numDstNodeIds = 0;

    XdbMeta *dstMeta = XdbMgr::xdbGetMeta(insertHandle->loadInfo->dstXdb);

    TransportPageMgr *tpMgr = TransportPageMgr::get();

    // if we don't have a demystification handle, we are indexing a dataset
    bool indexDataset = (demystifyPagesHandle == NULL);

    // XXX: only hash based on the first key
    // must supply a key if not using Random or Broadcast hash
    assert(numKeys > 0 || dht->ordering == Random || dht->broadcast);

    if (dht->broadcast) {
        // send to all nodes
        numDstNodeIds = numNodes;
        for (NodeId ii = 0; ii < numNodes; ii++) {
            dstNodeIds[ii] = ii;
        }
    } else if (dht->ordering == Random) {
        if (indexDataset) {
            // random just hashes to our node for datasets
            dstNodeIds[0] = myNodeId;
        } else {
            // for tables scatter the rows across the cluster
            dstNodeIds[0] = DhtMgr::get()->getNextNodeId();
        }
        numDstNodeIds = 1;
    } else if (likely(keyValid[0])) {
        dstNodeIds[0] =
            DataFormat::hashValueToNodeId(dht, keyValue[0], keyType[0]);
        assert(dstNodeIds[0] < numNodes);

        numDstNodeIds = 1;
    } else {
        // if we want descending, send all FNFs to the last node
        if (dstMeta->keyOrderings[0] & DescendingFlag) {
            dstNodeIds[0] = numNodes - 1;
        } else {
            dstNodeIds[0] = 0;
        }
        numDstNodeIds = 1;
    }

    // If we are local, insert directly into the xdb
    for (unsigned ii = 0; ii < numDstNodeIds; ii++) {
        NodeId dstNodeId = dstNodeIds[ii];
        if (dstNodeId == myNodeId) {
            status = opPopulateInsertHandle(insertHandle, dstKvEntry);
            if (status != StatusOk) {
                return status;
            }
        } else {
            do {
                pageAllocated = false;
                if (scalarPages[dstNodeId] == NULL) {
                    // this is the second level of index, use a different
                    // type of transport page than the first level
                    status = scalarPagesHandle
                                 ->allocQuotaTransportPage(bufType,
                                                           scalarPages,
                                                           demystifyPages,
                                                           demystifyPagesHandle,
                                                           numNodes,
                                                           dstNodeId);
                    BailIfFailed(status);

                    assert(scalarPages[dstNodeId] != NULL);

                    pageAllocated = true;
                }

                if (unlikely(scalarPages[dstNodeId]->dstXdbId == XidInvalid)) {
                    scalarPagesHandle->initPage(scalarPages[dstNodeId],
                                                dstNodeId,
                                                dstKvEntry->kvMeta_->tupMeta_,
                                                op);
                }

                status =
                    scalarPagesHandle->insertKv(&scalarPages[dstNodeId],
                                                dstKvEntry->kvMeta_->tupMeta_,
                                                &dstKvEntry->tuple_);

                // If our row won't fit on a new page, we've got a problem and
                // we don't want to endlessly allocate transport pages
                // XXX Eventually need an option to skip rows that are too big
                if (pageAllocated == true && status == StatusNoData) {
                    status = StatusMaxRowSizeExceeded;
                }

                if (status == StatusNoData) {
                    continue;
                }

                if (status != StatusOk) {
                    tpMgr->freeTransportPage(scalarPages[dstNodeId]);
                    scalarPages[dstNodeId] = NULL;
                    goto CommonExit;
                }
            } while (status == StatusNoData);
        }
    }

CommonExit:
    return status;
}

Status
Operators::indexRow(OpMeta *opMeta,
                    TransportPage **scalarPages,
                    TransportPage **demystifyPages,
                    TransportPageType bufType,
                    NewKeyValueEntry *srcKvEntry,
                    DfFieldValue *keys,
                    bool *keyValid,
                    const DfFieldType *keyTypesIn,
                    OpInsertHandle *insertHandle)
{
    Status status = StatusOk;
    unsigned numKeys = opMeta->indexOnRowNum ? 0 : opMeta->numVariables;
    DfFieldType keyTypes[numKeys];
    XdbMeta *dstMeta = opMeta->dstMeta;
    NewKeyValueEntry dstKvEntry(&dstMeta->kvNamedMeta.kvMeta_);
    XdbMgr *xdbMgr = XdbMgr::get();
    const NewTupleMeta *srcTupMeta = srcKvEntry->kvMeta_->tupMeta_;
    const NewTupleMeta *dstTupMeta = dstKvEntry.kvMeta_->tupMeta_;
    size_t dstNumFields = dstTupMeta->getNumFields();

    if (opMeta->opKvEntryCopyMapping &&
        !opMeta->opKvEntryCopyMapping->isReplica) {
        // sets up everything except the new key
        Operators::shallowCopyKvEntry(&dstKvEntry,
                                      dstNumFields,
                                      srcKvEntry,
                                      opMeta->opKvEntryCopyMapping,
                                      0,
                                      opMeta->opKvEntryCopyMapping->numEntries);
    } else {
        srcKvEntry->tuple_.cloneTo(srcTupMeta, &dstKvEntry.tuple_);
    }

    for (unsigned ii = 0; ii < numKeys; ii++) {
        int keyIdx = dstMeta->keyAttr[ii].valueArrayIndex;

        if (keyValid[ii]) {
            if (keyTypesIn[ii] == DfScalarObj) {
                Scalar *scalar = keys[ii].scalarVal;
                assert(scalar != NULL);

                // need to extract the actual value
                keyTypes[ii] = scalar->fieldType;
                status = scalar->getValue(&keys[ii]);
                assert(status == StatusOk);
            } else {
                keyTypes[ii] = keyTypesIn[ii];
            }

            // treat Null as FNF for index
            if (keyTypes[ii] == DfNull) {
                continue;
            }

            if (unlikely(dstMeta->keyAttr[ii].type == DfUnknown ||
                         dstMeta->keyAttr[ii].type == DfScalarObj)) {
                status = xdbMgr->xdbSetKeyType(dstMeta, keyTypes[ii], ii);
                BailIfFailed(status);
            }

            assert(dstTupMeta->getFieldType(keyIdx) != DfUnknown &&
                   dstTupMeta->getFieldType(keyIdx) != DfScalarObj);

            assert(dstMeta->keyAttr[ii].type != DfUnknown &&
                   dstMeta->keyAttr[ii].type != DfScalarObj);

            if (unlikely(dstMeta->keyAttr[ii].type != keyTypes[ii])) {
                // The types do not match
                status = StatusDfTypeMismatch;
                goto CommonExit;
            }
            dstKvEntry.tuple_.set(keyIdx,
                                  keys[ii],
                                  dstTupMeta->getFieldType(keyIdx));
        }
    }

    status = hashAndSendKv(opMeta->indexHandle,
                           scalarPages,
                           opMeta->fatptrTransPageHandle,
                           demystifyPages,
                           numKeys,
                           keyTypes,
                           keys,
                           keyValid,
                           &dstKvEntry,
                           insertHandle,
                           opMeta->dht,
                           bufType,
                           DemystifyMgr::Op::Index);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
Operators::indexProcessDemystifiedKeys(OpIndexDemystifyLocalArgs *args,
                                       NewKeyValueEntry *scalarValues)
{
    Status status;
    OpMeta *opMeta = args->opMeta;
    unsigned numKeys = opMeta->numVariables;
    bool retIsValid;
    DfFieldValue key = scalarValues->getKey(&retIsValid);
    assert(retIsValid);
    OpRowMeta *rowMeta = (OpRowMeta *) key.opRowMetaVal;

    status = populateRowMetaWithScalars(rowMeta->scalars,
                                        numKeys,
                                        scalarValues,
                                        args->scalarPile);
    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    uint32_t numIssuedRemaining = atomicDec32(&rowMeta->numIssued);
    if (numIssuedRemaining > 0) {
        // Sweep all problems for the last guy to handle
        return status;
    }

    // Last guy - process whatever's remaining.
    assert(numIssuedRemaining == 0);

    if (status == StatusOk) {
        DfFieldValue keys[numKeys];
        bool keyValid[numKeys];
        DfFieldType keyTypes[numKeys];

        // Get srcEntry from rowMeta
        NewKeyValueEntry srcKvEntry(rowMeta->opCursor.kvMeta_);

        status = rowMeta->opCursor.getNext(&srcKvEntry);
        BailIfFailed(status);

        for (unsigned ii = 0; ii < numKeys; ii++) {
            keys[ii].scalarVal = rowMeta->scalars[ii];
            keyValid[ii] = rowMeta->scalars[ii] != NULL;
            keyTypes[ii] = DfScalarObj;
        }

        status = indexRow(args->opMeta,
                          args->scalarPages,
                          args->demystifyPages,
                          args->bufType,
                          &srcKvEntry,
                          keys,
                          keyValid,
                          keyTypes,
                          args->insertHandle);
        BailIfFailed(status);
    }

CommonExit:
    assert(numIssuedRemaining == 0);

    putRowMeta(rowMeta, numKeys);

    if (unlikely(status != StatusOk)) {
        atomicWrite64(&opMeta->status, status.code());
    }

    return status;
}

Status
Operators::operatorsFilterAndMapWork(XcalarEvalClass1Ast *asts,
                                     Xdb *dstXdb,
                                     unsigned numVariables,
                                     NewKeyValueEntry *kvEntry,
                                     OpMeta *opMeta,
                                     MemoryPile **scalarPile,
                                     bool containsUdf,
                                     OpChildShmContext *childContext,
                                     OpEvalErrorStats *errorStats,
                                     OpInsertHandle *insertHandle)
{
    Status status;
    bool retIsValid;
    DfFieldValue key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    OpRowMeta *rowMeta = (OpRowMeta *) key.opRowMetaVal;

    status = populateRowMetaWithScalars(rowMeta->scalars,
                                        numVariables,
                                        kvEntry,
                                        scalarPile);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    uint32_t numIssuedRemaining = atomicDec32(&rowMeta->numIssued);
    if (numIssuedRemaining > 0) {
        // Sweep all problems for the last guy to handle
        return status;
    }

    // Last guy - process whatever's remaining.
    assert(numIssuedRemaining == 0);
    bool putRow = true;
    if (status == StatusOk) {
        // Once all the required values in a row has been demystified,
        // we perform the actual xcalarEval
        if (containsUdf) {
            status = udfExecFilterOrMap(opMeta, rowMeta, childContext, dstXdb);
            if (status == StatusOk) {
                // still need the rowMeta,
                // destroyed in processChildShmResponse
                putRow = false;
            }
        } else {
            // Get srcEntry from rowMeta
            NewKeyValueEntry srcKvEntry(rowMeta->opCursor.kvMeta_);

            status = rowMeta->opCursor.getNext(&srcKvEntry);

            if (status == StatusOk) {
                status = executeFilterOrMap(asts,
                                            dstXdb,
                                            &srcKvEntry,
                                            rowMeta->scalars,
                                            insertHandle,
                                            opMeta);
                for (unsigned ii = 0; ii < opMeta->numAggregateResults; ii++) {
                    // Don't want to free these
                    rowMeta->scalars[opMeta->aggregateResults[ii].variableIdx] =
                        NULL;
                }
            }
        }
        if (status != StatusOk) {
            atomicWrite64(&opMeta->status, status.code());
        }
    }

    if (putRow) {
        putRowMeta(rowMeta, numVariables);
    }

    return status;
}

Status
Operators::insertIntoScalarTable(Scalar **scalars,
                                 DfFieldValue key,
                                 bool keyValid,
                                 OpMeta *opMeta,
                                 NewKeyValueEntry *dstKvEntry,
                                 OpInsertHandle *insertHandle,
                                 XcalarEvalClass1Ast *ast)
{
    Status status = StatusOk;
    unsigned ii, jj;
    unsigned numVariables = opMeta->numVariables;
    XdbMeta *dstMeta = XdbMgr::xdbGetMeta(insertHandle->loadInfo->dstXdb);
    const NewTupleMeta *dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    int keyIdx = dstMeta->keyAttr[0].valueArrayIndex;
    XcalarEval *eval = XcalarEval::get();

    if (ast) {
        for (jj = 0; jj < ast->astCommon.numScalarVariables; jj++) {
            int idx = opMeta->evalArgIndices[0][jj];

            if (scalars[idx] == NULL || scalars[idx]->fieldUsedSize == 0) {
                ast->astCommon.scalarVariables[jj].content = NULL;
            } else {
                ast->astCommon.scalarVariables[jj].content = scalars[idx];
            }
        }

        DfFieldValue evalResult;
        DfFieldType resultType;
        status = eval->eval(ast, &evalResult, &resultType, opMeta->icvMode);
        if (eval->isFatalError(status)) {
            goto CommonExit;
        }

        if (status != StatusOk ||
            !XcalarEval::isEvalResultTrue(evalResult, resultType)) {
            // filter evaluated to false, skip this row
            status = StatusOk;
            goto CommonExit;
        }
    }

    dstKvEntry->init();
    for (ii = 0; ii < numVariables; ii++) {
        if (scalars[ii] != NULL && scalars[ii]->fieldUsedSize != 0 &&
            scalars[ii]->fieldType != DfNull &&
            dstTupMeta->getFieldType(ii) != DfUnknown) {
            if (dstTupMeta->getFieldType(ii) == DfScalarObj) {
                DfFieldValue value;
                value.scalarVal = scalars[ii];
                dstKvEntry->tuple_.set(ii, value, DfScalarObj);
            } else {
                if (scalars[ii]->fieldType != dstTupMeta->getFieldType(ii)) {
                    status =
                        scalars[ii]->convertType(dstTupMeta->getFieldType(ii));
                    if (status != StatusOk) {
                        continue;
                    }
                }
                DfFieldValue value;
                status = scalars[ii]->getValue(&value);
                if (status != StatusOk) {
                    continue;
                }
                dstKvEntry->tuple_.set(ii, value, dstTupMeta->getFieldType(ii));
            }
        }
    }

    if (keyIdx != NewTupleMeta::DfInvalidIdx && keyValid) {
        dstKvEntry->tuple_.set(keyIdx, key, dstTupMeta->getFieldType(keyIdx));
    }

    status = opPopulateInsertHandle(insertHandle, dstKvEntry);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
Operators::operatorsCreateScalarTableWork(Xdb *dstXdb,
                                          unsigned numVariables,
                                          NewKeyValueEntry *kvEntry,
                                          OpMeta *opMeta,
                                          MemoryPile **scalarPile,
                                          XcalarEvalClass1Ast *ast,
                                          OpInsertHandle *insertHandle)
{
    Status status = StatusUnknown;
    bool retIsValid;
    DfFieldValue key = kvEntry->getKey(&retIsValid);
    assert(retIsValid);
    OpScalarTableRowMeta *rowMeta = (OpScalarTableRowMeta *) key.opRowMetaVal;

    status = populateRowMetaWithScalars(rowMeta->scalars,
                                        numVariables,
                                        kvEntry,
                                        scalarPile);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    uint32_t numIssuedRemaining = atomicDec32(&rowMeta->numIssued);
    if (numIssuedRemaining > 0) {
        // Sweep all problems for the last guy to handle
        return status;
    }

    // Last guy logic
    assert(numIssuedRemaining == 0);
    if (status == StatusOk) {
        // Once all the required values in a row has been demystified,
        // we can insert the row into the demystified table
        NewKeyValueEntry dstKvEntry(&opMeta->dstMeta->kvNamedMeta.kvMeta_);

        status = insertIntoScalarTable(rowMeta->scalars,
                                       rowMeta->key,
                                       rowMeta->keyValid,
                                       opMeta,
                                       &dstKvEntry,
                                       insertHandle,
                                       ast);
        if (status != StatusOk) {
            atomicWrite64(&opMeta->status, status.code());
        }
    }

    putScalarTableRowMeta(rowMeta, numVariables);

    return status;
}

void
Operators::updateOpStatusForEval(OpEvalErrorStats *errorStats,
                                 OpEvalErrorStats *evalErrorStats)
{
    // XDF related errors
    // XXX: add assert to validate numTotal == sum of the broken down counters
    if (errorStats->evalXdfErrorStats.numTotal != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numTotalAtomic,
                    errorStats->evalXdfErrorStats.numTotal);
    }

    if (errorStats->evalXdfErrorStats.numUnsubstituted != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numUnsubstitutedAtomic,
                    errorStats->evalXdfErrorStats.numUnsubstituted);
    }

    if (errorStats->evalXdfErrorStats.numUnspportedTypes != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numUnspportedTypesAtomic,
                    errorStats->evalXdfErrorStats.numUnspportedTypes);
    }

    if (errorStats->evalXdfErrorStats.numMixedTypeNotSupported != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats
                         .numMixedTypeNotSupportedAtomic,
                    errorStats->evalXdfErrorStats.numMixedTypeNotSupported);
    }

    if (errorStats->evalXdfErrorStats.numEvalCastError != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numEvalCastErrorAtomic,
                    errorStats->evalXdfErrorStats.numEvalCastError);
    }

    if (errorStats->evalXdfErrorStats.numDivByZero != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numDivByZeroAtomic,
                    errorStats->evalXdfErrorStats.numDivByZero);
    }

    if (errorStats->evalXdfErrorStats.numMiscError != 0) {
        atomicAdd64(&evalErrorStats->evalXdfErrorStats.numMiscErrorAtomic,
                    errorStats->evalXdfErrorStats.numMiscError);
    }

    // UDF related errors
    if (errorStats->evalUdfErrorStats.numEvalUdfError != 0) {
        atomicAdd64(&evalErrorStats->evalUdfErrorStats.numEvalUdfErrorAtomic,
                    errorStats->evalUdfErrorStats.numEvalUdfError);
    }
}

Status
Operators::processIndexPage(TransportPage *scalarPage)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    NewTupleMeta *tupleMeta = &scalarPage->tupMeta;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    Xdb *dstXdb = NULL;
    XdbMeta *dstMeta = NULL;

    if (scalarPage->dstNodeTxnCookie != OpTxnCookieInvalid) {
        Operators::PerTxnInfo *perTxnInfo =
            (Operators::PerTxnInfo *) scalarPage->dstNodeTxnCookie;
        dstXdb = perTxnInfo->dstXdb;
        dstMeta = perTxnInfo->dstMeta;
    } else {
        status = xdbMgr->xdbGet(scalarPage->dstXdbId, &dstXdb, &dstMeta);
        assert(status == StatusOk);
    }

    NewKeyValueMeta kvMeta((const NewTupleMeta *) tupleMeta,
                           dstMeta->keyAttr[0].valueArrayIndex);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewTuplesCursor cursor(scalarPage->tupBuf);

    xdbMgr->xdbIncRecvTransPage(dstXdb);

    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               XdbInsertRandomHash);
    BailIfFailed(status);
    insertHandleInited = true;

    // XXX We want to just stick in the Transport page directly into the XDB.
    // This means that XDB page and Transport page sizes in addition to other
    // attributes of these pages have to match. This is non-trival since we
    // need to re-work the Transport page memory life cycle.
    //
    // As a staging option,  we will iterate the Transport page and construct
    // XDB pages and also track the minKey and maxKey along the way.

    while ((status = cursor.getNext((NewTupleMeta *) kvMeta.tupMeta_,
                                    &kvEntry.tuple_)) == StatusOk) {
        status = opPopulateInsertHandle(&insertHandle, &kvEntry);
        BailIfFailed(status);
    }
    status = StatusOk;

CommonExit:
    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    return status;
}

Status
IndexDemystifyRecv::processScalarPage(TransportPage *scalarPage, void *context)
{
    Status status = StatusOk;
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) scalarPage->dstNodeTxnCookie;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    TransportPage **scalarPages = (TransportPage **) context;

    OpMeta *opMeta = (OpMeta *) demystifyInput->opMeta;
    MemoryPile *scalarPile = NULL;

    OpInsertHandle insertHandle;
    bool insertHandleInited = false;

    XdbMeta *dstMeta = perTxnInfo->dstMeta;
    Operators *op = Operators::get();

    NewKeyValueMeta kvMeta((NewTupleMeta *) &scalarPage->tupMeta,
                           demystifyInput->numVariables);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewTuplesCursor cursor(scalarPage->tupBuf);

    OpIndexDemystifyLocalArgs localArgs(opMeta,
                                        scalarPages,
                                        NULL,
                                        TransportPageType::RecvPage,
                                        NULL,
                                        NULL,
                                        &scalarPile,
                                        &insertHandle);

    assert(opMeta->fatptrDemystRequired == true);

    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    scalarPile = MemoryPile::allocPile(Txn::currentTxn().id_,
                                       &status,
                                       1,
                                       BcHandle::BcScanCleanoutToFree);
    BailIfFailed(status);

    // TP packed with scalars, so packing is variable size.
    while ((status = cursor.getNextVar((NewTupleMeta *) kvMeta.tupMeta_,
                                       &kvEntry.tuple_)) == StatusOk) {
        status = op->indexProcessDemystifiedKeys(&localArgs, &kvEntry);
        BailIfFailed(status);
    }

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (scalarPile != NULL) {
        scalarPile->markPileAllocsDone();
    }

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }
    return status;
}

Status
FilterAndMapDemystifyRecv::processScalarPage(TransportPage *scalarPage,
                                             void *context)
{
    Status status = StatusOk;
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) scalarPage->dstNodeTxnCookie;
    PerOpGlobalInput *perOpGlobalIp = &perTxnInfo->perOpGlobIp;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    OpMeta *opMeta = (OpMeta *) demystifyInput->opMeta;
    XcalarEvalClass1Ast *asts = NULL;
    bool astsGenerated[opMeta->numEvals];
    memZero(astsGenerated, sizeof(astsGenerated));
    char **evalStrings;
    uint64_t numRecords = 0;
    OpEvalErrorStats *errorStats = NULL;

    OpChildShmContext childContext;
    bool childInited = false;
    bool containsUdf = false;

    NewKeyValueMeta kvMeta((const NewTupleMeta *) &scalarPage->tupMeta,
                           demystifyInput->numVariables);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewTuplesCursor cursor(scalarPage->tupBuf);

    MemoryPile *scalarPile = NULL;

    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    Xdb *dstXdb = perTxnInfo->dstXdb;
    XdbMeta *dstMeta = perTxnInfo->dstMeta;

    OpStatus *opStatus = opMeta->opStatus;
    XcalarEval *xcalarEval = XcalarEval::get();
    Operators *op = Operators::get();

    asts = (XcalarEvalClass1Ast *) memAlloc(sizeof(*asts) * opMeta->numEvals);
    BailIfNull(asts);

    assert(opMeta->fatptrDemystRequired == true);

    assert(opMeta->txn == Txn::currentTxn());

    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    evalStrings = opMeta->evalStrings;

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        status = xcalarEval->generateClass1Ast(evalStrings[ii], &asts[ii]);
        BailIfFailed(status);
        astsGenerated[ii] = true;

        if (!containsUdf) {
            containsUdf = xcalarEval->containsUdf(asts[ii].astCommon.rootNode);
        }
    }

    if (containsUdf) {
        // do initialization needed for UDF here
        status = op->initChildShm(opMeta,
                                  demystifyInput->numVariables,
                                  perOpGlobalIp->srcXdbId,
                                  opMeta->numEvals,
                                  evalStrings,
                                  asts,
                                  &childContext);
        BailIfFailed(status);
        childInited = true;
    }

    errorStats = (OpEvalErrorStats *) memAlloc(sizeof(OpEvalErrorStats));
    BailIfNull(errorStats);
    memset(errorStats, 0, sizeof(OpEvalErrorStats));

    scalarPile = MemoryPile::allocPile(Txn::currentTxn().id_,
                                       &status,
                                       1,
                                       BcHandle::BcScanCleanoutToFree);
    BailIfFailed(status);

    // TP packed with scalars, so packing is variable size.
    while ((status = cursor.getNextVar((NewTupleMeta *) kvMeta.tupMeta_,
                                       &kvEntry.tuple_)) == StatusOk) {
        numRecords++;

        status = op->operatorsFilterAndMapWork(asts,
                                               dstXdb,
                                               demystifyInput->numVariables,
                                               &kvEntry,
                                               opMeta,
                                               &scalarPile,
                                               containsUdf,
                                               &childContext,
                                               errorStats,
                                               &insertHandle);
        BailIfFailed(status);
    }

    Operators::get()->updateOpStatusForEval(errorStats,
                                            &opStatus->atomicOpDetails
                                                 .errorStats.evalErrorStats);

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == scalarPage->tupBuf->getNumTuples());
        status = StatusOk;
    }

    if (errorStats != NULL) {
        memFree(errorStats);
        errorStats = NULL;
    }

    if (childInited) {
        auto *input = childContext.input;
        assert(input != NULL);

        if (likely(input->tupBuf->getNumTuples() > 0)) {
            // Send partially filled input.
            status =
                op->udfSendToChildForEval(input, &childContext, opMeta, dstXdb);
        }
    }

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    if (scalarPile != NULL) {
        scalarPile->markPileAllocsDone();
    }

    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    if (childInited) {
        op->freeChildShm(&childContext);
    }

    if (asts != NULL) {
        for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
            if (astsGenerated[ii]) {
                // scratchPadScalars have already been freed
                xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                xcalarEval->destroyClass1Ast(&asts[ii]);
                astsGenerated[ii] = false;
            }
        }

        memFree(asts);
        asts = NULL;
    }

    return status;
}

void
Operators::substituteAggregateResultsIntoAst2(
    XcalarEvalClass2Ast *ast,
    OperatorsAggregateResult aggregateResults[],
    unsigned numAggregateResults)
{
    unsigned ii, jj;

    for (ii = 0; ii < numAggregateResults; ii++) {
        for (jj = 0; jj < ast->astCommon.numScalarVariables; jj++) {
            if (strcmp(aggregateResults[ii].aggName,
                       ast->astCommon.scalarVariables[jj].variableName) == 0) {
                ast->astCommon.scalarVariables[jj].content =
                    aggregateResults[ii].result;
            }
        }
    }
}

Status
GroupByDemystifyRecv::processScalarPage(TransportPage *scalarPage,
                                        void *context)
{
    Status status = StatusOk;
    XcalarEval *xcalarEval = XcalarEval::get();
    Xdb *scratchPadXdb;
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) scalarPage->dstNodeTxnCookie;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    OpMeta *opMeta = (OpMeta *) demystifyInput->opMeta;
    Scalar *results[opMeta->numEvals];
    memZero(results, sizeof(results));

    XdbMeta *srcMeta = perTxnInfo->srcMeta;
    MemoryPile *scalarPile = NULL;
    uint64_t numRecords = 0;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    Xdb *dstXdb = perTxnInfo->dstXdb;
    XdbMeta *dstMeta = perTxnInfo->dstMeta;
    Operators *op = Operators::get();
    XcalarEvalClass2Ast *asts = NULL;
    bool astsGenerated[opMeta->numEvals];
    memZero(astsGenerated, sizeof(astsGenerated));
    XdbMgr *xdbMgr = XdbMgr::get();

    NewKeyValueMeta kvMeta((const NewTupleMeta *) &scalarPage->tupMeta,
                           demystifyInput->numVariables);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewTuplesCursor cursor(scalarPage->tupBuf);

    scratchPadXdb = xdbMgr->xdbGetScratchPadXdb(dstXdb);

    asts = (XcalarEvalClass2Ast *) memAlloc(sizeof(*asts) * opMeta->numEvals);
    BailIfNull(asts);

    assert(opMeta->fatptrDemystRequired == true);

    assert(opMeta->txn == Txn::currentTxn());
    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        status = xcalarEval->generateClass2Ast(opMeta->evalStrings[ii],
                                               &asts[ii],
                                               NULL,
                                               opMeta->dag->getId(),
                                               opMeta->dstXdbId);
        BailIfFailed(status);
        astsGenerated[ii] = true;

        op->substituteAggregateResultsIntoAst2(&asts[ii],
                                               opMeta->aggregateResults,
                                               opMeta->numAggregateResults);

        status =
            xcalarEval->updateArgMappings(asts[ii].astCommon.rootNode,
                                          xdbMgr->xdbGetMeta(scratchPadXdb));
        BailIfFailed(status);
    }

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        results[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        BailIfNull(results[ii]);
    }

    scalarPile = MemoryPile::allocPile(Txn::currentTxn().id_,
                                       &status,
                                       1,
                                       BcHandle::BcScanCleanoutToFree);
    BailIfFailed(status);

    // TP packed with scalars, so packing is variable size.
    while ((status = cursor.getNextVar((NewTupleMeta *) kvMeta.tupMeta_,
                                       &kvEntry.tuple_)) == StatusOk) {
        numRecords++;

        status = op->operatorsGroupByWork(asts,
                                          dstXdb,
                                          dstMeta,
                                          scratchPadXdb,
                                          srcMeta,
                                          demystifyInput->numVariables,
                                          &kvEntry,
                                          opMeta,
                                          &scalarPile,
                                          demystifyInput->includeSrcTableSample,
                                          results,
                                          &insertHandle);
        BailIfFailed(status);
    }
    assert(status == StatusNoData);

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == scalarPage->tupBuf->getNumTuples());
        status = StatusOk;
    }

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    if (scalarPile != NULL) {
        scalarPile->markPileAllocsDone();
    }

    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    xdbMgr->xdbReleaseScratchPadXdb(scratchPadXdb);

    if (asts != NULL) {
        for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
            if (astsGenerated[ii]) {
                xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                xcalarEval->destroyClass2Ast(&asts[ii]);
                astsGenerated[ii] = false;
            }
        }

        memFree(asts);
        asts = NULL;
    }

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        if (results[ii]) {
            Scalar::freeScalar(results[ii]);
            results[ii] = NULL;
        }
    }

    return status;
}

Status
CreateScalarTableDemystifyRecv::processScalarPage(TransportPage *scalarPage,
                                                  void *context)
{
    Status status;

    MemoryPile *scalarPile = NULL;
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) scalarPage->dstNodeTxnCookie;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    OpMeta *opMeta = (OpMeta *) demystifyInput->opMeta;
    uint64_t numRecords = 0;
    OpInsertHandle insertHandle;
    bool insertHandleInited = false;
    Operators *op = Operators::get();
    Xdb *dstXdb = perTxnInfo->dstXdb;
    XdbMeta *dstMeta = perTxnInfo->dstMeta;
    XcalarEvalClass1Ast *ast = NULL;
    bool astInited = false;
    XcalarEval *eval = XcalarEval::get();

    NewKeyValueMeta kvMeta((const NewTupleMeta *) &scalarPage->tupMeta,
                           demystifyInput->numVariables);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewTuplesCursor cursor(scalarPage->tupBuf);

    if (opMeta->op == OperatorsSynthesizeTable && opMeta->numEvals > 0) {
        assert(opMeta->numEvals == 1);
        ast = (XcalarEvalClass1Ast *) memAlloc(sizeof(*ast));
        BailIfNull(ast);

        status = eval->generateClass1Ast(opMeta->evalStrings[0], ast);
        BailIfFailed(status);
        astInited = true;
    }

    assert(opMeta->fatptrDemystRequired == true);
    assert(opMeta->txn == Txn::currentTxn());

    status = opGetInsertHandle(&insertHandle,
                               &dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    scalarPile = MemoryPile::allocPile(Txn::currentTxn().id_,
                                       &status,
                                       1,
                                       BcHandle::BcScanCleanoutToFree);
    BailIfFailed(status);

    // TP packed with scalars, so packing is variable size.
    while ((status = cursor.getNextVar((NewTupleMeta *) kvMeta.tupMeta_,
                                       &kvEntry.tuple_)) == StatusOk) {
        numRecords++;
        status =
            op->operatorsCreateScalarTableWork(dstXdb,
                                               demystifyInput->numVariables,
                                               &kvEntry,
                                               opMeta,
                                               &scalarPile,
                                               ast,
                                               &insertHandle);
        BailIfFailed(status);
    }

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == scalarPage->tupBuf->getNumTuples());
        status = StatusOk;
    }

    if (insertHandleInited) {
        opPutInsertHandle(&insertHandle);
    }

    if (scalarPile != NULL) {
        scalarPile->markPileAllocsDone();
    }

    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    if (astInited) {
        // scratchPadScalars have already been freed
        eval->dropScalarVarRef(&ast->astCommon);
        eval->destroyClass1Ast(ast);
        astInited = false;
    }

    if (ast) {
        memFree(ast);
        ast = NULL;
    }

    return status;
}

Status
OperatorsEvalFnWork::doWork()
{
    OperatorsEvalFnWork *operatorsWork = this;
    int argc;
    Status status = StatusUnknown;
    Scalar **argv = NULL;
    Scalar **scratchPadScalars = NULL;
    unsigned numScratchPadScalars = 0;
    Xdb *outputXdb;
    XdbMeta *prevXdbMeta, *outputXdbMeta;
    ScalarGroupIter *groupIter;
    TableCursor srcCursor;
    NewKeyValueEntry *kvEntry = NULL;
    bool srcCursorInited = false;
    XdbId outputScalarXdbId;
    XcalarEvalRegisteredFn *registeredFn;
    int64_t rowNum, numRecords, slotId;
    Scalar *outputScalar = NULL;
    OpStatus *opStatus = NULL;
    XcalarEval *xcalarEval = XcalarEval::get();
    Operators *operators = Operators::get();
    OpEvalErrorStats *evalErrorStats = NULL;

    groupIter = operatorsWork->groupIter;
    outputScalarXdbId = operatorsWork->dstXdbId;
    rowNum = operatorsWork->startRecord;
    slotId = operatorsWork->slotId;
    registeredFn = operatorsWork->registeredFn;
    opStatus = operatorsWork->opStatus;

    assert(groupIter != NULL);
    assert(outputScalarXdbId != XdbIdInvalid);
    assert(groupIter->cursorType == ScalarLocalCursor);

    XdbMgr *xdbMgr = XdbMgr::get();
    status = xdbMgr->xdbGet(groupIter->srcXdbId, NULL, &prevXdbMeta);
    BailIfFailed(status);
    assert(prevXdbMeta != NULL);

    kvEntry =
        new (std::nothrow) NewKeyValueEntry(&prevXdbMeta->kvNamedMeta.kvMeta_);
    BailIfNull(kvEntry);

    status = xdbMgr->xdbGet(outputScalarXdbId, &outputXdb, &outputXdbMeta);
    BailIfFailed(status);
    assert(outputXdb != NULL);
    assert(outputXdbMeta != NULL);

    argv =
        (Scalar **) memAllocExt(groupIter->numArgs * sizeof(*argv), moduleName);
    BailIfNull(argv);

    scratchPadScalars =
        (Scalar **) memAllocExt(groupIter->numArgs * sizeof(*scratchPadScalars),
                                moduleName);
    BailIfNull(scratchPadScalars);

    for (numScratchPadScalars = 0; numScratchPadScalars < groupIter->numArgs;
         numScratchPadScalars++) {
        scratchPadScalars[numScratchPadScalars] = NULL;
    }

    for (numScratchPadScalars = 0; numScratchPadScalars < groupIter->numArgs;
         numScratchPadScalars++) {
        scratchPadScalars[numScratchPadScalars] =
            Scalar::allocScalar(DfMaxFieldValueSize);
        BailIfNull(scratchPadScalars[numScratchPadScalars]);
    }

    status = CursorManager::get()->createOnSlot(groupIter->srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);

    outputScalar = Scalar::allocScalar(DfMaxFieldValueSize);
    if (outputScalar == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    srcCursorInited = true;
    argc = groupIter->numArgs;

    evalErrorStats =
        (OpEvalErrorStats *) memAllocExt(sizeof(OpEvalErrorStats), moduleName);
    BailIfNull(evalErrorStats);

    memset(evalErrorStats, 0, sizeof(OpEvalErrorStats));

    numRecords = 0;
    unsigned workCounter;
    workCounter = 0;
    while (numRecords < operatorsWork->numRecords &&
           ((status = srcCursor.getNext(kvEntry)) == StatusOk)) {
        numRecords++;
        workCounter++;
        status = xcalarEval->evalAndInsert(argc,
                                           argv,
                                           scratchPadScalars,
                                           kvEntry,
                                           groupIter,
                                           registeredFn,
                                           prevXdbMeta,
                                           outputXdbMeta,
                                           outputXdb,
                                           outputScalar,
                                           slotId,
                                           evalErrorStats);
        BailIfFailed(status);

        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }
    }

    operators->updateOpStatusForEval(evalErrorStats,
                                     &opStatus->atomicOpDetails.errorStats
                                          .evalErrorStats);

    if (status == StatusNoData) {
        status = StatusOk;
    }
    BailIfFailed(status);

    // srcCursor is still pointing to an xdbPage in the slot we're about to
    // prune So we need to destroy the cursor first
    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                XdbInsertRandomHash,
                                this->srcXdb,
                                NULL,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (argv != NULL) {
        memFree(argv);
        argv = NULL;
    }

    if (scratchPadScalars != NULL) {
        for (unsigned ii = 0; ii < numScratchPadScalars; ii++) {
            Scalar::freeScalar(scratchPadScalars[ii]);
        }
        memFree(scratchPadScalars);
        scratchPadScalars = NULL;
    }

    if (outputScalar != NULL) {
        Scalar::freeScalar(outputScalar);
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }

    if (kvEntry) {
        delete kvEntry;
        kvEntry = NULL;
    }

    if (evalErrorStats != NULL) {
        memFree(evalErrorStats);
        evalErrorStats = NULL;
    }

    return status;
}

// XXX: Does this belong here?
Status
parallelEvalLocal(ScalarGroupIter *groupIter,
                  XdbId outputScalarXdbId,
                  XcalarEvalRegisteredFn *registeredFn)
{
    Status status = StatusOk;
    Status workerStatus = StatusOk;
    Xdb *srcXdb;
    TrackHelpers *trackHelpers = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();
    DagLib *dagLib = DagLib::get();
    unsigned numWorkers = 0;
    OperatorsEvalFnWork **operatorsWorkers = NULL;

    status = xdbMgr->xdbGet(groupIter->srcXdbId, &srcXdb, NULL);
    BailIfFailed(status);

    OpStatus *opStatus;
    status = (dagLib->getDagLocal(groupIter->dagId))
                 ->getOpStatusFromXdbId(groupIter->dstXdbId, &opStatus);

    if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    uint64_t numChunks;
    numChunks = xdbMgr->xdbGetNumHashSlots(srcXdb);
    numWorkers =
        Operators::getNumWorkers(opStatus->atomicOpDetails.numWorkTotal);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    BailIfNull(trackHelpers);

    operatorsWorkers = new (std::nothrow) OperatorsEvalFnWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsEvalFnWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsEvalFnWork((ii == numWorkers - 1)
                                    ? TrackHelpers::Master
                                    : TrackHelpers::NonMaster,
                                ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->dropSrcSlots = false;
        operatorsWork->serializeSlots = false;
        operatorsWork->groupIter = groupIter;
        operatorsWork->registeredFn = registeredFn;
        operatorsWork->dstXdbId = outputScalarXdbId;
        operatorsWork->opStatus = opStatus;
        operatorsWork->srcXdb = srcXdb;
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->op = OperatorsEvalFn;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned jj = 0; jj < numWorkers; jj++) {
            if (operatorsWorkers[jj]) {
                delete operatorsWorkers[jj];
                operatorsWorkers[jj] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    return status;
}

Status
OperatorsAggFnWork::doWork()
{
    OperatorsAggFnWork *operatorsWork = this;
    Status status = StatusUnknown;
    AggregateContext aggContext;
    Scalar **argv = NULL;
    ScalarGroupIter *groupIter;
    XdfAggregateHandlers aggHandler;
    XdfAggregateAccumulators *acc;
    int64_t rowNum, numRecords, slotId;
    int argc;
    TableCursor srcCursor;
    NewKeyValueEntry *kvEntry = NULL;
    OpStatus *opStatus = NULL;
    bool srcCursorInited = false;
    Scalar **scratchPadScalars = NULL;
    unsigned numScratchPadScalars = 0;
    OpEvalErrorStats *evalErrorStats = NULL;

    slotId = operatorsWork->slotId;
    operatorsWork->acc = &operatorsWork->aggAccs[slotId];
    operatorsWork->accValid = &operatorsWork->aggAccsValid[slotId];

    acc = operatorsWork->acc;
    aggHandler = operatorsWork->aggHandler;
    groupIter = operatorsWork->groupIter;
    opStatus = operatorsWork->opStatus;

    rowNum = operatorsWork->startRecord;

    status = xdfLocalInit(aggHandler, acc, NULL, 0);
    assert(status == StatusOk);

    aggContext.acc = acc;
    aggContext.aggregateHandler = aggHandler;

    XdbMeta *srcMeta = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();
    Operators *operators = Operators::get();

    status = xdbMgr->xdbGet(groupIter->srcXdbId, NULL, &srcMeta);
    BailIfFailed(status);

    kvEntry =
        new (std::nothrow) NewKeyValueEntry(&srcMeta->kvNamedMeta.kvMeta_);
    BailIfNull(kvEntry);

    status = CursorManager::get()->createOnSlot(groupIter->srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;
    *operatorsWork->accValid = true;

    argv =
        (Scalar **) memAllocExt(groupIter->numArgs * sizeof(*argv), moduleName);
    if (argv == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    scratchPadScalars =
        (Scalar **) memAllocExt(groupIter->numArgs * sizeof(*scratchPadScalars),
                                moduleName);
    if (scratchPadScalars == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (numScratchPadScalars = 0; numScratchPadScalars < groupIter->numArgs;
         numScratchPadScalars++) {
        scratchPadScalars[numScratchPadScalars] = NULL;
    }

    for (numScratchPadScalars = 0; numScratchPadScalars < groupIter->numArgs;
         numScratchPadScalars++) {
        scratchPadScalars[numScratchPadScalars] =
            Scalar::allocScalar(DfMaxFieldValueSize);
        BailIfNull(scratchPadScalars[numScratchPadScalars]);
    }

    evalErrorStats =
        (OpEvalErrorStats *) memAllocExt(sizeof(OpEvalErrorStats), moduleName);
    BailIfNull(evalErrorStats);

    memset(evalErrorStats, 0, sizeof(OpEvalErrorStats));

    argc = groupIter->numArgs;
    numRecords = 0;
    unsigned workCounter;
    workCounter = 0;
    while (numRecords < operatorsWork->numRecords &&
           ((status = srcCursor.getNext(kvEntry)) == StatusOk)) {
        numRecords++;
        workCounter++;
        aggregateAndUpdate(groupIter,
                           aggHandler,
                           acc,
                           &aggContext,
                           NULL,
                           0,
                           argc,
                           argv,
                           scratchPadScalars,
                           kvEntry,
                           evalErrorStats);

        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }
    }

    operators->updateOpStatusForEval(evalErrorStats,
                                     &opStatus->atomicOpDetails.errorStats
                                          .evalErrorStats);
    if (status == StatusNoData) {
        status = StatusOk;
    }
    BailIfFailed(status);

    // srcCursor is still pointing to an xdbPage in the slot we're about to
    // prune So we need to destroy the cursor first
    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                XdbInsertRandomHash,
                                this->srcXdb,
                                NULL,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (argv != NULL) {
        memFree(argv);
        argv = NULL;
    }

    if (scratchPadScalars != NULL) {
        for (unsigned ii = 0; ii < numScratchPadScalars; ii++) {
            Scalar::freeScalar(scratchPadScalars[ii]);
        }
        memFree(scratchPadScalars);
        scratchPadScalars = NULL;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }

    if (kvEntry) {
        delete kvEntry;
        kvEntry = NULL;
    }

    if (evalErrorStats != NULL) {
        memFree(evalErrorStats);
        evalErrorStats = NULL;
    }

    return status;
}

Status
parallelAggregateLocal(ScalarGroupIter *groupIter,
                       XdfAggregateHandlers aggHandler,
                       XdfAggregateAccumulators *acc,
                       void *broadcastPacket,
                       size_t broadcastPacketSize)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    AggregateContext aggContext;
    TrackHelpers *trackHelpers = NULL;
    XdfAggregateAccumulators *aggAccs = NULL;
    uint64_t numChunks;
    bool *aggAccsValid = NULL;
    unsigned numWorkers = 0;
    DagLib *dagLib = DagLib::get();
    OperatorsAggFnWork **operatorsWorkers = NULL;

    aggContext.acc = acc;
    aggContext.aggregateHandler = aggHandler;

    Xdb *srcXdb;
    XdbMgr *xdbMgr = XdbMgr::get();
    status = xdbMgr->xdbGet(groupIter->srcXdbId, &srcXdb, NULL);
    BailIfFailed(status);

    OpStatus *opStatus;
    status = (dagLib->getDagLocal(groupIter->dagId))
                 ->getOpStatusFromXdbId(groupIter->dstXdbId, &opStatus);
    if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    numChunks = xdbMgr->xdbGetNumHashSlots(srcXdb);

    aggAccsValid = new (std::nothrow) bool[numChunks];
    BailIfNull(aggAccsValid);
    for (unsigned ii = 0; ii < numChunks; ii++) {
        aggAccsValid[ii] = false;
    }
    aggAccs =
        (XdfAggregateAccumulators *) memAllocExt(sizeof(*aggAccs) * numChunks,
                                                 moduleName);

    if (aggAccs == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    numWorkers =
        Operators::getNumWorkers(opStatus->atomicOpDetails.numWorkTotal);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    if (trackHelpers == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    operatorsWorkers = new (std::nothrow) OperatorsAggFnWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsAggFnWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsAggFnWork((ii == numWorkers - 1) ? TrackHelpers::Master
                                                      : TrackHelpers::NonMaster,
                               ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->dropSrcSlots = false;
        operatorsWork->serializeSlots = false;
        operatorsWork->groupIter = groupIter;
        operatorsWork->opStatus = opStatus;
        operatorsWork->aggAccs = aggAccs;
        operatorsWork->aggAccsValid = aggAccsValid;
        operatorsWork->aggHandler = aggHandler;
        operatorsWork->op = OperatorsAggFn;
        operatorsWork->srcXdb = srcXdb;
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->insertHandle.insertState = XdbInsertRandomHash;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

    for (unsigned ii = 0; ii < numChunks; ii++) {
        if (aggAccsValid[ii]) {
            xdfAggComplete(aggHandler,
                           &aggAccs[ii],
                           status,
                           aggAccs[ii].status,
                           &aggContext);
        }
    }

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }

    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (aggAccs != NULL) {
        memFree(aggAccs);
        aggAccs = NULL;
    }

    if (aggAccsValid != NULL) {
        delete[] aggAccsValid;
        aggAccsValid = NULL;
    }

    return status;
}

void
Operators::combineValueArray(NewKeyValueEntry *leftKvEntry,
                             NewKeyValueEntry *rightKvEntry,
                             unsigned numLeftEntries,
                             unsigned numRightEntries,
                             OpKvEntryCopyMapping *opKvEntryCopyMapping,
                             NewKeyValueEntry *dstKvEntry)

{
    assert(numLeftEntries <= opKvEntryCopyMapping->numEntries);
    assert(numLeftEntries + numRightEntries ==
           opKvEntryCopyMapping->numEntries);
    size_t dstNumFields = dstKvEntry->kvMeta_->tupMeta_->getNumFields();

    dstKvEntry->init();

    if (leftKvEntry != NULL) {
        shallowCopyKvEntry(dstKvEntry,
                           dstNumFields,
                           leftKvEntry,
                           opKvEntryCopyMapping,
                           0,
                           numLeftEntries);
    }

    uint64_t dstIndex = numLeftEntries;
    if (rightKvEntry != NULL) {
        shallowCopyKvEntry(dstKvEntry,
                           dstNumFields,
                           rightKvEntry,
                           opKvEntryCopyMapping,
                           dstIndex,
                           numRightEntries);
    }
}

Status
OperatorsJoinWork::combineValueArrayAndInsert(NewKeyValueEntry *leftKvEntry,
                                              NewKeyValueEntry *rightKvEntry,
                                              NewKeyValueEntry *dstKvEntry)

{
    Operators::combineValueArray(leftKvEntry,
                                 rightKvEntry,
                                 this->numLeftCopyMappingEntries,
                                 this->numRightCopyMappingEntries,
                                 this->opKvEntryCopyMapping,
                                 dstKvEntry);

    Status status = opPopulateInsertHandle(&this->insertHandle, dstKvEntry);

    return status;
}

int
OperatorsJoinWork::compareValueArray(XdbMeta *leftMeta,
                                     XdbMeta *rightMeta,
                                     NewKeyValueEntry *leftKvEntry,
                                     NewKeyValueEntry *rightKvEntry)
{
    return DataFormat::fieldArrayCompare(leftMeta->numKeys,
                                         NULL,
                                         leftMeta->keyIdxOrder,
                                         leftMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                         &leftKvEntry->tuple_,
                                         rightMeta->keyIdxOrder,
                                         rightMeta->kvNamedMeta.kvMeta_
                                             .tupMeta_,
                                         &rightKvEntry->tuple_,
                                         this->nullSafe);
}

Status
OperatorsJoinWork::evalFilter(XdbMeta *leftMeta,
                              NewKeyValueEntry *leftKvEntry,
                              NewKeyValueEntry *rightKvEntry,
                              bool *filterResult)
{
    Status status = StatusOk;

    if (this->filterCtx.astInit_) {
        size_t leftNumFields =
            leftMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        status = this->filterCtx.filterMultiAst(leftKvEntry,
                                                leftNumFields,
                                                rightKvEntry,
                                                filterResult);
        BailIfFailed(status);
    } else {
        *filterResult = true;
    }

CommonExit:
    return status;
}

Status
OperatorsJoinWork::initCursor(TableCursor *cursor,
                              bool *cursorExhausted,
                              bool *cursorInited,
                              const bool left)
{
    Status status = StatusOk;

    if ((left && this->numRecsLeft == 0) ||
        (!left && this->numRecsRight == 0)) {
        *cursorExhausted = true;
        goto CommonExit;
    }

    if (left) {
        status = CursorManager::get()->createOnSlot(this->leftXdbId,
                                                    this->slotId,
                                                    this->startRecordLeft,
                                                    PartialAscending,
                                                    cursor);
    } else {
        status = CursorManager::get()->createOnSlot(this->rightXdbId,
                                                    this->slotId,
                                                    this->startRecordRight,
                                                    PartialAscending,
                                                    cursor);
    }

    if (status != StatusOk) {
        *cursorExhausted = true;
        if (status == StatusNoData) {
            status = StatusOk;
        }
        goto CommonExit;
    }

    *cursorInited = true;

CommonExit:
    assert(*cursorExhausted && !*cursorInited ||
           !*cursorExhausted && *cursorInited);

    return status;
}

Status
OperatorsJoinWork::checkLoopStatusAndUpdate(size_t *loopCounter,
                                            size_t *workCounter)
{
    OpStatus *opStatus = this->opStatus;
    Status status = checkLoopStatus(opStatus, loopCounter, workCounter);
    if (status == StatusOk) {
        ++*loopCounter;
    }
    return status;
}

Status
OperatorsJoinWork::processEqual(TableCursor *leftCursor,
                                TableCursor *rightCursor,
                                XdbMeta *leftMeta,
                                XdbMeta *rightMeta,
                                NewKeyValueEntry *leftKvEntry,
                                NewKeyValueEntry *rightKvEntry,
                                NewKeyValueEntry *dstKvEntry,
                                NewKeyValueEntry *leftStartKvEntry,
                                TableCursor::Position *&rightStartPosition,
                                TableCursor::Position *&rightTempPosition,
                                bool *leftExhausted,
                                bool *rightExhausted,
                                AccumulatorHashTable *accHashTable,
                                size_t *loopCounter,
                                size_t *workCounter)
{
    Status status = StatusOk;

    bool isEqualLeft = true;
    bool isEqualRight = true;

    while (isEqualLeft) {
        status = rightCursor->restorePosition(rightStartPosition);
        BailIfFailed(status);
        status = rightCursor->getNext(rightKvEntry);
        assert(status == StatusOk && "page has already been seen");
        BailIfFailed(status);
        isEqualRight = true;
        bool foundOneMatch = false;

        while (isEqualRight) {
            status = checkLoopStatusAndUpdate(loopCounter, workCounter);
            BailIfFailed(status);

            bool filterResult = false;
            status =
                evalFilter(leftMeta, leftKvEntry, rightKvEntry, &filterResult);
            BailIfFailed(status);

            if (filterResult) {
                foundOneMatch = true;

                // We maintain a hash table so that at the end of the nested
                // loops we know if a right row has been inserted.
                if (this->filterCtx.astInit_ &&
                    (this->joinType == RightOuterJoin ||
                     this->joinType == FullOuterJoin)) {
                    // XXX We don't have a general xdb based hash table, using
                    // accumulator hash table for now.
                    auto accEntry =
                        accHashTable->insert(&rightKvEntry->tuple_, rightMeta);
                    BailIfNullXdb(accEntry);
                }

                if (this->joinType == LeftAntiJoin) {
                    break;
                }

                status = combineValueArrayAndInsert(leftKvEntry,
                                                    rightKvEntry,
                                                    dstKvEntry);
                BailIfFailed(status);

                if (this->joinType == LeftSemiJoin) {
                    break;
                }
            }

            rightCursor->savePosition(rightTempPosition);
            status = rightCursor->getNext(rightKvEntry);
            if (status != StatusOk && status != StatusNoData) {
                goto CommonExit;
            }
            if (status == StatusNoData) {
                isEqualRight = false;
                *rightExhausted = true;
            } else if (compareValueArray(leftMeta,
                                         rightMeta,
                                         leftStartKvEntry,
                                         rightKvEntry) != 0) {
                isEqualRight = false;
            }
        }

        if (!foundOneMatch) {
            if (this->joinType == LeftOuterJoin ||
                this->joinType == LeftAntiJoin ||
                this->joinType == FullOuterJoin) {
                status =
                    combineValueArrayAndInsert(leftKvEntry, NULL, dstKvEntry);
                BailIfFailed(status);
            }
        }

        status = leftCursor->getNext(leftKvEntry);
        if (status != StatusOk && status != StatusNoData) {
            goto CommonExit;
        }
        if (status == StatusNoData) {
            isEqualLeft = false;
            *leftExhausted = true;
        } else if (compareValueArray(leftMeta,
                                     leftMeta,
                                     leftStartKvEntry,
                                     leftKvEntry) != 0) {
            isEqualLeft = false;
            ++*workCounter;
        }
    }

    // We finished the nested loops. For right outer joins and full outer joins,
    // we insert the right rows if there wasn't a match found.
    if (this->filterCtx.astInit_ &&
        (this->joinType == RightOuterJoin || this->joinType == FullOuterJoin)) {
        status = rightCursor->restorePosition(rightStartPosition);
        BailIfFailed(status);
        status = rightCursor->getNext(rightKvEntry);
        assert(status == StatusOk && "page has already been seen");
        BailIfFailed(status);
        isEqualRight = true;

        while (isEqualRight) {
            status = checkLoopStatusAndUpdate(loopCounter, workCounter);
            BailIfFailed(status);

            if (!accHashTable->find(&rightKvEntry->tuple_, rightMeta)) {
                status =
                    combineValueArrayAndInsert(NULL, rightKvEntry, dstKvEntry);
                BailIfFailed(status);
            }

            rightCursor->savePosition(rightTempPosition);
            status = rightCursor->getNext(rightKvEntry);
            if (status != StatusOk && status != StatusNoData) {
                goto CommonExit;
            }
            if (status == StatusNoData) {
                isEqualRight = false;
                *rightExhausted = true;
            } else if (compareValueArray(leftMeta,
                                         rightMeta,
                                         leftStartKvEntry,
                                         rightKvEntry) != 0) {
                isEqualRight = false;
            }
        }
    }

    // We broke the left anti join or left semi join whenever a right matching
    // row is found. We need to forward the right cursor to the next non-equal
    // position. This doesn't really change the correctness, but is for
    // performance and readability.
    if (this->joinType == LeftAntiJoin || this->joinType == LeftSemiJoin) {
        while (isEqualRight) {
            status = checkLoopStatusAndUpdate(loopCounter, workCounter);
            BailIfFailed(status);

            rightCursor->savePosition(rightTempPosition);
            status = rightCursor->getNext(rightKvEntry);
            if (status != StatusOk && status != StatusNoData) {
                goto CommonExit;
            }
            if (status == StatusNoData) {
                isEqualRight = false;
                *rightExhausted = true;
            } else if (compareValueArray(leftMeta,
                                         rightMeta,
                                         leftStartKvEntry,
                                         rightKvEntry) != 0) {
                isEqualRight = false;
            }
        }
    }

    std::swap(rightStartPosition, rightTempPosition);

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (accHashTable) {
        accHashTable->clear();
    }

    return status;
}

Status
OperatorsJoinWork::setUp()
{
    Status status;
    XdbMeta *dstMeta;
    XdbMgr *xdbMgr = XdbMgr::get();

    bool handleInit = false;

    status = xdbMgr->xdbGet(this->dstXdbId, NULL, &dstMeta);
    assert(status == StatusOk);

    status = opGetInsertHandle(&this->insertHandle,
                               &dstMeta->loadInfo,
                               this->insertState);
    BailIfFailed(status);
    handleInit = true;

CommonExit:
    if (status != StatusOk) {
        if (handleInit) {
            opPutInsertHandle(&this->insertHandle);
            handleInit = false;
        }
    }

    return status;
}

void
OperatorsJoinWork::tearDown()
{
    opPutInsertHandle(&this->insertHandle);
}

Status
OperatorsJoinWork::doWork()
{
    // Begin declaration.
    Status status = StatusOk;

    XdbMgr *xdbMgr = XdbMgr::get();
    Xdb *leftXdb, *rightXdb, *dstXdb;
    XdbMeta *leftMeta, *rightMeta, *dstMeta;

    NewKeyValueEntry *leftKvEntry = NULL;
    NewKeyValueEntry *rightKvEntry = NULL;
    NewKeyValueEntry *dstKvEntry = NULL;
    NewKeyValueEntry *leftStartKvEntry = NULL;

    TableCursor::Position *rightStartPosition = NULL;
    TableCursor::Position *rightTempPosition = NULL;

    bool leftExhausted = false;
    bool rightExhausted = false;

    TableCursor leftCursor;
    TableCursor rightCursor;
    bool leftCursorInited = false;
    bool rightCursorInited = false;

    AccumulatorHashTable *accHashTable = NULL;
    bool needAccHashTable = false;

    size_t loopCounter = 0;
    size_t workCounter = 0;

    // Begin setup.
    this->numRecsLeft =
        xdbMgr->xdbGetNumRowsInHashSlot(this->leftXdb, this->slotId);
    this->numRecsRight =
        xdbMgr->xdbGetNumRowsInHashSlot(this->rightXdb, this->slotId);

    if (this->numRecsLeft == 0 && this->numRecsRight == 0) {
        goto CommonExit;
    }

    this->startRecordLeft =
        xdbMgr->xdbGetHashSlotStartRecord(this->leftXdb, this->slotId);
    this->startRecordRight =
        xdbMgr->xdbGetHashSlotStartRecord(this->rightXdb, this->slotId);

    status = xdbMgr->xdbGet(this->leftXdbId, &leftXdb, &leftMeta);
    assert(status == StatusOk);
    assert(leftXdb != NULL);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(this->rightXdbId, &rightXdb, &rightMeta);
    assert(status == StatusOk);
    assert(rightXdb != NULL);
    BailIfFailed(status);

    // This should never fail since the current operation controls dest XDB
    status = xdbMgr->xdbGet(this->dstXdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);
    assert(dstXdb != NULL);
    assert(dstMeta != NULL);
    BailIfFailed(status);

    leftKvEntry =
        new (std::nothrow) NewKeyValueEntry(&leftMeta->kvNamedMeta.kvMeta_);
    BailIfNull(leftKvEntry);
    rightKvEntry =
        new (std::nothrow) NewKeyValueEntry(&rightMeta->kvNamedMeta.kvMeta_);
    BailIfNull(rightKvEntry);
    dstKvEntry =
        new (std::nothrow) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);
    BailIfNull(dstKvEntry);
    leftStartKvEntry =
        new (std::nothrow) NewKeyValueEntry(&leftMeta->kvNamedMeta.kvMeta_);
    BailIfNull(leftStartKvEntry);

    rightStartPosition = new (std::nothrow) TableCursor::Position();
    BailIfNull(rightStartPosition);
    rightTempPosition = new (std::nothrow) TableCursor::Position();
    BailIfNull(rightTempPosition);

    status = initCursor(&leftCursor, &leftExhausted, &leftCursorInited, true);
    BailIfFailed(status);
    if (!leftExhausted) {
        status = leftCursor.getNext(leftKvEntry);
        assert(status == StatusOk && "just created this cursor");
        BailIfFailed(status);
        ++workCounter;
    }

    status =
        initCursor(&rightCursor, &rightExhausted, &rightCursorInited, false);
    BailIfFailed(status);
    if (!rightExhausted) {
        rightCursor.savePosition(rightStartPosition);
        status = rightCursor.getNext(rightKvEntry);
        assert(status == StatusOk && "just created this cursor");
        BailIfFailed(status);
    }

    needAccHashTable =
        this->filterCtx.astInit_ &&
        (this->joinType == RightOuterJoin || this->joinType == FullOuterJoin);
    if (needAccHashTable) {
        accHashTable = new (std::nothrow) AccumulatorHashTable();
        BailIfNull(accHashTable);
        status = accHashTable->init(NULL, NULL, 0, true);
        BailIfFailed(status);
    }

    // Begin work.
    while (!leftExhausted && !rightExhausted) {
        status = checkLoopStatusAndUpdate(&loopCounter, &workCounter);
        BailIfFailed(status);

        int comp =
            compareValueArray(leftMeta, rightMeta, leftKvEntry, rightKvEntry);

        if (comp == 0) {
            leftKvEntry->tuple_.cloneTo(leftKvEntry->kvMeta_->tupMeta_,
                                        &leftStartKvEntry->tuple_);
            status = processEqual(&leftCursor,
                                  &rightCursor,
                                  leftMeta,
                                  rightMeta,
                                  leftKvEntry,
                                  rightKvEntry,
                                  dstKvEntry,
                                  leftStartKvEntry,
                                  rightStartPosition,
                                  rightTempPosition,
                                  &leftExhausted,
                                  &rightExhausted,
                                  accHashTable,
                                  &loopCounter,
                                  &workCounter);
            BailIfFailed(status);
        } else if (comp < 0) {
            if (this->joinType == LeftAntiJoin ||
                this->joinType == LeftOuterJoin ||
                this->joinType == FullOuterJoin) {
                status =
                    combineValueArrayAndInsert(leftKvEntry, NULL, dstKvEntry);
                BailIfFailed(status);
            }

            status = leftCursor.getNext(leftKvEntry);
            if (status == StatusNoData) {
                leftExhausted = true;
                status = StatusOk;
            }
            BailIfFailed(status);
            ++workCounter;
        } else {
            if (this->joinType == RightOuterJoin ||
                this->joinType == FullOuterJoin) {
                status =
                    combineValueArrayAndInsert(NULL, rightKvEntry, dstKvEntry);
                BailIfFailed(status);
            }

            rightCursor.savePosition(rightStartPosition);
            status = rightCursor.getNext(rightKvEntry);
            if (status == StatusNoData) {
                rightExhausted = true;
                status = StatusOk;
            }
            BailIfFailed(status);
        }
    }

    if ((this->joinType == LeftAntiJoin || this->joinType == LeftOuterJoin ||
         this->joinType == FullOuterJoin)) {
        while (!leftExhausted) {
            status = checkLoopStatusAndUpdate(&loopCounter, &workCounter);
            BailIfFailed(status);

            status = combineValueArrayAndInsert(leftKvEntry, NULL, dstKvEntry);
            BailIfFailed(status);

            status = leftCursor.getNext(leftKvEntry);
            if (status == StatusNoData) {
                status = StatusOk;
                break;
            }
            BailIfFailed(status);
            ++workCounter;
        }
    }

    if (this->joinType == RightOuterJoin || this->joinType == FullOuterJoin) {
        while (!rightExhausted) {
            status = checkLoopStatusAndUpdate(&loopCounter, &workCounter);
            BailIfFailed(status);

            status = combineValueArrayAndInsert(NULL, rightKvEntry, dstKvEntry);
            BailIfFailed(status);

            status = rightCursor.getNext(rightKvEntry);
            if (status == StatusNoData) {
                status = StatusOk;
                break;
            }
            BailIfFailed(status);
        }
    }

    if (leftCursorInited) {
        CursorManager::get()->destroy(&leftCursor);
        leftCursorInited = false;
    }

    if (rightCursorInited) {
        CursorManager::get()->destroy(&rightCursor);
        rightCursorInited = false;
    }

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                this->insertState,
                                this->leftXdb,
                                NULL,
                                this->slotId);

    atomicAdd64(&this->opStatus->atomicOpDetails.numWorkCompletedAtomic,
                workCounter);

CommonExit:
    if (accHashTable) {
        delete accHashTable;
        accHashTable = NULL;
    }

    if (leftCursorInited) {
        CursorManager::get()->destroy(&leftCursor);
        leftCursorInited = false;
    }

    if (rightCursorInited) {
        CursorManager::get()->destroy(&rightCursor);
        rightCursorInited = false;
    }

    if (rightStartPosition) {
        delete rightStartPosition;
        rightStartPosition = NULL;
    }

    if (rightTempPosition) {
        delete rightTempPosition;
        rightTempPosition = NULL;
    }

    if (leftKvEntry) {
        delete leftKvEntry;
        leftKvEntry = NULL;
    }
    if (rightKvEntry) {
        delete rightKvEntry;
        rightKvEntry = NULL;
    }
    if (dstKvEntry) {
        delete dstKvEntry;
        dstKvEntry = NULL;
    }
    if (leftStartKvEntry) {
        delete leftStartKvEntry;
        leftStartKvEntry = NULL;
    }
    return status;
}

OpKvEntryCopyMapping *
Operators::getOpKvEntryCopyMapping(unsigned numValueArrayEntriesInDst)
{
    OpKvEntryCopyMapping *opKvEntryCopyMapping = NULL;
    opKvEntryCopyMapping = (OpKvEntryCopyMapping *)
        memAllocExt(sizeof(*opKvEntryCopyMapping) +
                        (sizeof(opKvEntryCopyMapping->srcIndices[0]) *
                         numValueArrayEntriesInDst),
                    moduleName);
    if (opKvEntryCopyMapping == NULL) {
        return NULL;
    }

    opKvEntryCopyMapping->maxNumEntries = numValueArrayEntriesInDst;
    opKvEntryCopyMapping->numEntries = 0;
    opKvEntryCopyMapping->isReplica = true;

    for (unsigned ii = 0; ii < numValueArrayEntriesInDst; ii++) {
        opKvEntryCopyMapping->srcIndices[ii] = NewTupleMeta::DfInvalidIdx;
    }

    return opKvEntryCopyMapping;
}

Status
OperatorsCrossJoinWork::evalFilter(XdbMeta *dstMeta,
                                   NewKeyValueEntry *dstKvEntry,
                                   bool *filterResult)
{
    Status status;

    if (this->filterCtx.astInit_) {
        size_t dstNumFields =
            dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
        status =
            this->filterCtx.filterAst(dstKvEntry, dstNumFields, filterResult);
        BailIfFailed(status);
    } else {
        *filterResult = true;
    }

CommonExit:
    return status;
}

Status
OperatorsCrossJoinWork::workHelper(Xdb *broadcastXdb,
                                   XdbMeta *broadcastMeta,
                                   NewKeyValueEntry *broadcastKvEntry,
                                   TableCursor *broadcastCursor,
                                   Xdb *srcXdb,
                                   XdbMeta *srcMeta,
                                   NewKeyValueEntry *srcKvEntry,
                                   TableCursor *srcCursor,
                                   uint64_t startRecord,
                                   uint64_t totalRecords,
                                   NewKeyValueEntry *leftKvEntry,
                                   NewKeyValueEntry *rightKvEntry)
{
    Status status = StatusOk;
    bool srcCursorInit = false, broadcastCursorInit = false;
    bool filterResult = false;
    Xdb *dstXdb;
    XdbMeta *dstMeta;
    NewKeyValueEntry *dstKvEntry = NULL;

    size_t numRecords = 0, workCounter = 0;
    unsigned numLeftEntries = this->numLeftCopyMappingEntries;
    unsigned numRightEntries = this->numRightCopyMappingEntries;
    CursorManager *cursorMgr = CursorManager::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    OpStatus *opStatus = this->opStatus;
    OpKvEntryCopyMapping *opKvEntryCopyMapping = this->opKvEntryCopyMapping;

    status = xdbMgr->xdbGet(this->dstXdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);

    status = cursorMgr->createOnSlot(srcMeta->xdbId,
                                     this->slotId,
                                     startRecord,
                                     Unordered,
                                     srcCursor);
    BailIfFailed(status);
    srcCursorInit = true;

    dstKvEntry =
        new (std::nothrow) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);
    BailIfNull(dstKvEntry);

    for (numRecords = 0; numRecords < totalRecords; numRecords++) {
        status = checkLoopStatus(opStatus, &numRecords, &workCounter);
        BailIfFailed(status);

        status = srcCursor->getNext(srcKvEntry);
        if (status != StatusNoData && status != StatusOk) {
            goto CommonExit;
        }
        if (status == StatusNoData) {
            break;
        }

        workCounter++;

        status =
            XdbMgr::get()->createCursorFast(broadcastXdb, -1, broadcastCursor);
        if (status != StatusNoData && status != StatusOk) {
            goto CommonExit;
        }
        if (unlikely(status == StatusNoData)) {
            break;
        }

        broadcastCursorInit = true;
        size_t innerLoopCount = 0;

        while ((status = broadcastCursor->getNext(broadcastKvEntry)) ==
               StatusOk) {
            status = checkLoopStatus(opStatus, &innerLoopCount, &workCounter);
            BailIfFailed(status);

            innerLoopCount++;

            Operators::combineValueArray(leftKvEntry,
                                         rightKvEntry,
                                         numLeftEntries,
                                         numRightEntries,
                                         opKvEntryCopyMapping,
                                         dstKvEntry);

            status = evalFilter(dstMeta, dstKvEntry, &filterResult);
            BailIfFailed(status);

            if (filterResult) {
                status =
                    opPopulateInsertHandle(&this->insertHandle, dstKvEntry);
                BailIfFailed(status);
            }
        }
        if (status != StatusNoData && status != StatusOk) {
            goto CommonExit;
        }

        cursorMgr->destroy(broadcastCursor);
        broadcastCursorInit = false;
    }
    atomicAdd64(&this->opStatus->atomicOpDetails.numWorkCompletedAtomic,
                workCounter);

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (srcCursorInit) {
        cursorMgr->destroy(srcCursor);
        srcCursorInit = false;
    }

    if (broadcastCursorInit) {
        cursorMgr->destroy(broadcastCursor);
        broadcastCursorInit = false;
    }

    if (dstKvEntry) {
        delete dstKvEntry;
        dstKvEntry = NULL;
    }

    return status;
}

Status
OperatorsCrossJoinWork::doWork()
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    Xdb *leftXdb, *rightXdb;
    XdbMeta *leftMeta, *rightMeta;
    NewKeyValueEntry *broadcastLKvEntry = NULL;
    NewKeyValueEntry *broadcastRKvEntry = NULL;
    NewKeyValueEntry *srcRKvEntry = NULL;
    NewKeyValueEntry *srcLKvEntry = NULL;

    this->numRecsLeft =
        xdbMgr->xdbGetNumRowsInHashSlot(this->leftXdb, this->slotId);
    this->numRecsRight =
        xdbMgr->xdbGetNumRowsInHashSlot(this->rightXdb, this->slotId);

    if (this->numRecsLeft == 0 && this->numRecsRight == 0) {
        goto CommonExit;
    }

    this->startRecordLeft =
        xdbMgr->xdbGetHashSlotStartRecord(this->leftXdb, this->slotId);
    this->startRecordRight =
        xdbMgr->xdbGetHashSlotStartRecord(this->rightXdb, this->slotId);

    // XXX: CrossJoin is a stable operation unlike other join
    // types. This could be structured in a cleaner way
    XdbMgr::xdbCopySlotSortState(this->srcXdb, this->dstXdb, this->slotId);

    status = xdbMgr->xdbGet(this->leftXdbId, &leftXdb, &leftMeta);
    assert(status == StatusOk);

    status = xdbMgr->xdbGet(this->rightXdbId, &rightXdb, &rightMeta);
    assert(status == StatusOk);

    if (leftMeta->dhtId == XidMgr::XidSystemBroadcastDht) {
        TableCursor broadcastCursor;
        TableCursor srcCursor;
        int64_t startRecord = this->startRecordRight;
        int64_t totalRecords = this->numRecsRight;

        broadcastLKvEntry =
            new (std::nothrow) NewKeyValueEntry(&leftMeta->kvNamedMeta.kvMeta_);
        BailIfNull(broadcastLKvEntry);

        srcRKvEntry = new (std::nothrow)
            NewKeyValueEntry(&rightMeta->kvNamedMeta.kvMeta_);
        BailIfNull(srcRKvEntry);

        status = workHelper(leftXdb,
                            leftMeta,
                            broadcastLKvEntry,
                            &broadcastCursor,
                            rightXdb,
                            rightMeta,
                            srcRKvEntry,
                            &srcCursor,
                            startRecord,
                            totalRecords,
                            broadcastLKvEntry,
                            srcRKvEntry);
    } else {
        TableCursor broadcastCursor;
        TableCursor srcCursor;
        int64_t startRecord = this->startRecordLeft;
        int64_t totalRecords = this->numRecsLeft;

        broadcastRKvEntry = new (std::nothrow)
            NewKeyValueEntry(&rightMeta->kvNamedMeta.kvMeta_);
        BailIfNull(broadcastRKvEntry);

        srcLKvEntry =
            new (std::nothrow) NewKeyValueEntry(&leftMeta->kvNamedMeta.kvMeta_);
        BailIfNull(srcLKvEntry);

        status = workHelper(rightXdb,
                            rightMeta,
                            broadcastRKvEntry,
                            &broadcastCursor,
                            leftXdb,
                            leftMeta,
                            srcLKvEntry,
                            &srcCursor,
                            startRecord,
                            totalRecords,
                            srcLKvEntry,
                            broadcastRKvEntry);
    }

CommonExit:
    if (broadcastLKvEntry) {
        delete broadcastLKvEntry;
        broadcastLKvEntry = NULL;
    }
    if (broadcastRKvEntry) {
        delete broadcastRKvEntry;
        broadcastRKvEntry = NULL;
    }
    if (srcLKvEntry) {
        delete srcLKvEntry;
        srcLKvEntry = NULL;
    }
    if (srcRKvEntry) {
        delete srcRKvEntry;
        srcRKvEntry = NULL;
    }
    return status;
}

void
Operators::operatorsJoinLocal(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiJoinInput *joinInput = (XcalarApiJoinInput *) opInput->buf;
    XdbMgr *xdbMgr = XdbMgr::get();
    Xdb *leftXdb, *rightXdb, *dstXdb;
    XdbMeta *dstMeta, *leftMeta, *rightMeta;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    status = xdbMgr->xdbGet(joinInput->leftTable.xdbId, &leftXdb, &leftMeta);
    BailIfFailed(status);
    assert(leftXdb != NULL);

    status = xdbMgr->xdbGet(joinInput->rightTable.xdbId, &rightXdb, &rightMeta);
    BailIfFailed(status);
    assert(rightXdb != NULL);

    status = xdbMgr->xdbRehashXdbIfNeeded(leftXdb, XdbInsertCrcHash);
    BailIfFailed(status);

    status = xdbMgr->xdbRehashXdbIfNeeded(rightXdb, XdbInsertCrcHash);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(joinInput->joinTable.xdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);
    assert(dstXdb != NULL);

    status = joinHelper(opInput,
                        leftXdb,
                        leftMeta,
                        rightXdb,
                        rightMeta,
                        dstXdb,
                        dstMeta);

CommonExit:
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

Status
Operators::joinHelper(OperatorsApiInput *opInput,
                      Xdb *leftXdb,
                      XdbMeta *leftMeta,
                      Xdb *rightXdb,
                      XdbMeta *rightMeta,
                      Xdb *dstXdb,
                      XdbMeta *dstMeta)
{
    XcalarApiJoinInput *joinInput = (XcalarApiJoinInput *) opInput->buf;
    uint64_t numChunks;
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    OperatorsJoinWork **operatorsWorkers = NULL;
    OpStatus *opStatus;

    unsigned numValueArrayEntriesInDst = 0;
    OpKvEntryCopyMapping *opKvEntryCopyMapping = NULL;
    unsigned leftStartIdx, leftEndIdx, rightStartIdx, rightEndIdx;
    unsigned numLeftCopyMappingEntries, numRightCopyMappingEntries;

    TrackHelpers *trackHelpers = NULL;
    DagLib *dagLib = DagLib::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbInsertKvState insertState;
    status = (dagLib->getDagLocal(opInput->dagId))
                 ->getOpStatusFromXdbId(joinInput->joinTable.xdbId, &opStatus);
    assert(status == StatusOk);

    // Full Outer joins cannot use SlotHash because they generate new FNF keys.
    // Need to rehash them to the first slot
    if (joinInput->joinType != FullOuterJoin &&
        (dstMeta->keyOrderings[0] == Unordered ||
         dstMeta->keyOrderings[0] & PartiallySortedFlag)) {
        // since we are pure local, and use the same hash function as srcTable,
        // we can hash during insert and do it with no lock
        dstMeta->prehashed = true;

        insertState = XdbInsertSlotHash;
    } else {
        insertState = XdbInsertRandomHash;
    }

    numValueArrayEntriesInDst =
        dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
    opKvEntryCopyMapping = getOpKvEntryCopyMapping(numValueArrayEntriesInDst);
    if (opKvEntryCopyMapping == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate opKvEntryCopyMapping (numEntries: %u)",
                numValueArrayEntriesInDst);
        status = StatusNoMem;
        goto CommonExit;
    }

    leftStartIdx = 0;
    leftEndIdx = xcMin(leftMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields(),
                       (uint64_t) numValueArrayEntriesInDst);
    initKvEntryCopyMapping(opKvEntryCopyMapping,
                           leftMeta,
                           dstMeta,
                           leftStartIdx,
                           leftEndIdx,
                           true,
                           &joinInput->renameMap[0],
                           joinInput->numLeftColumns,
                           !joinInput->keepAllColumns);

    rightStartIdx = opKvEntryCopyMapping->numEntries;
    numLeftCopyMappingEntries = opKvEntryCopyMapping->numEntries;
    rightEndIdx = numValueArrayEntriesInDst;
    initKvEntryCopyMapping(opKvEntryCopyMapping,
                           rightMeta,
                           dstMeta,
                           rightStartIdx,
                           rightEndIdx,
                           true,
                           &joinInput->renameMap[joinInput->numLeftColumns],
                           joinInput->numRightColumns,
                           !joinInput->keepAllColumns);
    numRightCopyMappingEntries =
        opKvEntryCopyMapping->numEntries - rightStartIdx;

    numChunks = xdbMgr->xdbGetNumHashSlots(leftXdb);

    unsigned numWorkers;
    numWorkers = (uint64_t) Runtime::get()->getThreadsCount(
        Txn::currentTxn().rtSchedId_);
    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    if (trackHelpers == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    operatorsWorkers = new (std::nothrow) OperatorsJoinWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    // XXX: This assumes that the left and right table have the same
    // number of hash slots
    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsJoinWork *operatorsWork = NULL;
        if (joinInput->joinType == CrossJoin) {
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsCrossJoinWork((ii == numWorkers - 1)
                                           ? TrackHelpers::Master
                                           : TrackHelpers::NonMaster,
                                       ii);
            BailIfNull(operatorsWorkers[ii]);

            if (joinInput->filterString[0] != '\0') {
                status = operatorsWorkers[ii]
                             ->filterCtx.setupAst(joinInput->filterString,
                                                  dstMeta,
                                                  opInput->dagId);
                BailIfFailed(status);
            }
        } else {
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsJoinWork((ii == numWorkers - 1)
                                      ? TrackHelpers::Master
                                      : TrackHelpers::NonMaster,
                                  ii);
            BailIfNull(operatorsWorkers[ii]);

            if (joinInput->filterString[0] != '\0') {
                status =
                    operatorsWorkers[ii]
                        ->filterCtx.setupMultiAst(joinInput->filterString,
                                                  leftMeta,
                                                  rightMeta,
                                                  joinInput->renameMap,
                                                  joinInput->numLeftColumns,
                                                  joinInput->numRightColumns,
                                                  opInput->dagId);
                BailIfFailed(status);
            }
        }

        operatorsWork = operatorsWorkers[ii];

        operatorsWork->dstXdbId = joinInput->joinTable.xdbId;

        operatorsWork->dropSrcSlots = opInput->flags & OperatorFlagDropSrcSlots;
        operatorsWork->serializeSlots =
            opInput->flags & OperatorFlagSerializeSlots;
        operatorsWork->leftXdbId = joinInput->leftTable.xdbId;
        operatorsWork->rightXdbId = joinInput->rightTable.xdbId;
        operatorsWork->leftXdb = leftXdb;
        operatorsWork->rightXdb = rightXdb;
        operatorsWork->joinType = joinInput->joinType;
        operatorsWork->nullSafe = joinInput->nullSafe;
        operatorsWork->insertState = insertState;

        operatorsWork->opStatus = opStatus;
        operatorsWork->op = OperatorsJoin;
        operatorsWork->srcXdb = leftXdb;
        operatorsWork->trackHelpers = trackHelpers;

        operatorsWork->opKvEntryCopyMapping = opKvEntryCopyMapping;
        operatorsWork->numLeftCopyMappingEntries = numLeftCopyMappingEntries;
        operatorsWork->numRightCopyMappingEntries = numRightCopyMappingEntries;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

    xdbMgr->xdbResetDensityStats(leftXdb);
    xdbMgr->xdbResetDensityStats(rightXdb);

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (opKvEntryCopyMapping != NULL) {
        memFree(opKvEntryCopyMapping);
        opKvEntryCopyMapping = NULL;
    }

    return status;
}

Status
OperatorsUnionWork::setUp()
{
    Status status = StatusOk;
    bool insertHandleInited = false;

    this->numRecs =
        (int64_t *) memAlloc(this->numSrcXdbs * sizeof(*this->numRecs));
    BailIfNull(this->numRecs);

    this->startRecs =
        (int64_t *) memAlloc(this->numSrcXdbs * sizeof(*this->startRecs));
    BailIfNull(this->startRecs);

    status = opGetInsertHandle(&this->insertHandle,
                               &this->dstMeta->loadInfo,
                               XdbInsertSlotHash);
    BailIfFailed(status);
    insertHandleInited = true;

CommonExit:
    if (status != StatusOk) {
        memFree(this->numRecs);
        this->numRecs = NULL;
        memFree(this->startRecs);
        this->startRecs = NULL;

        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }
    }

    return status;
}

void
OperatorsUnionWork::tearDown()
{
    opPutInsertHandle(&this->insertHandle);

    memFree(this->numRecs);
    this->numRecs = NULL;
    memFree(this->startRecs);
    this->startRecs = NULL;
}

Status
OperatorsUnionWork::applyArgMap(NewKeyValueEntry *srcKvEntry,
                                NewKeyValueEntry *dstKvEntry,
                                unsigned srcXdbIdx)
{
    Status status = StatusOk;
    const NewTupleMeta *srcTupMeta =
        this->xdbMetas[srcXdbIdx]->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields = srcTupMeta->getNumFields();

    const NewTupleMeta *dstTupMeta =
        this->dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    dstKvEntry->init();

    for (unsigned jj = 0; jj < srcNumFields; jj++) {
        int dstIdx = this->argMap[srcXdbIdx][jj];

        if (dstIdx == NewTupleMeta::DfInvalidIdx) {
            continue;
        }

        DfFieldType dstType = dstTupMeta->getFieldType(dstIdx);
        DfFieldType srcType = srcTupMeta->getFieldType(jj);
        bool retIsValid;
        DfFieldValue srcValue =
            srcKvEntry->tuple_.get(jj, srcNumFields, srcType, &retIsValid);

        if (retIsValid) {
            assert(dstType != DfUnknown);

            DfFieldValue dstValue;
            if (srcTupMeta->getFieldType(jj) == DfScalarObj &&
                dstType != DfScalarObj) {
                // need to extract into DfFieldValue
                Scalar *scalar = srcValue.scalarVal;
                assert(scalar != NULL);

                if (scalar->fieldType != dstType) {
                    status = StatusDfTypeMismatch;
                    goto CommonExit;
                }

                status = scalar->getValue(&dstValue);
                if (status != StatusOk) {
                    // skip this field
                    continue;
                }
            } else {
                assert(dstType == srcTupMeta->getFieldType(jj));

                dstValue = srcValue;
            }
            dstKvEntry->tuple_.set(dstIdx,
                                   dstValue,
                                   dstKvEntry->kvMeta_->tupMeta_->getFieldType(
                                       dstIdx));
        }
    }

    status = StatusOk;

CommonExit:
    return status;
}

Status
OperatorsUnionWork::doUnionAllWork()
{
    Status status = StatusOk;
    TableCursor srcCursor;
    bool cursorInit = false;

    bool cursorsExhausted[this->numSrcXdbs];
    memZero(cursorsExhausted, sizeof(cursorsExhausted));

    unsigned workCounter = 0, numRecords, ii;
    CursorManager *cursorMgr = CursorManager::get();
    NewKeyValueEntry srcKvEntry;
    NewKeyValueEntry dstKvEntry(&this->dstMeta->kvNamedMeta.kvMeta_);

    for (ii = 0; ii < this->numSrcXdbs; ii++) {
        status = cursorMgr->createOnSlot(this->xdbMetas[ii]->xdbId,
                                         this->slotId,
                                         this->startRecs[ii],
                                         Unordered,
                                         &srcCursor);
        if (status == StatusNoData) {
            continue;
        }
        BailIfFailed(status);

        cursorInit = true;
        numRecords = 0;

        new (&srcKvEntry)
            NewKeyValueEntry(&this->xdbMetas[ii]->kvNamedMeta.kvMeta_);

        while (numRecords < this->numRecs[ii]) {
            if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                         0)) {
                if (usrNodeNormalShutdown()) {
                    status = StatusShutdownInProgress;
                    goto CommonExit;
                }

                if (this->opStatus->atomicOpDetails.cancelled) {
                    status = StatusCanceled;
                    goto CommonExit;
                }

                atomicAdd64(&this->opStatus->atomicOpDetails
                                 .numWorkCompletedAtomic,
                            workCounter);
                workCounter = 0;
            }
            numRecords++;
            workCounter++;

            status = srcCursor.getNext(&srcKvEntry);
            if (unlikely(status == StatusNoData)) {
                break;
            }
            BailIfFailed(status);

            status = applyArgMap(&srcKvEntry, &dstKvEntry, ii);
            BailIfFailed(status);

            status = opPopulateInsertHandle(&this->insertHandle, &dstKvEntry);
            BailIfFailed(status);
        }

        assert(cursorInit);
        cursorMgr->destroy(&srcCursor);
        cursorInit = false;

        Operators::pruneSlot(this->dropSrcSlots,
                             this->serializeSlots,
                             XdbInsertSlotHash,
                             this->xdbs[ii],
                             NULL,
                             this->slotId);
    }

    atomicAdd64(&this->opStatus->atomicOpDetails.numWorkCompletedAtomic,
                workCounter);

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (cursorInit) {
        cursorMgr->destroy(&srcCursor);
        cursorInit = false;
    }
    return status;
}

Status
OperatorsUnionWork::doWork()
{
    if (this->dedup || this->unionType != UnionStandard) {
        return this->doUnionWork();
    } else {
        return this->doUnionAllWork();
    }
}

Status
OperatorsUnionWork::doUnionWork()
{
    Status status = StatusOk;
    TableCursor srcCursor[this->numSrcXdbs];
    bool cursorInit[this->numSrcXdbs];
    memZero(cursorInit, sizeof(cursorInit));

    bool cursorsExhausted[this->numSrcXdbs];
    memZero(cursorsExhausted, sizeof(cursorsExhausted));

    // array of 1s used for testing
    bool allCursorsExhausted[this->numSrcXdbs];
    memset(allCursorsExhausted, 1, sizeof(allCursorsExhausted));

    unsigned numRecords[this->numSrcXdbs];
    bool isMin[this->numSrcXdbs];
    memZero(isMin, sizeof(isMin));

    unsigned workCounter = 0, ii;
    CursorManager *cursorMgr = CursorManager::get();
    const NewTupleMeta *dstTupMeta =
        this->dstMeta->kvNamedMeta.kvMeta_.tupMeta_;

    NewKeyValueEntry *srcKvEntry = NULL;
    NewKeyValueEntry *tmpKvEntry = NULL;
    NewKeyValueEntry *minKvEntry = NULL;
    NewKeyValueEntry *dstKvEntry = NULL;

    srcKvEntry = new (std::nothrow) NewKeyValueEntry[this->numSrcXdbs];
    BailIfNull(srcKvEntry);

    tmpKvEntry = new (std::nothrow)
        NewKeyValueEntry(&this->dstMeta->kvNamedMeta.kvMeta_);
    BailIfNull(tmpKvEntry);

    minKvEntry = new (std::nothrow)
        NewKeyValueEntry(&this->dstMeta->kvNamedMeta.kvMeta_);
    BailIfNull(minKvEntry);

    dstKvEntry = new (std::nothrow) NewKeyValueEntry[this->numSrcXdbs];
    BailIfNull(dstKvEntry);

    for (ii = 0; ii < this->numSrcXdbs; ii++) {
        new (&srcKvEntry[ii])
            NewKeyValueEntry(&this->xdbMetas[ii]->kvNamedMeta.kvMeta_);
        new (&dstKvEntry[ii])
            NewKeyValueEntry(&this->dstMeta->kvNamedMeta.kvMeta_);

        status = cursorMgr->createOnSlot(this->xdbMetas[ii]->xdbId,
                                         this->slotId,
                                         this->startRecs[ii],
                                         PartialAscending,
                                         &srcCursor[ii]);
        if (status == StatusNoData) {
            cursorsExhausted[ii] = true;
            continue;
        }
        BailIfFailed(status);

        cursorInit[ii] = true;
        numRecords[ii] = 0;

        status = srcCursor[ii].getNext(&srcKvEntry[ii]);
        assert(status == StatusOk);
        BailIfFailed(status);

        status = applyArgMap(&srcKvEntry[ii], &dstKvEntry[ii], ii);
        BailIfFailed(status);
    }

    while (memcmp(allCursorsExhausted,
                  cursorsExhausted,
                  sizeof(cursorsExhausted)) != 0) {
        if (unlikely(workCounter % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (this->opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&this->opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }
        workCounter++;

        // find the row with minimum value
        int minIdx = -1;
        for (ii = 0; ii < this->numSrcXdbs; ii++) {
            if (cursorsExhausted[ii]) {
                continue;
            }

            if (minIdx == -1) {
                minIdx = ii;
                isMin[ii] = true;
                dstKvEntry[ii].tuple_.cloneTo(minKvEntry->kvMeta_->tupMeta_,
                                              &minKvEntry->tuple_);
                continue;
            }

            int ret = DataFormat::fieldArrayCompare(this->dstMeta->numKeys,
                                                    NULL,
                                                    dstMeta->keyIdxOrder,
                                                    dstTupMeta,
                                                    &dstKvEntry[ii].tuple_,
                                                    dstMeta->keyIdxOrder,
                                                    dstTupMeta,
                                                    &dstKvEntry[minIdx].tuple_);
            if (ret <= 0) {
                dstKvEntry[ii].tuple_.cloneTo(minKvEntry->kvMeta_->tupMeta_,
                                              &minKvEntry->tuple_);
                isMin[ii] = true;

                if (ret < 0) {
                    minIdx = ii;
                    // the previous rows are no longer mins
                    memZero(isMin, sizeof(*isMin) * ii);
                }
            }
        }

        uint64_t numMin[this->numSrcXdbs];
        memZero(numMin, sizeof(numMin));
        // advance all cursors that had the minimum value row
        for (ii = 0; ii < this->numSrcXdbs; ii++) {
            if (!isMin[ii]) {
                continue;
            }

            if (numRecords[ii] < this->numRecs[ii]) {
                // advance until you see a new row
                do {
                    numMin[ii]++;

                    status = srcCursor[ii].getNext(&srcKvEntry[ii]);
                    if (unlikely(status == StatusNoData)) {
                        cursorsExhausted[ii] = true;
                        isMin[ii] = false;
                        break;
                    }
                    BailIfFailed(status);

                    workCounter++;
                    numRecords[ii]++;

                    status = applyArgMap(&srcKvEntry[ii], tmpKvEntry, ii);
                    BailIfFailed(status);
                } while (!DataFormat::fieldArrayCompare(this->dstMeta->numKeys,
                                                        NULL,
                                                        dstMeta->keyIdxOrder,
                                                        dstTupMeta,
                                                        &dstKvEntry[ii].tuple_,
                                                        dstMeta->keyIdxOrder,
                                                        dstTupMeta,
                                                        &tmpKvEntry->tuple_));
                if (!cursorsExhausted[ii]) {
                    tmpKvEntry->tuple_.cloneTo(tmpKvEntry->kvMeta_->tupMeta_,
                                               &dstKvEntry[ii].tuple_);
                }
            } else {
                cursorsExhausted[ii] = true;
            }

            isMin[ii] = false;
        }

        uint64_t numToInsert;
        status = getNumInserts(numMin, &numToInsert);
        BailIfFailed(status);

        for (ii = 0; ii < numToInsert; ii++) {
            // insert the row with minimum value
            status = opPopulateInsertHandle(&this->insertHandle, minKvEntry);
            BailIfFailed(status);
        }
    }

    for (ii = 0; ii < this->numSrcXdbs; ii++) {
        if (cursorInit[ii]) {
            cursorMgr->destroy(&srcCursor[ii]);
            cursorInit[ii] = false;
        }
        Operators::pruneSlot(this->dropSrcSlots,
                             this->serializeSlots,
                             XdbInsertSlotHash,
                             this->xdbs[ii],
                             NULL,
                             this->slotId);
    }

    atomicAdd64(&this->opStatus->atomicOpDetails.numWorkCompletedAtomic,
                workCounter);

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    for (unsigned ii = 0; ii < this->numSrcXdbs; ii++) {
        if (cursorInit[ii]) {
            cursorMgr->destroy(&srcCursor[ii]);
            cursorInit[ii] = false;
        }
    }

    delete[] srcKvEntry;
    delete tmpKvEntry;
    delete minKvEntry;
    delete[] dstKvEntry;
    return status;
}

Status
OperatorsUnionWork::getNumInserts(uint64_t *numMin, uint64_t *inserts) const
{
    Status status = StatusUnknown;
    size_t ii;
    uint64_t numToInsert = 1;

    *inserts = 0;

    switch (this->unionType) {
    case UnionStandard:
        assert(this->dedup);
        break;
    case UnionIntersect:
        if (this->dedup) {
            for (ii = 0; ii < this->numSrcXdbs; ii++) {
                // If the row doesn't appear at least once in all tables,
                // don't insert it
                if (numMin[ii] == 0) {
                    numToInsert = 0;
                    break;
                }
            }
        } else {
            numToInsert = std::numeric_limits<uint64_t>::max();
            for (ii = 0; ii < this->numSrcXdbs; ii++) {
                // Insert the row the minimum number of times it appears in
                // each table
                if (numMin[ii] < numToInsert) {
                    numToInsert = numMin[ii];
                }
            }
        }
        break;
    case UnionExcept:
        if (numMin[0] == 0) {
            // If the subtrahend row doesn't appear at all in the minuend
            // table, don't insert it
            numToInsert = 0;
        } else if (this->dedup) {
            for (ii = 1; ii < this->numSrcXdbs; ii++) {
                // If the row appears in any of the subtrahend tables,
                // don't insert it
                if (numMin[ii] != 0) {
                    numToInsert = 0;
                    break;
                }
            }
        } else {
            uint64_t subtrahends = 0;
            for (ii = 1; ii < this->numSrcXdbs; ii++) {
                subtrahends += numMin[ii];
            }

            // If the number of rows in the minuend table exceeds the sum
            // of the number of rows in subtrahend table(s), the number of
            // inserts is the difference
            numToInsert = numMin[0] > subtrahends ? numMin[0] - subtrahends : 0;
        }
        break;
    default:
        assert(0);
        status = StatusInval;
        goto CommonExit;
    }

    *inserts = numToInsert;
    status = StatusOk;

CommonExit:
    return status;
}

void
Operators::operatorsUnionLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiUnionInput *unionInput = (XcalarApiUnionInput *) opInput->buf;

    xcalarApiDeserializeUnionInput(unionInput);

    int64_t numChunks;
    Xdb *xdbs[unionInput->numSrcTables];
    XdbMeta *xdbMetas[unionInput->numSrcTables];
    int *argMap[unionInput->numSrcTables];
    memZero(argMap, sizeof(argMap));

    int renameArgMap[unionInput->numSrcTables][TupleMaxNumValuesPerRecord];
    Xdb *dstXdb;
    XdbMeta *dstMeta;

    Status workerStatus = StatusOk;
    Status status = StatusOk;
    OperatorsUnionWork **operatorsWorkers = NULL;
    OpStatus *opStatus;

    TrackHelpers *trackHelpers = NULL;
    DagLib *dagLib = DagLib::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    status = (dagLib->getDagLocal(opInput->dagId))
                 ->getOpStatus(unionInput->dstTable.tableId, &opStatus);
    assert(status == StatusOk);

    status = xdbMgr->xdbGet(unionInput->dstTable.xdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);

    size_t dstNumFields = dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();

    // generate a mapping from renameMap to arg indexes in dstXdb
    for (unsigned ii = 0; ii < unionInput->numSrcTables; ii++) {
        const NewTupleMeta *tupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;

        for (unsigned jj = 0; jj < unionInput->renameMapSizes[ii]; jj++) {
            for (unsigned kk = 0; kk < dstNumFields; kk++) {
                if (FatptrTypeMatch(unionInput->renameMap[ii][jj].type,
                                    tupMeta->getFieldType(kk)) &&
                    strcmp(unionInput->renameMap[ii][jj].newName,
                           dstMeta->kvNamedMeta.valueNames_[kk]) == 0) {
                    renameArgMap[ii][jj] = kk;
                }
            }
        }
    }

    // generate a mapping from src indexes to dst index;
    for (unsigned ii = 0; ii < unionInput->numSrcTables; ii++) {
        status = xdbMgr->xdbGet(unionInput->srcTables[ii].xdbId,
                                &xdbs[ii],
                                &xdbMetas[ii]);
        assert(status == StatusOk);

        const NewTupleMeta *tupMeta =
            xdbMetas[ii]->kvNamedMeta.kvMeta_.tupMeta_;

        size_t numFields = tupMeta->getNumFields();
        argMap[ii] = new (std::nothrow) int[numFields];
        BailIfNull(argMap[ii]);

        for (unsigned jj = 0; jj < numFields; jj++) {
            argMap[ii][jj] = NewTupleMeta::DfInvalidIdx;

            for (unsigned kk = 0; kk < unionInput->renameMapSizes[ii]; kk++) {
                if (FatptrTypeMatch(unionInput->renameMap[ii][kk].type,
                                    tupMeta->getFieldType(jj)) &&
                    strcmp(xdbMetas[ii]->kvNamedMeta.valueNames_[jj],
                           unionInput->renameMap[ii][kk].oldName) == 0) {
                    argMap[ii][jj] = renameArgMap[ii][kk];
                }
            }
        }
    }

    numChunks = xdbMgr->xdbHashSlots;
    dstMeta->prehashed = true;
    unsigned numWorkers;
    numWorkers =
        Operators::getNumWorkers(opStatus->atomicOpDetails.numWorkTotal);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    BailIfNull(trackHelpers);

    operatorsWorkers = new (std::nothrow) OperatorsUnionWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsUnionWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsUnionWork((ii == numWorkers - 1) ? TrackHelpers::Master
                                                      : TrackHelpers::NonMaster,
                               ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->xdbs = xdbs;
        operatorsWork->xdbMetas = xdbMetas;
        operatorsWork->dstXdb = dstXdb;
        operatorsWork->dstMeta = dstMeta;
        operatorsWork->argMap = argMap;
        operatorsWork->dedup = unionInput->dedup;
        operatorsWork->unionType = unionInput->unionType;
        operatorsWork->numSrcXdbs = unionInput->numSrcTables;
        operatorsWork->srcXdb = xdbs[0];

        operatorsWork->dropSrcSlots = opInput->flags & OperatorFlagDropSrcSlots;
        operatorsWork->serializeSlots =
            opInput->flags & OperatorFlagSerializeSlots;

        operatorsWork->opStatus = opStatus;
        operatorsWork->op = OperatorsUnion;
        operatorsWork->trackHelpers = trackHelpers;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    for (unsigned ii = 0; ii < unionInput->numSrcTables; ii++) {
        if (argMap[ii] != NULL) {
            delete[] argMap[ii];
            argMap[ii] = NULL;
        }
    }

    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

// this only works when all variables are immediates
Status
Operators::fillScratchPadWithImmediates(Scalar **scratchPadScalars,
                                        unsigned numVariables,
                                        OpMeta *opMeta,
                                        NewKeyValueEntry *kvEntry)
{
    Status status = StatusOk;
    DfFieldValue fieldVal;
    Scalar *scalar;
    DfFieldType argType;
    OpPossibleImmediateIndices *possibleImmediateIndices =
        opMeta->possibleImmediateIndices;
    unsigned possibleImmediateIndex;
    bool foundPossibleImmediateIndex;
    size_t numFields = kvEntry->kvMeta_->tupMeta_->getNumFields();

    for (unsigned ii = 0; ii < numVariables; ii++) {
        bool retIsValid;
        scalar = scratchPadScalars[ii];
        scalar->fieldUsedSize = 0;

        if (possibleImmediateIndices[ii].numPossibleImmediateIndices == 0) {
            continue;
        }

        foundPossibleImmediateIndex = false;
        for (unsigned jj = 0;
             jj < possibleImmediateIndices[ii].numPossibleImmediateIndices;
             jj++) {
            possibleImmediateIndex =
                possibleImmediateIndices[ii].possibleImmediateIndices[jj];

            argType = kvEntry->kvMeta_->tupMeta_->getFieldType(
                possibleImmediateIndex);
            fieldVal = kvEntry->tuple_.get(possibleImmediateIndex,
                                           numFields,
                                           argType,
                                           &retIsValid);
            if (retIsValid) {
                foundPossibleImmediateIndex = true;
                break;
            }
        }

        if (!foundPossibleImmediateIndex) {
            continue;
        }

        status = scalar->setValue(fieldVal, argType);
        BailIfFailed(status);
    }

    for (unsigned ii = 0; ii < opMeta->numAggregateResults; ii++) {
        unsigned variableIdx = opMeta->aggregateResults[ii].variableIdx;
        scalar = scratchPadScalars[variableIdx];
        scalar->fieldUsedSize = 0;

        status = scalar->copyFrom(opMeta->aggregateResults[ii].result);
        // aggregate result is guarenteed to be a valid Scalar
        assert(status == StatusOk);
    }

CommonExit:
    return status;
}

void
Operators::pruneSlot(bool dropSrcSlots,
                     bool serializeSlots,
                     XdbInsertKvState insertState,
                     Xdb *srcXdb,
                     Xdb *dstXdb,
                     uint64_t slotId)
{
    assert(srcXdb != NULL);

    if (dropSrcSlots) {
        XdbMgr::get()->xdbDropSlot(srcXdb, slotId, false);
    }
}

Status
OperatorsFilterAndMapWork::setUp()
{
    Status status = StatusOk;
    OpMeta *opMeta = this->opMeta;
    unsigned numVariables = opMeta->numVariables;
    bool astsGenerated[opMeta->numEvals];
    memZero(astsGenerated, sizeof(astsGenerated));
    bool insertHandleInited = false;
    bool childInited = false;
    size_t rowMetaSize = sizeof(OpRowMeta) + opMeta->scalarsSize;
    unsigned ii;
    unsigned numScalarsAlloced = 0;
    XcalarEval *xcalarEval = XcalarEval::get();
    Operators *operators = Operators::get();

    this->asts = (XcalarEvalClass1Ast *) memAlloc(sizeof(*this->asts) *
                                                  opMeta->numEvals);
    BailIfNull(this->asts);

    this->scratchPadScalars =
        (Scalar **) memAllocExt(numVariables * sizeof(*this->scratchPadScalars),
                                moduleName);
    BailIfNullWith(this->scratchPadScalars, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = NULL;
    }

    this->demystifyVariables =
        new (std::nothrow) DemystifyVariable[numVariables];
    BailIfNullWith(this->demystifyVariables, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);

        if (this->scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        status = this->demystifyVariables[ii].init(opMeta->variableNames[ii],
                                                   DfUnknown);
        BailIfFailed(status);
        this->demystifyVariables[ii].possibleEntries =
            opMeta->validIndicesMap[ii];

        numScalarsAlloced++;
    }

    this->demystifyPages = Operators::allocSourceTransPages();
    BailIfNull(this->demystifyPages);

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        status = xcalarEval->generateClass1Ast(opMeta->evalStrings[ii],
                                               &this->asts[ii]);
        BailIfFailed(status);
        astsGenerated[ii] = true;

        if (!this->containsUdf) {
            this->containsUdf =
                xcalarEval->containsUdf(this->asts[ii].astCommon.rootNode);
        }
    }

    if (this->containsUdf) {
        // do initialization needed for UDF here
        status = operators->initChildShm(opMeta,
                                         numVariables,
                                         opMeta->srcXdbId,
                                         opMeta->numEvals,
                                         opMeta->evalStrings,
                                         this->asts,
                                         &this->childContext);
        BailIfFailed(status);
        childInited = true;
    }

    if (opMeta->fatptrDemystRequired || containsUdf) {
        this->rowMetaPile =
            MemoryPile::allocPile(rowMetaSize,
                                  Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);

        this->scalarPile =
            MemoryPile::allocPile(Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);
    }

    status = opGetInsertHandle(&this->insertHandle,
                               &opMeta->dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    this->localArgs =
        new (std::nothrow) OpDemystifyLocalArgs(this->scratchPadScalars,
                                                this->demystifyVariables,
                                                NULL,
                                                this->asts,
                                                &this->scalarPile,
                                                NULL,
                                                this->containsUdf,
                                                &this->childContext,
                                                &this->insertHandle,
                                                opMeta);
    BailIfNull(this->localArgs);

    this->demystifyHandle = new (std::nothrow)
        FilterAndMapDemystifySend(this->localArgs,
                                  this->demystifyPages,
                                  opMeta->dstXdbId,
                                  opMeta->validIndices,
                                  opMeta->fatptrTransPageHandle,
                                  NULL,
                                  NULL);
    BailIfNull(this->demystifyHandle);

    status = this->demystifyHandle->init();
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        for (ii = 0; ii < numScalarsAlloced; ii++) {
            assert(this->scratchPadScalars[ii] != NULL);
            Scalar::freeScalar(this->scratchPadScalars[ii]);
            this->scratchPadScalars[ii] = NULL;
        }

        if (this->scratchPadScalars != NULL) {
            memFree(this->scratchPadScalars);
            this->scratchPadScalars = NULL;
        }

        if (this->demystifyVariables != NULL) {
            delete[] this->demystifyVariables;
            this->demystifyVariables = NULL;
        }

        if (this->demystifyPages != NULL) {
            memFree(this->demystifyPages);
            this->demystifyPages = NULL;
        }

        if (this->asts != NULL) {
            for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
                if (astsGenerated[ii]) {
                    // scratchPadScalars have already been freed
                    xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                    xcalarEval->destroyClass1Ast(&asts[ii]);
                    astsGenerated[ii] = false;
                }
            }

            memFree(this->asts);
            this->asts = NULL;
        }

        if (childInited) {
            operators->freeChildShm(&this->childContext);
        }

        if (this->rowMetaPile != NULL) {
            this->rowMetaPile->markPileAllocsDone();
        }

        if (this->scalarPile != NULL) {
            this->scalarPile->markPileAllocsDone();
        }

        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }

        if (this->localArgs) {
            delete this->localArgs;
            this->localArgs = NULL;
        }

        if (this->demystifyHandle) {
            this->demystifyHandle->destroy();
            this->demystifyHandle = NULL;
        }
    }

    return status;
}

Status
OperatorsFilterAndMapWork::doWork()
{
    OperatorsFilterAndMapWork *operatorsWork = this;
    TableCursor srcCursor;
    bool srcCursorInited = false;
    int64_t numRecords = 0;
    Status status = StatusUnknown;
    Operators *operators = Operators::get();

    OpMeta *opMeta = operatorsWork->opMeta;

    XdbId srcXdbId = opMeta->srcXdbId;
    Xdb *srcXdb = opMeta->srcXdb;
    XdbMeta *srcMeta = opMeta->srcMeta;
    Xdb *dstXdb = opMeta->dstXdb;
    OpStatus *opStatus = opMeta->opStatus;

    NewKeyValueEntry srcKvEntry(&srcMeta->kvNamedMeta.kvMeta_);

    OpRowMeta *rowMeta = NULL;
    size_t rowMetaSize = sizeof(*rowMeta) + opMeta->scalarsSize;
    int64_t rowNum;
    int64_t slotId;

    if (unlikely(atomicRead64(&opMeta->status) != StatusCodeOk)) {
        status.fromStatusCode((StatusCode) atomicRead64(&opMeta->status));
        goto CommonExit;
    }

    rowNum = operatorsWork->startRecord;
    slotId = operatorsWork->slotId;

    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;

    numRecords = 0;
    unsigned workCounter;
    workCounter = 0;

    while (numRecords < operatorsWork->numRecords) {
        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        // don't need rowMeta if we aren't demystifying
        if (opMeta->fatptrDemystRequired || this->containsUdf) {
            assert(rowMeta == NULL);
            status = operators->getRowMeta(&rowMeta,
                                           rowNum,
                                           &this->rowMetaPile,
                                           rowMetaSize,
                                           srcXdb,
                                           &srcCursor,
                                           opMeta->numVariables);
            BailIfFailed(status);
        }

        status = srcCursor.getNext(&srcKvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        workCounter++;
        numRecords++;
        rowNum++;

        if (opMeta->fatptrDemystRequired) {
            // save rowMeta pointer in key field
            DfFieldValue key;
            key.uint64Val = (uint64_t) rowMeta;
            status = demystifyHandle->demystifyRow(&srcKvEntry,
                                                   key,
                                                   &rowMeta->numIssued);
            rowMeta = NULL;
            BailIfFailed(status);
        } else {
            // All needed columns are immediates.
            status =
                operators->fillScratchPadWithImmediates(this->scratchPadScalars,
                                                        opMeta->numVariables,
                                                        opMeta,
                                                        &srcKvEntry);
            // this can only fail with unimplemented which should never happen
            // for immediates
            assert(status == StatusOk);

            if (containsUdf) {
                // Populate values into rowMeta.
                status = operators->populateRowMetaWithScalarsFromArray(
                    rowMeta->scalars,
                    opMeta->numVariables,
                    this->scratchPadScalars,
                    &this->scalarPile);
                BailIfFailed(status);

                status = operators->udfExecFilterOrMap(opMeta,
                                                       rowMeta,
                                                       &this->childContext,
                                                       dstXdb);
                rowMeta = NULL;
                BailIfFailed(status);
            } else {
                status = operators->executeFilterOrMap(this->asts,
                                                       dstXdb,
                                                       &srcKvEntry,
                                                       this->scratchPadScalars,
                                                       &this->insertHandle,
                                                       opMeta);
                BailIfFailed(status);
            }
        }
    }

    if (this->containsUdf) {
        auto *input = this->childContext.input;

        if (likely(input->tupBuf->getNumTuples() > 0)) {
            // Send partially filled input.
            status = operators->udfSendToChildForEval(input,
                                                      &this->childContext,
                                                      opMeta,
                                                      opMeta->dstXdb);
            if (status != StatusOk) {
                atomicWrite64(&opMeta->status, status.code());
            }
        }
    }

    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;
    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                opMeta->insertState,
                                this->srcXdb,
                                opMeta->dstXdb,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (rowMeta != NULL) {
        operators->putRowMeta(rowMeta, opMeta->numVariables);
        rowMeta = NULL;
    }

    if (status == StatusNoData) {
        assert(numRecords == operatorsWork->numRecords);
        status = StatusOk;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }
    return status;
}

void
OperatorsFilterAndMapWork::tearDown()
{
    Status status;
    OpMeta *opMeta = this->opMeta;

    Config *config = Config::get();
    XcalarEval *xcalarEval = XcalarEval::get();
    Operators *operators = Operators::get();

    // after walking through all of the records, we may (and are likely) to
    // end up with partially filled transport pages; so enqueue these before
    // returning
    // XXX when node level recovery is done, we will need an explicit number
    //     of entries to process since config->getActiveNodes() will become
    //     variable
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        if (this->demystifyPages[ii] == NULL) {
            continue;
        }
        status = opMeta->fatptrTransPageHandle->enqueuePage(
            this->demystifyPages[ii]);
        if (status != StatusOk) {
            atomicWrite64(&opMeta->status, status.code());
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to enqueue transport page in "
                    "filter/map completion: %s",
                    strGetFromStatus(status));
            TransportPageMgr::get()->freeTransportPage(
                this->demystifyPages[ii]);
        }
        this->demystifyPages[ii] = NULL;
    }

    if (this->containsUdf) {
        operators->freeChildShm(&this->childContext);
    }

    opPutInsertHandle(&this->insertHandle);

    if (this->rowMetaPile != NULL) {
        this->rowMetaPile->markPileAllocsDone();
    }

    if (this->scalarPile != NULL) {
        this->scalarPile->markPileAllocsDone();
    }

    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        assert(this->scratchPadScalars[ii] != NULL);
        Scalar::freeScalar(this->scratchPadScalars[ii]);
        this->scratchPadScalars[ii] = NULL;
    }

    assert(this->scratchPadScalars != NULL);
    memFree(this->scratchPadScalars);
    this->scratchPadScalars = NULL;

    assert(this->demystifyVariables != NULL);
    delete[] this->demystifyVariables;
    this->demystifyVariables = NULL;

    assert(this->demystifyPages != NULL);
    memFree(this->demystifyPages);
    this->demystifyPages = NULL;

    assert(this->asts != NULL);
    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        xcalarEval->dropScalarVarRef(&this->asts[ii].astCommon);
        xcalarEval->destroyClass1Ast(&this->asts[ii]);
    }
    memFree(asts);
    asts = NULL;

    if (this->localArgs) {
        delete this->localArgs;
        this->localArgs = NULL;
    }

    if (this->demystifyHandle) {
        this->demystifyHandle->destroy();
        this->demystifyHandle = NULL;
    }
}

Status
FilterAndMapDemystifySend::processDemystifiedRow(void *localArgs,
                                                 NewKeyValueEntry *srcKvEntry,
                                                 DfFieldValue rowMetaField,
                                                 Atomic32 *countToDecrement)
{
    assert(localArgs != NULL);
    assert(srcKvEntry != NULL);

    Status status;
    Operators *op = Operators::get();
    OpDemystifyLocalArgs *args = (OpDemystifyLocalArgs *) localArgs;
    OpMeta *opMeta = args->opMeta;
    DemystifyVariable *demystifyVariables = args->demystifyVariables;
    Scalar **scratchPadScalars = args->scratchPadScalars;
    NewTupleMeta tupleMeta;
    OpEvalErrorStats *errorStats = NULL;
    OpStatus *opStatus = opMeta->opStatus;

    tupleMeta.setNumFields(opMeta->numVariables + 1);

    for (size_t ii = 0; ii < opMeta->numVariables; ii++) {
        tupleMeta.setFieldType(DfScalarObj, ii);
    }
    tupleMeta.setFieldType(DfOpRowMetaPtr, opMeta->numVariables);
    NewKeyValueMeta kvMeta(&tupleMeta, opMeta->numVariables);
    NewKeyValueEntry scalarValues(&kvMeta);

    // copy over rowMeta
    scalarValues.tuple_.set(opMeta->numVariables, rowMetaField, DfOpRowMetaPtr);

    status = DataFormat::get()
                 ->extractFields(srcKvEntry,
                                 opMeta->srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                 opMeta->numVariables,
                                 demystifyVariables,
                                 scratchPadScalars,
                                 &scalarValues);
    if (unlikely(status != StatusOk)) {
        // must decrement count if we have an early failure. The count is
        // deceremented properly in the functions below, just need to do it here
        if (countToDecrement) {
            atomicDec32(countToDecrement);
        }
        goto CommonExit;
    }

    errorStats = (OpEvalErrorStats *) memAlloc(sizeof(OpEvalErrorStats));
    BailIfNull(errorStats);

    memset(errorStats, 0, sizeof(OpEvalErrorStats));

    status = op->operatorsFilterAndMapWork((XcalarEvalClass1Ast *) args->asts,
                                           opMeta->dstXdb,
                                           opMeta->numVariables,
                                           &scalarValues,
                                           opMeta,
                                           args->scalarPile,
                                           args->containsUdf,
                                           args->childContext,
                                           errorStats,
                                           args->insertHandle);
    BailIfFailed(status);

    op->updateOpStatusForEval(errorStats,
                              &opStatus->atomicOpDetails.errorStats
                                   .evalErrorStats);
CommonExit:
    if (errorStats != NULL) {
        memFree(errorStats);
        errorStats = NULL;
    }
    return status;
}

Status
GroupByDemystifySend::processDemystifiedRow(void *localArgs,
                                            NewKeyValueEntry *srcKvEntry,
                                            DfFieldValue rowMetaField,
                                            Atomic32 *countToDecrement)
{
    assert(localArgs != NULL);
    assert(srcKvEntry != NULL);

    Status status;
    Operators *op = Operators::get();
    OpDemystifyLocalArgs *args = (OpDemystifyLocalArgs *) localArgs;
    OpMeta *opMeta = args->opMeta;
    DemystifyVariable *demystifyVariables = args->demystifyVariables;
    Scalar **scratchPadScalars = args->scratchPadScalars;
    NewTupleMeta tupleMeta;
    tupleMeta.setNumFields(opMeta->numVariables + 1);
    for (size_t ii = 0; ii < opMeta->numVariables; ii++) {
        tupleMeta.setFieldType(DfScalarObj, ii);
    }
    tupleMeta.setFieldType(DfOpRowMetaPtr, opMeta->numVariables);
    NewKeyValueMeta kvMeta(&tupleMeta, opMeta->numVariables);
    NewKeyValueEntry scalarValues(&kvMeta);

    // copy over rowMeta
    scalarValues.tuple_.set(opMeta->numVariables, rowMetaField, DfOpRowMetaPtr);

    status = DataFormat::get()
                 ->extractFields(srcKvEntry,
                                 opMeta->srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                 opMeta->numVariables,
                                 demystifyVariables,
                                 scratchPadScalars,
                                 &scalarValues);
    if (unlikely(status != StatusOk)) {
        // must decrement count if we have an early failure. The count is
        // deceremented properly in the functions below, just need to do it here
        if (countToDecrement) {
            atomicDec32(countToDecrement);
        }
        goto CommonExit;
    }

    status = op->operatorsGroupByWork((XcalarEvalClass2Ast *) args->asts,
                                      opMeta->dstXdb,
                                      opMeta->dstMeta,
                                      args->scratchPadXdb,
                                      opMeta->srcMeta,
                                      opMeta->numVariables,
                                      &scalarValues,
                                      opMeta,
                                      args->scalarPile,
                                      opMeta->includeSrcTableSample,
                                      args->results,
                                      args->insertHandle);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
CreateScalarTableDemystifySend::processDemystifiedRow(
    void *localArgs,
    NewKeyValueEntry *srcKvEntry,
    DfFieldValue rowMetaField,
    Atomic32 *countToDecrement)
{
    assert(localArgs != NULL);
    assert(srcKvEntry != NULL);

    Status status;
    Operators *op = Operators::get();
    OpDemystifyLocalArgs *args = (OpDemystifyLocalArgs *) localArgs;
    OpMeta *opMeta = args->opMeta;
    DemystifyVariable *demystifyVariables = args->demystifyVariables;
    Scalar **scratchPadScalars = args->scratchPadScalars;
    NewTupleMeta tupleMeta;
    tupleMeta.setNumFields(opMeta->numVariables + 1);
    for (size_t ii = 0; ii < opMeta->numVariables; ii++) {
        tupleMeta.setFieldType(DfScalarObj, ii);
    }
    tupleMeta.setFieldType(DfOpRowMetaPtr, opMeta->numVariables);
    NewKeyValueMeta kvMeta(&tupleMeta, opMeta->numVariables);
    NewKeyValueEntry scalarValues(&kvMeta);

    // copy over rowMeta
    scalarValues.tuple_.set(opMeta->numVariables, rowMetaField, DfOpRowMetaPtr);

    status = DataFormat::get()
                 ->extractFields(srcKvEntry,
                                 opMeta->srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                 opMeta->numVariables,
                                 demystifyVariables,
                                 scratchPadScalars,
                                 &scalarValues);
    if (unlikely(status != StatusOk)) {
        // must decrement count if we have an early failure. The count is
        // deceremented properly in the functions below, just need to do it here
        if (countToDecrement) {
            atomicDec32(countToDecrement);
        }
        goto CommonExit;
    }

    status =
        op->operatorsCreateScalarTableWork(opMeta->dstXdb,
                                           opMeta->numVariables,
                                           &scalarValues,
                                           opMeta,
                                           args->scalarPile,
                                           (XcalarEvalClass1Ast *) args->asts,
                                           args->insertHandle);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
IndexDemystifySend::processDemystifiedRow(void *localArgs,
                                          NewKeyValueEntry *srcKvEntry,
                                          DfFieldValue rowMetaField,
                                          Atomic32 *countToDecrement)
{
    assert(localArgs != NULL);
    assert(srcKvEntry != NULL);

    Status status;
    Operators *op = Operators::get();
    OpIndexDemystifyLocalArgs *args = (OpIndexDemystifyLocalArgs *) localArgs;
    OpMeta *opMeta = args->opMeta;
    DemystifyVariable *demystifyVariables = args->demystifyVariables;
    Scalar **scratchPadScalars = args->scratchPadScalars;
    NewTupleMeta tupleMeta;
    tupleMeta.setNumFields(opMeta->numVariables + 1);
    for (size_t ii = 0; ii < opMeta->numVariables; ii++) {
        tupleMeta.setFieldType(DfScalarObj, ii);
    }
    tupleMeta.setFieldType(DfOpRowMetaPtr, opMeta->numVariables);
    NewKeyValueMeta kvMeta(&tupleMeta, opMeta->numVariables);
    NewKeyValueEntry scalarValues(&kvMeta);

    // copy over rowMeta
    scalarValues.tuple_.set(opMeta->numVariables, rowMetaField, DfOpRowMetaPtr);

    status = DataFormat::get()
                 ->extractFields(srcKvEntry,
                                 opMeta->srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                 opMeta->numVariables,
                                 demystifyVariables,
                                 scratchPadScalars,
                                 &scalarValues);
    if (unlikely(status != StatusOk)) {
        // must decrement count if we have an early failure. The count is
        // deceremented properly in the functions below, just need to do it here
        if (countToDecrement) {
            atomicDec32(countToDecrement);
        }
        goto CommonExit;
    }

    status = op->indexProcessDemystifiedKeys(args, &scalarValues);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
OperatorsDemystifyRecv::demystifySourcePage(TransportPage *sourcePage,
                                            TransportPageHandle *scalarHandle)
{
    Status status = StatusOk;
    Operators::PerTxnInfo *perTxnInfo =
        (Operators::PerTxnInfo *) sourcePage->dstNodeTxnCookie;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    unsigned numScalarsAlloced = 0;
    unsigned ii;

    TransportPage *scalarPage = NULL;
    unsigned numVariables = demystifyInput->numVariables;
    XdbMeta *srcMeta = perTxnInfo->srcMeta;
    TransportPageMgr *tpMgr = TransportPageMgr::get();
    DataFormat *df = DataFormat::get();

    NewTupleMeta *srcTupleMeta = &sourcePage->tupMeta;
    uint64_t srcNumFields = srcTupleMeta->getNumFields();
    // row meta ptr is the last index in tuple
    int rowMetaIdx = srcNumFields - 1;
    NewKeyValueMeta srcKvMeta((const NewTupleMeta *) &sourcePage->tupMeta,
                              rowMetaIdx);
    NewKeyValueEntry srcKvEntry((const NewKeyValueMeta *) &srcKvMeta);
    NewTuplesCursor cursor(sourcePage->tupBuf);

    NewTupleMeta dstTupleMeta;
    dstTupleMeta.setNumFields(numVariables + 1);
    dstTupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);
    NewKeyValueMeta dstKvMeta((const NewTupleMeta *) &dstTupleMeta,
                              NewTupleMeta::DfInvalidIdx);
    NewKeyValueEntry dstKvEntry((const NewKeyValueMeta *) &srcKvMeta);

    Scalar *scratchPadScalars[numVariables];
    char *variableNames[numVariables];
    DemystifyVariable demystifyVariables[numVariables];
    char stringPtr[demystifyInput->inputStringsSize];
    char *stringPtrTmp = stringPtr;

    memcpy(stringPtr,
           demystifyInput->inputStrings,
           demystifyInput->inputStringsSize);

    bool **validIndicesMap =
        (bool **) memAlloc(numVariables * sizeof(*validIndicesMap));
    BailIfNullWith(validIndicesMap, StatusNoMem);
    memZero(validIndicesMap, numVariables * sizeof(*validIndicesMap));

    for (ii = 0; ii < numVariables; ii++) {
        validIndicesMap[ii] =
            (bool *) memAlloc(srcNumFields * sizeof(*validIndicesMap[ii]));
        BailIfNullWith(validIndicesMap[ii], StatusNoMem);

        memZero(validIndicesMap[ii],
                srcNumFields * sizeof(*validIndicesMap[ii]));
    }

    for (ii = 0; ii < numVariables; ii++) {
        scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numScalarsAlloced++;
        dstTupleMeta.setFieldType(DfScalarObj, ii);
        variableNames[ii] = stringPtrTmp;
        stringPtrTmp += strlen(stringPtrTmp) + 1;
        for (unsigned jj = 0; jj < srcNumFields; jj++) {
            validIndicesMap[ii][jj] = false;
        }
        demystifyVariables[ii].possibleEntries = validIndicesMap[ii];
    }
    assert(numScalarsAlloced == demystifyInput->numVariables);
    dstTupleMeta.setFieldType(DfOpRowMetaPtr, demystifyInput->numVariables);

    // we received the pre-parsed variable names; this will modify variableNames
    // to de-prefix them, and separate the prefixes into a validIndicesMap
    status = df->createValidIndicesMap(srcMeta,
                                       numVariables,
                                       variableNames,
                                       (bool **) validIndicesMap);
    BailIfFailed(status);

    // Now that the variable names have been de-prefixed, we can parse them
    for (ii = 0; ii < numVariables; ii++) {
        status = demystifyVariables[ii].init(variableNames[ii], DfUnknown);
        BailIfFailed(status);
    }

    // Source page should just have FatPtrs, so use fixed size cursoring.
    // save rowMeta into key during cursoring
    while ((status = cursor.getNextFixed((NewTupleMeta *) srcKvMeta.tupMeta_,
                                         &srcKvEntry.tuple_)) == StatusOk) {
        // copy rowMeta pointer
        bool retIsValid;
        DfFieldValue key = srcKvEntry.getKey(&retIsValid);
        assert(retIsValid);
        dstKvEntry.init();
        dstKvEntry.tuple_.set(numVariables, key, DfOpRowMetaPtr);
        status = df->extractFields(&srcKvEntry,
                                   (NewTupleMeta *) srcKvMeta.tupMeta_,
                                   numVariables,
                                   demystifyVariables,
                                   scratchPadScalars,
                                   &dstKvEntry);
        BailIfFailed(status);

        do {
            bool pageAlloc = false;
            if (scalarPage == NULL) {
                scalarPage =
                    (TransportPage *) tpMgr->allocRecvTransportPage(&status);
                BailIfNullWith(scalarPage, status);

                pageAlloc = true;
                scalarHandle->initPage(scalarPage,
                                       sourcePage->srcNodeId,
                                       &dstTupleMeta,
                                       sourcePage->demystifyOp);
            }

            status = scalarHandle->insertKv(&scalarPage,
                                            &dstTupleMeta,
                                            &dstKvEntry.tuple_);
            if (status == StatusNoData && pageAlloc) {
                // Entire row couldn't even fit into a freshly minted
                // transpage
                status = StatusMaxRowSizeExceeded;
            }

            if (status == StatusNoData) {
                // Let's try to allocate a new page
                // We should have sent out the scalarPage in our previous call
                // to transportPageInsert
                assert(scalarPage == NULL);
                continue;
            }

            if (status != StatusOk) {
                tpMgr->freeTransportPage(scalarPage);
                scalarPage = NULL;
                goto CommonExit;
            }
        } while (status == StatusNoData);
    }

    assert(status == StatusNoData);
    status = StatusOk;

CommonExit:
    if (scalarPage != NULL) {
        if (status == StatusOk) {
            status = scalarHandle->enqueuePage(scalarPage);
        }

        if (status != StatusOk) {
            tpMgr->freeTransportPage(scalarPage);
        }
        scalarPage = NULL;
    }

    if (validIndicesMap != NULL) {
        for (ii = 0; ii < numVariables; ii++) {
            if (validIndicesMap[ii] != NULL) {
                memFree(validIndicesMap[ii]);
                validIndicesMap[ii] = NULL;
            }
        }
        memFree(validIndicesMap);
        validIndicesMap = NULL;
    }

    for (ii = 0; ii < numScalarsAlloced; ii++) {
        assert(scratchPadScalars[ii] != NULL);
        Scalar::freeScalar(scratchPadScalars[ii]);
        scratchPadScalars[ii] = NULL;
    }

    assert(scalarPage == NULL);
    return status;
}

template <typename OperatorsDemystifySend>
Status
Operators::processGroup(OperatorsDemystifySend *demystifyHandle,
                        OpDemystifyLocalArgs *localArgs,
                        uint64_t keyCount,
                        int64_t *rowNum,
                        MemoryPile **rowMetaPile,
                        MemoryPile **groupMetaPile,
                        OpMeta *opMeta,
                        Xdb *srcXdb,
                        TableCursor *keyCursor,
                        DfFieldValue key)
{
    Status status = StatusUnknown;
    OpGroupMeta *groupMeta = NULL;
    OpGroupByRowMeta *rowMeta = NULL;
    size_t rowMetaSize = sizeof(*rowMeta) + opMeta->scalarsSize;
    unsigned numVariables = opMeta->numVariables;
    uint64_t numRowsMissing = 0, ii = 0;
    NewTupleMeta tupMeta;
    NewKeyValueEntry kvEntry(keyCursor->kvMeta_);

    status = getGroupMeta(&groupMeta,
                          groupMetaPile,
                          srcXdb,
                          opMeta->dstXdb,
                          keyCursor,
                          key,
                          keyCount);
    BailIfFailed(status);

    for (ii = 0; ii < keyCount; ii++) {
        status = keyCursor->getNext(&kvEntry);
        BailIfFailed(status);

        if (opMeta->fatptrDemystRequired) {
            DfFieldValue keyTmp;
            status = getGroupByRowMeta(&rowMeta,
                                       rowMetaPile,
                                       rowMetaSize,
                                       numVariables,
                                       groupMeta);
            BailIfFailed(status);

            // save rowMeta pointer in key field
            keyTmp.uint64Val = (uint64_t) rowMeta;

            status = demystifyHandle->demystifyRow(&kvEntry,
                                                   keyTmp,
                                                   &rowMeta->numIssued);
            rowMeta = NULL;
            BailIfFailed(status);
        } else {
            status = fillScratchPadWithImmediates(localArgs->scratchPadScalars,
                                                  opMeta->numVariables,
                                                  opMeta,
                                                  &kvEntry);
            BailIfFailed(status);

            status = addRowToGroup(localArgs->scratchPadScalars,
                                   groupMeta,
                                   opMeta->srcMeta->keyAttr[0].type,
                                   opMeta->numVariables,
                                   opMeta,
                                   &numRowsMissing,
                                   &tupMeta,
                                   localArgs->scratchPadXdb,
                                   false);
            BailIfFailed(status);

            if (numRowsMissing > 0) {
                continue;
            }

            // this should only be called after processing the last key
            assert(ii == keyCount - 1);
            status = executeGroupBy((XcalarEvalClass2Ast *) localArgs->asts,
                                    opMeta->dstXdb,
                                    opMeta->dstMeta,
                                    localArgs->results,
                                    groupMeta,
                                    opMeta->srcMeta,
                                    localArgs->scratchPadXdb,
                                    &tupMeta,
                                    opMeta->includeSrcTableSample,
                                    opMeta,
                                    localArgs->insertHandle);
            BailIfFailed(status);

            // the groupMeta pages were freed during execute
            groupMeta->firstPage = NULL;
            putGroupMeta(groupMeta);
            groupMeta = NULL;
        }

        (*rowNum)++;
    }

CommonExit:
    if (status != StatusOk) {
        atomicWrite64(&opMeta->status, status.code());
    }

    return status;
}

Status
OperatorsGroupByWork::setUp()
{
    Status status = StatusOk;
    OpMeta *opMeta = this->opMeta;
    unsigned numVariables = opMeta->numVariables;
    bool astsGenerated[opMeta->numEvals];
    memZero(astsGenerated, sizeof(astsGenerated));
    bool insertHandleInited = false;
    size_t rowMetaSize = sizeof(OpGroupByRowMeta) + opMeta->scalarsSize;
    unsigned ii;
    unsigned numScalarsAlloced = 0;
    XdbMgr *xdbMgr = XdbMgr::get();
    XcalarEval *xcalarEval = XcalarEval::get();
    Operators *operators = Operators::get();

    this->asts = (XcalarEvalClass2Ast *) memAlloc(sizeof(*this->asts) *
                                                  opMeta->numEvals);
    BailIfNull(this->asts);

    this->scratchPadScalars =
        (Scalar **) memAllocExt(numVariables * sizeof(*this->scratchPadScalars),
                                moduleName);
    BailIfNullWith(this->scratchPadScalars, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = NULL;
    }

    this->demystifyVariables =
        new (std::nothrow) DemystifyVariable[numVariables];
    BailIfNullWith(this->demystifyVariables, StatusNoMem);

    this->results =
        (Scalar **) memAllocExt(opMeta->numEvals * sizeof(*this->results),
                                moduleName);
    BailIfNull(this->results);
    memZero(this->results, opMeta->numEvals * sizeof(*this->results));

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (this->scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numScalarsAlloced++;
        status = this->demystifyVariables[ii].init(opMeta->variableNames[ii],
                                                   DfUnknown);
        BailIfFailed(status);
        this->demystifyVariables[ii].possibleEntries =
            opMeta->validIndicesMap[ii];
    }

    this->demystifyPages = Operators::allocSourceTransPages();
    BailIfNull(this->demystifyPages);

    this->scratchPadXdb = xdbMgr->xdbGetScratchPadXdb(opMeta->dstXdb);
    assert(xdbMgr->xdbGetMeta(this->scratchPadXdb) != NULL);

    this->evalContexts = (GroupEvalContext *) memAlloc(
        sizeof(*this->evalContexts) * opMeta->numEvals);
    BailIfNull(this->evalContexts);

    this->evalContextsValid = true;

    for (ii = 0; ii < opMeta->numEvals; ii++) {
        status = xcalarEval->generateClass2Ast(opMeta->evalStrings[ii],
                                               &this->asts[ii],
                                               NULL,
                                               opMeta->dag->getId(),
                                               opMeta->dstXdbId);
        BailIfFailed(status);
        astsGenerated[ii] = true;

        operators
            ->substituteAggregateResultsIntoAst2(&this->asts[ii],
                                                 opMeta->aggregateResults,
                                                 opMeta->numAggregateResults);

        status =
            xcalarEval->updateArgMappings(this->asts[ii].astCommon.rootNode,
                                          xdbMgr->xdbGetMeta(
                                              this->scratchPadXdb));
        BailIfFailed(status);

        // see if we can make use of the evalContext optimization. All of the
        // asts must pass the check in order to make it
        if (!opMeta->includeSrcTableSample && !opMeta->icvMode &&
            this->evalContextsValid) {
            new (&this->evalContexts[ii]) GroupEvalContext();

            bool valid =
                this->evalContexts[ii].initFromAst(&this->asts[ii],
                                                   opMeta->srcMeta,
                                                   opMeta->srcMeta->numKeys +
                                                       ii);
            if (!valid) {
                this->evalContextsValid = false;
            }
        } else {
            this->evalContextsValid = false;
        }
    }

    if (opMeta->fatptrDemystRequired) {
        this->rowMetaPile =
            MemoryPile::allocPile(rowMetaSize,
                                  Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);

        this->scalarPile =
            MemoryPile::allocPile(Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);
    }

    // Must be aligned to XdbMinPageAlignment.
    this->groupMetaPile =
        MemoryPile::allocPile(roundUp(sizeof(OpGroupMeta),
                                      XdbMgr::XdbMinPageAlignment),
                              Txn::currentTxn().id_,
                              &status,
                              1,
                              BcHandle::BcScanCleanoutToFree);
    BailIfFailed(status);

    for (ii = 0; ii < opMeta->numEvals; ii++) {
        this->results[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        BailIfNull(this->results[ii]);
    }

    status = opGetInsertHandle(&this->insertHandle,
                               &opMeta->dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    this->localArgs =
        new (std::nothrow) OpDemystifyLocalArgs(this->scratchPadScalars,
                                                this->demystifyVariables,
                                                this->scratchPadXdb,
                                                this->asts,
                                                &this->scalarPile,
                                                this->results,
                                                false,
                                                NULL,
                                                &this->insertHandle,
                                                opMeta);
    BailIfNull(this->localArgs);

    this->demystifyHandle =
        new (std::nothrow) GroupByDemystifySend(this->localArgs,
                                                this->demystifyPages,
                                                opMeta->dstXdbId,
                                                opMeta->validIndices,
                                                opMeta->fatptrTransPageHandle,
                                                NULL,
                                                NULL);
    BailIfNull(this->demystifyHandle);

    status = this->demystifyHandle->init();
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }

        for (ii = 0; ii < numScalarsAlloced; ii++) {
            assert(this->scratchPadScalars[ii] != NULL);
            Scalar::freeScalar(this->scratchPadScalars[ii]);
            this->scratchPadScalars[ii] = NULL;
        }

        if (this->scratchPadScalars != NULL) {
            memFree(this->scratchPadScalars);
            this->scratchPadScalars = NULL;
        }

        if (this->demystifyVariables != NULL) {
            delete[] this->demystifyVariables;
            this->demystifyVariables = NULL;
        }

        if (this->demystifyPages != NULL) {
            memFree(this->demystifyPages);
            this->demystifyPages = NULL;
        }

        if (this->asts != NULL) {
            for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
                if (astsGenerated[ii]) {
                    xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                    xcalarEval->destroyClass2Ast(&asts[ii]);
                    astsGenerated[ii] = false;
                }
            }

            memFree(this->asts);
            this->asts = NULL;
        }

        if (this->rowMetaPile != NULL) {
            this->rowMetaPile->markPileAllocsDone();
        }

        if (this->scalarPile != NULL) {
            this->scalarPile->markPileAllocsDone();
        }

        if (this->groupMetaPile != NULL) {
            this->groupMetaPile->markPileAllocsDone();
        }

        if (this->scratchPadXdb != NULL) {
            xdbMgr->xdbReleaseScratchPadXdb(this->scratchPadXdb);
        }

        if (this->results != NULL) {
            for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
                if (this->results[ii]) {
                    Scalar::freeScalar(this->results[ii]);
                    this->results[ii] = NULL;
                }
            }

            memFree(this->results);
            this->results = NULL;
        }

        if (this->localArgs) {
            delete this->localArgs;
            this->localArgs = NULL;
        }

        if (this->demystifyHandle) {
            this->demystifyHandle->destroy();
            this->demystifyHandle = NULL;
        }

        memFree(this->evalContexts);
    }

    return status;
}

Status
OperatorsGroupByWork::doWork()
{
    OperatorsGroupByWork *operatorsWork = this;
    TableCursor srcCursor;
    TableCursor keyCursor;
    bool srcCursorInited = false;
    bool keyCursorInited = false;
    int64_t numRecords = 0;
    Status status = StatusUnknown;
    NewKeyValueEntry srcKvEntry, prevKvEntry;

    OpMeta *opMeta = operatorsWork->opMeta;

    XdbMgr *xdbMgr = XdbMgr::get();
    XdbId srcXdbId = opMeta->srcXdbId;
    Xdb *srcXdb = opMeta->srcXdb;
    XdbMeta *srcMeta = opMeta->srcMeta;
    OpStatus *opStatus = opMeta->opStatus;

    int64_t rowNum = operatorsWork->startRecord;
    int64_t slotId = operatorsWork->slotId;
    uint64_t keyCount = 0;
    DfFieldValue prevKey;
    bool prevKeyValid = false;
    Operators *operators = Operators::get();
    bool retIsValid;

    if (unlikely(atomicRead64(&opMeta->status) != StatusCodeOk)) {
        status.fromStatusCode((StatusCode) atomicRead64(&opMeta->status));
        goto CommonExit;
    }

    if (this->evalContextsValid) {
        // use the evalContext method of doing groupBy
        SortedFlags sortedFlag = xdbMgr->xdbGetSortedFlags(srcXdb, slotId);
        if (opStatus->atomicOpDetails.cancelled) {
            status = StatusCanceled;
            goto CommonExit;
        }

        if (sortedFlag == UnsortedFlag) {
            status = groupSlotHash(opMeta->srcXdb,
                                   opMeta->dstXdb,
                                   opMeta->opKvEntryCopyMapping,
                                   slotId,
                                   opMeta->numEvals,
                                   this->evalContexts);
        } else {
            status = groupSlot(opMeta->srcXdb,
                               opMeta->dstXdb,
                               opMeta->opKvEntryCopyMapping,
                               slotId,
                               opMeta->numEvals,
                               this->evalContexts);
        }

        atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                    xdbMgr->xdbGetNumRowsInHashSlot(srcXdb, slotId));
        goto CommonExit;
    }

    // need sorted cursor to group keys together
    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                PartialAscending,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;

    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                PartialAscending,
                                                &keyCursor);
    BailIfFailed(status);
    keyCursorInited = true;

    new (&srcKvEntry) NewKeyValueEntry(keyCursor.kvMeta_);
    new (&prevKvEntry) NewKeyValueEntry(keyCursor.kvMeta_);

    unsigned workCounter;
    workCounter = 0;
    while (numRecords < operatorsWork->numRecords &&
           ((status = srcCursor.getNext(&srcKvEntry)) == StatusOk)) {
        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        workCounter++;
        numRecords++;

        if (unlikely(!prevKeyValid)) {
            srcKvEntry.tuple_.cloneTo(srcKvEntry.kvMeta_->tupMeta_,
                                      &prevKvEntry.tuple_);
            prevKey = prevKvEntry.getKey(&retIsValid);
            prevKeyValid = true;
        }

        if (DataFormat::fieldArrayCompare(srcMeta->numKeys,
                                          NULL,
                                          srcMeta->keyIdxOrder,
                                          srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                          &srcKvEntry.tuple_,
                                          srcMeta->keyIdxOrder,
                                          srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                                          &prevKvEntry.tuple_) == 0) {
            keyCount++;
            continue;
        }

        // This processes the previous key
        status = operators->processGroup(this->demystifyHandle,
                                         this->localArgs,
                                         keyCount,
                                         &rowNum,
                                         &this->rowMetaPile,
                                         &this->groupMetaPile,
                                         opMeta,
                                         srcXdb,
                                         &keyCursor,
                                         prevKey);
        BailIfFailed(status);

        srcKvEntry.tuple_.cloneTo(srcKvEntry.kvMeta_->tupMeta_,
                                  &prevKvEntry.tuple_);
        prevKey = prevKvEntry.getKey(&retIsValid);

        keyCount = 1;
    }

    if (status == StatusNoData) {
        status = StatusOk;
    }
    BailIfFailed(status);

    if (keyCount > 0) {
        status = operators->processGroup(this->demystifyHandle,
                                         this->localArgs,
                                         keyCount,
                                         &rowNum,
                                         &this->rowMetaPile,
                                         &this->groupMetaPile,
                                         opMeta,
                                         srcXdb,
                                         &keyCursor,
                                         prevKey);
    }

    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    assert(keyCursorInited);
    CursorManager::get()->destroy(&keyCursor);
    keyCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                opMeta->insertState,
                                this->srcXdb,
                                opMeta->dstXdb,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == operatorsWork->numRecords);
        status = StatusOk;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }

    if (keyCursorInited) {
        CursorManager::get()->destroy(&keyCursor);
        keyCursorInited = false;
    }
    return status;
}

void
OperatorsGroupByWork::tearDown()
{
    OpMeta *opMeta = this->opMeta;
    Status status;
    XcalarEval *xcalarEval = XcalarEval::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    Config *config = Config::get();

    // after walking through all of the records, we may (and are likely) to
    // end up with partially filled transport pages; so enqueue these before
    // returning
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        if (this->demystifyPages[ii] == NULL) {
            continue;
        }
        status = opMeta->fatptrTransPageHandle->enqueuePage(
            this->demystifyPages[ii]);
        if (status != StatusOk) {
            atomicWrite64(&opMeta->status, status.code());
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to enqueue transport page in "
                    "groupBy completion: %s",
                    strGetFromStatus(status));
            TransportPageMgr::get()->freeTransportPage(
                this->demystifyPages[ii]);
        }
        this->demystifyPages[ii] = NULL;
    }

    opPutInsertHandle(&this->insertHandle);

    if (this->rowMetaPile != NULL) {
        this->rowMetaPile->markPileAllocsDone();
    }

    if (this->scalarPile != NULL) {
        this->scalarPile->markPileAllocsDone();
    }

    if (this->groupMetaPile != NULL) {
        this->groupMetaPile->markPileAllocsDone();
    }

    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        assert(this->scratchPadScalars[ii] != NULL);
        Scalar::freeScalar(this->scratchPadScalars[ii]);
        this->scratchPadScalars[ii] = NULL;
    }

    assert(this->scratchPadScalars != NULL);
    memFree(this->scratchPadScalars);
    this->scratchPadScalars = NULL;

    assert(this->demystifyVariables != NULL);
    delete[] this->demystifyVariables;
    this->demystifyVariables = NULL;

    assert(this->demystifyPages != NULL);
    memFree(this->demystifyPages);
    this->demystifyPages = NULL;

    assert(this->scratchPadXdb != NULL);
    xdbMgr->xdbReleaseScratchPadXdb(this->scratchPadXdb);

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        if (this->results[ii]) {
            Scalar::freeScalar(this->results[ii]);
            this->results[ii] = NULL;
        }
    }
    memFree(this->results);
    this->results = NULL;

    for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
        // scratchPadScalars have already been freed
        xcalarEval->dropScalarVarRef(&this->asts[ii].astCommon);
        xcalarEval->destroyClass2Ast(&this->asts[ii]);
    }

    memFree(this->asts);
    this->asts = NULL;

    if (this->localArgs) {
        delete this->localArgs;
        this->localArgs = NULL;
    }

    if (this->demystifyHandle) {
        this->demystifyHandle->destroy();
        this->demystifyHandle = NULL;
    }

    memFree(this->evalContexts);
}

Status
OperatorsCreateScalarTableWork::setUp()
{
    Status status = StatusOk;
    OpMeta *opMeta = this->opMeta;
    unsigned numVariables = opMeta->numVariables;

    bool insertHandleInited = false;
    size_t rowMetaSize = sizeof(OpScalarTableRowMeta) + opMeta->scalarsSize;
    unsigned ii;
    unsigned numScalarsAlloced = 0;
    bool astsGenerated[opMeta->numEvals];
    memZero(astsGenerated, sizeof(astsGenerated));
    XcalarEval *xcalarEval = XcalarEval::get();

    this->scratchPadScalars =
        (Scalar **) memAllocExt(numVariables * sizeof(*this->scratchPadScalars),
                                moduleName);
    BailIfNullWith(this->scratchPadScalars, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = NULL;
    }

    this->demystifyVariables =
        new (std::nothrow) DemystifyVariable[numVariables];
    BailIfNullWith(this->demystifyVariables, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (this->scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numScalarsAlloced++;
        status = this->demystifyVariables[ii].init(opMeta->variableNames[ii],
                                                   DfUnknown);
        BailIfFailed(status);
        this->demystifyVariables[ii].possibleEntries =
            opMeta->validIndicesMap[ii];
    }

    this->demystifyPages = Operators::allocSourceTransPages();
    BailIfNull(this->demystifyPages);

    if (opMeta->op == OperatorsSynthesizeTable && opMeta->numEvals > 0) {
        this->asts = (XcalarEvalClass1Ast *) memAlloc(sizeof(*this->asts) *
                                                      opMeta->numEvals);
        BailIfNull(this->asts);

        for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
            status = xcalarEval->generateClass1Ast(opMeta->evalStrings[ii],
                                                   &this->asts[ii]);
            BailIfFailed(status);
            astsGenerated[ii] = true;
        }
    }

    if (opMeta->fatptrDemystRequired) {
        this->rowMetaPile =
            MemoryPile::allocPile(rowMetaSize,
                                  Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);

        this->scalarPile =
            MemoryPile::allocPile(Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);
    }

    status = opGetInsertHandle(&this->insertHandle,
                               &opMeta->dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

    this->localArgs =
        new (std::nothrow) OpDemystifyLocalArgs(this->scratchPadScalars,
                                                this->demystifyVariables,
                                                NULL,
                                                this->asts,
                                                &this->scalarPile,
                                                NULL,
                                                false,
                                                NULL,
                                                &this->insertHandle,
                                                opMeta);
    BailIfNull(this->localArgs);

    demystifyHandle = new (std::nothrow)
        CreateScalarTableDemystifySend(this->localArgs,
                                       this->demystifyPages,
                                       opMeta->dstXdbId,
                                       opMeta->validIndices,
                                       opMeta->fatptrTransPageHandle,
                                       NULL,
                                       NULL);
    BailIfNull(demystifyHandle);

    status = demystifyHandle->init();
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }

        for (ii = 0; ii < numScalarsAlloced; ii++) {
            assert(this->scratchPadScalars[ii] != NULL);
            Scalar::freeScalar(this->scratchPadScalars[ii]);
            this->scratchPadScalars[ii] = NULL;
        }

        if (this->scratchPadScalars != NULL) {
            memFree(this->scratchPadScalars);
            this->scratchPadScalars = NULL;
        }

        if (this->demystifyVariables != NULL) {
            delete[] this->demystifyVariables;
            this->demystifyVariables = NULL;
        }

        if (this->demystifyPages != NULL) {
            memFree(this->demystifyPages);
            this->demystifyPages = NULL;
        }

        if (this->rowMetaPile != NULL) {
            this->rowMetaPile->markPileAllocsDone();
        }

        if (this->scalarPile != NULL) {
            this->scalarPile->markPileAllocsDone();
        }

        if (this->localArgs) {
            delete this->localArgs;
            this->localArgs = NULL;
        }

        if (this->demystifyHandle != NULL) {
            demystifyHandle->destroy();
            this->demystifyHandle = NULL;
        }

        if (this->asts != NULL) {
            for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
                if (astsGenerated[ii]) {
                    // scratchPadScalars have already been freed
                    xcalarEval->dropScalarVarRef(&asts[ii].astCommon);
                    xcalarEval->destroyClass1Ast(&asts[ii]);
                    astsGenerated[ii] = false;
                }
            }

            memFree(this->asts);
            this->asts = NULL;
        }
    }

    return status;
}

Status
OperatorsCreateScalarTableWork::doWork()
{
    OperatorsCreateScalarTableWork *operatorsWork = this;
    TableCursor srcCursor;
    bool srcCursorInited = false;
    int64_t numRecords = 0;
    Status status = StatusUnknown;
    Operators *operators = Operators::get();
    OpMeta *opMeta = operatorsWork->opMeta;
    NewKeyValueEntry srcKvEntry(&opMeta->srcMeta->kvNamedMeta.kvMeta_);
    NewKeyValueEntry dstKvEntry(&opMeta->dstMeta->kvNamedMeta.kvMeta_);

    XdbId srcXdbId = opMeta->srcXdbId;
    OpStatus *opStatus = opMeta->opStatus;

    OpScalarTableRowMeta *rowMeta = NULL;
    size_t rowMetaSize = sizeof(*rowMeta) + opMeta->scalarsSize;

    int64_t rowNum = operatorsWork->startRecord;
    int64_t slotId = operatorsWork->slotId;

    if (unlikely(atomicRead64(&opMeta->status) != StatusCodeOk)) {
        status.fromStatusCode((StatusCode) atomicRead64(&opMeta->status));
        goto CommonExit;
    }

    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;

    numRecords = 0;

    unsigned workCounter;
    DfFieldValue key;
    workCounter = 0;
    while (numRecords < operatorsWork->numRecords) {
        bool keyValid = true;
        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        status = srcCursor.getNext(&srcKvEntry);

        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        if (this->useSrcKey) {
            key = srcKvEntry.getKey(&keyValid);
        } else {
            key.int64Val = rowNum;
        }

        if (opMeta->fatptrDemystRequired) {
            DfFieldValue rowMetaField;
            status = operators->getScalarTableRowMeta(&rowMeta,
                                                      key,
                                                      keyValid,
                                                      &this->rowMetaPile,
                                                      rowMetaSize,
                                                      opMeta->numVariables);
            BailIfFailed(status);

            // save rowMeta pointer in key field
            rowMetaField.uint64Val = (uint64_t) rowMeta;

            status = this->demystifyHandle->demystifyRow(&srcKvEntry,
                                                         rowMetaField,
                                                         &rowMeta->numIssued);
            rowMeta = NULL;
            BailIfFailed(status);
        } else {
            status =
                operators->fillScratchPadWithImmediates(this->localArgs
                                                            ->scratchPadScalars,
                                                        opMeta->numVariables,
                                                        opMeta,
                                                        &srcKvEntry);
            BailIfFailed(status);

            status = operators->insertIntoScalarTable(this->localArgs
                                                          ->scratchPadScalars,
                                                      key,
                                                      keyValid,
                                                      opMeta,
                                                      &dstKvEntry,
                                                      &this->insertHandle,
                                                      this->asts);

            BailIfFailed(status);
        }

        numRecords++;
        rowNum++;
        workCounter++;
    }

    // srcCursor is still pointing to an xdbPage in the slot we're about to
    // prune So we need to destroy the cursor first
    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                opMeta->insertState,
                                this->srcXdb,
                                opMeta->dstXdb,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == operatorsWork->numRecords);

        status = StatusOk;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }

    return status;
}

void
OperatorsCreateScalarTableWork::tearDown()
{
    OpMeta *opMeta = this->opMeta;
    Status status = StatusOk;
    Config *config = Config::get();
    XcalarEval *xcalarEval = XcalarEval::get();
    TransportPageMgr *tpMgr = TransportPageMgr::get();

    // after walking through all of the records, we may (and are likely) to
    // end up with partially filled transport pages; so enqueue these before
    // returning
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        if (status == StatusOk) {
            if (this->demystifyPages[ii] != NULL) {
                status = opMeta->fatptrTransPageHandle->enqueuePage(
                    this->demystifyPages[ii]);
                if (status != StatusOk) {
                    atomicWrite64(&opMeta->status, status.code());
                    tpMgr->freeTransportPage(this->demystifyPages[ii]);
                }
            }
        } else {
            // We failed somewhere - just free the transport pages
            if (this->demystifyPages[ii] != NULL) {
                tpMgr->freeTransportPage(this->demystifyPages[ii]);
            }
        }

        this->demystifyPages[ii] = NULL;
    }

    opPutInsertHandle(&this->insertHandle);

    if (this->rowMetaPile != NULL) {
        this->rowMetaPile->markPileAllocsDone();
    }

    if (this->scalarPile != NULL) {
        this->scalarPile->markPileAllocsDone();
    }

    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        assert(this->scratchPadScalars[ii] != NULL);
        Scalar::freeScalar(this->scratchPadScalars[ii]);
        this->scratchPadScalars[ii] = NULL;
    }

    assert(this->scratchPadScalars != NULL);
    memFree(this->scratchPadScalars);
    this->scratchPadScalars = NULL;

    assert(this->demystifyVariables != NULL);
    delete[] this->demystifyVariables;
    this->demystifyVariables = NULL;

    assert(this->demystifyPages != NULL);
    memFree(this->demystifyPages);
    this->demystifyPages = NULL;

    if (this->asts) {
        for (unsigned ii = 0; ii < opMeta->numEvals; ii++) {
            xcalarEval->dropScalarVarRef(&this->asts[ii].astCommon);
            xcalarEval->destroyClass1Ast(&this->asts[ii]);
        }
        memFree(asts);
        this->asts = NULL;
    }

    if (this->demystifyHandle != NULL) {
        this->demystifyHandle->destroy();
        this->demystifyHandle = NULL;
    }

    if (this->localArgs) {
        delete this->localArgs;
        this->localArgs = NULL;
    }
}

Status
Operators::opMetaInit(OpMeta *opMeta,
                      Dag *dag,
                      int *newFieldIdxs,
                      int numVariables,
                      char **variableNames,
                      unsigned numEvals,
                      char **evalStrings,
                      int **evalArgIndices,
                      XdbId srcXdbId,
                      Xdb *srcXdb,
                      XdbMeta *srcMeta,
                      XdbId dstXdbId,
                      Xdb *dstXdb,
                      XdbMeta *dstMeta,
                      OpStatus *opStatus,
                      OperatorsEnum op,
                      bool fatptrDemystRequired,
                      XdbInsertKvState insertState,
                      OpPossibleImmediateIndices *possibleImmediateIndices,
                      bool **validIndicesMap,
                      bool *validIndices,
                      OperatorsAggregateResult *aggregateResults,
                      unsigned numAggregateResults,
                      ChildPool *childPool,
                      TransportPageHandle *fatptrTransPageHandle,
                      TransportPageHandle *indexHandle,
                      OperatorFlag flags)
{
    Status status = StatusUnknown;
    bool semInited = false;
    bool indexOnRowNum = (numVariables == 1 &&
                          strstr(variableNames[0], DsDefaultDatasetKeyName));

    OpKvEntryCopyMapping *opKvEntryCopyMapping = NULL;
    unsigned numValueArrayEntriesInDst =
        dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
    opMeta->sem = (Semaphore *) memAllocExt(sizeof(*opMeta->sem), moduleName);
    BailIfNullWith(opMeta->sem, StatusNoMem);
    new (opMeta->sem) Semaphore(0);
    semInited = true;

    opKvEntryCopyMapping = getOpKvEntryCopyMapping(numValueArrayEntriesInDst);
    if (opKvEntryCopyMapping == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    opMeta->newFieldIdxs = newFieldIdxs;
    opMeta->dag = dag;
    opMeta->numVariables = numVariables;
    opMeta->numEvals = numEvals;
    opMeta->evalStrings = evalStrings;
    opMeta->evalArgIndices = evalArgIndices;
    opMeta->srcXdbId = srcXdbId;
    opMeta->srcXdb = srcXdb;
    opMeta->srcMeta = srcMeta;
    opMeta->dstXdbId = dstXdbId;
    opMeta->dstXdb = dstXdb;
    opMeta->dstMeta = dstMeta;
    opMeta->opStatus = opStatus;
    opMeta->op = op;

    opMeta->fatptrDemystRequired = fatptrDemystRequired;
    opMeta->insertState = insertState;

    opMeta->indexOnRowNum = indexOnRowNum;
    opMeta->dht = DhtMgr::get()->dhtGetDht(dstMeta->dhtId);

    opMeta->possibleImmediateIndices = possibleImmediateIndices;
    opMeta->validIndicesMap = validIndicesMap;
    opMeta->validIndices = validIndices;
    opMeta->variableNames = variableNames;
    opMeta->aggregateResults = aggregateResults;
    opMeta->numAggregateResults = numAggregateResults;
    opMeta->fatptrTransPageHandle = fatptrTransPageHandle;
    opMeta->indexHandle = indexHandle;
    opMeta->txn = Txn::currentTxn();

    opMeta->opKvEntryCopyMapping = opKvEntryCopyMapping;
    opMeta->scalarsSize = numVariables * sizeof(ScalarPtr);
    opMeta->childPool = childPool;

    atomicWrite64(&opMeta->status, StatusOk.code());

    if (flags & OperatorFlagIcvMode) {
        opMeta->icvMode = true;
    } else {
        opMeta->icvMode = false;
    }

    if (flags & OperatorFlagDropSrcSlots) {
        opMeta->dropSrcSlots = true;
    } else {
        opMeta->dropSrcSlots = false;
    }

    if (flags & OperatorFlagSerializeSlots) {
        opMeta->serializeSlots = true;
    } else {
        opMeta->serializeSlots = false;
    }

    if (flags & OperatorFlagIncludeSrcTableInSample) {
        opMeta->includeSrcTableSample = true;
    } else {
        opMeta->includeSrcTableSample = false;
    }

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (opKvEntryCopyMapping != NULL) {
            memFree(opKvEntryCopyMapping);
            opKvEntryCopyMapping = NULL;
        }

        if (semInited) {
            opMeta->sem->~Semaphore();
            memFree(opMeta->sem);
            opMeta->sem = NULL;
            semInited = false;
        }
    }

    return status;
}

void
Operators::opMetaDestroy(OpMeta *opMeta)
{
    assert(opMeta->opKvEntryCopyMapping != NULL);
    memFree(opMeta->opKvEntryCopyMapping);
    opMeta->opKvEntryCopyMapping = NULL;
    opMeta->sem->~Semaphore();
    memFree(opMeta->sem);
    opMeta->sem = NULL;
}

bool
Operators::columnMatch(XcalarApiRenameMap renameMap[],
                       unsigned numRenameEntries,
                       const char *srcName,
                       DfFieldType srcType,
                       const char *dstName,
                       DfFieldType dstType,
                       bool matchColsInRenameMapOnly)
{
    const char *nameToCompare = NULL;

    nameToCompare = srcName;
    if (renameMap != NULL) {
        unsigned ii;
        for (ii = 0; ii < numRenameEntries; ii++) {
            if (FatptrTypeMatch(renameMap[ii].type, srcType) &&
                strcmp(renameMap[ii].oldName, srcName) == 0) {
                nameToCompare = renameMap[ii].newName;
                break;
            }
        }

        if (matchColsInRenameMapOnly && ii == numRenameEntries) {
            // column not present in rename map, don't keep it
            return false;
        }
    }

    return strcmp(nameToCompare, dstName) == 0;
}

void
Operators::initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                  XdbMeta *srcMeta,
                                  XdbMeta *dstMeta,
                                  uint64_t startDstIdx,
                                  uint64_t endDstIdx,
                                  bool sameOrder,
                                  XcalarApiRenameMap renameMap[],
                                  unsigned numRenameEntries,
                                  bool keepColsInRenameMapOnly)
{
    const NewTupleMeta *srcTupMeta;
    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields = srcTupMeta->getNumFields();

    const char *fieldNames[srcNumFields];
    for (unsigned ii = 0; ii < srcNumFields; ii++) {
        fieldNames[ii] = srcMeta->kvNamedMeta.valueNames_[ii];
    }

    initKvEntryCopyMapping(kvEntryCopyMapping,
                           srcMeta->kvNamedMeta.kvMeta_.tupMeta_,
                           fieldNames,
                           dstMeta,
                           startDstIdx,
                           endDstIdx,
                           sameOrder,
                           renameMap,
                           numRenameEntries,
                           keepColsInRenameMapOnly);
}

// create a mapping from the src column indexes to the dst column indexes
void
Operators::initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                  const NewTupleMeta *srcTupMeta,
                                  const char **srcFieldNames,
                                  XdbMeta *dstMeta,
                                  uint64_t startDstIdx,
                                  uint64_t endDstIdx,
                                  bool sameOrder,
                                  XcalarApiRenameMap renameMap[],
                                  unsigned numRenameEntries,
                                  bool keepColsInRenameMapOnly)
{
    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields = dstTupMeta->getNumFields();

    const char *dstFieldNames[dstNumFields];
    for (unsigned ii = 0; ii < dstNumFields; ii++) {
        dstFieldNames[ii] = dstMeta->kvNamedMeta.valueNames_[ii];
    }

    initKvEntryCopyMapping(kvEntryCopyMapping,
                           srcTupMeta,
                           srcFieldNames,
                           dstTupMeta,
                           dstFieldNames,
                           startDstIdx,
                           endDstIdx,
                           sameOrder,
                           renameMap,
                           numRenameEntries,
                           keepColsInRenameMapOnly);
}

void
Operators::initKvEntryCopyMapping(OpKvEntryCopyMapping *kvEntryCopyMapping,
                                  const NewTupleMeta *srcTupMeta,
                                  const char **srcFieldNames,
                                  const NewTupleMeta *dstTupMeta,
                                  const char **dstFieldNames,
                                  uint64_t startDstIdx,
                                  uint64_t endDstIdx,
                                  bool sameOrder,
                                  XcalarApiRenameMap renameMap[],
                                  unsigned numRenameEntries,
                                  bool keepColsInRenameMapOnly)
{
    uint64_t srcCursor, dstCursor;

    assert(kvEntryCopyMapping != NULL);
    assert(endDstIdx >= startDstIdx);
    assert(endDstIdx <= kvEntryCopyMapping->maxNumEntries);

    size_t srcNumFields = srcTupMeta->getNumFields();
    size_t dstNumFields = dstTupMeta->getNumFields();

    if (srcNumFields != dstNumFields) {
        kvEntryCopyMapping->isReplica = false;
    }

    for (dstCursor = startDstIdx; dstCursor < endDstIdx; dstCursor++) {
        const char *dstName = dstFieldNames[dstCursor];
        DfFieldType dstType = dstTupMeta->getFieldType(dstCursor);

        for (srcCursor = 0; srcCursor < srcNumFields; srcCursor++) {
            const char *srcName = srcFieldNames[srcCursor];
            DfFieldType srcType = srcTupMeta->getFieldType(srcCursor);

            if (FatptrTypeMatch(dstType, srcType) &&
                columnMatch(renameMap,
                            numRenameEntries,
                            srcName,
                            srcType,
                            dstName,
                            dstType,
                            keepColsInRenameMapOnly)) {
                kvEntryCopyMapping->srcIndices[dstCursor] = srcCursor;
                kvEntryCopyMapping->numEntries++;

                if (dstCursor - startDstIdx != srcCursor) {
                    // the match was not found at the same index
                    kvEntryCopyMapping->isReplica = false;
                }

                break;
            }
        }

        if (srcCursor == srcNumFields) {
            if (sameOrder) {
                // this means there is something in dst that could not be
                // found this src. Since we are expected the same field order
                // in src and dst we can break and try to find it in the next
                // src
                kvEntryCopyMapping->isReplica = false;
                break;
            } else {
                // couldn't find a matching field in src
                kvEntryCopyMapping->srcIndices[dstCursor] =
                    NewTupleMeta::DfInvalidIdx;
                kvEntryCopyMapping->numEntries++;
            }
        }
    }
}

const char *
Operators::columnNewName(XcalarApiRenameMap *renameMap,
                         unsigned numRenameEntries,
                         const char *srcName,
                         DfFieldType srcType)
{
    if (renameMap != NULL) {
        for (unsigned ii = 0; ii < numRenameEntries; ii++) {
            if (FatptrTypeMatch(renameMap[ii].type, srcType) &&
                strcmp(renameMap[ii].oldName, srcName) == 0) {
                return renameMap[ii].newName;
            }
        }
    }

    return srcName;
}

TransportPage **
Operators::allocSourceTransPages()
{
    unsigned numNodes = Config::get()->getActiveNodes();
    TransportPage **pages =
        (TransportPage **) memAlloc(sizeof(*pages) * numNodes);
    if (!pages) {
        return NULL;
    }
    memZero(pages, sizeof(*pages) * numNodes);

    return pages;
}

Status
Operators::issueWork(Xdb *srcXdb,
                     XdbMeta *srcMeta,
                     Xdb *dstXdb,
                     XdbMeta *dstMeta,
                     OperatorsEnum operators,
                     OpHandle *opHandle,
                     OpMeta *opMeta,
                     unsigned numWorkers)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    OperatorsDemystifyWork **operatorsWorkers = NULL;
    TrackHelpers *trackHelpers = NULL;
    bool refAcquired = false;

    assert(srcXdb != NULL);
    assert(srcMeta != NULL);
    assert(dstXdb != NULL);
    assert(dstMeta != NULL);

    if (opHandle->rowsToProcess == 0) {
        goto CommonExit;
    }

    trackHelpers =
        TrackHelpers::setUp(&workerStatus, numWorkers, opHandle->numSlots);
    BailIfNull(trackHelpers);

    // track all rowMetas with one reference, this prevents the unsorted
    // pages from getting freed and the xdb from getting rehashed
    // groupBy uses sorted groupMetas so this won't be an issue
    if (operators != OperatorsGroupBy) {
        status = CursorManager::get()->incUnsortedRef(srcMeta->xdbId);
        BailIfFailed(status);
        refAcquired = true;
    }

    operatorsWorkers = new (std::nothrow) OperatorsDemystifyWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsDemystifyWork *operatorsWork = NULL;
        switch (operators) {
        case OperatorsIndexTable:
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsIndexTableWork((ii == numWorkers - 1)
                                            ? TrackHelpers::Master
                                            : TrackHelpers::NonMaster,
                                        ii,
                                        &opHandle->indexHandle);
            break;
        case OperatorsFilter:
        case OperatorsMap:
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsFilterAndMapWork((ii == numWorkers - 1)
                                              ? TrackHelpers::Master
                                              : TrackHelpers::NonMaster,
                                          ii);
            break;
        case OperatorsGroupBy:
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsGroupByWork((ii == numWorkers - 1)
                                         ? TrackHelpers::Master
                                         : TrackHelpers::NonMaster,
                                     ii);
            break;
        case OperatorsSynthesizeTable:
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsCreateScalarTableWork((ii == numWorkers - 1)
                                                   ? TrackHelpers::Master
                                                   : TrackHelpers::NonMaster,
                                               ii,
                                               true);
            break;
        case OperatorsAggregate:  // Pass through
        case OperatorsExport:
            operatorsWorkers[ii] = new (std::nothrow)
                OperatorsCreateScalarTableWork((ii == numWorkers - 1)
                                                   ? TrackHelpers::Master
                                                   : TrackHelpers::NonMaster,
                                               ii,
                                               false);
            break;
        default:
            assert(0);
            break;
        }
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->dropSrcSlots = opMeta->dropSrcSlots;
        operatorsWork->serializeSlots = opMeta->serializeSlots;
        operatorsWork->srcXdbId = opMeta->srcXdbId;
        operatorsWork->dstXdbId = opMeta->dstXdbId;
        operatorsWork->opMeta = opMeta;
        operatorsWork->op = operators;
        operatorsWork->srcXdb = srcXdb;
        operatorsWork->trackHelpers = trackHelpers;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    Status status2;
    if (opMeta->fatptrDemystRequired) {
        status2 = opHandle->fatptrHandle.waitForCompletions();
        if (status == StatusOk && status2 != StatusOk) {
            status = status2;
        }
    }

    status2 = opHandle->indexHandle.waitForCompletions();
    if (status2 != StatusOk && status == StatusOk) {
        status = status2;
    }

    DemystifyMgr::get()->removeDemystifyContext(Txn::currentTxn());

    if (refAcquired) {
        CursorManager::get()->decUnsortedRef(srcMeta->xdbId);
        refAcquired = false;
    }

    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    return status;
}

// this function prepares some metaData, handles the
// opStatus (cancel, get progress) and
// issues rows to be processed in chunks of OpMaxConcurrentRows
// it is called when we begin processing an operator locally
Status
Operators::initiateOp(Operators::PerTxnInfo *perTxnInfo,
                      unsigned numEvals,
                      char **newFieldNames,
                      char **evalStrings,
                      int **evalArgIndices,
                      OperatorsEnum operators,
                      uint64_t numFunc,
                      EvalUdfModuleSet *udfModules,
                      const char *userIdName,
                      uint64_t sessionId)
{
    Status status = StatusOk;
    bool fatptrDemystRequired = false;
    bool fatptrHandleInit = false;
    bool indexHandleInit = false;
    bool **validIndicesMap = NULL;
    OpStatus *opStatus = NULL;
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    PerOpGlobalInput *perOpGlobalIp = &perTxnInfo->perOpGlobIp;
    XdbId srcXdbId = perOpGlobalIp->srcXdbId;
    XdbId dstXdbId = perOpGlobalIp->dstXdbId;
    unsigned numAggregateResults = 0;
    OpPossibleImmediateIndices *possibleImmediateIndices = NULL;
    OperatorsAggregateResult *aggregateResults = NULL;
    DagLib *dagLib = DagLib::get();
    DagTypes::DagId dagId = perOpGlobalIp->dagId;
    Dag *dag = dagLib->getDagLocal(dagId);
    bool *validIndices = NULL;
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numVariables = demystifyInput->numVariables;
    char *variableNames[numVariables];
    ChildPool childPool;
    int newFieldIdxs[numEvals];
    OpHandle opHandle;
    OpMeta opMeta;
    bool opMetaInited = false;
    size_t dmystInputSize = 0;
    char stringPtr[demystifyInput->inputStringsSize];
    char *stringPtrTmp = stringPtr;

    memcpy(stringPtr,
           demystifyInput->inputStrings,
           demystifyInput->inputStringsSize);
    for (unsigned ii = 0; ii < numVariables; ii++) {
        variableNames[ii] = stringPtrTmp;
        stringPtrTmp += strlen(stringPtrTmp) + 1;
    }
    demystifyInput->opMeta = &opMeta;
    dmystInputSize =
        sizeof(DsDemystifyTableInput) + demystifyInput->inputStringsSize;

    // This should always work since we've already checked the ID
    status = dag->getOpStatusFromXdbId(dstXdbId, &opStatus);
    assert(status == StatusOk);
    BailIfFailed(status);

    // It is barely conceivable that xdb get for the source could fail, but
    // get for the output table should always succeed since this operation's
    // global operator controls it.
    Xdb *srcXdb, *dstXdb;
    XdbMeta *srcMeta, *dstMeta;
    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    assert(status == StatusOk);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);
    BailIfFailed(status);

    const NewTupleMeta *srcTupMeta;
    srcTupMeta = srcMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t srcNumFields;
    srcNumFields = srcTupMeta->getNumFields();

    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields;
    dstNumFields = dstTupMeta->getNumFields();

    if (XdbMgr::xdbGetNumLocalRows(srcXdb) == 0) {
        goto CommonExit;
    }

    bool sameKeyHash;
    sameKeyHash =
        (operators != OperatorsAggregate && operators != OperatorsExport &&
         operators != OperatorsIndexTable);
    if (sameKeyHash) {
        // we should have the same min/max if we have the same key hash
        xdbMgr->xdbCopyMinMax(srcXdb, dstXdb);
    }

    validIndices = new (std::nothrow) bool[srcNumFields];
    BailIfNull(validIndices);
    memZero(validIndices, srcNumFields * sizeof(bool));

    validIndicesMap =
        (bool **) memAlloc(numVariables * sizeof(*validIndicesMap));
    BailIfNullWith(validIndicesMap, StatusNoMem);

    memZero(validIndicesMap, numVariables * sizeof(*validIndicesMap));
    for (unsigned ii = 0; ii < numVariables; ii++) {
        validIndicesMap[ii] =
            (bool *) memAlloc(srcNumFields * sizeof(*validIndicesMap[ii]));
        BailIfNullWith(validIndicesMap[ii], StatusNoMem);

        memZero(validIndicesMap[ii],
                srcNumFields * sizeof(*validIndicesMap[ii]));
    }

    possibleImmediateIndices = (OpPossibleImmediateIndices *) memAlloc(
        numVariables * sizeof(possibleImmediateIndices[0]));
    if (possibleImmediateIndices == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate possibleImmediateIndices "
                "(numVariables: %u)",
                numVariables);
        status = StatusNoMem;
        goto CommonExit;
    }

    aggregateResults = (OperatorsAggregateResult *)
        memAllocExt(numVariables * sizeof(*aggregateResults), moduleName);
    if (aggregateResults == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate aggregateResults "
                "(numVariables: %u)",
                numVariables);
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(aggregateResults, numVariables * sizeof(*aggregateResults));

    numAggregateResults = 0;
    // If numImmediates == 0, the only values in a valueArray are fatptrs
    for (unsigned ii = 0; ii < numVariables; ii++) {
        possibleImmediateIndices[ii].numPossibleImmediateIndices = 0;

        if (*variableNames[ii] == OperatorsAggregateTag) {
            // this variable comes from an aggregate
            XdbId aggId;

            // XXX TODO May need to support fully qualified table names
            status = dag->getDagNodeId(&variableNames[ii][1],
                                       Dag::TableScope::LocalOnly,
                                       &aggId);
            BailIfFailed(status);

            assert(numAggregateResults < numVariables);
            aggregateResults[numAggregateResults].aggId = aggId;

            strlcpy(aggregateResults[numAggregateResults].aggName,
                    variableNames[ii],
                    sizeof(aggregateResults[numAggregateResults].aggName));

            status = dag->getScalarResult(aggId,
                                          &aggregateResults[numAggregateResults]
                                               .result);
            if (status == StatusAggregateResultNotFound) {
                // this isn't an aggregate, so it must be a table
                // stick the table's xdbId as the agg result
                XdbId xdbId;

                // XXX TODO Do we need to support fully qualified table names?
                status = dag->getXdbId(&variableNames[ii][1],
                                       Dag::TableScope::LocalOnly,
                                       &xdbId);
                BailIfFailed(status);

                DfFieldValue val;
                val.int64Val = xdbId;

                aggregateResults[numAggregateResults].result =
                    Scalar::allocScalar(DfMaxFieldValueSize);
                BailIfNullWith(aggregateResults[numAggregateResults].result,
                               StatusNoMem);

                status = aggregateResults[numAggregateResults]
                             .result->setValue(val, DfInt64);
            }
            BailIfFailed(status);

            aggregateResults[numAggregateResults].variableIdx = ii;
            numAggregateResults++;
            // no need to check for immediates
            continue;
        }

        // use accessor names to compare with immediate names. The accessor
        // will be properly unescaped
        AccessorNameParser accessorParser;
        Accessor accessors;
        status = accessorParser.parseAccessor(variableNames[ii], &accessors);
        BailIfFailed(status);

        assert(accessors.nameDepth > 0);
        if (accessors.nameDepth <= 0) {
            continue;
        }

        char tmpName[DfMaxFieldNameLen + 1];
        int nameLen = 0;
        for (int kk = 0; kk < accessors.nameDepth; kk++) {
            switch (accessors.names[kk].type) {
            case AccessorName::Type::Field:
                if (kk > 0) {
                    tmpName[nameLen] = '.';
                    nameLen += 1;
                }
                strlcpy(tmpName + nameLen,
                        accessors.names[kk].value.field,
                        sizeof(tmpName) - nameLen);
                nameLen += strlen(accessors.names[kk].value.field);
                break;
            case AccessorName::Type::Subscript: {
                int numChars = snprintf(tmpName + nameLen,
                                        sizeof(tmpName) - nameLen,
                                        "[%u]",
                                        accessors.names[kk].value.subscript);
                nameLen += numChars;
                break;
            }
            default:
                break;
            }
            if (nameLen >= (int) sizeof(tmpName)) {
                xSyslog(moduleName,
                        XlogErr,
                        "Field name: \'%s\' too long.",
                        variableNames[ii]);
                status = StatusNoBufs;
                goto CommonExit;
            }
        }

        // check if we need demystification and fill in immediate indices
        for (unsigned jj = 0; jj < srcNumFields; jj++) {
            if (srcTupMeta->getFieldType(jj) != DfFatptr) {
                if (strcmp(srcMeta->kvNamedMeta.valueNames_[jj], tmpName) ==
                    0) {
                    // This variable is local
                    possibleImmediateIndices[ii].possibleImmediateIndices
                        [possibleImmediateIndices[ii]
                             .numPossibleImmediateIndices] = jj;
                    possibleImmediateIndices[ii].numPossibleImmediateIndices++;
                }
            }
        }

        if (possibleImmediateIndices[ii].numPossibleImmediateIndices == 0 &&
            srcMeta->numFatptrs > 0) {
            // Unfortunately, the variable we're looking for is not an
            // immediate, and thus must be a field provided by a dataset,
            // so a fatptr deymstification is required
            fatptrDemystRequired = true;
        }
    }

    if (numVariables == 0) {
        assert(!fatptrDemystRequired);
    }

    if (numVariables == 1 &&
        strcmp(variableNames[0], DsDefaultDatasetKeyName) == 0) {
        // special case where we are operating on the recordNum field
        fatptrDemystRequired = false;
    }

    if (numVariables >= TupleMaxNumValuesPerRecord - 1) {
        // Need to reserve one field for the RowMeta ptr.
        // XXX TODO this need to be fixed to have internal reserved metadata
        // fields instead.
        status = StatusMaxFieldSizeExceeded;
        goto CommonExit;
    }

    XdbInsertKvState insertState;
    if (fatptrDemystRequired == false && sameKeyHash) {
        // since we are pure local, and use the same hash function as srcTable,
        // we can hash during insert and do it with no lock
        dstMeta->prehashed = true;
        insertState = XdbInsertSlotHash;
    } else {
        insertState = XdbInsertRandomHash;
    }

    // parse variable names for prefixes after populating demystifyInput
    status =
        DataFormat::get()->createValidIndicesMap(srcMeta,
                                                 numVariables,
                                                 variableNames,
                                                 (bool **) validIndicesMap);
    BailIfFailed(status);

    // flatten validIndicesMap and store in validIndices
    for (unsigned ii = 0; ii < numVariables; ii++) {
        for (unsigned jj = 0; jj < srcNumFields; jj++) {
            if (validIndicesMap[ii][jj]) {
                validIndices[jj] = true;
            }
        }
    }

    status = opHandle.fatptrHandle
                 .initHandle(MsgTypeId::Msg2pcDemystifySourcePage,
                             TwoPcCallId::Msg2pcDemystifySourcePage1,
                             perTxnInfo->remoteCookies,
                             dstXdbId,
                             NULL);
    BailIfFailed(status);
    fatptrHandleInit = true;

    status = opHandle.indexHandle.initHandle(MsgTypeId::Msg2pcIndexPage,
                                             TwoPcCallId::Msg2pcIndexPage1,
                                             perTxnInfo->remoteCookies,
                                             dstXdbId,
                                             NULL);
    BailIfFailed(status);
    indexHandleInit = true;

    status = Operators::get()->operatorsCountLocal(srcXdbId,
                                                   false,
                                                   &opHandle.rowsToProcess);
    BailIfFailed(status);

    opHandle.numSlots = xdbMgr->xdbGetNumHashSlots(srcXdb);
    opHandle.evalStrings = evalStrings;

    if (operators == OperatorsAggregate) {
        // need to update the total work for aggregate now that we
        // know how many functions we need to execute
        opStatus->atomicOpDetails.numWorkTotal +=
            opHandle.rowsToProcess * numFunc;
    }

    // check if we have been cancelled before issue
    if (unlikely(opStatus->atomicOpDetails.cancelled)) {
        status = StatusCanceled;
        goto CommonExit;
    }

    if (unlikely(usrNodeNormalShutdown())) {
        status = StatusShutdownInProgress;
        goto CommonExit;
    }

    if (newFieldNames != NULL) {
        NewKeyValueNamedMeta *meta = &dstMeta->kvNamedMeta;
        for (unsigned ii = 0; ii < numEvals; ii++) {
            bool found = false;

            for (unsigned jj = 0; jj < dstNumFields; jj++) {
                if (strncmp(newFieldNames[ii],
                            meta->valueNames_[jj],
                            sizeof(meta->valueNames_[jj])) == 0 &&
                    dstTupMeta->getFieldType(jj) != DfFatptr) {
                    newFieldIdxs[ii] = jj;
                    found = true;
                    break;
                }
            }

            // dstMeta should have been populated with all our field names
            assert(found);
        }
    }

    unsigned numWorkers;
    numWorkers = Operators::getNumWorkers(opHandle.rowsToProcess);

    // Get the pool of children for this operation
    // XXX TODO
    // Childpool is per operation abstraction. Does this need to be a global
    // abstraction to limit operation concurrency?
    bool childPoolInited;
    childPoolInited = false;
    if (udfModules) {
        status = childPool.init(numWorkers, udfModules, userIdName, sessionId);
        BailIfFailed(status);
        childPoolInited = true;
    }

    status = opMetaInit(&opMeta,
                        dag,
                        newFieldIdxs,
                        numVariables,
                        variableNames,
                        numEvals,
                        evalStrings,
                        evalArgIndices,
                        srcXdbId,
                        srcXdb,
                        srcMeta,
                        dstXdbId,
                        dstXdb,
                        dstMeta,
                        opStatus,
                        operators,
                        fatptrDemystRequired,
                        insertState,
                        possibleImmediateIndices,
                        validIndicesMap,
                        validIndices,
                        aggregateResults,
                        numAggregateResults,
                        &childPool,
                        &opHandle.fatptrHandle,
                        &opHandle.indexHandle,
                        perOpGlobalIp->opFlag);
    BailIfFailed(status);
    opMetaInited = true;
    opStatus->opMeta = &opMeta;

    if (operators == OperatorsMap ||
        (operators == OperatorsGroupBy &&
         perOpGlobalIp->opFlag & OperatorFlagIncludeSrcTableInSample)) {
        uint64_t endDstIdx;
        int lastFieldIdx = newFieldIdxs[0];
        for (unsigned ii = 1; ii < numEvals; ii++) {
            if (newFieldIdxs[ii] > lastFieldIdx) {
                lastFieldIdx = newFieldIdxs[ii];
            }
        }

        if (lastFieldIdx != (int) dstNumFields - 1) {
            // this means we updated an existing field
            endDstIdx = dstNumFields;
        } else {
            endDstIdx = dstNumFields - 1;
        }
        initKvEntryCopyMapping(opMeta.opKvEntryCopyMapping,
                               srcMeta,
                               dstMeta,
                               0,
                               endDstIdx,
                               true,
                               NULL,
                               0);
    } else if (operators == OperatorsFilter ||
               operators == OperatorsIndexTable) {
        initKvEntryCopyMapping(opMeta.opKvEntryCopyMapping,
                               srcMeta,
                               dstMeta,
                               0,
                               dstNumFields,
                               true,
                               NULL,
                               0);
    } else {
        assert(operators == OperatorsSynthesizeTable ||
               operators == OperatorsAggregate ||
               operators == OperatorsGroupBy || operators == OperatorsExport);
    }

    status = issueWork(srcXdb,
                       srcMeta,
                       dstXdb,
                       dstMeta,
                       operators,
                       &opHandle,
                       &opMeta,
                       numWorkers);

    BailIfFailed(status);
    // Transport page handle is destroyed by the shipping thread.
    fatptrHandleInit = false;

    if (operators == OperatorsGroupBy) {
        xdbMgr->xdbResetDensityStats(srcXdb);
    }

CommonExit:
    if (opMetaInited) {
        if (status == StatusOk &&
            atomicRead64(&opMeta.status) != StatusCodeOk) {
            status.fromStatusCode((StatusCode) atomicRead64(&opMeta.status));
        }

        opStatus->opMeta = NULL;
        memBarrier();

        opMetaDestroy(&opMeta);
    }

    if (status != StatusOk) {
        // Need to clean up any scratchPad xdbs
        XdbMgr::get()->xdbFreeScratchPadXdbs(dstXdb);
    }

    if (fatptrHandleInit) {
        opHandle.fatptrHandle.destroy();
        fatptrHandleInit = false;
    }

    if (indexHandleInit) {
        opHandle.indexHandle.destroy();
        indexHandleInit = false;
    }

    if (possibleImmediateIndices != NULL) {
        memFree(possibleImmediateIndices);
        possibleImmediateIndices = NULL;
    }

    if (validIndicesMap != NULL) {
        for (unsigned ii = 0; ii < numVariables; ii++) {
            if (validIndicesMap[ii] != NULL) {
                memFree(validIndicesMap[ii]);
                validIndicesMap[ii] = NULL;
            }
        }
        memFree(validIndicesMap);
        validIndicesMap = NULL;
    }

    if (validIndices != NULL) {
        delete[] validIndices;
        validIndices = NULL;
    }

    if (aggregateResults != NULL) {
        for (unsigned ii = 0; ii < numAggregateResults; ii++) {
            if (aggregateResults[ii].result != NULL) {
                Scalar::freeScalar(aggregateResults[ii].result);
            }
        }
        memFree(aggregateResults);
        aggregateResults = NULL;
    }

    if (childPoolInited) {
        childPool.destroy();
        childPoolInited = false;
    }

    if (opStatus->atomicOpDetails.cancelled) {
        // StatusCanceled beats all other statuses
        status = StatusCanceled;
    }

    return status;
}

Status
Operators::operatorsInitOpStatus(OpStatus *opStatus,
                                 XcalarApis api,
                                 XcalarApiInput *apiInput)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    Xdb *srcXdb;

    opStatus->opMeta = NULL;
    opStatus->atomicOpDetails.cancelled = false;
    atomicWrite64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, 0);

    memZero(&opStatus->atomicOpDetails.errorStats,
            sizeof(opStatus->atomicOpDetails.errorStats));
    if (apiInput == NULL) {
        goto CommonExit;
    }

    switch (api) {
    case XcalarApiIndex:
        if (apiInput->indexInput.source.isTable) {
            status =
                xdbMgr->xdbGet(apiInput->indexInput.source.xid, &srcXdb, NULL);
            BailIfFailed(status);

            opStatus->atomicOpDetails.numWorkTotal =
                xdbMgr->xdbGetNumLocalRows(srcXdb);

            if (apiInput->indexInput.keys[0].ordering & SortedFlag) {
                // sorting has 3 additional steps, agg(min), agg(max) and sort
                opStatus->atomicOpDetails.numWorkTotal *= 4;
            }

        } else {
            DsDataset *dataset;
            dataset = Dataset::get()
                          ->getDatasetFromId(apiInput->indexInput.source.xid,
                                             &status);
            BailIfFailed(status);

            opStatus->atomicOpDetails.numWorkTotal =
                dataset->pageIndex_->getNumRecords();
        }

        break;

    case XcalarApiJoin:
        Xdb *leftXdb;
        status =
            xdbMgr->xdbGet(apiInput->joinInput.leftTable.xdbId, &leftXdb, NULL);
        BailIfFailed(status);
        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(leftXdb);
        break;

    case XcalarApiUnion:
        opStatus->atomicOpDetails.numWorkTotal = 0;
        xcalarApiDeserializeUnionInput(&apiInput->unionInput);
        for (unsigned ii = 0; ii < apiInput->unionInput.numSrcTables; ii++) {
            Xdb *xdb;

            status = xdbMgr->xdbGet(apiInput->unionInput.srcTables[ii].xdbId,
                                    &xdb,
                                    NULL);
            BailIfFailed(status);

            opStatus->atomicOpDetails.numWorkTotal +=
                xdbMgr->xdbGetNumLocalRows(xdb);
        }

        break;

    case XcalarApiProject:
        status = xdbMgr->xdbGet(apiInput->projectInput.srcTable.xdbId,
                                &srcXdb,
                                NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);

        break;

    case XcalarApiFilter:
        status =
            xdbMgr->xdbGet(apiInput->filterInput.srcTable.xdbId, &srcXdb, NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);
        break;

    case XcalarApiGroupBy:
        status = xdbMgr->xdbGet(apiInput->groupByInput.srcTable.xdbId,
                                &srcXdb,
                                NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);

        break;

    case XcalarApiMap:
        status =
            xdbMgr->xdbGet(apiInput->mapInput.srcTable.xdbId, &srcXdb, NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);
        break;

    case XcalarApiSynthesize:
        if (apiInput->synthesizeInput.source.isTable) {
            status = xdbMgr->xdbGet(apiInput->synthesizeInput.source.xid,
                                    &srcXdb,
                                    NULL);
            BailIfFailed(status);

            opStatus->atomicOpDetails.numWorkTotal =
                xdbMgr->xdbGetNumLocalRows(srcXdb);
        } else {
            DsDataset *dataset;
            dataset = Dataset::get()->getDatasetFromId(apiInput->synthesizeInput
                                                           .source.xid,
                                                       &status);
            BailIfFailed(status);

            opStatus->atomicOpDetails.numWorkTotal =
                dataset->pageIndex_->getNumRecords();
        }

        break;

    case XcalarApiGetRowNum:
        status = xdbMgr->xdbGet(apiInput->getRowNumInput.srcTable.xdbId,
                                &srcXdb,
                                NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);
        break;

    case XcalarApiAggregate:
        status = xdbMgr->xdbGet(apiInput->aggregateInput.srcTable.xdbId,
                                &srcXdb,
                                NULL);
        BailIfFailed(status);

        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb);

        break;

    case XcalarApiExport:
        status =
            xdbMgr->xdbGet(apiInput->exportInput.srcTable.xdbId, &srcXdb, NULL);
        BailIfFailed(status);

        // export has 2 steps, createScalarTable and export
        opStatus->atomicOpDetails.numWorkTotal =
            xdbMgr->xdbGetNumLocalRows(srcXdb) * 2;
        break;

    case XcalarApiSelect: {
        HashTree *hashTree = HashTreeMgr::get()->getHashTreeById(
            apiInput->selectInput.srcTable.tableId);
        if (!hashTree) {
            status = StatusPubTableNameNotFound;
            goto CommonExit;
        }

        // for select, treat each slot as a work item
        opStatus->atomicOpDetails.numWorkTotal = hashTree->getNumSlots();
    }
    default:
        opStatus->atomicOpDetails.numWorkTotal = 0;
        break;
    }

CommonExit:
    if (status != StatusOk) {
        opStatus->atomicOpDetails.numWorkTotal = 0;
    }

    return status;
}

void
Operators::operatorsAggregateLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiAggregateInput *aggregateInput =
        (XcalarApiAggregateInput *) opInput->buf;
    XcalarEvalClass2Ast ast2;
    bool ast2Created = false;
    Status status = StatusOk;
    uint64_t numFunc = 0;
    XcalarEval *xcalarEval = XcalarEval::get();
    const XdbId srcXdbId = aggregateInput->srcTable.xdbId;
    const XdbId dstXdbId = aggregateInput->dstTable.xdbId;
    uint64_t numRows = 0;
    PerTxnInfo *perTxnInfo = getPerTxnInfo();
    DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;
    Stopwatch stopwatch;
    logOperationStage(__func__, status, stopwatch, Begin);

    status = operatorsCountLocal(srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get number of rows in srcTable %lu: %s",
                srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        // Empty table. Nothing to do
        assert(status == StatusOk);
        goto CommonExit;
    }

    status = xcalarEval->generateClass2Ast(aggregateInput->evalStr,
                                           &ast2,
                                           &numFunc,
                                           opInput->dagId,
                                           dstXdbId);
    BailIfFailed(status);
    ast2Created = true;

    {
        int evalArgIndices[demystifyInput->numVariables];
        for (int ii = 0; ii < (int) demystifyInput->numVariables; ii++) {
            evalArgIndices[ii] = ii;
        }

        char *evalString = aggregateInput->evalStr;

        status = initiateOp(perTxnInfo,
                            1,
                            NULL,
                            &evalString,
                            (int **) &evalArgIndices,
                            OperatorsAggregate,
                            numFunc,
                            NULL,
                            opInput->userIdName,
                            opInput->sessionId);
        BailIfFailed(status);
    }
CommonExit:
    if (ast2Created) {
        xcalarEval->destroyClass2Ast(&ast2);
        ast2Created = false;
    }

    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

void
Operators::operatorsExportLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    ExExportBuildScalarTableInput *buildInput =
        (ExExportBuildScalarTableInput *) opInput->buf;
    Status status = StatusOk;
    uint64_t numRows = 0;
    PerTxnInfo *perTxnInfo = getPerTxnInfo();
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    status = operatorsCountLocal(buildInput->srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get numRows for srcTable %lu: %s",
                buildInput->srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        assert(status == StatusOk);
        goto CommonExit;
    }

    // export is a 2-step process, this is only the first step
    status = initiateOp(perTxnInfo,
                        0,
                        NULL,
                        NULL,
                        NULL,
                        OperatorsExport,
                        0,
                        NULL,
                        opInput->userIdName,
                        opInput->sessionId);

CommonExit:
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

Status
OperatorsProjectWork::setUp()
{
    Status status;
    XdbMeta *dstMeta;
    status = XdbMgr::get()->xdbGet(this->dstXdbId, NULL, &dstMeta);
    assert(status == StatusOk);

    status = opGetInsertHandle(&this->insertHandle,
                               &dstMeta->loadInfo,
                               this->insertState);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
OperatorsProjectWork::tearDown()
{
    opPutInsertHandle(&this->insertHandle);
}

Status
OperatorsProjectWork::doWork()
{
    OperatorsProjectWork *operatorsWork = this;
    XdbId srcXdbId;
    TableCursor srcCursor;
    bool srcCursorInited = false;
    Xdb *srcXdb, *dstXdb;
    XdbMeta *srcMeta, *dstMeta;
    int64_t numRecords = 0;
    Status status = StatusUnknown;
    NewKeyValueEntry srcKvEntry;
    NewKeyValueEntry dstKvEntry;
    int64_t rowNum = operatorsWork->startRecord;
    int64_t slotId = operatorsWork->slotId;
    OpStatus *opStatus = operatorsWork->opStatus;
    XdbMgr *xdbMgr = XdbMgr::get();

    srcXdbId = operatorsWork->srcXdbId;
    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(operatorsWork->dstXdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;

    new (&srcKvEntry) NewKeyValueEntry(&srcMeta->kvNamedMeta.kvMeta_);
    new (&dstKvEntry) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);

    unsigned workCounter;
    workCounter = 0;

    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields;
    dstNumFields = dstTupMeta->getNumFields();

    while (numRecords < operatorsWork->numRecords) {
        if (unlikely(numRecords % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }
        status = srcCursor.getNext(&srcKvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        dstKvEntry.init();
        Operators::shallowCopyKvEntry(&dstKvEntry,
                                      dstNumFields,
                                      &srcKvEntry,
                                      this->kvEntryCopyMapping,
                                      0,
                                      this->kvEntryCopyMapping->numEntries);
        numRecords++;
        workCounter++;
        status = opPopulateInsertHandle(&this->insertHandle, &dstKvEntry);
        BailIfFailed(status);
    }

    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                this->insertState,
                                this->srcXdb,
                                dstXdb,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (status == StatusNoData) {
        assert(numRecords == operatorsWork->numRecords);

        status = StatusOk;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }
    return status;
}

void
Operators::operatorsProjectLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiProjectInput *projectInput =
        (XcalarApiProjectInput *) opInput->buf;
    Status status = StatusOk;
    XdbId srcXdbId = projectInput->srcTable.xdbId;
    XdbId dstXdbId = projectInput->dstTable.xdbId;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbMeta *srcMeta, *dstMeta;
    Xdb *srcXdb, *dstXdb;
    Stopwatch stopwatch;
    logOperationStage(__func__, status, stopwatch, Begin);

    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    status = projectHelper(opInput, srcXdb, srcMeta, dstXdb, dstMeta);

    BailIfFailed(status);

CommonExit:
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

Status
Operators::projectHelper(OperatorsApiInput *opInput,
                         Xdb *srcXdb,
                         XdbMeta *srcMeta,
                         Xdb *dstXdb,
                         XdbMeta *dstMeta)
{
    Status status = StatusOk;
    Status workerStatus = StatusOk;

    DagTypes::DagId dagId = opInput->dagId;
    XcalarApiProjectInput *projectInput =
        (XcalarApiProjectInput *) opInput->buf;
    DagLib *dagLib = DagLib::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    OperatorsProjectWork **operatorsWorkers = NULL;
    XdbId srcXdbId = projectInput->srcTable.xdbId;
    XdbId dstXdbId = projectInput->dstTable.xdbId;
    TrackHelpers *trackHelpers = NULL;
    OpStatus *opStatus;
    unsigned numWorkers = 0;
    uint64_t numRows = 0;
    XdbInsertKvState insertState;
    OpKvEntryCopyMapping *copyMapping = NULL;

    status = operatorsCountLocal(srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get numRows for srcTable %lu: %s",
                srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        assert(status == StatusOk);
        goto CommonExit;
    }

    dstMeta->prehashed = true;
    // The srcXdb and dstXdb are indexed on the same key
    xdbMgr->xdbCopyMinMax(srcXdb, dstXdb);
    insertState = XdbInsertSlotHash;

    unsigned dstNumEntries;
    dstNumEntries = dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();

    copyMapping = getOpKvEntryCopyMapping(dstNumEntries);
    BailIfNull(copyMapping);

    initKvEntryCopyMapping(copyMapping,
                           srcMeta,
                           dstMeta,
                           0,
                           dstNumEntries,
                           true,
                           NULL,
                           0);

    uint64_t numChunks;
    numChunks = xdbMgr->xdbGetNumHashSlots(srcXdb);

    status =
        (dagLib->getDagLocal(dagId))->getOpStatusFromXdbId(dstXdbId, &opStatus);
    BailIfFailed(status);

    numWorkers =
        Operators::getNumWorkers(opStatus->atomicOpDetails.numWorkTotal);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    BailIfNull(trackHelpers);

    // Check for shutdown before initiating a large amount of work
    if (usrNodeNormalShutdown()) {
        status = StatusShutdownInProgress;
        goto CommonExit;
    }

    operatorsWorkers = new (std::nothrow) OperatorsProjectWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsProjectWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsProjectWork((ii == numWorkers - 1)
                                     ? TrackHelpers::Master
                                     : TrackHelpers::NonMaster,
                                 ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->dropSrcSlots = opInput->flags & OperatorFlagDropSrcSlots;
        operatorsWork->serializeSlots =
            opInput->flags & OperatorFlagSerializeSlots;
        operatorsWork->srcXdbId = srcXdbId;
        operatorsWork->dstXdbId = dstXdbId;
        operatorsWork->insertState = insertState;

        operatorsWork->kvEntryCopyMapping = copyMapping;
        operatorsWork->opStatus = opStatus;
        operatorsWork->srcXdb = srcXdb;
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->op = OperatorsProject;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }
    if (copyMapping != NULL) {
        memFree(copyMapping);
        copyMapping = NULL;
    }

    return status;
}

Status
OperatorsGetRowNumWork::setUp()
{
    Status status;
    XdbMeta *dstMeta;
    status = XdbMgr::get()->xdbGet(this->dstXdbId, NULL, &dstMeta);
    assert(status == StatusOk);

    status = opGetInsertHandle(&this->insertHandle,
                               &dstMeta->loadInfo,
                               this->insertState);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
OperatorsGetRowNumWork::tearDown()
{
    opPutInsertHandle(&this->insertHandle);
}

Status
OperatorsGetRowNumWork::doWork()
{
    OperatorsGetRowNumWork *operatorsWork = this;
    XdbId srcXdbId;
    TableCursor srcCursor;
    bool srcCursorInited = false;
    Xdb *srcXdb, *dstXdb;
    XdbMeta *srcMeta, *dstMeta;
    Status status = StatusUnknown;
    NewKeyValueEntry srcKvEntry;
    NewKeyValueEntry dstKvEntry;

    int64_t rowNum = operatorsWork->startRecord;
    int64_t slotId = operatorsWork->slotId;
    OpStatus *opStatus = operatorsWork->opStatus;
    OpKvEntryCopyMapping *opKvEntryCopyMapping =
        operatorsWork->opKvEntryCopyMapping;
    XdbMgr *xdbMgr = XdbMgr::get();
    Operators *operators = Operators::get();

    srcXdbId = operatorsWork->srcXdbId;
    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(operatorsWork->dstXdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    status = CursorManager::get()->createOnSlot(srcXdbId,
                                                slotId,
                                                rowNum,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    srcCursorInited = true;

    new (&srcKvEntry) NewKeyValueEntry(&srcMeta->kvNamedMeta.kvMeta_);
    new (&dstKvEntry) NewKeyValueEntry(&dstMeta->kvNamedMeta.kvMeta_);

    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields;
    dstNumFields = dstTupMeta->getNumFields();

    DfFieldValue recordNum;
    uint64_t originalRecordNum;
    originalRecordNum = operatorsWork->nodeRowStart + rowNum;
    recordNum.int64Val = originalRecordNum;
    operatorsWork->numRecords += recordNum.int64Val;

    unsigned workCounter;
    workCounter = 0;
    while (recordNum.int64Val < operatorsWork->numRecords) {
        if (unlikely(recordNum.int64Val %
                         XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        status = srcCursor.getNext(&srcKvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        dstKvEntry.init();
        operators->shallowCopyKvEntry(&dstKvEntry,
                                      dstNumFields,
                                      &srcKvEntry,
                                      opKvEntryCopyMapping,
                                      0,
                                      opKvEntryCopyMapping->numEntries);

        dstKvEntry.tuple_.set(rowNumIdx,
                              recordNum,
                              dstKvEntry.kvMeta_->tupMeta_->getFieldType(
                                  rowNumIdx));

        recordNum.int64Val++;
        workCounter++;
        status = opPopulateInsertHandle(&this->insertHandle, &dstKvEntry);
        BailIfFailed(status);
    }

    assert(srcCursorInited);
    CursorManager::get()->destroy(&srcCursor);
    srcCursorInited = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                this->insertState,
                                this->srcXdb,
                                dstXdb,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&srcCursor);
        srcCursorInited = false;
    }
    return status;
}

void
Operators::operatorsGetRowNumLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;

    XcalarApiGetRowNumInput *getRowNumInput =
        (XcalarApiGetRowNumInput *) opInput->buf;

    Status status = StatusOk;
    XdbId srcXdbId = getRowNumInput->srcTable.xdbId;
    XdbId dstXdbId = getRowNumInput->dstTable.xdbId;
    XdbMgr *xdbMgr = XdbMgr::get();
    XdbMeta *srcMeta, *dstMeta;
    Xdb *srcXdb, *dstXdb;
    Stopwatch stopwatch;
    logOperationStage(__func__, status, stopwatch, Begin);

    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    status = getRowNumHelper(opInput, srcXdb, srcMeta, dstXdb, dstMeta);
    BailIfFailed(status);

CommonExit:
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

Status
Operators::getRowNumHelper(OperatorsApiInput *opInput,
                           Xdb *srcXdb,
                           XdbMeta *srcMeta,
                           Xdb *dstXdb,
                           XdbMeta *dstMeta)
{
    XcalarApiGetRowNumInput *getRowNumInput =
        (XcalarApiGetRowNumInput *) opInput->buf;
    OpKvEntryCopyMapping *opKvEntryCopyMapping = NULL;
    XdbId srcXdbId = getRowNumInput->srcTable.xdbId;
    XdbId dstXdbId = getRowNumInput->dstTable.xdbId;
    TrackHelpers *trackHelpers = NULL;
    uint64_t numValueArrayEntriesInDst = 0;
    DagLib *dagLib = DagLib::get();
    Config *config = Config::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t numRows;
    XdbInsertKvState insertState;
    size_t rowNumIndex = (size_t)(-1);
    OperatorsGetRowNumWork **operatorsWorkers = NULL;

    Status workerStatus = StatusOk;
    Status status = operatorsCountLocal(srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get numRows for srcTable %lu: %s",
                srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        assert(status == StatusOk);
        goto CommonExit;
    }

    dstMeta->prehashed = true;
    // The srcXdb and dstXdb are indexed on the same key
    xdbMgr->xdbCopyMinMax(srcXdb, dstXdb);
    insertState = XdbInsertSlotHash;

    uint64_t numChunks;
    numChunks = xdbMgr->xdbGetNumHashSlots(srcXdb);
    OpStatus *opStatus;

    status = (dagLib->getDagLocal(opInput->dagId))
                 ->getOpStatusFromXdbId(dstXdbId, &opStatus);
    numValueArrayEntriesInDst =
        dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getNumFields();
    opKvEntryCopyMapping = getOpKvEntryCopyMapping(numValueArrayEntriesInDst);
    if (opKvEntryCopyMapping == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    for (size_t ii = 0; ii < numValueArrayEntriesInDst; ii++) {
        if (strncmp(getRowNumInput->newFieldName,
                    dstMeta->kvNamedMeta.valueNames_[ii],
                    sizeof(dstMeta->kvNamedMeta.valueNames_[ii])) == 0) {
            rowNumIndex = ii;
            break;
        }
    }
    assert(rowNumIndex != (size_t)(-1));

    uint64_t endDstIdx;

    if (rowNumIndex != numValueArrayEntriesInDst - 1) {
        // this means we updated an existing field
        endDstIdx = numValueArrayEntriesInDst;
    } else {
        endDstIdx = numValueArrayEntriesInDst - 1;
    }
    initKvEntryCopyMapping(opKvEntryCopyMapping,
                           srcMeta,
                           dstMeta,
                           0,
                           endDstIdx,
                           true,
                           NULL,
                           0);

    uint64_t nodeRowStart;
    nodeRowStart = 1;

    for (unsigned ii = 0; ii < config->getMyNodeId(); ii++) {
        nodeRowStart += getRowNumInput->rowCountPerNode[ii];
    }

    unsigned numWorkers;
    numWorkers =
        Operators::getNumWorkers(opStatus->atomicOpDetails.numWorkTotal);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    BailIfNull(trackHelpers);

    // Check for shutdown before initiating a large amount of work
    if (usrNodeNormalShutdown()) {
        status = StatusShutdownInProgress;
        goto CommonExit;
    }

    operatorsWorkers = new (std::nothrow) OperatorsGetRowNumWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsGetRowNumWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsGetRowNumWork((ii == numWorkers - 1)
                                       ? TrackHelpers::Master
                                       : TrackHelpers::NonMaster,
                                   ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->dropSrcSlots = opInput->flags & OperatorFlagDropSrcSlots;
        operatorsWork->serializeSlots =
            opInput->flags & OperatorFlagSerializeSlots;
        operatorsWork->srcXdbId = srcXdbId;
        operatorsWork->dstXdbId = dstXdbId;
        operatorsWork->insertState = insertState;

        operatorsWork->nodeRowStart = nodeRowStart;
        operatorsWork->rowNumIdx = rowNumIndex;
        operatorsWork->opStatus = opStatus;
        operatorsWork->opKvEntryCopyMapping = opKvEntryCopyMapping;
        operatorsWork->srcXdb = srcXdb;
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->op = OperatorsGetRowNum;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (opKvEntryCopyMapping != NULL) {
        memFree(opKvEntryCopyMapping);
        opKvEntryCopyMapping = NULL;
    }

    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    return status;
}

Status
Operators::issueWorkIndexDataset(uint64_t numRecords,
                                 OperatorsIndexHandle *indexHandle,
                                 DagTypes::DagId dagId)
{
    Status workerStatus = StatusOk;
    Status status = StatusOk;
    uint64_t numChunks = XdbMgr::xdbHashSlots;
    uint64_t recordsPerIndexWorkItem = numRecords / numChunks;
    TrackHelpers *trackHelpers = NULL;
    bool scalarHandleInit = false;
    uint64_t numWorkers;
    OperatorsIndexDatasetWork **operatorsWorkers = NULL;

    status =
        indexHandle->scalarHandle->initHandle(MsgTypeId::Msg2pcIndexPage,
                                              TwoPcCallId::Msg2pcIndexPage1,
                                              NULL,
                                              indexHandle->dstXdbId,
                                              NULL);
    BailIfFailed(status);
    scalarHandleInit = true;

    while (recordsPerIndexWorkItem < MinRecordsPerIndexWorkItem &&
           numChunks > 1) {
        numChunks /= 2;
        recordsPerIndexWorkItem = numRecords / numChunks;
    }

    assert(numChunks >= 1 && numChunks <= 1 * KB);

    uint64_t lastChunkLen;
    if (recordsPerIndexWorkItem == 0) {
        lastChunkLen = numRecords;
    } else {
        lastChunkLen =
            recordsPerIndexWorkItem + (numRecords % recordsPerIndexWorkItem);
    }

    assert(recordsPerIndexWorkItem * (numChunks - 1) + lastChunkLen ==
           numRecords);

    numWorkers = Operators::getNumWorkers(numRecords);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numChunks);
    BailIfFailed(status);

    operatorsWorkers =
        new (std::nothrow) OperatorsIndexDatasetWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsIndexDatasetWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsIndexDatasetWork((ii == numWorkers - 1)
                                          ? TrackHelpers::Master
                                          : TrackHelpers::NonMaster,
                                      ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->op = OperatorsIndexDataset;

        operatorsWork->indexHandle = indexHandle;
        operatorsWork->dstXdb = indexHandle->dstXdb;
        operatorsWork->recordsPerChunk = recordsPerIndexWorkItem;
        operatorsWork->lastChunkLen = lastChunkLen;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (scalarHandleInit) {
        Status status2;
        status2 = indexHandle->scalarHandle->waitForCompletions();
        if (status2 != StatusOk && status == StatusOk) {
            status = status2;
        }

        indexHandle->scalarHandle->destroy();
    }

    if (status != StatusOk) {
        indexHandle->indexStatus = status;
    }

    if (status == StatusOk && indexHandle->indexStatus != StatusOk) {
        status = indexHandle->indexStatus;
    }

    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    return status;
}

Status
Operators::initIndexHandle(OperatorsIndexHandle *indexHandle,
                           Xid srcXid,
                           bool isTable,
                           XdbId dstXdbId,
                           XcalarApiIndexInput *indexInput,
                           DagTypes::DagId dagId)
{
    DagLib *dagLib = DagLib::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    DhtMgr *dhtMgr = DhtMgr::get();
    Status status;

    memZero(indexHandle, sizeof(*indexHandle));
    indexHandle->indexStatus = StatusOk;
    indexHandle->srcXid = srcXid;
    indexHandle->dstXdbId = dstXdbId;
    indexHandle->indexInput = indexInput;

    new (&indexHandle->sem) Semaphore(0);

    status = (dagLib->getDagLocal(dagId))
                 ->getOpStatusFromXdbId(dstXdbId, &indexHandle->opStatus);
    BailIfFailed(status);

    status =
        xdbMgr->xdbGet(dstXdbId, &indexHandle->dstXdb, &indexHandle->dstMeta);
    BailIfFailed(status);

    if (isTable) {
        indexHandle->dataset = NULL;
        status = operatorsCountLocal(srcXid, false, &indexHandle->numRecords);
        BailIfFailed(status);

        indexHandle->fatptrHandle =
            (TransportPageHandle *) memAllocExt(sizeof(TransportPageHandle),
                                                moduleName);
        BailIfNull(indexHandle->fatptrHandle);
    } else {
        indexHandle->dataset =
            Dataset::get()->getDatasetFromId(srcXid, &status);
        assert(indexHandle->dataset != NULL && status == StatusOk &&
               "guaranteed by contract");
        indexHandle->numRecords =
            indexHandle->dataset->pageIndex_->getNumRecords();

        indexHandle->fatptrHandle = NULL;
    }

    indexHandle->dht = dhtMgr->dhtGetDht(indexHandle->dstMeta->dhtId);
    if (indexHandle->dht == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get DHT (ID %lu)",
                indexHandle->dstMeta->dhtId);
        assert(0 && "Failed to get DHT");
        status = StatusInval;
        goto CommonExit;
    }

    indexHandle->scalarHandle = new (std::nothrow) TransportPageHandle();
    BailIfNull(indexHandle->scalarHandle);

CommonExit:
    if (status != StatusOk) {
        if (indexHandle->fatptrHandle != NULL) {
            delete indexHandle->fatptrHandle;
            indexHandle->fatptrHandle = NULL;
        }

        if (indexHandle->scalarHandle != NULL) {
            delete indexHandle->scalarHandle;
            indexHandle->scalarHandle = NULL;
        }
    }

    return status;
}

void
Operators::destroyIndexHandle(OperatorsIndexHandle *indexHandle)
{
    if (indexHandle->fatptrHandle != NULL) {
        delete indexHandle->fatptrHandle;
        indexHandle->fatptrHandle = NULL;
    }

    if (indexHandle->scalarHandle != NULL) {
        delete indexHandle->scalarHandle;
        indexHandle->scalarHandle = NULL;
    }
}

void
Operators::operatorsIndexLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsIndexHandle *indexHandle = NULL;
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiIndexInput *indexInput = (XcalarApiIndexInput *) opInput->buf;
    DagTypes::DagId dagId = opInput->dagId;
    Status status = StatusOk;
    bool indexHandleInit = false;

    const XdbId dstXdbId = indexInput->dstTable.xdbId;
    XdbMeta *dstMeta;
    Xdb *dstXdb;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    status = XdbMgr::get()->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);

    if (indexInput->source.isTable) {
        PerTxnInfo *perTxnInfo = getPerTxnInfo();
        status = initiateOp(perTxnInfo,
                            0,
                            NULL,
                            NULL,
                            NULL,
                            OperatorsIndexTable,
                            0,
                            NULL,
                            opInput->userIdName,
                            opInput->sessionId);
        BailIfFailed(status);
    } else {
        indexHandle =
            (OperatorsIndexHandle *) memAllocExt(sizeof(OperatorsIndexHandle),
                                                 moduleName);
        BailIfNullWith(indexHandle, StatusNoMem);

        status = initIndexHandle(indexHandle,
                                 indexInput->source.xid,
                                 indexInput->source.isTable,
                                 indexInput->dstTable.xdbId,
                                 indexInput,
                                 dagId);
        BailIfFailed(status);
        indexHandleInit = true;

        status =
            issueWorkIndexDataset(indexHandle->numRecords, indexHandle, dagId);
    }

CommonExit:
    if (indexHandle != NULL) {
        if (indexHandleInit) {
            destroyIndexHandle(indexHandle);
        }

        memFree(indexHandle);
        indexHandle = NULL;
    }
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

void
Operators::operatorsSynthesizeLocal(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiSynthesizeInput *synthesizeInput =
        (XcalarApiSynthesizeInput *) opInput->buf;
    const XdbId dstXdbId = synthesizeInput->dstTable.xdbId;
    XdbMeta *dstMeta;
    Xdb *dstXdb;
    XcalarEval *eval = XcalarEval::get();
    XcalarEvalClass1Ast ast;
    int *evalArgIndices = NULL;
    bool astInited = false;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    status = XdbMgr::get()->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    assert(status == StatusOk);

    if (synthesizeInput->source.isTable) {
        unsigned numEvals = 0;
        PerTxnInfo *perTxnInfo = getPerTxnInfo();
        DsDemystifyTableInput *demystifyInput = perTxnInfo->demystifyInput;

        if (synthesizeInput->filterString[0] != '\0') {
            numEvals = 1;
            status =
                eval->generateClass1Ast(synthesizeInput->filterString, &ast);
            BailIfFailed(status);
            astInited = true;

            evalArgIndices = (int *) memAlloc(ast.astCommon.numScalarVariables *
                                              sizeof(*evalArgIndices));
            BailIfNull(evalArgIndices);

            for (unsigned jj = 0; jj < ast.astCommon.numScalarVariables; jj++) {
                char *varName = ast.astCommon.scalarVariables[jj].variableName;
                DataFormat::unescapeNestedDelim(varName);

                unsigned kk;
                for (kk = 0; kk < demystifyInput->numVariables; kk++) {
                    if (strcmp(dstMeta->kvNamedMeta.valueNames_[kk], varName) ==
                        0) {
                        break;
                    }
                }

                if (kk == demystifyInput->numVariables) {
                    status = StatusInval;
                    xSyslogTxnBuf(moduleName,
                                  XlogErr,
                                  "Field %s in filter string %s not found",
                                  varName,
                                  synthesizeInput->filterString);
                    goto CommonExit;
                }

                evalArgIndices[jj] = kk;
            }
        } else {
            numEvals = 0;
        }

        char *evalString = synthesizeInput->filterString;

        status = initiateOp(perTxnInfo,
                            numEvals,
                            NULL,
                            &evalString,
                            &evalArgIndices,
                            OperatorsSynthesizeTable,
                            0,
                            NULL,
                            opInput->userIdName,
                            opInput->sessionId);
        BailIfFailed(status);
    } else {
        status =
            synthesizeDataset(synthesizeInput, opInput->dagId, dstXdb, dstMeta);
        BailIfFailed(status);
    }

CommonExit:
    logOperationStage(__func__, status, stopwatch, End);
    eph->setAckInfo(status, 0);

    if (evalArgIndices != NULL) {
        memFree(evalArgIndices);
        evalArgIndices = NULL;
    }

    if (astInited) {
        // scratchPadScalars have already been freed
        eval->dropScalarVarRef(&ast.astCommon);
        eval->destroyClass1Ast(&ast);
        astInited = false;
    }
}

// Common local entrypoint for filter/map.
Status
Operators::operatorsFilterAndMapLocal(OperatorsEnum operators,
                                      OperatorsApiInput *opInput,
                                      const XdbId srcXdbId,
                                      const XdbId dstXdbId,
                                      unsigned numEvals,
                                      char **evalStrs)
{
    Status status = StatusOk;
    XcalarEvalClass1Ast *asts = NULL;
    bool astsCreated[numEvals];
    memZero(astsCreated, sizeof(astsCreated));
    int *evalArgIndices[numEvals];
    memZero(evalArgIndices, sizeof(evalArgIndices));
    EvalUdfModuleSet udfModules;
    EvalUdfModuleSet *usedModules = NULL;
    XcalarEval *xcalarEval = XcalarEval::get();
    uint64_t numRows = 0;
    char **variableNames = NULL;
    PerTxnInfo *perTxnInfo = getPerTxnInfo();
    unsigned numVariables = 0;

    asts = (XcalarEvalClass1Ast *) memAlloc(numEvals * sizeof(*asts));
    BailIfNull(asts);

    status = operatorsCountLocal(srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get numRows for srcTable %lu: %s",
                srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        assert(status == StatusOk);
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < numEvals; ii++) {
        status = xcalarEval->generateClass1Ast(evalStrs[ii], &asts[ii]);
        BailIfFailed(status);
        astsCreated[ii] = true;
        numVariables += asts[ii].astCommon.numScalarVariables;

        evalArgIndices[ii] =
            (int *) memAlloc(asts[ii].astCommon.numScalarVariables *
                             sizeof(*evalArgIndices[ii]));
        BailIfNull(evalArgIndices[ii]);

        if (xcalarEval->containsUdf(asts[ii].astCommon.rootNode)) {
            // Will require childnodes for UDF processing.
            // Lookup needed modules.
            usedModules = &udfModules;

            status = xcalarEval->getUdfModules(asts[ii].astCommon.rootNode,
                                               usedModules,
                                               NULL);
            BailIfFailed(status);
        }
    }

    // allocate for worst case numVariables
    variableNames = (char **) memAlloc(numVariables * sizeof(*variableNames));
    BailIfNull(variableNames);

    // redo count, this time removing duplicates
    numVariables = 0;
    for (unsigned ii = 0; ii < numEvals; ii++) {
        for (unsigned jj = 0; jj < asts[ii].astCommon.numScalarVariables;
             jj++) {
            char *varName = asts[ii].astCommon.scalarVariables[jj].variableName;
            unsigned kk;
            for (kk = 0; kk < numVariables; kk++) {
                if (strcmp(variableNames[kk], varName) == 0) {
                    break;
                }
            }
            evalArgIndices[ii][jj] = kk;
            if (kk == numVariables) {
                // first use of this variable
                variableNames[numVariables++] = varName;
            }
        }
    }

    char **newFieldNames;
    if (operators == OperatorsMap) {
        newFieldNames = ((XcalarApiMapInput *) opInput->buf)->newFieldNames;
    } else {
        newFieldNames = NULL;
    }

    status = initiateOp(perTxnInfo,
                        numEvals,
                        newFieldNames,
                        evalStrs,
                        evalArgIndices,
                        operators,
                        0,
                        usedModules,
                        opInput->userIdName,
                        opInput->sessionId);

CommonExit:
    udfModules.removeAll(&EvalUdfModule::del);
    if (variableNames != NULL) {
        memFree(variableNames);
        variableNames = NULL;
    }

    if (asts != NULL) {
        for (unsigned ii = 0; ii < numEvals; ii++) {
            if (astsCreated[ii]) {
                xcalarEval->destroyClass1Ast(&asts[ii]);
                astsCreated[ii] = false;
            }
        }
    }

    for (unsigned ii = 0; ii < numEvals; ii++) {
        memFree(evalArgIndices[ii]);
        evalArgIndices[ii] = NULL;
    }

    if (asts != NULL) {
        memFree(asts);
        asts = NULL;
    }

    return status;
}

// Local entry point for filter. Initiate filter on this node with given input.
void
Operators::operatorsFilterLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiFilterInput *filterInput = (XcalarApiFilterInput *) opInput->buf;

    assert(filterInput != NULL);
    const XdbId srcXdbId = filterInput->srcTable.xdbId;
    const XdbId dstXdbId = filterInput->dstTable.xdbId;
    char *evalString = filterInput->filterStr;
    Status status;
    Stopwatch stopwatch;
    logOperationStage(__func__, status, stopwatch, Begin);

    status = operatorsFilterAndMapLocal(OperatorsFilter,
                                        opInput,
                                        srcXdbId,
                                        dstXdbId,
                                        1,
                                        &evalString);
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

// Local entry point for map. Initiate map on this node with given input.
void
Operators::operatorsMapLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiMapInput *mapInput = (XcalarApiMapInput *) opInput->buf;
    Status status;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    xcalarApiDeserializeMapInput(mapInput);

    assert(mapInput != NULL);
    const XdbId srcXdbId = mapInput->srcTable.xdbId;
    const XdbId dstXdbId = mapInput->dstTable.xdbId;
    status = operatorsFilterAndMapLocal(OperatorsMap,
                                        opInput,
                                        srcXdbId,
                                        dstXdbId,
                                        mapInput->numEvals,
                                        mapInput->evalStrs);
    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

void
Operators::operatorsGroupByLocal(MsgEphemeral *eph, void *payload)
{
    OperatorsApiInput *opInput = (OperatorsApiInput *) payload;
    XcalarApiGroupByInput *groupByInput =
        (XcalarApiGroupByInput *) opInput->buf;
    xcalarApiDeserializeGroupByInput(groupByInput);

    Status status = StatusOk;
    NewTupleMeta tupMeta;
    unsigned numVariables = 0;
    uint64_t numFuncs = 0;
    XcalarEval *xcalarEval = XcalarEval::get();
    XdbMgr *xdbMgr = XdbMgr::get();
    unsigned numEvals = groupByInput->numEvals;
    XcalarEvalClass2Ast *asts = NULL;
    char **variableNames = NULL;
    Stopwatch stopwatch;

    logOperationStage(__func__, status, stopwatch, Begin);

    bool astsCreated[numEvals];
    memZero(astsCreated, sizeof(astsCreated));

    int *evalArgIndices[numEvals];
    memZero(evalArgIndices, sizeof(evalArgIndices));

    const XdbId srcXdbId = groupByInput->srcTable.xdbId;
    const XdbId dstXdbId = groupByInput->dstTable.xdbId;
    uint64_t numRows = 0;

    PerTxnInfo *perTxnInfo = getPerTxnInfo();

    status = operatorsCountLocal(srcXdbId, false, &numRows);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to get numRows for srcTable %lu: %s",
                srcXdbId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numRows == 0) {
        assert(status == StatusOk);
        goto CommonExit;
    }

    asts = (XcalarEvalClass2Ast *) memAlloc(numEvals * sizeof(*asts));
    BailIfNull(asts);

    XdbMeta *srcMeta, *dstMeta;
    Xdb *srcXdb, *dstXdb;
    status = xdbMgr->xdbGet(srcXdbId, &srcXdb, &srcMeta);
    BailIfFailed(status);

    status = xdbMgr->xdbRehashXdbIfNeeded(srcXdb, XdbInsertCrcHash);
    BailIfFailed(status);

    status = xdbMgr->xdbGet(dstXdbId, &dstXdb, &dstMeta);
    BailIfFailed(status);

    for (unsigned ii = 0; ii < numEvals; ii++) {
        uint64_t numFunc;
        status = xcalarEval->generateClass2Ast(groupByInput->evalStrs[ii],
                                               &asts[ii],
                                               &numFunc,
                                               opInput->dagId,
                                               dstXdbId);
        BailIfFailed(status);
        astsCreated[ii] = true;

        evalArgIndices[ii] = (int *) memAlloc(asts[ii].numUniqueVariables *
                                              sizeof(*evalArgIndices[ii]));
        BailIfNull(evalArgIndices[ii]);

        numVariables += asts[ii].numUniqueVariables;
        numFuncs += numFunc;
    }

    // allocate for worst case numVariables
    variableNames = (char **) memAlloc(numVariables * sizeof(*variableNames));
    BailIfNull(variableNames);

    // redo count, this time removing duplicates
    numVariables = 0;
    for (unsigned ii = 0; ii < numEvals; ii++) {
        for (unsigned jj = 0; jj < asts[ii].numUniqueVariables; jj++) {
            char *varName = asts[ii].variableNames[jj];
            unsigned kk;
            for (kk = 0; kk < numVariables; kk++) {
                if (strcmp(variableNames[kk], varName) == 0) {
                    break;
                }
            }

            evalArgIndices[ii][jj] = kk;

            if (kk == numVariables) {
                // first use of this variable
                variableNames[numVariables++] = varName;
            }
        }
    }

    tupMeta.setNumFields(numVariables);
    for (unsigned ii = 0; ii < numVariables; ii++) {
        tupMeta.setFieldType(DfScalarObj, ii);
    }
    status = xdbMgr->xdbCreateScratchPadXdbs(dstXdb,
                                             DsDefaultDatasetKeyName,
                                             DfInt64,
                                             &tupMeta,
                                             variableNames,
                                             numVariables);
    BailIfFailed(status);

    status = initiateOp(perTxnInfo,
                        numEvals,
                        groupByInput->newFieldNames,
                        groupByInput->evalStrs,
                        (int **) evalArgIndices,
                        OperatorsGroupBy,
                        numFuncs,
                        NULL,
                        opInput->userIdName,
                        opInput->sessionId);
    BailIfFailed(status);

CommonExit:
    if (variableNames != NULL) {
        memFree(variableNames);
        variableNames = NULL;
    }

    if (asts != NULL) {
        for (unsigned ii = 0; ii < numEvals; ii++) {
            if (astsCreated[ii]) {
                xcalarEval->destroyClass2Ast(&asts[ii]);
                astsCreated[ii] = false;
            }
        }
    }

    for (unsigned ii = 0; ii < numEvals; ii++) {
        memFree(evalArgIndices[ii]);
        evalArgIndices[ii] = NULL;
    }

    if (asts != NULL) {
        memFree(asts);
        asts = NULL;
    }

    logOperationStage(__func__, status, stopwatch, End);

    eph->setAckInfo(status, 0);
}

Status
Operators::operatorsCountLocal(XdbId xdbId, bool unique, uint64_t *count)
{
    XdbMgr *xdbMgr = XdbMgr::get();

    // XXX implement me
    assert(unique == false);
    // Can't currently happen, but if it does...
    if (unique) {
        *count = 0;
        return StatusUnimpl;
    }

    return xdbMgr->xdbGetNumLocalRowsFromXdbId(xdbId, count);
}

// Local handler for GetTableMeta. Get metadata for table that resides on this
// node. Delegate to libds if dataset.
// XXX(kkochis) This smells like it belongs in DAG since it's switching on
//              isTable.
void
Operators::operatorsGetTableMetaMsgLocal(MsgEphemeral *eph, void *payload)
{
    GetTableMetaInput *getTableMetaInput;
    XcalarApiTableMeta outMeta;
    Status status = StatusUnknown;
    XdbMgr *xdbMgr = XdbMgr::get();
    size_t payloadLength = 0;

    // Avoid returning residual data to caller in error path
    memZero(&outMeta, sizeof(outMeta));

    getTableMetaInput = (GetTableMetaInput *) payload;

    if (getTableMetaInput->isTable) {
        status =
            xdbMgr->xdbGetXcalarApiTableMetaLocal(getTableMetaInput->xid,
                                                  &outMeta,
                                                  getTableMetaInput->isPrecise);
        BailIfFailed(status);
    } else {
        DsDataset *dataset =
            Dataset::get()->getDatasetFromId(getTableMetaInput->xid, &status);
        BailIfFailed(status);

        assert(dataset != NULL);
        outMeta.numRows = dataset->pageIndex_->getNumRecords();
        outMeta.size = dataset->pageIndex_->getPageMemUsed();
    }

    assert(status == StatusOk);
    outMeta.status = status.code();
    *(XcalarApiTableMeta *) payload = outMeta;
    payloadLength = sizeof(outMeta);

CommonExit:
    xcAssertIf((status != StatusOk), (payloadLength == 0));

    eph->setAckInfo(status, payloadLength);
}

Status
OperatorsIndexTableWork::setUp()
{
    Status status;
    bool insertHandleInited = false;
    unsigned ii;
    unsigned numVariables = opMeta->numVariables;
    unsigned numScalarsAlloced = 0;
    size_t rowMetaSize = sizeof(OpRowMeta) + opMeta->scalarsSize;

    this->scratchPadScalars =
        (Scalar **) memAllocExt(numVariables * sizeof(*this->scratchPadScalars),
                                moduleName);
    BailIfNullWith(this->scratchPadScalars, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = NULL;
    }

    this->demystifyVariables =
        new (std::nothrow) DemystifyVariable[numVariables];
    BailIfNullWith(this->demystifyVariables, StatusNoMem);

    for (ii = 0; ii < numVariables; ii++) {
        this->scratchPadScalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        if (this->scratchPadScalars[ii] == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        numScalarsAlloced++;
        status = this->demystifyVariables[ii].init(opMeta->variableNames[ii],
                                                   DfUnknown);
        BailIfFailed(status);
        this->demystifyVariables[ii].possibleEntries =
            opMeta->validIndicesMap[ii];
    }
    if (opMeta->fatptrDemystRequired) {
        this->demystifyPages = Operators::allocSourceTransPages();
        BailIfNull(this->demystifyPages);

        this->rowMetaPile =
            MemoryPile::allocPile(rowMetaSize,
                                  Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);

        this->scalarPile =
            MemoryPile::allocPile(Txn::currentTxn().id_,
                                  &status,
                                  1,
                                  BcHandle::BcScanCleanoutToFree);
        BailIfFailed(status);
    }

    this->scalarPages = Operators::allocSourceTransPages();
    BailIfNull(this->scalarPages);

    status = opGetInsertHandle(&this->insertHandle,
                               &opMeta->dstMeta->loadInfo,
                               opMeta->insertState);
    BailIfFailed(status);
    insertHandleInited = true;

CommonExit:
    if (status != StatusOk) {
        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }

        if (this->demystifyPages != NULL) {
            memFree(this->demystifyPages);
            this->demystifyPages = NULL;
        }

        if (this->scalarPages != NULL) {
            memFree(this->scalarPages);
            this->scalarPages = NULL;
        }

        if (this->rowMetaPile != NULL) {
            this->rowMetaPile->markPileAllocsDone();
        }

        if (this->scalarPile != NULL) {
            this->scalarPile->markPileAllocsDone();
        }

        for (ii = 0; ii < numScalarsAlloced; ii++) {
            assert(this->scratchPadScalars[ii] != NULL);
            Scalar::freeScalar(this->scratchPadScalars[ii]);
            this->scratchPadScalars[ii] = NULL;
        }

        if (this->scratchPadScalars != NULL) {
            memFree(this->scratchPadScalars);
            this->scratchPadScalars = NULL;
        }

        if (this->demystifyVariables != NULL) {
            delete[] this->demystifyVariables;
            this->demystifyVariables = NULL;
        }
    }

    return status;
}

Status
OperatorsIndexTableWork::doWork()
{
    Status status;
    OpMeta *opMeta = this->opMeta;
    OpStatus *opStatus = opMeta->opStatus;
    XdbMeta *dstMeta = opMeta->dstMeta;
    assert(dstMeta != NULL);

    bool cursorInit = false;
    TableCursor srcCursor;
    NewKeyValueEntry *srcKvEntry = NULL;
    Operators *operators = Operators::get();
    OpRowMeta *rowMeta = NULL;
    size_t rowMetaSize = sizeof(*rowMeta) + opMeta->scalarsSize;

    bool keyValid[opMeta->numVariables];
    DfFieldValue keys[opMeta->numVariables];

    DfFieldType keyTypes[opMeta->numVariables];
    int keyIndices[opMeta->numVariables];

    TransportPageHandle *fatptrHandle = opMeta->fatptrTransPageHandle;
    bool retIsValid;

    OpIndexDemystifyLocalArgs localArgs(opMeta,
                                        this->scalarPages,
                                        this->demystifyPages,
                                        TransportPageType::SendSourcePage,
                                        this->scratchPadScalars,
                                        this->demystifyVariables,
                                        &this->scalarPile,
                                        &this->insertHandle);

    IndexDemystifySend *demystifyHandle = NULL;

    demystifyHandle =
        new (std::nothrow) IndexDemystifySend(DemystifyMgr::Op::Index,
                                              &localArgs,
                                              this->demystifyPages,
                                              opMeta->dstXdbId,
                                              opMeta->validIndices,
                                              fatptrHandle,
                                              this->scalarPages,
                                              opMeta->indexHandle);
    BailIfNull(demystifyHandle);

    status = demystifyHandle->init();
    BailIfFailed(status);

    if (unlikely(atomicRead64(&opMeta->status) != StatusCodeOk)) {
        status.fromStatusCode((StatusCode) atomicRead64(&opMeta->status));
        goto CommonExit;
    }

    if (!opMeta->fatptrDemystRequired) {
        // all keys are immediates, populate key index and type info
        OpPossibleImmediateIndices *possibleIndices =
            opMeta->possibleImmediateIndices;

        for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
            if (possibleIndices[ii].numPossibleImmediateIndices == 0) {
                keyIndices[ii] = NewTupleMeta::DfInvalidIdx;
                keyTypes[ii] = DfInt64;
            } else {
                keyIndices[ii] =
                    possibleIndices[ii].possibleImmediateIndices[0];
                keyTypes[ii] =
                    opMeta->srcMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(
                        keyIndices[ii]);
            }
        }
    }

    status = CursorManager::get()->createOnSlot(this->srcXdbId,
                                                this->slotId,
                                                this->startRecord,
                                                Unordered,
                                                &srcCursor);
    BailIfFailed(status);
    cursorInit = true;

    srcKvEntry = new (std::nothrow) NewKeyValueEntry(srcCursor.kvMeta_);
    BailIfNull(srcKvEntry);

    unsigned workCounter, rowNum;
    workCounter = 0;
    rowNum = this->startRecord;
    size_t srcNumFields;
    srcNumFields = srcKvEntry->kvMeta_->tupMeta_->getNumFields();

    for (unsigned ii = 0; ii < this->numRecords; ii++) {
        if ((ii % XcalarConfig::GlobalStateCheckInterval == 0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                goto CommonExit;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                goto CommonExit;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        // don't need rowMeta if we aren't demystifying
        if (opMeta->fatptrDemystRequired) {
            assert(rowMeta == NULL);
            status = operators->getRowMeta(&rowMeta,
                                           rowNum,
                                           &this->rowMetaPile,
                                           rowMetaSize,
                                           srcXdb,
                                           &srcCursor,
                                           opMeta->numVariables);
            BailIfFailed(status);
        }

        status = srcCursor.getNext(srcKvEntry);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);

        if (opMeta->fatptrDemystRequired) {
            // save rowMeta pointer in key field
            DfFieldValue rowMetaField;
            rowMetaField.uint64Val = (uint64_t) rowMeta;

            status = demystifyHandle->demystifyRow(srcKvEntry,
                                                   rowMetaField,
                                                   &rowMeta->numIssued);
            rowMeta = NULL;
            BailIfFailed(status);
        } else {
            // All needed columns are immediates.
            for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
                if (keyIndices[ii] == NewTupleMeta::DfInvalidIdx) {
                    keyValid[ii] = false;
                } else {
                    DfFieldType fieldType =
                        srcKvEntry->kvMeta_->tupMeta_->getFieldType(
                            keyIndices[ii]);

                    DfFieldValue fieldValue =
                        srcKvEntry->tuple_.get(keyIndices[ii],
                                               srcNumFields,
                                               fieldType,
                                               &retIsValid);
                    if (!retIsValid) {
                        keyValid[ii] = false;
                    } else {
                        keyValid[ii] = true;
                        keys[ii] = fieldValue;
                    }
                }
            }

            status = operators->indexRow(opMeta,
                                         this->scalarPages,
                                         this->demystifyPages,
                                         TransportPageType::SendSourcePage,
                                         srcKvEntry,
                                         keys,
                                         keyValid,
                                         keyTypes,
                                         &this->insertHandle);
            BailIfFailed(status);
        }
        workCounter++;
        rowNum++;
    }

    assert(cursorInit);
    CursorManager::get()->destroy(&srcCursor);
    cursorInit = false;

    Operators::get()->pruneSlot(this->dropSrcSlots,
                                this->serializeSlots,
                                XdbInsertRandomHash,
                                this->srcXdb,
                                NULL,
                                this->slotId);

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);
CommonExit:
    if (rowMeta != NULL) {
        operators->putRowMeta(rowMeta, opMeta->numVariables);
        rowMeta = NULL;
    }

    if (srcKvEntry) {
        delete srcKvEntry;
        srcKvEntry = NULL;
    }

    // One or more index workers can get no data in XDB - ignore it
    if (status == StatusNoData) {
        status = StatusOk;
    }

    if (cursorInit) {
        CursorManager::get()->destroy(&srcCursor);
        cursorInit = false;
    }
    if (demystifyHandle) {
        demystifyHandle->destroy();
        demystifyHandle = NULL;
    }
    return status;
}

void
OperatorsIndexTableWork::tearDown()
{
    OpMeta *opMeta = this->opMeta;
    Status status;
    status.fromStatusCode((StatusCode) atomicRead64(&opMeta->status));

    Config *config = Config::get();
    TransportPageMgr *tpMgr = TransportPageMgr::get();

    // after walking through all of the records, we may (and are likely) to
    // end up with partially filled transport pages; so enqueue these before
    // returning unless we have an error
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        if (status == StatusOk) {
            if (this->demystifyPages && this->demystifyPages[ii] != NULL) {
                status = opMeta->fatptrTransPageHandle->enqueuePage(
                    this->demystifyPages[ii]);
                if (status != StatusOk) {
                    atomicWrite64(&opMeta->status, status.code());
                    tpMgr->freeTransportPage(this->demystifyPages[ii]);
                }
            }

            if (this->scalarPages[ii] != NULL) {
                status = indexHandle->enqueuePage(this->scalarPages[ii]);
                if (status != StatusOk) {
                    atomicWrite64(&opMeta->status, status.code());
                    tpMgr->freeTransportPage(this->scalarPages[ii]);
                }
            }
        } else {
            // We failed somewhere - just free the transport pages
            if (this->demystifyPages && this->demystifyPages[ii] != NULL) {
                tpMgr->freeTransportPage(this->demystifyPages[ii]);
            }
            if (this->scalarPages[ii] != NULL) {
                tpMgr->freeTransportPage(this->scalarPages[ii]);
            }
        }

        if (this->demystifyPages) {
            this->demystifyPages[ii] = NULL;
        }
        this->scalarPages[ii] = NULL;
    }

    if (this->rowMetaPile != NULL) {
        this->rowMetaPile->markPileAllocsDone();
    }

    if (this->scalarPile != NULL) {
        this->scalarPile->markPileAllocsDone();
    }

    for (unsigned ii = 0; ii < opMeta->numVariables; ii++) {
        assert(this->scratchPadScalars[ii] != NULL);
        Scalar::freeScalar(this->scratchPadScalars[ii]);
        this->scratchPadScalars[ii] = NULL;
    }

    assert(this->scratchPadScalars != NULL);
    memFree(this->scratchPadScalars);
    this->scratchPadScalars = NULL;

    assert(this->demystifyVariables != NULL);
    delete[] this->demystifyVariables;
    this->demystifyVariables = NULL;

    opPutInsertHandle(&this->insertHandle);

    memFree(this->demystifyPages);
    this->demystifyPages = NULL;

    memFree(this->scalarPages);
    this->scalarPages = NULL;
}

Status
OperatorsIndexDatasetWork::setUp()
{
    Status status;
    bool insertHandleInited = false;

    this->scalarPages = Operators::allocSourceTransPages();
    BailIfNull(this->scalarPages);

    if (strstr(this->indexHandle->indexInput->keys[0].keyName,
               DsDefaultDatasetKeyName)) {
        this->numKeys = 0;
    } else {
        this->numKeys = this->indexHandle->dstMeta->numKeys;

        this->keyAccessors = new (std::nothrow) Accessor[this->numKeys];
        BailIfNullWith(this->keyAccessors, StatusNoMem);

        for (unsigned ii = 0; ii < this->numKeys; ii++) {
            AccessorNameParser accessorParser;
            status = accessorParser.parseAccessor(this->indexHandle->indexInput
                                                      ->keys[ii]
                                                      .keyName,
                                                  &this->keyAccessors[ii]);
            BailIfFailed(status);
        }
    }

    if (this->numKeys == 0) {
        // if no keys are selected, we don't actually need to hash based on data
        // The dataset is divided into chunks which can map to slots in the
        // dstXdb, so you can just use the chunkId as the slotId

        status = opGetInsertHandle(&this->insertHandle,
                                   &this->indexHandle->dstMeta->loadInfo,
                                   XdbInsertSlotHash);
        BailIfFailed(status);
    } else {
        status = opGetInsertHandle(&this->insertHandle,
                                   &this->indexHandle->dstMeta->loadInfo,
                                   XdbInsertRandomHash);
        BailIfFailed(status);
    }

    insertHandleInited = true;

CommonExit:
    if (status != StatusOk) {
        if (insertHandleInited) {
            opPutInsertHandle(&this->insertHandle);
        }

        if (this->scalarPages != NULL) {
            memFree(this->scalarPages);
            this->scalarPages = NULL;
        }

        if (this->keyAccessors != NULL) {
            delete[] this->keyAccessors;
            this->keyAccessors = NULL;
        }
    }

    return status;
}

Status
OperatorsIndexDatasetWork::doWork()
{
    Status status = StatusOk;

    if ((uint64_t) this->slotId ==
        this->trackHelpers->getWorkUnitsTotal() - 1) {
        this->numRecords = this->lastChunkLen;
    } else {
        this->numRecords = this->recordsPerChunk;
    }
    this->startRecord = this->slotId * this->recordsPerChunk;

    OperatorsIndexHandle *indexHandle = this->indexHandle;
    OpStatus *opStatus = indexHandle->opStatus;
    Scalar *scalar = NULL;
    XdbMeta *dstMeta = indexHandle->dstMeta;
    Dht *dht = indexHandle->dht;
    uint64_t recordsRead = 0;
    XdbMgr *xdbMgr = XdbMgr::get();
    Operators *opMgr = Operators::get();
    DataPageIndex::RecordIterator recIter(indexHandle->dataset->pageIndex_);
    DataFormat *df = DataFormat::get();
    ReaderRecord record;

    bool keyValid[this->numKeys];
    DfFieldType fieldType[this->numKeys];
    DfFieldValue fieldVal[this->numKeys];
    Scalar *scalars[this->numKeys];
    NodeId myNodeId = Config::get()->getMyNodeId();

    NewKeyValueEntry dstKvEntry(&dstMeta->kvNamedMeta.kvMeta_);

    for (unsigned ii = 0; ii < this->numKeys; ii++) {
        scalars[ii] = Scalar::allocScalar(DfMaxFieldValueSize);
        BailIfNull(scalars[ii]);
    }

    recIter.seek(this->startRecord);
    unsigned workCounter;
    workCounter = 0;
    for (int64_t recNum = this->startRecord;
         recNum < this->startRecord + this->numRecords;
         recNum++) {
        // Actually get the next record from the index
        if (unlikely(!recIter.getNext(&record))) {
            break;
        }
        DfRecordId recordId = indexHandle->dataset->startRecordId_ + recNum;

        recordsRead++;
        workCounter++;
        if (unlikely(recordsRead % XcalarConfig::GlobalStateCheckInterval ==
                     0)) {
            if (usrNodeNormalShutdown()) {
                status = StatusShutdownInProgress;
                break;
            }

            if (opStatus->atomicOpDetails.cancelled) {
                status = StatusCanceled;
                break;
            }

            atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                        workCounter);
            workCounter = 0;
        }

        for (unsigned ii = 0; ii < this->numKeys; ii++) {
            DataValueReader dValue;
            keyValid[ii] = false;
            fieldType[ii] = DfUnknown;

            status = df->accessField(&record, &this->keyAccessors[ii], &dValue);
            if (unlikely(status == StatusDfFieldNoExist)) {
                status = StatusOk;
            } else if (unlikely(status != StatusOk)) {
                // We encountered a fatal error getting this field value
                goto CommonExit;
            } else {
                status =
                    dValue.getAsFieldValueInArray(scalars[ii]->fieldAllocedSize,
                                                  &scalars[ii]->fieldVals,
                                                  0,
                                                  &fieldType[ii]);
                if (unlikely(status == StatusDfFieldNoExist ||
                             fieldType[ii] == DfNull)) {
                    status = StatusOk;
                    continue;
                } else if (status != StatusOk) {
                    assert(status == StatusOverflow &&
                           "overflow is the only failure");
                    continue;
                }

                keyValid[ii] = true;

                scalars[ii]->fieldType = fieldType[ii];

                if (dstMeta->keyAttr[ii].type == DfUnknown &&
                    fieldType[ii] != DfUnknown) {
                    status = xdbMgr->xdbSetKeyType(dstMeta, fieldType[ii], ii);
                    BailIfFailed(status);
                }

                if (dstMeta->keyAttr[ii].type != fieldType[ii]) {
                    status = StatusDfTypeMismatch;
                    goto CommonExit;
                }

                status = scalars[ii]->getValue(&fieldVal[ii]);
                // Only an invalid field type causes != StatusOk
                assert(status == StatusOk);
            }
        }

        // We're indexing from a dataset; fake a valueArray so we can put our
        // brand new fatptr somewhere
        // set fatptr
        DfFieldValue fieldValue;
        df->fatptrSet(&fieldValue.fatptrVal, myNodeId, recordId);
        dstKvEntry.tuple_.set(0, fieldValue, DfFatptr);
        dstKvEntry.tuple_.setInvalid(1, dstMeta->getNumFields() - 1);

        // set keys
        for (unsigned ii = 0; ii < numKeys; ii++) {
            if (keyValid[ii]) {
                dstKvEntry.tuple_.set(ii + 1, fieldVal[ii], fieldType[ii]);
            }
        }

        status = opMgr->hashAndSendKv(indexHandle->scalarHandle,
                                      this->scalarPages,
                                      NULL,
                                      NULL,
                                      this->numKeys,
                                      fieldType,
                                      fieldVal,
                                      keyValid,
                                      &dstKvEntry,
                                      &this->insertHandle,
                                      dht,
                                      TransportPageType::SendSourcePage,
                                      DemystifyMgr::Op::Index);
        BailIfFailed(status);
    }

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, workCounter);

CommonExit:
    if (scalar != NULL) {
        Scalar::freeScalar(scalar);
        scalar = NULL;
    }

    if (status != StatusOk) {
        indexHandle->indexStatus = status;
    }

    for (unsigned ii = 0; ii < this->numKeys; ii++) {
        if (scalars[ii] != NULL) {
            Scalar::freeScalar(scalars[ii]);
        }
    }
    return status;
}

void
OperatorsIndexDatasetWork::tearDown()
{
    OperatorsIndexHandle *indexHandle = this->indexHandle;
    Status status = indexHandle->indexStatus;
    Config *config = Config::get();
    TransportPageMgr *tpMgr = TransportPageMgr::get();

    // after walking through all of the records, we may (and are likely) to
    // end up with partially filled transport pages; so enqueue these before
    // returning unless we have an error
    for (unsigned ii = 0; ii < config->getActiveNodes(); ii++) {
        if (this->scalarPages[ii] == NULL) {
            continue;
        }
        if (status == StatusOk) {
            status =
                indexHandle->scalarHandle->enqueuePage(this->scalarPages[ii]);
            if (status != StatusOk) {
                indexHandle->indexStatus = status;
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to enqueue transport page when "
                        "indexing dataset: %s",
                        strGetFromStatus(status));
                tpMgr->freeTransportPage(this->scalarPages[ii]);
            }
        } else {
            tpMgr->freeTransportPage(this->scalarPages[ii]);
        }
        this->scalarPages[ii] = NULL;
    }

    opPutInsertHandle(&this->insertHandle);

    if (this->keyAccessors != NULL) {
        delete[] this->keyAccessors;
        this->keyAccessors = NULL;
    }

    assert(this->scalarPages != NULL);
    memFree(this->scalarPages);
    this->scalarPages = NULL;
}

Status
Operators::synthesizeDataset(XcalarApiSynthesizeInput *synthesizeInput,
                             DagTypes::DagId dagId,
                             Xdb *dstXdb,
                             XdbMeta *dstMeta)
{
    Status status = StatusOk;
    Status workerStatus = StatusOk;
    uint64_t numWorkers = (uint64_t) Runtime::get()->getThreadsCount(
        Txn::currentTxn().rtSchedId_);
    uint64_t numPages;
    TrackHelpers *trackHelpers = NULL;
    OperatorsSynthesizeDatasetWork **operatorsWorkers = NULL;
    DsDataset *dataset;
    OpStatus *opStatus;
    XdbLoadArgs *xdbLoadArgs = NULL;
    ParseArgs *parseArgs = NULL;
    uint64_t pagesPerWorker;
    int64_t lastChunkLen;

    parseArgs = new (std::nothrow) ParseArgs();
    BailIfNull(parseArgs);

    dataset =
        Dataset::get()->getDatasetFromId(synthesizeInput->source.xid, &status);
    assert(dataset != NULL && status == StatusOk && "guaranteed by contract");
    numPages = dataset->pageIndex_->getNumPages();

    pagesPerWorker = numPages / numWorkers;
    lastChunkLen = numPages % numWorkers + pagesPerWorker;

    xdbLoadArgs = new (std::nothrow) XdbLoadArgs();
    BailIfNull(xdbLoadArgs);

    status = (DagLib::get()->getDagLocal(dagId))
                 ->getOpStatus(synthesizeInput->dstTable.tableId, &opStatus);
    BailIfFailed(status);

    trackHelpers = TrackHelpers::setUp(&workerStatus, numWorkers, numWorkers);
    BailIfNull(trackHelpers);

    const NewTupleMeta *dstTupMeta;
    dstTupMeta = dstMeta->kvNamedMeta.kvMeta_.tupMeta_;
    size_t dstNumFields;
    dstNumFields = dstTupMeta->getNumFields();

    xdbLoadArgs->dstXdbId = dstMeta->xdbId;
    xdbLoadArgs->evalString[0] = '\0';
    xdbLoadArgs->keyName[0] = '\0';
    xdbLoadArgs->keyIndex = NewTupleMeta::DfInvalidIdx;
    xdbLoadArgs->valueDesc.numValuesPerTuple = dstNumFields;
    for (size_t jj = 0; jj < dstNumFields; jj++) {
        xdbLoadArgs->valueDesc.valueType[jj] =
            dstMeta->kvNamedMeta.kvMeta_.tupMeta_->getFieldType(jj);
    }

    xdbLoadArgs->fieldNamesCount = synthesizeInput->columnsCount;
    for (unsigned ii = 0; ii < xdbLoadArgs->fieldNamesCount; ii++) {
        strlcpy(xdbLoadArgs->fieldNames[ii],
                synthesizeInput->columns[ii].oldName,
                sizeof(xdbLoadArgs->fieldNames[ii]));
    }

    operatorsWorkers =
        new (std::nothrow) OperatorsSynthesizeDatasetWork *[numWorkers];
    BailIfNull(operatorsWorkers);

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        operatorsWorkers[ii] = NULL;
    }

    for (unsigned ii = 0; ii < numWorkers; ii++) {
        OperatorsSynthesizeDatasetWork *operatorsWork = NULL;
        operatorsWorkers[ii] = new (std::nothrow)
            OperatorsSynthesizeDatasetWork((ii == numWorkers - 1)
                                               ? TrackHelpers::Master
                                               : TrackHelpers::NonMaster,
                                           ii);
        BailIfNull(operatorsWorkers[ii]);

        operatorsWork = operatorsWorkers[ii];
        operatorsWork->trackHelpers = trackHelpers;
        operatorsWork->op = OperatorsSynthesizeDataset;
        operatorsWork->dataset = dataset;
        operatorsWork->opStatus = opStatus;
        operatorsWork->xdbLoadArgs = xdbLoadArgs;
        operatorsWork->parseArgs = parseArgs;
        operatorsWork->pagesPerWorker = pagesPerWorker;
        operatorsWork->lastChunkLen = lastChunkLen;
        operatorsWork->dstXdbId = synthesizeInput->dstTable.xdbId;
    }

    status = trackHelpers->schedThroughput((Schedulable **) operatorsWorkers,
                                           numWorkers);
    BailIfFailed(status);

    trackHelpers->waitForAllWorkDone();
    if (workerStatus != StatusOk && status == StatusOk) {
        status = workerStatus;
    }
    BailIfFailed(status);

CommonExit:
    if (operatorsWorkers != NULL) {
        for (unsigned ii = 0; ii < numWorkers; ii++) {
            if (operatorsWorkers[ii] != NULL) {
                delete operatorsWorkers[ii];
                operatorsWorkers[ii] = NULL;
            }
        }
        delete[] operatorsWorkers;
        operatorsWorkers = NULL;
    }
    if (trackHelpers != NULL) {
        TrackHelpers::tearDown(&trackHelpers);
    }

    if (xdbLoadArgs) {
        delete xdbLoadArgs;
        xdbLoadArgs = NULL;
    }
    if (parseArgs) {
        delete parseArgs;
        parseArgs = NULL;
    }

    return status;
}

Status
OperatorsSynthesizeDatasetWork::doWork()
{
    if ((uint64_t) this->slotId ==
        this->trackHelpers->getWorkUnitsTotal() - 1) {
        this->numPages = this->lastChunkLen;
    } else {
        this->numPages = this->pagesPerWorker;
    }
    this->startPageIdx = this->slotId * this->pagesPerWorker;

    Status status = StatusOk;
    DataFormat *df = DataFormat::get();
    DataPageIndex::PageIterator pageIterator(this->dataset->pageIndex_);
    uint8_t **pages = (uint8_t **) memAlloc(this->numPages * (sizeof(*pages)));
    BailIfNull(pages);

    pageIterator.seek(this->startPageIdx);

    for (unsigned ii = 0; ii < this->numPages; ii++) {
        bool pageExists = pageIterator.getNext(&pages[ii]);
        assert(pageExists);
    }

    status = df->loadDataPagesIntoXdb(this->dstXdbId,
                                      this->numPages,
                                      pages,
                                      this->dataset->pageIndex_->getPageSize(),
                                      this->xdbLoadArgs,
                                      this->opStatus,
                                      NULL);
    BailIfFailed(status);

CommonExit:
    if (pages != NULL) {
        memFree(pages);
        pages = NULL;
    }

    return status;
}

Status
OperatorsWorkBase::checkLoopStatus(OpStatus *opStatus,
                                   size_t *loopCounter,
                                   size_t *workCounter)
{
    Status status = StatusOk;

    if (unlikely(*loopCounter % XcalarConfig::GlobalStateCheckInterval == 0)) {
        if (usrNodeNormalShutdown()) {
            status = StatusShutdownInProgress;
            goto CommonExit;
        }

        if (opStatus->atomicOpDetails.cancelled) {
            status = StatusCanceled;
            goto CommonExit;
        }

        atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic,
                    *workCounter);
        *workCounter = 0;
    }

CommonExit:
    return status;
}

void
OperatorsWorkBase::run()
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    uint64_t beginWorkItem, totalWork;
    bool tearDown = false;

    status = this->trackHelpers->helperStart();
    if (status == StatusAllWorkDone) {
        status = StatusOk;
        goto CommonExit;
    } else if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    status = this->setUp();

    if (status == StatusOk) {
        tearDown = true;
    }

    if (this->dstXdb == NULL && this->dstXdbId != XidInvalid) {
        verifyOk(xdbMgr->xdbGet(this->dstXdbId, &this->dstXdb, NULL));
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
        if (!this->trackHelpers->workUnitRunnable(ii)) {
            continue;
        }

        if (status != StatusOk) {
            this->trackHelpers->workUnitComplete(ii);
            continue;
        }

        this->slotId = ii;
        this->insertHandle.slotId = ii;

        if (this->insertHandle.insertState == XdbInsertSlotHash) {
            assert(this->dstXdb != NULL);
            if (this->op == OperatorsJoin) {
                XdbMgr::xdbSetSlotSortState(this->dstXdb, ii, SortedFlag);
            } else if (this->op == OperatorsIndexDataset ||
                       this->op == OperatorsIndexTable ||
                       this->op == OperatorsUnion ||
                       this->op == OperatorsSynthesizeDataset ||
                       this->op == OperatorsIndexDataset) {
                // The slot at first during hte insert is unordered.
                // When we later cursor through it is when we'll sort it.
                // Then the sortedFlag will be set to true
                XdbMgr::xdbSetSlotSortState(this->dstXdb, ii, UnsortedFlag);
            } else {
                // Assume rest of the operation is stable (a stable
                // operation is defined as an operation that maintains
                // the sortedness of a table. E.g. if we perform a filter
                // on a sorted table, the destination table remains sorted.
                // Hence filter is stable
                XdbMgr::xdbCopySlotSortState(this->srcXdb, this->dstXdb, ii);
            }
        }

        // per slot setUp
        switch (this->op) {
        case OperatorsJoin:
        case OperatorsIndexDataset:
        case OperatorsSynthesizeDataset:
            break;
        case OperatorsUnion: {
            OperatorsUnionWork *work = (OperatorsUnionWork *) this;

            bool empty = true;
            for (unsigned jj = 0; jj < work->numSrcXdbs; jj++) {
                work->numRecs[jj] =
                    xdbMgr->xdbGetNumRowsInHashSlot(work->xdbs[jj], ii);
                if (work->numRecs[jj] > 0) {
                    empty = false;
                }

                work->startRecs[jj] =
                    xdbMgr->xdbGetHashSlotStartRecord(work->xdbs[jj], ii);
            }

            if (empty) {
                this->trackHelpers->workUnitComplete(ii);
                continue;
            }

            break;
        }
        default:
            this->numRecords =
                xdbMgr->xdbGetNumRowsInHashSlot(this->srcXdb, ii);
            this->startRecord =
                xdbMgr->xdbGetHashSlotStartRecord(this->srcXdb, ii);

            if (this->numRecords == 0) {
                this->trackHelpers->workUnitComplete(ii);
                continue;
            }

            // check if we can do range optimizations
            if (this->op == OperatorsFilter) {
                OperatorsFilterAndMapWork *work =
                    (OperatorsFilterAndMapWork *) this;

                bool valid;
                DfFieldType type;
                DfFieldValue min, max;
                const char *keyName =
                    XdbMgr::xdbGetMeta(this->srcXdb)->keyAttr[0].name;

                XdbMgr::xdbGetSlotMinMax(this->srcXdb,
                                         ii,
                                         &type,
                                         &min,
                                         &max,
                                         &valid);

                if (!XcalarEval::get()->inFilterRange(&work->asts[0],
                                                      min,
                                                      max,
                                                      valid,
                                                      keyName,
                                                      type)) {
                    this->trackHelpers->workUnitComplete(ii);
                    continue;
                }
            } else if (this->op == OperatorsSynthesizeTable) {
                OperatorsCreateScalarTableWork *work =
                    (OperatorsCreateScalarTableWork *) this;

                bool valid;
                DfFieldType type;
                DfFieldValue min, max;
                const char *keyName =
                    XdbMgr::xdbGetMeta(this->srcXdb)->keyAttr[0].name;

                XdbMgr::xdbGetSlotMinMax(this->srcXdb,
                                         ii,
                                         &type,
                                         &min,
                                         &max,
                                         &valid);

                if (work->asts != NULL &&
                    !XcalarEval::get()->inFilterRange(&work->asts[0],
                                                      min,
                                                      max,
                                                      valid,
                                                      keyName,
                                                      type)) {
                    this->trackHelpers->workUnitComplete(ii);
                    continue;
                }
            }

            break;
        }

        // no need to do the work here if other instances report error
        if (this->trackHelpers->getSavedWorkStatus() == StatusOk) {
            Status status2 = this->doWork();
            if (status2 != StatusOk) {
                status = status2;
                xSyslog(moduleName,
                        XlogErr,
                        "OperatorsWorkBase::run op=%d doWork returns status=%s",
                        this->op,
                        strGetFromStatus(status));
            }
        }
        this->trackHelpers->workUnitComplete(ii);
    }

CommonExit:
    if (tearDown) {
        this->tearDown();
    }
    // status is managed outside of trackHelpers
    this->trackHelpers->helperDone(status);
}

void
OperatorsWorkBase::done()
{
    delete this;
}

Status
Operators::addPerTxnInfo(void *payload, size_t *outputSizeOut)
{
    Status status = StatusOk;
    size_t inputSize = *((size_t *) payload);
    size_t dmystInputSize =
        inputSize - sizeof(size_t) - sizeof(PerOpGlobalInput);
    unsigned nodeCount = Config::get()->getActiveNodes();
    XdbMgr *xdbMgr = XdbMgr::get();

    PerTxnInfo *perTxnInfo = new (std::nothrow) PerTxnInfo();
    BailIfNull(perTxnInfo);

    perTxnInfo->remoteCookies = new (std::nothrow) uintptr_t[nodeCount];
    BailIfNull(perTxnInfo->remoteCookies);

    memZero(perTxnInfo->remoteCookies, sizeof(uintptr_t) * nodeCount);
    perTxnInfo->remoteCookies[Config::get()->getMyNodeId()] =
        (uintptr_t) perTxnInfo;

    perTxnInfo->txn = Txn::currentTxn();
    memcpy(&perTxnInfo->perOpGlobIp,
           (uintptr_t *) ((uintptr_t) payload + sizeof(size_t)),
           sizeof(PerOpGlobalInput));

    perTxnInfo->demystifyInput =
        (DsDemystifyTableInput *) memAlloc(dmystInputSize);
    BailIfNull(perTxnInfo->demystifyInput);
    memcpy(perTxnInfo->demystifyInput,
           (uintptr_t *) ((uintptr_t) payload + sizeof(size_t) +
                          sizeof(PerOpGlobalInput)),
           dmystInputSize);

    // Hash table is indexed by Txn, so this insert should not fail.
    txnTableLock_.lock();
    verifyOk(perTxnTable_.insert(perTxnInfo));
    txnTableLock_.unlock();

    if (perTxnInfo->perOpGlobIp.dstXdbId != XidInvalid) {
        Xdb *dstXdb;
        XdbMeta *dstMeta;
        status =
            xdbMgr->xdbGet(perTxnInfo->perOpGlobIp.dstXdbId, &dstXdb, &dstMeta);
        assert(status == StatusOk);
        perTxnInfo->dstXdb = dstXdb;
        perTxnInfo->dstMeta = dstMeta;
    }

    if (perTxnInfo->perOpGlobIp.srcXdbId != XidInvalid) {
        Xdb *srcXdb;
        XdbMeta *srcMeta;
        status =
            xdbMgr->xdbGet(perTxnInfo->perOpGlobIp.srcXdbId, &srcXdb, &srcMeta);
        assert(status == StatusOk);
        perTxnInfo->srcXdb = srcXdb;
        perTxnInfo->srcMeta = srcMeta;
    }

    // Return the perTxnInfo to the source node and cookie to refer to in
    // the downstream fastpath logic.
    *((OpTxnCookie *) ((uintptr_t) payload + inputSize)) =
        (OpTxnCookie) perTxnInfo;
    perTxnInfo = NULL;
    *outputSizeOut = inputSize + sizeof(OpTxnCookie);

CommonExit:
    if (status != StatusOk) {
        *outputSizeOut = 0;
    }
    if (perTxnInfo != NULL) {
        perTxnInfo->del();
        perTxnInfo = NULL;
    }
    return status;
}

void
Operators::PerTxnInfo::del()
{
    if (this->remoteCookies != NULL) {
        delete[] this->remoteCookies;
        this->remoteCookies = NULL;
    }
    if (this->demystifyInput != NULL) {
        memFree(this->demystifyInput);
        this->demystifyInput = NULL;
    }
    delete this;
}

void
Operators::removePerTxnInfo(void *payload)
{
    Txn txn = Txn::currentTxn();

    // Kick bcScan now b/c here we are guaranteed to have all side effects
    // of this Txn across all the nodes in the cluster.
    if (*((Status *) payload) != StatusOk) {
        Status status = XdbMgr::get()->bcScanCleanout(txn.id_);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to do bcScan xdbKvBuf cleanout on txn %lu: %s",
                    txn.id_,
                    strGetFromStatus(status));
            // Fall through, since this is cleanout codepath
        }
    }

    txnTableLock_.lock();
    // Since this is Txn cleanout, expect to not find the entry in Hash table.
    PerTxnInfo *perTxnInfo = perTxnTable_.remove(txn.id_);
    if (perTxnInfo != NULL) {
        perTxnInfo->del();
        perTxnInfo = NULL;
    }
    txnTableLock_.unlock();
}

Status
Operators::updatePerTxnInfo(void *payload)
{
    OpTxnCookie *remoteCookies = (OpTxnCookie *) payload;
    PerTxnInfo *perTxnInfo = NULL;
    Txn curTxn = Txn::currentTxn();
    unsigned nodeCount = Config::get()->getActiveNodes();
    NodeId nodeId = Config::get()->getMyNodeId();

    txnTableLock_.lock();
    perTxnInfo = perTxnTable_.find(curTxn.id_);
    assert(perTxnInfo != NULL &&
           "perTxnInfo is set up before the local operator invocation");
    txnTableLock_.unlock();

    for (unsigned ii = 0; ii < nodeCount; ii++) {
        OpTxnCookie *cookie = &remoteCookies[ii];
        if (ii == nodeId) {
            assert(*cookie == perTxnInfo->remoteCookies[ii]);
        } else {
            perTxnInfo->remoteCookies[ii] = *cookie;
        }
        assert(perTxnInfo->remoteCookies[ii] != 0);
    }
    return StatusOk;
}

Operators::PerTxnInfo *
Operators::getPerTxnInfo()
{
    PerTxnInfo *perTxnInfo = NULL;
    Txn curTxn = Txn::currentTxn();
    txnTableLock_.lock();
    perTxnInfo = perTxnTable_.find(curTxn.id_);
    assert(perTxnInfo != NULL &&
           "perTxnInfo is set up before the local operator invocation");
    txnTableLock_.unlock();
    return perTxnInfo;
}

void
Operators::logOperationStage(const char *opName,
                             const Status &status,
                             Stopwatch &watch,
                             OperationStage opStage)
{
    unsigned long hours, minutesLeftOver, secondsLeftOver, millisecondsLeftOver;

    switch (opStage) {
    case Begin:
        watch.restart();
        StatsLib::statAtomicIncr64(Operators::operationsOutstanding);
        break;
    case End:
        watch.stop();
        watch.getPrintableTime(hours,
                               minutesLeftOver,
                               secondsLeftOver,
                               millisecondsLeftOver);
        xSyslog(moduleName,
                status.ok() ? XlogInfo : XlogErr,
                "%s is finished in %lu:%02lu:%02lu.%03lu"
                ": %s",
                opName,
                hours,
                minutesLeftOver,
                secondsLeftOver,
                millisecondsLeftOver,
                strGetFromStatus(status));
        assert(atomicRead64(
                   &Operators::operationsOutstanding->statUint64Atomic) > 0);
        StatsLib::statAtomicDecr64(Operators::operationsOutstanding);
        break;
    default:
        assert(0);
        break;
    }
}
