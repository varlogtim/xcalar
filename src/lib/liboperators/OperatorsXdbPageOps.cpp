// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "primitives/Primitives.h"
#include "xdb/Xdb.h"
#include "operators/OperatorsXdbPageOps.h"
#include "operators/OperatorsEvalTypes.h"
#include "util/MemTrack.h"
#include "util/Vector.h"
#include "dataset/Dataset.h"
#include "df/DataFormat.h"
#include "sys/XLog.h"
#include "newtupbuf/NewTupleTypes.h"
#include "operators/Operators.h"

static constexpr const char *moduleName = "libxdbops";

static Status
allocOpPagePoolNode(OpPagePoolNode **nodeOut, Xdb *xdb)
{
    Status status = StatusOk;
    unsigned numNodes = Config::get()->getActiveNodes();

    OpPagePoolNode *node =
        (OpPagePoolNode *) memAllocExt(sizeof(*node), moduleName);
    BailIfNullWith(node, StatusNoMem);

    node->transPages =
        (TransportPage **) memAllocExt(numNodes * sizeof(*node->transPages),
                                       moduleName);
    BailIfNullWith(node->transPages, StatusNoMem);

    for (unsigned ii = 0; ii < numNodes; ii++) {
        node->transPages[ii] = NULL;
    }

    node->inUse = false;
    node->keyRangeValid = false;
    node->next = NULL;
    node->page = NULL;

CommonExit:
    if (status != StatusOk) {
        if (node != NULL) {
            memFree(node);
            node = NULL;
        }
    }

    *nodeOut = node;
    return status;
}

static void
freeOpPagePoolNode(OpPagePoolNode *node)
{
    XdbMgr *xdbMgr = XdbMgr::get();
    assert(!node->inUse);
    if (node->page != NULL) {
        xdbMgr->xdbFreeXdbPage(node->page);
        node->page = NULL;
    }

    if (node->transPages) {
        memFree(node->transPages);
        node->transPages = NULL;
    }

    memFree(node);
}

Status
opInitLoadInfo(OpLoadInfo *loadInfo, Xdb *dstXdb, unsigned maxPagePoolSize)
{
    Status status = StatusOk;

    new (&loadInfo->lock) Mutex;
    loadInfo->loadWasComplete = false;
    loadInfo->poolSize = 0;
    loadInfo->totalAllocs = 0;
    loadInfo->xdbPagePool = NULL;
    loadInfo->dstXdb = dstXdb;

    if (maxPagePoolSize > 0) {
        loadInfo->transPageHandle = new (std::nothrow) TransportPageHandle();
        BailIfNull(loadInfo->transPageHandle);

        status =
            loadInfo->transPageHandle->initHandle(MsgTypeId::Msg2pcIndexPage,
                                                  TwoPcCallId::Msg2pcIndexPage1,
                                                  NULL,
                                                  XdbMgr::xdbGetXdbId(dstXdb),
                                                  NULL);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

Status
opProcessLoadInfo(Xdb *xdb, OpLoadInfo *loadInfo)
{
    OpPagePoolNode *node = loadInfo->xdbPagePool;
    XdbMgr *xdbMgr = XdbMgr::get();
    Status status = StatusOk;

    while (node != NULL) {
        if (node->page != NULL) {
            XdbPage *xdbPage = node->page;
            if (xdbPage->tupBuf->getNumTuples() > 0) {
                status = xdbMgr->xdbInsertOnePage(xdb,
                                                  xdbPage,
                                                  node->keyRangeValid,
                                                  node->minKey,
                                                  node->maxKey);
                if (status != StatusOk) {
                    return status;
                }
            } else {
                xdbMgr->xdbFreeXdbPage(xdbPage);
            }
        }
        node->page = NULL;
        node = node->next;
    }

    loadInfo->loadWasComplete = true;

    return status;
}

void
opDestroyLoadInfo(OpLoadInfo *loadInfo)
{
    OpPagePoolNode *node = loadInfo->xdbPagePool;
    OpPagePoolNode *nodeTmp;
    unsigned count = 0;
    while (node != NULL) {
        nodeTmp = node;
        node = node->next;

        freeOpPagePoolNode(nodeTmp);
        count++;
    }
    assert(count == loadInfo->poolSize);
    delete loadInfo->transPageHandle;
    loadInfo->xdbPagePool = NULL;
    loadInfo->poolSize = 0;
}

Status
opGetInsertHandle(OpInsertHandle *insertHandle,
                  OpLoadInfo *loadInfo,
                  XdbInsertKvState insertState)
{
    OpPagePoolNode *node = NULL;
    Status status = StatusOk;
    bool locked = false;
    insertHandle->insertState = insertState;

    if (insertState != XdbInsertRandomHash) {
        // only need resources for random hash
        goto CommonExit;
    }

    loadInfo->lock.lock();
    locked = true;
    if (loadInfo->xdbPagePool == NULL) {
        // alloc more pages in this pool to max out CPU
        status = allocOpPagePoolNode(&node, loadInfo->dstXdb);
        BailIfFailed(status);

        loadInfo->totalAllocs++;

        node->listNext = loadInfo->allNodesList;
        loadInfo->allNodesList = node;
    } else {
        node = loadInfo->xdbPagePool;
        loadInfo->xdbPagePool = node->next;
        loadInfo->poolSize--;
    }

    assert(node->inUse == false);
    node->inUse = true;

CommonExit:
    insertHandle->node = node;
    insertHandle->loadInfo = loadInfo;

    if (locked) {
        loadInfo->lock.unlock();
    }
    return status;
}

void
opPutInsertHandle(OpInsertHandle *insertHandle)
{
    OpLoadInfo *loadInfo = insertHandle->loadInfo;
    OpPagePoolNode *node = insertHandle->node;

    if (node != NULL) {
        // put our current partially filled page into the pool
        assert(node->inUse == true);
        loadInfo->lock.lock();
        node->next = loadInfo->xdbPagePool;
        loadInfo->xdbPagePool = node;
        loadInfo->poolSize++;
        node->inUse = false;
        loadInfo->lock.unlock();
    }
}

Status
opPopulateInsertHandle(OpInsertHandle *insertHandle, NewKeyValueEntry *kvEntry)
{
    Status status = StatusOk;
    XdbMgr *xdbMgr = XdbMgr::get();
    OpPagePoolNode *node = insertHandle->node;
    Xdb *xdb = insertHandle->loadInfo->dstXdb;
    XdbMeta *xdbMeta = xdbMgr->xdbGetMeta(xdb);
    bool retIsValid;
    DfFieldValue key = kvEntry->getKey(&retIsValid);
    bool xdbPageAlloced = false;

    switch (insertHandle->insertState) {
    case XdbInsertRandomHash: {
        if (unlikely(node->page == NULL)) {
            node->page = xdbMgr->xdbAllocXdbPage(Txn::currentTxn().id_, xdb);
            BailIfNullWith(node->page, StatusNoXdbPageBcMem);

            xdbPageAlloced = true;
            xdbMgr->xdbInitXdbPage(xdb,
                                   node->page,
                                   NULL,
                                   XdbMgr::bcSize(),
                                   XdbUnsortedNormal);
        }

        status = xdbMgr->xdbInsertKvIntoXdbPage(node->page,
                                                false,
                                                &node->keyRangeValid,
                                                &node->minKey,
                                                &node->maxKey,
                                                &key,
                                                NULL,
                                                &kvEntry->tuple_,
                                                &xdbMeta->kvNamedMeta.kvMeta_,
                                                xdbMeta->keyAttr[0].type);
        if (status == StatusNoData) {
            if (node->page->tupBuf->getNumTuples() == 0) {
                // can't fit this row into an empty page
                status = StatusMaxRowSizeExceeded;
                goto CommonExit;
            }

            if (xdbPageAlloced) {
                // can't fit this row into an empty page
                status = StatusMaxRowSizeExceeded;
                goto CommonExit;
            }

            // If existing page is full, insert it into the xdb
            // and alloc a new page
            // Refcount was already incremented by a previous call to
            // opPopulateInsertHandle so pass takeRef=false to
            // xdbInsertOnePage
            status = xdbMgr->xdbInsertOnePage(xdb,
                                              node->page,
                                              node->keyRangeValid,
                                              node->minKey,
                                              node->maxKey,
                                              false);
            BailIfFailed(status);

            // our ref has been handed off to the slot, nullify node->page
            node->page = NULL;

            node->page = xdbMgr->xdbAllocXdbPage(Txn::currentTxn().id_, xdb);
            BailIfNullWith(node->page, StatusNoXdbPageBcMem);

            xdbPageAlloced = true;
            xdbMgr->xdbInitXdbPage(xdb,
                                   node->page,
                                   NULL,
                                   XdbMgr::bcSize(),
                                   XdbUnsortedNormal);
            node->keyRangeValid = false;

            status =
                xdbMgr->xdbInsertKvIntoXdbPage(node->page,
                                               false,
                                               &node->keyRangeValid,
                                               &node->minKey,
                                               &node->maxKey,
                                               &key,
                                               NULL,
                                               &kvEntry->tuple_,
                                               &xdbMeta->kvNamedMeta.kvMeta_,
                                               xdbMeta->keyAttr[0].type);
            if (unlikely(status == StatusNoData)) {
                // can't fit this row into an empty page
                status = StatusMaxRowSizeExceeded;
                goto CommonExit;
            }
        }
        break;
    }
    case XdbInsertSlotHash:
    case XdbInsertCrcHash:
    case XdbInsertRangeHash: {
        status = xdbMgr->xdbInsertKvNoLock(xdb,
                                           insertHandle->slotId,
                                           &key,
                                           &kvEntry->tuple_,
                                           insertHandle->insertState);
        break;
    }

    default:
        assert(0);
        status = StatusUnimpl;
    }
CommonExit:
    if (xdbPageAlloced && status != StatusOk) {
        xdbMgr->xdbFreeXdbPage(node->page);
        node->page = NULL;
    }
    return status;
}

Status
opInsertPageChainBatch(XdbPage **xdbPages,
                       unsigned numPages,
                       int pageIdx,
                       int *retPageIdx,
                       NewKeyValueMeta *kvMeta,
                       NewTupleValues *xdbValueArray,
                       Xdb *xdb)
{
    Status status;
    XdbPage *xdbPage = NULL;
    int curPageIdx = pageIdx;

    while (true) {
        if ((unsigned) curPageIdx >= numPages) {
            return StatusNoXdbPageBcMem;
        }
        xdbPage = xdbPages[curPageIdx];
        status = xdbPage->insertKv(kvMeta, xdbValueArray);
        if (status == StatusNoData) {
            if (curPageIdx == pageIdx) {
                curPageIdx++;
                continue;
            } else {
                assert(curPageIdx == pageIdx + 1);
                // This means that the row is too big to fit on one xdbPage
                return StatusMaxRowSizeExceeded;
            }
        } else if (status == StatusOk) {
            *retPageIdx = curPageIdx;
            if (curPageIdx != pageIdx) {
                assert(curPageIdx == pageIdx + 1);
                // Candidate for page out
                XdbMgr::get()->pagePutRef(xdb, 0, xdbPages[pageIdx]);
            }
        }
        break;
    }

    return status;
}

Status
opInsertPageChain(XdbPage **xdbPageOut,
                  uint64_t *numPages,
                  NewKeyValueMeta *kvMeta,
                  NewTupleValues *xdbValueArray,
                  Xdb *xdb)
{
    Status status;
    XdbPage *xdbPage = *xdbPageOut;
    XdbMgr *xdbMgr = XdbMgr::get();
    bool xdbPageAlloced = false;

    if (!xdbPage) {
        // First page to be slotted
        xdbPage = xdbMgr->xdbAllocXdbPage(Txn::currentTxn().id_,
                                          xdb,
                                          BcHandle::BcScanCleanoutToFree);
        if (xdbPage == NULL) {
            return StatusNoXdbPageBcMem;
        }

        xdbPageAlloced = true;
        xdbMgr->xdbInitXdbPage(kvMeta,
                               xdbPage,
                               NULL,
                               XdbMgr::bcSize(),
                               XdbUnsortedNormal);

        status = xdbPage->insertKv(kvMeta, xdbValueArray);
    } else {
        // Insert into exisiting page
        status = xdbPage->insertKv(kvMeta, xdbValueArray);

        // If exisitng page is full, alloc a new page
        if (status == StatusNoData) {
            XdbPage *xdbPageTemp = xdbPage;

            xdbPage = xdbMgr->xdbAllocXdbPage(Txn::currentTxn().id_,
                                              xdb,
                                              BcHandle::BcScanCleanoutToFree);
            if (xdbPage == NULL) {
                return StatusNoXdbPageBcMem;
            }

            xdbPageAlloced = true;
            xdbMgr->xdbInitXdbPage(kvMeta,
                                   xdbPage,
                                   xdbPageTemp,
                                   XdbMgr::bcSize(),
                                   XdbUnsortedNormal);

            status = xdbPage->insertKv(kvMeta, xdbValueArray);
            XdbMgr::get()->pagePutRef(xdb, 0, xdbPageTemp);
        }
    }

    if (status == StatusNoData) {
        // this means that the row is too big to fit on one xdbPage
        status = StatusMaxRowSizeExceeded;
    }

    if (status != StatusOk && xdbPageAlloced) {
        xdbMgr->xdbFreeXdbPage(xdbPage);
        xdbPage = NULL;
    }

    if (xdbPageAlloced && numPages) {
        (*numPages)++;
    }

    *xdbPageOut = xdbPage;
    return status;
}

Status
groupSlot(Xdb *srcXdb,
          Xdb *dstXdb,
          OpKvEntryCopyMapping *mapping,
          uint64_t slotId,
          unsigned numGroupEvals,
          GroupEvalContext *grpCtxs)
{
    Status status;
    TableCursor cur;
    bool srcCursorInited = false;

    OpInsertHandle insertHandle;
    bool insertHandleInit = false;

    assert(numGroupEvals < TupleMaxNumValuesPerRecord);
    Vector<ValAccumulator *> accs;
    accs.addZeros(numGroupEvals);

    XdbMeta *srcMeta = XdbMgr::xdbGetMeta(srcXdb);
    XdbMeta *dstMeta = XdbMgr::xdbGetMeta(dstXdb);

    NewKeyValueMeta *srcKvMeta = &srcMeta->kvNamedMeta.kvMeta_;
    NewKeyValueMeta *dstKvMeta = &dstMeta->kvNamedMeta.kvMeta_;

    unsigned srcNumFields = srcKvMeta->tupMeta_->getNumFields();
    unsigned dstNumFields = dstKvMeta->tupMeta_->getNumFields();

    NewKeyValueEntry curKv(srcKvMeta);
    NewKeyValueEntry srcKv(srcKvMeta);

    NewKeyValueEntry dstKv(dstKvMeta);

    bool sameKey = true;
    bool cursorExhausted = false;
    Ordering cursorOrdering;

    status =
        opGetInsertHandle(&insertHandle, &dstMeta->loadInfo, XdbInsertSlotHash);
    BailIfFailed(status);
    insertHandleInit = true;

    insertHandle.slotId = slotId;

    if (srcMeta->keyAttr[0].valueArrayIndex == InvalidIdx) {
        // Data has no key and therefore no ordering, no need to sort
        cursorOrdering = Unordered;
    } else {
        cursorOrdering = PartialAscending;
    }

    for (unsigned ii = 0; ii < numGroupEvals; ii++) {
        accs[ii] = GroupEvalContext::allocAccumulator(grpCtxs[ii].accType);
        BailIfNull(accs[ii]);

        accs[ii]->init(grpCtxs[ii].argType);
    }

    // need sorted cursor to group keys together
    status =
        CursorManager::get()->createOnSlot(srcMeta->xdbId,
                                           slotId,
                                           XdbMgr::getSlotStartRecord(srcXdb,
                                                                      slotId),
                                           cursorOrdering,
                                           &cur);
    if (status == StatusNoData) {
        if (srcMeta->keyAttr[0].valueArrayIndex == InvalidIdx) {
            // this indicates that we are performing an aggregate
            // add null results without keys

            dstKv.init();

            for (unsigned ii = 0; ii < numGroupEvals; ii++) {
                DfFieldValue result;
                DfFieldType resultType;
                status = accs[ii]->getResult(&resultType, &result);

                if (status == StatusOk) {
                    // add result to dst kv
                    dstKv.tuple_.set(grpCtxs[ii].resultIdx, result, resultType);
                } else {
                    assert(!dstKv.tuple_.isValid(grpCtxs[ii].resultIdx));
                }
                accs[ii]->reset();
            }

            status = opPopulateInsertHandle(&insertHandle, &dstKv);
            BailIfFailed(status);
        }

        cursorExhausted = true;
        status = StatusOk;
        goto CommonExit;
    }
    BailIfFailed(status);

    srcCursorInited = true;

    status = cur.getNext(&srcKv);
    assert(status == StatusOk);
    BailIfFailed(status);

    while (!cursorExhausted) {
        dstKv.init();
        for (unsigned ii = 0; ii < numGroupEvals; ii++) {
            accs[ii]->reset();
        }

        srcKv.tuple_.cloneTo(srcKvMeta->tupMeta_, &curKv.tuple_);

        while (sameKey) {
            for (unsigned ii = 0; ii < numGroupEvals; ii++) {
                bool valid = false;
                DfFieldValue val;

                if (grpCtxs[ii].argIdx != InvalidIdx) {
                    val = srcKv.tuple_.get(grpCtxs[ii].argIdx,
                                           srcNumFields,
                                           grpCtxs[ii].argType,
                                           &valid);
                } else if (grpCtxs[ii].constantValid) {
                    val = grpCtxs[ii].constantVal;
                    valid = true;
                }

                if (valid || grpCtxs[ii].includeNulls) {
                    status = accs[ii]->add(val, &cur);
                    BailIfFailed(status);
                }
            }

            status = cur.getNext(&srcKv);
            if (status == StatusNoData) {
                cursorExhausted = true;
                break;
            }
            assert(status == StatusOk);
            BailIfFailed(status);

            if (srcMeta->keyAttr[0].valueArrayIndex != InvalidIdx) {
                int ret = DataFormat::fieldArrayCompare(srcMeta->numKeys,
                                                        NULL,
                                                        srcMeta->keyIdxOrder,
                                                        srcKvMeta->tupMeta_,
                                                        &srcKv.tuple_,
                                                        srcMeta->keyIdxOrder,
                                                        srcKvMeta->tupMeta_,
                                                        &curKv.tuple_);
                sameKey = (ret == 0);
            }
        }

        // We've accumulated all of the values for a group. Now time to insert
        // the result into the desination table.
        if (mapping->numEntries == 0) {
            // we didn't explictly set up a mapping from src, so we don't
            // need to copy over any fields in particular.
            // The keys are always required, so just copy them over
            for (unsigned ii = 0; ii < dstMeta->numKeys; ii++) {
                int keyIndex = srcMeta->keyAttr[ii].valueArrayIndex;

                DfFieldType typeTmp =
                    srcKvMeta->tupMeta_->getFieldType(keyIndex);
                bool retIsValid;
                DfFieldValue valueTmp = curKv.tuple_.get(keyIndex,
                                                         srcNumFields,
                                                         typeTmp,
                                                         &retIsValid);
                if (retIsValid) {
                    dstKv.tuple_.set(ii,
                                     valueTmp,
                                     dstKvMeta->tupMeta_->getFieldType(ii));
                } else {
                    assert(!dstKv.tuple_.isValid(ii));
                }
            }
        } else {
            // use the supplied mapping to copy the required fields from src
            Operators::shallowCopyKvEntry(&dstKv,
                                          dstNumFields,
                                          &curKv,
                                          mapping,
                                          0,
                                          mapping->numEntries);
        }

        // Now add the groupby results to dest kv.
        for (unsigned ii = 0; ii < numGroupEvals; ii++) {
            DfFieldValue result;
            DfFieldType resultType;
            status = accs[ii]->getResult(&resultType, &result);

            assert(status != StatusOk ||
                   resultType == dstKvMeta->tupMeta_->getFieldType(
                                     grpCtxs[ii].resultIdx));
            if (status == StatusOk) {
                // add result to dst kv
                dstKv.tuple_.set(grpCtxs[ii].resultIdx, result, resultType);
            } else {
                assert(!dstKv.tuple_.isValid(grpCtxs[ii].resultIdx));
            }
            accs[ii]->reset();
        }

        status = opPopulateInsertHandle(&insertHandle, &dstKv);
        BailIfFailed(status);

        sameKey = true;
    }

    status = StatusOk;
CommonExit:
    if (insertHandleInit) {
        opPutInsertHandle(&insertHandle);
        insertHandleInit = false;
    }

    for (unsigned ii = 0; ii < numGroupEvals; ii++) {
        if (accs[ii]) {
            delete accs[ii];
        }
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&cur);
        srcCursorInited = false;
    }

    return status;
}

Status
groupSlotHash(Xdb *srcXdb,
              Xdb *dstXdb,
              OpKvEntryCopyMapping *mapping,
              uint64_t slotId,
              unsigned numGroupEvals,
              GroupEvalContext *grpCtxs)
{
    Status status;
    TableCursor cur;
    bool srcCursorInited = false;

    OpInsertHandle insertHandle;
    bool insertHandleInit = false;

    assert(numGroupEvals < TupleMaxNumValuesPerRecord);

    XdbMeta *srcMeta = XdbMgr::xdbGetMeta(srcXdb);
    XdbMeta *dstMeta = XdbMgr::xdbGetMeta(dstXdb);

    NewKeyValueMeta *srcKvMeta = &srcMeta->kvNamedMeta.kvMeta_;
    NewKeyValueMeta *dstKvMeta = &dstMeta->kvNamedMeta.kvMeta_;

    unsigned srcNumFields = srcKvMeta->tupMeta_->getNumFields();
    unsigned dstNumFields = dstKvMeta->tupMeta_->getNumFields();

    NewKeyValueEntry srcKv(srcKvMeta);
    NewKeyValueEntry dstKv(dstKvMeta);
    NewTupleValues *tupleTmp = NULL;

    AccumulatorType *accTypes = NULL;
    DfFieldType *argTypes = NULL;
    AccumulatorHashTable *accHashTable = NULL;

    Ordering cursorOrdering = Unordered;

    accTypes = new (std::nothrow) AccumulatorType[numGroupEvals];
    BailIfNull(accTypes);
    argTypes = new (std::nothrow) DfFieldType[numGroupEvals];
    BailIfNull(argTypes);

    for (unsigned ii = 0; ii < numGroupEvals; ++ii) {
        accTypes[ii] = grpCtxs[ii].accType;
        argTypes[ii] = grpCtxs[ii].argType;
    }
    accHashTable = new (std::nothrow) AccumulatorHashTable();
    BailIfNull(accHashTable);
    status = accHashTable->init(accTypes, argTypes, numGroupEvals);
    BailIfFailed(status);

    status =
        CursorManager::get()->createOnSlot(srcMeta->xdbId,
                                           slotId,
                                           XdbMgr::getSlotStartRecord(srcXdb,
                                                                      slotId),
                                           cursorOrdering,
                                           &cur);
    if (status == StatusNoData) {
        status = StatusOk;
        goto CommonExit;
    }
    BailIfFailed(status);
    srcCursorInited = true;

    status = cur.getNext(&srcKv);
    assert(status == StatusOk);
    BailIfFailed(status);

    status =
        opGetInsertHandle(&insertHandle, &dstMeta->loadInfo, XdbInsertSlotHash);
    BailIfFailed(status);

    insertHandleInit = true;
    insertHandle.slotId = slotId;

    while (true) {
        AccumulatorHashTableEntry *curEntry =
            accHashTable->insert(&srcKv.tuple_, srcMeta);
        BailIfNullXdb(curEntry);

        for (unsigned ii = 0; ii < numGroupEvals; ii++) {
            bool valid = false;
            DfFieldValue val;

            if (grpCtxs[ii].argIdx != InvalidIdx) {
                val = srcKv.tuple_.get(grpCtxs[ii].argIdx,
                                       srcNumFields,
                                       grpCtxs[ii].argType,
                                       &valid);
            } else if (grpCtxs[ii].constantValid) {
                val = grpCtxs[ii].constantVal;
                valid = true;
            }

            if (valid || grpCtxs[ii].includeNulls) {
                status = curEntry->accs[ii]->add(val, &cur);
                BailIfFailed(status);
            }
        }

        status = cur.getNext(&srcKv);
        if (status == StatusNoData) {
            break;
        }
        BailIfFailed(status);
    }

    // We've accumulated all of the values for all groups. Now time to insert
    // the results into the desination table.
    tupleTmp = new (std::nothrow) NewTupleValues();
    BailIfNull(tupleTmp);
    for (const auto it = accHashTable->begin(); it && it->entry; it->next()) {
        const NewTupleMeta *tupleMeta =
            it->entry->xdbMeta->kvNamedMeta.kvMeta_.tupMeta_;
        tupleTmp->cloneFrom(tupleMeta,
                            it->entry->tupleFields,
                            it->entry->tupleBitMaps);

        dstKv.init();
        if (mapping->numEntries == 0) {
            // we didn't explictly set up a mapping from src, so we don't
            // need to copy over any fields in particular.
            // The keys are always required, so just copy them over
            for (unsigned ii = 0; ii < dstMeta->numKeys; ii++) {
                int keyIndex = srcMeta->keyAttr[ii].valueArrayIndex;

                DfFieldType typeTmp =
                    srcKvMeta->tupMeta_->getFieldType(keyIndex);
                bool retIsValid;
                DfFieldValue valueTmp =
                    tupleTmp->get(keyIndex, srcNumFields, typeTmp, &retIsValid);
                if (retIsValid) {
                    dstKv.tuple_.set(ii,
                                     valueTmp,
                                     dstKvMeta->tupMeta_->getFieldType(ii));
                } else {
                    assert(!dstKv.tuple_.isValid(ii));
                }
            }
        } else {
            NewKeyValueEntry curKv(srcKvMeta);
            tupleTmp->cloneTo(srcKvMeta->tupMeta_, &curKv.tuple_);
            // use the supplied mapping to copy the required fields from src
            Operators::shallowCopyKvEntry(&dstKv,
                                          dstNumFields,
                                          &curKv,
                                          mapping,
                                          0,
                                          mapping->numEntries);
        }

        // Now add the groupby results to dest kv.
        for (unsigned ii = 0; ii < numGroupEvals; ii++) {
            DfFieldValue result;
            DfFieldType resultType;
            status = it->entry->accs[ii]->getResult(&resultType, &result);

            assert(status != StatusOk ||
                   resultType == dstKvMeta->tupMeta_->getFieldType(
                                     grpCtxs[ii].resultIdx));
            if (status == StatusOk) {
                // add result to dst kv
                dstKv.tuple_.set(grpCtxs[ii].resultIdx, result, resultType);
            } else {
                assert(!dstKv.tuple_.isValid(grpCtxs[ii].resultIdx));
            }
        }

        status = opPopulateInsertHandle(&insertHandle, &dstKv);
        BailIfFailed(status);
    }

CommonExit:
    if (insertHandleInit) {
        opPutInsertHandle(&insertHandle);
        insertHandleInit = false;
    }

    if (srcCursorInited) {
        CursorManager::get()->destroy(&cur);
        srcCursorInited = false;
    }

    if (tupleTmp) {
        delete tupleTmp;
        tupleTmp = NULL;
    }

    if (accHashTable) {
        delete accHashTable;
        accHashTable = NULL;
    }

    if (accTypes) {
        delete[] accTypes;
        accTypes = NULL;
    }

    if (argTypes) {
        delete[] argTypes;
        argTypes = NULL;
    }

    return status;
}
