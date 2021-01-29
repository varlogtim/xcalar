// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <math.h>

#include "df/DataFormat.h"
#include "xdb/HashMergeSort.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "newtupbuf/NewTuplesCursor.h"
#include "util/DFPUtils.h"

static constexpr const char *moduleName = "hashMergeSort";

HashMergeSort::HashMergeSort()
{
    xdbMgr_ = XdbMgr::get();
    msgMgr_ = MsgMgr::get();
}

int
HashMergeSort::cursorCompare(NewTuplesCursor *c1, NewTuplesCursor *c2)
{
    int ret = 0;
    Status status;
    NewTuplesCursor c1Tmp = *c1;
    NewTuplesCursor c2Tmp = *c2;

    NewKeyValueEntry kv1(kvMeta_);
    status = c1Tmp.getNext(kv1.kvMeta_->tupMeta_, &kv1.tuple_);
    assert(status == StatusOk);
    NewKeyValueEntry kv2(kvMeta_);
    status = c2Tmp.getNext(kv2.kvMeta_->tupMeta_, &kv2.tuple_);
    assert(status == StatusOk);

    ret = DataFormat::fieldArrayCompare(xdbMeta_->numKeys,
                                        xdbMeta_->keyOrderings,
                                        fieldOrderArray_,
                                        kv1.kvMeta_->tupMeta_,
                                        &kv1.tuple_,
                                        fieldOrderArray_,
                                        kv2.kvMeta_->tupMeta_,
                                        &kv2.tuple_);
    return ret;
}

int
HashMergeSort::fieldCompare(DfFieldValue k1,
                            bool k1Valid,
                            DfFieldValue k2,
                            bool k2Valid)
{
    if (!k1Valid && !k2Valid) {
        return 0;
    } else if (!k1Valid) {
        return -1;
    } else if (!k2Valid) {
        return 1;
    }

    int ret;

    ret = DataFormat::fieldCompare(keyType_, k1, k2);

    return ret;
}

int
HashMergeSort::hashTokenCompare(HashToken *t1, HashToken *t2)
{
    int ret;

    if (xdbMeta_->numKeys > 1) {
        ret = cursorCompare(&t1->tupCursor, &t2->tupCursor);
    } else {
        if (ordering_ & AscendingFlag) {
            ret = fieldCompare(t1->key, t1->keyValid, t2->key, t2->keyValid);
        } else {
            ret = fieldCompare(t2->key, t2->keyValid, t1->key, t1->keyValid);
        }
    }

    return ret;
}

Status
HashMergeSort::calculateDivs(const DfFieldValue &range)
{
    Status status = StatusOk;
    DFPUtils *dfp = DFPUtils::get();

    ranges_ = (DfFieldValue *) memAlloc(sizeof(*ranges_) * (maxIter_ + 1));
    BailIfNull(ranges_);

    for (unsigned ii = 0; ii <= maxIter_; ii++) {
        if (keyType_ != DfMoney) {
            ranges_[ii].float64Val =
                ii == 0 ? range.float64Val
                        : ranges_[ii - 1].float64Val / entriesPerHashNode_;
        } else {
            if (ii == 0) {
                ranges_[ii].numericVal = range.numericVal;
            } else {
                XlrDfp dfpEntriesPerNode;
                dfp->xlrDfpUInt32ToNumeric(&dfpEntriesPerNode,
                                           entriesPerHashNode_);
                dfp->xlrDfpDiv(&ranges_[ii].numericVal,
                               &ranges_[ii - 1].numericVal,
                               &dfpEntriesPerNode);
            }
        }
    }

CommonExit:
    return status;
}

Status
HashMergeSort::allocXdbPageBufs(uint64_t numPages)
{
    Status status = StatusOk;
    xdbPageBufs_ =
        (XdbPage **) memAllocExt(numPages * sizeof(*xdbPageBufs_), moduleName);
    BailIfNull(xdbPageBufs_);

    numXdbPageBufs_ = xdbMgr_->xdbAllocXdbPageBatch(xdbPageBufs_,
                                                    numPages,
                                                    XidInvalid,
                                                    xdb_,
                                                    XdbMgr::SlabHint::Default);
CommonExit:
    return status;
}

void *
HashMergeSort::getHashNodeBuf()
{
    void *buf = memPool_->getElem(hashNodeSize_);
    if (unlikely(buf == NULL)) {
        return NULL;
    }
    memZero(buf, hashNodeSize_);
    numHashNodeBufsAlloced_++;
    assert(numHashNodeBufsAlloced_ <= maxHashNodeBufs_);
    return buf;
}

void *
HashMergeSort::getHashLeafBuf()
{
    void *buf = leafMemPool_->getElem(hashLeafSize_);
    if (unlikely(buf == NULL)) {
        return NULL;
    }
    numHashLeafBufsAlloced_++;
    assert(numHashLeafBufsAlloced_ <= maxHashLeafBufs_);
    return buf;
}

XdbPage *
HashMergeSort::getXdbPageBuf()
{
    pageCount_++;

    if (likely(curXdbPageBuf_ < numXdbPageBufs_)) {
        return xdbPageBufs_[curXdbPageBuf_++];
    } else {
        XdbPage *page = (XdbPage *) xdbMgr_->xdbAllocXdbPage(XidInvalid, xdb_);
        if (page == NULL) {
            return page;
        }
        xdbMgr_->xdbInitXdbPage(xdb_,
                                page,
                                NULL,
                                XdbMgr::bcSize(),
                                XdbSortedNormal);

        if (unlikely(firstExtraXdbPage_ == NULL)) {
            firstExtraXdbPage_ = page;
        }

        return page;
    }
}

Status
HashMergeSort::sort(XdbPage *firstPage,
                    const NewKeyValueMeta *kvMeta,
                    DfFieldValue min,
                    DfFieldValue max,
                    uint64_t numEntries,
                    uint64_t slotId,
                    Xdb *xdb,
                    XdbPage **sortedPageOut,
                    uint64_t *numPagesOut)
{
    NewTuplesCursor tupCursor;
    NewTuplesCursor tupCursorTmp;
    XdbPage *xdbPage = firstPage;
    HashNode *head;
    Status status = StatusOk;
    uint64_t pageCount = 0;
    DfFieldValue hashDiv;
    NewTupleValues tuple;

    // MemoryPool max size allocation divided by 8;
    // allocating 8 leaves in one default page size
    const unsigned PageDiv = 8;
    const unsigned MaxDefaultPoolSize =
        XcalarConfig::XdbPageSizeDefault - sizeof(MemoryPool::Pool);
    // XXX: pick these numbers dynmically
    hashNodeSize_ = 128;
    hashLeafSize_ = roundDown(MaxDefaultPoolSize / PageDiv, WordSizeInBytes);

    assertStatic(MaxDefaultPoolSize % PageDiv == 0);

    entriesPerHashNode_ = hashNodeSize_ / sizeof(uintptr_t);
    entriesPerHashLeaf_ =
        (hashLeafSize_ - sizeof(HashLeaf)) / sizeof(HashToken);

    unsigned expectedLeafEntriesExp =
        ceil(log(entriesPerHashLeaf_) / log(entriesPerHashNode_));

    xdbMeta_ = xdbMgr_->xdbGetMeta(xdb);
    keyType_ = xdbMeta_->keyAttr[0].type;

    hashDiv =
        xdbMgr_->getHashDiv(keyType_, &min, &max, true, entriesPerHashNode_);
    ordering_ = xdbMeta_->keyOrderings[0];
    kvMeta_ = kvMeta;
    xdb_ = xdb;

    fieldOrderArray_ = xdbMeta_->keyIdxOrder;
    slotId_ = slotId;

    // maxIter_ is log_b(N)-k where b is entries per node and N is numRows
    // with k such that b^k is the expected number of entries at each leaf
    maxIter_ = (unsigned) (log(numEntries) / log(entriesPerHashNode_));
    if (maxIter_ >= expectedLeafEntriesExp) {
        maxIter_ -= expectedLeafEntriesExp;
    } else {
        maxIter_ = 0;
    }

    calculateDivs(hashDiv);

    for (unsigned iter = 0; iter <= maxIter_; iter++) {
        maxHashNodeBufs_ += pow(entriesPerHashNode_, iter);
    }
    maxHashLeafBufs_ = numEntries / entriesPerHashLeaf_ +
                       pow(entriesPerHashNode_, maxIter_ + 1);
    // can't have more than numEntries leaves
    maxHashLeafBufs_ = fmin(maxHashLeafBufs_, numEntries);

    leafMemPool_ = new (std::nothrow) MemoryPool();
    BailIfNull(leafMemPool_);
    memPool_ = new (std::nothrow) MemoryPool();
    BailIfNull(memPool_);

    assert(hashNodeSize_ <= memPool_->getMaxPoolSize());
    assert(hashLeafSize_ <= leafMemPool_->getMaxPoolSize());

    head = (HashNode *) getHashNodeBuf();
    BailIfNullXdb(head);

    tempTokens_ = (HashToken *) leafMemPool_->getElem(hashLeafSize_);
    BailIfNullXdb(tempTokens_);
    memZero(tempTokens_, hashLeafSize_);

    count_ = 0;
    size_t numFields;
    numFields = kvMeta->tupMeta_->getNumFields();

    while (xdbPage != NULL) {
        status = xdbPage->getRef(xdb_);
        BailIfFailed(status);

        new (&tupCursor) NewTuplesCursor(xdbPage->tupBuf);

        for (uint64_t ii = 0; ii < xdbPage->tupBuf->getNumTuples(); ii++) {
            tupCursorTmp = tupCursor;

            status = tupCursor.getNext(kvMeta->tupMeta_, &tuple);
            assert(status == StatusOk);

            // only hash on the first key
            bool keyValid;
            DfFieldType keyType = kvMeta->tupMeta_->getFieldType(
                xdbMeta_->keyAttr[0].valueArrayIndex);
            DfFieldValue key = tuple.get(xdbMeta_->keyAttr[0].valueArrayIndex,
                                         numFields,
                                         keyType,
                                         &keyValid);

            status = addKvToNode(&tupCursorTmp, &key, &keyValid, head, min, 0);
            BailIfFailed(status);
        }

        xdbPage = (XdbPage *) xdbPage->hdr.nextPage;
        pageCount++;
    }
    assert(count_ == numEntries);

    status = allocXdbPageBufs(pageCount);
    BailIfFailed(status);

    count_ = 0;
    sortLeaves(head, 0);
    assert(count_ == numEntries);

    count_ = 0;
    status = insertIntoXdbPages(head, sortedPageOut);
    BailIfFailed(status);
    assert(count_ == numEntries);
    *numPagesOut = pageCount_;

CommonExit:
    if (ranges_ != NULL) {
        memFree(ranges_);
        ranges_ = NULL;
    }

    delete leafMemPool_;
    leafMemPool_ = NULL;
    delete memPool_;
    memPool_ = NULL;

    if (xdbPageBufs_ != NULL) {
        if (status != StatusOk) {
            xdbMgr_->xdbFreeXdbPageBatch(xdbPageBufs_, numXdbPageBufs_);
            memFree(xdbPageBufs_);
            xdbPageBufs_ = NULL;

            // free spillover pages
            unsigned curIdx = 0;
            XdbPage *xdbPageBatch[XdbMgr::BatchFreeSize];
            XdbPage *xdbPage = firstExtraXdbPage_;
            while (xdbPage) {
                if (curIdx == XdbMgr::BatchFreeSize) {
                    xdbMgr_->xdbFreeXdbPageBatch(xdbPageBatch, curIdx);
                    curIdx = 0;
                }
                xdbPageBatch[curIdx++] = xdbPage;
                firstExtraXdbPage_ = xdbPage->hdr.nextPage;
                xdbPage = firstExtraXdbPage_;
            }
            if (curIdx) {
                xdbMgr_->xdbFreeXdbPageBatch(xdbPageBatch, curIdx);
            }
        } else {
            // free the unused pages in xdbPageBufs_
            uint64_t unusedPages = numXdbPageBufs_ - curXdbPageBuf_;
            xdbMgr_->xdbFreeXdbPageBatch(&xdbPageBufs_[curXdbPageBuf_],
                                         unusedPages);
            memFree(xdbPageBufs_);
        }
    }

    return status;
}

Status
HashMergeSort::insertIntoXdbPages(HashNode *hashStart, XdbPage **pageOut)
{
    Status status;
    XdbPage *firstPage = NULL;
    nextPage_ = getXdbPageBuf();
    BailIfNullWith(nextPage_, StatusNoXdbPageBcMem);

    nextPage_->hdr.xdbPageType = XdbSortedNormal;
    nextPage_->hdr.pageSize = XdbMgr::bcSize();

    nextPage_->hdr.nextPage = NULL;
    firstPage = nextPage_;

    status = nodeInsert(hashStart, 0);
    BailIfFailed(status);

CommonExit:
    // we don't need to clean up since we've been allocating from
    // xdbHashBufs_, this will get freed upstream
    *pageOut = firstPage;

    return status;
}

Status
HashMergeSort::getNextMin(unsigned iter,
                          uint64_t slot,
                          const DfFieldValue *curMin,
                          DfFieldValue *nextMinOut)
{
    Status status = StatusUnknown;
    DfFieldValue min;
    switch (keyType_) {
    case DfUInt64:
    case DfString:
        // relies on string hack in xdbInsertKv().  maxKey and minKey
        // store the hash of the string, not the string itself
        min.uint64Val =
            curMin->uint64Val + (uint64_t)(ranges_[iter].float64Val * slot);
        break;

    case DfInt64:
        min.int64Val =
            curMin->int64Val + (int64_t)(ranges_[iter].float64Val * slot);
        break;

    case DfFloat64:
        min.float64Val =
            curMin->float64Val + (int64_t)(ranges_[iter].float64Val * slot);
        break;

    case DfBoolean:
        min.boolVal = 0;
        break;

    case DfTimespec:
        min.timeVal.ms =
            curMin->timeVal.ms + (int64_t)(ranges_[iter].float64Val * slot);
        break;

    case DfMoney: {
        Status status;
        DFPUtils *dfp = DFPUtils::get();
        XlrDfp dfpRange;
        XlrDfp dfpSlot;

        status = dfp->xlrDfpUInt64ToNumeric(&dfpSlot, slot);
        assert(status == StatusOk);
        dfp->xlrDfpMulti(&dfpRange, &ranges_[iter].numericVal, &dfpSlot);
        dfp->xlrDfpAdd(&min.numericVal, &curMin->numericVal, &dfpRange);
    } break;

    case DfNull:
    case DfUnknown:
        break;

    case DfBlob:
    case DfMixed:
    default:
        assert(0);
        break;
    }

    *nextMinOut = min;
    status = StatusOk;

    return status;
}

Status
HashMergeSort::addKvToLeaf(NewTuplesCursor *kv,
                           DfFieldValue *keys,
                           bool *keyValid,
                           HashLeaf **leaves,
                           uint64_t slot)
{
    Status status = StatusOk;
    HashLeaf *leaf = leaves[slot];

    leaf->tokens[leaf->numTokens].tupCursor = *kv;
    leaf->tokens[leaf->numTokens].key = keys[0];
    leaf->tokens[leaf->numTokens].keyValid = keyValid[0];
    leaf->numTokens++;
    count_++;

    if (leaf->numTokens == entriesPerHashLeaf_) {
        HashLeaf *newLeaf = (HashLeaf *) getHashLeafBuf();
        BailIfNullXdb(newLeaf);

        newLeaf->next = leaf;
        newLeaf->numTokens = 0;

        leaves[slot] = newLeaf;
    }

CommonExit:
    return status;
}

Status
HashMergeSort::addKvToNode(NewTuplesCursor *kv,
                           DfFieldValue *keys,
                           bool *keyValid,
                           HashNode *node,
                           DfFieldValue min,
                           unsigned iter)
{
    uint64_t slot;
    Status status = StatusOk;

    if (keyValid[0]) {
        status = xdbMgr_->hashXdbFixedRange(keys[0],
                                            xdbMeta_->keyAttr[0].type,
                                            entriesPerHashNode_,
                                            &min,
                                            ranges_[iter],
                                            &slot,
                                            Ascending);
        assert(status == StatusOk);
        BailIfFailed(status);
    } else {
        slot = 0;
    }

    if (iter == maxIter_) {
        if (unlikely(node->leaves[slot] == NULL)) {
            node->leaves[slot] = (HashLeaf *) getHashLeafBuf();
            BailIfNullXdb(node->leaves[slot]);

            node->leaves[slot]->next = NULL;
            node->leaves[slot]->numTokens = 0;
        }

        return addKvToLeaf(kv, keys, keyValid, node->leaves, slot);
    } else {
        DfFieldValue nextMin;
        if (unlikely(node->nodes[slot] == NULL)) {
            node->nodes[slot] = (HashNode *) getHashNodeBuf();
            BailIfNullXdb(node->nodes[slot]);
        }

        status = getNextMin(iter, slot, &min, &nextMin);
        BailIfFailed(status);

        return addKvToNode(kv,
                           keys,
                           keyValid,
                           node->nodes[slot],
                           nextMin,
                           iter + 1);
    }

CommonExit:
    return status;
}

void
HashMergeSort::sortLeaves(HashNode *head, unsigned iter)
{
    for (unsigned ii = 0; ii < entriesPerHashNode_; ii++) {
        if (head->nodes[ii] == NULL) {
            continue;
        }

        if (iter == maxIter_) {
            mergeSortLeaf(head->leaves[ii]);
        } else {
            sortLeaves(head->nodes[ii], iter + 1);
        }
    }
}

void
HashMergeSort::mergeSortLeaf(HashLeaf *firstLeaf)
{
    HashLeaf *leaf = firstLeaf;

    while (leaf) {
        count_ += leaf->numTokens;
        mergeSort(leaf->tokens, leaf->numTokens);
        leaf = leaf->next;
    }
}

void
HashMergeSort::mergeSort(HashToken *tokens, uint64_t count)
{
    uint64_t right, rightEnd;
    uint64_t ii, jj, mm;
    int ret;

    for (uint64_t kk = 1; kk < count; kk *= 2) {
        for (uint64_t left = 0; left + kk < count; left += kk * 2) {
            right = left + kk;
            rightEnd = right + kk;

            if (rightEnd > count) {
                rightEnd = count;
            }

            mm = left;
            ii = left;
            jj = right;

            while (ii < right && jj < rightEnd) {
                ret = hashTokenCompare(&tokens[ii], &tokens[jj]);

                if (ret <= 0) {
                    tempTokens_[mm] = tokens[ii];
                    ++ii;
                } else {
                    tempTokens_[mm] = tokens[jj];
                    ++jj;
                }

                ++mm;
            }

            while (ii < right) {
                tempTokens_[mm] = tokens[ii];
                ++ii;
                ++mm;
            }

            while (jj < rightEnd) {
                tempTokens_[mm] = tokens[jj];
                ++jj;
                ++mm;
            }

            for (mm = left; mm < rightEnd; ++mm) {
                tokens[mm] = tempTokens_[mm];
            }
        }
    }
}

Status
HashMergeSort::nodeInsert(HashNode *node, unsigned iter)
{
    Status status = StatusOk;
    unsigned slot = 0;

    for (unsigned ii = 0; ii < entriesPerHashNode_; ii++) {
        if (ordering_ & DescendingFlag) {
            // if we are desceding, process the nodes in reverse order
            slot = entriesPerHashNode_ - ii - 1;
        } else {
            slot = ii;
        }

        if (node->nodes[slot] == NULL) {
            continue;
        }

        if (iter == maxIter_) {
            status = leafInsert(node->leaves[slot]);
            BailIfFailed(status);
        } else {
            status = nodeInsert(node->nodes[slot], iter + 1);
            BailIfFailed(status);
        }
    }

CommonExit:
    return status;
}

Status
HashMergeSort::leafInsert(HashLeaf *firstLeaf)
{
    Status status = StatusOk;
    unsigned numLeaves = 0;
    unsigned heapSize = 0;
    HeapNode *minHeap = NULL;
    HashLeafCursor *leafArray = NULL;
    NewTupleValues tuple;
    HashLeaf *leaf = firstLeaf;
    while (leaf != NULL) {
        leaf = leaf->next;
        numLeaves++;
    }

    if (numLeaves == 0) {
        goto CommonExit;
    }

    leafArray = (HashLeafCursor *) memAllocExt(numLeaves * sizeof(*leafArray),
                                               moduleName);
    BailIfNull(leafArray);

    minHeap =
        (HeapNode *) memAllocExt(numLeaves * sizeof(*minHeap), moduleName);
    BailIfNull(minHeap);

    leaf = firstLeaf;
    while (leaf != NULL) {
        if (likely(leaf->numTokens > 0)) {
            leafArray[heapSize].leaf = leaf;
            leafArray[heapSize].cur = 0;
            heapInsert(minHeap, heapSize, heapSize, &leaf->tokens[0]);
            heapSize++;
        }
        leaf = leaf->next;
    }

    while (heapSize > 0) {
        int leafId = heapRemove(minHeap, heapSize);
        heapSize--;

        leaf = leafArray[leafId].leaf;
        unsigned cur = leafArray[leafId].cur;

        status = leaf->tokens[cur].tupCursor.getNext(kvMeta_->tupMeta_, &tuple);
        assert(status == StatusOk);

        status = nextPage_->insertKv(kvMeta_, &tuple);
        if (unlikely(status != StatusOk)) {
            XdbPage *newPage = getXdbPageBuf();
            BailIfNullWith(newPage, StatusNoXdbPageBcMem);

            newPage->hdr.xdbPageType = XdbSortedNormal;
            newPage->hdr.pageSize = XdbMgr::bcSize();
            nextPage_->hdr.nextPage = newPage;
            newPage->hdr.prevPage = nextPage_;

            nextPage_ = newPage;

            status = nextPage_->insertKv(kvMeta_, &tuple);
            assert(status == StatusOk);
        }

        count_++;
        cur = ++leafArray[leafId].cur;
        if (likely(cur < leaf->numTokens)) {
            heapInsert(minHeap, heapSize, leafId, &leaf->tokens[cur]);
            heapSize++;
        }
    }

CommonExit:
    if (leafArray) {
        memFree(leafArray);
    }

    if (minHeap) {
        memFree(minHeap);
    }

    return status;
}

void inline HashMergeSort::heapInsert(HeapNode *minHeap,
                                      unsigned heapSize,
                                      unsigned leafId,
                                      HashToken *token)
{
    unsigned index = heapSize;
    token->leafId = leafId;
    minHeap[index].token = token;

    int ret;

    while (index != 0) {
        // parent at (index - 1) / 2
        unsigned parentIndex = (index - 1) >> 1;

        // compare left and right, then compare key with smaller of the 2
        ret = hashTokenCompare(minHeap[parentIndex].token, token);

        if (ret <= 0) {
            // parent smaller or equal to element
            break;
        }

        // swap element with parent
        minHeap[index].token = minHeap[parentIndex].token;
        minHeap[parentIndex].token = token;

        index = parentIndex;
    }
}

unsigned inline HashMergeSort::heapRemove(HeapNode *minHeap, unsigned heapSize)
{
    unsigned minLeaf = minHeap[0].token->leafId;
    if (heapSize == 1) {
        return minLeaf;
    }

    int ret;
    minHeap[0] = minHeap[heapSize - 1];
    heapSize--;

    unsigned index = 0, swapIndex = 0;
    HashToken *token = minHeap[0].token;

    do {
        // leftChild at 2 * index + 1
        unsigned leftIndex = (index << 1) + 1;
        unsigned rightIndex = leftIndex + 1;

        if (leftIndex >= heapSize) {
            // no children, we are at the bottom of the heap
            break;
        }

        if (rightIndex >= heapSize) {
            // no right child, compare key with left
            swapIndex = leftIndex;
        } else {
            // compare left and right, then compare key with smaller of the 2
            ret = hashTokenCompare(minHeap[leftIndex].token,
                                   minHeap[rightIndex].token);

            if (ret < 0) {
                swapIndex = leftIndex;
            } else {
                swapIndex = rightIndex;
            }
        }

        ret = hashTokenCompare(minHeap[swapIndex].token, token);
        if (ret < 0) {
            // child smaller, swap with child
            minHeap[index].token = minHeap[swapIndex].token;
            minHeap[swapIndex].token = token;

            index = swapIndex;
            continue;
        }

        // we are smaller than both children. exit loop
        break;
    } while (index < heapSize - 1);

    return minLeaf;
}
