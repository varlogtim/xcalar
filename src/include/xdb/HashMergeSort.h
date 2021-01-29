// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _HASHMERGESORT_H_
#define _HASHMERGESORT_H_

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "xdb/DataModelTypes.h"
#include "bc/BufferCache.h"
#include "xdb/Xdb.h"
#include "newtupbuf/NewTuplesCursor.h"
#include "msg/Message.h"
#include "util/MemoryPool.h"

class HashMergeSort
{
  public:
    HashMergeSort();
    ~HashMergeSort(){};

    struct HashToken {
        NewTuplesCursor tupCursor;
        DfFieldValue key;
        // using the unused memory due to alignment,
        // So HeapNode can use leafId from here
        unsigned leafId;
        bool keyValid;
    };

    struct HashLeaf {
        unsigned numTokens;
        HashLeaf *next;
        HashToken tokens[0];
    };

    struct HashNode {
        union {
            HashNode *nodes[0];
            HashLeaf *leaves[0];
        };
    };

    struct HashLeafCursor {
        HashLeaf *leaf;
        unsigned cur;
    };

    struct HeapNode {
        HashToken *token;
    };

    XdbMgr *xdbMgr_;
    MsgMgr *msgMgr_;
    unsigned entriesPerHashNode_;
    unsigned entriesPerHashLeaf_;
    unsigned hashNodeSize_;
    unsigned hashLeafSize_;

    const NewKeyValueMeta *kvMeta_;
    Ordering ordering_;
    unsigned maxIter_;

    DfFieldValue *ranges_ = NULL;

    HashToken *tempTokens_ = NULL;

    // buffers for the sorted pages, allocate the same amount as the
    // unsorted pages we might end up needing more because of some
    // fragmentation issues
    XdbPage **xdbPageBufs_ = NULL;
    uint64_t numXdbPageBufs_ = 0;
    uint64_t curXdbPageBuf_ = 0;
    // keep track of the chain of spillover xdb pages in case we need to free
    XdbPage *firstExtraXdbPage_ = NULL;

    Xdb *xdb_;
    XdbMeta *xdbMeta_;
    int *fieldOrderArray_ = NULL;
    XdbPage *nextPage_ = NULL;
    uint64_t count_;
    uint64_t pageCount_ = 0;
    uint64_t slotId_;
    DfFieldType keyType_;

    // to assert unbounded nodes and leaf growth
    uint64_t maxHashNodeBufs_ = 0;
    uint64_t numHashNodeBufsAlloced_ = 0;
    uint64_t maxHashLeafBufs_ = 0;
    uint64_t numHashLeafBufsAlloced_ = 0;

    Status sort(XdbPage *firstPage,
                const NewKeyValueMeta *kvMeta,
                DfFieldValue min,
                DfFieldValue max,
                uint64_t numEntries,
                uint64_t slotId,
                Xdb *xdb,
                XdbPage **sortedPageOut,
                uint64_t *numPagesOut);

  private:
    // this pool is used to allocate hash leaf nodes
    MemoryPool *leafMemPool_ = NULL;
    // this is a general pool which can be used for hashNodes, etc
    MemoryPool *memPool_ = NULL;

    Status allocXdbPageBufs(uint64_t numPages);
    Status calculateDivs(const DfFieldValue &range);

    void *getHashNodeBuf();
    void *getHashLeafBuf();

    // can fail
    XdbPage *getXdbPageBuf();

    Status insertIntoXdbPages(HashNode *head, XdbPage **pageOut);

    MustCheck Status getNextMin(unsigned iter,
                                uint64_t slot,
                                const DfFieldValue *curMin,
                                DfFieldValue *nextMinOut);

    Status addKvToLeaf(NewTuplesCursor *tupCursor,
                       DfFieldValue *keys,
                       bool *keyValid,
                       HashLeaf **leaves,
                       uint64_t slot);

    Status addKvToNode(NewTuplesCursor *tupCursor,
                       DfFieldValue *keys,
                       bool *keyValid,
                       HashNode *node,
                       DfFieldValue min,
                       unsigned iter);

    void sortLeaves(HashNode *head, unsigned iter);

    void mergeSortLeaf(HashLeaf *firstLeaf);

    void mergeSort(HashToken *tokens, uint64_t count);

    Status nodeInsert(HashNode *node, unsigned iter);

    Status leafInsert(HashLeaf *firstLeaf);

    int inline cursorCompare(NewTuplesCursor *c1, NewTuplesCursor *c2);

    int inline fieldCompare(DfFieldValue k1,
                            bool k1Valid,
                            DfFieldValue k2,
                            bool k2Valid);

    int hashTokenCompare(HashToken *t1, HashToken *t2);

    void inline heapInsert(HeapNode *minHeap,
                           unsigned heapSize,
                           unsigned leafId,
                           HashToken *token);

    unsigned inline heapRemove(HeapNode *minHeap, unsigned heapSize);
};

#endif  // _HASHMERGESORT_H_
