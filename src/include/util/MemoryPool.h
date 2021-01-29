// Copyright 2019 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MEMORYPOOL_H_
#define _MEMORYPOOL_H_

#include "primitives/Primitives.h"
#include "bc/BufferCache.h"

// the interface looks similar compared to MemoryPile. One difference is free
// interface which is a easy way to free up all the memory allocated using this
// as this keeps tracks of memory allocations from buf$.
class MemoryPool
{
  public:
    class Pool
    {
      public:
        size_t count_ = 0;
        uint64_t nextOffset_ = 0;
        Pool *prev_ = NULL;
        Pool *next_ = NULL;
        uint8_t baseBuf_[0];

        void reset();
    };

    // Memory pool allows variable sized allocations and the elements
    // internally track it's size.
    // There is issue of fragmentation because putElem() frees the entire
    // underlying Pool XdbPage only when all the elements are freed.
    // The getElem() always allocates a new XdbPage if the previous Pool
    // XdbPage is full. It does not go back to used freed holes in the previous
    // Pool pages.
    MemoryPool();
    ~MemoryPool();

    // Get an element of elemSize from pool.
    void *getElem(size_t elemSize);
    // Return an element to its pool
    void putElem(void *elem);

    // Return max pool size
    size_t getMaxPoolSize() { return maxMemory_; }

    // Free all pools.
    void free();

  private:
    // Allocate a pool from the xdbPage bc
    Pool *allocPool();
    inline Pool *getSourcePool(void *elem);
    void freePool(Pool *pool);

    // Don't rely on this.
    static constexpr const size_t DefaultElemSize_ = 1;
    Pool *curPool_;
    Xid txnId_;
    size_t elemSize_;
    uint64_t maxMemory_;
    BcHandle::BcScanCleanoutMode cleanoutMode_;
};

#endif  // _MEMORYPOOL_H_
