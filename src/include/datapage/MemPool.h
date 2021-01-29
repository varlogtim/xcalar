// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

//
// ----- Overview -----
// This allows for memory access to a set of fixed size pages which use a
// specific allocation pattern.
// The user performs many allocations over time and then free all of them
// in bulk.
// For example: we need to store many small strings during CSV parsing, but
// at after each record we can forget about all of them.
// Allocation sizes must be less than or equal to (pageSize-8)

#ifndef _MEMPOOL_H_
#define _MEMPOOL_H_

#include "primitives/Primitives.h"
#include "util/MemTrack.h"
#include "bc/BufferCache.h"

class IPageAllocator
{
  public:
    virtual int32_t getPageSize() const = 0;
    virtual void *allocatePage(Status *outStatus = nullptr) = 0;
    virtual void *allocatePage(Xid txnXid, Status *outStatus = nullptr) = 0;
    virtual void freePage(void *page) = 0;
};

class MallocAllocator : public IPageAllocator
{
  public:
    void init(int32_t pageSize) { pageSize_ = pageSize; }

    int32_t getPageSize() const override { return pageSize_; }

    void *allocatePage(Status *outStatus) override {
      void *buf = memAlloc(pageSize_);
      if (outStatus) {
        *outStatus = buf ? StatusOk : StatusNoMem;
      }
      return buf;
    }

    void *allocatePage(Xid txnXid, Status *outStatus) override {
      void *buf = memAlloc(pageSize_);
      if (outStatus) {
        *outStatus = buf ? StatusOk : StatusNoMem;
      }
      return buf;
    }

    void freePage(void *page) override { return memFree(page); }

  private:
    int32_t pageSize_;
};

class BufCacheAllocator : public IPageAllocator
{
  public:
    void init(BcHandle *bc) { bc_ = bc; }

    int32_t getPageSize() const override { return bc_->getBufSize(); }

    void *allocatePage(Status *outStatus) override {
      return bc_->allocBuf(XidInvalid, outStatus);
    }

    void *allocatePage(Xid txnXid, Status *outStatus) override {
      return bc_->allocBuf(txnXid, outStatus);
    }

    void freePage(void *page) override { return bc_->freeBuf(page); }

  private:
    BcHandle *bc_;
};

class MemPool final
{
  private:
    struct AllocedPage {
        AllocedPage *nextPage;
        uint8_t data[0];
    };

  public:
    static constexpr int32_t PageOverhead = sizeof(AllocedPage);

    MemPool() = default;
    ~MemPool();

    void init(IPageAllocator *pageAllocator);

    // This will allocate a chunk of memory for the caller
    // size: must be <= (pageSize-8)
    // returns NULL on failure
    void *alloc(int32_t size);

    // Invalidates all previous allocations. Will keep allocated pages around
    // to be used in later allocs
    void resetPool();

  private:
    int32_t curPageSizeRemaining() const;

    // Returns true on success and false on allocation failure
    MustCheck bool gotoNextPage();

    // Returns true on success and false on allocation failure
    MustCheck bool allocNewPage();

    AllocedPage *firstPage_ = NULL;
    AllocedPage *lastPage_ = NULL;
    AllocedPage *curPage_ = NULL;
    uint8_t *curPos_ = NULL;

    IPageAllocator *pageAllocator_ = NULL;
    int32_t pageSize_ = -1;
};

#endif  // _MEMPOOL_H_
