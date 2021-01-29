// Copyright 2019 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <new>

#include "util/MemoryPool.h"
#include "xdb/Xdb.h"

void
MemoryPool::Pool::reset()
{
    count_ = 0;
    nextOffset_ = 0;
    prev_ = NULL;
    next_ = NULL;
}

MemoryPool::MemoryPool()
    : curPool_(NULL),
      txnId_(Txn::currentTxn().id_),
      elemSize_(DefaultElemSize_),
      maxMemory_(XdbMgr::bcSize() - sizeof *curPool_),
      cleanoutMode_(BcHandle::BcScanCleanoutToFree)
{
}

MemoryPool::~MemoryPool()
{
    free();

    if (curPool_) {
        freePool(curPool_);
    }
}

MemoryPool::Pool *
MemoryPool::allocPool()
{
    Status status = StatusOk;
    Pool *pool = NULL;

    pool = (Pool *) XdbMgr::get()->bcAlloc(txnId_,
                                           &status,
                                           XdbMgr::SlabHint::Default,
                                           cleanoutMode_);
    if (unlikely(pool == NULL)) {
        return NULL;
    }
#ifdef BUFCACHESHADOW
    pool = (Pool *) XdbMgr::get()->xdbGetBackingBcAddr(pool);
#endif
    new (pool) Pool();

    // make sure we are aligned to pool size
    assert(((uintptr_t) pool & ~(XdbMgr::bcSize() - 1)) == (uintptr_t) pool);

    return pool;
}

void *
MemoryPool::getElem(size_t elemSize)
{
    void *elem = NULL;
    Status status = StatusOk;

    if (unlikely(!curPool_)) {
        curPool_ = allocPool();
        BailIfNullXdb(curPool_);
    }

    if (unlikely(elemSize > maxMemory_)) {
        // There's no way it can fit
        status = StatusNoXdbPageBcMem;
        goto CommonExit;
    }

    if (unlikely(curPool_->nextOffset_ + elemSize > maxMemory_)) {
        Pool *newPool = allocPool();
        BailIfNullXdb(newPool);

        curPool_->next_ = newPool;
        newPool->prev_ = curPool_;
        curPool_ = newPool;
    }

    elem = (void *) ((size_t) curPool_->baseBuf_ + curPool_->nextOffset_);
    curPool_->nextOffset_ += elemSize;
    ++curPool_->count_;

CommonExit:
    return status == StatusOk ? elem : NULL;
}

void
MemoryPool::freePool(Pool *pool)
{
    XdbMgr::get()->bcFree(pool);
}

// assumes that pool allocations are poolSize aligned
MemoryPool::Pool *
MemoryPool::getSourcePool(void *elem)
{
    size_t elemAddress = (size_t) elem;
    size_t offset = (size_t) elemAddress % XdbMgr::bcSize();
    return (Pool *) (elemAddress - offset);
}

void
MemoryPool::putElem(void *elem)
{
    Pool *pool = getSourcePool(elem);

    // Otherwise you're putting elem before getting any elem.
    assert(pool->count_ != 0);

    if (unlikely(--(pool->count_) == 0)) {
        // We reuse the first page so we don't need to put and get page for
        // every putElem and getElem operation.
        if (pool->next_ == NULL && pool->prev_ == NULL) {
            assert(pool == curPool_);
            pool->reset();
            return;
        }

        if (likely(pool->next_)) {
            pool->next_->prev_ = pool->prev_;
        }

        if (likely(pool->prev_)) {
            pool->prev_->next_ = pool->next_;
        }

        if (unlikely(pool == curPool_)) {
            curPool_ = curPool_->prev_;
        }

        freePool(pool);
    }
}

void
MemoryPool::free()
{
    // We always keep at least one xdb page.
    while (curPool_ && curPool_->prev_) {
        Pool *prevPool = curPool_->prev_;
        freePool(curPool_);
        curPool_ = prevPool;
    }

    // We reuse the first page so we don't need to put and get page for every
    // free and getElem operation.
    if (curPool_) {
        curPool_->reset();
    }
}
