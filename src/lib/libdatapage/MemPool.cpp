// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "datapage/MemPool.h"

void
MemPool::init(IPageAllocator *pageAllocator)
{
    pageAllocator_ = pageAllocator;
    pageSize_ = pageAllocator_->getPageSize();
}

MemPool::~MemPool()
{
    AllocedPage *page = firstPage_;
    while (page) {
        AllocedPage *nextPage = page->nextPage;
        pageAllocator_->freePage(page);
        page = nextPage;
    }
    firstPage_ = NULL;
    lastPage_ = NULL;
}

void *
MemPool::alloc(int32_t size)
{
    void *result;
    if (unlikely(size > pageSize_ - (int32_t) sizeof(AllocedPage))) {
        assert(false && "this must be caught by the caller");
        return NULL;
    }

    if (unlikely(size > curPageSizeRemaining())) {
        bool success = gotoNextPage();
        if (unlikely(!success)) {
            return NULL;
        }
    }
    assert(curPageSizeRemaining() >= size);
    result = curPos_;
    curPos_ += size;

    return result;
}

int32_t
MemPool::curPageSizeRemaining() const
{
    if (unlikely(curPage_ == NULL)) {
        return 0;
    }
    uintptr_t pageStart = reinterpret_cast<uintptr_t>(curPage_->data);
    uintptr_t curPtr = reinterpret_cast<uintptr_t>(curPos_);
    int32_t usedSpace = curPtr - pageStart + sizeof(AllocedPage);
    return pageSize_ - usedSpace;
}

bool
MemPool::gotoNextPage()
{
    if (unlikely(curPage_ == NULL)) {
        if (unlikely(firstPage_ == NULL)) {
            bool success = allocNewPage();
            if (unlikely(!success)) {
                return false;
            }
        }
        assert(firstPage_ != NULL);
        assert(firstPage_ == lastPage_);

        curPage_ = firstPage_;
        curPos_ = curPage_->data;
    } else if (unlikely(curPage_->nextPage == NULL)) {
        assert(curPage_ == lastPage_);
        bool success = allocNewPage();
        if (unlikely(!success)) {
            return false;
        }
        assert(curPage_->nextPage != NULL);
        assert(curPage_->nextPage == lastPage_);

        curPage_ = curPage_->nextPage;
        curPos_ = curPage_->data;
    } else {
        curPage_ = curPage_->nextPage;
        curPos_ = curPage_->data;
    }
    return true;
}

bool
MemPool::allocNewPage()
{
    AllocedPage *newPage =
        static_cast<AllocedPage *>(pageAllocator_->allocatePage());
    if (unlikely(newPage == NULL)) {
        return false;
    }
    if (unlikely(firstPage_ == NULL)) {
        firstPage_ = newPage;
        lastPage_ = firstPage_;
        lastPage_->nextPage = NULL;
    } else {
        lastPage_->nextPage = newPage;
        lastPage_ = newPage;
        lastPage_->nextPage = NULL;
    }
    return true;
}

void
MemPool::resetPool()
{
    curPage_ = firstPage_;
    if (likely(curPage_)) {
        curPos_ = curPage_->data;
    }
}
