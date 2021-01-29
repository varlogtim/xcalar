// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BUFCACHEOBJECTMGR_H_
#define _BUFCACHEOBJECTMGR_H_

#include "bc/BufferCache.h"

// Buffer Cache Object is a single object instance.
class BcObject
{
  public:
    // init flags.
    enum Init : uint32_t {
        InitNone = 0x0,
        InitFastAllocsOnly = 0x1,  // do fast allocs only
        // do slow allocs, i.e. mallocs when fast allocs fail
        InitSlowAllocsOk = 0x2,
        InitDontCoreDump = 0x4,  // exclude from core dumps
    };

    BcObject() {}
    ~BcObject() {}

#ifdef XLR_VALGRIND
    // Under valgrind we bound each buffer by a redzone of the original buffer
    // size on each side.  This preserves the original (non-valgrind) alignment.
    uint64_t vgBufferSize() const { return 3 * bufferSize; };
#endif

    MustCheck uint64_t getNumElements() { return numElements_; }

    MustCheck const char *getObjectName() { return objectName_; }

    MustCheck uint64_t getBufferSize() { return bufferSize_; }

    MustCheck BufferCacheObjects getObjectId() { return objectId_; }

    MustCheck Init getInitFlag() { return initFlag_; }

    // return Total size in bytes, i.e. numElements * bufferSize.
    MustCheck uint64_t getObjectSize();

    // return true for aligned allocs, false otherwise.
    MustCheck bool alignedAllocs();

    MustCheck uint64_t getAlignment() { return alignment_; }

    MustCheck Status setUp(const char *objectName,
                           uint64_t numElements,
                           uint64_t alignment,
                           uint64_t bufferSize,
                           Init initFlag,
                           BufferCacheObjects objectId);

    void tearDown();

    MustCheck void *getBaseAddr();
    MustCheck void *getEndAddr();

  private:
    // Base address from backing Buffer cache memory manager. Note that this
    // would point to contiguous address space.
    void *baseAddr_ = NULL;

    // address of last valid byte in range
    void *endAddr_ = NULL;

    const char *objectName_ = NULL;
    uint64_t numElements_ = 0;
    uint64_t bufferSize_ = 0;
    Init initFlag_ = InitNone;
    BufferCacheObjects objectId_ = BufferCacheObjects::ObjectIdUnknown;
    uint64_t alignment_ = 0;

    BcObject(const BcObject &) = delete;
    BcObject &operator=(const BcObject &) = delete;
};

#endif  // _BUFCACHEOBJECTMGR_H_
