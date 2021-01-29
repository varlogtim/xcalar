// Copyright 2013 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _BUFCACHEMEMMGR_H_
#define _BUFCACHEMEMMGR_H_

#include "primitives/Primitives.h"
#include "shmsg/SharedMemory.h"
#include "bc/BufferCache.h"

// Buffer Cache Memory Manager. Note that there is just one instance of
// this in the system.
// - Manages the underlying memory for the entire system.
// - During boot strapping, this can be set up to manage large contiguous
// address space or use just malloc.
// XXX This can potentially be it's own library and hence preserve the
// abstraction.
class BufCacheMemMgr final
{
  public:
    enum Type : uint8_t {
        TypeMin = 0,
        TypeContigAlloc,  // Use contiguous Virtual Memory.
        TypeMalloc,       // Use Malloc.
    };

    static MustCheck Status init(BufferCacheMgr::Type type,
                                 uint64_t bufCacheSize);

    void destroy();

    MustCheck uint64_t getSize();

    MustCheck uint64_t getMlockedSize();

    MustCheck void *getStartAddr();

    MustCheck uint64_t getBufCacheOffset(void *ptr);

    MustCheck Type getType();

    static MustCheck BufCacheMemMgr *get();

    uint64_t MustCheck getNextOffset();

    void setNextOffset(uint64_t offset);

    static MustCheck SharedMemory::Type shmType();

  private:
    static constexpr const uint64_t PrefillPattern = 0xf111cafef111cafe;
    Type type_ = TypeMin;

    // Start address of contiguous address space.
    void *startAddr_ = NULL;

    // Next available spot in contiguous address space.
    uint64_t nextOffset_ = 0;

    // backing size in Bytes.
    uint64_t bufCacheSize_ = 0;

    // Shared memory allocation
    SharedMemory shm_;

    MustCheck Status setUpShm();

    void tearDownShm();

    static BufCacheMemMgr *instance;

    BufCacheMemMgr() {}

    ~BufCacheMemMgr() {}

    BufCacheMemMgr(const BufCacheMemMgr &) = delete;
    BufCacheMemMgr &operator=(const BufCacheMemMgr &) = delete;
};

#endif  // _BUFCACHEMEMMGR_H_
