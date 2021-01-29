// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MEMORYPILE_H_
#define _MEMORYPILE_H_

#include "primitives/Primitives.h"
#include "bc/BufferCache.h"

class MemoryPile
{
  public:
    static MustCheck Status init();

    // Allocate a pile from the xdbPage bc
    static MemoryPile *allocPile(Xid txnId,
                                 Status *statusOut,
                                 size_t alignment,
                                 BcHandle::BcScanCleanoutMode cleanoutMode =
                                     BcHandle::BcScanCleanoutNotToFree);

    // Allocate a pile and set it's default size to elemSize
    static MemoryPile *allocPile(size_t elemSize,
                                 Xid txnId,
                                 Status *statusOut,
                                 size_t alignment,
                                 BcHandle::BcScanCleanoutMode cleanoutMode =
                                     BcHandle::BcScanCleanoutNotToFree);

    // Get an element of size
    static void *getElem(MemoryPile **memoryPileIn,
                         size_t size,
                         Status *statusOut);

    // Return an element to its pile (pileSize is the pile's backing page size)
    static void putElem(void *elem, size_t pileSize);

    // Allow the pile to be freed, must ensure no further getElem will be called
    void markPileAllocsDone();

    // Accesors for the pile's default size
    void setSize(size_t size) { elemSize_ = size; }

    size_t getSize() { return elemSize_; }

  private:
    static constexpr const unsigned BatchAllocSize = 16;
    static inline MemoryPile *getSourcePile(void *elem, size_t pileSize);
    static StatGroupId statsGrpId_;
    static StatHandle numPilePagesAlloced_;
    static StatHandle numPilePagesFreed_;
    void putMemoryPile();
    void freeMemoryPile();

    Xid txnId_ = XidInvalid;
    MemoryPile *batchMemPileNext_ = NULL;
    // Please note that the total size of MemoryPile must be a multiple of
    // XdbMinPageAlignment
    unsigned maxMemory_ = 0;
    Atomic32 count_;
    unsigned nextOffset_ = 0;
    unsigned elemSize_ = 0;
    unsigned alignment_ = 1;

    BcHandle::BcScanCleanoutMode cleanoutMode_ =
        BcHandle::BcScanCleanoutNotToFree;
    uint8_t baseBuf_[0];
};

#endif  // _MEMORYPILE_H_
