// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#ifdef XLR_VALGRIND
#include <valgrind/valgrind.h>
#endif

#include "util/MemTrack.h"
#include "util/Math.h"
#include "bc/BufCacheMemMgr.h"
#include "bc/BufCacheObjectMgr.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libbc";

uint64_t
BcObject::getObjectSize()
{
#ifdef XLR_VALGRIND
    return this->numElements_ * this->vgBufferSize();
#else
    return this->numElements_ * this->bufferSize_;
#endif
}

bool
BcObject::alignedAllocs()
{
    return this->alignment_ == 1 ? false : true;
}

Status
BcObject::setUp(const char *objectName,
                uint64_t numElements,
                uint64_t alignment,
                uint64_t bufferSize,
                Init initFlag,
                BufferCacheObjects objectId)
{
    Status status = StatusOk;
    this->objectName_ = objectName;
    this->numElements_ = numElements;
    this->bufferSize_ = bufferSize;
    assert((initFlag & InitSlowAllocsOk) ^ (initFlag & InitFastAllocsOnly));
    this->initFlag_ = initFlag;
    this->objectId_ = objectId;
    this->alignment_ = alignment;

    if (numElements == 0) {
        goto CommonExit;
    }

    if (BufCacheMemMgr::get()->getType() == BufCacheMemMgr::TypeContigAlloc &&
        objectId != BufferCacheObjects::ObjectIdUnknown) {
        void *startAddr = BufCacheMemMgr::get()->getStartAddr();
        uint64_t curOffset = BufCacheMemMgr::get()->getNextOffset();
        uint64_t endOffset =
            roundUp((uintptr_t) startAddr + curOffset, this->alignment_) +
            this->getObjectSize() - (uintptr_t) startAddr;
        if (endOffset > BufCacheMemMgr::get()->getSize()) {
            status = StatusOverflow;
            xSyslog(moduleName,
                    XlogErr,
                    "BcObject::setUp failed, objectName %s numElements %lu"
                    " bufferSize %lu initFlag %u objectId %u: %s",
                    objectName,
                    numElements,
                    bufferSize,
                    initFlag,
                    objectId,
                    strGetFromStatus(status));
            return status;
        }

        BufCacheMemMgr::get()->setNextOffset(endOffset);
        this->baseAddr_ = (void *) ((uintptr_t) startAddr + endOffset -
                                    this->getObjectSize());
        assert(!((uintptr_t) this->baseAddr_ % this->alignment_));
    } else {
        assert(BufCacheMemMgr::get()->getType() == BufCacheMemMgr::TypeMalloc ||
               objectId == BufferCacheObjects::ObjectIdUnknown);
        if (this->alignment_ == 1) {
            // Unaligned allocs.
            this->baseAddr_ = memAllocExt(this->getObjectSize(), moduleName);
        } else {
            // Aligned allocs.
            this->baseAddr_ = memAllocAlignedExt(this->alignment_,
                                                 this->getObjectSize(),
                                                 moduleName);
        }

#ifdef XLR_VALGRIND
        VALGRIND_CREATE_MEMPOOL(this->baseAddr_, 0, 0);
#endif

        if (this->baseAddr_ == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "BcObject::setUp failed, objectName %s numElements %lu"
                    " bufferSize %lu initFlag %u objectId %u: %s",
                    objectName,
                    numElements,
                    bufferSize,
                    initFlag,
                    objectId,
                    strGetFromStatus(status));
            return status;
        }
    }

    if (this->initFlag_ & InitDontCoreDump) {
        verifyOk(memMarkDontCoreDump(this->baseAddr_, this->getObjectSize()));
    }

    this->endAddr_ =
        (void *) ((uintptr_t) this->baseAddr_ + this->getObjectSize());

CommonExit:
    xSyslog(moduleName,
            XlogDebug,
            "BcObject::setUp objectName %s numElements %lu"
            " bufferSize %lu totalSize %lu initFlag %u objectId %u",
            objectName,
            numElements,
            bufferSize,
            numElements * bufferSize,
            initFlag,
            objectId);
    return StatusOk;
}

void
BcObject::tearDown()
{
    if (this->numElements_ != 0) {
        if (this->initFlag_ & InitDontCoreDump) {
            verifyOk(memMarkDoCoreDump(this->baseAddr_, this->getObjectSize()));
        }

        if (BufCacheMemMgr::get()->getType() ==
            BufCacheMemMgr::TypeContigAlloc) {
            // NOOP
        } else {
            // The number of elements might have been memaligned, or malloc,
            // depending on the size
            assert(BufCacheMemMgr::get()->getType() ==
                   BufCacheMemMgr::TypeMalloc);
#ifdef XLR_VALGRIND
            VALGRIND_DESTROY_MEMPOOL(this->baseAddr_);
#endif
            if (this->alignedAllocs()) {
                memAlignedFree(this->baseAddr_);
            } else {
                memFree(this->baseAddr_);
            }
        }
    }

    this->baseAddr_ = NULL;
    this->alignment_ = 0;
    this->objectId_ = BufferCacheObjects::ObjectIdUnknown;
    this->initFlag_ = InitNone;
    this->bufferSize_ = 0;
    this->numElements_ = 0;
    this->objectName_ = NULL;
}

void *
BcObject::getBaseAddr()
{
    return this->baseAddr_;
}

void *
BcObject::getEndAddr()
{
    return this->endAddr_;
}
