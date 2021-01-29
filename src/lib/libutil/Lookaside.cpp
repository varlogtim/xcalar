// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/Lookaside.h"
#include <sys/mman.h>
#include <unistd.h>
#include "util/System.h"
#include "runtime/Semaphore.h"
#include "util/MemTrack.h"

static Status
initFreeList(LookasideList *lookasideList,
             size_t physicalEltSize,
             unsigned numElements,
             void *buffer)
{
    LookasideListElt *lookasideListElt;
    unsigned ii;

    lookasideList->freeListAnchor.next = lookasideList->freeListAnchor.prev =
        &lookasideList->freeListAnchor;

    for (ii = 0; ii < numElements; ii++) {
        lookasideListElt =
            (LookasideListElt *) ((uintptr_t) buffer + (physicalEltSize * ii));
        // append to list
        lookasideListElt->prev = lookasideList->freeListAnchor.prev;
        lookasideListElt->next = lookasideList->freeListAnchor.prev->next;
        lookasideList->freeListAnchor.prev->next = lookasideListElt;
        lookasideList->freeListAnchor.prev = lookasideListElt;
    }

    return StatusOk;
}

static void
destroyFreeList(LookasideList *lookasideList,
                size_t physicalEltSize,
                unsigned numElements,
                void *buffer)
{
    unsigned ii;
    LookasideListElt *lookasideListElt;

    for (ii = 0; ii < numElements; ii++) {
        lookasideListElt =
            (LookasideListElt *) ((uintptr_t) buffer + (physicalEltSize * ii));
        lookasideListElt->prev->next = lookasideListElt->next;
        lookasideListElt->next->prev = lookasideListElt->prev;
    }

    assert(lookasideList->freeListAnchor.next ==
           &lookasideList->freeListAnchor);

    assert(lookasideList->freeListAnchor.prev ==
           &lookasideList->freeListAnchor);
}

Status
lookasideInit(size_t elementSize,
              unsigned numElements,
              LookasideListOpts lookasideOpts,
              LookasideList *lookasideOut)
{
    size_t physicalEltSize;
    void *lookasideBuffer = MAP_FAILED;
    int ignoreFd = -1;
    off_t ignoreOffset = 0;
    Status status;
    bool listInited = false;
    int ret;
    size_t bufSize;
    enum { Alignment = 8 };

    // Align all elements on 8 byte boundaries
    physicalEltSize =
        roundUp(xcMax(elementSize, sizeof(LookasideListElt)), Alignment);
    bufSize = roundUp(physicalEltSize * numElements, sysconf(_SC_PAGE_SIZE));

    lookasideBuffer = memMap(NULL,
                             bufSize,
                             PROT_READ | PROT_WRITE,
                             MAP_PRIVATE | MAP_ANONYMOUS,
                             ignoreFd,
                             ignoreOffset);
    if (lookasideBuffer == MAP_FAILED) {
        assert(0);
        status = sysErrnoToStatus(errno);
        goto common_exit;
    }
    assert(lookasideBuffer != MAP_FAILED);
    assert(((uintptr_t) lookasideBuffer % sysconf(_SC_PAGE_SIZE)) == 0);

    status = initFreeList(lookasideOut,
                          physicalEltSize,
                          numElements,
                          lookasideBuffer);
    if (status != StatusOk) {
        goto common_exit;
    }
    listInited = true;

    lookasideOut->buffer = lookasideBuffer;
    lookasideOut->bufSize = bufSize;
    lookasideOut->physicalEltSize = physicalEltSize;
    lookasideOut->numElements = numElements;
    lookasideOut->options = lookasideOpts;
    // XXX FIXME make LookasideEnableAllocWait a compile time selection not
    // a runtime selection.  tricky without closures but possible with autogen
    // code
    if ((lookasideOpts & LookasideEnableAllocWait) != 0) {
        lookasideOut->sem.init(numElements);
    }

common_exit:
    if (status != StatusOk) {
        if (listInited) {
            assert(lookasideBuffer != MAP_FAILED);
            destroyFreeList(lookasideOut,
                            physicalEltSize,
                            numElements,
                            lookasideBuffer);
            listInited = false;
        }

        if (lookasideBuffer != MAP_FAILED) {
            ret = memUnmap(lookasideBuffer, physicalEltSize * numElements);
            assert(ret == 0);
            lookasideBuffer = MAP_FAILED;
        }
    }

    return status;
}

void
lookasideDestroy(LookasideList *lookasideList)
{
    int ret;

    destroyFreeList(lookasideList,
                    lookasideList->physicalEltSize,
                    lookasideList->numElements,
                    lookasideList->buffer);
    ret = memUnmap(lookasideList->buffer, lookasideList->bufSize);
    assert(ret == 0);
}

void *
lookasideAlloc(LookasideList *lookasideList)
{
    LookasideListElt *lookasideListElt;

    if ((lookasideList->options & LookasideEnableAllocWait) != 0) {
        lookasideList->sem.semWait();
    }

    lookasideList->freeListLock.lock();

    lookasideListElt = lookasideList->freeListAnchor.prev;

    if (lookasideListElt == &lookasideList->freeListAnchor) {
        assert((lookasideList->options & LookasideEnableAllocWait) == 0);
        lookasideList->freeListLock.unlock();
        return NULL;
    }
    // remove tail from list
    lookasideListElt->prev->next = lookasideListElt->next;
    lookasideListElt->next->prev = lookasideListElt->prev;

    lookasideList->freeListLock.unlock();

    return lookasideListElt->content;
}

void
lookasideFree(LookasideList *lookasideList, void *buf)
{
    LookasideListElt *lookasideListElt;
    lookasideListElt = (LookasideListElt *) buf;

    lookasideList->freeListLock.lock();

    // append to list
    lookasideListElt->prev = lookasideList->freeListAnchor.prev;
    lookasideListElt->next = lookasideList->freeListAnchor.prev->next;
    lookasideList->freeListAnchor.prev->next = lookasideListElt;
    lookasideList->freeListAnchor.prev = lookasideListElt;

    lookasideList->freeListLock.unlock();

    if ((lookasideList->options & LookasideEnableAllocWait) != 0) {
        lookasideList->sem.post();
    }
}
