// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdint.h>
#include <stdio.h>
#include "shmsg/RingBuffer.h"
#include "util/MemTrack.h"
#include "strings/String.h"

static constexpr const char *moduleName = "libshmmsg::RingBuffer";

// Initializes RingBuffer given a pre-allocated chunk of memory. Chunk must be
// of correct size given by ringBufGetSize.
Status
ringBufCreatePreAlloc(RingBuffer *ringBuf,
                      const char *label,
                      size_t ringBufSize,
                      size_t elementSize,
                      size_t elementCount)
{
    Status status;
    bool semFullSlotsInit = false;
    bool semFreeSlotsInit = false;
    char *labelTmp = NULL;

    assert(elementSize != 0);
    assert(elementCount != 0);
    assert(ringBufGetSize(elementSize, elementCount) == ringBufSize);
    if (ringBufGetSize(elementSize, elementCount) != ringBufSize) {
        assert(false);
        status = StatusInval;
        goto CommonExit;
    }

    labelTmp = strAllocAndCopy(label);
    BailIfNull(labelTmp);

    if (sem_init(&ringBuf->semFullSlots, true, 0) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    semFullSlotsInit = true;

    if (sem_init(&ringBuf->semFreeSlots, true, (unsigned int) elementCount) !=
        0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    semFreeSlotsInit = true;

    ringBuf->elementSize = elementSize;
    ringBuf->elementCount = elementCount;
    ringBuf->head = 0;
    ringBuf->tail = 0;
    ringBuf->bufferSize = elementSize * elementCount;
    ringBuf->label = labelTmp;

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (semFreeSlotsInit) {
            sem_destroy(&ringBuf->semFreeSlots);
        }
        if (semFullSlotsInit) {
            sem_destroy(&ringBuf->semFullSlots);
        }
        if (labelTmp != NULL) {
            memFree(labelTmp);
        }
    }
    return status;
}

// Allocates and initializes new RingBuffer.
Status
ringBufCreate(RingBuffer **ringBuf,
              const char *label,
              size_t elementSize,
              size_t elementCount)
{
    Status status;
    RingBuffer *ringBufTmp = NULL;

    if (ringBuf == NULL || elementSize == 0 || elementCount == 0) {
        assert(false);
        return StatusInval;
    }

    *ringBuf = NULL;

    size_t ringBufSize = ringBufGetSize(elementSize, elementCount);
    if (mathIsPowerOfTwo(ringBufSize) ||
        (ringBufSize > (size_t) PageSize &&
         ringBufSize % (size_t) PageSize == 0)) {
        ringBufTmp = (RingBuffer *) memAllocAlignedExt(xcMin(ringBufSize,
                                                             (size_t) PageSize),
                                                       ringBufSize,
                                                       moduleName);
    } else {
        ringBufTmp = (RingBuffer *) memAllocExt(ringBufSize, moduleName);
    }
    BailIfNull(ringBufTmp);

    status = ringBufCreatePreAlloc(ringBufTmp,
                                   label,
                                   ringBufSize,
                                   elementSize,
                                   elementCount);
    BailIfFailed(status);

    *ringBuf = ringBufTmp;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (ringBufTmp != NULL) {
            ringBufDelete(&ringBufTmp);
        }
    }
    return status;
}

void
ringBufDeletePreAlloc(RingBuffer *ringBuf)
{
    memFree(ringBuf->label);
    sem_destroy(&ringBuf->semFullSlots);
    sem_destroy(&ringBuf->semFreeSlots);
}

void
ringBufDelete(RingBuffer **ringBuf)
{
    size_t ringBufSize =
        ringBufGetSize((*ringBuf)->elementSize, (*ringBuf)->elementCount);

    ringBufDeletePreAlloc(*ringBuf);

    if (mathIsPowerOfTwo(ringBufSize) ||
        (ringBufSize > PageSize && ringBufSize % PageSize == 0)) {
        memAlignedFree(*ringBuf);
    } else {
        memFree(*ringBuf);
    }

    *ringBuf = NULL;
}

// Enqueues an element into the ring buffer. See caller synchronization
// requirements in RingBuffer.h.
Status
ringBufEnqueue(RingBuffer *ringBuf, const void *element, time_t timeoutSecs)
{
    Status status;

    do {
        if (timeoutSecs > 0) {
            status = sysSemTimedWait(&ringBuf->semFreeSlots,
                                     timeoutSecs * USecsPerSec);
        } else {
            if (sem_wait(&ringBuf->semFreeSlots) != 0) {
                status = sysErrnoToStatus(errno);
            } else {
                status = StatusOk;
            }
        }
    } while (status == StatusIntr);
    assert(status == StatusOk || status == StatusTimedOut);
    BailIfFailed(status);

    // Copy into queue and signal consumer.
    memBarrier();
    memcpy(ringBuf->buffer + ringBuf->tail, element, ringBuf->elementSize);
    ringBuf->tail =
        (ringBuf->tail + ringBuf->elementSize) % ringBuf->bufferSize;

    verify(sem_post(&ringBuf->semFullSlots) == 0);

CommonExit:
    return status;
}

// Dequeues an element from head of ring buffer. Waits for work if empty.
// See caller synchronization requirements in RingBuffer.h.
Status
ringBufDequeue(RingBuffer *ringBuf,
               void *element,
               size_t elementSize,
               time_t timeoutSecs)
{
    Status status;

    assert(elementSize == ringBuf->elementSize);

    do {
        if (timeoutSecs > 0) {
            status = sysSemTimedWait(&ringBuf->semFullSlots,
                                     timeoutSecs * USecsPerSec);
        } else {
            if (sem_wait(&ringBuf->semFullSlots) != 0) {
                status = sysErrnoToStatus(errno);
            } else {
                status = StatusOk;
            }
        }
    } while (status == StatusIntr);
    assert(status == StatusOk || status == StatusTimedOut);
    BailIfFailed(status);

    // Grab element and free up its slot.
    memBarrier();
    memcpy(element, ringBuf->buffer + ringBuf->head, elementSize);
    ringBuf->head =
        (ringBuf->head + ringBuf->elementSize) % ringBuf->bufferSize;

    verify(sem_post(&ringBuf->semFreeSlots) == 0);

CommonExit:
    return status;
}

// Returns number of elements currently enqueued in RingBuffer. Must not be
// called at the same time as enqueue/dequeue.
static unsigned
ringBufGetLen(RingBuffer *ringBuf)
{
    int numFullSlots;
    int numFreeSlots;

    verify(sem_getvalue(&ringBuf->semFullSlots, &numFullSlots) == 0);
    verify(sem_getvalue(&ringBuf->semFreeSlots, &numFreeSlots) == 0);

    assert((int) ringBuf->elementCount - numFullSlots == numFreeSlots);
    (void) numFreeSlots;

    return numFullSlots;
}

// Prints RingBuffer for debugging purposes. printFn will be called with a
// pointer to each individual element. Must not be called at the same time
// as an enqueue/dequeue.
void
ringBufPrint(RingBuffer *ringBuf, void (*printFn)(void *))
{
    printf("RingBuffer %s:\n", ringBuf->label);

    bool sawTail = false;
    unsigned len = UINT32_MAX;

    for (size_t i = ringBuf->head; true;
         i = (i + ringBuf->elementSize) % ringBuf->bufferSize) {
        // Verify length of ring buffer isn't changing during print.
        unsigned lenTmp = ringBufGetLen(ringBuf);
        assert(len == UINT32_MAX || len == lenTmp);
        len = lenTmp;

        size_t index = i / ringBuf->elementSize;

        if (i == ringBuf->head && i == ringBuf->tail) {
            if (sawTail) {
                break;
            }

            // Indicates empty or full buffer.
            if (len == 0) {
                printf("head/tail -> %8lu -> (empty)\n", index);
                break;
            }

            // Break next time we see tail.
            sawTail = true;
        }

        if (i == ringBuf->head && i == ringBuf->tail) {
            printf("head/tail -> %6lu -> ", index);
        } else if (i == ringBuf->head) {
            printf("     head -> %6lu -> ", index);
        } else if (i == ringBuf->tail) {
            printf("     tail -> %6lu -> ", index);
        } else {
            printf("             %6lu -> ", index);
        }

        printFn(ringBuf->buffer + i);
    }
}
