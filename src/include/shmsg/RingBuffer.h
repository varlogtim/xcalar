// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RINGBUFFER_H_
#define _RINGBUFFER_H_

#include <semaphore.h>
#include "primitives/Primitives.h"

//
// This data structure represents a generic RingBuffer for message passing
// between different threads or processes. Messages are intended to be small
// and fixed size. They are copied into/out of the ring buffer on enqueue/
// dequeue.
//
// RingBuffer does no synchronization. The following properties must be
// guaranteed by the caller:
//  - At most one thread can be executing in ringBufEnqueue at the same time.
//  - At most one thread can be executing in ringBufDequeue at the same time.
//  - Because enqueue only writes head and dequeue only writes tail and because
//    the count of enqueued elements is maintained via semaphores, it is safe
//    to call ringBufEnqueue and ringBufDequeue concurrently.
//

// XXX usrnode shouldn't crash if pieces of this data structure are overwritten.
//     Keep constant data members in local memory.

struct RingBuffer {
    char *label;  // Used for debugging.

    size_t elementSize;   // Size of individual element.
    size_t elementCount;  // Number of elements.

    // XXX In shared case, use futex to allow semaphore corruption without
    //     usrnode segfaulting.
    sem_t semFullSlots;  // Number of elements currently enqueued.
    sem_t semFreeSlots;  // Number of elements that can be enqueued until full.

    size_t head;  // Next element to dequeue.
    size_t tail;  // Next insertion slot.

    size_t bufferSize;  // Bytes in buffer. Equals elementSize * elementCount.
    uint8_t buffer[0];  // Memory used to back queue.
};

Status ringBufCreatePreAlloc(RingBuffer *ringBuf,
                             const char *label,
                             size_t ringBufSize,
                             size_t elementSize,
                             size_t elementCount);

Status ringBufCreate(RingBuffer **ringBuf,
                     const char *label,
                     size_t elementSize,
                     size_t elementCount);

void ringBufDeletePreAlloc(RingBuffer *ringBuf);

void ringBufDelete(RingBuffer **ringBuf);

Status ringBufEnqueue(RingBuffer *ringBuf,
                      const void *element,
                      time_t timeoutSecs);

Status ringBufDequeue(RingBuffer *ringBuf,
                      void *element,
                      size_t elementSize,
                      time_t timeoutSecs);

void ringBufPrint(RingBuffer *ringBuf, void (*printFn)(void *));

// RingBuffer is a dynamically sized data structure. Returns the size, in bytes,
// needed for a RingBuffer with the given parameters.
static inline size_t
ringBufGetSize(size_t elementSize, size_t elementCount)
{
    return sizeof(RingBuffer) + elementSize * elementCount;
}

#endif  // _RINGBUFFER_H_
