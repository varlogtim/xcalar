// Copyright 2017 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

//
// ----- Overview -----
// This a simple dynamically sized array. The caller is responsible for calling
// the grow function.
// NOTE: This does a realloc in growBuffer. Any pointers obtained before the
// growBuffer must be discarded

#ifndef _ELEMBUF_H_
#define _ELEMBUF_H_

#include "primitives/Primitives.h"
#include "util/MemTrack.h"

template <class T>
struct ElemBuf {
    ElemBuf() = default;
    ElemBuf(const ElemBuf &) = delete;
    ElemBuf &operator=(const ElemBuf &) = delete;

    ElemBuf(ElemBuf &&other)
    {
        bufCapacity = other.bufCapacity;
        bufSize = other.bufSize;
        buf = other.buf;

        other.buf = NULL;
    }

    ElemBuf &operator=(ElemBuf &&other)
    {
        bufCapacity = other.bufCapacity;
        bufSize = other.bufSize;
        buf = other.buf;

        other.buf = NULL;
        return *this;
    }

    ~ElemBuf();
    MustCheck Status init(int64_t initialSize);
    // May grow by more than minGrowth
    MustCheck Status growBuffer(int64_t minGrowth);
    int64_t bufCapacity = 0;
    int64_t bufSize = 0;
    T *buf = NULL;
};

template <class T>
Status
ElemBuf<T>::init(int64_t initialSize)
{
    return growBuffer(initialSize);
}

template <class T>
ElemBuf<T>::~ElemBuf()
{
    if (this->buf) {
        for (int64_t ii = 0; ii < this->bufCapacity; ii++) {
            this->buf[ii].~T();
        }
        memFree(this->buf);
        this->buf = NULL;
    }
}

// Grow enough to support minGrowth new elements. May not grow at all.
template <class T>
Status
ElemBuf<T>::growBuffer(int64_t minGrowth)
{
    Status status = StatusOk;
    int64_t minNewCap = this->bufSize + minGrowth;
    int64_t newCapacity = this->bufCapacity * 2;

    if (minNewCap < this->bufCapacity) {
        return StatusOk;
    }

    if (newCapacity < minNewCap) {
        newCapacity = minNewCap;
    }
    assert(newCapacity > this->bufCapacity);
    assert(this->bufCapacity >= this->bufSize);
    T *newBuf;

    if (std::is_scalar<T>::value) {
        newBuf =
            static_cast<T *>(memRealloc(this->buf, newCapacity * sizeof(T)));
        BailIfNull(newBuf);
    } else {
        newBuf = static_cast<T *>(memAlloc(newCapacity * sizeof(T)));
        BailIfNull(newBuf);
        for (int64_t ii = 0; ii < this->bufCapacity; ii++) {
            new (&newBuf[ii]) T(std::move(this->buf[ii]));
            this->buf[ii].~T();
        }
        memFree(this->buf);
    }

    // Construct all of our new elements
    for (int64_t ii = this->bufCapacity; ii < newCapacity; ii++) {
        new (&newBuf[ii]) T();
    }

    this->buf = newBuf;
    this->bufCapacity = newCapacity;

CommonExit:
    return status;
}

#endif  // _ELEMBUF_H_
