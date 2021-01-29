// Copyright 2018 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#ifndef _VECTOR_H_
#define _VECTOR_H_

#include "primitives/Primitives.h"
#include "util/ElemBuf.h"

template <class T>
class Vector
{
  public:
    Vector() = default;
    ~Vector()
    {
        for (int32_t ii = 0; ii < elemBuf_.bufSize; ii++) {
            elemBuf_.buf[ii].~T();
        }
    }

    // Same as call append(T()) addSize times
    Status addZeros(int32_t addSize)
    {
        Status status = StatusOk;
        status = elemBuf_.growBuffer(addSize);
        if (status != StatusOk) {
            return status;
        }

        assert(size() < capacity());

        // growBuffer already zeroed rest of the buffer
        elemBuf_.bufSize += addSize;

        return StatusOk;
    }

    void clear()
    {
        for (int32_t ii = 0; ii < elemBuf_.bufSize; ii++) {
            elemBuf_.buf[ii].~T();
        }
        elemBuf_.bufSize = 0;
    }

    int32_t size() const { return elemBuf_.bufSize; }

    int32_t capacity() const { return elemBuf_.bufCapacity; }

    Status append(T elem)
    {
        Status status = StatusOk;
        status = elemBuf_.growBuffer(1);
        if (status != StatusOk) {
            return status;
        }
        assert(size() < capacity());
        elemBuf_.buf[elemBuf_.bufSize] = std::move(elem);
        ++elemBuf_.bufSize;
        return StatusOk;
    }

    T get(int32_t index)
    {
        assert(index < size());
        return elemBuf_.buf[index];
    }

    T& operator[](int32_t index)
    {
        assert(index < size());
        return elemBuf_.buf[index];
    }

    const T& operator[](int32_t index) const
    {
        assert(index < size());
        return elemBuf_.buf[index];
    }

  private:
    ElemBuf<T> elemBuf_;
};

#endif  // _VECTOR_H_
