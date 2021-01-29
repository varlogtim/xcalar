// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "primitives/Primitives.h"

template <typename elem, bool (*cmp)(const elem *, const elem *)>
class Heap
{
  public:
    Heap() = default;
    ~Heap()
    {
        if (heap_) {
            delete[] heap_;
            heap_ = NULL;
        }
    }

    Status init(int maxSize)
    {
        assert(maxSize > 0);
        heap_ = new (std::nothrow) elem[maxSize];
        if (heap_ == NULL) {
            return StatusNoMem;
        }
        size_ = 0;
        maxSize_ = maxSize;
        return StatusOk;
    }

    // Returns true on success and false when the heap is full
    MustCheck Status insert(elem newElem)
    {
        if (size() == maxSize_) {
            return StatusInval;
        }
        assert(size() < maxSize_);
        int index = size_;
        heap_[index] = newElem;
        ++size_;

        while (index != 0) {
            // parent at (index - 1) / 2
            int parentIndex = (index - 1) / 2;
            assert(index >= 0 && index < maxSize_);
            assert(parentIndex >= 0 && parentIndex < maxSize_);
            elem *parent = &heap_[parentIndex];
            elem *curElem = &heap_[index];

            if (cmp(parent, curElem)) {
                break;
            }

            // swap element with parent
            elem tmp = *parent;
            *parent = *curElem;
            *curElem = tmp;

            index = parentIndex;
        }
        return StatusOk;
    }

    // Returns true on success and false when the heap is empty
    MustCheck Status remove(elem *result)
    {
        if (size() == 0) {
            return StatusInval;
        }
        *result = heap_[0];
        --size_;

        if (size() == 0) {
            return StatusOk;
        }

        heap_[0] = heap_[size_];

        int index = 0;
        int swapIndex = 0;

        do {
            int leftIndex = 2 * index + 1;
            int rightIndex = leftIndex + 1;

            if (leftIndex >= size()) {
                // no children, we are at the bottom of the heap
                break;
            }

            if (rightIndex >= size()) {
                swapIndex = leftIndex;
            } else {
                if (cmp(&heap_[leftIndex], &heap_[rightIndex])) {
                    swapIndex = leftIndex;
                } else {
                    swapIndex = rightIndex;
                }
            }

            if (cmp(&heap_[swapIndex], &heap_[index])) {
                elem tmp = heap_[index];
                heap_[index] = heap_[swapIndex];
                heap_[swapIndex] = tmp;

                index = swapIndex;
                continue;
            }

            // we are smaller than both children. exit loop
            break;
        } while (index < size() - 1);

        return StatusOk;
    }

    int size() const { return size_; }

  private:
    elem *heap_ = NULL;
    int size_ = -1;
    int maxSize_ = -1;
};
