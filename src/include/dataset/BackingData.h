// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef BACKINGDATA_H
#define BACKINGDATA_H

#include "primitives/Primitives.h"
#include "bc/BufferCache.h"

// This allows us to manage the physical data of a dataset, whereas
// DataSegments manage the logical data of a dataset.
class BackingData
{
  public:
    BackingData() {}
    virtual ~BackingData(){};

    virtual void *deref(size_t *sizeOut) = 0;

    virtual bool setDataSize(size_t dataSize) = 0;
    virtual size_t getDataSize() const = 0;

    virtual size_t getBufferCapacity() const = 0;

    virtual const char *getLabel() const = 0;

  private:
    // Disallow.
    BackingData(const BackingData &) = delete;
    BackingData &operator=(const BackingData &) = delete;
};

class BackingDataMem final : public BackingData
{
  public:
    BackingDataMem(char *label, void *data, size_t size, size_t capacity);
    ~BackingDataMem();

    void *deref(size_t *sizeOut) override;

    bool setDataSize(size_t dataSize) override
    {
        if (dataSize > capacity_) {
            return false;
        }

        size_ = dataSize;
        return true;
    }

    size_t getDataSize() const override { return size_; }

    size_t getBufferCapacity() const override { return capacity_; }

    const char *getLabel() const override { return label_; }

    Status resize(size_t newSize);

  private:
    char *label_;
    void *data_;
    size_t size_;
    size_t capacity_;
};

#endif  // BACKINGDATA_H
