// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "dataset/BackingData.h"
#include "util/MemTrack.h"
#include "xdb/Xdb.h"

//
// BackingDataShm
// A dataset backed by malloced memory
//

BackingDataMem::BackingDataMem(char *label,
                               void *data,
                               size_t size,
                               size_t capacity)
    : label_(label), data_(data), size_(size), capacity_(capacity)
{
    assert(data);
}

BackingDataMem::~BackingDataMem()
{
    if (data_) {
        memFree(data_);
        data_ = NULL;
    }
    if (label_) {
        memFree(label_);
        label_ = NULL;
    }
}

void *
BackingDataMem::deref(size_t *sizeOut)
{
    if (sizeOut) {
        *sizeOut = size_;
    }
    return data_;
}

Status
BackingDataMem::resize(size_t newSize)
{
    void *ret = memRealloc(data_, newSize);
    if (ret == NULL) {
        return StatusNoMem;
    }
    data_ = ret;
    return StatusOk;
}
