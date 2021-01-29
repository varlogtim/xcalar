// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SGTYPES_H_
#define _SGTYPES_H_

#include "config.h"
#include "primitives/Primitives.h"

struct SgElem {
    void *baseAddr;
    size_t len;
};

//!
//! \brief Scatter/Gather array
//!
//! A scatter/gather array of memory locations.  A scatter/gather array is a
//! data structure used to describe multiple discontiguous regions of memory.
//! One example of where this is useful is when an app needs to read a
//! contiguous region from disk into 2 different memory locations.  While this
//! could be accomplished with 2 I/Os instead of 1, since an I/O is very
//! expensive we can use scatter/gather based I/O instead.  See UNIX READV(2)
//! manpage for further study.
//!
struct SgArray {
    unsigned numElems;
    SgElem elem[0];  // variable length
};

#endif  // _SGTYPES_H_
