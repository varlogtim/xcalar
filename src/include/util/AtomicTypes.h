// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _ATOMICTYPES_H_
#define _ATOMICTYPES_H_

#include "config.h"
#include "primitives/Primitives.h"

typedef struct {
    volatile int32_t val;
} Atomic32 Aligned(4);

typedef struct {
    volatile int64_t val;
} Atomic64 Aligned(8);

#endif  // _ATOMICTYPES_H_
