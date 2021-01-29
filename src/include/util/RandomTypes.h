// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RANDOMTYPES_H_
#define _RANDOMTYPES_H_

#include <cstdlib>

#include "primitives/Primitives.h"

// Strong random numbers
typedef struct {
    struct random_data priv;
    // see RANDOM(3) NOTES
    char stateArray[256];
} Aligned(sizeof(uintptr_t)) RandHandle;

// Weak random numbers (uses xorshift128 algorithm)
// See https://en.wikipedia.org/wiki/Xorshift
struct RandWeakHandle {
    uint32_t x;
    uint32_t y;
    uint32_t z;
    uint32_t w;
};

#endif  // _RANDOMTYPES_H_
