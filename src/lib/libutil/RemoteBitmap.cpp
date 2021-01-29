// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "util/RemoteBitmap.h"

// We're going to use some preprocessor magic to generate a lookup table for the
// number of bits set in a byte
// These macros are based on the fact that for a given 2 bit set, the number of
// bits set is 0, 1, 1, 2: 00 01 10 11
// This macro expands that out 4 times so that we cover all 8 bits worth
int numBitsLookup[] = {
#define B2(n) n, n + 1, n + 1, n + 2
#define B4(n) B2(n), B2(n + 1), B2(n + 1), B2(n + 2)
#define B6(n) B4(n), B4(n + 1), B4(n + 1), B4(n + 2)
    B6(0), B6(1), B6(1), B6(2)};
