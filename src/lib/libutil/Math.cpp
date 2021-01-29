// Copyright 2013-2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <math.h>
#include <assert.h>

#include "primitives/Primitives.h"
#include "util/Math.h"
#include "LibUtilConstants.h"

using namespace util;

uint64_t
mathGetPrime(uint64_t val)
{
    unsigned ii;

    assert(val >= 2);

    for (ii = 1; ii < ArrayLen(primes); ii++) {
        if (primes[ii] > val) {
            break;
        }
    }

    assert(primes[ii - 1] <= val);
    assert(ii <= ArrayLen(primes));  // if we hit this, grow the primes array

    return primes[ii - 1];
}

float64_t
mathLog(float64_t base, float64_t val)
{
    assertStatic(sizeof(val) == sizeof(double));

    return log2(val) / log2(base);
}

int64_t
mathRound(float64_t val)
{
    assertStatic(sizeof(val) == sizeof(double));

    return (int64_t) roundl(val);
}

uint64_t
mathNearestPowerOf2(uint64_t val)
{
    unsigned ii;

    for (ii = 0; ii < (BitsPerUInt64 - 1); ii++) {
        if (val <= 1ULL << ii) {
            return 1ULL << ii;
        }
    }

    assert(val <= 1ULL << (BitsPerUInt64 - 1));

    return 1ULL << (BitsPerUInt64 - 1);
}
