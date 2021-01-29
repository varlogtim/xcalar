// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XMATH_H_
#define _XMATH_H_

#include "primitives/Primitives.h"

//!
//! \brief Return a prime number <= val
//!
extern MustCheck uint64_t mathGetPrime(uint64_t val);

//!
//! \brief Compute logarithm
//!
//! Compute logarithm of val to the specified base
//!
//! \param[in] base specific base to compute logarithm to
//! \param[in] val value to compute logarithm of
//!
//! \returns logarithm expressed as 64-bit float
//!
extern MustCheck float64_t mathLog(float64_t base, float64_t val);

//!
//! \brief Round to the nearest integer
//!
//! Round the specified 64-bit floating point value to the nearest 64-bit
//! integer
//!
//! \param[in] val value to round
//!
//! \returns nearest 64-bit integer
//!
extern MustCheck int64_t mathRound(float64_t val);

extern uint64_t mathNearestPowerOf2(uint64_t val);

#define mathIsPowerOfTwo(num) ((num) != 0 && !((num) & ((num) -1)))

static inline bool
mathIsAligned(const void *addr, size_t alignmentInBytes)
{
    return ((uintptr_t) addr % alignmentInBytes) == 0;
}

#endif  // _XMATH_H_
