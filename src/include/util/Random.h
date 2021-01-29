// Copyright 2013 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RANDOM_H_
#define _RANDOM_H_

#include <stdio.h>
#include <cstdlib>
#include <assert.h>
#include "primitives/Primitives.h"
#include "util/RandomTypes.h"

//!
//! \brief Initialize a handle to a random number generator
//!
//! Obtain and initialize a handle to a random number generator.  The
//! generator should be seeded with the specified value.
//! \note Callers MUST instantiate 1 handle per thread.  The same handle
//!       MUST not be used by multiple threads to generate a random number.
//!
//! \param[out] rndHandle initialized handle to random number generator
//! \param[in] seed seed value for the random number generator
//!
void rndInitHandle(RandHandle *rndHandle, uint32_t seed) NonNull(1);

//!
//! \brief Generate a random number
//!
//! Generate a 32-bit random number from the given handle
//!
//! \param[out] rndHandle initialized handle to random number generator
//!
//! \pre Must have previously initialized rndHandle with rndInitHandle()
//!
MustCheck uint32_t rndGenerate32(RandHandle *rndHandle) NonNull(1);

//!
//! \brief Generate a random number
//!
//! Generate a 64-bit random number from the given handle
//!
//! \param[out] rndHandle initialized handle to random number generator
//!
//! \pre Must have previously initialized rndHandle with rndInitHandle()
//!
MustCheck uint64_t rndGenerate64(RandHandle *rndHandle) NonNull(1);

//!
//! \brief Generate a random number within a range
//!
//! Generate a 64-bit random number from the given handle within a range
//!
//! \param[out] rndHandle initialized handle to random number generator
//!
//! \param[in] rndLimit inclusive range for the random number generator
//
//! \pre Must have previously initialized rndHandle with rndInitHandle()
//!
MustCheck uint64_t rndGenerate64Range(RandHandle *rndHandle, uint64_t rndLimit)
    NonNull(1);
uint32_t rndWeakGenerate32(RandWeakHandle *rndWeakHandle);
uint64_t rndWeakGenerate64(RandWeakHandle *rndWeakHandle);

static inline uint64_t
rndWeakGenerate64Range(RandWeakHandle *rndWeakHandle, uint64_t rndLimit)
{
    return rndWeakGenerate64(rndWeakHandle) % (rndLimit + 1);
}

// Weak random generator using the xorshift128 algorithm
// See https://en.wikipedia.org/wiki/Xorshift
void rndInitWeakHandle(RandWeakHandle *rndWeakHandle, uint32_t seed);

// these are used to set context for builtIn function genRandom
void *initRndWeakHandle(void *context);

void destroyRndWeakHandle(void *rndWeakHandle);

#endif  // _RANDOM_H_
