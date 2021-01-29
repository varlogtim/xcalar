// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>

#include "util/Random.h"
#include "util/System.h"

void
rndInitHandle(RandHandle *rndHandle, uint32_t seed)
{
    int ret;

    // initstate_r() manpage fails to inform callers libc expects the
    // stateArray and random_data structures zero'd
    memZero(rndHandle, sizeof(*rndHandle));

    ret = initstate_r(seed,
                      rndHandle->stateArray,
                      sizeof(rndHandle->stateArray),
                      &rndHandle->priv);

    assert(ret == 0);
}

// XXX temporarily disable fast randnum due to mask
// libapisSanity divide-by-zero crash
#if 0
uint64_t
rndGenerate64(RandHandle *rndHandle)
{
    uint8_t CF;
    uint64_t val;

    //
    // https://software.intel.com/en-us/articles/intel-digital-random-number-
    //         generator-drng-software-implementation-guide
    //
    // RDRAND instruction spec says:
    //
    // Loads a hardware generated random value and store it in the destination
    // register. The size of the random value is determined by the destination
    // register size and operating mode. The Carry Flag indicates whether a
    // random value is available at the time the instruction is executed. CF=1
    // indicates that the data in the destination is valid. Otherwise CF=0 and
    // the data in the destination operand will be returned as zeros for the
    // specified width. All other flags are forced to 0 in either situation.
    // Software must check the state of CF=1 for determining if a valid random
    // value has been returned, otherwise it is expected to loop and retry
    // execution of RDRAND.
    do {
        asm volatile("rdrand %0; setc %1":"=r"(val), "=qm"(CF));
    } while (CF == 0);

    return val;
}

uint32_t
rndGenerate32(RandHandle *rndHandle)
{
    return (uint32_t) rndGenerate64(rndHandle);
}
#else
// In DEBUG code we need to do something else since valgrind barfs with an
// illegal instruction error.  see:
// bugs.launchpad.net/ubuntu/+source/valgrind/+bug/852795
// XXX FIXME
// Also, if we need to support pre-Ivy Bridge
// cpus then we also need to do something else.  Consider:
// software.intel.com/en-us/articles/fast-random-number-generator-
// on-the-intel-pentiumr-4-processor
//
uint64_t
rndGenerate64(RandHandle *rndHandle)
{
    uint64_t val;

    val =
        ((uint64_t) rndGenerate32(rndHandle)) << 32 | rndGenerate32(rndHandle);

    return val;
}

uint32_t
rndGenerate32(RandHandle *rndHandle)
{
    int ret;
    uint32_t randNum;

    // XXX FIXME random_r() only returns 31-bits of random data not
    // 32-bits; see RAND_MAX
    ret = random_r(&rndHandle->priv, (int32_t *) &randNum);
    assert(ret == 0);

    return randNum;
}

#endif

// rndLimit is included
uint64_t
rndGenerate64Range(RandHandle *rndHandle, uint64_t rndLimit)
{
    return rndGenerate64(rndHandle) % (rndLimit + 1);
}

// Weak random generator using the xorshift128 algorithm
// See https://en.wikipedia.org/wiki/Xorshift
void
rndInitWeakHandle(RandWeakHandle *rndWeakHandle, uint32_t seed)
{
    RandHandle rndHandle;
    rndInitHandle(&rndHandle, seed);
    rndWeakHandle->x = rndGenerate32(&rndHandle);
    rndWeakHandle->y = rndGenerate32(&rndHandle);
    rndWeakHandle->z = rndGenerate32(&rndHandle);
    rndWeakHandle->w = rndGenerate32(&rndHandle);
}

void *
initRndWeakHandle(void *context)
{
    RandWeakHandle *randWeakHandle = new RandWeakHandle();
    if (randWeakHandle == NULL) {
        return NULL;
    }
    struct timespec ts;
    uint64_t seed;
    int ret;
    ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
    if (ret == 0) {
        seed = clkTimespecToNanos(&ts) * sysGetTid();
    } else {
        seed = sysGetTid();
    }
    rndInitWeakHandle(randWeakHandle, seed);
    return (void *) randWeakHandle;
}

void
destroyRndWeakHandle(void *rndWeakHandle)
{
    delete (RandWeakHandle *) rndWeakHandle;
}

uint32_t
rndWeakGenerate32(RandWeakHandle *rndWeakHandle)
{
    uint32_t x, y, z, w, t;

    x = rndWeakHandle->x;
    y = rndWeakHandle->y;
    z = rndWeakHandle->z;
    w = rndWeakHandle->w;
    t = x;
    t ^= t << 11;
    t ^= t >> 8;
    x = y;
    y = z;
    z = w;
    w ^= w >> 19;
    w ^= t;
    rndWeakHandle->x = x;
    rndWeakHandle->y = y;
    rndWeakHandle->z = z;
    rndWeakHandle->w = w;
    return w;
}

uint64_t
rndWeakGenerate64(RandWeakHandle *rndWeakHandle)
{
    uint64_t w1, w2;
    w1 = rndWeakGenerate32(rndWeakHandle);
    w2 = rndWeakGenerate32(rndWeakHandle);

    return w1 << 32 | w2;
}
