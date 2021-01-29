// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "hash/Fnv.h"
#include "hash/Hash.h"

#include "HashInt.h"

uint64_t
hashIdentity(HashKey key)
{
    return (uint64_t) key;
}

// Warning! This uses FNV hash which is fast, but not cryptographically secure
// Fnv does a pretty good job at avoiding collision and has a little bit of
// avalanche effect, which is good for diffusion.
uint64_t
hashStringFast(const char *buf)
{
    return fnv_64a_str(buf, FNV1A_64_INIT);
}

uint64_t
hashBufFast(const uint8_t *buf, int64_t len)
{
    return fnv_64a_buf(buf, len, FNV1A_64_INIT);
}

Status
hashInit()
{
    return hashCrc32cInit();
}

void
hashCombine(uint64_t &seed, const uint64_t hash)
{
    // 2^64 / [golden ratio].
    seed ^= hash + 0x9e3779b97f4a7c15 + (seed << 12) + (seed >> 4);
}