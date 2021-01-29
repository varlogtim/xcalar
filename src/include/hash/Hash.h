// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _HASH_H
#define _HASH_H
#include "primitives/Primitives.h"

enum {
    HashCrc32Alignment = 8,
};

bool hashIsInit();
uint32_t hashCrc32c(uint32_t crc, const void *buf, size_t len);

typedef uintptr_t HashKey;

uint64_t hashIdentity(HashKey key);
uint64_t hashStringFast(const char *buf);
uint64_t hashBufFast(const uint8_t *buf, int64_t len);
void hashCombine(uint64_t &seed, const uint64_t hash);
Status hashInit();
void hashDestroy();

template <typename T>
uint64_t
hashCastUint64(T key)
{
    return (uint64_t) key;
}

#endif  // _HASH_H
