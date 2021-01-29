// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Library of hash functions for hashing arbitrary types by key

#include <time.h>
#include <assert.h>
#include <stdio.h>

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "operators/Dht.h"
#include "config/Config.h"
#include "hash/Hash.h"

// XXX FIXME should rename and move to libhash

void
operatorsHashByStringInverse(char *keyBuf, uint64_t *hash)
{
    int ii;
    unsigned jj = 0;
    char *string = (char *) hash;

    for (ii = sizeof(*hash) - 1; ii >= 0; ii--) {
        keyBuf[jj] = string[ii];
        if (keyBuf[jj] == '\0') {
            return;
        }
        jj++;
    }

    keyBuf[jj - 1] = '\0';
}

uint64_t
operatorsHashByString(const char *stringIn)
{
    uint64_t ret;
    char *retStr = (char *) &ret;
    unsigned ii;

    ret = 0;
    // assumes little-endian
    for (ii = 0; ii < sizeof(ret) && stringIn[ii] != '\0'; ii++) {
        retStr[sizeof(ret) - (ii + 1)] = stringIn[ii];
    }

    return ret;
}

uint64_t
operatorsHashByHour(HashKey tsIn)
{
    struct timespec *ts = (struct timespec *) tsIn;

    return ts->tv_sec / SecsPerHour;
}

uint64_t
operatorsHashByBoolPtr(HashKey boolIn)
{
    bool *key = (bool *) boolIn;

    return *key;
}

uint64_t
operatorsHashByInt32Ptr(HashKey intIn)
{
    uint32_t *key = (uint32_t *) intIn;

    return *key;
}

uint64_t
operatorsHashByInt64(HashKey intIn)
{
    uint64_t key = (uint64_t) intIn;

    return key;
}

uint64_t
operatorsHashByInt64Ptr(HashKey intIn)
{
    uint64_t *key = (uint64_t *) intIn;

    return *key;
}

uint64_t
operatorsHashByFloat32Ptr(HashKey floatIn)
{
    float32_t *key = (float32_t *) floatIn;

    return (uint64_t) *key;
}

uint64_t
operatorsHashByFloat64Ptr(HashKey floatIn)
{
    float64_t *key = (float64_t *) floatIn;

    return (uint64_t) *key;
}
