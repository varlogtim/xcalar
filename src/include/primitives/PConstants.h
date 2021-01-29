// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PRIM_CONSTANTS_H_
#define _PRIM_CONSTANTS_H_

#include <limits.h>

//
// XXX TODO
// We need a Singleton for Contants which can be initialized during bootstrap
// time.
//

// XXX FIXME should merge with ../Constants.h once that file is cleaned up

enum {
    MSecsPerSec = 1000UL,
    USecsPerMSec = 1000UL,
    NSecsPerUSec = 1000UL,
    USecsPerSec = MSecsPerSec * USecsPerMSec,
    NSecsPerSec = USecsPerSec * NSecsPerUSec,
    NSecsPerMSec = USecsPerMSec * NSecsPerUSec,
    SecsPerMinute = 60UL,
    MinutesPerHour = 60UL,
    SecsPerHour = SecsPerMinute * MinutesPerHour,
    HoursPerDay = 24,
    DaysPerWeek = 7,
    SecsPerDay = SecsPerHour * HoursPerDay,
    HoursPerWeek = DaysPerWeek * HoursPerDay,
    SecsPerWeek = SecsPerDay * DaysPerWeek,
    USecsPerHour = (unsigned long) SecsPerHour * USecsPerSec,

    KB = 1UL << 10,
    MB = 1UL << 20,
    GB = 1UL << 30,
    TB = 1ULL << 40,
    PB = 1ULL << 50,
    EB = 1ULL << 60,

    BitsPerUInt8Shift = 3,
    BitsPerUInt8 = 8UL,
    BitsPerUInt16 = 16UL,
    BitsPerUInt32 = 32UL,
    BitsPerUInt64 = 64UL,
    BitsPerFloat32 = 32UL,
    BitsPerFloat64 = 64UL,
    BitsPerInt = BitsPerUInt8 * sizeof(int),
    BitsPerLong = BitsPerUInt8 * sizeof(long),
    BlockSize = 4UL * KB,
    PageSize = 4UL * KB,
    SectorSize = 512UL,
    InvalidAddr = 0xFEEDFEEDFEEDFEED,
    WordSizeInBits = __WORDSIZE,
    WordSizeInBytes = WordSizeInBits / BitsPerUInt8,
};

// XXX TODO Use sysconf (_SC_LEVEL1_DCACHE_LINESIZE)
// must be #define to be used in Aligned()
#define CpuCacheLineSize (64)

typedef enum Base {
    BaseCanonicalForm = 0UL,
    Base2 = 2UL,
    Base8 = 8UL,
    Base10 = 10UL,
    Base16 = 16UL,
} Base;

#endif  // _PRIM_CONSTANTS_H_
