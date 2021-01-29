// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _STATISTICSTYPES_H_
#define _STATISTICSTYPES_H_

#include "util/AtomicTypes.h"

typedef uint64_t StatGroupId;

// Ensure 64 bit alignment
typedef union Stat {
    uint64_t statUint64;
    Atomic64 statUint64Atomic;
    double statDouble;
    struct {
        Atomic32 statUint32;
        uint32_t statMaxRefValUint32;
    };
} Stat Aligned(8);

// type of the stat union
typedef enum { StatDouble = 1, StatUint32 = 2, StatUint64 = 3 } StatValType;
typedef enum {
    StatDontFreeOnDestroy = false,
    StatFreeOnDestroy = true
} StatFreeOption;
typedef Stat *StatHandle;

// Stats can have an accompanying reference stat to allow calculation
// of percentages. Only absolute stats can have an accompanying reference stat.
typedef enum {
    StatCumulative = 10,
    StatAbsoluteWithRefVal = 11,
    StatAbsoluteWithNoRefVal = 12,
    // NOTE: HWM stats are always going to be absolute stats without ref value.
    StatHWM = 13
} StatType;

typedef enum {
    StatRefValueNotApplicable = 0xdeadbeef,
    StatStringLen = 64,
} StatMisc;

typedef char StatGroupName[StatStringLen];
#endif  // _STATISTICSTYPES_H_
