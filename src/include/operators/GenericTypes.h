// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _GENERICTYPES_H_
#define _GENERICTYPES_H_

#include <string.h>

#include "primitives/Macros.h"
#include "msg/TwoPcFuncDefs.h"
#include "libapis/LibApisEnums.h"
#include "LibApisConstants.h"

typedef uint64_t Xid;
static constexpr const Xid XidInvalid = 0;

typedef uint64_t VersionId;
static constexpr const uint64_t InvalidVersion = (uint64_t)(-1);

typedef uint32_t NodeId;
static constexpr const NodeId NodeIdInvalid = 0xffffffff;

static constexpr uint64_t MaxHostName = 128;
// strlen of 2^64 - 1 (decminal)
static constexpr unsigned UInt64MaxStrLen = 20;
// Max nodes.
// Do not exceed 1024 nodes as we will not have enough bits to generate
// globally unique xid which uses the high order 10 bits of an uint64_t for
// the nodeId.
static constexpr unsigned MaxNodesBitShift = 10;
static constexpr int MaxNodes = 1 << MaxNodesBitShift;

static constexpr unsigned XcalarApiMaxPathLen =
    XcalarApiMaxFileNameLen + XcalarApiMaxUrlLen;
class Scalar;

typedef uintptr_t OpTxnCookie;
static constexpr const OpTxnCookie OpTxnCookieInvalid = 0xc001c0041e;

enum SortedFlags : uint8_t {
    UnsortedFlag = 0x0,
    SortedFlag = 0x1,
};

enum OrderingDirectionFlags : uint8_t {
    AscendingFlag = 0x2,
    DescendingFlag = 0x4,
};

enum OrderingCompletenessFlags : uint8_t {
    PartiallySortedFlag = 0x8,
};

enum OrderingRandomnessFlags : uint8_t {
    RandomFlag = 0x10,
};

// Cursors and hash slots only care about sortedness
// Dhts only care about sortedness and direction
// Xdbs care about sortedness, direction, and completeness.

// An Ordering value of Ordered can imply Ascending, Descending,
// or PartialAscending
enum Ordering : uint32_t {
    Unordered = UnsortedFlag,
    OrderingInvalid = 0xbadc0de,
    Ordered = SortedFlag,
    PartiallyOrdered = PartiallySortedFlag,
    Ascending = Ordered | AscendingFlag,
    Descending = Ordered | DescendingFlag,
    PartialAscending = Ordered | PartiallyOrdered | Ascending,
    PartialDescending = Ordered | PartiallyOrdered | Descending,
    Random = RandomFlag,
};

MustInline Ordering
strToOrdering(const char *str)
{
    if (strcmp(str, "Unordered") == 0) {
        return Unordered;
    } else if (strcmp(str, "PartialAscending") == 0) {
        return PartialAscending;
    } else if (strcmp(str, "PartialDescending") == 0) {
        return PartialDescending;
    } else if (strcmp(str, "Ascending") == 0) {
        return Ascending;
    } else if (strcmp(str, "Descending") == 0) {
        return Descending;
    } else if (strcmp(str, "Random") == 0) {
        return Random;
    } else {
        return OrderingInvalid;
    }
}

MustInline const char *
strGetFromOrdering(Ordering ordering)
{
    switch (ordering) {
    case Unordered:
        return "Unordered";
    case PartialAscending:
        return "PartialAscending";
    case PartialDescending:
        return "PartialDescending";
    case Ascending:
        return "Ascending";
    case Descending:
        return "Descending";
    case Random:
        return "Random";
    case OrderingInvalid:
        return "Invalid";
    default:
        return "";
    }
}

#endif  // _GENERICTYPES_H_
