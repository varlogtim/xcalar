// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIBUTIL_CONSTANTS_H_
#define _LIBUTIL_CONSTANTS_H_

#include <time.h>

#include "primitives/Primitives.h"

namespace util
{
//
// Archive related
//

enum {
    NumArchiveFilesInBc = 32,
    NumArchiveManifestInBc = 32,
};

//
// Math related
//

// primes nearest to but less than 2^n where n{1..26
static constexpr uint64_t primes[] = {
    2,       3,       7,        13,       31,       61,      127,
    251,     509,     1021,     2039,     4093,     8191,    16381,
    32749,   65521,   131071,   262139,   524287,   1048573, 2097143,
    4194301, 8388593, 16777213, 33554393, 67108859,
};

//
// MemTrack related
//
enum {
    NumTagHashSlots = 257,
    NumAlignPtrHashSlots = 2049,
};

//
// License related
//

// Change into a real number on a release branch
static constexpr time_t TimebombExpiry = 9999999999;
static constexpr time_t timebombAlarm = TimebombExpiry - SecsPerWeek * 2;

//
// Cmd Parser related
//
enum State {
    DblQuoteOpened,
    SingleQuoteOpened,
    DefaultState,
    Whitespace,
    Comment,
    EscapeChar
};

//
// File Utill related
//
enum {
    // Buffer size for the FileUtils::copyFile call in between reads and writes
    CopyFileBufLen = 4096
};

}  // namespace util

#endif  // _LIBUTIL_CONSTANTS_H_
