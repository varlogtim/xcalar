// Copyright 2013 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XSTRING_H_
#define _XSTRING_H_

#include "primitives/Primitives.h"
#include "util/RandomTypes.h"
#include "util/MemTrack.h"
#include "StrlFunc.h"

#ifdef __cplusplus
extern "C" {
#endif
// Implemented in 3rd/boyerMoore/BoyerMoore.c
const char *strBoyerMoore(const char *string,
                          int stringLen,
                          const char *pat,
                          uint32_t patLen);
#ifdef __cplusplus
}
#endif  // __cplusplus

// returns true if 'a' is a hex digit
MustCheck bool strIsAsciiHexDigit(char a);

// Convert character [0-9]||[a-f] to int [0-15
uint8_t strAsciiToHexDigit(char a);

char *strTrim(char *input);
bool strIsWhiteSpace(char c);
bool strMatch(const char *pattern, const char *string);
Status strEscapeQuotes(char *out, size_t outLen, const char *in);
char *strTmpFile(char *, RandHandle *rndHandle);
const char *strBasename(const char *str);
Status strHumanSize(char *buf, size_t bufSize, size_t size);
MustCheck Status strSnprintf(char *dst, size_t size, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));
MustCheck char *allocAndSnprintf(const char *fmt, ...)
    __attribute__((format(printf, 1, 2)));
Status jsonStrCmp(const char *str1, const char *str2, bool *isIdentical);

// Allocates a string the size of <source> and copies <source> into it. Returns
// NULL on out-of-memory.
static MustInline MustCheck char *NonNull(1) strAllocAndCopy(const char *source)
{
    size_t sourceSize = strlen(source) + 1;
    char *dest = (char *) memAlloc(sourceSize);

    if (dest != NULL) {
        strlcpy(dest, source, sourceSize);
    }

    return dest;
}

static inline MustCheck char *NonNull(1)
    strAllocAndCopyWithSize(const char *source, size_t sourceSize)
{
    // sourceSize already accounts for NULL byte
    char *dest = (char *) memAlloc(sourceSize);

    if (dest != NULL) {
        memcpy(dest, source, sourceSize);
    }

    return dest;
}

static inline MustCheck Status NonNull(1, 2)
    strStrlcpy(char *dst, const char *src, const size_t len)
{
    if (strlcpy(dst, src, len) >= len) {
        return StatusOverflow;
    } else {
        return StatusOk;
    }
}

#endif  // _STRING_H_
