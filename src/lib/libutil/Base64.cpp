// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <string.h>

#include "primitives/Primitives.h"
#include "util/MemTrack.h"

static inline bool
isBase64(char c)
{
    return isalnum(c) || (c == '+') || (c == '/');
}

static inline uint8_t
decodeChar(char c)
{
    if (c == '+') {
        return 62;
    } else if (c == '/') {
        return 63;
    } else if (c >= 'A' && c <= 'Z') {
        return (uint8_t)(c - 'A');
    } else if (c >= 'a' && c <= 'z') {
        return (uint8_t)(26 + c - 'a');
    } else {
        return (uint8_t)(52 + c - '0');
    }
}

size_t
base64BufSizeMax(const size_t bufInSize)
{
    return (((bufInSize / 3) + 1) * 4) + 1;
}

Status
base64Decode(const char bufIn[],
             size_t bufInSize,
             uint8_t **bufOut,
             size_t *bufOutSizeOut)
{
    Status status = StatusUnknown;
    size_t bufSizeMax = (((bufInSize / 4) + 1) * 3) + 1;
    size_t bufSizeUsed = 0;
    uint8_t *buf = NULL;
    uint64_t ii, jj;
    uint8_t charGroup[4];
    int numEquals = 0;

    buf = (uint8_t *) memAlloc(bufSizeMax);
    if (buf == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ii = 0;
    while (ii < bufInSize) {
        char c;
        numEquals = 0;
        for (jj = 0; jj < 4; jj++) {
            if (ii >= bufInSize) {
                break;
            }

            c = bufIn[ii++];
            if (c == '\0') {
                break;
            } else if (isBase64(c)) {
                charGroup[jj] = decodeChar(c);
            } else if (c == '=') {
                charGroup[jj] = 0;
                numEquals++;
            } else {
                status = StatusInval;
                goto CommonExit;
            }
        }

        if (jj < 4) {
            for (; jj < 4; jj++) {
                charGroup[jj] = 0;
            }
        }

        if (bufSizeUsed + 3 >= bufSizeMax) {
            status = StatusInval;
            goto CommonExit;
        }

        buf[bufSizeUsed++] =
            (uint8_t)((charGroup[0] << 2) + ((charGroup[1] & 0x30) >> 4));
        if (numEquals < 2) {
            buf[bufSizeUsed++] = (uint8_t)(((charGroup[1] & 0xf) << 4) +
                                           ((charGroup[2] & 0x3c) >> 2));
            if (numEquals < 1) {
                buf[bufSizeUsed++] =
                    (uint8_t)(((charGroup[2] & 0x3) << 6) + charGroup[3]);
            }
        }

        if (c == '\0') {
            break;
        }
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (buf != NULL) {
            memFree(buf);
            buf = NULL;
            bufSizeUsed = 0;
        }
    }

    *bufOut = buf;
    *bufOutSizeOut = bufSizeUsed;

    return status;
}

Status
base64Encode(const uint8_t bufIn[],
             size_t bufInSize,
             char **bufOut,
             size_t *bufOutSizeOut)
{
    Status status = StatusUnknown;
    size_t bufSizeMax = base64BufSizeMax(bufInSize);
    char *buf = NULL;
    uint8_t charGroup[3];
    size_t bufSizeUsed = 0;
    uint64_t ii, jj;
    uint64_t numEquals = 0;
    const char base64Charset[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789+/";

    buf = (char *) memAlloc(bufSizeMax + 1);
    if (buf == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ii = 0;
    while (ii < bufInSize) {
        for (jj = 0; jj < 3; jj++) {
            if (ii >= bufInSize) {
                break;
            }

            charGroup[jj] = bufIn[ii++];
        }

        if (jj < 3) {
            numEquals = 3 - jj;
            for (; jj < 3; jj++) {
                charGroup[jj] = 0;
            }
        }

        if (bufSizeUsed + 4 >= bufSizeMax) {
            status = StatusInval;
            goto CommonExit;
        }

        buf[bufSizeUsed++] = base64Charset[(charGroup[0] & 0xfc) >> 2];
        buf[bufSizeUsed++] = base64Charset[(((charGroup[0] & 0x3) << 4) +
                                            ((charGroup[1] & 0xf0) >> 4))];
        if (numEquals < 2) {
            buf[bufSizeUsed++] = base64Charset[(((charGroup[1] & 0xf) << 2) +
                                                ((charGroup[2] & 0xc0) >> 6))];
            if (numEquals < 1) {
                buf[bufSizeUsed++] = base64Charset[charGroup[2] & 0x3f];
            }
        }

        for (jj = 0; jj < numEquals; jj++) {
            buf[bufSizeUsed++] = '=';
        }
    }

    buf[bufSizeUsed] = '\0';
    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (buf != NULL) {
            memFree(buf);
            buf = NULL;
            bufSizeUsed = 0;
        }
    }

    *bufOut = buf;
    *bufOutSizeOut = bufSizeUsed;

    return status;
}
