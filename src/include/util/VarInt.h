// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _VARINT_H_
#define _VARINT_H_

#include "primitives/Primitives.h"
#include "assert.h"

// Returns true on success; false on failure
template <typename int_t = uint64_t>
inline bool
decodeVarint(const uint8_t *rawBytes,
             size_t rawBytesSize,
             int_t *varint,
             size_t *varintLength)
{
    assert(rawBytes);
    assert(rawBytesSize > 0);
    int_t varintVal = 0;
    size_t ii = 0;
    bool moreBytes;
    do {
        // The highest order bit of each byte tells us if we need more bytes
        moreBytes = (rawBytes[ii] >> 7) & 0x1;

        // Each byte needs to get left shifted by 7 * byteNum
        varintVal += ((int_t)(rawBytes[ii] & 0x7f)) << (7 * ii);
        ++ii;
        if (!moreBytes) {
            break;
        }
        if (moreBytes && ii >= rawBytesSize) {
            return false;
        }
    } while (moreBytes);

    if (varintLength) {
        *varintLength = ii;
    }
    if (varint) {
        *varint = varintVal;
    }
    return true;
}

// Returns the serialized size of the varint
// 'dryRun' is a templatized function argument so that we don't generate any
// branches in the body of this function
template <bool dryRun, bool checkSpace, typename int_t = uint64_t>
inline bool
encodeVarint(int_t value,
             uint8_t *rawBytes,
             size_t rawBytesSize,
             size_t *varintLength)
{
    if (!dryRun) {
        assert(rawBytes);
    }

    size_t ii = 0;
    bool moreBytes;
    do {
        moreBytes = value > 0x7f;
        if (checkSpace && ii >= rawBytesSize) {
            return false;
        }
        if (!dryRun) {
            rawBytes[ii] = (value & 0x7f) | (moreBytes << 7);
        }
        value >>= 7;
        ++ii;
    } while (moreBytes);

    if (varintLength) {
        *varintLength = ii;
    }
    return true;
}

// Here's an explanation of how zigZag encoding works
// https://gist.github.com/mfuerstenau/ba870a29e16536fdbaba
inline uint32_t
zigZagEncode32(int32_t n)
{
    // We want to right shift the sign bit, so keep n as signed
    // We want the left shift to be unsigned because overflow for ints is
    // undefined
    return (((uint32_t)(n)) << 1) ^ ((uint32_t)(n >> 31));
}

inline int32_t
zigZagDecode32(uint32_t n)
{
    // Do all shifts using uint because shifting signed ints is undefined in
    // overflow
    return ((int32_t)((n >> 1) ^ (~(n & 1) + 1)));
}

inline uint64_t
zigZagEncode64(int64_t n)
{
    // We want to right shift the sign bit, so keep n as signed
    // We want the left shift to be unsigned because overflow for ints is
    // undefined
    return (((uint64_t)(n)) << 1) ^ ((uint64_t)(n >> 63));
}

inline int64_t
zigZagDecode64(uint64_t n)
{
    // Do all shifts using uint because shifting signed ints is undefined in
    // overflow
    return ((int64_t)((n >> 1) ^ (~(n & 1) + 1)));
}

#endif  // _VARINT_H_
