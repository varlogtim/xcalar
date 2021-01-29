// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _REMOTEBITMAP_H
#define _REMOTEBITMAP_H

#include <type_traits>
#include "primitives/Primitives.h"

extern int numBitsLookup[256];

inline int
numBitsSetFast8(uint8_t byte)
{
    return numBitsLookup[byte];
}

inline int
numBitsSetFast64(uint64_t word)
{
    return __builtin_popcountll(word);
}

template <typename word_t = uint8_t>
class RemoteBitmap final
{
  public:
    static constexpr const size_t BitsPerWord = sizeof(word_t) * 8;
    static inline size_t sizeInWords(int numBits)
    {
        return (numBits + BitsPerWord - 1) / BitsPerWord;
    }

    int numBits_;
    word_t *bitmapWords_;

    RemoteBitmap() : numBits_(0), bitmapWords_(NULL) {}

    RemoteBitmap(word_t *bitmapWords, int numBits)
        : numBits_(numBits), bitmapWords_(bitmapWords)
    {
        static_assert(!std::is_signed<word_t>::value,
                      "must use unsigned ints for a bitmap");
    }
    // Copy constructor; so we can pass this class by value, since it is small
    RemoteBitmap(const RemoteBitmap &bitmap2) = default;
    ~RemoteBitmap() = default;

    inline int sizeInBytes() const
    {
        return sizeInWords(numBits_) * sizeof(word_t);
    }

    inline void set(int bitNum)
    {
        assert(bitNum < numBits_);
        int wordIndex = bitNum / BitsPerWord;
        int bitIndex = bitNum % BitsPerWord;
        bitmapWords_[wordIndex] |= ((word_t) 1) << bitIndex;
    }

    inline bool test(int bitNum) const
    {
        assert(bitNum < numBits_);
        int wordIndex = bitNum / BitsPerWord;
        int bitIndex = bitNum % BitsPerWord;
        return bitmapWords_[wordIndex] & ((word_t) 1) << bitIndex;
    }

    inline void clear(int bitNum)
    {
        assert(bitNum < numBits_);
        int wordIndex = bitNum / BitsPerWord;
        int bitIndex = bitNum % BitsPerWord;
        bitmapWords_[wordIndex] &= ~(((word_t) 1) << bitIndex);
    }

    inline void clearAll() { memZero(bitmapWords_, sizeInBytes()); }

    inline int numSetUntilBit(int numBitsToCheck) const
    {
        static_assert(sizeof(word_t) == 1,
                      "this only works for byte words due to endianness");
        assert(numBitsToCheck <= numBits_);
        int sum = 0;
        int fullChunks = numBitsToCheck / (8 * sizeof(uint64_t));
        int startByte = fullChunks * sizeof(uint64_t);
        int leftoverBytes =
            (numBitsToCheck - fullChunks * 8 * sizeof(uint64_t)) / BitsPerWord;
        assert(leftoverBytes >= 0);
        int leftoverBits = numBitsToCheck % BitsPerWord;
        // We're going to fast count in 64 bit chunks where possible. This is
        // only allowed because 'number of bits set' is endian independent
        for (int ii = 0; ii < fullChunks; ii++) {
            sum += numBitsSetFast64(((const uint64_t *) bitmapWords_)[ii]);
        }
        for (int ii = startByte; ii < startByte + leftoverBytes; ii++) {
            sum += numBitsSetFast8(bitmapWords_[ii]);
        }
        if (leftoverBits) {
            assert(startByte + leftoverBytes < sizeInBytes());
            uint8_t lastbyte = bitmapWords_[startByte + leftoverBytes];
            lastbyte <<= (BitsPerWord - leftoverBits);
            sum += numBitsSetFast8(lastbyte);
        }
        return sum;
    }

    inline int numSet() const { return numSetUntilBit(numBits_); }
};

#endif  // _REMOTEBITMAP_H
