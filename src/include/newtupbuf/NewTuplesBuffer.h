// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _NEW_TUPLES_BUFFER_H
#define _NEW_TUPLES_BUFFER_H

#include "primitives/Primitives.h"
#include "newtupbuf/NewTupleTypes.h"

//
// Avoid virtual methods here to save the vtable lookup cost.
// All the Impls are in the header file are inlined deliberately.
//

// #define NewTupleDebug

class NewTuplesCursor;

// Organize collection of tuples in a contiguous buffer
class NewTuplesBuffer
{
    friend class NewTuplesCursor;

  protected:
    union {
        void *bufferEnd_;
        size_t bufferEndOffset_;
    };
    uint32_t numTuples_;
    uint32_t bufSize_;
    uint32_t bufUsedFromBegin_;
    uint32_t bufUsedFromEnd_;
    uint32_t bufSizeTotal_;
    uint32_t bufRemaining_;

    union {
        void *bufferStart_;  // Must be the last field
        size_t bufferStartOffset_;
    };

    // Return the next available spot in the buffer.
    MustCheck MustInline uintptr_t getNextAvailSpot(size_t *retRemaining)
    {
        *retRemaining = getBytesRemaining();
        return (uintptr_t) bufferStart_ + bufUsedFromBegin_;
    }

    MustCheck MustInline uintptr_t getNextAvailSpot(size_t *retRemaining,
                                                    uintptr_t *nextEndAddr)
    {
        uintptr_t nextStartAddr = getNextAvailSpot(retRemaining);
        *nextEndAddr = (uintptr_t) bufferEnd_ - bufUsedFromEnd_;
        return nextStartAddr;
    }

    MustCheck bool setFieldVar(uintptr_t startAddr,
                               uintptr_t endAddr,
                               size_t curRemaining,
                               size_t *bytesFromStart,
                               size_t *bytesFromEnd,
                               uint8_t fieldBmap,
                               uint64_t srcSize,
                               uintptr_t **retVarAddr);

    MustCheck bool setField(size_t curRemaining,
                            size_t *bytesFromStart,
                            uintptr_t bufFieldAddr,
                            uint8_t fieldBmap,
                            uint64_t fieldValue)
    {
        assert(fieldBmap <= NewTupleMeta::BmapMask && "Unknown fieldBmap");

        if (NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
            NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap)) {
            // This does not bother to allocate space for an invalid field.
            return true;
        }

        size_t fieldSize = NewTupleMeta::getBmapFieldSize(fieldBmap);
        if (curRemaining < fieldSize) {
            return false;  // No space
        }
        assert(bufFieldAddr + fieldSize <= (uintptr_t) bufferEnd_);

        // Use switch statements so compiler can use jump tables
        switch (fieldBmap) {
        case NewTupleMeta::BmapOneByteField:
            ((uint8_t *) (bufFieldAddr))[0] = fieldValue;
            break;
        case NewTupleMeta::BmapTwoBytesField:
            ((uint16_t *) (bufFieldAddr))[0] = fieldValue;
            break;
        case NewTupleMeta::BmapThreeBytesField:
            memcpy((uint8_t *) (bufFieldAddr),
                   &fieldValue,
                   NewTupleMeta::ThreeBytesFieldSize);
            break;
        case NewTupleMeta::BmapFourBytesField:
            ((uint32_t *) (bufFieldAddr))[0] = fieldValue;
            break;
        case NewTupleMeta::BmapFiveBytesField:
            memcpy((uint8_t *) (bufFieldAddr),
                   &fieldValue,
                   NewTupleMeta::FiveBytesFieldSize);
            break;
        case NewTupleMeta::BmapSixBytesField:
            memcpy((uint8_t *) (bufFieldAddr),
                   &fieldValue,
                   NewTupleMeta::SixBytesFieldSize);
            break;
        default:
            assert(fieldBmap == NewTupleMeta::BmapEightBytesField &&
                   "Unknown fieldBmap");
            ((uint64_t *) (bufFieldAddr))[0] = fieldValue;
            break;
        }
        (*bytesFromStart) += fieldSize;
        return true;
    }

    MustCheck static MustInline uint8_t
    getBmapForField(uintptr_t recordStartAddr, size_t fieldIdx)
    {
        size_t bmapShft = (fieldIdx & (NewTupleMeta::BmapNumEntriesPerByte - 1))
                          << NewTupleMeta::BmapNumBitsPerFieldShft;
        size_t byteIdx = fieldIdx >> NewTupleMeta::BmapNumEntriesPerByteShft;
        uint8_t *curBmapAddr =
            (uint8_t *) ((uintptr_t) recordStartAddr + byteIdx);
        uint8_t bmapMask = ((uint8_t) NewTupleMeta::BmapMask << bmapShft);
        uint8_t bmapValue = ((*curBmapAddr) & bmapMask) >> bmapShft;
        assert(bmapValue <= NewTupleMeta::BmapMask && "Unknown bmap");
        return bmapValue;
    }

    MustInline static void setBmapForField(uintptr_t recordStartAddr,
                                           size_t fieldIdx,
                                           uint8_t bmapValue)
    {
        assert(bmapValue <= NewTupleMeta::BmapMask && "Unknown bmapValue");
        size_t bmapShft = (fieldIdx & (NewTupleMeta::BmapNumEntriesPerByte - 1))
                          << NewTupleMeta::BmapNumBitsPerFieldShft;
        size_t byteIdx = fieldIdx >> NewTupleMeta::BmapNumEntriesPerByteShft;
        uint8_t *curBmapAddr = (uint8_t *) (recordStartAddr + byteIdx);
        uint8_t bmapMask = ((uint8_t) NewTupleMeta::BmapMask << bmapShft);
        (*curBmapAddr) = ((*curBmapAddr) & ~bmapMask) | (bmapValue << bmapShft);
#ifdef NewTupleDebug
        assert(getBmapForField(recordStartAddr, fieldIdx) == bmapValue &&
               "bmapValue mismatch");
#endif  // NewTupleDebug
    }

  public:
    MustInline void reset()
    {
        bufUsedFromBegin_ = 0;
        bufUsedFromEnd_ = 0;
        bufRemaining_ = bufSize_;
        numTuples_ = 0;
    }

    NewTuplesBuffer(void *buffer, size_t bufSize)
    {
        assert(buffer != NULL && bufSize != 0);
        bufSizeTotal_ = bufSize;
        bufSize_ = bufSizeTotal_ - sizeof(NewTuplesBuffer);
        bufferStart_ = (void *) ((uintptr_t) buffer + bufSizeTotal_ - bufSize_);
        bufferEnd_ = (void *) ((uintptr_t) bufferStart_ + bufSize_);
        reset();
    }

    ~NewTuplesBuffer() = default;

    MustInline void serialize()
    {
        bufferStartOffset_ = (uintptr_t) bufferStart_ - (uintptr_t) this;
        bufferEndOffset_ = (uintptr_t) bufferEnd_ - (uintptr_t) this;
    }

    MustInline void deserialize()
    {
        bufferStart_ = (void *) ((uintptr_t) this + bufferStartOffset_);
        bufferEnd_ = (void *) ((uintptr_t) this + bufferEndOffset_);
    }

    MustInline MustCheck size_t getNumTuples() { return numTuples_; }

    MustInline MustCheck void *getBufferStart() { return bufferStart_; }

    MustInline MustCheck size_t getBufferStartOffset()
    {
        return (uintptr_t) bufferStart_ - (uintptr_t) this;
    }

    MustInline void setBufferStartOffset(size_t offset)
    {
        bufferStartOffset_ = offset;
    }

    MustInline MustCheck void *getBufferEnd() { return bufferEnd_; }

    MustInline MustCheck size_t getBufferEndOffset()
    {
        return (uintptr_t) bufferEnd_ - (uintptr_t) this;
    }

    MustInline void setBufferEndOffset(size_t offset)
    {
        bufferEndOffset_ = offset;
    }

    MustInline MustCheck size_t getBufSize() { return bufSize_; }

    MustCheck Status appendFixed(const NewTupleMeta *tupleMeta,
                                 NewTupleValues *tupleValues);

    MustCheck Status appendVar(const NewTupleMeta *tupleMeta,
                               NewTupleValues *tupleValues);

    MustInline MustCheck Status append(const NewTupleMeta *tupleMeta,
                                       NewTupleValues *tupleValues)
    {
        if (tupleMeta->getFieldsPacking() ==
            NewTupleMeta::FieldsPacking::Fixed) {
            return appendFixed(tupleMeta, tupleValues);
        } else {
            assert(tupleMeta->getFieldsPacking() ==
                   NewTupleMeta::FieldsPacking::Variable);
            return appendVar(tupleMeta, tupleValues);
        }
    }

    void removeFromEndFixed(const NewTupleMeta *tupleMeta, size_t removeCount);

    void removeFromEndVar(const NewTupleMeta *tupleMeta, size_t removeCount);

    MustInline void removeFromEnd(const NewTupleMeta *tupleMeta,
                                  size_t removeCount)
    {
        if (tupleMeta->getFieldsPacking() ==
            NewTupleMeta::FieldsPacking::Fixed) {
            removeFromEndFixed(tupleMeta, removeCount);
        } else {
            assert(tupleMeta->getFieldsPacking() ==
                   NewTupleMeta::FieldsPacking::Variable);
            removeFromEndVar(tupleMeta, removeCount);
        }
    }

    // Considering the new packing style, we need to declare the entire
    // buffer is used, since we consume space from top and bottom of the
    // buffer.
    MustCheck MustInline size_t getTotalBytesUsed() { return bufSizeTotal_; }

    MustCheck MustInline size_t getBytesRemaining() { return bufRemaining_; }

    friend bool operator==(const NewTuplesBuffer b1, const NewTuplesBuffer b2)
    {
        return b1.numTuples_ == b2.numTuples_ && b1.bufSize_ == b2.bufSize_ &&
               b1.bufUsedFromBegin_ == b2.bufUsedFromBegin_ &&
               b1.bufUsedFromEnd_ == b2.bufUsedFromEnd_ &&
               b1.bufSizeTotal_ == b2.bufSizeTotal_ &&
               b1.bufRemaining_ == b2.bufRemaining_;
    }

    friend bool operator!=(const NewTuplesBuffer b1, const NewTuplesBuffer b2)
    {
        return b1.numTuples_ != b2.numTuples_ || b1.bufSize_ != b2.bufSize_ ||
               b1.bufUsedFromBegin_ != b2.bufUsedFromBegin_ ||
               b1.bufUsedFromEnd_ != b2.bufUsedFromEnd_ ||
               b1.bufSizeTotal_ != b2.bufSizeTotal_ ||
               b1.bufRemaining_ != b2.bufRemaining_;
    }
};

#endif  // _NEW_TUPLES_BUFFER_H
