// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _NEW_TUPLES_CURSOR_H
#define _NEW_TUPLES_CURSOR_H

#include "primitives/Primitives.h"
#include "df/DataFormatBaseTypes.h"
#include "newtupbuf/NewTuplesBuffer.h"

//
// Avoid virtual methods here to save the vtable lookup cost.
// All the Impls are in the header file are inlined deliberately.
//

// Cursor for tuples in a contiguous buffer.
class NewTuplesCursor
{
  public:
    struct Position {
        uint32_t nextTupleOffset = 0;
        uint32_t nextTupleIdx = 0;
        uint32_t bufOffsetFromEnd = 0;

        MustInline void init()
        {
            nextTupleOffset = 0;
            nextTupleIdx = 0;
            bufOffsetFromEnd = 0;
        }

        Position() = default;
        ~Position() = default;

        friend bool operator==(const Position &p1, const Position &p2)
        {
            return p1.bufOffsetFromEnd == p2.bufOffsetFromEnd &&
                   p1.nextTupleIdx == p2.nextTupleIdx &&
                   p1.nextTupleOffset == p2.nextTupleOffset;
        }
        friend bool operator!=(const Position &p1, const Position &p2)
        {
            return p1.bufOffsetFromEnd != p2.bufOffsetFromEnd ||
                   p1.nextTupleIdx != p2.nextTupleIdx ||
                   p1.nextTupleOffset != p2.nextTupleOffset;
        }
    };

  protected:
    Position curPosition_;
    NewTuplesBuffer *tupBuffer_ = NULL;

  private:
    void advanceFixed(const NewTupleMeta *tupleMeta, size_t numTuples);

    void advanceVar(const NewTupleMeta *tupleMeta, size_t numTuples);

    MustInline void advance(const NewTupleMeta *tupleMeta, size_t numTuples)
    {
        if (tupleMeta->getFieldsPacking() ==
            NewTupleMeta::FieldsPacking::Fixed) {
            advanceFixed(tupleMeta, numTuples);
        } else {
            assert(tupleMeta->getFieldsPacking() ==
                   NewTupleMeta::FieldsPacking::Variable);
            advanceVar(tupleMeta, numTuples);
        }
    }

  public:
    enum class SeekOpt {
        Begin,
        Cur,
        End,
    };

    NewTuplesCursor() {}

    NewTuplesCursor(NewTuplesBuffer *tupBuffer) { tupBuffer_ = tupBuffer; }

    ~NewTuplesCursor() = default;

    MustCheck Status getNextFixed(const NewTupleMeta *tupleMeta,
                                  NewTupleValues *retNewTuple);

    MustCheck Status getNextVar(const NewTupleMeta *tupleMeta,
                                NewTupleValues *retNewTuple);

    MustCheck Status getNext(const NewTupleMeta *tupleMeta,
                             NewTupleValues *retNewTuple);

    MustCheck MustInline Position getPosition() { return curPosition_; }

    MustInline void setPosition(Position position) { curPosition_ = position; }

    MustInline void deserialize(NewTuplesBuffer *tupBuffer)
    {
        tupBuffer_ = tupBuffer;
    }

    MustInline void serialize() { tupBuffer_ = NULL; }

    MustInline void reset() { curPosition_.init(); }

    MustCheck Status seek(const NewTupleMeta *tupleMeta,
                          int64_t numTuples,
                          SeekOpt seekOpt)
    {
        int64_t position;
        if (seekOpt == SeekOpt::Cur) {
            position = curPosition_.nextTupleIdx + numTuples;
        } else if (seekOpt == SeekOpt::End) {
            position = tupBuffer_->getNumTuples() + numTuples;
        } else {
            assert(seekOpt == SeekOpt::Begin && "Unknown seekOpt");
            position = numTuples;
        }

        if (position < 0 || (size_t) position >= tupBuffer_->getNumTuples()) {
            return StatusNoData;  // No space
        }

        uint64_t positionUnsigned = position;
        if (positionUnsigned < curPosition_.nextTupleIdx) {
            // Start searching from the beginning since there are no previous
            // pointers.
            curPosition_.init();
        } else if (positionUnsigned == curPosition_.nextTupleIdx) {
            return StatusOk;
        }

        advance(tupleMeta,
                (size_t)(positionUnsigned - curPosition_.nextTupleIdx));

        return StatusOk;
    }

    MustCheck Status invalFixed(const NewTupleMeta *tupleMeta, bool gotoNext);

    MustCheck Status invalVar(const NewTupleMeta *tupleMeta, bool gotoNext);

    MustInline MustCheck Status invalidate(const NewTupleMeta *tupleMeta,
                                           bool gotoNext)
    {
        if (tupleMeta->getFieldsPacking() ==
            NewTupleMeta::FieldsPacking::Fixed) {
            return invalFixed(tupleMeta, gotoNext);
        } else {
            assert(tupleMeta->getFieldsPacking() ==
                   NewTupleMeta::FieldsPacking::Variable);
            return invalVar(tupleMeta, gotoNext);
        }
    }

    friend bool operator==(const NewTuplesCursor &c1, const NewTuplesCursor &c2)
    {
        return c1.curPosition_ == c2.curPosition_;
    }
    friend bool operator!=(const NewTuplesCursor &c1, const NewTuplesCursor &c2)
    {
        return c1.curPosition_ != c2.curPosition_;
    }
};

#endif  // _NEW_TUPLES_CURSOR_H
