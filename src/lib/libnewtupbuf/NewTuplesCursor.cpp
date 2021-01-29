// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "newtupbuf/NewTuplesCursor.h"
#include "df/DataFormat.h"
#include "xdb/DataModelTypes.h"

void
NewTuplesCursor::advanceFixed(const NewTupleMeta *tupleMeta, size_t numTuples)
{
    assert(curPosition_.nextTupleIdx + numTuples < tupBuffer_->getNumTuples() &&
           "Not enough tuples in the buffer to advance cursor by numTuples");

    size_t numFields = tupleMeta->getNumFields();
    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();
    size_t bytesFromStart = 0;

    for (size_t tupleIter = 0; tupleIter < numTuples; tupleIter++) {
        // Expectation is that the Cursor engine precisely tracks position
        // within the tuple buffer to prevent out of bound conditions.
        void *nextTuple =
            (void *) (bufStart + curPosition_.nextTupleOffset + bytesFromStart);
        assert((uintptr_t) nextTuple < bufEnd);
        bytesFromStart += NewTupleMeta::getPerRecordBmapSize(numFields);

        for (size_t ii = 0; ii < numFields; ii++) {
            uint8_t fieldBmap =
                tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);
            bytesFromStart += NewTupleMeta::getBmapFieldSize(fieldBmap);
        }
    }

    curPosition_.nextTupleOffset += bytesFromStart;
    curPosition_.nextTupleIdx += numTuples;
}

Status
NewTuplesCursor::invalFixed(const NewTupleMeta *tupleMeta, bool gotoNext)
{
    if (unlikely(curPosition_.nextTupleIdx >= tupBuffer_->getNumTuples())) {
        return StatusNoData;  // no more tuples.
    }

    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();

    // Expectation is that the Cursor engine precisely tracks position
    // within the tuple buffer to prevent out of bound conditions.
    void *nextTuple = (void *) (bufStart + curPosition_.nextTupleOffset);
    assert((uintptr_t) nextTuple < bufEnd);
    size_t numFields = tupleMeta->getNumFields();
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);

    for (size_t ii = 0; ii < numFields; ii++) {
        uint8_t fieldBmap =
            tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);
        if (!(NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
              NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap))) {
            uint8_t invalFieldBmap =
                NewTupleMeta::getBmapFieldMarkInvalid(fieldBmap);
            tupBuffer_->setBmapForField((uintptr_t) nextTuple,
                                        ii,
                                        invalFieldBmap);
        }
        if (gotoNext) {
            bytesFromStart += NewTupleMeta::getBmapFieldSize(fieldBmap);
        }
    }

    if (gotoNext) {
        curPosition_.nextTupleOffset += bytesFromStart;
        curPosition_.nextTupleIdx++;
    }

    return StatusOk;
}

MustCheck Status
NewTuplesCursor::getNext(const NewTupleMeta *tupleMeta,
                         NewTupleValues *retNewTuple)
{
#ifdef TUPLE_BUFFER_POISON
    retNewTuple->poison();
#endif  // TUPLE_BUFFER_POISON

    if (tupleMeta->getFieldsPacking() == NewTupleMeta::FieldsPacking::Fixed) {
        return getNextFixed(tupleMeta, retNewTuple);
    } else {
        assert(tupleMeta->getFieldsPacking() ==
               NewTupleMeta::FieldsPacking::Variable);
        return getNextVar(tupleMeta, retNewTuple);
    }
}

MustCheck Status
NewTuplesCursor::getNextFixed(const NewTupleMeta *tupleMeta,
                              NewTupleValues *retNewTuple)
{
    if (unlikely(curPosition_.nextTupleIdx >= tupBuffer_->getNumTuples())) {
        return StatusNoData;  // no more tuples.
    }

    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();

    // Expectation is that the Cursor engine precisely tracks position
    // within the tuple buffer to prevent out of bound conditions.
    void *nextTuple = (void *) (bufStart + curPosition_.nextTupleOffset);
    assert((uintptr_t) nextTuple < bufEnd);
    size_t numFields = tupleMeta->getNumFields();
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);

    for (size_t ii = 0; ii < numFields; ii++) {
        uint8_t fieldBmap =
            tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);

        retNewTuple->bitMaps_[ii] = fieldBmap;

        uint8_t cursorBmap = NewTupleMeta::getBmapFieldUnMarkInvalid(fieldBmap);

        switch (cursorBmap) {
        case NewTupleMeta::BmapInvalid:
            // Noop
            break;
        case NewTupleMeta::BmapOneByteField:
            retNewTuple->fields_[ii].uint64Val =
                ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
            bytesFromStart += NewTupleMeta::OneByteFieldSize;
            break;
        case NewTupleMeta::BmapTwoBytesField:
            retNewTuple->fields_[ii].uint64Val =
                ((uint16_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
            bytesFromStart += NewTupleMeta::TwoBytesFieldSize;
            break;
        case NewTupleMeta::BmapThreeBytesField:
            retNewTuple->fields_[ii].uint64Val = 0;
            memcpy(&retNewTuple->fields_[ii].uint64Val,
                   ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                   NewTupleMeta::ThreeBytesFieldSize);
            bytesFromStart += NewTupleMeta::ThreeBytesFieldSize;
            break;
        case NewTupleMeta::BmapFourBytesField:
            retNewTuple->fields_[ii].uint64Val =
                ((uint32_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
            bytesFromStart += NewTupleMeta::FourBytesFieldSize;
            break;
        case NewTupleMeta::BmapFiveBytesField:
            retNewTuple->fields_[ii].uint64Val = 0;
            memcpy(&retNewTuple->fields_[ii].uint64Val,
                   ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                   NewTupleMeta::FiveBytesFieldSize);
            bytesFromStart += NewTupleMeta::FiveBytesFieldSize;
            break;
        case NewTupleMeta::BmapSixBytesField:
            retNewTuple->fields_[ii].uint64Val = 0;
            memcpy(&retNewTuple->fields_[ii].uint64Val,
                   ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                   NewTupleMeta::SixBytesFieldSize);
            bytesFromStart += NewTupleMeta::SixBytesFieldSize;
            break;
        default:
            assert(fieldBmap < NewTupleMeta::BmapMax && "Invalid field bmap");
            retNewTuple->fields_[ii].uint64Val =
                ((uint64_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
            bytesFromStart += NewTupleMeta::EightBytesFieldSize;
            break;
        }
    }

    curPosition_.nextTupleOffset += bytesFromStart;
    curPosition_.nextTupleIdx++;

    return StatusOk;
}

void
NewTuplesCursor::advanceVar(const NewTupleMeta *tupleMeta, size_t numTuples)
{
    assert(curPosition_.nextTupleIdx + numTuples < tupBuffer_->getNumTuples() &&
           "Not enough tuples in the buffer to advance cursor by numTuples");

    size_t numFields = tupleMeta->getNumFields();
    size_t bytesFromStart = 0;
    size_t bytesFromEnd = 0;
    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();

    for (size_t tupleIter = 0; tupleIter < numTuples; tupleIter++) {
        // Expectation is that the Cursor engine precisely tracks position
        // within the tuple buffer to prevent out of bound conditions.
        void *nextTuple =
            (void *) (bufStart + curPosition_.nextTupleOffset + bytesFromStart);
        assert((uintptr_t) nextTuple < bufEnd);
        size_t curFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);
        size_t curSrcSize = 0;

        for (size_t ii = 0; ii < numFields; ii++) {
            DfFieldType fieldType = tupleMeta->getFieldType(ii);
            uint8_t fieldBmap =
                tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);
            uint8_t cursorBmap =
                NewTupleMeta::getBmapFieldUnMarkInvalid(fieldBmap);

            if (DataFormat::fieldTypeIsFixed(fieldType)) {
                curFromStart += NewTupleMeta::getBmapFieldSize(fieldBmap);
            } else {
                if (NewTupleMeta::isFieldBmapZeroByteInvalid(fieldBmap)) {
                    // We don't bother allocating space for an invalid field.
                    continue;
                }

#ifdef DEBUG
                switch (fieldType) {
                case DfString:
                case DfTimespec:
                case DfMoney:
                case DfScalarObj:
                    break;
                case DfUnknown:
                    // If field type is DfUnknown, bitmap value would have
                    // indicated an invalid field and we will not reached here.
                    assert(0 && "Valid field cannot have field type Dfunknown");
                    break;
                default:
                    assert(0 && "Uknown variable length field type");
                    break;
                }
#endif  // DEBUG

                switch (cursorBmap) {
                case NewTupleMeta::BmapOneByteField:
                    curSrcSize +=
                        *(uint8_t *) ((uintptr_t) nextTuple + curFromStart);
                    curFromStart += NewTupleMeta::OneByteFieldSize;
                    break;
                case NewTupleMeta::BmapTwoBytesField: {
                    curSrcSize +=
                        *(uint16_t *) ((uintptr_t) nextTuple + curFromStart);
                    curFromStart += NewTupleMeta::TwoBytesFieldSize;
                    break;
                }
                case NewTupleMeta::BmapThreeBytesField: {
                    size_t curBytes = 0;
                    memcpy(&curBytes,
                           (uint8_t *) ((uintptr_t) nextTuple + curFromStart),
                           NewTupleMeta::ThreeBytesFieldSize);
                    curSrcSize += curBytes;
                    curFromStart += NewTupleMeta::ThreeBytesFieldSize;
                    break;
                }
                default: {
                    assert(NewTupleMeta::isFieldBmapFourBytesSize(fieldBmap) &&
                           "Unknown fieldBmap");
                    curSrcSize +=
                        *(uint32_t *) ((uintptr_t) nextTuple + curFromStart);
                    curFromStart += NewTupleMeta::FourBytesFieldSize;
                    break;
                }
                }
            }
        }
        bytesFromEnd += curSrcSize;
        bytesFromStart += curFromStart;
    }

    curPosition_.nextTupleOffset += bytesFromStart;
    curPosition_.bufOffsetFromEnd += bytesFromEnd;
    curPosition_.nextTupleIdx += numTuples;
}

Status
NewTuplesCursor::invalVar(const NewTupleMeta *tupleMeta, bool gotoNext)
{
    if (unlikely(curPosition_.nextTupleIdx >= tupBuffer_->getNumTuples())) {
        return StatusNoData;  // no more tuples.
    }

    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();

    // Expectation is that the Cursor engine precisely tracks position
    // within the tuple buffer to prevent out of bound conditions.
    void *nextTuple = (void *) (bufStart + curPosition_.nextTupleOffset);
    assert((uintptr_t) nextTuple < bufEnd);
    size_t numFields = tupleMeta->getNumFields();
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);
    size_t bytesFromEnd = 0;

    for (size_t ii = 0; ii < numFields; ii++) {
        DfFieldType fieldType = tupleMeta->getFieldType(ii);
        uint8_t fieldBmap =
            tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);
        uint8_t cursorBmap = NewTupleMeta::getBmapFieldUnMarkInvalid(fieldBmap);

        if (DataFormat::fieldTypeIsFixed(fieldType)) {
            if (!(NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
                  NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap))) {
                uint8_t invalFieldBmap =
                    NewTupleMeta::getBmapFieldMarkInvalid(fieldBmap);
                tupBuffer_->setBmapForField((uintptr_t) nextTuple,
                                            ii,
                                            invalFieldBmap);
            }
            if (gotoNext) {
                bytesFromStart += NewTupleMeta::getBmapFieldSize(fieldBmap);
            }
        } else {
            if (NewTupleMeta::isFieldBmapZeroByteInvalid(fieldBmap)) {
                // We don't bother allocating space for an invalid field.
                continue;
            }

            if (!(NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
                  NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap))) {
                uint8_t invalFieldBmap =
                    NewTupleMeta::getBmapFieldMarkInvalid(fieldBmap);
                tupBuffer_->setBmapForField((uintptr_t) nextTuple,
                                            ii,
                                            invalFieldBmap);
            }

            if (gotoNext) {
                size_t srcSize;
                size_t fieldSize;
                switch (cursorBmap) {
                case NewTupleMeta::BmapOneByteField:
                    srcSize =
                        *(uint8_t *) ((uintptr_t) nextTuple + bytesFromStart);
                    fieldSize = NewTupleMeta::OneByteFieldSize;
                    break;
                case NewTupleMeta::BmapTwoBytesField:
                    srcSize =
                        *(uint16_t *) ((uintptr_t) nextTuple + bytesFromStart);
                    fieldSize = NewTupleMeta::TwoBytesFieldSize;
                    break;
                case NewTupleMeta::BmapThreeBytesField:
                    srcSize = 0;
                    memcpy(&srcSize,
                           (uint8_t *) ((uintptr_t) nextTuple + bytesFromStart),
                           NewTupleMeta::ThreeBytesFieldSize);
                    fieldSize = NewTupleMeta::ThreeBytesFieldSize;
                    break;
                default:
                    assert(NewTupleMeta::BmapFourBytesField == cursorBmap &&
                           "Unknown cursorBmap");
                    srcSize =
                        *(uint32_t *) ((uintptr_t) nextTuple + bytesFromStart);
                    fieldSize = NewTupleMeta::FourBytesFieldSize;
                    break;
                }

                bytesFromStart += fieldSize;
                bytesFromEnd += srcSize;
#ifdef DEBUG
                uintptr_t varAddr =
                    bufEnd - curPosition_.bufOffsetFromEnd - bytesFromEnd;
                assert(varAddr < bufEnd && varAddr + srcSize <= bufEnd);
#endif  // DEBUG
            }
        }
    }

    if (gotoNext) {
        curPosition_.bufOffsetFromEnd += bytesFromEnd;
        curPosition_.nextTupleOffset += bytesFromStart;
        curPosition_.nextTupleIdx++;
    }

    return StatusOk;
}

MustCheck Status
NewTuplesCursor::getNextVar(const NewTupleMeta *tupleMeta,
                            NewTupleValues *retNewTuple)
{
    assert(tupBuffer_ != NULL);
    if (unlikely(curPosition_.nextTupleIdx >= tupBuffer_->getNumTuples())) {
        return StatusNoData;  // no more tuples.
    }

    uintptr_t bufStart = (uintptr_t) tupBuffer_->getBufferStart();
    uintptr_t bufEnd = (uintptr_t) tupBuffer_->getBufferEnd();

    // Expectation is that the Cursor engine precisely tracks position
    // within the tuple buffer to prevent out of bound conditions.
    void *nextTuple = (void *) (bufStart + curPosition_.nextTupleOffset);
    assert((uintptr_t) nextTuple < bufEnd);
    size_t numFields = tupleMeta->getNumFields();
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);
    size_t bytesFromEnd = 0;

    for (size_t ii = 0; ii < numFields; ii++) {
        DfFieldType fieldType = tupleMeta->getFieldType(ii);
        uint8_t fieldBmap =
            tupBuffer_->getBmapForField((uintptr_t) nextTuple, ii);
        retNewTuple->bitMaps_[ii] = fieldBmap;
        uint8_t cursorBmap = NewTupleMeta::getBmapFieldUnMarkInvalid(fieldBmap);

        if (DataFormat::fieldTypeIsFixed(fieldType)) {
            switch (cursorBmap) {
            case NewTupleMeta::BmapInvalid:
                // This does not bother to allocate space for an invalid field.
                break;
            case NewTupleMeta::BmapOneByteField:
                retNewTuple->fields_[ii].uint64Val =
                    ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
                bytesFromStart += NewTupleMeta::OneByteFieldSize;
                break;
            case NewTupleMeta::BmapTwoBytesField:
                retNewTuple->fields_[ii].uint64Val =
                    ((uint16_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
                bytesFromStart += NewTupleMeta::TwoBytesFieldSize;
                break;
            case NewTupleMeta::BmapThreeBytesField:
                retNewTuple->fields_[ii].uint64Val = 0;
                memcpy(&retNewTuple->fields_[ii].uint64Val,
                       ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                       NewTupleMeta::ThreeBytesFieldSize);
                bytesFromStart += NewTupleMeta::ThreeBytesFieldSize;
                break;
            case NewTupleMeta::BmapFourBytesField:
                retNewTuple->fields_[ii].uint64Val =
                    ((uint32_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
                bytesFromStart += NewTupleMeta::FourBytesFieldSize;
                break;
            case NewTupleMeta::BmapFiveBytesField:
                retNewTuple->fields_[ii].uint64Val = 0;
                memcpy(&retNewTuple->fields_[ii].uint64Val,
                       ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                       NewTupleMeta::FiveBytesFieldSize);
                bytesFromStart += NewTupleMeta::FiveBytesFieldSize;
                break;
            case NewTupleMeta::BmapSixBytesField:
                retNewTuple->fields_[ii].uint64Val = 0;
                memcpy(&retNewTuple->fields_[ii].uint64Val,
                       ((uint8_t *) ((uintptr_t) nextTuple + bytesFromStart)),
                       NewTupleMeta::SixBytesFieldSize);
                bytesFromStart += NewTupleMeta::SixBytesFieldSize;
                break;
            default:
                assert(NewTupleMeta::isFieldBmapEightBytesSize(fieldBmap) &&
                       "Uknown fieldBmap");
                retNewTuple->fields_[ii].uint64Val =
                    ((uint64_t *) ((uintptr_t) nextTuple + bytesFromStart))[0];
                bytesFromStart += NewTupleMeta::EightBytesFieldSize;
                break;
            }
        } else {
            if (NewTupleMeta::isFieldBmapZeroByteInvalid(fieldBmap)) {
                // We don't bother allocating space for an invalid field.
                continue;
            }

            size_t srcSize;
            switch (cursorBmap) {
            case NewTupleMeta::BmapOneByteField:
                srcSize = *(uint8_t *) ((uintptr_t) nextTuple + bytesFromStart);
                bytesFromStart += NewTupleMeta::OneByteFieldSize;
                break;
            case NewTupleMeta::BmapTwoBytesField:
                srcSize =
                    *(uint16_t *) ((uintptr_t) nextTuple + bytesFromStart);
                bytesFromStart += NewTupleMeta::TwoBytesFieldSize;
                break;
            case NewTupleMeta::BmapThreeBytesField:
                srcSize = 0;
                memcpy(&srcSize,
                       (uint8_t *) ((uintptr_t) nextTuple + bytesFromStart),
                       NewTupleMeta::ThreeBytesFieldSize);
                bytesFromStart += NewTupleMeta::ThreeBytesFieldSize;
                break;
            default:
                assert(NewTupleMeta::isFieldBmapFourBytesSize(fieldBmap) &&
                       "Unknown fieldBmap");
                srcSize =
                    *(uint32_t *) ((uintptr_t) nextTuple + bytesFromStart);
                bytesFromStart += NewTupleMeta::FourBytesFieldSize;
                break;
            }

            DfFieldValue *src = &retNewTuple->fields_[ii];
            bytesFromEnd += srcSize;
            uintptr_t varAddr =
                bufEnd - curPosition_.bufOffsetFromEnd - bytesFromEnd;
            assert(varAddr < bufEnd && varAddr + srcSize <= bufEnd);
            switch (fieldType) {
            case DfString: {
                src->stringVal.strActual = (const char *) varAddr;
                src->stringVal.strSize = srcSize;
                break;
            }
            case DfScalarObj: {
                src->scalarVal = (Scalar *) varAddr;
                break;
            }
            case DfTimespec: {
                src->timeVal = *(DfTimeval *) varAddr;
                break;
            }
            case DfMoney: {
                src->numericVal = *(XlrDfp *) varAddr;
                break;
            }
            case DfUnknown: {
                // If field type is DfUnknown, bitmap value would have indicated
                // an invalid field and we will not reached here.
                assert(0 && "Valid field cannot have field type Dfunknown");
                break;
            }
            default:
                assert(0 && "Uknown variable length field type");
                break;
            }
        }
    }

    curPosition_.bufOffsetFromEnd += bytesFromEnd;
    curPosition_.nextTupleOffset += bytesFromStart;
    curPosition_.nextTupleIdx++;

    return StatusOk;
}
