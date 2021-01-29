// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "newtupbuf/NewTuplesBuffer.h"
#include "df/DataFormat.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "NewTuplesBuffer";

void
NewTuplesBuffer::removeFromEndFixed(const NewTupleMeta *tupleMeta,
                                    size_t removeCount)
{
    if (!removeCount) {
        return;
    }

    assert(removeCount <= numTuples_);
    size_t keepCount = numTuples_ - removeCount;

    NewTuplesCursor cursor(this);
    Status status =
        cursor.seek(tupleMeta, keepCount, NewTuplesCursor::SeekOpt::Begin);
    assert(status == StatusOk);

    NewTuplesCursor::Position curPos = cursor.getPosition();
    numTuples_ = keepCount;
    bufUsedFromBegin_ = curPos.nextTupleOffset;
    assert(curPos.bufOffsetFromEnd == 0);
    bufRemaining_ = bufSize_ - bufUsedFromBegin_;
    assert(bufUsedFromBegin_ + bufUsedFromEnd_ <= bufSize_ &&
           "Used bytes from buffer exceeds bufSize");
    assert(bufUsedFromEnd_ == 0 &&
           "Fixed size fields cannot consume from variable size buffer space");
}

MustCheck Status
NewTuplesBuffer::appendFixed(const NewTupleMeta *tupleMeta,
                             NewTupleValues *tupleValues)
{
    size_t numFields = tupleMeta->getNumFields();
    size_t curRemaining;
    uintptr_t startAddr = (uintptr_t) getNextAvailSpot(&curRemaining);
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);

    if (curRemaining < bytesFromStart) {
        return StatusNoData;  // No space
    }

    assert(startAddr < (uintptr_t) bufferEnd_ &&
           startAddr + bytesFromStart <= (uintptr_t) bufferEnd_);

    for (size_t ii = 0; ii < numFields; ii++) {
        DfFieldType fieldType = tupleMeta->getFieldType(ii);
        uint8_t fieldBmap = tupleValues->getBitMap(ii);

        assert(fieldBmap <= NewTupleMeta::BmapMask && "Unknown fieldBmap");

        if (NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
            NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap)) {
            // Given the field is invalid, move on to the next one.
            setBmapForField(startAddr, ii, NewTupleMeta::BmapInvalid);
            continue;
        }

        DfFieldValue fieldValue = tupleValues->get(ii, numFields, fieldType);
        assert(curRemaining >= bytesFromStart);
        if (!setField(curRemaining - bytesFromStart,
                      &bytesFromStart,
                      startAddr + bytesFromStart,
                      fieldBmap,
                      fieldValue.uint64Val)) {
            return StatusNoData;  // No space
        }
        setBmapForField(startAddr, ii, fieldBmap);

#ifdef NewTupleDebug
        assert(getBmapForField(startAddr, ii) == fieldBmap &&
               "fieldBmap mismatch");
#endif  // NewTupleDebug
    }

    numTuples_++;
    bufUsedFromBegin_ += bytesFromStart;
    bufRemaining_ = bufSize_ - bufUsedFromBegin_;
    assert(bufUsedFromBegin_ + bufUsedFromEnd_ <= bufSize_ &&
           "Used bytes from buffer exceeds bufSize");
    assert(bufUsedFromEnd_ == 0 &&
           "Fixed size fields cannot consume from variable size buffer space");
    return StatusOk;
}

void
NewTuplesBuffer::removeFromEndVar(const NewTupleMeta *tupleMeta,
                                  size_t removeCount)
{
    if (!removeCount) {
        return;
    }

    assert(removeCount <= numTuples_);
    size_t keepCount = numTuples_ - removeCount;

    NewTuplesCursor cursor(this);
    Status status =
        cursor.seek(tupleMeta, keepCount, NewTuplesCursor::SeekOpt::Begin);
    assert(status == StatusOk);

    NewTuplesCursor::Position curPos = cursor.getPosition();
    numTuples_ = keepCount;
    bufUsedFromBegin_ = curPos.nextTupleOffset;
    bufUsedFromEnd_ = curPos.bufOffsetFromEnd;
    bufRemaining_ = bufSize_ - bufUsedFromBegin_;
    assert(bufUsedFromBegin_ + bufUsedFromEnd_ <= bufSize_ &&
           "Used bytes from buffer exceeds bufSize");
}

MustCheck bool
NewTuplesBuffer::setFieldVar(uintptr_t startAddr,
                             uintptr_t endAddr,
                             size_t curRemaining,
                             size_t *bytesFromStart,
                             size_t *bytesFromEnd,
                             uint8_t fieldBmap,
                             uint64_t srcSize,
                             uintptr_t **retVarAddr)
{
    assert(srcSize != 0 && "Valid field cannot have 0 srcSize");
    assert(srcSize <= DfMaxFieldValueSize &&
           "srcSize exceeds DfMaxFieldValueSize");
    if (!setField(curRemaining - (*bytesFromStart) - (*bytesFromEnd),
                  bytesFromStart,
                  startAddr + (*bytesFromStart),
                  fieldBmap,
                  srcSize)) {
        return false;  // No space
    }

    if (curRemaining < (*bytesFromStart) + (*bytesFromEnd) + srcSize) {
        return false;  // No space
    }

    uintptr_t *varAddr = (uintptr_t *) (endAddr - (*bytesFromEnd) - srcSize);
    (*bytesFromEnd) += srcSize;
    assert((uintptr_t) varAddr + srcSize <= (uintptr_t) bufferEnd_);
    (*retVarAddr) = varAddr;

    return true;
}

MustCheck Status
NewTuplesBuffer::appendVar(const NewTupleMeta *tupleMeta,
                           NewTupleValues *tupleValues)
{
    size_t numFields = tupleMeta->getNumFields();
    size_t curRemaining;
    uintptr_t endAddr;
    uintptr_t startAddr = (uintptr_t) getNextAvailSpot(&curRemaining, &endAddr);
    size_t bytesFromStart = NewTupleMeta::getPerRecordBmapSize(numFields);
    size_t bytesFromEnd = 0;

    if (curRemaining < bytesFromStart) {
        return StatusNoData;  // No space
    }

    assert(startAddr < (uintptr_t) bufferEnd_ &&
           startAddr + bytesFromStart <= (uintptr_t) bufferEnd_);

    for (size_t ii = 0; ii < numFields; ii++) {
        DfFieldType fieldType = tupleMeta->getFieldType(ii);
        uint8_t fieldBmap = tupleValues->getBitMap(ii);

        assert(fieldBmap <= NewTupleMeta::BmapMask && "Unknown fieldBmap");

        if (NewTupleMeta::isFieldBmapInvalid(fieldBmap) ||
            NewTupleMeta::isFieldBmapMarkedInvalid(fieldBmap)) {
            // Given the field is invalid, move on to the next one.
            setBmapForField(startAddr, ii, NewTupleMeta::BmapInvalid);
            continue;
        }

        DfFieldValue fieldValue = tupleValues->get(ii, numFields, fieldType);

        if (!DataFormat::fieldTypeIsFixed(fieldType)) {
            assert((fieldBmap == NewTupleMeta::BmapOneByteField ||
                    fieldBmap == NewTupleMeta::BmapTwoBytesField ||
                    fieldBmap == NewTupleMeta::BmapThreeBytesField ||
                    fieldBmap == NewTupleMeta::BmapFourBytesField) &&
                   "Unknown fieldBmap");
            assert(curRemaining >= bytesFromStart + bytesFromEnd);

            switch (fieldType) {
            case DfTimespec: {
                size_t srcSize = sizeof(DfTimeval);
                uintptr_t *varAddr = NULL;
                if (!setFieldVar(startAddr,
                                 endAddr,
                                 curRemaining,
                                 &bytesFromStart,
                                 &bytesFromEnd,
                                 fieldBmap,
                                 srcSize,
                                 &varAddr)) {
                    return StatusNoData;  // No space
                }
                memcpy(varAddr, &fieldValue.timeVal, srcSize);
                break;
            }
            case DfMoney: {
                size_t srcSize = sizeof(XlrDfp);
                uintptr_t *varAddr = NULL;
                if (!setFieldVar(startAddr,
                                 endAddr,
                                 curRemaining,
                                 &bytesFromStart,
                                 &bytesFromEnd,
                                 fieldBmap,
                                 srcSize,
                                 &varAddr)) {
                    return StatusNoData;  // No space
                }
                memcpy(varAddr, &fieldValue.numericVal, srcSize);
                break;
            }
            case DfString: {
                size_t srcSize = fieldValue.stringVal.strSize;
                uintptr_t *varAddr = NULL;
                if (!setFieldVar(startAddr,
                                 endAddr,
                                 curRemaining,
                                 &bytesFromStart,
                                 &bytesFromEnd,
                                 fieldBmap,
                                 srcSize,
                                 &varAddr)) {
                    return StatusNoData;  // No space
                }
                // Strip out the '\0'. We don't want to waste invaluable
                // memory.
                memcpy(varAddr, fieldValue.stringVal.strActual, srcSize);
                break;
            }
            case DfScalarObj: {
                assert(fieldValue.scalarVal != NULL &&
                       "Valid field cannot have NULL scalarVal");

                size_t srcSize = fieldValue.scalarVal->getUsedSize();
                uintptr_t *varAddr = NULL;
                if (!setFieldVar(startAddr,
                                 endAddr,
                                 curRemaining,
                                 &bytesFromStart,
                                 &bytesFromEnd,
                                 fieldBmap,
                                 srcSize,
                                 &varAddr)) {
                    return StatusNoData;  // No space
                }

                memcpy(varAddr, fieldValue.scalarVal, srcSize);

                // update the fieldBufSize of the packed scalar
                Scalar *scalar = (Scalar *) varAddr;
                scalar->fieldAllocedSize = scalar->fieldUsedSize;
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
        } else {
            if (!setField(curRemaining - bytesFromStart - bytesFromEnd,
                          &bytesFromStart,
                          startAddr + bytesFromStart,
                          fieldBmap,
                          fieldValue.uint64Val)) {
                return StatusNoData;  // No space
            }
        }
        setBmapForField(startAddr, ii, fieldBmap);
#ifdef NewTupleDebug
        assert(getBmapForField(startAddr, ii) == fieldBmap &&
               "fieldBmap mismatch");
#endif  // NewTupleDebug
    }

    numTuples_++;
    bufUsedFromBegin_ += bytesFromStart;
    bufUsedFromEnd_ += bytesFromEnd;
    bufRemaining_ = bufSize_ - bufUsedFromBegin_ - bufUsedFromEnd_;
    assert(bufUsedFromBegin_ + bufUsedFromEnd_ <= bufSize_ &&
           "Used bytes from buffer exceeds bufSize");
    return StatusOk;
}
