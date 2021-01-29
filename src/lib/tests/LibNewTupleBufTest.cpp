// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "util/MemTrack.h"
#include "df/DataFormat.h"
#include "newtupbuf/NewKeyValueTypes.h"
#include "test/QA.h"
#include "common/InitTeardown.h"

static constexpr const size_t TupleBufTestBufSize = 2 * MB;
static char **tupleBufString = NULL;
static char **tupleBufStringBk = NULL;
static constexpr const size_t FixedTuplesFieldsTestIter = 64;
static constexpr const size_t VariableTuplesFieldsTestIter = 64;
static constexpr const size_t FixedTuplesSeekPerIter = 16;
static constexpr const size_t VariableTuplesSeekPerIter = 16;

static size_t numThreads;

struct ArgsPerThread {
    size_t idx;
    pthread_t threadId;
    Status retStatus;
};

static uint64_t UseDefaultSeed = 0;
static uint64_t RandomSeed = UseDefaultSeed;

static uint64_t
getRand64(RandWeakHandle *randHandle)
{
    return rndWeakGenerate64(randHandle);
}

static constexpr const size_t TestAllDefault = 0;
static constexpr const size_t TestAllInvalid = 1;
static constexpr const size_t TestAllValid = 2;
static constexpr const size_t TestAllScalars = 4;
static constexpr const size_t TestAllStrings = 8;
static constexpr const size_t TestAllRandomValues = 16;
static constexpr const size_t TestAllUint64 = 32;
static constexpr const size_t TestMinFields = 64;
static constexpr const size_t TestMaxFields = 128;
static constexpr const size_t TestRemoveFromEnd = 256;

void
invalidateRecord(NewTuplesBuffer *buf,
                 NewTuplesBuffer *bufBk,
                 NewTupleMeta *tupleMeta)
{
    NewTuplesCursor cursor(buf);
    NewTuplesCursor cursorBk(bufBk);
    Status status;
    size_t numFields = tupleMeta->getNumFields();

    do {
        status = cursor.invalidate(tupleMeta, true);
        if (status != StatusOk) {
            break;
        }
        status = cursorBk.invalidate(tupleMeta, true);
        assert(status == StatusOk);
    } while (status == StatusOk);

    new (&cursor) NewTuplesCursor(buf);
    new (&cursorBk) NewTuplesCursor(bufBk);
    do {
        NewKeyValueMeta kvMeta(tupleMeta, -1);
        NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
        NewKeyValueEntry kvEntryBk((const NewKeyValueMeta *) &kvMeta);

        status = cursor.getNext(tupleMeta, &kvEntry.tuple_);
        if (status != StatusOk) {
            break;
        }
        status = cursorBk.getNext(tupleMeta, &kvEntryBk.tuple_);
        assert(status == StatusOk);

        for (unsigned ii = 0; ii < numFields; ii++) {
            DfFieldType typeTmp = tupleMeta->getFieldType(ii);
            bool retIsValid, retIsValidBk;
            DfFieldValue valueTmp =
                kvEntry.tuple_.get(ii, numFields, typeTmp, &retIsValid);
            assert(&valueTmp);
            DfFieldValue valueTmpBk =
                kvEntryBk.tuple_.get(ii, numFields, typeTmp, &retIsValidBk);
            assert(&valueTmpBk);

            assert(retIsValid == retIsValidBk);
            assert(retIsValid == false);
        }
    } while (status == StatusOk);
}

static Status
fixedTupleFieldsTests(size_t idx, size_t testBmap, RandWeakHandle *randHandle)
{
    Status status = StatusOk;
    NewTupleMeta tupleMeta;
    bool invalidated = false;

    if (testBmap & TestMinFields) {
        tupleMeta.setNumFields(1);
    } else if (testBmap & TestMaxFields) {
        tupleMeta.setNumFields(TupleMaxNumValuesPerRecord);
    } else {
        tupleMeta.setNumFields(
            (getRand64(randHandle) % TupleMaxNumValuesPerRecord) + 1);
    }

    size_t numFields = tupleMeta.getNumFields();

    for (unsigned ii = 0; ii < numFields; ii++) {
        if (testBmap & TestAllUint64) {
            tupleMeta.setFieldType(DfInt64, ii);
            continue;
        }
        switch (getRand64(randHandle) % 7) {
        case 0:
            tupleMeta.setFieldType(DfUInt32, ii);
            break;
        case 1:
            tupleMeta.setFieldType(DfUInt64, ii);
            break;
        case 2:
            tupleMeta.setFieldType(DfFloat32, ii);
            break;
        case 3:
            tupleMeta.setFieldType(DfFloat64, ii);
            break;
        case 4:
            tupleMeta.setFieldType(DfInt32, ii);
            break;
        case 5:
            tupleMeta.setFieldType(DfInt64, ii);
            break;
        case 6:
            tupleMeta.setFieldType(DfFatptr, ii);
            break;
        default:
            assert(0);
            break;
        }
    }

    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    NewTuplesBuffer *fTbuf = (NewTuplesBuffer *) tupleBufString[idx];
    new (fTbuf)
        NewTuplesBuffer((void *) tupleBufString[idx], TupleBufTestBufSize);

    NewTuplesBuffer *fTbufBk = (NewTuplesBuffer *) tupleBufStringBk[idx];
    new (fTbufBk)
        NewTuplesBuffer((void *) tupleBufStringBk[idx], TupleBufTestBufSize);

    size_t tupleCount = 0;

    do {
        NewTupleValues *tupleValues = new NewTupleValues();  // let throw

        tupleValues->setInvalid(0, numFields);

        for (unsigned ii = 0; ii < numFields; ii++) {
            DfFieldValue valueTmp;
            bool valid;

            if (testBmap & TestAllInvalid) {
                valid = false;
            } else if (testBmap & TestAllValid) {
                valid = true;
            } else if (getRand64(randHandle) % 2) {
                valid = true;
            } else {
                valid = false;
            }

            if (valid) {
                if (testBmap & TestAllRandomValues) {
                    valueTmp.uint64Val = getRand64(randHandle);
                } else {
                    valueTmp.uint64Val = tupleCount * numFields + ii;
                }
                tupleValues->set(ii, valueTmp, DfUInt64);
            }
        }

        assert(fTbuf->getNumTuples() == tupleCount);

        status = fTbuf->append(&tupleMeta, tupleValues);
        if (status == StatusOk) {
            status = fTbufBk->append(&tupleMeta, tupleValues);
            assert(status == StatusOk);
            tupleCount++;
        }
        delete tupleValues;
    } while (status == StatusOk);

    assert(status == StatusNoData);
    assert(fTbuf->getNumTuples() == tupleCount);
    assert(*fTbuf == *fTbufBk);

    if (testBmap & TestRemoveFromEnd) {
        size_t removeCount = getRand64(randHandle) % tupleCount;
        fTbuf->removeFromEnd(&tupleMeta, removeCount);
        fTbufBk->removeFromEnd(&tupleMeta, removeCount);
        tupleCount -= removeCount;
    }
    assert(*fTbuf == *fTbufBk);

    NewTuplesCursor fCursor(fTbuf);
    NewTuplesCursor fCursorBk(fTbufBk);
    size_t tupleCountCursor = 0;
    NewKeyValueMeta kvMeta((const NewTupleMeta *) &tupleMeta, -1);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewKeyValueEntry kvEntryBk((const NewKeyValueMeta *) &kvMeta);

    if (tupleCount != 0) {
        do {
            if (getRand64(randHandle) % 2) {
                status = fCursor.invalidate(&tupleMeta, true);
                if (status == StatusOk) {
                    status = fCursorBk.invalidate(&tupleMeta, true);
                    assert(status == StatusOk);
                    assert(fCursor == fCursorBk);
                    invalidated = true;
                }
            } else {
                status = fCursor.getNext(&tupleMeta, &kvEntry.tuple_);
                if (status == StatusOk) {
                    status = fCursorBk.getNext(&tupleMeta, &kvEntryBk.tuple_);
                    assert(status == StatusOk);
                    assert(fCursor == fCursorBk);
                    for (unsigned ii = 0; ii < numFields; ii++) {
                        DfFieldType typeTmp = tupleMeta.getFieldType(ii);
                        bool retIsValid, retIsValidBk;
                        DfFieldValue valueTmp = kvEntry.tuple_.get(ii,
                                                                   numFields,
                                                                   typeTmp,
                                                                   &retIsValid);
                        DfFieldValue valueTmpBk =
                            kvEntryBk.tuple_.get(ii,
                                                 numFields,
                                                 typeTmp,
                                                 &retIsValidBk);
                        assert(retIsValid == retIsValidBk);
                        if (!retIsValid) {
                            continue;
                        }

                        assert(valueTmp.uint64Val == valueTmpBk.uint64Val);
                        if (!(testBmap & TestAllRandomValues)) {
                            assert(valueTmp.uint64Val ==
                                   (fCursor.getPosition().nextTupleIdx - 1) *
                                           numFields +
                                       ii);
                        }
                    }
                }
            }
        } while (status == StatusOk);
    }
    assert(*fTbuf == *fTbufBk);

    assert(status == StatusNoData);
    new (&fCursor) NewTuplesCursor(fTbuf);
    new (fTbufBk)
        NewTuplesBuffer((void *) tupleBufStringBk[idx], TupleBufTestBufSize);
    new (&fCursorBk) NewTuplesCursor(fTbufBk);
    do {
        status = fCursor.getNext(&tupleMeta, &kvEntry.tuple_);
        if (status == StatusOk) {
            tupleCountCursor++;
            status = fTbufBk->append(&tupleMeta, &kvEntry.tuple_);
            assert(status == StatusOk);
        }
    } while (status == StatusOk);
    assert(status == StatusNoData);

    assert(tupleCount == tupleCountCursor);

    if (tupleCount != 0) {
        new (&fCursor) NewTuplesCursor(fTbuf);
        new (&fCursorBk) NewTuplesCursor(fTbufBk);
        do {
            status = fCursor.getNext(&tupleMeta, &kvEntry.tuple_);
            if (status != StatusOk) {
                break;
            }
            status = fCursorBk.getNext(&tupleMeta, &kvEntryBk.tuple_);
            assert(status == StatusOk);

            for (unsigned ii = 0; ii < numFields; ii++) {
                DfFieldType typeTmp = tupleMeta.getFieldType(ii);
                bool retIsValid, retIsValidBk;
                DfFieldValue valueTmp =
                    kvEntry.tuple_.get(ii, numFields, typeTmp, &retIsValid);
                DfFieldValue valueTmpBk =
                    kvEntryBk.tuple_.get(ii, numFields, typeTmp, &retIsValidBk);

                assert(retIsValid == retIsValidBk);

                if (!retIsValid) {
                    continue;
                }

                assert(valueTmp.uint64Val == valueTmpBk.uint64Val);

                if (!(testBmap & TestAllRandomValues)) {
                    assert(valueTmp.uint64Val ==
                           (fCursor.getPosition().nextTupleIdx - 1) *
                                   numFields +
                               ii);
                }
            }
        } while (status == StatusOk);

        new (&fCursor) NewTuplesCursor(fTbuf);
        new (&fCursorBk) NewTuplesCursor(fTbufBk);
        NewTuplesCursor::SeekOpt seekOpt;

        NewTuplesCursor::Position pos = fCursor.getPosition();
        NewTuplesCursor::Position posBk = fCursorBk.getPosition();

        for (size_t ii = 0; ii < FixedTuplesSeekPerIter; ii++) {
            int64_t numTuples;

            switch (getRand64(randHandle) % 3) {
            case 0:
                seekOpt = NewTuplesCursor::SeekOpt::Begin;
                numTuples = (getRand64(randHandle) % fTbuf->getNumTuples());
                break;
            case 1:
                seekOpt = NewTuplesCursor::SeekOpt::Cur;
                if (getRand64(randHandle) % 2) {
                    numTuples = pos.nextTupleIdx - fTbuf->getNumTuples();
                } else {
                    numTuples = fTbuf->getNumTuples() - pos.nextTupleIdx;
                }
                break;
            case 2:
                seekOpt = NewTuplesCursor::SeekOpt::End;
                numTuples = fTbuf->getNumTuples() - pos.nextTupleIdx;
                break;
            default:
                assert(0);
                break;
            }

            status = fCursor.seek(&tupleMeta, numTuples, seekOpt);
            if (seekOpt == NewTuplesCursor::SeekOpt::Begin) {
                assert(status == StatusOk);
            } else {
                assert(status == StatusNoData || status == StatusOk);
            }

            status = fCursorBk.seek(&tupleMeta, numTuples, seekOpt);
            if (seekOpt == NewTuplesCursor::SeekOpt::Begin) {
                assert(status == StatusOk);
            } else {
                assert(status == StatusNoData || status == StatusOk);
            }

            pos = fCursor.getPosition();
            posBk = fCursorBk.getPosition();
            assert(pos.nextTupleIdx == posBk.nextTupleIdx);
            if (!invalidated) {
                assert(pos.nextTupleOffset == posBk.nextTupleOffset);
                assert(pos.bufOffsetFromEnd == posBk.bufOffsetFromEnd);
            }
        }
        invalidateRecord(fTbuf, fTbufBk, &tupleMeta);
    }

    return StatusOk;
}

static void *
fixedTupleFieldsTestsLoop(void *args)
{
    ArgsPerThread *input = (ArgsPerThread *) args;
    uint64_t seed;

    if (RandomSeed == UseDefaultSeed) {
        struct timespec ts;
        int ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        if (ret == 0) {
            seed = clkTimespecToNanos(&ts);
        } else {
            seed = sysGetTid();
        }
    } else {
        seed = RandomSeed;
    }
    RandWeakHandle randHandle;
    rndInitWeakHandle(&randHandle, seed);

    Status status = StatusOk;
    for (size_t ii = 0; ii < FixedTuplesFieldsTestIter; ii++) {
        status = fixedTupleFieldsTests(input->idx,
                                       TestAllDefault | TestRemoveFromEnd,
                                       &randHandle);
        BailIfFailed(status);
    }

    status = fixedTupleFieldsTests(input->idx, TestAllInvalid, &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllInvalid | TestMinFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllInvalid | TestMaxFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllInvalid | TestAllUint64,
                                   &randHandle);
    BailIfFailed(status);

    status =
        fixedTupleFieldsTests(input->idx,
                              TestAllInvalid | TestAllUint64 | TestMinFields,
                              &randHandle);
    BailIfFailed(status);

    status =
        fixedTupleFieldsTests(input->idx,
                              TestAllInvalid | TestAllUint64 | TestMaxFields,
                              &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx, TestAllValid, &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestMinFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestMaxFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestAllUint64 | TestMinFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestAllUint64 | TestMaxFields,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestAllUint64 |
                                       TestMinFields | TestAllRandomValues,
                                   &randHandle);
    BailIfFailed(status);

    status = fixedTupleFieldsTests(input->idx,
                                   TestAllValid | TestAllUint64 |
                                       TestMaxFields | TestAllRandomValues,
                                   &randHandle);
    BailIfFailed(status);

CommonExit:
    input->retStatus = status;
    return NULL;
}

const char *randomStrings[] = {
    "hello world",
    "what time is it?",
    "we hold these truths to be self-evident",
    "space.... the final frontier",
    "that's one small step for man... one giant leap for mankind",
    "juan es muy guapo",
    "are you talkin' to me?",
    "And I will strike down upon thee with great vengeance",
};

static constexpr const size_t NumScalarObjects = 16;
static constexpr const size_t MaxScalarObjectSize = 128;
static Scalar *randomScalarArray[NumScalarObjects];

static Status
variableTupleFieldsTests(size_t idx,
                         size_t testBmap,
                         RandWeakHandle *randHandle)
{
    Status status = StatusOk;
    NewTupleMeta tupleMeta;
    bool invalidated = false;

    if (testBmap & TestMinFields) {
        tupleMeta.setNumFields(1);
    } else if (testBmap & TestMaxFields) {
        tupleMeta.setNumFields(TupleMaxNumValuesPerRecord);
    } else {
        tupleMeta.setNumFields(
            (getRand64(randHandle) % TupleMaxNumValuesPerRecord) + 1);
    }

    size_t numFields = tupleMeta.getNumFields();

    for (unsigned ii = 0; ii < numFields; ii++) {
        if (testBmap & TestAllUint64) {
            tupleMeta.setFieldType(DfInt64, ii);
            continue;
        } else if (testBmap & TestAllScalars) {
            tupleMeta.setFieldType(DfScalarObj, ii);
            continue;
        } else if (testBmap & TestAllStrings) {
            tupleMeta.setFieldType(DfString, ii);
            continue;
        }

        switch (getRand64(randHandle) % 10) {
        case 0:
            tupleMeta.setFieldType(DfUInt32, ii);
            break;
        case 1:
            tupleMeta.setFieldType(DfUInt64, ii);
            break;
        case 2:
            tupleMeta.setFieldType(DfFloat32, ii);
            break;
        case 3:
            tupleMeta.setFieldType(DfFloat64, ii);
            break;
        case 4:
            tupleMeta.setFieldType(DfInt32, ii);
            break;
        case 5:
            tupleMeta.setFieldType(DfInt64, ii);
            break;
        case 6:
            tupleMeta.setFieldType(DfFatptr, ii);
            break;
        case 7:
            tupleMeta.setFieldType(DfString, ii);
            break;
        case 8:
            tupleMeta.setFieldType(DfScalarObj, ii);
            break;
        case 9:
            tupleMeta.setFieldType(DfUnknown, ii);
            break;
        default:
            assert(0);
            break;
        }
    }

    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    NewTuplesBuffer *vTbuf = (NewTuplesBuffer *) tupleBufString[idx];
    new (vTbuf)
        NewTuplesBuffer((void *) tupleBufString[idx], TupleBufTestBufSize);

    NewTuplesBuffer *vTbufBk = (NewTuplesBuffer *) tupleBufStringBk[idx];
    new (vTbufBk)
        NewTuplesBuffer((void *) tupleBufStringBk[idx], TupleBufTestBufSize);

    size_t tupleCount = 0;

    do {
        // let throw
        NewTupleValues *tupleValues = new (std::nothrow) NewTupleValues();

        tupleValues->setInvalid(0, numFields);

        for (unsigned ii = 0; ii < numFields; ii++) {
            DfFieldType typeTmp = tupleMeta.getFieldType(ii);
            bool valid;
            if (testBmap & TestAllInvalid) {
                valid = false;
            } else if (testBmap & TestAllValid) {
                valid = true;
            } else if (getRand64(randHandle) % 2) {
                valid = true;
            } else {
                valid = false;
            }

            DfFieldValue valueTmp;
            if (DataFormat::fieldTypeIsFixed(typeTmp) == false) {
                switch (typeTmp) {
                case DfString: {
                    if (valid) {
                        char *str =
                            (char *) randomStrings[getRand64(randHandle) %
                                                   ArrayLen(randomStrings)];
                        valueTmp.stringVal.strSize = strlen(str);
                        valueTmp.stringVal.strActual = str;
                    }
                    break;
                }
                case DfScalarObj: {
                    if (valid) {
                        valueTmp.scalarVal =
                            randomScalarArray[getRand64(randHandle) %
                                              NumScalarObjects];
                    }
                    break;
                }
                case DfUnknown: {
                    valid = false;
                    break;
                }
                default:
                    assert(0);
                    break;
                }
                if (valid) {
                    tupleValues->set(ii, valueTmp, typeTmp);
                }
            } else {
                if (valid) {
                    if ((testBmap & TestAllUint64) &&
                        !(testBmap & TestAllRandomValues)) {
                        valueTmp.uint64Val = tupleCount * numFields + ii;
                    } else {
                        valueTmp.uint64Val = getRand64(randHandle);
                    }
                    tupleValues->set(ii, valueTmp, DfUInt64);
                }
            }
        }

        assert(vTbuf->getNumTuples() == tupleCount);

        status = vTbuf->append(&tupleMeta, tupleValues);
        if (status == StatusOk) {
            status = vTbufBk->append(&tupleMeta, tupleValues);
            assert(status == StatusOk);
            tupleCount++;
        }
        delete tupleValues;
    } while (status == StatusOk);

    assert(status == StatusNoData);
    assert(vTbuf->getNumTuples() == tupleCount);
    assert(*vTbuf == *vTbufBk);

    if (testBmap & TestRemoveFromEnd) {
        size_t removeCount = getRand64(randHandle) % tupleCount;
        vTbuf->removeFromEnd(&tupleMeta, removeCount);
        vTbufBk->removeFromEnd(&tupleMeta, removeCount);
        tupleCount -= removeCount;
    }
    assert(*vTbuf == *vTbufBk);

    NewTuplesCursor vCursor(vTbuf);
    NewTuplesCursor vCursorBk(vTbufBk);
    NewKeyValueMeta kvMeta((const NewTupleMeta *) &tupleMeta, -1);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    NewKeyValueEntry kvEntryBk((const NewKeyValueMeta *) &kvMeta);

    if (tupleCount != 0) {
        do {
            if (getRand64(randHandle) % 2) {
                status = vCursor.invalidate(&tupleMeta, true);
                if (status == StatusOk) {
                    status = vCursorBk.invalidate(&tupleMeta, true);
                    assert(status == StatusOk);
                    assert(vCursor == vCursorBk);
                    invalidated = true;
                }
            } else {
                status = vCursor.getNext(&tupleMeta, &kvEntry.tuple_);
                if (status == StatusOk) {
                    status = vCursorBk.getNext(&tupleMeta, &kvEntryBk.tuple_);
                    assert(status == StatusOk);
                    assert(vCursor == vCursorBk);
                    for (unsigned ii = 0; ii < numFields; ii++) {
                        DfFieldType typeTmp = tupleMeta.getFieldType(ii);
                        bool retIsValid, retIsValidBk;
                        DfFieldValue valueTmp = kvEntry.tuple_.get(ii,
                                                                   numFields,
                                                                   typeTmp,
                                                                   &retIsValid);
                        DfFieldValue valueTmpBk =
                            kvEntryBk.tuple_.get(ii,
                                                 numFields,
                                                 typeTmp,
                                                 &retIsValidBk);
                        assert(retIsValid == retIsValidBk);
                        if (!retIsValid) {
                            continue;
                        }

                        if ((DataFormat::fieldTypeIsFixed(typeTmp) == false)) {
                            switch (typeTmp) {
                            case DfString: {
                                assert(memcmp(valueTmp.stringVal.strActual,
                                              valueTmpBk.stringVal.strActual,
                                              valueTmp.stringVal.strSize) == 0);
                                break;
                            }
                            case DfScalarObj: {
                                assert(
                                    memcmp(valueTmp.scalarVal,
                                           valueTmpBk.scalarVal,
                                           valueTmp.scalarVal->getUsedSize()) ==
                                    0);
                                break;
                            }
                            default:
                                assert(0);
                                break;
                            }
                        } else {
                            assert(valueTmp.uint64Val == valueTmpBk.uint64Val);

                            if ((testBmap & TestAllUint64) &&
                                !(testBmap & TestAllRandomValues)) {
                                assert(valueTmp.uint64Val ==
                                       (vCursor.getPosition().nextTupleIdx -
                                        1) * numFields +
                                           ii);
                            }
                        }
                    }
                }
            }
        } while (status == StatusOk);
    }
    assert(status == StatusNoData);

    size_t tupleCountCursor = 0;
    new (&vCursor) NewTuplesCursor(vTbuf);
    new (vTbufBk)
        NewTuplesBuffer((void *) tupleBufStringBk[idx], TupleBufTestBufSize);
    new (&vCursorBk) NewTuplesCursor(vTbufBk);
    do {
        status = vCursor.getNext(&tupleMeta, &kvEntry.tuple_);
        if (status == StatusOk) {
            tupleCountCursor++;
            status = vTbufBk->append(&tupleMeta, &kvEntry.tuple_);
            assert(status == StatusOk);
        }
    } while (status == StatusOk);
    assert(status == StatusNoData);

    assert(tupleCount == tupleCountCursor);

    if (tupleCount != 0) {
        new (&vCursor) NewTuplesCursor(vTbuf);
        new (&vCursorBk) NewTuplesCursor(vTbufBk);
        do {
            status = vCursor.getNext(&tupleMeta, &kvEntry.tuple_);
            if (status != StatusOk) {
                break;
            }
            status = vCursorBk.getNext(&tupleMeta, &kvEntryBk.tuple_);
            assert(status == StatusOk);

            for (unsigned ii = 0; ii < numFields; ii++) {
                DfFieldType typeTmp = tupleMeta.getFieldType(ii);
                bool retIsValid, retIsValidBk;
                DfFieldValue valueTmp =
                    kvEntry.tuple_.get(ii, numFields, typeTmp, &retIsValid);
                DfFieldValue valueTmpBk =
                    kvEntryBk.tuple_.get(ii, numFields, typeTmp, &retIsValidBk);

                assert(retIsValid == retIsValidBk);

                if (!retIsValid) {
                    continue;
                }

                if ((DataFormat::fieldTypeIsFixed(typeTmp) == false)) {
                    switch (typeTmp) {
                    case DfString: {
                        assert(memcmp(valueTmp.stringVal.strActual,
                                      valueTmpBk.stringVal.strActual,
                                      valueTmp.stringVal.strSize) == 0);
                        break;
                    }
                    case DfScalarObj: {
                        assert(memcmp(valueTmp.scalarVal,
                                      valueTmpBk.scalarVal,
                                      valueTmp.scalarVal->getUsedSize()) == 0);
                        break;
                    }
                    default:
                        assert(0);
                        break;
                    }
                } else {
                    assert(valueTmp.uint64Val == valueTmpBk.uint64Val);

                    if ((testBmap & TestAllUint64) &&
                        !(testBmap & TestAllRandomValues)) {
                        assert(valueTmp.uint64Val ==
                               (vCursor.getPosition().nextTupleIdx - 1) *
                                       numFields +
                                   ii);
                    }
                }
            }
        } while (status == StatusOk);

        new (&vCursor) NewTuplesCursor(vTbuf);
        new (&vCursorBk) NewTuplesCursor(vTbufBk);
        NewTuplesCursor::SeekOpt seekOpt;

        NewTuplesCursor::Position pos = vCursor.getPosition();
        NewTuplesCursor::Position posBk = vCursorBk.getPosition();

        for (size_t ii = 0; ii < VariableTuplesSeekPerIter; ii++) {
            int64_t numTuples;

            switch (getRand64(randHandle) % 3) {
            case 0:
                seekOpt = NewTuplesCursor::SeekOpt::Begin;
                numTuples = (getRand64(randHandle) % vTbuf->getNumTuples());
                break;
            case 1:
                seekOpt = NewTuplesCursor::SeekOpt::Cur;
                if (getRand64(randHandle) % 2) {
                    numTuples = pos.nextTupleIdx - vTbuf->getNumTuples();
                } else {
                    numTuples = vTbuf->getNumTuples() - pos.nextTupleIdx;
                }
                break;
            case 2:
                seekOpt = NewTuplesCursor::SeekOpt::End;
                numTuples = vTbuf->getNumTuples() - pos.nextTupleIdx;
                break;
            default:
                assert(0);
                break;
            }

            status = vCursor.seek(&tupleMeta, numTuples, seekOpt);
            if (seekOpt == NewTuplesCursor::SeekOpt::Begin) {
                assert(status == StatusOk);
            } else {
                assert(status == StatusNoData || status == StatusOk);
            }
            status = vCursorBk.seek(&tupleMeta, numTuples, seekOpt);
            if (seekOpt == NewTuplesCursor::SeekOpt::Begin) {
                assert(status == StatusOk);
            } else {
                assert(status == StatusNoData || status == StatusOk);
            }

            pos = vCursor.getPosition();
            posBk = vCursorBk.getPosition();
            assert(pos.nextTupleIdx == posBk.nextTupleIdx);
            if (!invalidated) {
                assert(pos.nextTupleOffset == posBk.nextTupleOffset);
                assert(pos.bufOffsetFromEnd == posBk.bufOffsetFromEnd);
            }
        }
        invalidateRecord(vTbuf, vTbufBk, &tupleMeta);
    }

    return StatusOk;
}

static void *
variableTupleFieldsTestsLoop(void *args)
{
    uint64_t seed;

    if (RandomSeed == UseDefaultSeed) {
        struct timespec ts;
        int ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        if (ret == 0) {
            seed = clkTimespecToNanos(&ts);
        } else {
            seed = sysGetTid();
        }
    } else {
        seed = RandomSeed;
    }
    RandWeakHandle randHandle;
    rndInitWeakHandle(&randHandle, seed);

    ArgsPerThread *input = (ArgsPerThread *) args;
    Status status = StatusOk;
    for (size_t ii = 0; ii < VariableTuplesFieldsTestIter; ii++) {
        status = variableTupleFieldsTests(input->idx,
                                          TestAllDefault | TestRemoveFromEnd,
                                          &randHandle);
        BailIfFailed(status);
    }

    status = variableTupleFieldsTests(input->idx, TestAllInvalid, &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestMinFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestMaxFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllScalars,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllScalars |
                                          TestMinFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllScalars |
                                          TestMaxFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllStrings,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllStrings |
                                          TestMinFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllStrings |
                                          TestMaxFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllInvalid | TestAllUint64,
                                      &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllInvalid | TestAllUint64 | TestMinFields,
                                 &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllInvalid | TestAllUint64 | TestMaxFields,
                                 &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx, TestAllValid, &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestMinFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestMaxFields,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestAllScalars,
                                      &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllScalars | TestMinFields,
                                 &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllScalars | TestMaxFields,
                                 &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestAllStrings,
                                      &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllStrings | TestMinFields,
                                 &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllStrings | TestMaxFields,
                                 &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllUint64 | TestMinFields,
                                 &randHandle);
    BailIfFailed(status);

    status =
        variableTupleFieldsTests(input->idx,
                                 TestAllValid | TestAllUint64 | TestMaxFields,
                                 &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestAllUint64 |
                                          TestMinFields | TestAllRandomValues,
                                      &randHandle);
    BailIfFailed(status);

    status = variableTupleFieldsTests(input->idx,
                                      TestAllValid | TestAllUint64 |
                                          TestMaxFields | TestAllRandomValues,
                                      &randHandle);
    BailIfFailed(status);

CommonExit:
    input->retStatus = status;
    return NULL;
}

static Status
fixedTupleTests()
{
    ArgsPerThread args[numThreads];
    Status status = StatusOk;

    for (size_t ii = 0; ii < numThreads; ii++) {
        args[ii].idx = ii;
        verifyOk(
            Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                  NULL,
                                                  fixedTupleFieldsTestsLoop,
                                                  &args[ii]));
    }

    for (size_t ii = 0; ii < numThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    return status;
}

static Status
variableTupleTests()
{
    ArgsPerThread args[numThreads];
    Status status = StatusOk;

    for (size_t ii = 0; ii < numThreads; ii++) {
        args[ii].idx = ii;
        verifyOk(
            Runtime::get()->createBlockableThread(&args[ii].threadId,
                                                  NULL,
                                                  variableTupleFieldsTestsLoop,
                                                  &args[ii]));
    }

    for (size_t ii = 0; ii < numThreads; ii++) {
        int ret = sysThreadJoin(args[ii].threadId, NULL);
        verify(ret == 0);
        if (args[ii].retStatus != StatusOk && status == StatusOk) {
            status = args[ii].retStatus;
        }
    }

    return status;
}

static Status
fixedTupleBenchmark()
{
    Status status = StatusOk;
    NewTupleValues *tupleValues = new NewTupleValues();  // let throw
    static constexpr const size_t BenchmarkLoop = 64;

    NewTupleMeta tupleMeta;
    tupleMeta.setNumFields(1);
    size_t numFields = tupleMeta.getNumFields();
    for (unsigned ii = 0; ii < numFields; ii++) {
        tupleMeta.setFieldType(DfUInt64, ii);
    }
    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Fixed);

    Stopwatch stopwatch;
    unsigned long hours, minutes, seconds, millis;
    unsigned long duration;
    uint64_t recordCount = 0;
    static constexpr const uint64_t MagicValue = 0x1234567890;

    for (size_t kk = 0; kk < BenchmarkLoop; kk++) {
        for (size_t ii = 0; ii < numThreads; ii++) {
            NewTuplesBuffer *fTbuf = (NewTuplesBuffer *) tupleBufString[ii];
            new (fTbuf) NewTuplesBuffer((void *) tupleBufString[ii],
                                        TupleBufTestBufSize);
            tupleValues->setInvalid(0, numFields);
            do {
                for (size_t jj = 0; jj < numFields; jj++) {
                    DfFieldValue valueTmp;
                    valueTmp.uint64Val = MagicValue;
                    tupleValues->set(jj, valueTmp, tupleMeta.getFieldType(jj));
                }
                status = fTbuf->append(&tupleMeta, tupleValues);
                if (status == StatusOk) {
                    recordCount++;
                }
            } while (status == StatusOk);
        }
    }
    stopwatch.stop();
    stopwatch.getPrintableTime(hours, minutes, seconds, millis);
    duration = (stopwatch.getElapsedNSecs() / NSecsPerMSec);
    fprintf(stderr,
            "fixedTupleBenchmark: Wrote %lu records\n"
            "  Total duration: %02lu:%02lu.%03lu\n"
            "  Records / msec : %lu\n"
            "  Bytes / msec: %lu\n",
            recordCount,
            minutes,
            seconds,
            millis,
            (recordCount / duration),
            ((TupleBufTestBufSize * numThreads * BenchmarkLoop) / duration));

    NewKeyValueMeta kvMeta((const NewTupleMeta *) &tupleMeta, -1);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    stopwatch.restart();
    for (size_t kk = 0; kk < BenchmarkLoop; kk++) {
        for (size_t ii = 0; ii < numThreads; ii++) {
            NewTuplesBuffer *fTbuf = (NewTuplesBuffer *) tupleBufString[ii];
            NewTuplesCursor fCursor(fTbuf);
            do {
                status = fCursor.getNext(&tupleMeta, &kvEntry.tuple_);
                if (status == StatusOk) {
                    for (size_t jj = 0; jj < numFields; jj++) {
                        DfFieldValue valueTmp;
                        bool retIsValid;
                        valueTmp = tupleValues->get(jj,
                                                    numFields,
                                                    tupleMeta.getFieldType(jj),
                                                    &retIsValid);
                        assert(retIsValid);
                        assert(valueTmp.uint64Val == MagicValue);
                    }
                }
            } while (status == StatusOk);
        }
    }
    stopwatch.stop();
    stopwatch.getPrintableTime(hours, minutes, seconds, millis);
    duration = (stopwatch.getElapsedNSecs() / NSecsPerMSec);
    fprintf(stderr,
            "fixedTupleBenchmark: Read %lu records\n"
            "  Total duration: %02lu:%02lu.%03lu\n"
            "  Records / msec : %lu\n"
            "  Bytes / msec: %lu\n",
            recordCount,
            minutes,
            seconds,
            millis,
            (recordCount / duration),
            ((TupleBufTestBufSize * numThreads * BenchmarkLoop) / duration));

    delete tupleValues;
    return StatusOk;
}

static Status
variableTupleBenchmark()
{
    Status status = StatusOk;
    NewTupleValues *tupleValues = new NewTupleValues();  // let throw
    static constexpr const size_t BenchmarkLoop = 64;

    NewTupleMeta tupleMeta;
    tupleMeta.setNumFields(1);
    size_t numFields = tupleMeta.getNumFields();
    for (unsigned ii = 0; ii < numFields; ii++) {
        tupleMeta.setFieldType(DfString, ii);
    }
    tupleMeta.setFieldsPacking(NewTupleMeta::FieldsPacking::Variable);

    Stopwatch stopwatch;
    unsigned long hours, minutes, seconds, millis;
    unsigned long duration;
    uint64_t recordCount = 0;
    for (size_t kk = 0; kk < BenchmarkLoop; kk++) {
        for (size_t ii = 0; ii < numThreads; ii++) {
            NewTuplesBuffer *vTbuf = (NewTuplesBuffer *) tupleBufString[ii];
            new (vTbuf) NewTuplesBuffer((void *) tupleBufString[ii],
                                        TupleBufTestBufSize);
            tupleValues->setInvalid(0, numFields);
            do {
                for (size_t jj = 0; jj < numFields; jj++) {
                    DfFieldValue valueTmp;
                    tupleValues->set(jj, valueTmp, DfUInt64);
                    char *str = (char *) randomStrings[0];
                    valueTmp.stringVal.strSize = strlen(str);
                    valueTmp.stringVal.strActual = str;
                    tupleValues->set(jj, valueTmp, tupleMeta.getFieldType(jj));
                }
                status = vTbuf->append(&tupleMeta, tupleValues);
                if (status == StatusOk) {
                    recordCount++;
                }
            } while (status == StatusOk);
        }
    }
    stopwatch.stop();
    stopwatch.getPrintableTime(hours, minutes, seconds, millis);
    duration = (stopwatch.getElapsedNSecs() / NSecsPerMSec);
    fprintf(stderr,
            "variableTupleBenchmark: Wrote %lu records\n"
            "  Total duration: %02lu:%02lu.%03lu\n"
            "  Records / sec : %lu\n"
            "  Bytes / sec: %lu bytes\n",
            recordCount,
            minutes,
            seconds,
            millis,
            (recordCount / duration),
            ((TupleBufTestBufSize * numThreads * BenchmarkLoop) / duration));

    NewKeyValueMeta kvMeta((const NewTupleMeta *) &tupleMeta, -1);
    NewKeyValueEntry kvEntry((const NewKeyValueMeta *) &kvMeta);
    stopwatch.restart();
    for (size_t kk = 0; kk < BenchmarkLoop; kk++) {
        for (size_t ii = 0; ii < numThreads; ii++) {
            NewTuplesBuffer *vTbuf = (NewTuplesBuffer *) tupleBufString[ii];
            NewTuplesCursor vCursor(vTbuf);
            do {
                status = vCursor.getNext(&tupleMeta, &kvEntry.tuple_);
                if (status == StatusOk) {
                    for (size_t jj = 0; jj < numFields; jj++) {
                        DfFieldValue valueTmp;
                        bool retIsValid;
                        valueTmp = tupleValues->get(jj,
                                                    numFields,
                                                    tupleMeta.getFieldType(jj),
                                                    &retIsValid);
                        assert(retIsValid);
                        assert(!strcmp(valueTmp.stringValTmp,
                                       (char *) randomStrings[0]));
                    }
                }
            } while (status == StatusOk);
        }
    }
    stopwatch.stop();
    stopwatch.getPrintableTime(hours, minutes, seconds, millis);
    duration = (stopwatch.getElapsedNSecs() / NSecsPerMSec);
    fprintf(stderr,
            "variableTupleBenchmark: Read %lu records\n"
            "  Total duration: %02lu:%02lu.%03lu\n"
            "  Records / msec : %lu\n"
            "  Bytes / msec: %lu\n",
            recordCount,
            minutes,
            seconds,
            millis,
            (recordCount / duration),
            ((TupleBufTestBufSize * numThreads * BenchmarkLoop) / duration));

    delete tupleValues;
    return StatusOk;
}

static TestCase testCases[] = {
    {"Fixed tuple fields tests", fixedTupleTests, TestCaseEnable, ""},
    {"Variable tuple fields tests", variableTupleTests, TestCaseEnable, ""},
    {"Fixed tuple fields benchmark", fixedTupleBenchmark, TestCaseEnable, ""},
    {"Variable tuple fields benchmark",
     variableTupleBenchmark,
     TestCaseEnable,
     ""},
};

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime);

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);
    Status status = StatusOk;

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    uint64_t seed;

    if (RandomSeed == UseDefaultSeed) {
        struct timespec ts;
        int ret = clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
        if (ret == 0) {
            seed = clkTimespecToNanos(&ts);
        } else {
            seed = sysGetTid();
        }
    } else {
        seed = RandomSeed;
    }
    RandWeakHandle randHandle;
    rndInitWeakHandle(&randHandle, seed);

    numThreads = XcSysHelper::get()->getNumOnlineCores();
    tupleBufString = (char **) memAlloc(numThreads * sizeof(char *));
    assert(tupleBufString != NULL);
    tupleBufStringBk = (char **) memAlloc(numThreads * sizeof(char *));
    assert(tupleBufStringBk != NULL);

    for (size_t ii = 0; ii < numThreads; ii++) {
        tupleBufString[ii] =
            (char *) memAllocAligned(PageSize, TupleBufTestBufSize + PageSize);
        assert(tupleBufString[ii] != NULL);
        assert(mlock(tupleBufString[ii], TupleBufTestBufSize) == 0);
        assert(mprotect((void *) ((uintptr_t) tupleBufString[ii] +
                                  TupleBufTestBufSize),
                        PageSize,
                        PROT_NONE) == 0);
        tupleBufStringBk[ii] =
            (char *) memAllocAligned(PageSize, TupleBufTestBufSize + PageSize);
        assert(tupleBufStringBk[ii] != NULL);
        assert(mlock(tupleBufStringBk[ii], TupleBufTestBufSize) == 0);
        assert(mprotect((void *) ((uintptr_t) tupleBufStringBk[ii] +
                                  TupleBufTestBufSize),
                        PageSize,
                        PROT_NONE) == 0);
    }

    for (size_t ii = 0; ii < NumScalarObjects; ii++) {
        // Must be large enough to hold the value
        randomScalarArray[ii] = Scalar::allocScalar(
            sizeof(uint64_t) + (getRand64(&randHandle) % MaxScalarObjectSize));
        assert(randomScalarArray[ii] != NULL);
        DfFieldValue val;
        *((uint64_t *) &val) = getRand64(&randHandle);
        status = randomScalarArray[ii]->setValue(val, DfUInt64);
        assert(status == StatusOk);
    }

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    for (size_t ii = 0; ii < NumScalarObjects; ii++) {
        Scalar::freeScalar(randomScalarArray[ii]);
    }

    for (size_t ii = 0; ii < numThreads; ii++) {
        assert(munlock(tupleBufString[ii], TupleBufTestBufSize) == 0);
        memAlignedFree(tupleBufString[ii]);
        assert(munlock(tupleBufStringBk[ii], TupleBufTestBufSize) == 0);
        memAlignedFree(tupleBufStringBk[ii]);
    }

    memFree(tupleBufString);
    tupleBufString = NULL;

    memFree(tupleBufStringBk);
    tupleBufStringBk = NULL;

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
