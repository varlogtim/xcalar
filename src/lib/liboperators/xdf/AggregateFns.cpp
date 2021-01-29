// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// AggregateFns are of the form f: List<(scalar, ..., scalar)> |-> scalar
// In C-Speak, it has the function signature:
// Status f(ScalarIter *scalarIter, Scalar *scalarOut)
//
// As far as possible, don't iterate through scalarIter one-by-one, as this
// can potentially be extremely slow. As far as possible, make extensive use
// of the parallelOpHandler infrastructure.

#include "primitives/Primitives.h"
#include "XdfInt.h"
#include "operators/Xdf.h"
#include "df/DataFormat.h"
#include "AggregateSum.h"
#include "util/DFPUtils.h"

#define emitFnHeaders(fn, type)                                      \
    static Status fn##type##InitLocal(XdfAggregateAccumulators *acc, \
                                      void *broadcastPacket,         \
                                      size_t broadcastPacketSize);   \
    static Status fn##type##Local(XdfAggregateAccumulators *acc,     \
                                  int argc,                          \
                                  Scalar *argv[]);                   \
    static Status fn##type##Global(XdfAggregateAccumulators *dstAcc, \
                                   XdfAggregateAccumulators *srcAcc)

// sum
emitFnHeaders(sum, Int64);
emitFnHeaders(sum, Float64);
emitFnHeaders(sum, Numeric);

static Status countInitLocal(XdfAggregateAccumulators *acc,
                             void *broadcastPacket,
                             size_t broadcastPacketSize);
static Status countLocal(XdfAggregateAccumulators *acc,
                         int argc,
                         Scalar *argv[]);
static Status countGlobal(XdfAggregateAccumulators *dstAcc,
                          XdfAggregateAccumulators *srcAcc);

static Status scalarInitLocal(XdfAggregateAccumulators *acc,
                              void *broadcastPacket,
                              size_t broadcastPacketSize);
static Status overwriteNanScalar(Scalar *dstScalar, Scalar *srcScalar);
static Status maxLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[]);
static Status maxGlobal(XdfAggregateAccumulators *dstAcc,
                        XdfAggregateAccumulators *srcAcc);
static Status maxLocalNumeric(XdfAggregateAccumulators *acc,
                              int argc,
                              Scalar *argv[]);
static Status maxGlobalNumeric(XdfAggregateAccumulators *dstAcc,
                               XdfAggregateAccumulators *srcAcc);
static Status minLocalNumeric(XdfAggregateAccumulators *acc,
                              int argc,
                              Scalar *argv[]);
static Status minGlobalNumeric(XdfAggregateAccumulators *dstAcc,
                               XdfAggregateAccumulators *srcAcc);

static Status minLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[]);
static Status minGlobal(XdfAggregateAccumulators *dstAcc,
                        XdfAggregateAccumulators *srcAcc);

static Status listAggInitLocal(XdfAggregateAccumulators *acc,
                               void *broadcastPacket,
                               size_t broadcastPacketSize);
static Status listAggLocal(XdfAggregateAccumulators *acc,
                           int argc,
                           Scalar *argv[]);
static Status listAggGlobal(XdfAggregateAccumulators *dstAcc,
                            XdfAggregateAccumulators *srcAcc);

// These function pointers are invoked in CommonFns.c
ParallelOpHandlers parallelOpHandlers[] = {
    // sum
    [AggregateSumInt64] =
        {
            .localInitFn = sumInt64InitLocal,
            .localFn = sumInt64Local,
            .globalFn = sumInt64Global,
        },
    [AggregateSumFloat64] =
        {
            .localInitFn = sumFloat64InitLocal,
            .localFn = sumFloat64Local,
            .globalFn = sumFloat64Global,
        },
    [AggregateSumNumeric] =
        {
            .localInitFn = sumNumericInitLocal,
            .localFn = sumNumericLocal,
            .globalFn = sumNumericGlobal,
        },
    // max
    [AggregateMax] =
        {
            .localInitFn = scalarInitLocal,
            .localFn = maxLocal,
            .globalFn = maxGlobal,
        },
    // maxNumeric
    [AggregateMaxNumeric] =
        {
            .localInitFn = scalarInitLocal,
            .localFn = maxLocalNumeric,
            .globalFn = maxGlobalNumeric,
        },
    // min
    [AggregateMin] =
        {
            .localInitFn = scalarInitLocal,
            .localFn = minLocal,
            .globalFn = minGlobal,
        },
    // minNumeric
    [AggregateMinNumeric] =
        {
            .localInitFn = scalarInitLocal,
            .localFn = minLocalNumeric,
            .globalFn = minGlobalNumeric,
        },
    // count
    [AggregateCount] =
        {
            .localInitFn = countInitLocal,
            .localFn = countLocal,
            .globalFn = countGlobal,
        },
    // listAgg
    [AggregateListAgg] =
        {
            .localInitFn = listAggInitLocal,
            .localFn = listAggLocal,
            .globalFn = listAggGlobal,
        },
};

emitSumInitLocal(Int64);
emitSumLocal(int64_t, Int64);
emitSumGlobal(Int64);
emitXdfAggregateSum(Int64);

emitSumInitLocal(Float64);
emitSumLocal(double, Float64);
emitSumGlobal(Float64);
emitXdfAggregateSum(Float64);

emitXdfAggregateSum(Numeric);

uint32_t numParallelOpHandlers = ArrayLen(parallelOpHandlers);

static Status
countInitLocal(XdfAggregateAccumulators *acc,
               void *broadcastPacket,
               size_t broadcastPacketSize)
{
    assert(broadcastPacket == NULL);
    assert(broadcastPacketSize == 0);
    acc->countAcc.numRecords = 0;
    return StatusOk;
}

static Status
countLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    if (argc != 1) {
        return StatusAstWrongNumberOfArgs;
    }

    assert(argc == 1);
    if (xdfIsValidArg(0, DfUnknown, argc, argv)) {
        acc->countAcc.numRecords++;
    }

    return StatusOk;
}

static Status
countGlobal(XdfAggregateAccumulators *dstAcc, XdfAggregateAccumulators *srcAcc)
{
    XdfCountAccumulator *dstCountAcc, *srcCountAcc;

    dstCountAcc = &dstAcc->countAcc;

    srcCountAcc = &srcAcc->countAcc;
    assert(srcAcc->status == StatusOk);

    dstCountAcc->numRecords += srcCountAcc->numRecords;

    return StatusOk;
}

static Status
scalarInitLocal(XdfAggregateAccumulators *acc,
                void *broadcastPacket,
                size_t broadcastPacketSize)
{
    assert(broadcastPacket == NULL);
    assert(broadcastPacketSize == 0);
    acc->scalarAcc.val.fieldUsedSize = 0;
    acc->scalarAcc.val.fieldAllocedSize = DfMaxFieldValueSize;
    acc->scalarAcc.val.fieldType = DfUnknown;

    return StatusOk;
}

static Status
overwriteNanScalar(Scalar *dstScalar, Scalar *srcScalar)
{
    Status status;
    DfFieldValue dstVal;
    DfFieldValue srcVal;

    // Assumes dstScalar is already initialized
    assert(dstScalar->fieldType != DfUnknown);

    status = dstScalar->getValue(&dstVal);
    BailIfFailed(status);
    status = srcScalar->getValue(&srcVal);
    BailIfFailed(status);

    // The idea here is that min/max functions should run only over non-NANs.
    // Note that this contravenes IEEE754, where any comparison involving nan
    // is nan, but is consistent with other databases (eg Postgres, Snow)
    if (dstScalar->fieldType == DfFloat64 && ::isnanl(dstVal.float64Val)) {
        status = dstScalar->copyFrom(srcScalar);
    } else if (dstScalar->fieldType == DfFloat32 &&
               ::isnanf(dstVal.float32Val)) {
        status = dstScalar->copyFrom(srcScalar);
    } else if (dstScalar->fieldType == DfMoney &&
               DFPUtils::get()->xlrDfpIsNan(&dstVal.numericVal)) {
        status = dstScalar->copyFrom(srcScalar);
    }

CommonExit:
    return status;
}

static Status
maxLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    Status status;

    if (argc != 1) {
        return StatusAstWrongNumberOfArgs;
    }

    if (unlikely(acc->scalarAcc.val.fieldType == DfUnknown)) {
        return acc->scalarAcc.val.copyFrom(argv[0]);
    }

    if (unlikely(acc->scalarAcc.val.fieldType != argv[0]->fieldType)) {
        status = argv[0]->convertType(acc->scalarAcc.val.fieldType);
        if (status != StatusOk) {
            return status;
        }
    }

    status = overwriteNanScalar(&acc->scalarAcc.val, argv[0]);
    BailIfFailed(status);

    // Comparison always returns 0 if any NaNs
    if (acc->scalarAcc.val.compare(argv[0]) < 0) {
        status = acc->scalarAcc.val.copyFrom(argv[0]);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

static Status
maxGlobal(XdfAggregateAccumulators *dstAcc, XdfAggregateAccumulators *srcAcc)
{
    Scalar *srcScalar = &srcAcc->scalarAcc.val;
    return maxLocal(dstAcc, 1, &srcScalar);
}

static Status
minLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    Status status;

    if (argc != 1) {
        return StatusAstWrongNumberOfArgs;
    }

    if (unlikely(acc->scalarAcc.val.fieldType == DfUnknown)) {
        return acc->scalarAcc.val.copyFrom(argv[0]);
    }

    if (unlikely(acc->scalarAcc.val.fieldType != argv[0]->fieldType)) {
        status = argv[0]->convertType(acc->scalarAcc.val.fieldType);
        if (status != StatusOk) {
            return status;
        }
    }

    status = overwriteNanScalar(&acc->scalarAcc.val, argv[0]);
    BailIfFailed(status);

    // Comparison always returns 0 if any NaNs
    if (acc->scalarAcc.val.compare(argv[0]) > 0) {
        status = acc->scalarAcc.val.copyFrom(argv[0]);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

static Status
minGlobal(XdfAggregateAccumulators *dstAcc, XdfAggregateAccumulators *srcAcc)
{
    Scalar *srcScalar = &srcAcc->scalarAcc.val;
    return minLocal(dstAcc, 1, &srcScalar);
}

static Status
listAggInitLocal(XdfAggregateAccumulators *acc,
                 void *broadcastPacket,
                 size_t broadcastPacketSize)
{
    assert(broadcastPacket == NULL);
    assert(broadcastPacketSize == 0);
    acc->listAggAcc.index = 0;
    acc->listAggAcc.delimLen = 0;
    return StatusOk;
}

static Status
listAggLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    XdfListAggAccumulator *listAggAcc = &acc->listAggAcc;
    // don't copy null terminating character
    size_t strLen = argv[0]->fieldVals.strVals->strSize - 1;
    if (argc == 1 || argv[1]->fieldVals.strVals->strSize == 0) {
        listAggAcc->delimLen = 0;
    } else {
        listAggAcc->delimLen = argv[1]->fieldVals.strVals->strSize - 1;
    }

    if (listAggAcc->index + strLen + listAggAcc->delimLen <
        sizeof(listAggAcc->buf) - 1) {
        // copy over actual string
        memcpy(&listAggAcc->buf[listAggAcc->index],
               argv[0]->fieldVals.strVals->strActual,
               strLen);
        listAggAcc->index += strLen;

        // copy delim
        if (listAggAcc->delimLen > 0) {
            memcpy(&listAggAcc->buf[listAggAcc->index],
                   argv[1]->fieldVals.strVals->strActual,
                   listAggAcc->delimLen);

            listAggAcc->index += listAggAcc->delimLen;
        }
    } else {
        return StatusOverflow;
    }

    return StatusOk;
}

static Status
listAggGlobal(XdfAggregateAccumulators *dstAcc,
              XdfAggregateAccumulators *srcAcc)
{
    XdfListAggAccumulator *dstListAggAcc, *srcListAggAcc;

    dstListAggAcc = &dstAcc->listAggAcc;

    srcListAggAcc = &srcAcc->listAggAcc;
    assert(srcAcc->status == StatusOk);

    size_t srcLen = srcListAggAcc->index;

    if (dstListAggAcc->index + srcLen > sizeof(dstListAggAcc->buf) - 1) {
        return StatusOverflow;
    } else {
        memcpy(&dstListAggAcc->buf[dstListAggAcc->index],
               srcListAggAcc->buf,
               srcLen);
        dstListAggAcc->index += srcLen;
    }

    return StatusOk;
}

Status
xdfAggregateAverage(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status;

    XdfAggregateAccumulators acc;
    XdfSumAccumulatorFloat64 *sumAcc;

    status = parallelDo(groupIter, AggregateSumFloat64, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        sumAcc = &acc.sumFloat64Acc;
        return xdfMakeFloat64IntoScalar(out,
                                        sumAcc->sum /
                                            (double) sumAcc->numRecords);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }

    NotReached();
}

Status
xdfAggregateAverageNumeric(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status;

    XdfAggregateAccumulators acc;
    XdfSumAccumulatorNumeric *sumAcc;

    status = parallelDo(groupIter, AggregateSumNumeric, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        DFPUtils *dfp = DFPUtils::get();
        XlrDfp numRecords;
        XlrDfp divResult;
        sumAcc = &acc.sumNumericAcc;

        dfp->xlrDfpUInt64ToNumeric(&numRecords, sumAcc->numRecords);
        dfp->xlrDfpDiv(&divResult, &sumAcc->sum, &numRecords);

        return xdfMakeNumericIntoScalar(out, divResult);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }

    NotReached();
}

Status
xdfAggregateCount(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status = StatusUnknown;

    XdfAggregateAccumulators acc;
    XdfCountAccumulator *countAcc;

    status = parallelDo(groupIter, AggregateCount, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        countAcc = &acc.countAcc;
        return xdfMakeInt64IntoScalar(out, countAcc->numRecords);
    } else {
        countAcc = &acc.countAcc;
        return (acc.status == StatusAggregateAccNotInited)
                   ? xdfMakeInt64IntoScalar(out, countAcc->numRecords)
                   : acc.status;
    }

    NotReached();
}

Status
xdfAggregateListAgg(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status = StatusUnknown;

    XdfAggregateAccumulators acc;
    XdfListAggAccumulator *listAggAcc;

    status = parallelDo(groupIter, AggregateListAgg, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        listAggAcc = &acc.listAggAcc;
        // null terminate string
        listAggAcc->buf[listAggAcc->index] = '\0';
        return xdfMakeStringIntoScalar(out, listAggAcc->buf, listAggAcc->index);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }

    NotReached();
}

Status
xdfAggregateMax(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status = StatusUnknown;

    XdfAggregateAccumulators acc;

    status = parallelDo(groupIter, AggregateMax, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        return out->copyFrom(&acc.scalarAcc.val);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }
}

Status
xdfAggregateMin(ScalarGroupIter *groupIter, Scalar *out)
{
    Status status = StatusUnknown;

    XdfAggregateAccumulators acc;

    status = parallelDo(groupIter, AggregateMin, &acc, NULL, 0);

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        return out->copyFrom(&acc.scalarAcc.val);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }
}

static Status
sumNumericInitLocal(XdfAggregateAccumulators *acc,
                    void *broadcastPacket,
                    size_t broadcastPacketSize)
{
    assert(broadcastPacket == NULL);
    assert(broadcastPacketSize == 0);
    DFPUtils::get()->xlrDfpZero(&acc->sumNumericAcc.sum);
    acc->sumNumericAcc.numRecords = 0;

    return StatusOk;
}

static Status
sumNumericLocal(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    Status status;
    XlrDfp in;
    DfFieldValue fieldVal;
    XdfSumAccumulatorNumeric *sumAcc;
    Scalar *scalarIn;
    sumAcc = &acc->sumNumericAcc;

    DFPUtils *dfp = DFPUtils::get();

    assert(argc == 1);
    scalarIn = argv[0];
    status = scalarIn->getValue(&fieldVal);
    if (status != StatusOk) {
        return status;
    }
    status = DataFormat::fieldToNumeric(fieldVal, scalarIn->fieldType, &in);
    if (status != StatusOk) {
        return status;
    }

    dfp->xlrDfpAdd(&acc->sumNumericAcc.sum, &in);
    sumAcc->numRecords++;
    return StatusOk;
}

static Status
sumNumericGlobal(XdfAggregateAccumulators *dstAcc,
                 XdfAggregateAccumulators *srcAcc)
{
    XdfSumAccumulatorNumeric *dstSumAcc, *srcSumAcc;
    DFPUtils *dfp = DFPUtils::get();

    dstSumAcc = &dstAcc->sumNumericAcc;
    srcSumAcc = &srcAcc->sumNumericAcc;
    assert(srcAcc->status == StatusOk);

    dfp->xlrDfpAdd(&dstSumAcc->sum, &srcSumAcc->sum);
    dstSumAcc->numRecords += srcSumAcc->numRecords;
    return StatusOk;
}

static Status
minmaxLocalNumeric(XdfAggregateAccumulators *acc,
                   int argc,
                   Scalar *argv[],
                   bool isMax)
{
    Status status;
    int32_t cmp;

    if (argc != 1) {
        status = StatusAstWrongNumberOfArgs;
        goto CommonExit;
    }

    if (unlikely(acc->scalarAcc.val.fieldType == DfUnknown)) {
        status = acc->scalarAcc.val.copyFrom(argv[0]);
        goto CommonExit;
    }

    if (unlikely(acc->scalarAcc.val.fieldType != argv[0]->fieldType)) {
        status = argv[0]->convertType(acc->scalarAcc.val.fieldType);
        BailIfFailed(status);
    }

    status = overwriteNanScalar(&acc->scalarAcc.val, argv[0]);
    BailIfFailed(status);

    cmp = DFPUtils::get()
              ->xlrDfp64Cmp(&acc->scalarAcc.val.fieldVals.numericVal[0],
                            &argv[0]->fieldVals.numericVal[0]);
    if ((isMax && cmp < 0) || (!isMax && cmp > 0)) {
        status = acc->scalarAcc.val.copyFrom(argv[0]);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

static Status
maxLocalNumeric(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    return minmaxLocalNumeric(acc, argc, argv, true);
}

static Status
minLocalNumeric(XdfAggregateAccumulators *acc, int argc, Scalar *argv[])
{
    return minmaxLocalNumeric(acc, argc, argv, false);
}

static Status
maxGlobalNumeric(XdfAggregateAccumulators *dstAcc,
                 XdfAggregateAccumulators *srcAcc)
{
    Scalar *srcScalar = &srcAcc->scalarAcc.val;
    return maxLocalNumeric(dstAcc, 1, &srcScalar);
}

static Status
minGlobalNumeric(XdfAggregateAccumulators *dstAcc,
                 XdfAggregateAccumulators *srcAcc)
{
    Scalar *srcScalar = &srcAcc->scalarAcc.val;
    return minLocalNumeric(dstAcc, 1, &srcScalar);
}

Status
xdfAggregateMinMaxNumeric(ScalarGroupIter *groupIter, Scalar *out, bool isMax)
{
    Status status = StatusUnknown;

    XdfAggregateAccumulators acc;

    if (isMax) {
        status = parallelDo(groupIter, AggregateMaxNumeric, &acc, __null, 0);
    } else {
        status = parallelDo(groupIter, AggregateMinNumeric, &acc, __null, 0);
    }

    if (status != StatusOk) {
        return status;
    }

    if (acc.status == StatusOk) {
        return out->copyFrom(&acc.scalarAcc.val);
    } else {
        return (acc.status == StatusAggregateAccNotInited)
                   ? StatusAggregateNoSuchField
                   : acc.status;
    }
}

Status
xdfAggregateMaxNumeric(ScalarGroupIter *groupIter, Scalar *out)
{
    return xdfAggregateMinMaxNumeric(groupIter, out, true);
}

Status
xdfAggregateMinNumeric(ScalarGroupIter *groupIter, Scalar *out)
{
    return xdfAggregateMinMaxNumeric(groupIter, out, false);
}
