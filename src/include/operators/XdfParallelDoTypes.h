// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDFPARALLELDOTYPES_H
#define _XDFPARALLELDOTYPES_H

#include "XlrDecimal64.h"

#define emitSumAccumulator(ctype, type) \
    typedef struct {                    \
        ctype sum;                      \
        uint64_t numRecords;            \
    } XdfSumAccumulator##type

emitSumAccumulator(int64_t, Int64);
emitSumAccumulator(double, Float64);
emitSumAccumulator(XlrDfp, Numeric);

struct XdfCountAccumulator {
    uint64_t numRecords;
};

struct XdfListAggAccumulator {
    unsigned index;
    unsigned delimLen;
    char buf[DfMaxFieldValueSize];
};

struct XdfScalarAccumulator {
    Scalar val;
    char buf[DfMaxFieldValueSize];
};

struct XdfAggregateAccumulators {
    Status status;
    union {
        XdfSumAccumulatorInt64 sumInt64Acc;
        XdfSumAccumulatorFloat64 sumFloat64Acc;
        XdfSumAccumulatorNumeric sumNumericAcc;
        XdfScalarAccumulator scalarAcc;
        XdfCountAccumulator countAcc;
        XdfListAggAccumulator listAggAcc;
    };
};

typedef enum {
    // sum
    AggregateSumInt64 = 0,
    AggregateSumFloat64,
    // max
    AggregateMax,
    // min
    AggregateMin,
    // count
    AggregateCount,
    // listAgg
    AggregateListAgg,
    AggregateSumNumeric,
    AggregateMaxNumeric,
    AggregateMinNumeric,
    AggregateCountNumeric,
} XdfAggregateHandlers;

#endif  // _XDFPARALLELDOTYPES_H
