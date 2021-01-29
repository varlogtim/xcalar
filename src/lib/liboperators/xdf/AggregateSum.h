// Copyright 2015 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Use this pattern if you would like to create variants of your same function
// with different outputType. This is C's poor man's version of C++ template

#define emitSumInitLocal(type)                                        \
    static Status sum##type##InitLocal(XdfAggregateAccumulators *acc, \
                                       void *broadcastPacket,         \
                                       size_t broadcastPacketSize)    \
    {                                                                 \
        assert(broadcastPacket == NULL);                              \
        assert(broadcastPacketSize == 0);                             \
        acc->sum##type##Acc.sum = 0;                                  \
        acc->sum##type##Acc.numRecords = 0;                           \
        return StatusOk;                                              \
    }

#define emitSumLocal(ctype, type)                                          \
    static Status sum##type##Local(XdfAggregateAccumulators *acc,          \
                                   int argc,                               \
                                   Scalar *argv[])                         \
    {                                                                      \
        Status status;                                                     \
        ctype in;                                                          \
        DfFieldValue fieldVal;                                             \
        XdfSumAccumulator##type *sumAcc;                                   \
        Scalar *scalarIn;                                                  \
                                                                           \
        sumAcc = &acc->sum##type##Acc;                                     \
                                                                           \
        assert(argc == 1);                                                 \
        scalarIn = argv[0];                                                \
        status = scalarIn->getValue(&fieldVal);                            \
        if (status != StatusOk) {                                          \
            return status;                                                 \
        }                                                                  \
                                                                           \
        status =                                                           \
            DataFormat::fieldTo##type(fieldVal, scalarIn->fieldType, &in); \
        if (status != StatusOk) {                                          \
            return status;                                                 \
        }                                                                  \
                                                                           \
        sumAcc->sum += in;                                                 \
        sumAcc->numRecords++;                                              \
        return StatusOk;                                                   \
    }

#define emitSumGlobal(type)                                           \
    static Status sum##type##Global(XdfAggregateAccumulators *dstAcc, \
                                    XdfAggregateAccumulators *srcAcc) \
    {                                                                 \
        XdfSumAccumulator##type *dstSumAcc, *srcSumAcc;               \
                                                                      \
        dstSumAcc = &dstAcc->sum##type##Acc;                          \
                                                                      \
        srcSumAcc = &srcAcc->sum##type##Acc;                          \
        assert(srcAcc->status == StatusOk);                           \
                                                                      \
        dstSumAcc->sum += srcSumAcc->sum;                             \
        dstSumAcc->numRecords += srcSumAcc->numRecords;               \
        return StatusOk;                                              \
    }

#define emitXdfAggregateSum(type)                                          \
    Status xdfAggregateSum##type(ScalarGroupIter *groupIter, Scalar *out)  \
    {                                                                      \
        XdfAggregateAccumulators acc;                                      \
        XdfSumAccumulator##type *sumAcc;                                   \
        Status status;                                                     \
                                                                           \
        status = parallelDo(groupIter, AggregateSum##type, &acc, NULL, 0); \
        if (status != StatusOk) {                                          \
            return status;                                                 \
        }                                                                  \
                                                                           \
        if (acc.status == StatusOk) {                                      \
            sumAcc = &acc.sum##type##Acc;                                  \
            return xdfMake##type##IntoScalar(out, sumAcc->sum);            \
        } else {                                                           \
            return (acc.status == StatusAggregateAccNotInited)             \
                       ? StatusAggregateNoSuchField                        \
                       : acc.status;                                       \
        }                                                                  \
                                                                           \
        NotReached();                                                      \
    }
