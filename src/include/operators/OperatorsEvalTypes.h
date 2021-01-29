// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORSEVALTYPES_H
#define _OPERATORSEVALTYPES_H

#include "sys/XLog.h"
#include "df/DataFormat.h"
#include "operators/XcalarEval.h"
#include "util/MemoryPool.h"
#include "util/DFPUtils.h"

class ValAccumulator
{
  public:
    DfFieldType valType_ = DfUnknown;
    virtual ~ValAccumulator(){};

    virtual void init(DfFieldType valType) { valType_ = valType; }

    // add might fail for some accumulators because we're constantly
    // adding values to the accumulator. For e.g. listAgg might fail.
    // However, some accumulator that takes constant space, like sum,
    // won't fail
    virtual Status add(DfFieldValue val, TableCursor *cur) = 0;
    virtual Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) = 0;
    virtual void reset() = 0;
};

class CountAccumulator : public ValAccumulator
{
  public:
    uint64_t count_ = 0;

    Status add(DfFieldValue val, TableCursor *cur) override
    {
        count_++;
        return StatusOk;
    }

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        *typeOut = DfInt64;
        valOut->int64Val = count_;

        return StatusOk;
    }

    void reset() override { count_ = 0; }
};

class SumFloatAccumulator : public ValAccumulator
{
  public:
    bool init_ = false;
    float64_t sumFloat_ = 0;

    Status add(DfFieldValue val, TableCursor *cur) override
    {
        switch (valType_) {
        case DfInt64:
            sumFloat_ += val.int64Val;
            init_ = true;
            break;
        case DfFloat64:
            sumFloat_ += val.float64Val;
            init_ = true;
            break;
        case DfBoolean:
            sumFloat_ += val.boolVal;
            init_ = true;
            break;
        default:
            assert(0);
            break;
        }
        return StatusOk;
    }

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        if (likely(init_)) {
            *typeOut = DfFloat64;
            valOut->float64Val = sumFloat_;

            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }

    void reset() override
    {
        sumFloat_ = 0;
        init_ = false;
    }
};

class SumIntegerAccumulator : public ValAccumulator
{
  public:
    bool init_ = false;
    int64_t sumInt_ = 0;

    Status add(DfFieldValue val, TableCursor *cur) override
    {
        switch (valType_) {
        case DfInt64:
            sumInt_ += val.int64Val;
            init_ = true;
            break;
        case DfBoolean:
            sumInt_ += val.boolVal;
            init_ = true;
            break;
        default:
            assert(0);
            break;
        }
        return StatusOk;
    }

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        if (likely(init_)) {
            *typeOut = DfInt64;
            valOut->int64Val = sumInt_;

            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }

    void reset() override
    {
        sumInt_ = 0;
        init_ = false;
    }
};

class SumNumericAccumulator : public ValAccumulator
{
  public:
    bool init_ = false;
    XlrDfp sumNum_;
    DFPUtils *dfp_;

    SumNumericAccumulator()
    {
        dfp_ = DFPUtils::get();
        dfp_->xlrDfpZero(&sumNum_);
    }

    Status add(DfFieldValue val, TableCursor *cur) override
    {
        if (valType_ == DfMoney) {
            dfp_->xlrDfpAdd(&sumNum_, &val.numericVal);
            init_ = true;
        }
        return StatusOk;
    }

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        if (likely(init_)) {
            *typeOut = DfMoney;
            valOut->numericVal = sumNum_;

            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }

    void reset() override
    {
        dfp_->xlrDfpZero(&sumNum_);
        init_ = false;
    }
};

class AvgAccumulator : public ValAccumulator
{
  public:
    SumFloatAccumulator sumFloatAcc;
    SumNumericAccumulator sumNumAcc;

    CountAccumulator countAcc;

    void init(DfFieldType valType) override
    {
        valType_ = valType;
        sumFloatAcc.init(valType);
        sumNumAcc.init(valType);
        countAcc.init(valType);
    }

    Status add(DfFieldValue val, TableCursor *cur) override
    {
        Status status = StatusUnknown;
        if (valType_ == DfMoney) {
            status = sumNumAcc.add(val, cur);
        } else {
            status = sumFloatAcc.add(val, cur);
        }

        if (status != StatusOk) {
            return status;
        }

        status = countAcc.add(val, cur);
        return status;
    }

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        DFPUtils *dfp = DFPUtils::get();

        if (likely(countAcc.count_ > 0)) {
            *typeOut = DfFloat64;

            if (valType_ == DfMoney) {
                XlrDfp count;
                Status status =
                    dfp->xlrDfpInt64ToNumeric(&count, countAcc.count_);
                if (unlikely(status != StatusOk)) {
                    return status;
                }

                XlrDfp avg;
                dfp->xlrDfpDiv(&avg, &sumNumAcc.sumNum_, &count);

                return dfp->xlrDfpNumericToFloat64(&valOut->float64Val, &avg);
            } else {
                valOut->float64Val = sumFloatAcc.sumFloat_ / countAcc.count_;
            }

            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }

    void reset() override
    {
        if (valType_ == DfMoney) {
            sumNumAcc.reset();
        } else {
            sumFloatAcc.reset();
        }

        countAcc.reset();
    }
};

class AvgNumericAccumulator : public AvgAccumulator
{
    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        DFPUtils *dfp = DFPUtils::get();

        if (likely(countAcc.count_ > 0)) {
            *typeOut = DfMoney;
            assert(valType_ == DfMoney);

            XlrDfp count;
            Status status = dfp->xlrDfpInt64ToNumeric(&count, countAcc.count_);
            if (unlikely(status != StatusOk)) {
                return status;
            }

            dfp->xlrDfpDiv(&valOut->numericVal, &sumNumAcc.sumNum_, &count);
            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }
};

class RowValAccumulator : public ValAccumulator
{
  public:
    DfFieldValue val_;
    // We need to keep an outstanding pageRef to the value
    // we're pointing to
    TableCursor cur_;
    bool valInit_ = false;
    static constexpr const char *moduleName = "RowValAccumulator";

    Status getResult(DfFieldType *typeOut, DfFieldValue *valOut) override
    {
        if (likely(valInit_)) {
            *valOut = val_;
            *typeOut = valType_;
            return StatusOk;
        } else {
            return StatusAggregateAccNotInited;
        }
    }

    void reset() override
    {
        Status status;
        TableCursor dummy;
        valInit_ = false;
        status = dummy.duplicate(&cur_);
        assert(status == StatusOk);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Reset failed: %s",
                    strGetFromStatus(status));
        }
    }

    ~RowValAccumulator() { reset(); }
};

class MaxAccumulator : public RowValAccumulator
{
    Status add(DfFieldValue valIn, TableCursor *cur) override
    {
        Status status = StatusOk;
        if (valInit_) {
            int ret = DataFormat::fieldCompare(valType_, val_, valIn);
            if (ret < 0) {
                val_ = valIn;
                status = cur->duplicate(&cur_);
            }
        } else {
            val_ = valIn;
            valInit_ = true;
            status = cur->duplicate(&cur_);
        }
        return status;
    }
};

class MinAccumulator : public RowValAccumulator
{
    Status add(DfFieldValue valIn, TableCursor *cur) override
    {
        Status status = StatusOk;
        if (valInit_) {
            int ret = DataFormat::fieldCompare(valType_, valIn, val_);
            if (ret < 0) {
                val_ = valIn;
                status = cur->duplicate(&cur_);
            }
        } else {
            val_ = valIn;
            valInit_ = true;
            status = cur->duplicate(&cur_);
        }
        return status;
    }
};

struct GroupEvalContext {
    AccumulatorType accType;
    DfFieldType argType;
    int argIdx = InvalidIdx;
    bool includeNulls = false;
    bool distinct = false;

    int resultIdx = InvalidIdx;

    bool constantValid = false;
    DfFieldValue constantVal;
    Scalar *scalarVal = NULL;

    ~GroupEvalContext()
    {
        if (scalarVal) {
            Scalar::freeScalar(scalarVal);
        }
    }

    static bool isConstant(const char *fieldName)
    {
        return strlen(fieldName) > 0 &&
               (fieldName[0] == '"' || isDigit(fieldName[0]));
    }

    static ValAccumulator *allocAccumulator(AccumulatorType accType);
    static ValAccumulator *allocAccumulator(AccumulatorType accType,
                                            MemoryPool *memPool);

    static DfFieldType getOutputType(AccumulatorType accType,
                                     DfFieldType argType);
    static bool isValidInputType(AccumulatorType accType, DfFieldType argType);
    static AccumulatorType fnNameToAccumulatorType(const char *fnName);

    static bool isValidAst(XcalarEvalAstCommon *ast);

    static Status getArgType(const char *fieldName,
                             const char *srcFields[],
                             NewTupleMeta *tupMeta,
                             DfFieldType &argType);

    Status initArgVal(const char *fieldName,
                      const char *srcFields[],
                      NewTupleMeta *tupMeta);

    bool initFromAst(XcalarEvalClass2Ast *ast,
                     XdbMeta *srcMeta,
                     int resultIdxIn);

    bool initFromAst(XcalarEvalAstCommon *ast,
                     XdbMeta *srcMeta,
                     int resultIdxIn);
};

struct EvalContext {
    bool astInit_ = false;
    XcalarEvalClass1Ast ast_;
    int *argIndices_ = NULL;
    Scalar **scalars_ = NULL;
    int resultIdx_ = InvalidIdx;

    DfFieldType resultType_ = DfUnknown;

    DfFieldValue result_;
    bool resultValid_ = false;

    ~EvalContext();

    Status setupAst(const char *evalString,
                    unsigned numFields,
                    const char **fieldNames,
                    DagTypes::DagId dagId = XidInvalid);

    Status setupAst(const char *evalString,
                    XdbMeta *xdbMeta,
                    DagTypes::DagId dagId = XidInvalid);

    Status setupMultiAst(const char *evalString,
                         XdbMeta *leftMeta,
                         XdbMeta *rightMeta,
                         XcalarApiRenameMap *renameMap,
                         unsigned numRenameEntriesLeft,
                         unsigned numRenameEntriesRight,
                         DagTypes::DagId dagId);

    void setupResultIdx(const char *newFieldName,
                        unsigned numFields,
                        const char *fieldNames[]);

    Status filterAst(NewKeyValueEntry *dstKvEntry,
                     unsigned dstNumFields,
                     bool *result);

    Status filterMultiAst(NewKeyValueEntry *leftKvEntry,
                          unsigned leftNumFields,
                          NewKeyValueEntry *rightKvEntry,
                          bool *result);
};

struct FilterRange {
    const EvalContext *filterCtx_;
    const char *keyName_;
    const char *fnName_;

    DfFieldType valType_;

    bool minValSet_ = false;
    DfFieldValue minVal_;

    bool maxValSet_ = false;
    DfFieldValue maxVal_;

    bool init(EvalContext *filterCtxIn, XdbMeta *xdbMeta);
    Status setupRange();

    bool inRange(DfFieldValue min, DfFieldValue max);
};

#endif
