// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdint.h>
#include <assert.h>
#include <float.h>
#include <math.h>

#include "XlrDecimal64.h"

#if defined(XLR_DEC_BITS_64)
#include "decimal64.h"
#elif defined(XLR_DEC_BITS_128)
#include "decimal128.h"
#else
#error "Unset/unknown DFP bitwidth"
#endif

#include "decNumber.h"
#include "util/DFPUtils.h"

#include "primitives/Primitives.h"
#include "strings/String.h"

#define DfpBailIfFailed(ctx)                     \
    do {                                         \
        if (unlikely(ctx.status & DEC_Errors)) { \
            status = StatusDfpError;             \
            goto CommonExit;                     \
        } else {                                 \
            status = StatusOk;                   \
        }                                        \
    } while (false)

DFPUtils *DFPUtils::instance = NULL;

static void
xlrDecNumberRescale(decNumber *res,
                    const decNumber *lhs,
                    const decNumber *rhs,
                    decContext *set)
{
    if (decNumberIsSpecial(lhs)) {
        // Attempting to rescale a special number (eg Inf, -Inf) results in
        // NaN, so return to preserve the special value.
        return;
    }

    decNumberRescale(res, lhs, rhs, set);
}

DFPUtils *
DFPUtils::get()
{
    return instance;
}

Status
DFPUtils::init()
{
    if (instance != NULL) {
        return StatusOk;
    }

    instance = new (std::nothrow) DFPUtils();
    if (instance == NULL) {
        return StatusNoMem;
    }

    return StatusOk;
}

DFPUtils::DFPUtils() : ieeeEncoding(XLR_DFP_ENCODING){};

void
DFPUtils::destroy()
{
    delete instance;
    instance = NULL;
}

int32_t
DFPUtils::getScaleDigits()
{
    return (0 - (int32_t) XcalarConfig::get()->decimalScaleDigits_);
}

void
DFPUtils::xlrDfpContextDefault(decContext *context, int32_t kind)
{
    // For now everything uses DEC_INIT_DECIMAL64 or DEC_INIT_DECIMAL128
    // context.
    //
    // TODO: Intermediate calculations should use higher precision and we
    // should convert from/to dfp during ingress/egress
    // TODO: Column-dependent IEEE encoding (DEC_INIT_DECIMAL64 vs.
    // DEC_INIT_DECIMAL128)
    decContext *ret = decContextDefault(context, kind);
    assert(ret == context);

    // Traps are enabled by default; disable to avoid generating a SIGFPE
    // All errors (eg divide by zero) simply result in a NaN, infinity or zero
    // passed back to the application
    context->traps = 0;
    context->round = DEC_ROUND_HALF_UP;
}

int32_t
DFPUtils::xlrDfp64Cmp(const XlrDfp *d64Num1, const XlrDfp *d64Num2)
{
    decNumber dfpNum1;
    decNumber dfpNum2;
    decNumber dfpNumCmpRes;
    decContext ctx;
    decNumber dfpScale;

    xlrDfpContextDefault(&ctx, ieeeEncoding);

    if (xlrDfpIsNan(d64Num1) && xlrDfpIsNan(d64Num2)) {
        // Consider the values equal if both are NaN (contravenes IEEE754)
        return 0;
    } else if (xlrDfpIsNan(d64Num1)) {
        // Any non-NaN d64Num2 is considered greater than a NaN d64Num1
        // (contravenes IEEE754)
        return 1;
    } else if (xlrDfpIsNan(d64Num2)) {
        // Any non-NaN d64Num1 is considered greater than a NaN d64Num2
        // (contravenes IEEE754)
        return -1;
    }

    decimalToNumber(&d64Num1->dfp, &dfpNum1);
    decimalToNumber(&d64Num2->dfp, &dfpNum2);

    XcalarConfig *xc = XcalarConfig::get();
    if (xc->decimalRescale_) {
        decNumberFromInt32(&dfpScale, getScaleDigits());
        xlrDecNumberRescale(&dfpNum1, &dfpNum1, &dfpScale, &ctx);
        xlrDecNumberRescale(&dfpNum2, &dfpNum2, &dfpScale, &ctx);
    }

    decNumberCompare(&dfpNumCmpRes, &dfpNum1, &dfpNum2, &ctx);
    return decNumberToInt32(&dfpNumCmpRes, &ctx);
}

Status
DFPUtils::xlrDfpInt64ToNumeric(XlrDfp *dst, int64_t src)
{
    Status status;
    char buf[XLR_DFP_STRLEN];
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    status = strSnprintf(buf, sizeof(buf), "%ld", src);
    BailIfFailed(status);

    decNumberFromString(&dfpNum, buf, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);

CommonExit:
    return status;
}

Status
DFPUtils::xlrDfpUInt64ToNumeric(XlrDfp *dst, uint64_t src)
{
    Status status;
    char buf[XLR_DFP_STRLEN];
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    status = strSnprintf(buf, sizeof(buf), "%lu", src);
    BailIfFailed(status);

    decNumberFromString(&dfpNum, buf, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);

CommonExit:
    return status;
}

Status
DFPUtils::xlrNumericFromString(XlrDfp *dst, const char *src)
{
    Status status;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalFromString(&dst->dfp, src, &ctx);
    DfpBailIfFailed(ctx);

CommonExit:
    return status;
}

void
DFPUtils::xlrNumericToString(char *dst, const XlrDfp *src)
{
    xlrNumericToStringInternal(dst, src, XcalarConfig::get()->decimalRescale_);
}

void
DFPUtils::xlrNumericToStringInternal(char *dst, const XlrDfp *src, bool rescale)
{
    decNumber dfpNum;
    decNumber dfpScale;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);
    strcpy(dst, "NaN");

    decimalToNumber(&src->dfp, &dfpNum);
    if (rescale) {
        decNumberFromInt32(&dfpScale, getScaleDigits());
        xlrDecNumberRescale(&dfpNum, &dfpNum, &dfpScale, &ctx);
    }
    decNumberToString(&dfpNum, dst);
}

void
DFPUtils::xlrDfpRound(XlrDfp *dst, const XlrDfp *src, int32_t dp)
{
    decNumber dfpNum;
    decNumber dfpScale;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&src->dfp, &dfpNum);
    decNumberFromInt32(&dfpScale, -dp);
    xlrDecNumberRescale(&dfpNum, &dfpNum, &dfpScale, &ctx);

    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);
}

void
DFPUtils::xlrDfpAbs(XlrDfp *dst, const XlrDfp *src)
{
    decNumber dfpNum;
    decNumber dfpNumAbs;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&src->dfp, &dfpNum);
    decNumberAbs(&dfpNumAbs, &dfpNum, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNumAbs, &ctx);
}

void
DFPUtils::xlrDfpAdd(XlrDfp *dstAcc, const XlrDfp *srcAcc)
{
    decNumber dfpNumDst;
    decNumber dfpNumSrc;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&dstAcc->dfp, &dfpNumDst);
    decimalToNumber(&srcAcc->dfp, &dfpNumSrc);
    decNumberAdd(&dfpNumDst, &dfpNumDst, &dfpNumSrc, &ctx);
    decimalFromNumber(&dstAcc->dfp, &dfpNumDst, &ctx);
}

void
DFPUtils::xlrDfpAdd(XlrDfp *dst, const XlrDfp *addend0, const XlrDfp *addend1)
{
    decNumber dfpNumDst;
    decNumber dfpNumA0;
    decNumber dfpNumA1;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&addend0->dfp, &dfpNumA0);
    decimalToNumber(&addend1->dfp, &dfpNumA1);
    decNumberAdd(&dfpNumDst, &dfpNumA0, &dfpNumA1, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNumDst, &ctx);
}

void
DFPUtils::xlrDfpSub(XlrDfp *dst,
                    const XlrDfp *minuend,
                    const XlrDfp *subtrahend)
{
    decNumber dfpNumMin;
    decNumber dfpNumSub;
    decNumber dfpNumDst;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&minuend->dfp, &dfpNumMin);
    decimalToNumber(&subtrahend->dfp, &dfpNumSub);
    decNumberSubtract(&dfpNumDst, &dfpNumMin, &dfpNumSub, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNumDst, &ctx);
}

void
DFPUtils::xlrDfpMulti(XlrDfp *dst, const XlrDfp *lhs, const XlrDfp *rhs)
{
    decNumber dfpNumDst;
    decNumber dfpNumM1;
    decNumber dfpNumM2;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&lhs->dfp, &dfpNumM1);
    decimalToNumber(&rhs->dfp, &dfpNumM2);
    decNumberMultiply(&dfpNumDst, &dfpNumM1, &dfpNumM2, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNumDst, &ctx);
}

void
DFPUtils::xlrDfpDiv(XlrDfp *dst, const XlrDfp *dividend, const XlrDfp *divisor)
{
    decNumber dfpNumDividend;
    decNumber dfpNumDivisor;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&dividend->dfp, &dfpNumDividend);
    decimalToNumber(&divisor->dfp, &dfpNumDivisor);
    decNumberDivide(&dfpNumDividend, &dfpNumDividend, &dfpNumDivisor, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNumDividend, &ctx);
}

void
DFPUtils::xlrDfpSubMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals)
{
    decNumber dfpNumAccum;
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&srcs[0].dfp, &dfpNumAccum);
    for (size_t ii = 1; ii < numVals; ii++) {
        decimalToNumber(&srcs[ii].dfp, &dfpNum);
        decNumberSubtract(&dfpNumAccum, &dfpNumAccum, &dfpNum, &ctx);
    }

    decimalFromNumber(&dst->dfp, &dfpNumAccum, &ctx);
}

void
DFPUtils::xlrDfpAddMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals)
{
    decNumber dfpNumAccum;
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberZero(&dfpNumAccum);
    for (size_t ii = 0; ii < numVals; ii++) {
        decimalToNumber(&srcs[ii].dfp, &dfpNum);
        decNumberAdd(&dfpNumAccum, &dfpNumAccum, &dfpNum, &ctx);
    }

    decimalFromNumber(&dst->dfp, &dfpNumAccum, &ctx);
}

void
DFPUtils::xlrDfpMultiMulti(XlrDfp *dst, XlrDfp *srcs, size_t numVals)
{
    decNumber dfpNumAccum;
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberFromInt32(&dfpNumAccum, 1);
    for (size_t ii = 0; ii < numVals; ii++) {
        decimalToNumber(&srcs[ii].dfp, &dfpNum);
        decNumberMultiply(&dfpNumAccum, &dfpNumAccum, &dfpNum, &ctx);
    }

    decimalFromNumber(&dst->dfp, &dfpNumAccum, &ctx);
}

void
DFPUtils::xlrDfpZero(XlrDfp *dst)
{
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberZero(&dfpNum);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);
}

void
DFPUtils::xlrDfpNan(XlrDfp *dst)
{
#if defined(XLR_DEC_BITS_64)
    dst->ieee[0] = ((uint64_t) DECIMAL_NaN) << 56;
#elif defined(XLR_DEC_BITS_128)
    dst->ieee[0] = 0;
    dst->ieee[1] = ((uint64_t) DECIMAL_NaN) << 56;
#else
#error "Unset/unknown DFP bitwidth"
#endif
}

void
DFPUtils::xlrDfpUInt32ToNumeric(XlrDfp *dst, uint32_t src)
{
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberFromUInt32(&dfpNum, src);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);
}

void
DFPUtils::xlrDfpInt32ToNumeric(XlrDfp *dst, int32_t src)
{
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberFromInt32(&dfpNum, src);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);
}

int32_t
DFPUtils::xlrDfpNumericToInt32(const XlrDfp *src)
{
    decNumber dfpScale;
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&src->dfp, &dfpNum);
    // Only numbers of scale zero directly convert to ints
    decNumberZero(&dfpScale);
    xlrDecNumberRescale(&dfpNum, &dfpNum, &dfpScale, &ctx);
    return decNumberToInt32(&dfpNum, &ctx);
}

uint32_t
DFPUtils::xlrDfpNumericToUInt32(const XlrDfp *src)
{
    decNumber dfpScale;
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decimalToNumber(&src->dfp, &dfpNum);
    // Only numbers of scale zero directly convert to ints
    decNumberZero(&dfpScale);
    xlrDecNumberRescale(&dfpNum, &dfpNum, &dfpScale, &ctx);
    return decNumberToUInt32(&dfpNum, &ctx);
}

void
DFPUtils::xlrDfpStringToNumeric(XlrDfp *dst, const char *src)
{
    decNumber dfpNum;
    decContext ctx;
    xlrDfpContextDefault(&ctx, ieeeEncoding);

    decNumberFromString(&dfpNum, src, &ctx);
    decimalFromNumber(&dst->dfp, &dfpNum, &ctx);
}

bool
DFPUtils::xlrDfpIsZero(const XlrDfp *src)
{
    decNumber dfpNum;
    decimalToNumber(&src->dfp, &dfpNum);
    return decNumberIsZero(&dfpNum);
}

bool
DFPUtils::xlrDfpIsNan(const XlrDfp *src)
{
    decNumber dfpNum;
    decimalToNumber(&src->dfp, &dfpNum);
    return decNumberIsNaN(&dfpNum);
}

bool
DFPUtils::xlrDfpIsInf(const XlrDfp *src)
{
    decNumber dfpNum;
    decimalToNumber(&src->dfp, &dfpNum);
    return decNumberIsInfinite(&dfpNum);
}

// Warning: converting DFP to Floats is inherently lossy
Status
DFPUtils::xlrDfpNumericToFloat64(float64_t *dst, const XlrDfp *src)
{
    char *endPtr;
    char buf[XLR_DFP_STRLEN];

    xlrNumericToString(buf, src);
    errno = 0;
    *dst = strtod(buf, &endPtr);
    if (errno == ERANGE || endPtr == buf) {
        *dst = NAN;
        return StatusTypeConversionError;
    }

    return StatusOk;
}

// Warning: converting Floats to DFP is inherently lossy
Status
DFPUtils::xlrDfpFloat64ToNumeric(XlrDfp *dst, const float64_t src)
{
    Status status;
    char buf[DFPUtils::ieeeDblStrChars];

    // Convert from scientific notation to avoid huge digit counts for
    // large/small numbers. This string is only used internally.
    status = strSnprintf(buf, sizeof(buf), "%.*e", DBL_DECIMAL_DIG, src);
    BailIfFailed(status);

    status = xlrNumericFromString(dst, buf);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        xlrDfpNan(dst);
    }

    return status;
}
