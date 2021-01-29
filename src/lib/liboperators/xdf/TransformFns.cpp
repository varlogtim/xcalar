// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// TransformFns are of the form f: scalar X ... X scalar |-> scalar
// In C-Speak, it has the function signature:
// Status f(int argc, Scalar argv[], Scalar *out)

#include <cstdlib>
#include <math.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "df/DataFormat.h"
#include "scalars/Scalars.h"
#include "XdfInt.h"
#include "util/MemTrack.h"
#include "operators/Xdf.h"
#include "strings/String.h"
#include "XcalarEvalInt.h"
#include "operators/OperatorsHash.h"
#include "util/Random.h"
#include "xdb/Xdb.h"
#include "util/DFPUtils.h"

static constexpr const size_t IpAddrDefaultNumDigitsPerOctet = 3;
static constexpr const size_t MacAddrDefaultNumDigitsPerOctet = 2;

static Status xdfTransformCast(int argc,
                               Scalar *argv[],
                               DfFieldValue *fieldValOut,
                               DfFieldType outType,
                               size_t fieldValOutStrBufSize);

Status
xdfTransformFloatCmp(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[3];
    assert(argc == 3);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    float64_t epsilon = vals[2];
    float64_t diff = fabs(vals[0] - vals[1]);
    int answer;

    if (diff < epsilon) {
        answer = 0;
    } else if (vals[0] > vals[1]) {
        answer = 1;
    } else {
        answer = -1;
    }

    return xdfMakeInt64IntoScalar(out, answer);
}

Status
xdfTransformAdd(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    double ans = 0;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    for (int ii = 0; ii < argc; ii++) {
        ans += vals[ii];
    }

    return xdfMakeFloat64IntoScalar(out, ans);
}

Status
xdfTransformAddInteger(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    int64_t ans = 0;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    for (int ii = 0; ii < argc; ii++) {
        ans += (int64_t) vals[ii];
    }

    return xdfMakeInt64IntoScalar(out, ans);
}

Status
xdfTransformAddNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    XlrDfp vals[argc];
    XlrDfp dfp64Num;
    DFPUtils *dfp = DFPUtils::get();

    assert(argc >= 2);

    status = xdfGetNumericFromArgv(argc, vals, argv);
    assert(status == StatusOk || status == StatusDfpError);
    BailIfFailed(status);

    dfp->xlrDfpAddMulti(&dfp64Num, vals, argc);

    status = xdfMakeNumericIntoScalar(out, dfp64Num);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTransformSub(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    double ans;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    ans = vals[0];

    for (int ii = 1; ii < argc; ii++) {
        ans -= vals[ii];
    }

    return xdfMakeFloat64IntoScalar(out, ans);
}

Status
xdfTransformSubInteger(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    int64_t ans;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    ans = (int64_t) vals[0];

    for (int ii = 1; ii < argc; ii++) {
        ans -= (int64_t) vals[ii];
    }

    return xdfMakeInt64IntoScalar(out, ans);
}

Status
xdfTransformSubNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    XlrDfp vals[argc];
    XlrDfp dfp64Num;
    DFPUtils *dfp = DFPUtils::get();

    assert(argc >= 2);

    status = xdfGetNumericFromArgv(argc, vals, argv);
    assert(status == StatusOk || status == StatusDfpError);
    BailIfFailed(status);

    dfp->DFPUtils::xlrDfpSubMulti(&dfp64Num, vals, argc);

    return xdfMakeNumericIntoScalar(out, dfp64Num);
CommonExit:
    return status;
}

Status
xdfTransformMult(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    double ans = 1;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    for (int ii = 0; ii < argc; ii++) {
        ans *= vals[ii];
    }

    return xdfMakeFloat64IntoScalar(out, ans);
}

Status
xdfTransformMultInteger(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[argc];
    int64_t ans = 1;
    assert(argc >= 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    for (int ii = 0; ii < argc; ii++) {
        ans *= (int64_t) vals[ii];
    }

    return xdfMakeInt64IntoScalar(out, ans);
}

Status
xdfTransformMultNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    XlrDfp vals[argc];
    XlrDfp dfp64Num;
    DFPUtils *dfp = DFPUtils::get();

    assert(argc >= 2);

    status = xdfGetNumericFromArgv(argc, vals, argv);
    assert(status == StatusOk || status == StatusDfpError);
    BailIfFailed(status);

    dfp->xlrDfpMultiMulti(&dfp64Num, vals, argc);

    return xdfMakeNumericIntoScalar(out, dfp64Num);

CommonExit:
    return status;
}

Status
xdfTransformDiv(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    double vals[2];
    assert(argc == 2);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    if (vals[1] == 0) {
        return StatusXdfDivByZero;
    }

    assert(vals[1] != 0);
    return xdfMakeFloat64IntoScalar(out, vals[0] / vals[1]);
}

Status
xdfTransformDivNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    XlrDfp vals[2];
    XlrDfp ret;
    DFPUtils *dfp = DFPUtils::get();

    assert(argc == 2);

    status = xdfGetNumericFromArgv(argc, vals, argv);
    assert(status == StatusOk || status == StatusDfpError);
    BailIfFailed(status);

    // Div-by-zero is allowed in IEE754-2008 (result is NaN)

    dfp->xlrDfpDiv(&ret, &vals[0], &vals[1]);

    status = xdfMakeNumericIntoScalar(out, ret);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTransformMod(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    assert(argc == 2);
    int64_t vals[2];

    status = xdfGetInt64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    if (vals[1] == 0) {
        return StatusXdfDivByZero;
    }

    return xdfMakeInt64IntoScalar(out, vals[0] % vals[1]);
}

Status
xdfTransformBitLength(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeInt64IntoScalar(out, argv[0]->fieldUsedSize * 8);
}

Status
xdfTransformOctetLength(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeInt64IntoScalar(out, argv[0]->fieldUsedSize);
}

Status
xdfTransformBitOr(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    uint64_t vals[2];
    assert(argc == 2);

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, vals[0] | vals[1]);
}

Status
xdfTransformBitXor(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    uint64_t vals[2];
    assert(argc == 2);

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, vals[0] ^ vals[1]);
}

Status
xdfTransformBitAnd(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    uint64_t vals[2];
    assert(argc == 2);

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, vals[0] & vals[1]);
}

Status
xdfTransformBitLShift(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    uint64_t vals[2];
    assert(argc == 2);

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, vals[0] << vals[1]);
}

Status
xdfTransformBitRShift(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    uint64_t vals[2];
    assert(argc == 2);

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, vals[0] >> vals[1]);
}

Status
xdfTransformColsDefinedBitmap(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out)
{
    assert(argc > 0 && argc < 65);
    uint64_t ans = 0;

    for (int ii = 0; ii < argc; ii++) {
        if (argv[ii] != NULL && argv[ii]->fieldNumValues > 0) {
            ans |= (1 << (argc - ii - 1));
        }
    }

    return xdfMakeInt64IntoScalar(out, ans);
}

Status
xdfTransformBitCount(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    Status status;
    uint64_t count;
    uint64_t vals[1];

    status = xdfGetUIntptrFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    count = __builtin_popcountll(vals[0]);
    return xdfMakeInt64IntoScalar(out, count);
}

// Math.h
Status
xdfTransformPi(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 0);

    return xdfMakeFloat64IntoScalar(out, M_PI);
}

static inline Status
xdfTransformUnaryDoubleFn(int argc,
                          Scalar *argv[],
                          Scalar *out,
                          double (*fnc)(double x))
{
    Status status;
    double vals[1];
    assert(argc == 1);

    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeFloat64IntoScalar(out, fnc(vals[0]));
}

static inline Status
xdfTransformBinaryDoubleFn(int argc,
                           Scalar *argv[],
                           Scalar *out,
                           double (*fnc)(double x, double y))
{
    Status status;
    double vals[2];

    assert(argc == 2);
    status = xdfGetFloat64FromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeFloat64IntoScalar(out, fnc(vals[0], vals[1]));
}

Status
xdfTransformASin(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, asin);
}

Status
xdfTransformATan(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, atan);
}

Status
xdfTransformACos(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, acos);
}

Status
xdfTransformATanh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, atanh);
}

Status
xdfTransformACosh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, acosh);
}

Status
xdfTransformASinh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, asinh);
}

Status
xdfTransformATan2(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 2);
    return xdfTransformBinaryDoubleFn(argc, argv, out, atan2);
}

Status
xdfTransformSin(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, sin);
}

Status
xdfTransformSinh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, sinh);
}

Status
xdfTransformCos(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, cos);
}

Status
xdfTransformCosh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, cosh);
}

Status
xdfTransformTan(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, tan);
}

Status
xdfTransformTanh(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, tanh);
}

Status
xdfTransformLog(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, log);
}

Status
xdfTransformLog2(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, log2);
}

Status
xdfTransformLog10(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, log10);
}

Status
xdfTransformFindMinIdx(void *context, int argc, Scalar *argv[], Scalar *out)
{
    int64_t minValue = INT64_MAX;
    int64_t argVal;
    int64_t minIdx = -1;
    int ii;
    Status status = StatusUnknown;

    for (ii = 0; ii < argc; ii++) {
        if (argv[ii] != NULL) {
            status = xdfGetInt64FromArgv(1, &argVal, &argv[ii]);
            if (status == StatusOk) {
                if (minValue > argVal) {
                    minValue = argVal;
                    minIdx = ii;
                }
            }
        }
    }

    return xdfMakeInt64IntoScalar(out, minIdx);
}

static double
radians(double degrees)
{
    return degrees * (M_PI / 180.0);
}

Status
xdfTransformRadians(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, radians);
}

static double
degrees(double radians)
{
    return radians * (180.0 / M_PI);
}

Status
xdfTransformDegrees(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, degrees);
}

Status
xdfTransformAbs(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    DfFieldValue fieldVal;
    Status status;
    status = argv[0]->getValue(&fieldVal);
    if (status != StatusOk) {
        assert(0);
        return status;
    }

    switch (argv[0]->fieldType) {
    case (DfFloat64):
        if (fieldVal.float64Val < 0.0) {
            return xdfMakeFloat64IntoScalar(out, -1.0 * fieldVal.float64Val);
        } else {
            return xdfMakeFloat64IntoScalar(out, fieldVal.float64Val);
        }
    case (DfFloat32):
        if (fieldVal.float32Val < 0.0) {
            return xdfMakeFloat64IntoScalar(out, -1.0f * fieldVal.float32Val);
        } else {
            return xdfMakeFloat64IntoScalar(out, fieldVal.float32Val);
        }
    case (DfUInt32):
        return xdfMakeFloat64IntoScalar(out, (float64_t)(fieldVal.uint32Val));
    case (DfUInt64):
        if (fieldVal.uint64Val >= INT64_MAX) {
            return StatusOverflow;
        }
        return xdfMakeFloat64IntoScalar(out, (float64_t)(fieldVal.uint64Val));
    case (DfInt32):
        return xdfMakeFloat64IntoScalar(out,
                                        (float64_t)(abs(fieldVal.int32Val)));
    case (DfInt64):
        if (fieldVal.int64Val < 0) {
            return xdfMakeFloat64IntoScalar(out, -1.0 * fieldVal.int64Val);
        } else {
            return xdfMakeFloat64IntoScalar(out, fieldVal.int64Val);
        }
    case (DfMoney): {
        XlrDfp ret;
        DFPUtils::get()->xlrDfpAbs(&ret, &fieldVal.numericVal);
        return xdfMakeNumericIntoScalar(out, ret);
    }
    default:
        return StatusDfFieldTypeUnsupported;
    }
}

Status
xdfTransformAbsInt(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    DfFieldValue fieldVal;
    Status status;
    status = argv[0]->getValue(&fieldVal);
    if (status != StatusOk) {
        assert(0);
        return status;
    }
    if (fieldVal.int64Val < 0) {
        return xdfMakeInt64IntoScalar(out, -1.0 * fieldVal.int64Val);
    } else {
        return xdfMakeInt64IntoScalar(out, fieldVal.int64Val);
    }
}

Status
xdfTransformAbsNumberic(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    Status status;
    XlrDfp ret[argc];
    status = xdfGetNumericFromArgv(argc, ret, argv);
    if (status != StatusOk) {
        assert(0);
        return status;
    }
    XlrDfp res;
    DFPUtils::get()->xlrDfpAbs(&res, &ret[0]);
    return xdfMakeNumericIntoScalar(out, res);
}

Status
xdfTransformCeil(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, ceil);
}

Status
xdfTransformFloor(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, floor);
}

Status
xdfTransformRound(void *context, int argc, Scalar *argv[], Scalar *out)
{
    if (argc == 1) {
        return xdfTransformUnaryDoubleFn(argc, argv, out, round);
    } else {
        assert(argc == 2);

        float64_t vals[argc];

        xdfGetFloat64FromArgv(argc, vals, argv);

        float64_t sigfigs = pow(10, vals[1]);

        float64_t nearest = roundf(vals[0] * sigfigs) / sigfigs;

        return xdfMakeFloat64IntoScalar(out, nearest);
    }
}

Status
xdfTransformRoundNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    XlrDfp nearest;
    XlrDfp val;
    int64_t dp;
    Status status;

    DFPUtils *dfp = DFPUtils::get();
    assert(argc == 2);

    status = xdfGetNumericFromArgv(1, &val, argv);
    BailIfFailed(status);
    status = xdfGetInt64FromArgv(1, &dp, &argv[1]);
    BailIfFailed(status);
    dfp->xlrDfpRound(&nearest, &val, (int32_t) dp);

    status = xdfMakeNumericIntoScalar(out, nearest);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTransformPow(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 2);
    return xdfTransformBinaryDoubleFn(argc, argv, out, pow);
}

Status
xdfTransformExp(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, exp);
}

Status
xdfTransformSqrt(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfTransformUnaryDoubleFn(argc, argv, out, sqrt);
}

Status
xdfTransformGenUnique(void *context, int argc, Scalar *argv[], Scalar *out)
{
    static Atomic64 count = {.val = 0};

    int64_t num = Config::get()->getMyNodeId() * 1000000000000000ULL;
    num += atomicInc64(&count);

    return xdfMakeInt64IntoScalar(out, num);
}

Status
xdfTransformGenRandom(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusUnknown;
    int64_t start, end, randVal;
    uint64_t range;
    RandWeakHandle *xcalarEvalRandWeakHandle = (RandWeakHandle *) context;
    DCHECK(xcalarEvalRandWeakHandle != NULL);

    assert(argc == 2);
    if (argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        start = INT64_MIN;
    } else {
        status = xdfGetInt64FromArgv(1, &start, &argv[0]);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
        end = INT64_MAX;
    } else {
        status = xdfGetInt64FromArgv(1, &end, &argv[1]);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (end < start) {
        status = StatusInval;
        goto CommonExit;
    }

    range = end - start + 1;

    randVal = (int64_t)((rndWeakGenerate64(xcalarEvalRandWeakHandle)) % range) +
              start;
CommonExit:
    if (status != StatusOk) {
        return status;
    }

    return xdfMakeInt64IntoScalar(out, randVal);
}

Status
xdfTransformDhtHash(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;
    uint64_t hashVal;

    // fake an unordered DHT
    Dht dht;
    dht.ordering = Unordered;

    if (argv[0]->fieldType == DfString) {
        status = argv[0]->getValue(&fieldValOut);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }

        hashVal = hashStringFast(fieldValOut.stringVal.strActual);
    } else {
        status = xdfTransformCast(argc, argv, &fieldValOut, DfUInt64, 0);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }

        float64_t in = (float64_t) fieldValOut.uint64Val;
        hashVal = hashCrc32c(0, &in, sizeof(in));
    }

    // mod the value when optional argument is passed
    if (argc > 1) {
        int64_t mod;
        status = xdfGetInt64FromArgv(1, &mod, &argv[1]);
        if (status == StatusOk && mod != 0) {
            hashVal = hashVal % mod;
        }
    }

    return xdfMakeInt64IntoScalar(out, hashVal);
}

Status
xdfTransformXdbHash(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    if (argc < 1 || argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        return StatusEvalUnsubstitutedVariables;
    }

    status = argv[0]->getValue(&fieldValOut);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    int64_t hashVal = XdbMgr::hashXdbUniform(fieldValOut,
                                             argv[0]->fieldType,
                                             XdbMgr::xdbHashSlots);

    return xdfMakeInt64IntoScalar(out, hashVal);
}

Status
xdfTransformAscii(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    assert(argv[0]->fieldType == DfString);

    char c = argv[0]->fieldVals.strVals->strActual[0];

    return xdfMakeInt64IntoScalar(out, (int) c);
}

Status
xdfTransformChr(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    assert(argv[0]->fieldType == DfInt64);

    char c[2];
    c[0] = (char) (*argv[0]->fieldVals.int64Val % 256);
    c[1] = '\0';
    size_t len = 1;
    if (!c[0]) {
        len = 0;
    }
    return xdfMakeStringIntoScalar(out, c, len);
}

Status
xdfTransformFormatNumber(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 2);
    Status status;
    char outString[DfMaxFieldValueSize];

    float64_t val;
    int64_t d;

    status = xdfGetFloat64FromArgv(1, &val, &argv[0]);
    assert(status == StatusOk);

    status = xdfGetInt64FromArgv(1, &d, &argv[1]);
    assert(status == StatusOk);

    int ret = snprintf(outString, sizeof(outString), "%'.*f", (int) d, val);

    return xdfMakeStringIntoScalar(out, outString, ret);
}

Status
xdfTransformStringRPad(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 3);
    Status status;
    char outString[DfMaxFieldValueSize];
    outString[0] = '\0';

    int64_t maxLen;

    char *str = argv[0]->fieldVals.strVals->strActual;
    int64_t strLen = argv[0]->fieldVals.strVals->strSize - 1;

    status = xdfGetInt64FromArgv(1, &maxLen, &argv[1]);
    assert(status == StatusOk);

    if (maxLen <= 0 || maxLen > (int64_t) sizeof(outString)) {
        return StatusInval;
    }

    char *pad = argv[2]->fieldVals.strVals->strActual;
    int64_t padLen = argv[2]->fieldVals.strVals->strSize - 1;

    if (padLen == 0) {
        return StatusInval;
    }

    int totalPadLen = (int) (maxLen - strLen + 1);
    if (totalPadLen < 0) {
        totalPadLen = 0;
    }

    int ret = snprintf(outString, sizeof(outString), "%s", str);
    char *cursor = &outString[ret];

    while (totalPadLen > 0) {
        ret = snprintf(cursor, totalPadLen, "%s", pad);
        cursor += padLen;
        totalPadLen -= ret;
    }

    // truncate string to maxLen
    outString[maxLen] = '\0';

    return xdfMakeStringIntoScalar(out, outString, strlen(outString));
}

Status
xdfTransformStringLPad(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 3);
    Status status;
    char outString[DfMaxFieldValueSize];
    outString[0] = '\0';

    int64_t maxLen;

    char *str = argv[0]->fieldVals.strVals->strActual;
    int64_t strLen = argv[0]->fieldVals.strVals->strSize - 1;

    status = xdfGetInt64FromArgv(1, &maxLen, &argv[1]);
    assert(status == StatusOk);

    if (maxLen <= 0 || maxLen > (int64_t) sizeof(outString)) {
        return StatusInval;
    }

    char *pad = argv[2]->fieldVals.strVals->strActual;
    int64_t padLen = argv[2]->fieldVals.strVals->strSize - 1;

    if (padLen == 0) {
        return StatusInval;
    }

    int totalPadLen = (int) (maxLen - strLen + 1);
    if (totalPadLen < 0) {
        totalPadLen = 0;
    }

    char *cursor = outString;
    int ret;

    while (totalPadLen > 0) {
        ret = snprintf(cursor, totalPadLen, "%s", pad);
        cursor += padLen;
        totalPadLen -= ret;
    }

    ret = strlcat(outString, str, sizeof(outString));
    // truncate string to maxLen
    outString[maxLen] = '\0';

    return xdfMakeStringIntoScalar(out, outString, strlen(outString));
}

Status
xdfTransformInitCap(void *context, int argc, Scalar *argv[], Scalar *out)
{
    char outString[DfMaxFieldValueSize];

    assert(argc == 1);
    assert(argv[0] != NULL);
    assert(argv[0]->fieldType == DfString);

    int ret;
    ret = strlcpy(outString,
                  argv[0]->fieldVals.strVals->strActual,
                  sizeof(outString));

    for (int ii = 0; ii < ret; ii++) {
        if (isalpha(outString[ii])) {
            if (ii == 0 || strIsWhiteSpace(outString[ii - 1])) {
                outString[ii] = toupper(outString[ii]);
            } else {
                outString[ii] = tolower(outString[ii]);
            }
        }
    }

    return xdfMakeStringIntoScalar(out, outString, ret);
}

Status
xdfTransformStringReverse(void *context, int argc, Scalar *argv[], Scalar *out)
{
    char outString[DfMaxFieldValueSize];

    assert(argc == 1);
    assert(argv[0] != NULL);
    assert(argv[0]->fieldType == DfString);

    const char *str = argv[0]->fieldVals.strVals->strActual;
    size_t strSize = argv[0]->fieldVals.strVals->strSize - 1;
    assert(strSize < sizeof(outString));

    for (unsigned ii = 0; ii < strSize; ii++) {
        unsigned jj = strSize - ii - 1;
        outString[jj] = str[ii];
    }

    outString[strSize] = '\0';

    return xdfMakeStringIntoScalar(out, outString, strSize);
}

Status
xdfTransformWordCount(void *context, int argc, Scalar *argv[], Scalar *out)
{
    typedef enum {
        NonWhitespace,
        Whitespace,
    } WordCountState;

    WordCountState currentState;
    DfFieldValue fieldVal;
    Status status;
    const char *sentence;
    unsigned ii;
    int64_t numWords;

    assert(argc == 1);
    assert(argv[0] != NULL);
    assert(argv[0]->fieldType == DfString);

    status = argv[0]->getValue(&fieldVal);
    assert(status == StatusOk);

    sentence = fieldVal.stringVal.strActual;
    numWords = 0;
    currentState = Whitespace;

    for (ii = 0; ii < fieldVal.stringVal.strSize && *sentence != '\0';
         ii++, sentence++) {
        switch (currentState) {
        case Whitespace:
            if (!strIsWhiteSpace(*sentence)) {
                currentState = NonWhitespace;
                numWords++;
            }
            break;
        case NonWhitespace:
            if (strIsWhiteSpace(*sentence)) {
                currentState = Whitespace;
            }
            break;
        default:
            assert(0);
        }
    }

    return xdfMakeInt64IntoScalar(out, numWords);
}

Status
xdfTransformStringLen(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldVal;
    Status status;

    assert(argc == 1);
    assert(argv[0] != NULL);
    assert(argv[0]->fieldType == DfString);

    status = argv[0]->getValue(&fieldVal);
    assert(status == StatusOk);

    // -1 for null terminator
    return xdfMakeInt64IntoScalar(out, fieldVal.stringVal.strSize - 1);
}

Status
xdfTransformCharacterCount(void *context,
                           int argc,
                           Scalar *argvIn[],
                           Scalar *out)
{
    const unsigned CharacterCountNumArgs = 2;
    Status status;
    unsigned ii;
    DfFieldValue argv[CharacterCountNumArgs];
    int64_t numOccur = 0;

    assert((unsigned) argc == CharacterCountNumArgs);
    assert(argvIn[0] != NULL);
    assert(argvIn[1] != NULL);
    assert(argvIn[0]->fieldType == DfString);
    assert(argvIn[1]->fieldType == DfString);

    enum { Str, CharToCount };
    for (ii = 0; ii < CharacterCountNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    if (argv[CharToCount].stringVal.strSize <= 1) {
        return StatusInval;
    }

    const char *cur = argv[Str].stringVal.strActual;
    ii = 0;
    while (ii < argv[Str].stringVal.strSize - 1) {
        const char *ret =
            strstr(cur + ii, argv[CharToCount].stringVal.strActual);
        if (!ret) {
            break;
        }
        ii = (unsigned) (ret - cur) +
             (unsigned) (argv[CharToCount].stringVal.strSize - 1);
        numOccur++;
    }

    return xdfMakeInt64IntoScalar(out, numOccur);
}

Status
xdfTransformConcatDelim(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    int ii;
    char strBuf[DfMaxFieldValueSize] = "";

    assert(argc >= 4);

    DfFieldValue argv[argc];
    const DfFieldValue DfFieldValueNull = {
        .stringVal = {.strActual = NULL, .strSize = 0}};
    size_t ret = 0;

    Status status;
    for (ii = 0; ii < argc; ii++) {
        if (argvIn[ii] == NULL || argvIn[ii]->fieldNumValues == 0 ||
            argvIn[ii]->fieldType == DfNull) {
            argv[ii] = DfFieldValueNull;
            continue;
        }
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *delim = argv[0].stringVal.strActual;
    const char *nullValue = argv[1].stringVal.strActual;
    const bool includeNull = argv[2].boolVal;
    ii = 3;
    char *tmp = NULL;

    do {
        if (argv[ii].stringVal.strActual != NULL) {
            tmp = (char *) argv[ii].stringVal.strActual;
        } else if (includeNull) {
            tmp = (char *) nullValue;
        }
        ii += 1;
    } while (!tmp && ii < argc);

    if (tmp) {
        ret = snprintf(strBuf, sizeof(strBuf), "%s", tmp);
        if (ret >= sizeof(strBuf)) {
            // This allows a more meaningful status string to be pushed upwards
            // customized to the scalar function context
            return StatusScalarFunctionFieldOverflow;
        }
    }

    for (; ii < argc; ii++) {
        if (argv[ii].stringVal.strActual == NULL && !includeNull) continue;
        ret = strlcat(strBuf, delim, sizeof(strBuf));
        if (ret >= sizeof(strBuf)) {
            return StatusScalarFunctionFieldOverflow;
        }
        if (argv[ii].stringVal.strActual != NULL) {
            ret = strlcat(strBuf, argv[ii].stringVal.strActual, sizeof(strBuf));
        } else {
            ret = strlcat(strBuf, nullValue, sizeof(strBuf));
        }
        if (ret >= sizeof(strBuf)) {
            return StatusScalarFunctionFieldOverflow;
        }
    }

    return xdfMakeStringIntoScalar(out, &strBuf[0], ret);
}

Status
xdfTransformStringPosCompare(void *context,
                             int argc,
                             Scalar *argvIn[],
                             Scalar *out)
{
    const unsigned StrPosCompareArgs = 5;
    unsigned ii;

    assert((unsigned) argc == StrPosCompareArgs);

    enum { Str1, Str2, Delim, MinDiff, MaxDiff };
    DfFieldValue argv[StrPosCompareArgs];

    Status status;
    for (ii = 0; ii < StrPosCompareArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *cur1 = argv[Str1].stringVal.strActual;
    const char *cur2 = argv[Str2].stringVal.strActual;
    const char *delim = argv[Delim].stringVal.strActual;
    int64_t minDiff = argv[MinDiff].int64Val;
    int64_t maxDiff = argv[MaxDiff].int64Val;
    if (minDiff >= maxDiff) {
        return StatusInval;
    }
    int64_t diff = 0;
    bool ans = true;

    const char *tmp1 = cur1;
    const char *tmp2 = cur2;
    int64_t len1 = 0;
    int64_t len2 = 0;
    int64_t delimLen = strlen(delim);

    do {
        tmp1 = (char *) strstr(cur1, delim);
        tmp2 = (char *) strstr(cur2, delim);
        if (!tmp1 && !tmp2) {
            // compare from present position to end poistion of both strings
            len1 = strlen(cur1);
            len2 = strlen(cur2);
        } else if (!tmp1 || !tmp2) {
            return StatusInval;  // strings not equally delimied
        } else {
            len1 = tmp1 - cur1;
            len2 = tmp2 - cur2;
        }
        if (len1 != len2 || strncmp(cur1, cur2, len1) != 0) {
            diff += 1;
        }
        cur1 += len1 + delimLen;
        cur2 += len2 + delimLen;
    } while (tmp1 && tmp2 && diff < maxDiff);

    if (diff >= minDiff && diff < maxDiff)
        ans = true;
    else
        ans = false;

    return xdfMakeBoolIntoScalar(out, ans);
}

static Status
xdfTransformCast(int argc,
                 Scalar *argv[],
                 DfFieldValue *fieldValOut,
                 DfFieldType outType,
                 size_t fieldValOutStrBufSize)
{
    DfFieldValue fieldValIn;
    Status status;
    Base inputBase = (Base) DfDefaultConversionBase;

    if (argc < 1 || argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        return StatusEvalUnsubstitutedVariables;
    }

    if (argc >= 2 && argv[1] != NULL) {
        DfFieldValue baseValOrig;
        DfFieldValue baseValConvert;

        status = argv[1]->getValue(&baseValOrig);
        assert(status == StatusOk);
        if (status != StatusOk) {
            return status;
        }

        status = DataFormat::convertValueType(DfInt64,
                                              argv[1]->fieldType,
                                              &baseValOrig,
                                              &baseValConvert,
                                              sizeof(baseValConvert),
                                              (Base) DfDefaultConversionBase);
        if (status != StatusOk) {
            return status;
        }

        inputBase = (Base) baseValConvert.int64Val;
    }

    status = argv[0]->getValue(&fieldValIn);
    assert(status == StatusOk);

    status = DataFormat::convertValueType(outType,
                                          argv[0]->fieldType,
                                          &fieldValIn,
                                          fieldValOut,
                                          fieldValOutStrBufSize,
                                          inputBase);

    return status;
}

Status
xdfTransformTimestamp(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    status = xdfTransformCast(argc, argv, &fieldValOut, DfTimespec, 0);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    return xdfMakeTimestampIntoScalar(out, fieldValOut.timeVal);
}

Status
xdfTransformInt(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    status = xdfTransformCast(argc, argv, &fieldValOut, DfInt64, 0);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    return xdfMakeInt64IntoScalar(out, fieldValOut.int64Val);
}

Status
xdfTransformFloat(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    status = xdfTransformCast(argc, argv, &fieldValOut, DfFloat64, 0);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    return xdfMakeFloat64IntoScalar(out, fieldValOut.float64Val);
}

Status
xdfTransformNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    status = xdfTransformCast(argc, argv, &fieldValOut, DfMoney, 0);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    return xdfMakeNumericIntoScalar(out, fieldValOut.numericVal);
}

Status
xdfTransformBool(void *context, int argc, Scalar *argv[], Scalar *out)
{
    DfFieldValue fieldValOut;
    Status status;

    status = xdfTransformCast(argc, argv, &fieldValOut, DfBoolean, 0);
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    return xdfMakeBoolIntoScalar(out, fieldValOut.boolVal);
}

Status
xdfTransformString(void *context, int argc, Scalar *argv[], Scalar *out)
{
    char buf[DfMaxFieldValueSize];
    DfFieldValue fieldValOut;
    Status status;

    // XXX why was stringValTmp introduced instead of just stringVal?
    fieldValOut.stringValTmp = &buf[0];
    status = xdfTransformCast(argc, argv, &fieldValOut, DfString, sizeof(buf));
    if (status != StatusOk) {
        return StatusEvalCastError;
    }

    if (fieldValOut.stringVal.strSize == 0) {
        return xdfMakeStringIntoScalar(out, fieldValOut.stringVal.strActual, 0);
    } else {
        return xdfMakeStringIntoScalar(out,
                                       fieldValOut.stringVal.strActual,
                                       fieldValOut.stringVal.strSize - 1);
    }
}

Status
xdfTransformFnfToInt(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    int64_t toValue;

    if (argc != 2) {
        return StatusAstWrongNumberOfArgs;
    }

    // First arg must be FNF or number
    if (argv[0] != NULL && argv[0]->fieldNumValues > 0) {
        if (argv[0]->fieldType != DfInt64) {
            return StatusXdfTypeUnsupported;
        }
    }

    // Replacment must be number
    if (argv[1]->fieldType != DfInt64) {
        return StatusXdfTypeUnsupported;
    }

    if (argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        // Use new value
        status = xdfGetInt64FromArgv(1, &toValue, &argv[1]);
    } else {
        // Keep original value
        status = xdfGetInt64FromArgv(1, &toValue, &argv[0]);
    }
    assert(status == StatusOk);

    return xdfMakeInt64IntoScalar(out, toValue);
}

static Status
dateToTime(Scalar *dateIn, Scalar *formatIn, tm *timeOut)
{
    Status status;
    const char *date;
    int dateLen;

    const char *format;
    int formatLen;
    memZero(timeOut, sizeof(*timeOut));

    status = xdfGetStringFromArgv(1, &date, &dateLen, &dateIn);
    assert(status == StatusOk);

    status = xdfGetStringFromArgv(1, &format, &formatLen, &formatIn);
    assert(status == StatusOk);
    if (formatLen == 0) {
        return StatusUnderflow;
    }

    // allow partial parsing of input format
    strptime(date, format, timeOut);

    // mday will never be 0 for a valid date
    // we initialized it to 0, so if it still is 0, then date parsing failed
    if (timeOut->tm_mday == 0) {
        return StatusInval;
    }

    return StatusOk;
}

Status
xdfConvertToUnixTS(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    time_t unixTS;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    unixTS = timegm(&time);

    return xdfMakeInt64IntoScalar(out, (int64_t) unixTS);
}

Status
xdfConvertFromUnixTS(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;

    time_t unixTS;
    char date[DfMaxFieldValueSize];

    const char *format;
    int formatLen;

    switch (argv[0]->fieldType) {
    case DfInt32:
    case DfUInt32:
    case DfInt64:
    case DfUInt64: {
        int64_t val;
        status = xdfGetInt64FromArgv(1, &val, argv);
        assert(status == StatusOk);

        unixTS = val;
        break;
    }
    case DfTimespec: {
        DfTimeval t;
        status = xdfGetTimevalFromArgv(1, &t, argv);
        assert(status == StatusOk);

        unixTS = t.ms / 1000;
        break;
    }
    case DfFloat32:
    case DfFloat64: {
        double val;
        status = xdfGetFloat64FromArgv(1, &val, argv);
        assert(status == StatusOk);

        unixTS = (time_t) val;
        break;
    }
    default:
        NotReached();
        break;
    }

    tm time;
    gmtime_r(&unixTS, &time);

    status = xdfGetStringFromArgv(1, &format, &formatLen, &argv[1]);
    assert(status == StatusOk);

    size_t ret = strftime(date, sizeof(date), format, &time);
    if (ret == 0) {
        return StatusScalarFunctionFieldOverflow;
    }

    return xdfMakeStringIntoScalar(out, date, ret);
}

Status
xdfConvertDate(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    char dateOut[DfMaxFieldValueSize];

    const char *formatOut;
    int formatOutLen;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    status = xdfGetStringFromArgv(1, &formatOut, &formatOutLen, &argv[2]);
    assert(status == StatusOk);

    size_t ret = strftime(dateOut, sizeof(dateOut), formatOut, &time);
    if (ret == 0) {
        return StatusScalarFunctionFieldOverflow;
    }

    return xdfMakeStringIntoScalar(out, dateOut, ret);
}

Status
normalizeTimeToScalar(Scalar *out, const char *formatOut, tm *time)
{
    char dateOut[DfMaxFieldValueSize];

    timegm(time);

    size_t ret = strftime(dateOut, sizeof(dateOut), formatOut, time);
    if (ret == 0) {
        return StatusScalarFunctionFieldOverflow;
    }

    return xdfMakeStringIntoScalar(out, dateOut, ret);
}

Status
xdfDateAddMonth(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    const char *formatOut;
    int formatOutLen;
    status = xdfGetStringFromArgv(1, &formatOut, &formatOutLen, &argv[1]);
    assert(status == StatusOk);

    int64_t months;
    status = xdfGetInt64FromArgv(1, &months, &argv[2]);
    assert(status == StatusOk);

    time.tm_mon += months;

    return normalizeTimeToScalar(out, formatOut, &time);
}

Status
xdfDateAddYear(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    const char *formatOut;
    int formatOutLen;
    status = xdfGetStringFromArgv(1, &formatOut, &formatOutLen, &argv[1]);
    assert(status == StatusOk);

    int64_t years;
    status = xdfGetInt64FromArgv(1, &years, &argv[2]);
    assert(status == StatusOk);

    time.tm_year += years;

    return normalizeTimeToScalar(out, formatOut, &time);
}

Status
xdfDateAddDay(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    const char *formatOut;
    int formatOutLen;
    status = xdfGetStringFromArgv(1, &formatOut, &formatOutLen, &argv[1]);
    assert(status == StatusOk);

    int64_t days;
    status = xdfGetInt64FromArgv(1, &days, &argv[2]);
    assert(status == StatusOk);

    time.tm_mday += days;

    return normalizeTimeToScalar(out, formatOut, &time);
}

Status
xdfDateDiff(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time1;
    struct tm time2;

    status = dateToTime(argv[0], argv[2], &time1);
    if (status != StatusOk) {
        return status;
    }

    status = dateToTime(argv[1], argv[2], &time2);
    if (status != StatusOk) {
        return status;
    }

    time_t t1 = timegm(&time1);
    time_t t2 = timegm(&time2);

    double secs = difftime(t2, t1);
    int64_t days = secs / SecsPerDay;

    return xdfMakeInt64IntoScalar(out, days);
}

Status
xdfDateAddInterval(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    struct tm time;

    status = dateToTime(argv[0], argv[1], &time);
    if (status != StatusOk) {
        return status;
    }

    const char *formatOut;
    int formatOutLen;
    status = xdfGetStringFromArgv(1, &formatOut, &formatOutLen, &argv[1]);
    assert(status == StatusOk);

    int64_t years;
    status = xdfGetInt64FromArgv(1, &years, &argv[2]);
    assert(status == StatusOk);

    int64_t months;
    status = xdfGetInt64FromArgv(1, &months, &argv[3]);
    assert(status == StatusOk);

    int64_t days;
    status = xdfGetInt64FromArgv(1, &days, &argv[4]);
    assert(status == StatusOk);

    time.tm_year += years;
    time.tm_mon += months;
    time.tm_mday += days;

    return normalizeTimeToScalar(out, formatOut, &time);
}

/*
 * modeled after SQL's REPLACE
 *
 * https://msdn.microsoft.com/en-us/library/ms186862.aspx
 * http://www.techonthenet.com/oracle/functions/replace.php
 * https://dev.mysql.com/doc/refman/5.0/en/string-functions.html
 */
Status
xdfTransformReplace(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned ReplaceNumArgs = 3;
    unsigned ii;
    char strBuf[DfMaxFieldValueSize];
    char *curDst, *tmp;
    size_t toCopy, bytesUsed;
    bool ignoreCase = false;
    Status status;

    // The fourth argument is "ignoreCase" and is optional.  If it isn't
    // specified then the default is case sensitive.
    if (argc < 3 || argc > 4) {
        return StatusAstWrongNumberOfArgs;
    }

    // Sanity checking as this XDF doesn't use EnforceArgCheck
    for (ii = 0; ii < ReplaceNumArgs; ii++) {
        if (argvIn[ii] == NULL) {
            // FNF doesn't have anything to replace
            return StatusEvalUnsubstitutedVariables;
        } else if (argvIn[ii]->fieldType != DfString) {
            return StatusXdfTypeUnsupported;
        }
    }

    enum { OrigStr, SearchStr, ReplaceStr };
    DfFieldValue argv[ReplaceNumArgs];

    for (ii = 0; ii < ReplaceNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *search = argv[SearchStr].stringVal.strActual;
    const char *replace = argv[ReplaceStr].stringVal.strActual;

    if (search[0] == '\0') {
        return StatusInval;
    }

    if (argc == 4) {
        // Fourth arg is a boolean
        if (argvIn[3] == NULL) {
            return StatusEvalCastError;
        }
        status = xdfGetBoolFromArgv(1, &ignoreCase, &argvIn[3]);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }
    }

    const size_t searchSize = argv[SearchStr].stringVal.strSize;
    const size_t replaceSize = argv[ReplaceStr].stringVal.strSize;

    bytesUsed = 0;
    const char *curSrc = argv[OrigStr].stringVal.strActual;
    curDst = &strBuf[0];
    curDst[0] = '\0';

    do {
        if (ignoreCase) {
            tmp = (char *) strcasestr(curSrc, search);
        } else {
            tmp = (char *) strstr(curSrc, search);
        }
        if (tmp) {
            toCopy = tmp - curSrc;
            if (toCopy >= sizeof(strBuf) - bytesUsed) {
                return StatusScalarFunctionFieldOverflow;
            }
            memcpy(curDst, curSrc, toCopy);
            curDst += toCopy;
            curSrc += toCopy;
            bytesUsed += toCopy;
            toCopy = replaceSize - 1;
            if (toCopy >= sizeof(strBuf) - bytesUsed) {
                return StatusScalarFunctionFieldOverflow;
            }
            memcpy(curDst, replace, toCopy);
            curDst += toCopy;
            bytesUsed += toCopy;
            curSrc += searchSize - 1;
        } else {
            toCopy = strlen(curSrc) + 1;
            if (toCopy > sizeof(strBuf) - bytesUsed) {
                return StatusScalarFunctionFieldOverflow;
            }
            memcpy(curDst, curSrc, toCopy);
            bytesUsed += toCopy;
            break;
        }
    } while (true);

    return xdfMakeStringIntoScalar(out, &strBuf[0], bytesUsed - 1);
}

Status
xdfTransformConcat(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    int ii;
    char strBuf[DfMaxFieldValueSize];

    assert(argc >= 2);

    DfFieldValue argv[argc];

    Status status;
    for (ii = 0; ii < argc; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    size_t ret = snprintf(strBuf,
                          sizeof(strBuf),
                          "%s%s",
                          argv[0].stringVal.strActual,
                          argv[1].stringVal.strActual);
    if (ret >= sizeof(strBuf)) {
        return StatusScalarFunctionFieldOverflow;
    }

    for (ii = 2; ii < argc; ii++) {
        ret = strlcat(strBuf, argv[ii].stringVal.strActual, sizeof(strBuf));
        if (ret >= sizeof(strBuf)) {
            return StatusScalarFunctionFieldOverflow;
        }
    }

    return xdfMakeStringIntoScalar(out, &strBuf[0], ret);
}

Status
xdfTransformStrip(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    int ii;
    char strBuf[DfMaxFieldValueSize];

    enum { Str1, Delim };
    DfFieldValue argv[argc];

    Status status;
    for (ii = 0; ii < argc; ii++) {
        if (ii == Delim &&
            (argvIn[ii] == NULL || argvIn[ii]->fieldNumValues == 0 ||
             argvIn[ii]->fieldType == DfNull)) {
            argv[ii] = DfFieldValueNull;
            continue;
        }
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    if (argv[Str1].stringVal.strSize == 0) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    char delim = '\0';
    bool stripSpace = (argc == 1 || argvIn[Delim] == NULL ||
                       argv[Delim].stringVal.strSize == 0);
    if (!stripSpace) {
        delim = argv[Delim].stringVal.strActual[0];
    }

    // Subtract one for the null terminator
    int strLen = argv[Str1].stringVal.strSize - 1;

    for (ii = 0; ii < strLen &&
                 ((stripSpace && isspace(argv[Str1].stringVal.strActual[ii])) ||
                  (!stripSpace && argv[Str1].stringVal.strActual[ii] == delim));
         ii++) {
        // empty loop body intentional
    }

    if (ii == strLen) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    unsigned lastValid = 0;
    unsigned jj;
    for (jj = 0; ii < strLen; ii++, jj++) {
        strBuf[jj] = argv[Str1].stringVal.strActual[ii];
        if ((stripSpace && !isspace(strBuf[jj])) ||
            (!stripSpace && strBuf[jj] != delim)) {
            lastValid = jj;
        }
    }
    assert(lastValid + 1 <= sizeof(strBuf));
    strBuf[lastValid + 1] = '\0';

    return xdfMakeStringIntoScalar(out, &strBuf[0], lastValid + 1);
}

Status
xdfTransformStripLeft(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    int ii;
    char strBuf[DfMaxFieldValueSize];

    enum { Str1, Delim };
    DfFieldValue argv[argc];

    Status status;
    for (ii = 0; ii < argc; ii++) {
        if (ii == Delim &&
            (argvIn[ii] == NULL || argvIn[ii]->fieldNumValues == 0 ||
             argvIn[ii]->fieldType == DfNull)) {
            argv[ii] = DfFieldValueNull;
            continue;
        }
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    if (argv[Str1].stringVal.strSize == 0) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    char delim = '\0';
    bool stripSpace = (argc == 1 || argvIn[Delim] == NULL ||
                       argv[Delim].stringVal.strSize == 0);
    if (!stripSpace) {
        delim = argv[Delim].stringVal.strActual[0];
    }

    // Subtract one for the null terminator
    int strLen = argv[Str1].stringVal.strSize - 1;

    for (ii = 0; ii < strLen &&
                 ((stripSpace && isspace(argv[Str1].stringVal.strActual[ii])) ||
                  (!stripSpace && argv[Str1].stringVal.strActual[ii] == delim));
         ii++) {
        // empty loop body intentional
    }

    if (ii == strLen) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    unsigned jj;
    for (jj = 0; ii < strLen; ii++, jj++) {
        strBuf[jj] = argv[Str1].stringVal.strActual[ii];
    }
    strBuf[jj] = '\0';

    return xdfMakeStringIntoScalar(out, &strBuf[0], strlen(strBuf));
}

Status
xdfTransformStripRight(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    int ii;
    char strBuf[DfMaxFieldValueSize];

    enum { Str1, Delim };
    DfFieldValue argv[argc];

    Status status;
    for (ii = 0; ii < argc; ii++) {
        if (ii == Delim &&
            (argvIn[ii] == NULL || argvIn[ii]->fieldNumValues == 0 ||
             argvIn[ii]->fieldType == DfNull)) {
            argv[ii] = DfFieldValueNull;
            continue;
        }
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    if (argv[Str1].stringVal.strSize == 0) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    char delim = '\0';
    bool stripSpace = (argc == 1 || argvIn[Delim] == NULL ||
                       argv[Delim].stringVal.strSize == 0);
    if (!stripSpace) {
        delim = argv[Delim].stringVal.strActual[0];
    }

    // Subtract one for the null terminator
    int strLen = argv[Str1].stringVal.strSize - 1;

    unsigned lastValid = 0;
    unsigned jj;
    for (ii = 0, jj = 0; ii < strLen; ii++, jj++) {
        strBuf[jj] = argv[Str1].stringVal.strActual[ii];
        if ((stripSpace && !isspace(strBuf[jj])) ||
            (!stripSpace && strBuf[jj] != delim)) {
            lastValid = jj;
        }
    }
    assert(lastValid + 1 <= sizeof(strBuf));
    strBuf[lastValid + 1] = '\0';

    return xdfMakeStringIntoScalar(out, &strBuf[0], lastValid + 1);
}

Status
xdfTransformCut(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned CutNumArgs = 3;
    unsigned ii;

    assert((unsigned) argc == CutNumArgs);
    assert(argvIn[0]->fieldType == DfString);
    assert(argvIn[2]->fieldType == DfString);

    enum { Str, FieldNum, Delim };
    DfFieldValue argv[CutNumArgs];

    Status status;
    for (ii = 0; ii < CutNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *delim = argv[Delim].stringVal.strActual;

    if (delim[0] == '\0') {
        return StatusInval;
    }

    const char *cur = argv[Str].stringVal.strActual;
    const uint64_t fieldNum = argv[FieldNum].uint64Val;

    // Index should start at 1. We will return FNF's for 0 index
    if (fieldNum == 0) {
        return StatusInval;
    }

    for (uint64_t ii = 1; ii < fieldNum; ii++) {
        cur = strstr(cur, delim);
        if (cur == NULL) {
            return xdfMakeStringIntoScalar(out, "", 0);
        }
        cur += strlen(delim);
    }
    char *tmp = (char *) strstr(cur, delim);
    char old;
    if (tmp) {
        old = tmp[0];
        tmp[0] = '\0';
    }

    status = xdfMakeStringIntoScalar(out, cur, strlen(cur));

    if (tmp) {
        tmp[0] = old;
    }

    return status;
}

Status
xdfTransformExplodeString(void *context,
                          int argc,
                          Scalar *argvIn[],
                          Scalar *out)
{
    Status status = StatusOk;
    assert((unsigned) argc == 2);
    assert(argvIn[0]->fieldType == DfString);
    assert(argvIn[1]->fieldType == DfString);

    const char *vals[2];
    int lens[2];

    // First two args are strings
    status = xdfGetStringFromArgv(2, vals, lens, argvIn);
    assert(status == StatusOk);

    const char *delim = vals[1];
    size_t delimLen = strlen(delim);

    if (delim[0] == '\0') {
        return StatusInval;
    }

    char *cur = (char *) vals[0];
    size_t bufLen = 0;

    out->fieldNumValues = 0;
    out->fieldType = DfString;

    bool finished = false;
    do {
        char *next = strstr(cur, delim);
        if (next == NULL) {
            next = cur + strlen(cur);
            finished = true;
        }

        DfFieldValue val;

        val.stringVal.strActual = cur;
        // add 1 since strSize accounts for a null terminator
        val.stringVal.strSize = next - cur + 1;

        status = DataFormat::setFieldValueInArray(&out->fieldVals,
                                                  out->fieldAllocedSize,
                                                  DfString,
                                                  out->fieldNumValues++,
                                                  val);
        BailIfFailed(status);

        bufLen += DataFormat::fieldGetSize(DfString, val);

        cur = next + delimLen;
    } while (!finished);
    out->fieldUsedSize = bufLen;

CommonExit:
    return status;
}

Status
xdfTransformFindInSet(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned FindInSetNumArgs = 2;
    unsigned ii;

    assert((unsigned) argc == FindInSetNumArgs);
    assert(argvIn[0]->fieldType == DfString);
    assert(argvIn[1]->fieldType == DfString);

    enum { Str, Elt };
    DfFieldValue argv[FindInSetNumArgs];

    Status status;
    for (ii = 0; ii < FindInSetNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    char delim = ',';

    const char *cur = argv[Str].stringVal.strActual;
    const char *elt = argv[Elt].stringVal.strActual;
    unsigned strLen = argv[Str].stringVal.strSize - 1;
    unsigned eltLen = argv[Elt].stringVal.strSize - 1;

    uint64_t idx = 1;  // it's 1-indexed

    unsigned start = 0;

    // If element string contains delimiter, return 0
    for (ii = 0; ii < eltLen; ii++) {
        if (elt[ii] == delim) {
            return xdfMakeInt64IntoScalar(out, 0);
        }
    }

    for (ii = 0; ii < strLen + 1; ii++) {
        if (cur[ii] == delim || ii == strLen) {
            if (ii - start == eltLen) {
                if (strncmp(cur + start, elt, eltLen) == 0) {
                    return xdfMakeInt64IntoScalar(out, idx);
                }
            }
            start = ii + 1;
            idx++;
        }
    }

    status = xdfMakeInt64IntoScalar(out, 0);
    return status;
}

Status
xdfTransformFind(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned FindNumArgs = 4;
    unsigned ii;

    assert((unsigned) argc == FindNumArgs);

    enum { Str, StrToFind, Start, End };
    DfFieldValue argv[FindNumArgs];

    Status status;
    for (ii = 0; ii < FindNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *strToFind = argv[StrToFind].stringVal.strActual;
    const int64_t start = argv[Start].int64Val;
    int64_t end = argv[End].int64Val;
    const char *str = argv[Str].stringVal.strActual;

    if (strToFind[0] == '\0') {
        return StatusInval;
    }

    if (start >= (int64_t) argv[Str].stringVal.strSize) {
        return StatusInval;
    }

    if (end == 0) {
        end = (int64_t) argv[Str].stringVal.strSize - 1;
    }

    char *tmp = (char *) strstr(&str[start], strToFind);
    int64_t offset;
    if (tmp) {
        offset = tmp - str;
        if (offset > end) {
            offset = -1;
        }
    } else {
        offset = -1;
    }

    return xdfMakeInt64IntoScalar(out, offset);
}

Status
xdfTransformRfind(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned FindNumArgs = 4;
    unsigned ii;

    assert((unsigned) argc == FindNumArgs);

    enum { Str, StrToFind, Start, End };
    DfFieldValue argv[FindNumArgs];

    Status status;
    for (ii = 0; ii < FindNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *strToFind = argv[StrToFind].stringVal.strActual;
    const int64_t start = argv[Start].uint64Val;
    int64_t end = argv[End].uint64Val;
    const char *str = argv[Str].stringVal.strActual;

    if (strToFind[0] == '\0') {
        return StatusInval;
    }

    if (start >= (int64_t) argv[Str].stringVal.strSize) {
        return StatusInval;
    }

    if (end == 0) {
        end = (int64_t) argv[Str].stringVal.strSize - 1;
    }

    const char *tmp = &str[start];
    int64_t offset = -1;
    do {
        tmp = (char *) strstr(tmp, strToFind);
        if (tmp) {
            if (tmp - str <= end) {
                offset = tmp - str;
            }
            tmp += strlen(strToFind);
        }
    } while (tmp);

    return xdfMakeInt64IntoScalar(out, offset);
}

Status
xdfTransformSubstring(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned SubstringNumArgs = 3;
    unsigned ii;
    char strBuf[DfMaxFieldValueSize];

    assert((unsigned) argc == SubstringNumArgs);
    assert(argvIn[0]->fieldType == DfString);

    enum { Str, StartIdx, EndIdx };
    DfFieldValue argv[SubstringNumArgs];

    Status status;
    for (ii = 0; ii < SubstringNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *cur = argv[Str].stringVal.strActual;
    int64_t startIdx = argv[StartIdx].int64Val;
    int64_t endIdx = argv[EndIdx].int64Val;

    if (startIdx < 0) {
        startIdx = (int64_t)(argv[Str].stringVal.strSize) + startIdx - 1;
        if (startIdx < 0) {
            startIdx = 0;
        }
    } else if (((uint64_t) startIdx) >= argv[Str].stringVal.strSize) {
        startIdx = argv[Str].stringVal.strSize - 1;
    }

    if (endIdx <= 0) {
        endIdx = (int64_t)(argv[Str].stringVal.strSize) + endIdx - 1;
    } else if (((uint64_t) endIdx) >= argv[Str].stringVal.strSize) {
        endIdx = argv[Str].stringVal.strSize - 1;
    }
    if (endIdx < startIdx) {
        endIdx = startIdx;
    }

    strlcpy(strBuf, cur + startIdx, endIdx - startIdx + 1);

    return xdfMakeStringIntoScalar(out, strBuf, strlen(strBuf));
}

Status
xdfTransformSubstringIndex(void *context,
                           int argc,
                           Scalar *argvIn[],
                           Scalar *out)
{
    const unsigned SubstringIndexNumArgs = 3;
    unsigned ii;
    char strBuf[DfMaxFieldValueSize];
    char revStr[DfMaxFieldValueSize];
    char revDelim[DfMaxFieldValueSize];
    char *string;
    char *delim;
    int idx;

    assert((unsigned) argc == SubstringIndexNumArgs);
    for (ii = 0; ii < SubstringIndexNumArgs - 1; ii++) {
        assert(argvIn[ii]->fieldType == DfString);
    }
    assert(argvIn[SubstringIndexNumArgs - 1]->fieldType == DfInt64);

    enum { Str, Delim, Idx };
    DfFieldValue argv[SubstringIndexNumArgs];

    Status status;
    for (ii = 0; ii < SubstringIndexNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    if (argv[Delim].stringVal.strSize <= 1) {
        return StatusInval;
    }

    if (argv[Str].stringVal.strSize <= 1 || argv[Idx].int64Val == 0) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    unsigned stringLen = argv[Str].stringVal.strSize - 1;
    unsigned delimLen = argv[Delim].stringVal.strSize - 1;

    string = (char *) argv[Str].stringVal.strActual;
    delim = (char *) argv[Delim].stringVal.strActual;
    idx = argv[Idx].int64Val;

    if (idx < 0) {
        // Reverse both string and delim
        for (ii = 0; ii < stringLen; ii++) {
            revStr[stringLen - ii - 1] = string[ii];
        }
        revStr[stringLen] = '\0';
        for (ii = 0; ii < delimLen; ii++) {
            revDelim[delimLen - ii - 1] = delim[ii];
        }
        revDelim[delimLen] = '\0';
        string = revStr;
        delim = revDelim;
        idx = -1 * idx;
    }

    char *substring = NULL;
    char *origStr = string;
    while ((substring =
                (char *) (strBoyerMoore(string, stringLen, delim, delimLen)))) {
        idx--;
        if (idx == 0) {
            // Done :)
            unsigned matchLen = substring - origStr;
            if (argv[Idx].int64Val < 0) {
                for (ii = 0; ii < matchLen; ii++) {
                    strBuf[matchLen - ii - 1] = origStr[ii];
                }
            } else {
                memcpy(strBuf, origStr, matchLen);
            }
            strBuf[matchLen] = '\0';
            break;
        }
        string = substring + delimLen;
        stringLen = strlen(string);
    }

    if (idx > 0) {  // Return entire string
        memcpy(strBuf,
               argv[Str].stringVal.strActual,
               argv[Str].stringVal.strSize - 1);
        strBuf[argv[Str].stringVal.strSize - 1] = '\0';
    }

    return xdfMakeStringIntoScalar(out, strBuf, strlen(strBuf));
}

Status
xdfTransformRepeat(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    const unsigned RepeatNumArgs = 2;
    unsigned ii;
    char strBuf[DfMaxFieldValueSize];

    assert((unsigned) argc == RepeatNumArgs);
    assert(argvIn[0]->fieldType == DfString);

    enum { Str, NumTimes };
    DfFieldValue argv[RepeatNumArgs];

    Status status;
    for (ii = 0; ii < RepeatNumArgs; ii++) {
        status = argvIn[ii]->getValue(&argv[ii]);
        assert(status == StatusOk);
    }

    const char *cur = argv[Str].stringVal.strActual;
    size_t strLen = strlen(cur);
    int64_t numTimes = argv[NumTimes].int64Val;

    if (numTimes <= 0) {
        return xdfMakeStringIntoScalar(out, "", 0);
    }

    if (numTimes * strLen >= DfMaxFieldValueSize) {
        return StatusScalarFunctionFieldOverflow;
    }

    for (ii = 0; ii < numTimes; ii++) {
        strlcpy(strBuf + ii * strLen, cur, argv[Str].stringVal.strSize);
    }

    return xdfMakeStringIntoScalar(out, strBuf, strlen(strBuf));
}

Status
xdfTransformToUpper(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    Status status;
    DfFieldValue fieldVal;
    const char *startp;
    char strBuf[DfMaxFieldValueSize];
    unsigned ii;

    assert(argc == 1);
    assert(argvIn[0]->fieldType == DfString);

    status = argvIn[0]->getValue(&fieldVal);
    assert(status == StatusOk);

    startp = fieldVal.stringVal.strActual;

    for (ii = 0; ii < fieldVal.stringVal.strSize && *startp != '\0';
         ii++, startp++) {
        if (isalpha(*startp)) {
            strBuf[ii] = toupper(*startp);
        } else {
            strBuf[ii] = *startp;
        }
    }
    strBuf[ii] = '\0';

    return xdfMakeStringIntoScalar(out, strBuf, strlen(strBuf));
}

Status
xdfTransformToLower(void *context, int argc, Scalar *argvIn[], Scalar *out)
{
    Status status;
    DfFieldValue fieldVal;
    const char *startp;
    char strBuf[DfMaxFieldValueSize];
    unsigned ii;

    assert(argc == 1);
    assert(argvIn[0]->fieldType == DfString);

    status = argvIn[0]->getValue(&fieldVal);
    assert(status == StatusOk);

    startp = fieldVal.stringVal.strActual;

    for (ii = 0; ii < fieldVal.stringVal.strSize && *startp != '\0';
         ii++, startp++) {
        if (isalpha(*startp)) {
            strBuf[ii] = tolower(*startp);
        } else {
            strBuf[ii] = *startp;
        }
    }
    strBuf[ii] = '\0';

    return xdfMakeStringIntoScalar(out, strBuf, strlen(strBuf));
}
Status
xdfTransformIf(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    float64_t vals[3];
    bool nullAsFalse = false;

    if (argc == 4) {
        status = xdfGetBoolFromArgv(1, &nullAsFalse, &argv[3]);
    }

    status = xdfGetFloat64FromArgv(1, vals, argv);
    if (nullAsFalse && status == StatusInval) {
        vals[0] = 0.0;
    } else {
        BailIfFailed(status);
    }

    status = xdfGetFloat64FromArgv(2, &vals[1], &argv[1]);
    BailIfFailed(status);

    if (vals[0] == 0.0) {
        if (argv[2] == NULL || argv[2]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeFloat64IntoScalar(out, vals[2]);
    } else {
        if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeFloat64IntoScalar(out, vals[1]);
    }

CommonExit:
    return status;
}

Status
xdfTransformIfStr(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    float64_t cond;
    const char *vals[2];
    int valsLen[2];
    bool nullAsFalse = false;

    if (argc == 4) {
        status = xdfGetBoolFromArgv(1, &nullAsFalse, &argv[3]);
    }

    status = xdfGetFloat64FromArgv(1, &cond, argv);
    if (nullAsFalse && status == StatusInval) {
        cond = 0.0;
    } else {
        BailIfFailed(status);
    }

    status = xdfGetStringFromArgv(2, vals, valsLen, &argv[1]);
    BailIfFailed(status);

    if (cond == 0.0) {
        if (argv[2] == NULL || argv[2]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeStringIntoScalar(out, vals[1], valsLen[1]);
    } else {
        if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeStringIntoScalar(out, vals[0], valsLen[0]);
    }

CommonExit:
    return status;
}

Status
xdfTransformIfInt(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    float64_t cond;
    int64_t vals[2];
    bool nullAsFalse = false;

    if (argc == 4) {
        status = xdfGetBoolFromArgv(1, &nullAsFalse, &argv[3]);
    }

    status = xdfGetFloat64FromArgv(1, &cond, argv);
    if (nullAsFalse && status == StatusInval) {
        cond = 0.0;
    } else {
        BailIfFailed(status);
    }

    status = xdfGetInt64FromArgv(2, vals, &argv[1]);
    BailIfFailed(status);

    if (cond == 0.0) {
        if (argv[2] == NULL || argv[2]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeInt64IntoScalar(out, vals[1]);
    } else {
        if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeInt64IntoScalar(out, vals[0]);
    }

CommonExit:
    return status;
}

Status
xdfTransformIfNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    XlrDfp cond;
    XlrDfp zero;
    XlrDfp vals[2];
    DFPUtils *dfp = DFPUtils::get();
    bool nullAsFalse = false;

    dfp->xlrDfpZero(&zero);

    if (argc == 4) {
        status = xdfGetBoolFromArgv(1, &nullAsFalse, &argv[3]);
    }

    status = xdfGetNumericFromArgv(1, &cond, argv);
    if (nullAsFalse && status == StatusInval) {
        cond = zero;
    } else {
        BailIfFailed(status);
    }

    status = xdfGetNumericFromArgv(2, vals, &argv[1]);
    BailIfFailed(status);

    if (dfp->xlrDfp64Cmp(&cond, &zero) == 0) {
        if (argv[2] == NULL || argv[2]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeNumericIntoScalar(out, vals[1]);
    } else {
        if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeNumericIntoScalar(out, vals[0]);
    }

CommonExit:
    return status;
}

Status
xdfTransformIfTimestamp(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    float64_t cond;
    DfTimeval vals[2];
    bool nullAsFalse = false;

    if (argc == 4) {
        status = xdfGetBoolFromArgv(1, &nullAsFalse, &argv[3]);
    }

    status = xdfGetFloat64FromArgv(1, &cond, argv);
    if (nullAsFalse && status == StatusInval) {
        cond = 0.0;
    } else {
        BailIfFailed(status);
    }

    status = xdfGetTimevalFromArgv(2, vals, &argv[1]);
    BailIfFailed(status);

    if (cond == 0.0) {
        if (argv[2] == NULL || argv[2]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeTimestampIntoScalar(out, vals[1]);
    } else {
        if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
            return StatusEvalUnsubstitutedVariables;
        }

        status = xdfMakeTimestampIntoScalar(out, vals[0]);
    }

CommonExit:
    return status;
}

static Status
getSoundexCode(char c, char *code)
{
    Status status = StatusOk;
    switch (c) {
    case 'A':
    case 'E':
    case 'I':
    case 'O':
    case 'U':
    case 'Y':
        *code = '0';
        break;
    case 'B':
    case 'F':
    case 'P':
    case 'V':
        *code = '1';
        break;
    case 'C':
    case 'G':
    case 'J':
    case 'K':
    case 'Q':
    case 'S':
    case 'X':
    case 'Z':
        *code = '2';
        break;
    case 'D':
    case 'T':
        *code = '3';
        break;
    case 'L':
        *code = '4';
        break;
    case 'M':
    case 'N':
        *code = '5';
        break;
    case 'R':
        *code = '6';
        break;
    case 'H':
    case 'W':
        *code = '7';
        break;
    default:
        status = StatusInval;
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
xdfTransformSoundEx(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusOk;
    const char *name;
    int nameLen;
    // SoundEx code is 1 letter followed by 3 numbers
    // Initialize to character 0 so we don't rightfill later; NUL terminate
    char codeBuf[4 + 1] = {'0', '0', '0', '0', '\0'};
    char prevCode;
    int codeIdx = 0;

    assert(argc == 1);

    status = xdfGetStringFromArgv(argc, &name, &nameLen, &argv[0]);
    BailIfFailed(status);

    // First character should be a normal alphabetic char
    if (nameLen < 1 || !isalpha(name[0])) {
        status = StatusInval;
        goto CommonExit;
    }

    // Remember what the code would be for the first character as prevCode
    status = getSoundexCode(toupper(name[0]), &prevCode);
    BailIfFailed(status);

    // The SoundEx code begins with the first character of the name
    codeBuf[codeIdx] = toupper(name[0]);
    codeIdx++;

    for (int ii = 1; ii < nameLen; ii++) {
        char c = name[ii];
        if (!isalpha(c)) {
            // Ignore all non-alphabetics
            // reset prevCode since this means it is no longer duplicated
            prevCode = '0';
            continue;
        }

        char code;
        status = getSoundexCode(toupper(c), &code);
        BailIfFailed(status);

        if (code == '7') {
            // This is an ignored character; don't change lastCode
        } else {
            if (code != '0' && code != prevCode) {
                codeBuf[codeIdx] = code;
                codeIdx++;
                if (codeIdx == 4) {
                    // We only take the first 3 numbers; end here
                    break;
                }
            }
            // All codes other than 7 are assigned to prevCode to avoid dupes
            prevCode = code;
        }
    }

    status = xdfMakeStringIntoScalar(out, codeBuf, sizeof(codeBuf) - 1);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTransformLevenshtein(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusOk;
    const char *strA;
    int strALen;
    const char *strB;
    int strBLen;

    const char *shortStr;
    int shortStrLen;
    const char *longStr;
    int longStrLen;

    int64_t levCost;

    status = xdfGetStringFromArgv(1, &strA, &strALen, &argv[0]);
    BailIfFailed(status);

    status = xdfGetStringFromArgv(1, &strB, &strBLen, &argv[1]);
    BailIfFailed(status);

    // Use the smaller string in order to reduce memory usage
    if (strALen >= strBLen) {
        shortStr = strB;
        shortStrLen = strBLen;
        longStr = strA;
        longStrLen = strALen;
    } else {
        shortStr = strA;
        shortStrLen = strALen;
        longStr = strB;
        longStrLen = strBLen;
    }

    // Levenshtein conceptually uses an MxN grid of edit distances, using
    // bottom up dynamic programming. This implementation additionally uses
    // an optimization that takes advantage of the fact that we only actually
    // need the most recent column as we move across the grid from left to right
    // See https://en.wikipedia.org/wiki/Levenshtein_distance for an algo spec
    {
        int column[shortStrLen + 1];
        column[0] = 0;  // empty string is distance 0 from empty string

        // First column means we are editing into the empty string; so we init
        // each value into its length
        for (int jj = 1; jj <= shortStrLen; jj++) {
            // empty string is strlen(x) distance away from x
            column[jj] = jj;
        }

        for (int ii = 1; ii <= longStrLen; ii++) {
            column[0] = ii;
            int prevDiag = ii - 1;  // we need to peek up-left for substitution
            for (int jj = 1; jj <= shortStrLen; jj++) {
                int subCost =
                    prevDiag + (shortStr[jj - 1] == longStr[ii - 1] ? 0 : 1);
                int delCost = column[jj - 1] + 1;
                int addCost = column[jj] + 1;
                prevDiag = column[jj];
                column[jj] = xcMin(xcMin(subCost, delCost), addCost);
            }
        }
        levCost = column[shortStrLen];
    }

    status = xdfMakeInt64IntoScalar(out, levCost);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampAddDateInterval(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;
    int64_t years, months, days;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &years, &argv[1]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &months, &argv[2]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &days, &argv[3]);
    BailIfFailed(status);

    xdfGetTmFromTimeval(&t, &time);

    time.tm_year += years;
    time.tm_mon += months;
    time.tm_mday += days;

    xdfUpdateTimevalFromTm(&time, &t);

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampAddTimeInterval(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;
    int64_t hours, minutes, seconds;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &hours, &argv[1]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &minutes, &argv[2]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &seconds, &argv[3]);
    BailIfFailed(status);

    xdfGetTmFromTimeval(&t, &time);

    time.tm_hour += hours;
    time.tm_min += minutes;
    time.tm_sec += seconds;

    xdfUpdateTimevalFromTm(&time, &t);

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampAddIntervalString(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;

    const char *interval;
    int intervalLen;
    int year = 0, month = 0, day = 0, hour = 0, min = 0;
    float sec = 0;
    int milliseconds = 0;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetStringFromArgv(1, &interval, &intervalLen, &argv[1]);
    BailIfFailed(status);

    xdfGetTmFromTimeval(&t, &time);

    // allow partial parsing of input format
    sscanf(interval,
           "%d,%d,%d,%d,%d,%f",
           &year,
           &month,
           &day,
           &hour,
           &min,
           &sec);

    time.tm_year += year;
    time.tm_mon += month;
    time.tm_mday += day;
    time.tm_hour += hour;
    time.tm_min += min;
    time.tm_sec += (int) sec;

    xdfUpdateTimevalFromTm(&time, &t);

    milliseconds = (int) (sec * 1000) % 1000;
    t.ms += milliseconds;

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}
Status
xdfTimestampDateDiff(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t[2];

    status = xdfGetTimevalFromArgv(2, t, argv);
    BailIfFailed(status);

    int64_t diff;

    // convert to days and subtract
    diff = t[1].ms / (1000 * 60 * 60 * 24) - t[0].ms / (1000 * 60 * 60 * 24);

    status = xdfMakeInt64IntoScalar(out, diff);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampDatePart(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;
    int ret;

    const char *part;
    int partLen;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetStringFromArgv(1, &part, &partLen, &argv[1]);
    BailIfFailed(status);

    if (partLen == 0) {
        status = StatusInval;
        goto CommonExit;
    }

    xdfGetTmFromTimeval(&t, &time);

    switch (part[0]) {
    case 'Y':
    case 'y':
        ret = time.tm_year + 1900;
        break;
    case 'Q':
    case 'q':
        ret = time.tm_mon / 3 + 1;
        break;
    case 'M':
    case 'm':
        ret = time.tm_mon + 1;
        break;
    case 'D':
    case 'd':
        ret = time.tm_mday;
        break;
    case 'W':
    case 'w':
        ret = time.tm_wday + 1;
        break;
    default:
        status = StatusInval;
        goto CommonExit;
    }

    status = xdfMakeInt64IntoScalar(out, ret);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampDayOfYear(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    xdfGetTmFromTimeval(&t, &time);

    status = xdfMakeInt64IntoScalar(out, time.tm_yday + 1);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampTimePart(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;
    int ret;

    const char *part;
    int partLen;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetStringFromArgv(1, &part, &partLen, &argv[1]);
    BailIfFailed(status);

    if (partLen == 0) {
        status = StatusInval;
        goto CommonExit;
    }

    xdfGetTmFromTimeval(&t, &time);

    switch (part[0]) {
    case 'H':
    case 'h':
        ret = time.tm_hour;
        break;
    case 'M':
    case 'm':
        ret = time.tm_min;
        break;
    case 'S':
    case 's':
        ret = time.tm_sec;
        break;
    default:
        status = StatusInval;
        goto CommonExit;
    }

    status = xdfMakeInt64IntoScalar(out, ret);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampLastDayOfMonth(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    xdfGetTmFromTimeval(&t, &time);

    switch (time.tm_mon) {
    case 0:
    case 2:
    case 4:
    case 6:
    case 7:
    case 9:
    case 11:
        time.tm_mday = 31;
        break;
    case 3:
    case 5:
    case 8:
    case 10:
        time.tm_mday = 30;
        break;
    case 1: {  // Feb
        int year = time.tm_year + 1900;

        if (year % 4 == 0) {
            if (year % 100 == 0) {
                if (year % 400 == 0) {
                    time.tm_mday = 29;
                } else {
                    time.tm_mday = 28;
                }
            } else {
                time.tm_mday = 29;
            }
        } else {
            time.tm_mday = 28;
        }

        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

    xdfUpdateTimevalFromTm(&time, &t);

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampConvertTimezone(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out)
{
    Status status;
    DfTimeval t;
    int64_t tzoffset;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    BailIfFailed(status);

    status = xdfGetInt64FromArgv(1, &tzoffset, &argv[1]);
    BailIfFailed(status);

    t.tzoffset = tzoffset;

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampDateTrunc(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    const char *fmt;
    int fmtLen;
    tm time;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    if (status != StatusOk) {
        return status;
    }

    status = xdfGetStringFromArgv(1, &fmt, &fmtLen, &argv[1]);
    if (status != StatusOk) {
        return status;
    }

    xdfGetTmFromTimeval(&t, &time);
    char fmtLower[fmtLen + 1];
    memZero(fmtLower, sizeof(fmtLower));
    for (int i = 0; i < fmtLen; i++) {
        fmtLower[i] = std::tolower(fmt[i]);
    }
    int fmtNum;
    if (strcmp(fmtLower, "year") == 0 || strcmp(fmtLower, "yyyy") == 0 ||
        strcmp(fmtLower, "yy") == 0) {
        fmtNum = 0;
    } else if (strcmp(fmtLower, "quarter") == 0) {
        fmtNum = 1;
    } else if (strcmp(fmtLower, "month") == 0 || strcmp(fmtLower, "mon") == 0 ||
               strcmp(fmtLower, "mm") == 0) {
        fmtNum = 2;
    } else if (strcmp(fmtLower, "day") == 0 || strcmp(fmtLower, "dd") == 0) {
        fmtNum = 3;
    } else if (strcmp(fmtLower, "hour") == 0) {
        fmtNum = 4;
    } else if (strcmp(fmtLower, "minute") == 0) {
        fmtNum = 5;
    } else if (strcmp(fmtLower, "second") == 0) {
        fmtNum = 6;
    } else if (strcmp(fmtLower, "week") == 0) {
        fmtNum = 7;
    } else {
        NotReached();
    }

    switch (fmtNum) {
    case 0: {
        time.tm_mon = 0;
    }
    case 1: {
        time.tm_mon = time.tm_mon - time.tm_mon % 3;
    }
    case 2: {
        time.tm_mday = 1;
    }
    case 3: {
        time.tm_hour = 0;
    }
    case 4: {
        time.tm_min = 0;
    }
    case 5: {
        time.tm_sec = 0;
    }
    case 6: {
        break;
    }
    case 7: {
        time.tm_mday -= time.tm_wday == 0 ? 6 : (time.tm_wday - 1);
        time.tm_hour = 0;
        time.tm_min = 0;
        time.tm_sec = 0;
        break;
    }
    default:
        NotReached();
        break;
    }

    t.ms = 0;
    xdfUpdateTimevalFromTm(&time, &t);

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampMonthsBetween(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t1;
    DfTimeval t2;
    tm time1;
    tm time2;
    int lastDay1;
    int lastDay2;
    double res;

    status = xdfGetTimevalFromArgv(1, &t1, &argv[0]);
    if (status != StatusOk) {
        return status;
    }

    status = xdfGetTimevalFromArgv(1, &t2, &argv[1]);
    if (status != StatusOk) {
        return status;
    }
    xdfGetTmFromTimeval(&t1, &time1);
    xdfGetTmFromTimeval(&t2, &time2);

    switch (time1.tm_mon) {
    case 0:
    case 2:
    case 4:
    case 6:
    case 7:
    case 9:
    case 11:
        lastDay1 = 31;
        break;
    case 3:
    case 5:
    case 8:
    case 10:
        lastDay1 = 30;
        break;
    case 1: {  // Feb
        int year = time1.tm_year + 1900;

        if (year % 4 == 0) {
            if (year % 100 == 0) {
                if (year % 400 == 0) {
                    lastDay1 = 29;
                } else {
                    lastDay1 = 28;
                }
            } else {
                lastDay1 = 29;
            }
        } else {
            lastDay1 = 28;
        }
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

    switch (time2.tm_mon) {
    case 0:
    case 2:
    case 4:
    case 6:
    case 7:
    case 9:
    case 11:
        lastDay2 = 31;
        break;
    case 3:
    case 5:
    case 8:
    case 10:
        lastDay2 = 30;
        break;
    case 1: {  // Feb
        int year = time2.tm_year + 1900;

        if (year % 4 == 0) {
            if (year % 100 == 0) {
                if (year % 400 == 0) {
                    lastDay2 = 29;
                } else {
                    lastDay2 = 28;
                }
            } else {
                lastDay2 = 29;
            }
        } else {
            lastDay2 = 28;
        }
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }

    if (time1.tm_mday == lastDay1 && time2.tm_mday == lastDay2) {
        res =
            (time1.tm_year - time2.tm_year) * 12 + time1.tm_mon - time2.tm_mon;
    } else {
        res = (time1.tm_year - time2.tm_year) * 12 + time1.tm_mon -
              time2.tm_mon + (double) (time1.tm_mday - time2.tm_mday) / 31 +
              (double) (time1.tm_hour - time2.tm_hour) / (24 * 31) +
              (double) (time1.tm_min - time2.tm_min) / (60 * 24 * 31) +
              (t1.ms % (60 * 1000) - t2.ms % (60 * 1000)) /
                  ((double) 31 * 24 * 60 * 60 * 1000);
    }

    status = xdfMakeFloat64IntoScalar(out, res);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampNextDay(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    const char *dayOfWeek;
    int dowLen;
    tm time;
    int wday;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    if (status != StatusOk) {
        return status;
    }

    status = xdfGetStringFromArgv(1, &dayOfWeek, &dowLen, &argv[1]);
    if (status != StatusOk) {
        return status;
    }

    xdfGetTmFromTimeval(&t, &time);
    char dowLower[dowLen + 1];
    memZero(dowLower, sizeof(dowLower));
    for (int i = 0; i < dowLen; i++) {
        dowLower[i] = std::tolower(dayOfWeek[i]);
    }

    if (strcmp(dowLower, "mo") == 0 || strcmp(dowLower, "mon") == 0 ||
        strcmp(dowLower, "monday") == 0) {
        wday = 1;
    } else if (strcmp(dowLower, "tu") == 0 || strcmp(dowLower, "tue") == 0 ||
               strcmp(dowLower, "tuesday") == 0) {
        wday = 2;
    } else if (strcmp(dowLower, "we") == 0 || strcmp(dowLower, "wed") == 0 ||
               strcmp(dowLower, "wednesday") == 0) {
        wday = 3;
    } else if (strcmp(dowLower, "th") == 0 || strcmp(dowLower, "thu") == 0 ||
               strcmp(dowLower, "thursday") == 0) {
        wday = 4;
    } else if (strcmp(dowLower, "fr") == 0 || strcmp(dowLower, "fri") == 0 ||
               strcmp(dowLower, "friday") == 0) {
        wday = 5;
    } else if (strcmp(dowLower, "sa") == 0 || strcmp(dowLower, "sat") == 0 ||
               strcmp(dowLower, "saturday") == 0) {
        wday = 6;
    } else if (strcmp(dowLower, "su") == 0 || strcmp(dowLower, "sun") == 0 ||
               strcmp(dowLower, "sunday") == 0) {
        wday = 0;
    } else {
        NotReached();
    }

    time.tm_mday += (wday - time.tm_wday <= 0) ? (wday - time.tm_wday + 7)
                                               : (wday - time.tm_wday);
    time.tm_hour = 0;
    time.tm_min = 0;
    time.tm_sec = 0;
    t.ms = 0;
    xdfUpdateTimevalFromTm(&time, &t);

    status = xdfMakeTimestampIntoScalar(out, t);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampWeekOfYear(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval t;
    tm time;
    int res;

    status = xdfGetTimevalFromArgv(1, &t, &argv[0]);
    if (status != StatusOk) {
        return status;
    }

    xdfGetTmFromTimeval(&t, &time);
    int daysBeforeCurWeek =
        time.tm_yday + 1 - (time.tm_wday == 0 ? 7 : time.tm_wday);
    if (daysBeforeCurWeek % 7 > 3) {
        res = daysBeforeCurWeek / 7 + 2;
    } else {
        res = daysBeforeCurWeek / 7 + 1;
    }

    status = xdfMakeInt64IntoScalar(out, res);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfTimestampNow(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval timeval;
    time_t now = time(NULL);

    timeval.ms = now * 1000;
    timeval.tzoffset = 0;

    status = xdfMakeTimestampIntoScalar(out, timeval);

    return status;
}

Status
xdfTimestampToday(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    DfTimeval timeval;
    time_t now = time(NULL);
    struct tm timeStructured;

    if (gmtime_r(&now, &timeStructured) == NULL) {
        status = StatusInval;
        return status;
    }

    timeStructured.tm_hour = 0;
    timeStructured.tm_min = 0;
    timeStructured.tm_sec = 0;

    now = timegm(&timeStructured);

    timeval.ms = now * 1000;
    timeval.tzoffset = 0;

    status = xdfMakeTimestampIntoScalar(out, timeval);

    return status;
}
