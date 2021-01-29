// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// ConditionFns are of the form f: scalar X ... X scalar |-> { true, false }
// In C-Speak, it has the function signature:
// bool f(int argc, DfScalar argv[], DfScalar *out)

#include <regex.h>
#include <cmath>

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "operators/Xdf.h"
#include "XdfInt.h"
#include "strings/String.h"
#include "util/DFPUtils.h"

Status
xdfConditionEq(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    bool answer;

    assert(argc == 2);
    if (argv[0] == NULL || argv[1] == NULL) {
        answer = (argv[0] == NULL) && (argv[1] == NULL);
        return xdfMakeBoolIntoScalar(out, answer);
    }

    if (!supportedMatrix[argv[0]->fieldType][argv[1]->fieldType]) {
        return StatusXdfTypeUnsupported;
    }

    switch (argv[0]->fieldType) {
    case DfBoolean: {
        bool vals[2];
        assert(argv[1]->fieldType == DfBoolean);
        status = xdfGetBoolFromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] == vals[1];
        break;
    }
    case DfString: {
        const char *vals[2];
        int stringLens[2];
        assert(argv[1]->fieldType == DfString);
        status = xdfGetStringFromArgv(argc, vals, stringLens, argv);
        BailIfFailed(status);
        answer = (strcmp(vals[0], vals[1]) == 0);
        break;
    }
    case DfTimespec: {
        int64_t vals[2];
        status = xdfGetInt64FromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] == vals[1];
        break;
    }
    case DfMoney: {
        XlrDfp vals[2];
        status = xdfGetNumericFromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = DFPUtils::get()->xlrDfp64Cmp(&vals[0], &vals[1]) == 0;
        break;
    }
    default:
        // Number
        if ((argv[0]->fieldType == DfInt32 || argv[0]->fieldType == DfInt64) &&
            (argv[1]->fieldType == DfInt32 || argv[1]->fieldType == DfInt64)) {
            // if they are both ints, do an int compare
            int64_t vals[2];
            status = xdfGetInt64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] == vals[1];
        } else {
            float64_t vals[2];
            status = xdfGetFloat64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] == vals[1];
        }
    }

CommonExit:
    if (status == StatusOk) {
        return xdfMakeBoolIntoScalar(out, answer);
    } else {
        assert(0);
        return status;
    }
}

Status
xdfConditionNeq(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    status = xdfConditionEq(context, argc, argv, out);
    if (status != StatusOk) {
        return status;
    }

    out->fieldVals.boolVal[0] = !out->fieldVals.boolVal[0];

    return StatusOk;
}

Status
xdfConditionIn(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    bool result = false;
    Scalar *argvForEq[2];

    argvForEq[0] = argv[0];

    for (int ii = 1; ii < argc; ii++) {
        argvForEq[1] = argv[ii];

        status = xdfConditionEq(context, 2, argvForEq, out);
        if (status != StatusOk) {
            return status;
        }

        if (out->fieldVals.boolVal[0]) {
            result = true;
            break;
        }
    }

    return xdfMakeBoolIntoScalar(out, result);
}

Status
xdfConditionGreaterThan(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    bool answer;

    assert(argc == 2);

    if (!supportedMatrix[argv[0]->fieldType][argv[1]->fieldType]) {
        return StatusXdfTypeUnsupported;
    }

    switch (argv[0]->fieldType) {
    case DfBoolean: {
        bool vals[2];
        assert(argv[1]->fieldType == DfBoolean);
        status = xdfGetBoolFromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] > vals[1];
        break;
    }
    case DfString: {
        const char *vals[2];
        int stringLens[2];
        assert(argv[1]->fieldType == DfString);
        status = xdfGetStringFromArgv(argc, vals, stringLens, argv);
        BailIfFailed(status);
        answer = (strcmp(vals[0], vals[1]) > 0);
        break;
    }
    case DfTimespec: {
        int64_t vals[2];
        status = xdfGetInt64FromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] > vals[1];
        break;
    }
    case DfMoney: {
        XlrDfp vals[2];
        status = xdfGetNumericFromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = DFPUtils::get()->xlrDfp64Cmp(&vals[0], &vals[1]) == 1;
        break;
    }
    default:
        // Number
        if ((argv[0]->fieldType == DfInt32 || argv[0]->fieldType == DfInt64) &&
            (argv[1]->fieldType == DfInt32 || argv[1]->fieldType == DfInt64)) {
            // if they are both ints, do an int compare
            int64_t vals[2];
            status = xdfGetInt64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] > vals[1];
        } else {
            float64_t vals[2];
            status = xdfGetFloat64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] > vals[1];
        }
    }

CommonExit:
    if (status == StatusOk) {
        return xdfMakeBoolIntoScalar(out, answer);
    } else {
        assert(0);
        return status;
    }
}

Status
xdfConditionGreaterThanOrEqualTo(void *context,
                                 int argc,
                                 Scalar *argv[],
                                 Scalar *out)
{
    Status status;
    bool answer;

    assert(argc == 2);

    if (!supportedMatrix[argv[0]->fieldType][argv[1]->fieldType]) {
        return StatusXdfMixedTypeNotSupported;
    }

    switch (argv[0]->fieldType) {
    case DfBoolean: {
        bool vals[2];
        assert(argv[1]->fieldType == DfBoolean);
        status = xdfGetBoolFromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] >= vals[1];
        break;
    }
    case DfString: {
        const char *vals[2];
        int stringLens[2];
        assert(argv[1]->fieldType == DfString);
        status = xdfGetStringFromArgv(argc, vals, stringLens, argv);
        BailIfFailed(status);
        answer = (strcmp(vals[0], vals[1]) >= 0);
        break;
    }
    case DfTimespec: {
        int64_t vals[2];
        status = xdfGetInt64FromArgv(argc, vals, argv);
        BailIfFailed(status);
        answer = vals[0] >= vals[1];
        break;
    }
    case DfMoney: {
        XlrDfp vals[2];
        int32_t cmp;
        status = xdfGetNumericFromArgv(argc, vals, argv);
        BailIfFailed(status);
        cmp = DFPUtils::get()->xlrDfp64Cmp(&vals[0], &vals[1]);
        answer = cmp == 0 || cmp == 1;
        break;
    }
    default:
        // Number
        if ((argv[0]->fieldType == DfInt32 || argv[0]->fieldType == DfInt64) &&
            (argv[1]->fieldType == DfInt32 || argv[1]->fieldType == DfInt64)) {
            // if they are both ints, do an int compare
            int64_t vals[2];
            status = xdfGetInt64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] >= vals[1];
        } else {
            float64_t vals[2];
            status = xdfGetFloat64FromArgv(argc, vals, argv);
            BailIfFailed(status);
            answer = vals[0] >= vals[1];
        }
    }

CommonExit:
    if (status == StatusOk) {
        return xdfMakeBoolIntoScalar(out, answer);
    } else {
        assert(0);
        return status;
    }
}

Status
xdfConditionLessThan(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    if (!supportedMatrix[argv[0]->fieldType][argv[1]->fieldType]) {
        return StatusXdfTypeUnsupported;
    }

    status = xdfConditionGreaterThanOrEqualTo(context, argc, argv, out);
    if (status != StatusOk) {
        return status;
    }

    out->fieldVals.boolVal[0] = !out->fieldVals.boolVal[0];

    return StatusOk;
}

Status
xdfConditionLessThanOrEqualTo(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out)
{
    Status status;
    if (!supportedMatrix[argv[0]->fieldType][argv[1]->fieldType]) {
        return StatusXdfTypeUnsupported;
    }

    status = xdfConditionGreaterThan(context, argc, argv, out);
    if (status != StatusOk) {
        return status;
    }

    out->fieldVals.boolVal[0] = !out->fieldVals.boolVal[0];

    return StatusOk;
}

Status
xdfConditionLogicalOr(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusOk;

    bool isTrue[2] = {false, false};
    bool isValid[2] = {true, true};
    assert(argc == 2);

    if (argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        isValid[0] = false;
    } else {
        xdfGetBoolFromArgv(1, &isTrue[0], &argv[0]);
    }

    if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
        isValid[1] = false;
    } else {
        xdfGetBoolFromArgv(1, &isTrue[1], &argv[1]);
    }

    if ((isValid[0] && isTrue[0]) || (isValid[1] && isTrue[1])) {
        status = xdfMakeBoolIntoScalar(out, true);
    } else if (isValid[0] && !isTrue[0] && isValid[1] && !isTrue[1]) {
        status = xdfMakeBoolIntoScalar(out, false);
    } else {
        status = xdfMakeNullIntoScalar(out);
    }

    return status;
}

Status
xdfConditionLogicalAnd(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusOk;

    bool isTrue[2] = {false, false};
    bool isValid[2] = {true, true};

    assert(argc == 2);

    if (argv[0] == NULL || argv[0]->fieldNumValues == 0) {
        isValid[0] = false;
    } else {
        xdfGetBoolFromArgv(1, &isTrue[0], &argv[0]);
    }

    if (argv[1] == NULL || argv[1]->fieldNumValues == 0) {
        isValid[1] = false;
    } else {
        xdfGetBoolFromArgv(1, &isTrue[1], &argv[1]);
    }

    if ((isValid[0] && !isTrue[0]) || (isValid[1] && !isTrue[1])) {
        status = xdfMakeBoolIntoScalar(out, false);
    } else if (isValid[0] && isTrue[0] && isValid[1] && isTrue[1]) {
        status = xdfMakeBoolIntoScalar(out, true);
    } else {
        status = xdfMakeNullIntoScalar(out);
    }

    return status;
}

Status
xdfConditionLogicalNot(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    bool vals[1];
    assert(argc == 1);

    status = xdfGetBoolFromArgv(argc, vals, argv);
    assert(status == StatusOk);

    return xdfMakeBoolIntoScalar(out, !(vals[0]));
}

Status
xdfConditionBetween(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    Scalar *argvLessThan[2];
    assert(argc == 3);

    // first check lower bound
    status = xdfConditionGreaterThanOrEqualTo(context, 2, argv, out);
    BailIfFailed(status);

    if (out->fieldVals.boolVal[0] == false) {
        goto CommonExit;
    }

    // if that succeds, return result of checking upper bound
    argvLessThan[0] = argv[0];
    argvLessThan[1] = argv[2];

    status = xdfConditionLessThanOrEqualTo(context, 2, argvLessThan, out);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
xdfConditionIsInf(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusXdfTypeUnsupported;
    assert(argc == 1);

    switch (argv[0]->fieldType) {
    case DfFloat64: {
        float64_t vals[1];
        status = xdfGetFloat64FromArgv(argc, vals, argv);
        assert(status == StatusOk);
        BailIfFailed(status);

        status = xdfMakeBoolIntoScalar(out, ::isinfl(vals[0]));
        BailIfFailed(status);
        break;
    }
    case DfMoney: {
        XlrDfp vals[1];
        status = xdfGetNumericFromArgv(argc, vals, argv);
        BailIfFailed(status);
        bool isInf = DFPUtils::get()->xlrDfpIsInf(&vals[0]);
        status = xdfMakeBoolIntoScalar(out, isInf);
        BailIfFailed(status);
        break;
    }
    default:
        assert(0);
        break;
    }

CommonExit:
    return status;
}

Status
xdfConditionIsNan(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusXdfTypeUnsupported;
    assert(argc == 1);

    switch (argv[0]->fieldType) {
    case DfFloat64: {
        float64_t vals[1];
        status = xdfGetFloat64FromArgv(argc, vals, argv);
        assert(status == StatusOk);
        BailIfFailed(status);

        status = xdfMakeBoolIntoScalar(out, ::isnanl(vals[0]));
        BailIfFailed(status);
        break;
    }
    case DfMoney: {
        XlrDfp vals[1];
        status = xdfGetNumericFromArgv(argc, vals, argv);
        BailIfFailed(status);
        bool isNan = DFPUtils::get()->xlrDfpIsNan(&vals[0]);
        status = xdfMakeBoolIntoScalar(out, isNan);
        BailIfFailed(status);
        break;
    }
    default:
        assert(0);
        break;
    }

CommonExit:
    return status;
}

Status
xdfConditionStringLike(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    const char *vals[2];
    int stringLens[2];
    const char *string;
    const char *pattern;

    assert(argc == 2);

    status = xdfGetStringFromArgv(argc, vals, stringLens, argv);
    assert(status == StatusOk);

    string = vals[0];
    pattern = vals[1];

    return xdfMakeBoolIntoScalar(out, strMatch(pattern, string));
}

Status
xdfConditionStringContains(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    const char *vals[2];
    int stringLens[2];
    const char *string;
    int stringLen;
    const char *pattern;
    int patLen;
    bool boolVals[1];

    // The third argument is "ignoreCase" and is optional.  If it isn't
    // specified then the default is case sensitive.

    if (argv[0] == NULL) {
        // FNF cannot contain anything
        return xdfMakeBoolIntoScalar(out, false);
    }

    // First two args are strings
    status = xdfGetStringFromArgv(2, vals, stringLens, argv);
    assert(status == StatusOk);

    string = vals[0];
    stringLen = stringLens[0];

    pattern = vals[1];
    patLen = stringLens[1];

    boolVals[0] = false;

    if (argc == 3) {
        // Third arg is a boolean
        if (argv[2] == NULL) {
            return StatusEvalCastError;
        }
        status = xdfGetBoolFromArgv(1, boolVals, &argv[2]);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }
    }

    if (argc == 2 || boolVals[0] == false) {
        // Case sensitive
        return xdfMakeBoolIntoScalar(out,
                                     strBoyerMoore(string,
                                                   stringLen,
                                                   pattern,
                                                   patLen) != NULL);
    } else {
        // We don't have a case insensitive BoyerMoore function so use the
        // libc function.  Maybe not ideal but much better performance than
        // a map UDF.
        return xdfMakeBoolIntoScalar(out, strcasestr(string, pattern));
    }
}

Status
xdfConditionStringStartsWith(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out)
{
    Status status;
    const char *vals[2];
    int stringLens[2];
    const char *string;
    int stringLen;
    const char *pattern;
    int patLen;
    bool boolVals[1];

    // The third argument is "ignoreCase" and is optional.  If it isn't
    // specified then the default is case sensitive.

    if (argv[0] == NULL) {
        // FNF cannot contain anything
        return xdfMakeBoolIntoScalar(out, false);
    }

    // First two args are strings
    status = xdfGetStringFromArgv(2, vals, stringLens, argv);
    assert(status == StatusOk);

    string = vals[0];
    stringLen = stringLens[0];

    pattern = vals[1];
    patLen = stringLens[1];

    if (patLen > stringLen) {
        return xdfMakeBoolIntoScalar(out, false);
    }

    boolVals[0] = false;

    if (argc == 3) {
        // Third arg is a boolean
        if (argv[2] == NULL) {
            return StatusEvalCastError;
        }
        status = xdfGetBoolFromArgv(1, boolVals, &argv[2]);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }
    }

    if (argc == 2 || boolVals[0] == false) {
        // Case sensitive
        return xdfMakeBoolIntoScalar(out,
                                     strncmp(string, pattern, patLen) == 0);
    } else {
        // Case insensitive
        return xdfMakeBoolIntoScalar(out,
                                     strncasecmp(string, pattern, patLen) == 0);
    }
}

Status
xdfConditionStringEndsWith(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status;
    const char *vals[2];
    int stringLens[2];
    const char *string;
    int stringLen;
    const char *pattern;
    int patLen;
    bool boolVals[1];

    // The third argument is "ignoreCase" and is optional.  If it isn't
    // specified then the default is case sensitive.

    if (argv[0] == NULL) {
        // FNF cannot contain anything
        return xdfMakeBoolIntoScalar(out, false);
    }

    // First two args are strings
    status = xdfGetStringFromArgv(2, vals, stringLens, argv);
    assert(status == StatusOk);

    string = vals[0];
    stringLen = stringLens[0];

    pattern = vals[1];
    patLen = stringLens[1];

    if (patLen > stringLen) {
        return xdfMakeBoolIntoScalar(out, false);
    }

    boolVals[0] = false;

    if (argc == 3) {
        // Third arg is a boolean
        if (argv[2] == NULL) {
            return StatusEvalCastError;
        }
        status = xdfGetBoolFromArgv(1, boolVals, &argv[2]);
        if (status != StatusOk) {
            return StatusEvalCastError;
        }
    }

    if (argc == 2 || boolVals[0] == false) {
        // Case sensitive
        return xdfMakeBoolIntoScalar(out,
                                     strncmp(string + stringLen - patLen,
                                             pattern,
                                             patLen) == 0);
    } else {
        // Case insensitive
        return xdfMakeBoolIntoScalar(out,
                                     strncasecmp(string + stringLen - patLen,
                                                 pattern,
                                                 patLen) == 0);
    }
}

Status
xdfConditionStringRegex(void *context, int argc, Scalar *argv[], Scalar *out)
{
    Status status = StatusOk;
    const char *vals[2];
    int stringLens[2];
    const char *string;
    const char *pattern;
    regex_t regexComp;
    bool regexInited = false;
    int ret;
    bool answer = true;

    assert(argc == 2);

    status = xdfGetStringFromArgv(argc, vals, stringLens, argv);
    assert(status == StatusOk);

    string = vals[0];
    pattern = vals[1];

    // XXX Should think of a way to not create the DFA for the same regex
    // pattern for every record
    ret = regcomp(&regexComp, pattern, REG_NOSUB | REG_ICASE);
    if (ret != 0) {
        status = sysErrnoToStatus(ret);
        goto CommonExit;
    }
    regexInited = true;

    ret = regexec(&regexComp, string, 0, NULL, 0);
    if (ret == 0) {
        answer = true;
    } else if (ret == REG_NOMATCH) {
        answer = false;
    } else {
        status = sysErrnoToStatus(ret);
        goto CommonExit;
    }

CommonExit:
    if (regexInited) {
        regfree(&regexComp);
        regexInited = false;
    }

    if (status == StatusOk) {
        return xdfMakeBoolIntoScalar(out, answer);
    } else {
        return status;
    }
}

Status
xdfConditionFieldExists(void *context, int argc, Scalar *argv[], Scalar *out)
{
    if (argc > 1) {
        return StatusXdfWrongNumberOfArgs;
    } else if (argc == 0) {
        return xdfMakeBoolIntoScalar(out, false);
    } else {
        assert(argc == 1);
        return xdfMakeBoolIntoScalar(out,
                                     (argv[0] != NULL &&
                                      argv[0]->fieldNumValues != 0));
    }
}

Status
xdfConditionIsInteger(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeBoolIntoScalar(out,
                                 (argv[0]->fieldType == DfInt32 ||
                                  argv[0]->fieldType == DfInt64) &&
                                     argv[0]->fieldNumValues == 1);
}

Status
xdfConditionIsNumeric(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeBoolIntoScalar(out,
                                 argv[0]->fieldType == DfMoney &&
                                     argv[0]->fieldNumValues == 1);
}

Status
xdfConditionIsFloat(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeBoolIntoScalar(out,
                                 (argv[0]->fieldType == DfFloat32 ||
                                  argv[0]->fieldType == DfFloat64) &&
                                     argv[0]->fieldNumValues == 1);
}

Status
xdfConditionIsString(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeBoolIntoScalar(out,
                                 argv[0]->fieldType == DfString &&
                                     argv[0]->fieldNumValues == 1);
}

Status
xdfConditionIsBoolean(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    return xdfMakeBoolIntoScalar(out,
                                 argv[0]->fieldType == DfBoolean &&
                                     argv[0]->fieldNumValues == 1);
}

Status
xdfConditionIsNull(void *context, int argc, Scalar *argv[], Scalar *out)
{
    assert(argc == 1);
    if (argv[0] == NULL) {
        // this is Null. Note that fnf and null are treated the
        // same in backend
        return xdfMakeBoolIntoScalar(out, true);
    }

    return xdfMakeBoolIntoScalar(out,
                                 argv[0]->fieldType == DfNull &&
                                     argv[0]->fieldNumValues == 1);
}
