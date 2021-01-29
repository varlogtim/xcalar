// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "operators/XcalarEvalTypes.h"
#include "XcalarEvalInt.h"
#include "operators/Xdf.h"
#include "util/Random.h"

#define fnFooter(checkArgs)                                            \
    .enforceArgCheck = checkArgs, .isBuiltIn = true, .hook = {NULL},   \
    .cleanupFn = NULL, .refCount = {{0}, NULL}, .makeContextFn = NULL, \
    .delContextFn = NULL
#define makeEvalFn(fn)                             \
    {                                              \
        {.evalFn = fn}, .fnType = XcalarFnTypeEval \
    }
#define makeAggFn(fn)                            \
    {                                            \
        {.aggFn = fn}, .fnType = XcalarFnTypeAgg \
    }

// XDF can take in arguments of multiple types (e.g. eq takes in bool, double,
// and ints) but XDF must have exactly one type of output. If any XDF
// takes in multiple types of arguments, it's up to the implementation of the
// XDF to distinguish the behavior depending on input type
XcalarEvalRegisteredFn builtInFns[] = {
    // Arithmetic functions
    // floatCompare
    {.fnDesc =
         {
             "floatCompare",  // fnName
             "Compares floating point numbers with custom precision amount. "
             "floatCompare(a,b,d) returns 0 if a is within d of b, "
             "returns -1 if a < b, or returns 1 if a > b.",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Precision",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFloatCmp),
     fnFooter(EnforceArgCheck)},
    // add
    {.fnDesc =
         {
             "add",                           // fnName
             "add(a, b, ...) = a + b + ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformAdd),
     fnFooter(EnforceArgCheck)},
    // addInteger
    {.fnDesc =
         {
             "addInteger",  // fnName
             "addInteger(a, b, ...) = a + b + .... Arguments will first get "
             "truncated then add up",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformAddInteger),
     fnFooter(EnforceArgCheck)},
    // addNumeric
    {.fnDesc =
         {
             "addNumeric",                           // fnName
             "addNumeric(a, b, ...) = a + b + ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(
                             XdfAcceptInt | XdfAcceptString | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(
                             XdfAcceptInt | XdfAcceptString | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformAddNumeric),
     fnFooter(EnforceArgCheck)},
    // sub
    {.fnDesc =
         {
             "sub",                           // fnName
             "sub(a, b, ...) = a - b - ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformSub),
     fnFooter(EnforceArgCheck)},
    // subInteger
    {.fnDesc =
         {
             "subInteger",  // fnName
             "subInteger(a, b, ...) = a - b - .... Arguments will first get "
             "truncated",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformSubInteger),
     fnFooter(EnforceArgCheck)},
    // subNumeric
    {.fnDesc =
         {
             "subNumeric",                           // fnName
             "subNumeric(a, b, ...) = a + b + ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(
                             XdfAcceptInt | XdfAcceptString | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(
                             XdfAcceptInt | XdfAcceptString | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformSubNumeric),
     fnFooter(EnforceArgCheck)},
    // mult
    {.fnDesc =
         {
             "mult",                           // fnName
             "mult(a, b, ...) = a * b * ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformMult),
     fnFooter(EnforceArgCheck)},
    // multInteger
    {.fnDesc =
         {
             "multInteger",  // fnName
             "multInteger(a, b, ...) = a * b * .... Arguments will first get "
             "truncated",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformMultInteger),
     fnFooter(EnforceArgCheck)},
    // multNumeric
    {.fnDesc =
         {
             "multNumeric",                           // fnName
             "multNumeric(a, b, ...) = a + b + ...",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptString |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptString |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformMultNumeric),
     fnFooter(EnforceArgCheck)},
    // div
    {.fnDesc =
         {
             "div",                // fnName
             "div(a, b) = a / b",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) -- Cannot be zero!!",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformDiv),
     fnFooter(EnforceArgCheck)},
    // divNumeric
    {.fnDesc =
         {
             "divNumeric",                // fnName
             "divNumeric(a, b) = a / b",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptString |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) -- Cannot be zero!!",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptString |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformDivNumeric),
     fnFooter(EnforceArgCheck)},
    // mod
    {.fnDesc =
         {
             "mod",                                                 // fnName
             "mod(a, b) is the remainder after calculating a / b",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) -- Cannot be zero!!",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformMod),
     fnFooter(EnforceArgCheck)},
    // Notice how you can have the same function handle multiple XDFs
    // (abs and absInt)
    {.fnDesc =
         {
             "absInt",  // fnName
             "Takes the absolute value of an integer",
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformAbsInt),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "absNumeric",  // fnName
             "Takes the absolute value of an integer",
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformAbsNumberic),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "abs",  // fnName
             "Takes the absolute value of a number",
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformAbs),
     fnFooter(EnforceArgCheck)},
    // ceil
    {.fnDesc =
         {
             "ceil",                                                   // fnName
             "ceil(x) = smallest integer greater than or equal to x",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformCeil),
     fnFooter(EnforceArgCheck)},
    // floor
    {.fnDesc =
         {
             "floor",                                               // fnName
             "floor(x) = largest integer less than or equal to x",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFloor),
     fnFooter(EnforceArgCheck)},
    // round
    {.fnDesc =
         {
             "round",                                             // fnName
             "round to a number of decimal places (default: 0)",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "decimal places",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .int64Val = 0,
                             },
                         .defaultType = DfInt64,
                     },
                 },
         },
     makeEvalFn(xdfTransformRound),
     fnFooter(EnforceArgCheck)},
    // roundNumeric
    {.fnDesc =
         {
             "roundNumeric",                         // fnName
             "round to a number of decimal places",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptString |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "decimal places",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformRoundNumeric),
     fnFooter(EnforceArgCheck)},
    // pow
    {.fnDesc =
         {
             "pow",                                  // fnName
             "pow(x, y) = x raised to the y power",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (x)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (y)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformPow),
     fnFooter(EnforceArgCheck)},
    // exp
    {.fnDesc =
         {
             "exp",                               // fnName
             "exp(x) = e raised to the x power",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Operand 1 (x)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformExp),
     fnFooter(EnforceArgCheck)},
    // sqrt
    {.fnDesc =
         {
             "sqrt",         // fnName
             "Square root",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSqrt),
     fnFooter(EnforceArgCheck)},
    // log
    {.fnDesc =
         {
             "log",                         // fnName
             "Natural logarithm (base e)",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformLog),
     fnFooter(EnforceArgCheck)},
    // log2
    {.fnDesc =
         {
             "log2",              // fnName
             "Logarithm base 2",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformLog2),
     fnFooter(EnforceArgCheck)},
    // log10
    {.fnDesc =
         {
             "log10",              // fnName
             "Logarithm base 10",  // fnDesc
             .category = FunctionCategoryArithmetic,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformLog10),
     fnFooter(EnforceArgCheck)},

    // findMinIdx
    {
        .fnDesc =
            {
                "findMinIdx",  // fnName
                "findMinIdx(arg0, arg1, arg2, ...) returns x "
                "where argx is the smallest argument",  // fnDesc
                .category = FunctionCategoryArithmetic,
                .isSingletonOutput = true,
                .outputType = DfInt64,
                .context = NULL,
                .numArgs = -1,
                .argDescs = {},
            },
        makeEvalFn(xdfTransformFindMinIdx),
        fnFooter(DontEnforceArgCheck),
    },

    // Bitwise functions
    // bitLength
    {.fnDesc =
         {
             "bitLength",                         // fnName
             "Returns the bit length of 'expr'",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "expr",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitLength),
     fnFooter(EnforceArgCheck)},

    // octetLength
    {.fnDesc =
         {
             "octetLength",                        // fnName
             "Returns the byte length of 'expr'",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "expr",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformOctetLength),
     fnFooter(EnforceArgCheck)},

    // bitor
    {.fnDesc =
         {
             "bitor",  // fnName
             "bitwise or: Takes two bit patterns and performs the logical "
             "inclusive OR operation on each pair of corresponding bits. "
             "bitor(a, b) = a | b",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitOr),
     fnFooter(EnforceArgCheck)},
    // bitxor
    {.fnDesc =
         {
             "bitxor",  // fnName
             "bitwise exclusive or: Takes two bit patterns and performs the "
             "logical exclusive OR operation on each pair of corresponding "
             "bits. bitxor(a, b) = a ^ b",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitXor),
     fnFooter(EnforceArgCheck)},
    // bitand
    {.fnDesc =
         {
             "bitand",  // fnName
             "bitwise and: Takes two bit patterns and performs the logical "
             "inclusive AND operation on each pair of corresponding bits. "
             "bitand(a, b) = a & b",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitAnd),
     fnFooter(EnforceArgCheck)},
    // bitlshift
    {
        .fnDesc =
            {
                "bitlshift",  // fnName
                "bit left-shift: Returns 'a' shifted left by 'b' bits. "
                "bitlshift(a, b) = a << b",  // fnDesc
                .category = FunctionCategoryBitwise,
                .isSingletonOutput = true,
                .outputType = DfInt64,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "Operand 1 (a)",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "Operand 2 (b)",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                    },
            },
        makeEvalFn(xdfTransformBitLShift),
        fnFooter(EnforceArgCheck),
    },
    // bitrshift
    {.fnDesc =
         {
             "bitrshift",  // fnName
             "bit right-shift: Returns 'a' shifted right by 'b' bits. "
             "bitrshift(a, b) = a >> b",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitRShift),
     fnFooter(EnforceArgCheck)},
    // colsDefinedBitmap
    {.fnDesc =
         {
             "colsDefinedBitmap",  // fnName
             "colsDefinedBitmap: Returns a bitmap with where index i = 1 if "
             "field i exists ",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Operands",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptAll | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = 64,
                     },
                 },
         },
     makeEvalFn(xdfTransformColsDefinedBitmap),
     fnFooter(EnforceArgCheck)},
    // bitCount
    {.fnDesc =
         {
             "bitCount",  // fnName
             "bitCount: Returns count of 1's in the binary representation of "
             "input value e.g. bitCount(3) = 2",  // fnDesc
             .category = FunctionCategoryBitwise,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBitCount),
     fnFooter(EnforceArgCheck)},
    // Trigonometric functions
    // pi
    {.fnDesc =
         {
             "pi",                       // fnName
             "returns the value of pi",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 0,
             .argDescs = {},
         },
     makeEvalFn(xdfTransformPi),
     fnFooter(EnforceArgCheck)},
    // asin
    {.fnDesc =
         {
             "asin",                                                  // fnName
             "asin(value) returns the arcsine of value, in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformASin),
     fnFooter(EnforceArgCheck)},
    // atan
    {.fnDesc =
         {
             "atan",  // fnName
             "atan(value) returns the arctangent of value, in "
             "radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformATan),
     fnFooter(EnforceArgCheck)},
    // acos
    {.fnDesc =
         {
             "acos",  // fnName
             "acos(value) returns the arccosine of value, in "
             "radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformACos),
     fnFooter(EnforceArgCheck)},
    // atanh
    {.fnDesc =
         {
             "atanh",  // fnName
             "atanh(value) returns the hyperbolic arctangent of value, "
             "in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformATanh),
     fnFooter(EnforceArgCheck)},
    // acosh
    {.fnDesc =
         {
             "acosh",  // fnName
             "acosh(value) returns the hyperbolic arccosine of value, "
             "in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformACosh),
     fnFooter(EnforceArgCheck)},
    // asinh
    {.fnDesc =
         {
             "asinh",  // fnName
             "asinh(value) returns the hyperbolic arcsine of value, "
             "in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformASinh),
     fnFooter(EnforceArgCheck)},
    // atan2
    {.fnDesc =
         {
             "atan2",  // fnName
             "atan2(y, x) returns principal value of "
             "arctangent(y/x)",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "y",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "x",
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformATan2),
     fnFooter(EnforceArgCheck)},
    // sin
    {.fnDesc =
         {
             "sin",  // fnName
             "sin(angle) returns the sine of angle, with angle expressed "
             "in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSin),
     fnFooter(EnforceArgCheck)},
    // sinh
    {.fnDesc =
         {
             "sinh",  // fnName
             "sinh(angle) returns the hyperbolic sine of angle, with "
             "angle expressed in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSinh),
     fnFooter(EnforceArgCheck)},
    // cos
    {.fnDesc =
         {
             "cos",  // fnName
             "cos(angle) returns the cosine of angle, with angle "
             "expressed in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformCos),
     fnFooter(EnforceArgCheck)},
    // cosh
    {.fnDesc =
         {
             "cosh",  // fnName
             "cosh(angle) returns the hyperbolic cosine of angle, with "
             "angle expressed in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformCosh),
     fnFooter(EnforceArgCheck)},
    // tan
    {.fnDesc =
         {
             "tan",  // fnName
             "tan(angle) returns the tangent of angle, with angle "
             "expressed in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformTan),
     fnFooter(EnforceArgCheck)},
    // tanh
    {.fnDesc =
         {
             "tanh",  // fnName
             "tanh(angle) returns the hyperbolic tangent of angle, "
             "with angle expressed in radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "angle",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformTanh),
     fnFooter(EnforceArgCheck)},
    // radians
    {.fnDesc =
         {
             "radians",                     // fnName
             "Convert degrees to radians",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Degrees to convert",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformRadians),
     fnFooter(EnforceArgCheck)},
    // degrees
    {.fnDesc =
         {
             "degrees",                     // fnName
             "Convert radians to degrees",  // fnDesc
             .category = FunctionCategoryTrigonometry,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Radians to convert",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptFloat | XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformDegrees),
     fnFooter(EnforceArgCheck)},

    // Cast functions
    // int
    {.fnDesc =
         {
             "int",                                                    // fnName
             "int(x, [base]) -- Cast x to an integer in base [base]",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "x",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "base -- optional",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .int64Val = 10,
                             },
                         .defaultType = DfInt64,
                     },
                 },
         },
     makeEvalFn(xdfTransformInt),
     fnFooter(EnforceArgCheck)},

    // money (subset of numeric)
    {.fnDesc =
         {
             "money",                          // fnName
             "money(x) -- Cast x to a money",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "x",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformNumeric),
     fnFooter(EnforceArgCheck)},

    // float
    {.fnDesc =
         {
             "float",                          // fnName
             "float(x) -- Cast x to a float",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "x",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFloat),
     fnFooter(EnforceArgCheck)},

    // bool
    {.fnDesc =
         {
             "bool",                            // fnName
             "bool(x) -- Cast x to a boolean",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "x",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptFloat |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformBool),
     fnFooter(EnforceArgCheck)},

    // string
    {.fnDesc =
         {
             "string",                           // fnName
             "string(x) -- Cast x to a string",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "x",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformString),
     fnFooter(EnforceArgCheck)},

    // timestamp
    {.fnDesc =
         {
             "timestamp",  // fnName
             "Cast field to a timestamp. Numeric values will be treated "
             "as milliseconds since epoch. Strings will be interpreted using"
             " default format \"%Y-%m-%dT%H:%M:%SZ\"",  // fnDesc
             .category = FunctionCategoryCast,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "field",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptString | XdfAcceptInt |
                                                XdfAcceptTimestamp |
                                                XdfAcceptFloat |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     }},
         },
     makeEvalFn(xdfTransformTimestamp),
     fnFooter(EnforceArgCheck)},

    // convertToUnixTS
    {.fnDesc =
         {
             "convertToUnixTS",                                   // fnName
             "Converts a date string to a unix timestamp (UTC)",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConvertToUnixTS),
     fnFooter(EnforceArgCheck)},

    // convertFromUnixTS
    {.fnDesc =
         {
             "convertFromUnixTS",                                   // fnName
             "Converts a unix timestamp (UTC) into a date string",  // fnDesc
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "unix timestamp",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "output format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConvertFromUnixTS),
     fnFooter(EnforceArgCheck)},

    // convertDate
    {.fnDesc =
         {
             "convertDate",             // fnName
             "Reformat a date string",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "output format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConvertDate),
     fnFooter(EnforceArgCheck)},

    // dateAddMonth
    {.fnDesc =
         {
             "dateAddMonth",                                        // fnName
             "Add a number of months to a date (can be negative)",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "months",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfDateAddMonth),
     fnFooter(EnforceArgCheck)},
    // dateAddDay
    {.fnDesc =
         {
             "dateAddDay",                                        // fnName
             "Add a number of days to a date (can be negative)",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "days",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfDateAddDay),
     fnFooter(EnforceArgCheck)},
    // dateAddYear
    {.fnDesc =
         {
             "dateAddYear",                                        // fnName
             "Add a number of years to a date (can be negative)",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "years",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfDateAddYear),
     fnFooter(EnforceArgCheck)},

    // dateAddInterval
    {.fnDesc =
         {
             "dateAddInterval",                                      // fnName
             "Add years, months, days to a date (can be negative)",  // fnDesc
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 5,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "years",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "months",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "days",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfDateAddInterval),
     fnFooter(EnforceArgCheck)},

    // dateDiffDay
    {.fnDesc =
         {
             "dateDiffDay",  // fnName
             "Returns the difference in days between dateEnd and dateStart",
             .category = FunctionCategoryConversion,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "dateStart",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "dateEnd",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "input format",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfDateDiff),
     fnFooter(EnforceArgCheck)},

    // Misc functions
    // genUnique
    {.fnDesc =
         {
             "genUnique",                           // fnName
             "Generates a unique integer per row",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 0,
             .argDescs = {},
         },
     makeEvalFn(xdfTransformGenUnique),
     fnFooter(EnforceArgCheck)},

    // genRandom
    {
        .fnDesc =
            {
                "genRandom",                          // fnName
                "Generates a random number per row",  // fnDesc
                .category = FunctionCategoryMisc,
                .isSingletonOutput = true,
                .outputType = DfInt64,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "start",  // argDesc
                            .typesAccepted = (XdfTypesAccepted) XdfAcceptInt,
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "end",  // argDesc
                            .typesAccepted = (XdfTypesAccepted) XdfAcceptInt,
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                    },
            },
        makeEvalFn(xdfTransformGenRandom),
        .enforceArgCheck = EnforceArgCheck,
        .isBuiltIn = true,
        .hook = {NULL},
        .makeContextFn = initRndWeakHandle,
        .delContextFn = destroyRndWeakHandle,
        .cleanupFn = NULL,
        .refCount = {{0}, NULL},
    },

    // dhtHash
    {
        .fnDesc =
            {
                "dhtHash",                                // fnName
                "Generates the hashKey used by the dht",  // fnDesc
                .category = FunctionCategoryMisc,
                .isSingletonOutput = true,
                .outputType = DfInt64,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "field to compute the hashKeys of",  // argDesc
                            .typesAccepted = (XdfTypesAccepted) XdfAcceptAll,
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "mod by (default no mod)",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt |
                                                                XdfAcceptNull),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        },
                    },
            },
        makeEvalFn(xdfTransformDhtHash),
        fnFooter(EnforceArgCheck),
    },

    // xdbHash
    {
        .fnDesc =
            {
                "xdbHash",                                            // fnName
                "Determines which Xdb hash slot a key will hash to",  // fnDesc
                .category = FunctionCategoryMisc,
                .isSingletonOutput = true,
                .outputType = DfInt64,
                .context = NULL,
                .numArgs = 1,
                .argDescs =
                    {
                        {
                            "key",  // argDesc
                            .typesAccepted = (XdfTypesAccepted) XdfAcceptAll,
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                    },
            },
        makeEvalFn(xdfTransformXdbHash),
        fnFooter(EnforceArgCheck),
    },

    // String functions
    // ascii
    {.fnDesc =
         {
             "ascii",  // fnName
             "Returns the numeric value of the first character of 'str'",
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "str",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformAscii),
     fnFooter(EnforceArgCheck)},

    // chr
    {.fnDesc =
         {
             "chr",  // fnName
             "Returns the ASCII character having the binary equivalent to 'n'. "
             "If n is larger than 255 the result is equivalent to "
             "chr('n' % 256)",
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "n",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformChr),
     fnFooter(EnforceArgCheck)},

    // formatNumber
    {.fnDesc =
         {
             "formatNumber",  // fnName
             "Formats the number 'n' to a format like '#,###,###.##', "
             "rounded to 'd' decimal places.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "n",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "d",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFormatNumber),
     fnFooter(EnforceArgCheck)},

    // stringRPad
    {.fnDesc =
         {
             "stringRPad",  // fnName
             "Returns 'str', right-padded with 'pad' to a length of 'len'."
             "If 'str' is longer than 'len', the return value is shortened"
             "to 'len' characters",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "str",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "len",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pad",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformStringRPad),
     fnFooter(EnforceArgCheck)},

    // stringLPad
    {.fnDesc =
         {
             "stringLPad",  // fnName
             "Returns 'str', left-padded with 'pad' to a length of 'len'."
             "If 'str' is longer than 'len', the return value is shortened"
             "to 'len' characters",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "str",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "len",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pad",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformStringLPad),
     fnFooter(EnforceArgCheck)},

    // initCap
    {.fnDesc =
         {
             "initCap",  // fnName
             "Returns 'str', with the first letter of each word in uppercase,"
             "all other letters in lowercase. Words are delimited by "
             "whitespace.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "str",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformInitCap),
     fnFooter(EnforceArgCheck)},

    // stringReverse
    {.fnDesc =
         {
             "stringReverse",           // fnName
             "Returns 'str' reversed",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "str",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformStringReverse),
     fnFooter(EnforceArgCheck)},

    // wordCount
    {.fnDesc =
         {
             "wordCount",            // fnName
             "Get number of words",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "text",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformWordCount),
     fnFooter(EnforceArgCheck)},
    // len
    {.fnDesc =
         {
             "len",                     // fnName
             "Get length of a string",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "text",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformStringLen),
     fnFooter(EnforceArgCheck)},
    // countChar
    {.fnDesc =
         {
             "countChar",                                 // fnName
             "Get number of occurrences of a character",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Column name",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Character or String to count occurrences "
                         "of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformCharacterCount),
     fnFooter(EnforceArgCheck)},

    // replace
    {.fnDesc =
         {
             "replace",  // fnName
             "replace(origStr, searchStr, replaceStr, [ignoreCase]) -> Search "
             "and replace all instances of searchStr appearing in origStr with "
             "replaceStr (case-sensitive)",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "Original string to search",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Search string",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Replace string",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ignoreCase -- optional (defaults to "
                         "false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformReplace),
     fnFooter(EnforceArgCheck)},

    // concatenate
    {.fnDesc =
         {
             "concat",                                          // fnName
             "concat(str1, str2, ...) -> Concatenate strings",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string 1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Additional Strings",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformConcat),
     fnFooter(EnforceArgCheck)},

    // strip
    {
        .fnDesc =
            {
                "strip",  // fnName
                "strip(str1) -> Strip leading and trailing whitespace "
                "from a string. If delim is specified, strips delim instead",
                .category = FunctionCategoryString,
                .isSingletonOutput = true,
                .outputType = DfString,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "string",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptString),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "delim character",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(
                                    XdfAcceptString |
                                    XdfAcceptNull),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .stringVal.strSize = 0,
                                },
                            .defaultType = DfString,
                        }},
            },
        makeEvalFn(xdfTransformStrip),
        fnFooter(EnforceArgCheck)},

    // stripLeft
    {
        .fnDesc =
            {
                "stripLeft",  // fnName
                "stripLeft(str1) -> Strip leading whitespace "
                "from a string. If delim is specified, strips delim instead",
                .category = FunctionCategoryString,
                .isSingletonOutput = true,
                .outputType = DfString,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "string",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptString),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "delim character",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(
                                    XdfAcceptString |
                                    XdfAcceptNull),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .stringVal.strSize = 0,
                                },
                            .defaultType = DfString,
                        }},
            },
        makeEvalFn(xdfTransformStripLeft),
        fnFooter(EnforceArgCheck)},

    // stripRight
    {
        .fnDesc =
            {
                "stripRight",  // fnName
                "stripRight(str1) -> Strip trailing whitespace "
                "from a string",  // fnDesc
                .category = FunctionCategoryString,
                .isSingletonOutput = true,
                .outputType = DfString,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "string",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptString),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "delim character",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(
                                    XdfAcceptString |
                                    XdfAcceptNull),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .stringVal.strSize = 0,
                                },
                            .defaultType = DfString,
                        }},
            },
        makeEvalFn(xdfTransformStripRight),
        fnFooter(EnforceArgCheck)},

    // cut
    {.fnDesc =
         {
             "cut",  // fnName
             "cut(str,fieldNum,delim) -> Pull out field number "
             "<fieldNum> from <str> given delimiter <delim>",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to parse",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "field number",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "delimiter",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformCut),
     fnFooter(EnforceArgCheck)},

    // findInSet
    {.fnDesc =
         {
             "findInSet",  // fnName
             "findInSet(str,element) -> Find index of element in "
             "comma delimited str. 1-indexed.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "element",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFindInSet),
     fnFooter(EnforceArgCheck)},

    // find
    {.fnDesc =
         {
             "find",  // fnName
             "find(source, strToFind, beg, end) -> returns the first index of "
             "strToFind between indices beg and end in source "
             "(case-sensitive). "
             "Returns -1 if not found. ",
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "string to search",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "string to find",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "starting index",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ending index (use 0 to mean no ending "
                         "index)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformFind),
     fnFooter(EnforceArgCheck)},

    // rfind
    {.fnDesc =
         {
             "rfind",  // fnName
             "rfind(source, strToFind, beg, end) -> returns the last index of "
             "strToFind between indexes beg and end in source "
             "(case-sensitive). "
             "Returns -1 if not found. ",
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "string to search",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "string to find",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "starting index",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ending index (use 0 to mean no ending "
                         "index)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformRfind),
     fnFooter(EnforceArgCheck)},

    // substring
    {.fnDesc =
         {
             "substring",  // fnName
             "substring(str, startIdx, endIdx) -> creates a substring "
             "from <str> that starts at startIdx (inclusive) and ends at "
             "endIdx "
             "(exclusive). This command is the equivalent of the python "
             "command str[startIdx:endIdx]. To get str[startIdx:] equivalent, "
             "use substring(str, startIdx, 0). To get str[:endIdx] "
             "equivalent, use substring(str, 0, endIdx). To index back "
             "from the end of the string, use negative index "
             "values.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to parse",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "start index",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "end index",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSubstring),
     fnFooter(EnforceArgCheck)},
    // substringIndex
    {.fnDesc =
         {
             "substringIndex",  // fnName
             "substringIndex(str, delim, idx) -> returns a substring "
             "from <str> before count occurrences of delimiter delim. If count "
             "is positive, everything to the left of the final delimiter "
             "(counting from left) is returned. If count is negative, "
             "everything"
             " to the right of the final delimiter (counting from the right) "
             "is returned. The search is case sensitive.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to parse",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "delimiter",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "index",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSubstringIndex),
     fnFooter(EnforceArgCheck)},

    // repeat
    {.fnDesc =
         {
             "repeat",  // fnName
             "repeat(str, numTimes) -> repeats str for numTimes times. E.g. "
             "repeat(\"Hello\", 3) will result in HelloHelloHello.",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string to repeat",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "number of times",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformRepeat),
     fnFooter(EnforceArgCheck)},

    // Convert to uppercase
    {.fnDesc =
         {
             "upper",                                // fnName
             "upper(str) -- convert to upper case",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "string to convert",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformToUpper),
     fnFooter(EnforceArgCheck)},

    // Convert to lowercase
    {.fnDesc =
         {
             "lower",                                // fnName
             "lower(str) -- convert to lower case",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "string to convert",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformToLower),
     fnFooter(EnforceArgCheck)},

    {.fnDesc =
         {
             "soundEx",  // fnName
             "soundEx(name) -- "
             "Computes the American Soundex code for a given name",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "string to soundex",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformSoundEx),
     fnFooter(EnforceArgCheck)},

    {.fnDesc =
         {
             "levenshtein",  // fnName
             "levenshtein(str1, str2) -- "
             "Computes the Levenshtein edit distance between "
             "strings",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfInt32,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string 1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "string 2",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformLevenshtein),
     fnFooter(EnforceArgCheck)},

    {.fnDesc =
         {
             "concatDelim",  // fnName
             "concatDelim(delim, null_value, include_null, str1, str2) -- "
             "Concatenates str1, str2 with delimiter delim replaces FNF with "
             "null_value if include_null is set to true",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "delim",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "null value",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                         .defaultValue =
                             {
                                 .stringVal = {.strActual = "null",
                                               .strSize = 5},
                             },
                         .defaultType = DfString,
                     },
                     {
                         "include null",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptBoolean,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                         .defaultValue =
                             {
                                 .boolVal = true,
                             },
                         .defaultType = DfBoolean,
                     },
                     {
                         "Strings to concat",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfTransformConcatDelim),
     fnFooter(EnforceArgCheck)},

    {.fnDesc =
         {
             "stringsPosCompare",  // fnName
             "stringsPosCompare(str1, str2, delim, min_diff, max_diff) -- "
             "Splits str1, str2 by delim and returns True if difference"
             "is in between min_diff and max_diff, else False",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 5,
             .argDescs =
                 {
                     {
                         "string1",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "string2",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "delim",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptString,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "min_diff(inclusive)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptInt,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "max_diff(exclusive), should be greater than min_diff",  // argDesc
                         .typesAccepted = (XdfTypesAccepted) XdfAcceptInt,
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformStringPosCompare),
     fnFooter(EnforceArgCheck)},

    // Condition functions
    // eq
    {.fnDesc =
         {
             "eq",                  // fnName
             "eq(a, b) -> a == b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) Must be of same type as operand "
                         "1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionEq),
     fnFooter(EnforceArgCheck)},
    // eqNonNull
    {.fnDesc =
         {
             "eqNonNull",  // fnName
             "eq(a, b) -> a == b. Returns null if either a or b is null",
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) Must be of same type as operand "
                         "1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionEq),
     fnFooter(EnforceArgCheck)},
    // neq
    {.fnDesc =
         {
             "neq",                  // fnName
             "neq(a, b) -> a != b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b) Must be of same type as operand "
                         "1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionNeq),
     fnFooter(EnforceArgCheck)},
    // in
    {.fnDesc =
         {
             "in",  // fnName
             "in(a, x1, x2, x3, ..., xn): returns true if a is equal "
             "to any value xi where 1 <= i <= n. Otherwise returns false",
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "[x1, x2, ..., xn] Must be of same type as operand 1",
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptNull |
                                 XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = VariableArg,
                         .minArgs = 1,
                         .maxArgs = TupleMaxNumValuesPerRecord,
                     },
                 },
         },
     makeEvalFn(xdfConditionIn),
     fnFooter(EnforceArgCheck)},
    // gt
    {.fnDesc =
         {
             "gt",                 // fnName
             "gt(a, b) -> a > b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionGreaterThan),
     fnFooter(EnforceArgCheck)},
    // ge
    {.fnDesc =
         {
             "ge",                  // fnName
             "ge(a, b) -> a >= b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionGreaterThanOrEqualTo),
     fnFooter(EnforceArgCheck)},
    // lt
    {.fnDesc =
         {
             "lt",                 // fnName
             "lt(a, b) -> a < b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionLessThan),
     fnFooter(EnforceArgCheck)},
    // le
    {.fnDesc =
         {
             "le",                  // fnName
             "le(a, b) -> a <= b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionLessThanOrEqualTo),
     fnFooter(EnforceArgCheck)},
    // or
    {.fnDesc =
         {
             "or",                  // fnName
             "or(a, b) -> a || b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionLogicalOr),
     fnFooter(EnforceArgCheck)},
    // and
    {.fnDesc =
         {
             "and",                  // fnName
             "and(a, b) -> a && b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "Operand 1 (a)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Operand 2 (b)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionLogicalAnd),
     fnFooter(EnforceArgCheck)},
    // not
    {.fnDesc =
         {
             "not",           // fnName
             "not(a) -> !a",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "value",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionLogicalNot),
     fnFooter(EnforceArgCheck)},
    // between
    {.fnDesc =
         {
             "between",                          // fnName
             "between(x, a, b) -> a <= x <= b",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "Value to check (x)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Left value (a)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Right value (b)",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(
                                 XdfAcceptBoolean | XdfAcceptString |
                                 XdfAcceptInt | XdfAcceptTimestamp |
                                 XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionBetween),
     fnFooter(EnforceArgCheck)},
    // isInf
    {.fnDesc =
         {
             "isInf",                                         // fnName
             "Checks if float or numeric value is infinity",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Float value to check",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptFloat64 |
                                                             XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsInf),
     fnFooter(EnforceArgCheck)},
    // isNan
    {.fnDesc =
         {
             "isNan",  // fnName
             "Checks if float or numeric value is not a number (NaN)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Float value to check",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptFloat64 |
                                                             XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsNan),
     fnFooter(EnforceArgCheck)},
    // contains
    {.fnDesc =
         {
             "contains",  // fnName
             "contains(string, pattern, [ignoreCase]) -- "
             "does string contain pattern? (case-sensitive)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to match",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pattern",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ignoreCase -- optional (defaults to "
                         "false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfConditionStringContains),
     fnFooter(EnforceArgCheck)},
    // startsWith
    {.fnDesc =
         {
             "startsWith",  // fnName
             "startsWith(string, pattern, [ignoreCase]) -- "
             "does string start with pattern? (case-sensitive)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to match",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pattern",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ignoreCase -- optional (defaults to "
                         "false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfConditionStringStartsWith),
     fnFooter(EnforceArgCheck)},
    // endsWith
    {.fnDesc =
         {
             "endsWith",  // fnName
             "endsWith(string, pattern, [ignoreCase]) -- "
             "does string end with pattern? (case-sensitive)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "string to match",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pattern",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "ignoreCase -- optional (defaults to "
                         "false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfConditionStringEndsWith),
     fnFooter(EnforceArgCheck)},

    // like
    {.fnDesc =
         {
             "like",  // fnName
             "like(string, pattern) -- does pattern match string? "
             "pattern may contain the * wild card",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string to match",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "pattern (may contain the * wild card)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionStringLike),
     fnFooter(EnforceArgCheck)},

    // regex
    {.fnDesc =
         {
             "regex",                 // fnName
             "regex(string, regex)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "String to match",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Regular expression",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionStringRegex),
     fnFooter(EnforceArgCheck)},
    // exists
    {.fnDesc =
         {
             "exists",                                      // fnName
             "exists(fieldName) -- does field name exist",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionFieldExists),
     fnFooter(DontEnforceArgCheck)},
    // isInteger
    {.fnDesc =
         {
             "isInteger",             // fnName
             "isInteger(fieldName)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsInteger),
     fnFooter(EnforceArgCheck)},
    // isNumeric
    {.fnDesc =
         {
             "isNumeric",             // fnName
             "isNumeric(fieldName)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsNumeric),
     fnFooter(EnforceArgCheck)},
    // isFloat
    {.fnDesc =
         {
             "isFloat",             // fnName
             "isFloat(fieldName)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsFloat),
     fnFooter(EnforceArgCheck)},
    // isString
    {.fnDesc =
         {
             "isString",             // fnName
             "isString(fieldName)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsString),
     fnFooter(EnforceArgCheck)},
    // isBoolean
    {.fnDesc =
         {
             "isBoolean",             // fnName
             "isBoolean(fieldName)",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsBoolean),
     fnFooter(EnforceArgCheck)},
    // isNull
    {.fnDesc =
         {
             "isNull",                                      // fnName
             "returns true if the field has a null value",  // fnDesc
             .category = FunctionCategoryCondition,
             .isSingletonOutput = true,
             .outputType = DfBoolean,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "fieldName",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfConditionIsNull),
     fnFooter(DontEnforceArgCheck)},
    // Ternary conditional operator
    {.fnDesc =
         {
             "if",  // fnName
             "if(condition, conditionTrueValue, conditionFalseValue, "
             "NullAsFalse) -- if condition (a boolean expression) "
             "is true, return conditionTrueValue, if null, return null, "
             "otherwise, return conditionFalseValue, if NullAsFalse "
             "is specified as true, null is treated as false",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "condition to test",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is true",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is false",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "NullAsFalse -- optional (defaults "
                         "to false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformIf),
     fnFooter(EnforceArgCheck)},
    // Ternary conditional operator (string)
    {.fnDesc =
         {
             "ifStr",  // fnName
             "ifStr(condition, conditionTrueString, conditionFalseString, "
             "NullAsFalse) -- if condition (a boolean expression) "
             "is true, return conditionTrueString, if null, return null, "
             "otherwise, return conditionFalseString, if NullAsFalse "
             "is specified as true, null is treated as false",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "condition to test",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is true",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is false",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString |
                                                             XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "NullAsFalse -- optional (defaults "
                         "to false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformIfStr),
     fnFooter(EnforceArgCheck)},
    // Ternary conditional operator (int)
    {.fnDesc =
         {
             "ifInt",  // fnName
             "ifInt(condition, conditionTrueInt, conditionFalseInt, "
             "NullAsFalse) -- if condition (a boolean expression) "
             "is true, return conditionTrueInt, if null, return null, "
             "otherwise, return conditionFalseInt, if NullAsFalse "
             "is specified as true, null is treated as false",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "condition to test",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is true",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is false",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "NullAsFalse -- optional (defaults "
                         "to false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformIfInt),
     fnFooter(EnforceArgCheck)},
    // Ternary conditional operator (numeric)
    {.fnDesc =
         {
             "ifNumeric",  // fnName
             "ifNumeric(condition, conditionTrueInt, conditionFalseInt, "
             "NullAsFalse) -- if condition (a boolean expression) "
             "is true, return conditionTrueNumeric, if null, return null, "
             "otherwise, return conditionFalseNumeric, if NullAsFalse "
             "is specified as true, null is treated as false",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "condition to test",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is true",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptNull | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is false",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptNull | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "NullAsFalse -- optional (defaults "
                         "to false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformIfNumeric),
     fnFooter(EnforceArgCheck)},
    // Ternary conditional operator (timestamp)
    {.fnDesc =
         {
             "ifTimestamp",  // fnName
             "ifTimestamp(condition, conditionTrueTs, conditionFalseTs, "
             "NullAsFalse) -- if condition (a boolean expression) "
             "is true, return conditionTrueTs, if null, return null, "
             "otherwise, return conditionFalseTs, if NullAsFalse "
             "is specified as true, null is treated as false",  // fnDesc
             .category = FunctionCategoryMisc,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 4,
             .argDescs =
                 {
                     {
                         "condition to test",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat |
                                                XdfAcceptMoney | XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is true",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp |
                                                XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "value returned when condition is false",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp |
                                                XdfAcceptNull),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "NullAsFalse -- optional (defaults "
                         "to false)",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = false,
                             },
                         .defaultType = DfBoolean,
                     },
                 },
         },
     makeEvalFn(xdfTransformIfTimestamp),
     fnFooter(EnforceArgCheck)},

    // Aggregate functions
    // sum
    {.fnDesc =
         {
             "sum",                        // fnName
             "Sums the values in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to sum over",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateSumFloat64),
     fnFooter(EnforceArgCheck)},
    // avg
    {.fnDesc =
         {
             "avg",                                          // fnName
             "Computes the average (mean) value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute average (mean) value "
                         "of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateAverage),
     fnFooter(EnforceArgCheck)},
    // avgNumeric
    {.fnDesc =
         {
             "avgNumeric",                                   // fnName
             "Computes the average (mean) value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute average (mean) value "
                         "of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateAverageNumeric),
     fnFooter(EnforceArgCheck)},
    // max
    {.fnDesc =
         {
             "max",                               // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfScalarObj,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMax),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "maxString",                         // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMax),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "maxInteger",                        // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMax),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "maxFloat",                          // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMax),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "maxTimestamp",                      // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMax),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "maxNumeric",                        // fnName
             "Finds the maximum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute max value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMaxNumeric),
     fnFooter(EnforceArgCheck)},
    // min
    {.fnDesc =
         {
             "min",                                // fnName
             "Finds the minimum value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfScalarObj,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMin),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "minString",                          // fnName
             "Finds the minimum value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMin),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "minFloat",                           // fnName
             "Finds the minimum value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMin),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "minInteger",                         // fnName
             "Finds the minimum value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMin),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "minTimestamp",                       // fnName
             "Finds the minimum value in a set.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMin),
     fnFooter(EnforceArgCheck)},
    {.fnDesc =
         {
             "minNumeric",                        // fnName
             "Finds the minimum value in a set",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to compute min value of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateMinNumeric),
     fnFooter(EnforceArgCheck)},
    // count
    {.fnDesc =
         {
             "count",                              // fnName
             "Counts the occurrences of a field",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to count occurrences of",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptAll),
                         .isSingletonValue = false,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateCount),
     fnFooter(DontEnforceArgCheck)},
    // sumInteger
    {.fnDesc =
         {
             "sumInteger",  // fnName
             "Sums up all values in a set. Casts input and output "
             "to integers. Use this if double is not precise "
             "enough.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to sum over",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptInt | XdfAcceptFloat),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateSumInt64),
     fnFooter(EnforceArgCheck)},
    // sumNumeric
    {.fnDesc =
         {
             "sumNumeric",  // fnName
             "Sums up all values in a set. Casts input and output "
             "to numeric2. Use this if double is not precise "
             "enough.",  // fnDesc
             .category = FunctionCategoryAggregate,
             .isSingletonOutput = true,
             .outputType = DfMoney,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "Field name to sum over",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptMoney),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeAggFn(xdfAggregateSumNumeric),
     fnFooter(EnforceArgCheck)},
    // listAgg
    {
        .fnDesc =
            {
                "listAgg",                                        // fnName
                "Concatenates all strings of a column in order",  // fnDesc
                .category = FunctionCategoryAggregate,
                .isSingletonOutput = true,
                .outputType = DfString,
                .context = NULL,
                .numArgs = 2,
                .argDescs =
                    {
                        {
                            "Field name to concatenate over",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptString),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "delim",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(
                                    XdfAcceptString |
                                    XdfAcceptNone),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .stringVal.strSize = 0,
                                },
                            .defaultType = DfString,
                        }},
            },
        makeAggFn(xdfAggregateListAgg),
        fnFooter(EnforceArgCheck)},
    // explodeString
    {.fnDesc =
         {
             "explodeString",  // fnName
             "Splits a string based on delim and returns the results as "
             "multiple rows",  // fnDesc
             .category = FunctionCategoryString,
             .isSingletonOutput = false,
             .outputType = DfString,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "string",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "delim",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTransformExplodeString),
     fnFooter(EnforceArgCheck)},
    // addDateInterval
    {
        .fnDesc =
            {
                "addDateInterval",  // fnName
                "Add the specified year, month, day interval to start date",
                .category = FunctionCategoryTimestamp,
                .isSingletonOutput = true,
                .outputType = DfTimespec,
                .context = NULL,
                .numArgs = 4,
                .argDescs =
                    {
                        {
                            "start date",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptTimestamp),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "years",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        },
                        {
                            "months",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        },
                        {
                            "days",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        }},
            },
        makeEvalFn(xdfTimestampAddDateInterval),
        fnFooter(EnforceArgCheck)},
    // addTimeInterval
    {
        .fnDesc =
            {
                "addTimeInterval",  // fnName
                "Add the specified hour, miniutes, second, interval to start "
                "time",
                .category = FunctionCategoryTimestamp,
                .isSingletonOutput = true,
                .outputType = DfTimespec,
                .context = NULL,
                .numArgs = 4,
                .argDescs =
                    {
                        {
                            "start timestamp",  // argDesc
                            .typesAccepted =
                                (XdfTypesAccepted)(XdfAcceptTimestamp),
                            .isSingletonValue = true,
                            .argType = RequiredArg,
                        },
                        {
                            "hour",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        },
                        {
                            "minute",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        },
                        {
                            "second",  // argDesc
                            .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                            .isSingletonValue = true,
                            .argType = OptionalArg,
                            .defaultValue =
                                {
                                    .int64Val = 0,
                                },
                            .defaultType = DfInt64,
                        }},
            },
        makeEvalFn(xdfTimestampAddTimeInterval),
        fnFooter(EnforceArgCheck)},
    // addIntervalString
    {.fnDesc =
         {
             "addIntervalString",  // fnName
             "Add an interval formatted as "
             "years,months,days,hours,minutes,seconds to timestamp\n"
             "Seconds can be float all others must be int",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "start timestamp",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "interval string",  // argDesc
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTimestampAddIntervalString),
     fnFooter(EnforceArgCheck)},
    // dateDiff
    {.fnDesc =
         {
             "dateDiff",  // fnName
             "Returns the number of days from start date to end date",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {
                     {
                         "start date",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "end date",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                 },
         },
     makeEvalFn(xdfTimestampDateDiff),
     fnFooter(EnforceArgCheck)},
    // datePart
    {.fnDesc =
         {
             "datePart",  // fnName
             "Returns a part of a date\n"
             "Y = year\n"
             "Q = quarter\n"
             "M = month\n"
             "D = day\n"
             "W = day of week",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {{
                      "date",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptTimestamp),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  },
                  {
                      "part",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  }},
         },
     makeEvalFn(xdfTimestampDatePart),
     fnFooter(EnforceArgCheck)},
    // lastDayOfMonth
    {.fnDesc =
         {
             "lastDayOfMonth",  // fnName
             "Returns the last date of the month",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     }},
         },
     makeEvalFn(xdfTimestampLastDayOfMonth),
     fnFooter(EnforceArgCheck)},
    // dayOfYear
    {.fnDesc =
         {
             "dayOfYear",  // fnName
             "Returns the day number of the year",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     }},
         },
     makeEvalFn(xdfTimestampDayOfYear),
     fnFooter(EnforceArgCheck)},
    // timePart
    {.fnDesc =
         {
             "timePart",  // fnName
             "Returns a part of a timestamp\n"
             "H = hour\n"
             "M = minute\n"
             "S = second",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {{
                      "timestamp",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptTimestamp),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  },
                  {
                      "part",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  }

                 },
         },
     makeEvalFn(xdfTimestampTimePart),
     fnFooter(EnforceArgCheck)},
    // convertTimezone
    {.fnDesc =
         {
             "convertTimezone",  // fnName
             "Changes timzeone of a timestamp to a timezone specified by"
             "the offset in hours from UTC",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {{
                      "timestamp",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptTimestamp),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  },
                  {
                      "offset from UTC in hours",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptInt),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  }

                 },
         },
     makeEvalFn(xdfTimestampConvertTimezone),
     fnFooter(EnforceArgCheck)},

    // dateTrunc
    {.fnDesc =
         {
             "dateTrunc",  // fnName
             "Returns timestamp truncated to the unit specified by the format "
             "model fmt. "
             "fmt should be one of [\"YEAR\", \"YYYY\", \"YY\", \"MON\", "
             "\"MONTH\", "
             "\"MM\", \"DAY\", \"DD\", \"HOUR\", \"MINUTE\", \"SECOND\", "
             "\"WEEK\", \"QUARTER\"]",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {{
                      "date",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptTimestamp),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  },
                  {
                      "fmt",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  }},
         },
     makeEvalFn(xdfTimestampDateTrunc),
     fnFooter(EnforceArgCheck)},

    // monthsBetween
    {.fnDesc =
         {
             "monthsBetween",  // fnName
             "monthsBetween(date1, date2) returns "
             "the number of months between date1 and date2. "
             "If date1 is later than date2, the result is positive. "
             "If date1 is earlier than date2, the result is negative. "
             "If date1 and date2 either have the same day of the month or "
             "the last day of the month, the result is an integer. "
             "Otherwise, it calculates the fractional portion of the result "
             "based "
             "on a 31-days month and also considers the difference in the time "
             "components. "
             "Roundoff rounds to 8 fractional digits (not implemented). ",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfFloat64,
             .context = NULL,
             .numArgs = 3,
             .argDescs =
                 {
                     {
                         "Date 1",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Date 2",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     },
                     {
                         "Roundoff",  // For Spark plan compatibility, currently
                                      // not implemented
                         .typesAccepted = (XdfTypesAccepted)(XdfAcceptBoolean),
                         .isSingletonValue = true,
                         .argType = OptionalArg,
                         .defaultValue =
                             {
                                 .boolVal = true,
                             },
                         .defaultType = DfBoolean,
                     },

                 },
         },
     makeEvalFn(xdfTimestampMonthsBetween),
     fnFooter(EnforceArgCheck)},

    // nextDay
    {.fnDesc =
         {
             "nextDay",  // fnName
             "Returns the first date which is later than start_date and named "
             "as indicated.",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 2,
             .argDescs =
                 {{
                      "start_date",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptTimestamp),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  },
                  {
                      "day_of_week",  // argDesc
                      .typesAccepted = (XdfTypesAccepted)(XdfAcceptString),
                      .isSingletonValue = true,
                      .argType = RequiredArg,
                  }},
         },
     makeEvalFn(xdfTimestampNextDay),
     fnFooter(EnforceArgCheck)},

    // weekOfYear
    {.fnDesc =
         {
             "weekOfYear",  // fnName
             "Returns the week of the year of the given date. A week is "
             "considered"
             " to start on a Monday and week 1 is the first week with >3 days.",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfInt64,
             .context = NULL,
             .numArgs = 1,
             .argDescs =
                 {
                     {
                         "date",  // argDesc
                         .typesAccepted =
                             (XdfTypesAccepted)(XdfAcceptTimestamp),
                         .isSingletonValue = true,
                         .argType = RequiredArg,
                     }},
         },
     makeEvalFn(xdfTimestampWeekOfYear),
     fnFooter(EnforceArgCheck)},

    // now()
    {.fnDesc =
         {
             "now",  // fnName
             "Returns the current date and time.",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 0,
             .argDescs = {},
         },
     makeEvalFn(xdfTimestampNow),
     fnFooter(EnforceArgCheck)},
    // now()
    {.fnDesc =
         {
             "today",  // fnName
             "Returns the current date (as a timestamp).",
             .category = FunctionCategoryTimestamp,
             .isSingletonOutput = true,
             .outputType = DfTimespec,
             .context = NULL,
             .numArgs = 0,
             .argDescs = {},
         },
     makeEvalFn(xdfTimestampToday),
     fnFooter(EnforceArgCheck)},

};

const size_t builtInFnsCount = ArrayLen(builtInFns);
