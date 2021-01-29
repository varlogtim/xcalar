// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDF_H
#define _XDF_H

#include "primitives/Primitives.h"
#include "df/DataFormatTypes.h"
#include "scalars/Scalars.h"
#include "operators/XdfParallelDoTypes.h"

struct MsgEphemeral;
extern bool supportedMatrix[DfFieldTypeLen][DfFieldTypeLen];

// Transform functions. Impl found in src/lib/liboperators/xdf/TransformFns.c
Status xdfTransformFloatCmp(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfTransformAdd(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformAddInteger(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformAddNumeric(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformSub(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformSubInteger(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformSubNumeric(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformMult(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformMultInteger(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfTransformMultNumeric(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfTransformDiv(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformDivNumeric(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformMod(void *context, int argc, Scalar *argv[], Scalar *out);

Status xdfTransformBitLength(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformOctetLength(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfTransformBitOr(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformBitXor(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformBitAnd(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformBitLShift(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformBitRShift(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformColsDefinedBitmap(void *context,
                                     int argc,
                                     Scalar *argv[],
                                     Scalar *out);
Status xdfTransformBitCount(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);

Status xdfTransformPi(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformASin(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformATan(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformACos(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformATan2(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformACosh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformASinh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformATanh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformSin(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformCos(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformTan(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformSinh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformCosh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformTanh(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformLog2(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformLog10(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformLog(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformFindMinIdx(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformRadians(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformDegrees(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformAbs(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformAbsInt(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformAbsNumberic(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);

Status xdfTransformCeil(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformFloor(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformRound(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformRoundNumeric(void *context,
                                int argc,
                                Scalar *argv[],
                                Scalar *out);
Status xdfTransformPow(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformExp(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformSqrt(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformGenUnique(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformGenRandom(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformDhtHash(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformXdbHash(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformAscii(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformChr(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformFormatNumber(void *context,
                                int argc,
                                Scalar *argv[],
                                Scalar *out);
Status xdfTransformStringRPad(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformStringLPad(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTransformStringReverse(void *context,
                                 int argc,
                                 Scalar *argv[],
                                 Scalar *out);
Status xdfTransformInitCap(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformWordCount(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformStringLen(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformCharacterCount(void *context,
                                  int argc,
                                  Scalar *argv[],
                                  Scalar *out);
Status xdfTransformReplace(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformConcat(void *context,
                          int argc,
                          Scalar *argvIn[],
                          Scalar *out);
Status xdfTransformStrip(void *context,
                         int argc,
                         Scalar *argvIn[],
                         Scalar *out);
Status xdfTransformStripLeft(void *context,
                             int argc,
                             Scalar *argvIn[],
                             Scalar *out);
Status xdfTransformStripRight(void *context,
                              int argc,
                              Scalar *argvIn[],
                              Scalar *out);
Status xdfTransformCut(void *context, int argc, Scalar *argvIn[], Scalar *out);
Status xdfTransformFindInSet(void *context,
                             int argc,
                             Scalar *argvIn[],
                             Scalar *out);
Status xdfTransformFind(void *context, int argc, Scalar *argvIn[], Scalar *out);
Status xdfTransformRfind(void *context,
                         int argc,
                         Scalar *argvIn[],
                         Scalar *out);
Status xdfTransformSubstring(void *context,
                             int argc,
                             Scalar *argvIn[],
                             Scalar *out);
Status xdfTransformSubstringIndex(void *context,
                                  int argc,
                                  Scalar *argvIn[],
                                  Scalar *out);
Status xdfTransformRepeat(void *context,
                          int argc,
                          Scalar *argvIn[],
                          Scalar *out);
Status xdfTransformToUpper(void *context,
                           int argc,
                           Scalar *argvIn[],
                           Scalar *out);
Status xdfTransformToLower(void *context,
                           int argc,
                           Scalar *argvIn[],
                           Scalar *out);
Status xdfTransformConcatDelim(void *context,
                               int argc,
                               Scalar *argvIn[],
                               Scalar *out);
Status xdfTransformStringPosCompare(void *context,
                                    int argc,
                                    Scalar *argvIn[],
                                    Scalar *out);
Status xdfTransformTimestamp(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformInt(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformFloat(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformNumeric(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformBool(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformString(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConvertToUnixTS(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConvertFromUnixTS(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfConvertDate(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfDateAddMonth(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfDateAddYear(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfDateAddDay(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfDateDiff(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfDateAddInterval(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformIf(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformIfStr(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformIfInt(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformIfNumeric(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTransformIfTimestamp(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfTransformSoundEx(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTransformLevenshtein(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfTransformFnfToInt(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);

// Condition functions. Impl found in src/lib/liboperators/xdf/ConditionFns.c
Status xdfConditionEq(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConditionIn(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConditionNeq(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConditionGreaterThan(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfConditionGreaterThanOrEqualTo(void *context,
                                        int argc,
                                        Scalar *argv[],
                                        Scalar *out);
Status xdfConditionLessThan(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfConditionLessThanOrEqualTo(void *context,
                                     int argc,
                                     Scalar *argv[],
                                     Scalar *out);
Status xdfConditionLogicalAnd(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfConditionLogicalOr(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfConditionLogicalNot(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfConditionBetween(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfConditionIsInf(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConditionIsNan(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfConditionStringLike(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfConditionStringContains(void *context,
                                  int argc,
                                  Scalar *argv[],
                                  Scalar *out);
Status xdfConditionStringStartsWith(void *context,
                                    int argc,
                                    Scalar *argv[],
                                    Scalar *out);
Status xdfConditionStringEndsWith(void *context,
                                  int argc,
                                  Scalar *argv[],
                                  Scalar *out);
Status xdfConditionStringRegex(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfConditionFieldExists(void *context,
                               int argc,
                               Scalar *argv[],
                               Scalar *out);
Status xdfConditionIsInteger(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfConditionIsNumeric(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfConditionIsFloat(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfConditionIsString(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfConditionIsBoolean(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfConditionIsNull(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTransformExplodeString(void *context,
                                 int argc,
                                 Scalar *argv[],
                                 Scalar *out);

Status xdfTimestampAddDateInterval(void *context,
                                   int argc,
                                   Scalar *argv[],
                                   Scalar *out);
Status xdfTimestampAddTimeInterval(void *context,
                                   int argc,
                                   Scalar *argv[],
                                   Scalar *out);
Status xdfTimestampAddIntervalString(void *context,
                                     int argc,
                                     Scalar *argv[],
                                     Scalar *out);
Status xdfTimestampDateDiff(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfTimestampDatePart(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfTimestampLastDayOfMonth(void *context,
                                  int argc,
                                  Scalar *argv[],
                                  Scalar *out);
Status xdfTimestampDayOfYear(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTimestampTimePart(void *context,
                            int argc,
                            Scalar *argv[],
                            Scalar *out);
Status xdfTimestampConvertTimezone(void *context,
                                   int argc,
                                   Scalar *argv[],
                                   Scalar *out);
Status xdfTimestampDateTrunc(void *context,
                             int argc,
                             Scalar *argv[],
                             Scalar *out);
Status xdfTimestampMonthsBetween(void *context,
                                 int argc,
                                 Scalar *argv[],
                                 Scalar *out);
Status xdfTimestampNextDay(void *context,
                           int argc,
                           Scalar *argv[],
                           Scalar *out);
Status xdfTimestampWeekOfYear(void *context,
                              int argc,
                              Scalar *argv[],
                              Scalar *out);
Status xdfTimestampToday(void *context, int argc, Scalar *argv[], Scalar *out);
Status xdfTimestampNow(void *context, int argc, Scalar *argv[], Scalar *out);

// Aggregate functions. Impl found in src/lib/liboperators/xdf/AggregateFns.c
Status xdfAggregateSumFloat64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateSumInt64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateSumNumeric(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMaxFloat64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMaxInt64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMinFloat64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMinInt64(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateAverage(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateAverageNumeric(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateCount(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMax(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMaxNumeric(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMinNumeric(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateMin(ScalarGroupIter *groupIter, Scalar *out);
Status xdfAggregateListAgg(ScalarGroupIter *groupIter, Scalar *out);

// Functions to achieve parallel aggregates.
// Impl found in src/lib/liboperators/xdf/CommonFns.c
Status xdfLocalInit(XdfAggregateHandlers aggregateHandler,
                    XdfAggregateAccumulators *acc,
                    void *broadcastPacketIn,
                    size_t broadcastPacketSize);
Status xdfLocalAction(XdfAggregateHandlers aggregateHandler,
                      XdfAggregateAccumulators *acc,
                      int argc,
                      Scalar *argv[]);
void xdfMsgParallelDo(MsgEphemeral *eph, void *payload);
void xdfMsgParallelDoComplete(MsgEphemeral *eph, void *payload);

static inline bool
xdfIsValidArg(int argNum, DfFieldType argType, int numArgs, Scalar *argv[])
{
    return argNum < numArgs && argv[argNum] != NULL &&
           (argv[argNum]->fieldType == argType || argType == DfUnknown);
}

void xdfGetTmFromTimeval(DfTimeval *timeval, tm *time);
// updates the seconds portion of timeval using tm
void xdfUpdateTimevalFromTm(tm *time, DfTimeval *timeval);

Status xdfGetTimevalFromArgv(int argc, DfTimeval vals[], Scalar *argv[]);

Status xdfGetInt64FromArgv(int argc, int64_t vals[], Scalar *argv[]);
Status xdfGetUInt64FromArgv(int argc, uint64_t vals[], Scalar *argv[]);
Status xdfGetUIntptrFromArgv(int argc, uint64_t vals[], Scalar *argv[]);
Status xdfGetBoolFromArgv(int argc, bool vals[], Scalar *argv[]);
Status xdfGetFloat64FromArgv(int argc, float64_t vals[], Scalar *argv[]);
Status xdfGetNumericFromArgv(int argc, XlrDfp vals[], Scalar *argv[]);
Status xdfGetStringFromArgv(int argc,
                            const char *vals[],
                            int stringLens[],
                            Scalar *argv[]);
Status xdfMakeTimestampIntoScalar(Scalar *out, DfTimeval in);
Status xdfMakeUInt64IntoScalar(Scalar *out, uint64_t in);
Status xdfMakeInt64IntoScalar(Scalar *out, int64_t in);
Status xdfMakeFloatIntoScalar(Scalar *out, float in);
Status xdfMakeFloat64IntoScalar(Scalar *out, double in);
Status xdfMakeNumericIntoScalar(Scalar *out, XlrDfp in);
Status xdfMakeBoolIntoScalar(Scalar *out, bool in);
Status xdfMakeNullIntoScalar(Scalar *out);
Status xdfMakeStringIntoScalar(Scalar *out, const char *in, size_t inLen);
#endif  // _XDF_H
