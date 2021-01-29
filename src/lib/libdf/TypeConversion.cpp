// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <float.h>
#include <math.h>
#include <string.h>

#include <jansson.h>

#include "primitives/Primitives.h"
#include "df/DataFormat.h"
#include "DataFormatConstants.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "util/DFPUtils.h"

static constexpr const char *moduleName = "libdf";

using namespace df;

#ifndef FLT_DECIMAL_DIG
#define FLT_DECIMAL_DIG (9)
#endif
#ifndef DBL_DECIMAL_DIG
#define DBL_DECIMAL_DIG (17)
#endif

typedef Status (*DfConvertTypeFunc)(const DfFieldValue *inputFieldValue,
                                    DfFieldValue *outputFieldValue);

typedef Status (*DfConvertToStrFunc)(const DfFieldValue *inputFieldValue,
                                     DfFieldValue *outputFieldValue,
                                     size_t outputStrBufSize);

typedef Status (*DfConvertStrToIntFunc)(const DfFieldValue *inputFieldValue,
                                        DfFieldValue *outputFieldValue,
                                        Base inputBase);

/* *** static functions - Typeconversion *** */
Status
convertAssert(const DfFieldValue *inputFieldValue,
              DfFieldValue *outputFieldValue)
{
    assert(0);
    return StatusTypeConversionError;
}

Status
convertToStrAssert(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue,
                   size_t outputStrBufSize)
{
    assert(0);
    return StatusTypeConversionError;
}

// ================= conversion function for DfNull ===========================
static Status
convertNullToInt64(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = 0;
    return StatusOk;
}

static Status
convertNullToInt32(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = 0;
    return StatusOk;
}

static Status
convertNullToUInt64(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = 0;
    return StatusOk;
}

static Status
convertNullToUInt32(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = 0;
    return StatusOk;
}

static Status
convertNullToFloat64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = 0;
    return StatusOk;
}

static Status
convertNullToFloat32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = 0;
    return StatusOk;
}

static Status
convertNullToBool(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = false;
    return StatusOk;
}

static Status
convertNullToStr(const DfFieldValue *inputFieldValue,
                 DfFieldValue *outputFieldValue,
                 size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%s",
                   nullStrValue);
    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = NullStrValueLen + 1;

    return StatusOk;
}

static Status
convertNullToNumeric(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrDfpUInt32ToNumeric(&outputFieldValue->numericVal, 0);
    return StatusOk;
}

// ================= conversion function for String ===========================
//
static Status
convertStrToStr(const DfFieldValue *inputFieldValue,
                DfFieldValue *outputFieldValue,
                size_t outputStrBufSize)
{
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);

    if (outputStrBufSize < inputFieldValue->stringVal.strSize) {
        return StatusNoBufs;
    }

    size_t ret = strlcpy(outputFieldValue->stringValTmp,
                         inputFieldValue->stringValTmp,
                         inputFieldValue->stringVal.strSize);

    assert(ret < inputFieldValue->stringVal.strSize);

    // Cover the case when inputFieldValue.stringValTmp == ""
    if (ret == 0) {
        outputFieldValue->stringValTmp[0] = '\0';
    }
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertStrToInt64(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue,
                  Base inputBase)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->int64Val =
        strtoll(inputFieldValue->stringVal.strActual, &endPtr, inputBase);
    if (errno == EINVAL || errno == ERANGE ||
        endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToInt32(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue,
                  Base inputBase)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->int32Val =
        (uint32_t) strtoll(inputFieldValue->stringVal.strActual,
                           &endPtr,
                           inputBase);
    if (errno == EINVAL || errno == ERANGE ||
        endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToUInt64(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue,
                   Base inputBase)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->uint64Val =
        strtoull(inputFieldValue->stringVal.strActual, &endPtr, inputBase);
    if (errno == EINVAL || errno == ERANGE ||
        endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToUInt32(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue,
                   Base inputBase)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->uint32Val =
        (uint32_t) strtoull(inputFieldValue->stringVal.strActual,
                            &endPtr,
                            inputBase);
    if (errno == EINVAL || errno == ERANGE ||
        endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToFloat64(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->float64Val =
        strtod(inputFieldValue->stringVal.strActual, &endPtr);
    if (errno == ERANGE || endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    if ((::isnanl(outputFieldValue->float64Val) ||
         ::isinfl(outputFieldValue->float64Val)) &&
        endPtr[0] != '\0') {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToFloat32(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    char *endPtr;

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    errno = 0;
    outputFieldValue->float32Val =
        strtof(inputFieldValue->stringVal.strActual, &endPtr);
    if (errno == ERANGE || endPtr == inputFieldValue->stringVal.strActual) {
        return StatusTypeConversionError;
    }

    if ((::isnanf(outputFieldValue->float32Val) ||
         ::isinff(outputFieldValue->float32Val)) &&
        endPtr[0] != '\0') {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertStrToBoolean(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    const char *strStart;
    unsigned ii;

    for (ii = 0; ii < inputFieldValue->stringVal.strSize; ++ii) {
        strStart = &inputFieldValue->stringVal.strActual[ii];
        if (*strStart != ' ') {
            break;
        }
    }

    // 4 is the size of string "TRUE"
    if (inputFieldValue->stringVal.strSize - ii >= 4) {
        outputFieldValue->boolVal = (strncasecmp(strStart, "TRUE", 4) == 0);
    } else {
        outputFieldValue->boolVal = false;
    }

    return StatusOk;
}

/*
 * Fill in the "date" portion of a timestamp.  Input format may be either
 * YYYY-MM-DD or YYYYMMDD.
 */

static Status
parseDateFromString(struct tm *mytime, const char *strDate)
{
    if (strchr(strDate, '-') != NULL) {
        if (sscanf(strDate,
                   "%d-%d-%d",
                   &mytime->tm_year,
                   &mytime->tm_mon,
                   &mytime->tm_mday) != 3 ||
            mytime->tm_year < 1000 || mytime->tm_year > 9999) {
            return StatusInval;
        }
    } else {
        char *end;
        unsigned long long date = strtoull(strDate, &end, 10);
        // Many databases (e.g. Postgres) do not allow dates < 1000 AD or > 9999
        // AD
        if (date < 10000000ull || date > 99991231ull) {
            return StatusInval;
        }
        lldiv_t div = lldiv(date, 100);
        mytime->tm_mday = div.rem;

        div = lldiv(div.quot, 100);
        mytime->tm_mon = div.rem;
        mytime->tm_year = div.quot;
    }
    mytime->tm_mon -= 1;
    mytime->tm_year -= 1900;

    return StatusOk;
}

static int
parseMilliseconds(char *begin, char **end)
{
    int msec = strtol(begin, end, 10);
    int diff = *end - begin;
    if (diff > 3) {
        msec /= pow(10, diff - 3);
    } else if (diff < 3) {
        msec *= (diff == 1 ? 100 : 10);
    }

    return msec;
}
/*
 * SDK-799 requested that this conversion fully support ISO-8601 timestamps.
 * ISO-8601 defines several related timestamp formats, including the following:
 *     YYYY:MM:DD  (Date, without time)
 *     YYYYMMDD    (Date, without time)
 *     YYYY:MM:DDThh:mm:ss (date/time, with delimiters, in local time)
 *     YYYY:MM:DDThh:mm:ss.ffff (date/time with fractional seconds, localtime)
 *     YYYY:MM:DDThh:mm:ssZ (date/time, with delimiters in UCT)
 *     YYYY:MM:DD hh:mm:ssZ (date/time, no 'T' delimiter, with colon delimters,
 * UCT) YYYY:MM:DDThh:mm:ss+HH:MM (date/time, with delimiters, offset from UCT)
 *     YYYY:MM:DDThh:mm:ss-HH (date/time, with delimiters, offset from UCT)
 *     YYYYMMDDThhmmss (date/time, without delimiters, in local time)
 *     YYYYMMDDThhmmssU (date/time, without delimiters, in PST (timezone 'U'))
 *
 *     etc.
 */
static Status
convertStrToTimestamp(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    constexpr int MaxLength = 64;

    int msec = 0;
    int tzoff = 0;
    char *end;
    struct tm mytime;
    memZero(&mytime, sizeof(mytime));
    Status status{StatusOk};

    char tmpString[MaxLength];

    status =
        strStrlcpy(tmpString, inputFieldValue->stringVal.strActual, MaxLength);
    if (status != StatusOk) {
        return status;
    }

    // ISO8601 permits either a 'T' or a ' ' to delimit date from time.
    // It must be no more than 10 characters into the string, though.
    // This is important, because 'T' is also a valid time zone spec.
    char *delim = strchr(tmpString, 'T');
    if (delim == NULL || delim - tmpString > 10) {
        delim = strchr(tmpString, ' ');
    }

    // Null-terminate this, otherwise parseDateFromString may choke if
    // we have something like "20180101T180555-0800, which is a valid
    // timestamp
    if (delim != NULL) {
        *delim = '\0';
        ++delim;
    }

    time_t utcTime = 0;

    status = parseDateFromString(&mytime, tmpString);
    BailIfFailed(status);

    if (delim == NULL) {
        utcTime = timegm(&mytime);
        tzoff = 0;
    } else {
        char timeZone[MaxLength];
        char *period;
        timeZone[0] = '\0';

        period = strchr(delim, '.');
        if (strchr(delim, ':')) {
            // Did we specify fractional seconds?
            if (period) {
                if (sscanf(delim,
                           "%d:%d:%d",
                           &mytime.tm_hour,
                           &mytime.tm_min,
                           &mytime.tm_sec) < 2) {
                    status = StatusInval;
                    goto CommonExit;
                }

                msec = parseMilliseconds(period + 1, &end);
                status = strStrlcpy(timeZone, end, MaxLength);
                BailIfFailed(status);

                // whole seconds only
            } else {
                if (sscanf(delim,
                           "%d:%d:%d%s",
                           &mytime.tm_hour,
                           &mytime.tm_min,
                           &mytime.tm_sec,
                           timeZone) < 2) {
                    status = StatusInval;
                    goto CommonExit;
                }
            }
        } else {
            int64_t longtime = strtoull(delim, &end, 10);
            // Must have at least HHMM.  Secs are optional
            if (end - delim < 4) {
                status = StatusInval;
            } else if (end - delim == 4) {
                // Seconds not specified, shift over by 100
                longtime *= 100;
            }

            if (*end == '.') {
                msec = parseMilliseconds(period + 1, &end);
            }
            strlcpy(timeZone, end, MaxLength);

            lldiv_t result = lldiv(longtime, 100);
            mytime.tm_sec = result.rem;

            result = lldiv(result.quot, 100);
            mytime.tm_min = result.rem;
            mytime.tm_hour = result.quot;
        }

        // Did we find a timezone?
        if (timeZone[0] != '\0') {
            int hours = 0, mins = 0;
            if (timeZone[0] == '+' || timeZone[0] == '-') {
                // Possiblities are +-HH:MM, +-HHMM, +-HH
                // We parse the minutes, but right now we don't support
                // them. To be supported in a future release
                char *colon;

                colon = strchr(timeZone + 1, ':');
                if (colon != NULL) {
                    *colon = '\0';
                    hours = strtol(timeZone + 1, &end, 10);
                    mins = strtol(colon + 1, &end, 10);
                } else {
                    hours = strtol(timeZone + 1, &end, 10);
                    if (strnlen(timeZone + 1, MaxLength) > 2) {
                        div_t result = div(hours, 100);
                        mins = result.rem;
                        hours = result.quot;
                    }
                }
                if (mins != 0) {
                    status = StatusUnimpl;
                    goto CommonExit;
                }

                if (timeZone[0] == '+') {
                    tzoff = hours;  // (We actually convert to a "whole" TZ and
                                    // report as such)
                } else {
                    tzoff = -hours;
                }
            } else {
                if (timeZone[0] == 'Z') {
                    // GMT, do nothing
                } else if ('N' <= timeZone[0] && timeZone[0] <= 'Y') {
                    // Timezone is behind GMT, zone 'N'=UCT-1:00,
                    // 'O'=UCT-2:00, etc
                    tzoff = 'M' - timeZone[0];
                } else if ('A' <= timeZone[0] && timeZone[0] <= 'I') {
                    // Timezone is ahead of GMT, zone 'A'=UCT+1:00,
                    // 'B'=UCT+2:00
                    tzoff = timeZone[0] - 'A' + 1;
                    // There is no 'J' zone.  we go straight from I to K.
                } else if ('K' <= timeZone[0] && timeZone[0] <= 'M') {
                    // Timezone is ahead of GMT, subtract hour offset
                    tzoff = timeZone[0] - 'K' + 10;
                } else {
                    // Invalid timezone
                    status = StatusInval;
                    goto CommonExit;
                }
            }

            mytime.tm_hour -= tzoff;  // Correct for GMT

            utcTime = timegm(&mytime);
        } else {
            // Time specified in local time.  Report as such with the
            // correct offset.
            tzoff = (-timezone / 3600);
            mytime.tm_hour -= tzoff;
            utcTime = timegm(&mytime);
        }
    }

    outputFieldValue->timeVal.ms = utcTime * 1000 + msec;
    outputFieldValue->timeVal.tzoffset = tzoff;

CommonExit:
    return status;
}

static Status
convertStrToNumeric(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    Status status;
    DFPUtils *dfp = DFPUtils::get();

    assert(inputFieldValue->stringVal.strActual != NULL);
    assert(inputFieldValue->stringVal.strSize > 0);

    status = dfp->xlrNumericFromString(&outputFieldValue->numericVal,
                                       inputFieldValue->stringVal.strActual);

    return status;
}

// ================= conversion function for Int32
// ===========================
static Status
convertInt32ToInt32(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToStr(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue,
                  size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%d",
                   inputFieldValue->int32Val);
    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertInt32ToUInt32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t) inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToInt64(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t) inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToUInt64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t) inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToFloat32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = (float32_t) inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToFloat64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t) inputFieldValue->int32Val;
    return StatusOk;
}

static Status
convertInt32ToBoolean(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->int32Val == 0 ? false : true;
    return StatusOk;
}

static Status
convertInt32ToNumeric(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrDfpInt32ToNumeric(&outputFieldValue->numericVal,
                              inputFieldValue->int32Val);
    return StatusOk;
}

// ================= conversion function for UInt32
// ===========================
static Status
convertUInt32ToUInt32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToStr(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue,
                   size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%u",
                   inputFieldValue->uint32Val);
    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertUInt32ToInt32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t) inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToInt64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t) inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToUInt64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t) inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToFloat32(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = (float32_t) inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToFloat64(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t) inputFieldValue->uint32Val;
    return StatusOk;
}

static Status
convertUInt32ToBoolean(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->uint32Val == 0 ? false : true;
    return StatusOk;
}

static Status
convertUInt32ToNumeric(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrDfpUInt32ToNumeric(&outputFieldValue->numericVal,
                               inputFieldValue->uint32Val);
    return StatusOk;
}

// ================= conversion function for Int64
// ===========================
static Status
convertInt64ToInt64(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToStr(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue,
                  size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%lld",
                   (long long int) inputFieldValue->int64Val);

    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertInt64ToInt32(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t) inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToUInt32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t) inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToUInt64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t) inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToTimestamp(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->timeVal.ms = (int64_t) inputFieldValue->int64Val;
    outputFieldValue->timeVal.tzoffset = 0;
    return StatusOk;
}

static Status
convertInt64ToFloat32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = (float32_t) inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToFloat64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t) inputFieldValue->int64Val;
    return StatusOk;
}

static Status
convertInt64ToBoolean(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->int64Val == 0 ? false : true;
    return StatusOk;
}

static Status
convertInt64ToNumeric(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    return DFPUtils::get()->xlrDfpInt64ToNumeric(&outputFieldValue->numericVal,
                                                 inputFieldValue->int64Val);
}

// ================= conversion function for UInt64
// ===========================
static Status
convertUInt64ToUInt64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToTimestamp(const DfFieldValue *inputFieldValue,
                         DfFieldValue *outputFieldValue)
{
    outputFieldValue->timeVal.ms = (int64_t) inputFieldValue->uint64Val;
    outputFieldValue->timeVal.tzoffset = 0;

    return StatusOk;
}

static Status
convertUInt64ToStr(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue,
                   size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%llu",
                   (unsigned long long) inputFieldValue->uint64Val);

    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertUInt64ToInt32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t) inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToUInt32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t) inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToInt64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t) inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToFloat32(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = (float32_t) inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToFloat64(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t) inputFieldValue->uint64Val;
    return StatusOk;
}

static Status
convertUInt64ToBoolean(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->uint64Val == 0 ? false : true;
    return StatusOk;
}

static Status
convertUInt64ToNumeric(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    return DFPUtils::get()->xlrDfpUInt64ToNumeric(&outputFieldValue->numericVal,
                                                  inputFieldValue->uint64Val);
}

// ================= conversion function for float32
// ==========================
static Status
convertFloat32ToFloat32(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToStr(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue,
                    size_t outputStrBufSize)
{
    int ret;

    assertStatic(sizeof(float) == sizeof(float32_t));

    if (isnan(inputFieldValue->float32Val) ||
        isinf(inputFieldValue->float32Val)) {
        ret = snprintf(outputFieldValue->stringValTmp,
                       outputStrBufSize,
                       "\"%f\"",
                       inputFieldValue->float32Val);
    } else {
        char decimalRep[256];
        assertStatic(sizeof(float) == sizeof(float32_t));
        ret = snprintf(decimalRep,
                       sizeof(decimalRep),
                       "%.*g",
                       FLT_DECIMAL_DIG,
                       inputFieldValue->float32Val);
        if ((size_t) ret >= sizeof(decimalRep)) {
            return StatusOverflow;
        }
        // Figure out if decimalRep contains a decimal.
        // In order to maintain json type consistency, (in this case
        // json_real), we need a .0 if the number is a whole number
        if (strchr(decimalRep, '.') == NULL) {
            ret = snprintf(outputFieldValue->stringValTmp,
                           outputStrBufSize,
                           "%s.0",
                           decimalRep);
        } else {
            ret = snprintf(outputFieldValue->stringValTmp,
                           outputStrBufSize,
                           "%s",
                           decimalRep);
        }
    }

    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertFloat32ToInt32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t) inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToUInt32(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t) inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToInt64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t) inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToTimestamp(const DfFieldValue *inputFieldValue,
                          DfFieldValue *outputFieldValue)
{
    outputFieldValue->timeVal.ms = (int64_t) inputFieldValue->float32Val;
    outputFieldValue->timeVal.tzoffset = 0;

    return StatusOk;
}

static Status
convertFloat32ToUInt64(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t) inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToFloat64(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t) inputFieldValue->float32Val;
    return StatusOk;
}

static Status
convertFloat32ToBoolean(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->float32Val == 0 ? false : true;
    return StatusOk;
}

// ================= conversion function for float64
// ==========================
static Status
convertFloat64ToFloat64(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = inputFieldValue->float64Val;
    return StatusOk;
}

// we have to be careful here to avoid truncation due to rounding,
// as values we present to the user may come back to us in the form
// of a filter.  any rounding introduces an eplison difference between
// the value the filter tests and the actual value, causing things like
// equality to yield false negatives.  A 16 digit hexadecimal value
// should require at most no more than 17 decimal digits.  An 8 digit
// hexadecimal value should require at most no more than 9 decimal
// digits
static Status
convertFloat64ToStr(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue,
                    size_t outputStrBufSize)
{
    int ret;

    assertStatic(sizeof(double) == sizeof(float64_t));

    if (isnan(inputFieldValue->float64Val) ||
        isinf(inputFieldValue->float64Val)) {
        ret = snprintf(outputFieldValue->stringValTmp,
                       outputStrBufSize,
                       "%lf",
                       inputFieldValue->float64Val);
    } else {
        char decimalRep[256];
        assertStatic(sizeof(double) == sizeof(float64_t));
        ret = snprintf(decimalRep,
                       sizeof(decimalRep),
                       "%.*g",
                       DBL_DECIMAL_DIG,
                       inputFieldValue->float64Val);
        if ((size_t) ret >= sizeof(decimalRep)) {
            return StatusOverflow;
        }
        // Figure out if decimalRep contains a decimal.
        // In order to maintain json type consistency, (in this case
        // json_real), we need a .0 if the number is a whole number
        if (strchr(decimalRep, '.') == NULL) {
            ret = snprintf(outputFieldValue->stringValTmp,
                           outputStrBufSize,
                           "%s.0",
                           decimalRep);
        } else {
            ret = snprintf(outputFieldValue->stringValTmp,
                           outputStrBufSize,
                           "%s",
                           decimalRep);
        }
    }

    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertFloat64ToInt32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t) inputFieldValue->float64Val;
    return StatusOk;
}

static Status
convertFloat64ToUInt32(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t) inputFieldValue->float64Val;
    return StatusOk;
}

static Status
convertFloat64ToInt64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t) inputFieldValue->float64Val;
    return StatusOk;
}

static Status
convertFloat64ToTimestamp(const DfFieldValue *inputFieldValue,
                          DfFieldValue *outputFieldValue)
{
    outputFieldValue->timeVal.ms = (int64_t) inputFieldValue->float64Val;
    outputFieldValue->timeVal.tzoffset = 0;

    return StatusOk;
}

static Status
convertFloat64ToUInt64(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t) inputFieldValue->float64Val;
    return StatusOk;
}

static Status
convertFloat64ToFloat32(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val = (float32_t) inputFieldValue->float64Val;
    return StatusOk;
}

static Status
convertFloat64ToBoolean(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->float64Val == 0 ? false : true;
    return StatusOk;
}

static Status
convertFloat32ToNumeric(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    Status status;
    char buf[DFPUtils::ieeeDblStrChars];
    DFPUtils *dfp = DFPUtils::get();

    // Convert from scientific notation to avoid huge digit counts for
    // large/small numbers. This string is only used internally.
    status = strSnprintf(buf,
                         sizeof(buf),
                         "%.*e",
                         DBL_DECIMAL_DIG,
                         inputFieldValue->float32Val);
    BailIfFailed(status);

    status = dfp->xlrNumericFromString(&outputFieldValue->numericVal, buf);
    BailIfFailed(status);

CommonExit:
    return status;
}

static Status
convertFloat64ToNumeric(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    return DFPUtils::get()
        ->xlrDfpFloat64ToNumeric(&outputFieldValue->numericVal,
                                 inputFieldValue->float64Val);
}

// ================= conversion function for Money
// ==========================
static Status
convertNumericToNumeric(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->numericVal = inputFieldValue->numericVal;
    return StatusOk;
}

static Status
convertNumericToStr(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue,
                    size_t outputStrBufSize)
{
    size_t ret;
    char buf[XLR_DFP_STRLEN + 1];
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrNumericToString(buf, &inputFieldValue->numericVal);
    ret = strlcpy(outputFieldValue->stringValTmp, buf, outputStrBufSize);

    if (ret >= outputStrBufSize) {
        return StatusNoBufs;
    }

    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);

    // Cover the case when inputFieldValue.stringValTmp == ""
    if (ret == 0) {
        outputFieldValue->stringValTmp[0] = '\0';
    }
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertNumericToInt32(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    outputFieldValue->int32Val =
        dfp->xlrDfpNumericToInt32(&inputFieldValue->numericVal);

    return StatusOk;
}

static Status
convertNumericToUInt32(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    outputFieldValue->uint32Val =
        dfp->xlrDfpNumericToUInt32(&inputFieldValue->numericVal);

    return StatusOk;
}

static Status
convertNumericToInt64(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue)
{
    char buf[XLR_DFP_STRLEN];
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrNumericToString(buf, &inputFieldValue->numericVal);
    outputFieldValue->int64Val = (int64_t) strtol(buf, NULL, 10);

    return StatusOk;
}

static Status
convertNumericToUInt64(const DfFieldValue *inputFieldValue,
                       DfFieldValue *outputFieldValue)
{
    char buf[XLR_DFP_STRLEN];
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrNumericToString(buf, &inputFieldValue->numericVal);
    outputFieldValue->uint64Val = (uint64_t) strtoul(buf, NULL, 10);

    return StatusOk;
}

static Status
convertNumericToFloat64(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    return DFPUtils::get()
        ->xlrDfpNumericToFloat64(&outputFieldValue->float64Val,
                                 &inputFieldValue->numericVal);
}

static Status
convertNumericToFloat32(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    char *endPtr;

    char buf[XLR_DFP_STRLEN];
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrNumericToString(buf, &inputFieldValue->numericVal);
    errno = 0;
    outputFieldValue->float32Val = strtof(buf, &endPtr);
    if (errno == ERANGE || endPtr == buf) {
        return StatusTypeConversionError;
    }

    return StatusOk;
}

static Status
convertNumericToBoolean(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    outputFieldValue->boolVal =
        !dfp->xlrDfpIsZero(&inputFieldValue->numericVal);

    return StatusOk;
}

static Status
convertNumericToTimestamp(const DfFieldValue *inputFieldValue,
                          DfFieldValue *outputFieldValue)
{
    char buf[XLR_DFP_STRLEN];
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrNumericToString(buf, &inputFieldValue->numericVal);
    outputFieldValue->timeVal.ms = (uint64_t) strtoul(buf, NULL, 10);
    outputFieldValue->timeVal.tzoffset = 0;
    return StatusOk;
}

// ================= conversion function for boolean
// ==========================
static Status
convertBoolToBool(const DfFieldValue *inputFieldValue,
                  DfFieldValue *outputFieldValue)
{
    outputFieldValue->boolVal = inputFieldValue->boolVal;
    return StatusOk;
}

static Status
convertBoolToStr(const DfFieldValue *inputFieldValue,
                 DfFieldValue *outputFieldValue,
                 size_t outputStrBufSize)
{
    int ret;

    ret = snprintf(outputFieldValue->stringValTmp,
                   outputStrBufSize,
                   "%s",
                   inputFieldValue->boolVal ? "True" : "False");

    if (ret < 0) {
        return StatusTypeConversionError;
    }
    if (ret >= (int) outputStrBufSize) {
        return StatusNoBufs;
    }
    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertBoolToInt32(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue)
{
    outputFieldValue->int32Val = (int32_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToUInt32(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint32Val = (uint32_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToInt64(const DfFieldValue *inputFieldValue,
                   DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToUInt64(const DfFieldValue *inputFieldValue,
                    DfFieldValue *outputFieldValue)
{
    outputFieldValue->uint64Val = (uint64_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToFloat32(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->float32Val =
        (float32_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToFloat64(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val =
        (float64_t)(inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

static Status
convertBoolToNumeric(const DfFieldValue *inputFieldValue,
                     DfFieldValue *outputFieldValue)
{
    DFPUtils *dfp = DFPUtils::get();

    dfp->xlrDfpUInt32ToNumeric(&outputFieldValue->numericVal,
                               inputFieldValue->boolVal ? 1 : 0);
    return StatusOk;
}

// ================= conversion function for timestamp
// ========================
static Status
convertTimestampToInt64(const DfFieldValue *inputFieldValue,
                        DfFieldValue *outputFieldValue)
{
    outputFieldValue->int64Val = (int64_t)(inputFieldValue->timeVal.ms);
    return StatusOk;
}

static Status
convertTimestampToFloat64(const DfFieldValue *inputFieldValue,
                          DfFieldValue *outputFieldValue)
{
    outputFieldValue->float64Val = (float64_t)(inputFieldValue->timeVal.ms);
    return StatusOk;
}

static Status
convertTimestampToTimestamp(const DfFieldValue *inputFieldValue,
                            DfFieldValue *outputFieldValue)
{
    outputFieldValue->timeVal = inputFieldValue->timeVal;
    return StatusOk;
}

Status
convertTimestampToStr(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue,
                      size_t outputStrBufSize)
{
    size_t ret;
    struct tm time;
    int tzoffset = inputFieldValue->timeVal.tzoffset;

    time_t unixTs = inputFieldValue->timeVal.ms / 1000;
    int milliseconds = inputFieldValue->timeVal.ms % 1000;
    if (unlikely(milliseconds < 0)) {
        unixTs--;
        milliseconds = 1000 + milliseconds;
    }

    if (tzoffset == 0) {
        gmtime_r(&unixTs, &time);

        ret = snprintf(outputFieldValue->stringValTmp,
                       outputStrBufSize,
                       DataFormat::DefaultDateFormatOutUTC,
                       time.tm_year + 1900,
                       time.tm_mon + 1,
                       time.tm_mday,
                       time.tm_hour,
                       time.tm_min,
                       time.tm_sec,
                       milliseconds);
        if (ret >= outputStrBufSize) {
            return StatusOverflow;
        }
    } else {
        // convert to seconds
        char sign = tzoffset < 0 ? '-' : '+';

        unixTs += tzoffset * 60 * 60;

        gmtime_r(&unixTs, &time);

        ret = snprintf(outputFieldValue->stringValTmp,
                       outputStrBufSize,
                       DataFormat::DefaultDateFormatOutTZ,
                       time.tm_year + 1900,
                       time.tm_mon + 1,
                       time.tm_mday,
                       time.tm_hour,
                       time.tm_min,
                       time.tm_sec,
                       milliseconds,
                       sign,
                       abs(tzoffset));
        if (ret >= outputStrBufSize) {
            return StatusOverflow;
        }
    }

    outputFieldValue->stringVal.strSize = ret + 1;

    return StatusOk;
}

static Status
convertTimestampToNumeric(const DfFieldValue *inputFieldValue,
                          DfFieldValue *outputFieldValue)
{
    Status status;
    char buf[XLR_DFP_STRLEN];
    DFPUtils *dfp = DFPUtils::get();

    status = strSnprintf(buf, sizeof(buf), "%ld", inputFieldValue->timeVal.ms);
    BailIfFailed(status);

    status = dfp->xlrNumericFromString(&outputFieldValue->numericVal, buf);
    BailIfFailed(status);

CommonExit:
    return status;
}

// ================= conversion function for scalarObj
// ========================
static Status
convertScalarObjToStr(const DfFieldValue *inputFieldValue,
                      DfFieldValue *outputFieldValue,
                      size_t outputStrBufSize)
{
    ssize_t ret;
    Scalar *scalar;
    char *string;
    size_t bytesRemaining = outputStrBufSize;
    DfFieldValue tmpFieldVal;
    DfFieldValue tmpOutputFieldVal;
    Status status;

    scalar = inputFieldValue->scalarVal;
    string = outputFieldValue->stringValTmp;
    if (scalar == NULL) {
        ret = snprintf(string, outputStrBufSize, "null");
        if (ret < 0 || (size_t) ret >= bytesRemaining) {
            return StatusNoBufs;
        }
        bytesRemaining -= ret;
    } else {
        unsigned ii;

        if (scalar->fieldNumValues > 1) {
            ret = snprintf(string, outputStrBufSize, "[");
            if (ret < 0 || (size_t) ret >= bytesRemaining) {
                return StatusNoBufs;
            }
            bytesRemaining -= ret;
            string += ret;
        }

        for (ii = 0; ii < scalar->fieldNumValues; ii++) {
            if (ii >= 1) {
                ret = snprintf(string, outputStrBufSize, ", ");
                if (ret < 0 || (size_t) ret >= bytesRemaining) {
                    return StatusNoBufs;
                }
                bytesRemaining -= ret;
                string += ret;
            }

            status = scalar->getValue(&tmpFieldVal, ii);
            if (status != StatusOk) {
                return status;
            }

            tmpOutputFieldVal.stringVal.strActual =
                tmpOutputFieldVal.stringValTmp = string;
            status = DataFormat::convertValueType(DfString,
                                                  scalar->fieldType,
                                                  &tmpFieldVal,
                                                  &tmpOutputFieldVal,
                                                  bytesRemaining,
                                                  BaseCanonicalForm);
            if (status != StatusOk) {
                return status;
            }

            // Ignore the \0 byte
            ret = tmpOutputFieldVal.stringVal.strSize - 1;
            bytesRemaining -= ret;
            string += ret;
        }

        if (scalar->fieldNumValues > 1) {
            ret = snprintf(string, outputStrBufSize, "]");
            if (ret < 0 || (size_t) ret >= bytesRemaining) {
                return StatusNoBufs;
            }
            bytesRemaining -= ret;
            string += ret;
        }
    }

    size_t bytesUsed;
    assert(outputStrBufSize >= bytesRemaining);
    bytesUsed = outputStrBufSize - bytesRemaining;

    assert(outputFieldValue->stringValTmp ==
           outputFieldValue->stringVal.strActual);
    outputFieldValue->stringVal.strSize = bytesUsed + 1;

    return StatusOk;
}

// ============================ end conversion function
// ========================

static DfConvertTypeFunc convertTypeFuncs[DfFieldTypeLen][DfFieldTypeLen] =
    {[DfUnknown] = {[DfUnknown] = convertAssert,
                    [DfString] = convertAssert,
                    [DfInt32] = convertAssert,
                    [DfUInt32] = convertAssert,
                    [DfInt64] = convertAssert,
                    [DfUInt64] = convertAssert,
                    [DfFloat32] = convertAssert,
                    [DfFloat64] = convertAssert,
                    [DfMoney] = convertAssert,
                    [DfBoolean] = convertAssert,
                    [DfTimespec] = convertAssert,
                    [DfBlob] = convertAssert,
                    [DfNull] = convertAssert,
                    [DfMixed] = convertAssert},

     [DfString] = {[DfUnknown] = convertAssert,
                   [DfString] = convertAssert,
                   [DfInt32] = convertAssert,
                   [DfUInt32] = convertAssert,
                   [DfInt64] = convertAssert,
                   [DfUInt64] = convertAssert,
                   [DfFloat32] = convertStrToFloat32,
                   [DfFloat64] = convertStrToFloat64,
                   [DfMoney] = convertStrToNumeric,
                   [DfBoolean] = convertStrToBoolean,
                   [DfTimespec] = convertStrToTimestamp,
                   [DfBlob] = convertAssert,
                   [DfNull] = convertAssert,
                   [DfMixed] = convertAssert},

     [DfInt32] = {[DfUnknown] = convertAssert,
                  [DfString] = convertAssert,
                  [DfInt32] = convertInt32ToInt32,
                  [DfUInt32] = convertInt32ToUInt32,
                  [DfInt64] = convertInt32ToInt64,
                  [DfUInt64] = convertInt32ToUInt64,
                  [DfFloat32] = convertInt32ToFloat32,
                  [DfFloat64] = convertInt32ToFloat64,
                  [DfMoney] = convertInt32ToNumeric,
                  [DfBoolean] = convertInt32ToBoolean,
                  [DfTimespec] = convertAssert,
                  [DfBlob] = convertAssert,
                  [DfNull] = convertAssert,
                  [DfMixed] = convertAssert},

     [DfUInt32] = {[DfUnknown] = convertAssert,
                   [DfString] = convertAssert,
                   [DfInt32] = convertUInt32ToInt32,
                   [DfUInt32] = convertUInt32ToUInt32,
                   [DfInt64] = convertUInt32ToInt64,
                   [DfUInt64] = convertUInt32ToUInt64,
                   [DfFloat32] = convertUInt32ToFloat32,
                   [DfFloat64] = convertUInt32ToFloat64,
                   [DfMoney] = convertUInt32ToNumeric,
                   [DfBoolean] = convertUInt32ToBoolean,
                   [DfTimespec] = convertAssert,
                   [DfBlob] = convertAssert,
                   [DfNull] = convertAssert,
                   [DfMixed] = convertAssert},

     [DfInt64] = {[DfUnknown] = convertAssert,
                  [DfString] = convertAssert,
                  [DfInt32] = convertInt64ToInt32,
                  [DfUInt32] = convertInt64ToUInt32,
                  [DfInt64] = convertInt64ToInt64,
                  [DfUInt64] = convertInt64ToUInt64,
                  [DfFloat32] = convertInt64ToFloat32,
                  [DfFloat64] = convertInt64ToFloat64,
                  [DfMoney] = convertInt64ToNumeric,
                  [DfBoolean] = convertInt64ToBoolean,
                  [DfTimespec] = convertInt64ToTimestamp,
                  [DfBlob] = convertAssert,
                  [DfNull] = convertAssert,
                  [DfMixed] = convertAssert},

     [DfUInt64] = {[DfUnknown] = convertAssert,
                   [DfString] = convertAssert,
                   [DfInt32] = convertUInt64ToInt32,
                   [DfUInt32] = convertUInt64ToUInt32,
                   [DfInt64] = convertUInt64ToInt64,
                   [DfUInt64] = convertUInt64ToUInt64,
                   [DfFloat32] = convertUInt64ToFloat32,
                   [DfFloat64] = convertUInt64ToFloat64,
                   [DfMoney] = convertUInt64ToNumeric,
                   [DfBoolean] = convertUInt64ToBoolean,
                   [DfTimespec] = convertUInt64ToTimestamp,
                   [DfBlob] = convertAssert,
                   [DfNull] = convertAssert,
                   [DfMixed] = convertAssert},

     [DfFloat32] = {[DfUnknown] = convertAssert,
                    [DfString] = convertAssert,
                    [DfInt32] = convertFloat32ToInt32,
                    [DfUInt32] = convertFloat32ToUInt32,
                    [DfInt64] = convertFloat32ToInt64,
                    [DfUInt64] = convertFloat32ToUInt64,
                    [DfFloat32] = convertFloat32ToFloat32,
                    [DfFloat64] = convertFloat32ToFloat64,
                    [DfMoney] = convertFloat32ToNumeric,
                    [DfBoolean] = convertFloat32ToBoolean,
                    [DfTimespec] = convertFloat32ToTimestamp,
                    [DfBlob] = convertAssert,
                    [DfNull] = convertAssert,
                    [DfMixed] = convertAssert},

     [DfFloat64] = {[DfUnknown] = convertAssert,
                    [DfString] = convertAssert,
                    [DfInt32] = convertFloat64ToInt32,
                    [DfUInt32] = convertFloat64ToUInt32,
                    [DfInt64] = convertFloat64ToInt64,
                    [DfUInt64] = convertFloat64ToUInt64,
                    [DfFloat32] = convertFloat64ToFloat32,
                    [DfFloat64] = convertFloat64ToFloat64,
                    [DfMoney] = convertFloat64ToNumeric,
                    [DfBoolean] = convertFloat64ToBoolean,
                    [DfTimespec] = convertFloat64ToTimestamp,
                    [DfBlob] = convertAssert,
                    [DfNull] = convertAssert,
                    [DfMixed] = convertAssert},

     [DfMoney] = {[DfUnknown] = convertAssert,
                  [DfString] = convertAssert,
                  [DfInt32] = convertNumericToInt32,
                  [DfUInt32] = convertNumericToUInt32,
                  [DfInt64] = convertNumericToInt64,
                  [DfUInt64] = convertNumericToUInt64,
                  [DfFloat32] = convertNumericToFloat32,
                  [DfFloat64] = convertNumericToFloat64,
                  [DfMoney] = convertNumericToNumeric,
                  [DfBoolean] = convertNumericToBoolean,
                  [DfTimespec] = convertNumericToTimestamp,
                  [DfBlob] = convertAssert,
                  [DfNull] = convertAssert,
                  [DfMixed] = convertAssert},

     [DfBoolean] = {[DfUnknown] = convertAssert,
                    [DfString] = convertAssert,
                    [DfInt32] = convertBoolToInt32,
                    [DfUInt32] = convertBoolToUInt32,
                    [DfInt64] = convertBoolToInt64,
                    [DfUInt64] = convertBoolToUInt64,
                    [DfFloat32] = convertBoolToFloat32,
                    [DfFloat64] = convertBoolToFloat64,
                    [DfMoney] = convertBoolToNumeric,
                    [DfBoolean] = convertBoolToBool,
                    [DfTimespec] = convertAssert,
                    [DfBlob] = convertAssert,
                    [DfNull] = convertAssert,
                    [DfMixed] = convertAssert},

     [DfTimespec] = {[DfUnknown] = convertAssert,
                     [DfString] = convertAssert,
                     [DfInt32] = convertAssert,
                     [DfUInt32] = convertAssert,
                     [DfInt64] = convertTimestampToInt64,
                     [DfUInt64] = convertAssert,
                     [DfFloat32] = convertAssert,
                     [DfFloat64] = convertTimestampToFloat64,
                     [DfMoney] = convertTimestampToNumeric,
                     [DfBoolean] = convertAssert,
                     [DfTimespec] = convertTimestampToTimestamp,
                     [DfBlob] = convertAssert,
                     [DfNull] = convertAssert,
                     [DfMixed] = convertAssert},

     [DfBlob] = {[DfUnknown] = convertAssert,
                 [DfString] = convertAssert,
                 [DfInt32] = convertAssert,
                 [DfUInt32] = convertAssert,
                 [DfInt64] = convertAssert,
                 [DfUInt64] = convertAssert,
                 [DfFloat32] = convertAssert,
                 [DfFloat64] = convertAssert,
                 [DfMoney] = convertAssert,
                 [DfBoolean] = convertAssert,
                 [DfTimespec] = convertAssert,
                 [DfBlob] = convertAssert,
                 [DfNull] = convertAssert,
                 [DfMixed] = convertAssert},

     [DfNull] = {[DfUnknown] = convertAssert,
                 [DfString] = convertAssert,
                 [DfInt32] = convertNullToInt32,
                 [DfUInt32] = convertNullToUInt32,
                 [DfInt64] = convertNullToInt64,
                 [DfUInt64] = convertNullToUInt64,
                 [DfFloat32] = convertNullToFloat32,
                 [DfFloat64] = convertNullToFloat64,
                 [DfMoney] = convertNullToNumeric,
                 [DfBoolean] = convertNullToBool,
                 [DfTimespec] = convertAssert,
                 [DfBlob] = convertAssert,
                 [DfNull] = convertAssert,
                 [DfMixed] = convertAssert},

     [DfMixed] = {[DfUnknown] = convertAssert,
                  [DfString] = convertAssert,
                  [DfInt32] = convertAssert,
                  [DfUInt32] = convertAssert,
                  [DfInt64] = convertAssert,
                  [DfUInt64] = convertAssert,
                  [DfFloat32] = convertAssert,
                  [DfFloat64] = convertAssert,
                  [DfMoney] = convertAssert,
                  [DfBoolean] = convertAssert,
                  [DfTimespec] = convertAssert,
                  [DfBlob] = convertAssert,
                  [DfNull] = convertAssert,
                  [DfMixed] = convertAssert}};

// No idea why g++ doesn't complain about this
static DfConvertToStrFunc convertToStrFuncs[DfFieldTypeLen] = {
    [DfUnknown] = convertToStrAssert,
    [DfString] = convertStrToStr,
    [DfInt32] = convertInt32ToStr,
    [DfUInt32] = convertUInt32ToStr,
    [DfInt64] = convertInt64ToStr,
    [DfUInt64] = convertUInt64ToStr,
    [DfFloat32] = convertFloat32ToStr,
    [DfFloat64] = convertFloat64ToStr,
    [DfMoney] = convertNumericToStr,
    [DfBoolean] = convertBoolToStr,
    [DfTimespec] = convertTimestampToStr,
    [DfBlob] = convertToStrAssert,
    [DfNull] = convertNullToStr,
    [DfMixed] = convertToStrAssert,
    [DfFatptr] = convertToStrAssert,
    [DfScalarPtr] = convertToStrAssert,
    [DfScalarObj] = convertScalarObjToStr,
};

static DfConvertStrToIntFunc convertStrToIntFuncs[DfFieldTypeLen];

// In order to avoid extra copy operation, do not call the function if
// convert to same type. In the case of coverting to the same type, the same
// value will be copied to outputFieldValue. After calling this function,
// the caller should expect the outputFieldValue will always have the
// correct value
Status
DataFormat::convertValueType(DfFieldType outputType,
                             DfFieldType inputType,
                             const DfFieldValue *inputFieldValue,
                             DfFieldValue *outputFieldValue,
                             size_t outputStrBufSize,
                             Base inputBase)
{
    Status status = StatusUnknown;

    assert(outputType != DfUnknown);

    if (outputType == DfString) {
        status = convertToStrFuncs[inputType](inputFieldValue,
                                              outputFieldValue,
                                              outputStrBufSize);
    } else {
        if (outputType == inputType) {
            *outputFieldValue = *inputFieldValue;
            status = StatusOk;
        } else if ((inputType == DfString) &&
                   (outputType == DfInt32 || outputType == DfInt64 ||
                    outputType == DfUInt32 || outputType == DfUInt64)) {
            // XXX Call constructor to initialize converStrToIntFuncs
            convertStrToIntFuncs[DfInt32] = convertStrToInt32;
            convertStrToIntFuncs[DfUInt32] = convertStrToUInt32;
            convertStrToIntFuncs[DfInt64] = convertStrToInt64;
            convertStrToIntFuncs[DfUInt64] = convertStrToUInt64;

            BailIfNullWith(convertStrToIntFuncs[outputType], StatusInval);
            status = convertStrToIntFuncs[outputType](inputFieldValue,
                                                      outputFieldValue,
                                                      inputBase);
        } else {
            BailIfNullWith(convertTypeFuncs[inputType], StatusInval);
            BailIfNullWith(convertTypeFuncs[inputType][outputType],
                           StatusInval);
            status = convertTypeFuncs[inputType][outputType](inputFieldValue,
                                                             outputFieldValue);
        }
    }

CommonExit:
    return status;
}

Status
DataFormat::jsonifyValue(DfFieldType inputType,
                         const DfFieldValue *fieldValue,
                         json_t **jsonVal)
{
    Status status = StatusOk;
    *jsonVal = NULL;

    switch (inputType) {
    case DfString:
        *jsonVal = json_string(fieldValue->stringVal.strActual);
        BailIfNull(*jsonVal);
        break;
    case DfInt64:
        *jsonVal = json_integer(fieldValue->int64Val);
        BailIfNull(*jsonVal);
        break;
    case DfInt32:
        *jsonVal = json_integer(fieldValue->int32Val);
        BailIfNull(*jsonVal);
        break;
    case DfUInt64:
        *jsonVal = json_integer(fieldValue->uint64Val);
        BailIfNull(*jsonVal);
        break;
    case DfMoney: {
        char buf[XLR_DFP_STRLEN];

        DFPUtils::get()->xlrNumericToString(buf, &fieldValue->numericVal);
        *jsonVal = json_string(buf);
        BailIfNull(*jsonVal);
        break;
    }
    case DfTimespec: {
        DfFieldValue strVal;
        char buf[TIMESTAMP_STR_LEN];  // buf to hold timestamp string

        strVal.stringValTmp = buf;

        status = convertTimestampToStr(fieldValue, &strVal, sizeof(buf));
        assert(status == StatusOk);  // buf should be big enough to hold ts

        *jsonVal = json_string(buf);
        BailIfNull(*jsonVal);
        break;
    }
    case DfUInt32:
        *jsonVal = json_integer(fieldValue->uint32Val);
        BailIfNull(*jsonVal);
        break;
    case DfFloat64:
        if (isnan(fieldValue->float64Val) || isinf(fieldValue->float64Val)) {
            // longest potential string is '-Infinity'
            char tmpBuf[10];
            verify(
                snprintf(tmpBuf, sizeof(tmpBuf), "%f", fieldValue->float64Val) <
                static_cast<int>(sizeof(tmpBuf)));
            *jsonVal = json_string(tmpBuf);
            BailIfNull(*jsonVal);
        } else {
            *jsonVal = json_real(fieldValue->float64Val);
            BailIfNull(*jsonVal);
        }
        break;
    case DfFloat32:
        if (isnan(fieldValue->float32Val) || isinf(fieldValue->float32Val)) {
            // longest potential string is '-Infinity'
            char tmpBuf[10];
            verify(
                snprintf(tmpBuf, sizeof(tmpBuf), "%f", fieldValue->float32Val) <
                static_cast<int>(sizeof(tmpBuf)));
            *jsonVal = json_string(tmpBuf);
            BailIfNull(*jsonVal);
        } else {
            *jsonVal = json_real(fieldValue->float32Val);
            BailIfNull(*jsonVal);
        }
        break;
    case DfScalarObj: {
        const Scalar *scalar = fieldValue->scalarVal;
        if (scalar == NULL) {
            *jsonVal = json_null();
        } else {
            if (scalar->fieldNumValues > 1) {
                *jsonVal = json_array();
                BailIfNull(*jsonVal);

                for (unsigned ii = 0; ii < scalar->fieldNumValues; ii++) {
                    DfFieldValue thisFieldValue;
                    json_t *thisJson;

                    status = scalar->getValue(&thisFieldValue, ii);
                    BailIfFailed(status);

                    // We need to be careful not to leak thisJson
                    status = jsonifyValue(scalar->fieldType,
                                          &thisFieldValue,
                                          &thisJson);
                    BailIfFailed(status);

                    // append_new will steal the reference from thisJson
                    int ret = json_array_append_new(*jsonVal, thisJson);
                    if (ret == -1) {
                        // XXX - there's no way to guarantee -why- this
                        // failed
                        status = StatusNoMem;
                        json_decref(thisJson);
                        thisJson = NULL;
                        goto CommonExit;
                    }
                    thisJson = NULL;
                }
            } else {
                assert(scalar->fieldNumValues == 1);
                DfFieldValue fieldValue;

                status = scalar->getValue(&fieldValue);
                BailIfFailed(status);

                status = jsonifyValue(scalar->fieldType, &fieldValue, jsonVal);
                BailIfFailed(status);
            }
        }
        break;
    }
    case DfNull:
        *jsonVal = json_null();
        BailIfNull(*jsonVal);
        break;
    case DfBoolean:
        *jsonVal = json_boolean(fieldValue->boolVal);
        BailIfNull(*jsonVal);
        break;
    case DfUnknown:
        assert(0);
        *jsonVal = json_null();
        BailIfNull(*jsonVal);
        break;
    default:
        assert(0);
        status = StatusUnimpl;
    }
CommonExit:
    if (status != StatusOk) {
        if (*jsonVal) {
            json_decref(*jsonVal);
            *jsonVal = NULL;
        }
    }

    return status;
}
