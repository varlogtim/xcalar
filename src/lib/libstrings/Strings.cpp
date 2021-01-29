// Copyright 2013 - 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <assert.h>
#include <string.h>
#include <jansson.h>

#include "primitives/Primitives.h"
#include "strings/String.h"
#include "util/Random.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libstrings";

bool
strIsWhiteSpace(char c)
{
    return (c == ' ') || (c == '\n') || (c == '\t');
}

char *
strTmpFile(char *fileStr, RandHandle *rndHandle)
{
    char charset[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    int ii;
    char c;

    for (ii = 0; (c = fileStr[ii]) != '\0'; ii++) {
        if (c == 'X') {
            int index =
                (int) (rndGenerate32(rndHandle) % (uint32_t) sizeof(charset));
            fileStr[ii] = charset[index];
        }
    }
    return fileStr;
}

char *
strTrim(char *input)
{
    char *tail;

    // ltrim
    while (strIsWhiteSpace(*input)) {
        input++;
    }

    if (*input == '\0') {
        return input;
    }

    // rtrim
    tail = input + strlen(input) - 1;
    while (tail != input && strIsWhiteSpace(*tail)) {
        tail--;
    }
    *(tail + 1) = '\0';

    return input;
}

bool
strMatch(const char *pattern, const char *string)
{
    if (*pattern == '\0' && *string == '\0') {
        return true;
    }

    if (*pattern == '*') {
        if (*string == '\0') {
            return strMatch(pattern + 1, string);
        }

        return strMatch(pattern + 1, string) || strMatch(pattern, string + 1);
    }

    if (*pattern != *string) {
        return false;
    }

    return strMatch(pattern + 1, string + 1);
}

bool
strIsAsciiHexDigit(char a)
{
    return (a >= '0' && a <= '9') || (a >= 'a' && a <= 'f');
}

uint8_t
strAsciiToHexDigit(char a)
{
    assert(strIsAsciiHexDigit(a));

    if (a >= '0' && a <= '9') {
        return (uint8_t)(a - '0');
    }

    return (uint8_t)(a - 'a' + 10);
}

Status
strEscapeQuotes(char *out, size_t outLen, const char *in)
{
    size_t ii;
    size_t curLen = 0;
    size_t strLen;

    assert(out != NULL);
    assert(in != NULL);

    strLen = strlen(in);
    for (ii = 0; ii < strLen; ii++) {
        if (in[ii] == '"') {
            if (curLen + 2 >= outLen) {
                return StatusOverflow;
            }
            out[curLen++] = '\\';
            out[curLen++] = '"';
        } else {
            if (curLen + 1 >= outLen) {
                return StatusOverflow;
            }
            out[curLen++] = in[ii];
        }
    }

    if (curLen + 1 >= outLen) {
        return StatusOverflow;
    }

    out[curLen++] = '\0';
    return StatusOk;
}

const char *
strBasename(const char *str)
{
    const char *lastSlash = strrchr(str, '/');
    if (lastSlash == NULL) {
        return str;
    } else {
        return lastSlash + 1;
    }
}

Status
strHumanSize(char *buf, size_t bufSize, size_t size)
{
    const char *units[] = {"B", "KB", "MB", "GB", "TB"};
    size_t ii, ret;

    for (ii = 0; ii < ArrayLen(units); ii++) {
        if (size < 1024 || ii == ArrayLen(units) - 1) {
            break;
        }
        size /= 1024;
    }

    ret = snprintf(buf, bufSize, "%lu%s", size, units[ii]);
    if (ret >= bufSize) {
        return StatusNoBufs;
    }

    return StatusOk;
}

Status
strSnprintf(char *dst, size_t size, const char *fmt, ...)
{
    int ret;
    va_list args;

    va_start(args, fmt);
    ret = vsnprintf(dst, size, fmt, args);
    va_end(args);

    if (ret < 0) {
        return StatusInval;
    }

    if (ret >= (int) size) {
        return StatusOverflow;
    }

    return StatusOk;
}

char *
allocAndSnprintf(const char *fmt, ...)
{
    char *printedString = NULL;
    int len;
    int ret;
    va_list args;

    va_start(args, fmt);
    len = vsnprintf(NULL, 0, fmt, args);
    va_end(args);

    printedString = (char *) memAlloc(len + 1);
    if (printedString == NULL) {
        return NULL;
    }

    va_start(args, fmt);
    ret = vsnprintf(printedString, len + 1, fmt, args);
    va_end(args);
    assert(ret == len);
    return printedString;
}

Status
jsonStrCmp(const char *str1, const char *str2, bool *isIdentical)
{
    Status status = StatusUnknown;
    json_t *json1 = NULL, *json2 = NULL;
    char *jsonStr1 = NULL, *jsonStr2 = NULL;
    json_error_t jsonError;

    if (strcmp(str1, str2) == 0) {
        *isIdentical = true;
        status = StatusOk;
        goto CommonExit;
    }

    json1 = json_loads(str1, 0, &jsonError);
    if (json1 == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\" at line %d: %s",
                str1,
                jsonError.line,
                jsonError.text);
        status = StatusInval;
        goto CommonExit;
    }

    json2 = json_loads(str2, 0, &jsonError);
    if (json2 == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing \"%s\" at line %d: %s",
                str2,
                jsonError.line,
                jsonError.text);
        status = StatusInval;
        goto CommonExit;
    }

    jsonStr1 = json_dumps(json1, JSON_SORT_KEYS);
    if (jsonStr1 == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "Error converting json_t to string");
        goto CommonExit;
    }

    jsonStr2 = json_dumps(json2, JSON_SORT_KEYS);
    if (jsonStr2 == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName, XlogErr, "Error converting json_t to string");
        goto CommonExit;
    }

    *isIdentical = (strcmp(jsonStr1, jsonStr2) == 0);
    status = StatusOk;
CommonExit:
    if (jsonStr1 != NULL) {
        memFreeJson(jsonStr1);
        jsonStr1 = NULL;
    }

    if (jsonStr2 != NULL) {
        memFreeJson(jsonStr2);
        jsonStr2 = NULL;
    }

    if (json2 != NULL) {
        json_decref(json2);
        json2 = NULL;
    }

    if (json1 != NULL) {
        json_decref(json1);
        json1 = NULL;
    }

    return status;
}
