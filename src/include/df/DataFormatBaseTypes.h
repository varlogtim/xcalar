// Copyright 2014 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATAFORMATBASETYPES_H_
#define _DATAFORMATBASETYPES_H_

#include <time.h>

#include "XlrDecimal64.h"

#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "df/FatPointerTypes.h"
#include "DataFormatEnums.h"
#include "libapis/LibApisConstants.h"

class Scalar;

extern const char *DfFatptrPrefixDelimiter;
extern const char *DfFatptrPrefixDelimiterReplaced;
const char DfCsvDefaultRecordDelimiter = '\n';
const char DfCsvDefaultFieldDelimiter = '\t';
const char DfCsvDefaultQuoteDelimiter = '"';
const char DfCsvDefaultEscapeDelimiter = '\\';

enum {
    // WARNING: CHANGE in DfMaxFieldNameLen MUST CAREFULLY CONSIDER UPGRADE
    // SCENARIOS. SEE Xc-10937.
    DfMaxFieldNameLen = XcalarApiMaxFieldNameLen,

    DfVariableFieldSize = 0xffffffff,
    DfNestedDelimiter = '.',
    DfArrayIndexStartDelimiter = '[',
    DfArrayIndexEndDelimiter = ']',

    DfCsvFieldDelimiterMaxLen = 255,

    DfRecOffsetSize = sizeof(uint64_t),
    DfMaxFieldValueSize = XcalarApiMaxFieldValueSize
};

// XXX FIXME change strSize to uint32_t or uint16_t for better memory
// efficiency.  must be same type as DfFieldValueStringPtr.strSize
struct DfFieldValueString {
    size_t strSize;  // in bytes including '\0'
    char strActual[0];
};

// XXX FIXME change strSize to uint32_t or uint16_t for better memory
// efficiency.  must be same type as DfFieldValueString.strSize
struct DfFieldValueStringPtr {
    const char *strActual;
    size_t strSize;  // in bytes including '\0'
};

struct DfTimeval {
    int64_t ms;    // milliseconds since epoch in UTC
    int tzoffset;  // offset in hours to reach desired tz
};

union DfFieldValue {
    DfFieldValueStringPtr stringVal;
    bool boolVal;
    uint32_t uint32Val;
    int32_t int32Val;
    uint64_t uint64Val;
    int64_t int64Val;
    float32_t float32Val;
    float64_t float64Val;
    DfTimeval timeVal;
    char *stringValTmp;
    Scalar *scalarVal;
    DfRecordFatptr fatptrVal;
    uintptr_t opRowMetaVal;
    XlrDfp numericVal;  // Storage for IEEE754 DFP types
};

// cannot simply make an array of DfFieldValue because the values of smaller
// types will not be contiguous in memory; so something like dfGetFieldValues
// could not be used.
union DfFieldValueArray {
    DfFieldValueString strVals[0];
    bool boolVal[0];
    uint32_t uint32Val[0];
    int32_t int32Val[0];
    uint64_t uint64Val[0];
    int64_t int64Val[0];
    float32_t float32Val[0];
    float64_t float64Val[0];
    DfTimeval timeVal[0];
    XlrDfp numericVal[0];
};
#define DfDeclFieldValueArray(sizeInBytes, suffix) \
    typedef union {                                \
        uint8_t buf[sizeInBytes];                  \
        DfFieldValueArray fields;                  \
    } DfFieldValueArray##suffix

// ****** do not update DfFieldAttr without also updating ******
// ****** LibApisCommon.thrift                           ******
struct DfFieldAttrHeader {
    // XXX possible to make this const char * instead of strlcpy()'ing?
    // or perhaps use a constant DfFieldAttrHeader *?
    char name[DfMaxFieldNameLen + 1];
    DfFieldType type;
    int valueArrayIndex;
    Ordering ordering;
};

struct DfFieldAttr {
    DfFieldAttrHeader header;
    size_t fieldSize;
    size_t valueSize;
    unsigned fieldNum;
    unsigned numValues;
};

typedef uint64_t DfRecordId;

static constexpr const DfFieldValue DeleteOpCode = {.int64Val = 0};
static constexpr const DfFieldValue InsertOpCode = {.int64Val = 1};
static constexpr const DfFieldValue UpdateOpCode = {.int64Val = 2};
static constexpr const DfFieldValue DeleteStrictOpCode = {.int64Val = 3};
static constexpr const DfFieldValue InsertStrictOpCode = {.int64Val = 4};
static constexpr const DfFieldValue UpdateStrictOpCode = {.int64Val = 5};
static constexpr const DfFieldValue MaxOpCode = {.int64Val = 6};

static constexpr const DfFieldValue InitialRank = {.int64Val = 0};

#endif  // _DATAFORMATBASETYPES_H_
