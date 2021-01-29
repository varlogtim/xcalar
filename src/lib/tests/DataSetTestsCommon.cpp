// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/types.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <cstdlib>
#include <libgen.h>

#include "primitives/Primitives.h"
#include "dataset/Dataset.h"
#include "df/DataFormat.h"
#include "DataFormatConstants.h"
#include "export/DataTarget.h"
#include "config/Config.h"
#include "operators/Dht.h"
#include "DatasetInt.h"
#include "xdb/Xdb.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "usrnode/UsrNode.h"
#include "sys/XLog.h"
#include "table/ResultSet.h"
#include "callout/Callout.h"
#include "strings/String.h"
#include "operators/OperatorsApiWrappers.h"
#include "common/InitTeardown.h"
#include "libapis/LibApisRecv.h"
#include "constants/XcalarConfig.h"
#include "dataset/BackingData.h"
#include "dataset/AppLoader.h"
#include "parent/Parent.h"
#include "app/AppMgr.h"
#include "datapage/DataPageIndex.h"

#include "test/QA.h"  // Must be last

using namespace df;

enum { BufSize = 4096, NumNodes = 2, MyNodeId = 1 };

static DhtId testDhtId;
static const char *moduleName = "LibDsTest";
static Dag *myDag;

static void
dsFieldTests()
{
    DfDeclFieldValueArray(256, DsTest);
    DfFieldValueArrayDsTest fieldVals;
    DfFieldValue fieldVal;
    Status status;
    DfFieldValueString *tmp;

    assert(sizeof(fieldVals) == 256);
    memZero(&fieldVals, sizeof(fieldVals));
    uint64_t *fieldValsUint64Val = &fieldVals.fields.uint64Val[0];
    DfFieldValueString *fieldValsStrVals = &fieldVals.fields.strVals[0];
    char *fieldValsStrValsStrActual = &fieldValsStrVals->strActual[0];

    fieldVal.uint64Val = 0xdeadbeef;
    status = DataFormat::setFieldValueInArray(&fieldVals.fields,
                                              256,
                                              DfUInt64,
                                              0,
                                              fieldVal);
    assert(status == StatusOk);
    assert(fieldValsUint64Val[0] == 0xdeadbeef);
    fieldVal.uint64Val = 0xbadc0de;
    status = DataFormat::setFieldValueInArray(&fieldVals.fields,
                                              256,
                                              DfUInt64,
                                              1,
                                              fieldVal);
    assert(status == StatusOk);
    assert(fieldValsUint64Val[1] == 0xbadc0de);
    memZero(&fieldVal, sizeof(fieldVal));
    status = DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                                256,
                                                DfUInt64,
                                                1,
                                                &fieldVal);
    assert(status == StatusOk);
    assert(fieldVal.uint64Val == 0xbadc0de);
    status = DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                                256,
                                                DfUInt64,
                                                0,
                                                &fieldVal);
    assert(status == StatusOk);
    assert(fieldVal.uint64Val == 0xdeadbeef);
    status =
        DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                           256,
                                           DfFloat32,
                                           (unsigned) (256 / sizeof(float32_t)),
                                           &fieldVal);
    assert(status == StatusOverflow);
    status =
        DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                           256,
                                           DfFloat32,
                                           (unsigned) (256 / sizeof(float32_t) -
                                                       1),
                                           &fieldVal);
    assert(status == StatusOk);

    fieldVal.stringVal.strActual = "hello world";
    fieldVal.stringVal.strSize = strlen(fieldVal.stringVal.strActual) + 1;
    status = DataFormat::setFieldValueInArray(&fieldVals.fields,
                                              256,
                                              DfString,
                                              0,
                                              fieldVal);
    assert(status == StatusOk);
    assert(strcmp(&fieldVals.fields.strVals[0].strActual[0], "hello world") ==
           0);
    fieldVal.stringVal.strActual = "goodbye world";
    fieldVal.stringVal.strSize = strlen(fieldVal.stringVal.strActual) + 1;
    status = DataFormat::setFieldValueInArray(&fieldVals.fields,
                                              256,
                                              DfString,
                                              1,
                                              fieldVal);
    assert(status == StatusOk);

    assert(strcmp(fieldValsStrValsStrActual, "hello world") == 0);
    tmp = (DfFieldValueString
               *) &fieldValsStrValsStrActual[fieldValsStrVals[0].strSize];
    assert(strcmp(&tmp->strActual[0], "goodbye world") == 0);

    fieldVal.stringVal.strActual = "abcdefghijklmnop";
    fieldVal.stringVal.strSize = strlen(fieldVal.stringVal.strActual) + 1;
    status = DataFormat::setFieldValueInArray(&fieldVals.fields,
                                              1,
                                              DfString,
                                              2,
                                              fieldVal);
    assert(status == StatusOverflow);

    status = DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                                256,
                                                DfString,
                                                0,
                                                &fieldVal);
    assert(status == StatusOk);
    assert(strcmp(fieldVal.stringVal.strActual, "hello world") == 0);
    status = DataFormat::getFieldValueFromArray(&fieldVals.fields,
                                                256,
                                                DfString,
                                                1,
                                                &fieldVal);
    assert(status == StatusOk);
    assert(strcmp(fieldVal.stringVal.strActual, "goodbye world") == 0);
}

Status
dsFormatTests()
{
    dsFieldTests();
    return StatusOk;
}

// Create a DHT and ask for it to be written to disk.  Then unceremonioiusly
// delete it.  (Why is it in LibDsTest?  Because it uses Ds functions.)
Status
dsDhtPersistTest()
{
    Status status = StatusUnknown;
    const char *testDhtName = "LibDs Test DHT - Delete me!";
    DhtMgr *dhtMgr = DhtMgr::get();

    if (Config::get()->getMyNodeId() == 0) {
        // Just for kicks, create a test DHT
        status = dhtMgr->dhtCreate(testDhtName,
                                   0,
                                   999,
                                   Ascending,
                                   DoNotBroadcast,
                                   DoNotAutoRemove,
                                   NULL);
        assert(status == StatusOk);
        status = dhtMgr->dhtGetDhtId(testDhtName, &testDhtId);
        assert(status == StatusOk);
        xSyslog(moduleName,
                XlogDebug,
                "DHT successfully created: %s",
                testDhtName);
        // Delete our DHT
        status = dhtMgr->dhtDelete(testDhtName);
        assert(status == StatusOk);
        xSyslog(moduleName,
                XlogDebug,
                "DHT successfully deleted: %s",
                testDhtName);
    } else {
        status = StatusOk;
        xSyslog(moduleName,
                XlogDebug,
                "LibDs DHT test run elsewhere -- I'm not Node 0");
    }

    return status;
}

#ifdef DEBUG
extern unsigned nodeIdNumBits;
extern unsigned recIdNumBits;
#endif

Status
dsFatptrTests()
{
    DfRecordFatptr recFatptr;

    DataFormat::get()->fatptrInit(4, KB);
#ifdef DEBUG
    assert(nodeIdNumBits == 2);
    assert(recIdNumBits == 10);
    assert(DataFormat::get()->fatptrGetNumBits() ==
           nodeIdNumBits + recIdNumBits);
#endif
    assert(DataFormat::get()->recordFatptrMask_ ==
           (1UL << DataFormat::get()->fatptrGetNumBits()) - 1);
    DataFormat::get()->fatptrSet(&recFatptr, 0, 0);
    DataFormat::get()->fatptrSet(&recFatptr, 3, KB - 1);

    DataFormat::get()->fatptrInit(32, MB);
#ifdef DEBUG
    assert(nodeIdNumBits == 5);
    assert(recIdNumBits == 20);
    assert(DataFormat::get()->fatptrGetNumBits() ==
           nodeIdNumBits + recIdNumBits);
#endif
    assert(DataFormat::get()->recordFatptrMask_ ==
           (1UL << DataFormat::get()->fatptrGetNumBits()) - 1);
    DataFormat::get()->fatptrSet(&recFatptr, 0, 0);
    DataFormat::get()->fatptrSet(&recFatptr, 31, MB - 1);

    DataFormat::get()->fatptrInit(KB - 5, GB - 20);
#ifdef DEBUG
    assert(nodeIdNumBits == 10);
    assert(recIdNumBits == 30);
    assert(DataFormat::get()->fatptrGetNumBits() ==
           nodeIdNumBits + recIdNumBits);
#endif
    assert(DataFormat::get()->recordFatptrMask_ ==
           (1UL << DataFormat::get()->fatptrGetNumBits()) - 1);
    DataFormat::get()->fatptrSet(&recFatptr, 0, 0);
    DataFormat::get()->fatptrSet(&recFatptr, KB - 6, GB - 21);

    return StatusOk;
}

Status
dsTypeConversionTestActual(float64_t val)
{
    Status status;
    DfFieldValue Val;
    DfFieldValue Val2;
    DfFieldValue Val3;

    Val.float64Val = val;
    char buf[100];

    verify(snprintf(buf, sizeof(buf), "%.15f", val) < (int) sizeof(buf));

    Val2.stringVal.strActual = buf;
    Val2.stringVal.strSize = strlen(buf) + 1;

    status = DataFormat::convertValueType(DfFloat64,
                                          DfString,
                                          &Val2,
                                          &Val3,
                                          0,
                                          (Base) DfDefaultConversionBase);
    assert(status == StatusOk);

    assert(DataFormat::fieldCompare(DfFloat64, Val, Val3) == 0);

    memZero(buf, sizeof(buf));
    memZero(&Val2, sizeof(Val2));
    memZero(&Val3, sizeof(Val3));

    Val2.stringVal.strActual = buf;
    Val2.stringVal.strSize = sizeof(buf);

    status = DataFormat::convertValueType(DfString,
                                          DfFloat64,
                                          &Val,
                                          &Val2,
                                          sizeof(buf),
                                          (Base) DfDefaultConversionBase);
    assert(status == StatusOk);

    status = DataFormat::convertValueType(DfFloat64,
                                          DfString,
                                          &Val2,
                                          &Val3,
                                          0,
                                          (Base) DfDefaultConversionBase);
    assert(status == StatusOk);

    assert(DataFormat::fieldCompare(DfFloat64, Val, Val3) == 0);

    return StatusOk;
}

// Verifies convert from string to numeric type fails for all
// numeric types.
static void
verifyConvertFromStrFails(const char *str)
{
    DfFieldValue valInput;
    DfFieldValue valOutput;

    DfFieldType numericTypes[] =
        {DfInt32, DfInt64, DfUInt32, DfUInt64, DfFloat32, DfFloat64};

    valInput.stringVal.strActual = str;
    valInput.stringVal.strSize = strlen(str) + 1;

    for (unsigned i = 0; i < ArrayLen(numericTypes); i++) {
        verify(DataFormat::convertValueType(numericTypes[i],
                                            DfString,
                                            &valInput,
                                            &valOutput,
                                            0,
                                            BaseCanonicalForm) ==
               StatusTypeConversionError);
    }
}

#define emitVerifyFn(ctype, field, type)                          \
    static void verifyConvertFromStrTo##type(const char *str,     \
                                             ctype expectedValue, \
                                             Base inputBase)      \
    {                                                             \
        DfFieldValue valInput;                                    \
        DfFieldValue valOutput;                                   \
        Status status;                                            \
                                                                  \
        printf("%s(%s)\n", #type, str);                           \
                                                                  \
        valInput.stringVal.strActual = str;                       \
        valInput.stringVal.strSize = strlen(str) + 1;             \
                                                                  \
        status = DataFormat::convertValueType(Df##type,           \
                                              DfString,           \
                                              &valInput,          \
                                              &valOutput,         \
                                              0,                  \
                                              inputBase);         \
        assert(status == StatusOk ||                              \
               status == StatusDfCastTruncationOccurred);         \
        assert(valOutput.field == expectedValue);                 \
    }

emitVerifyFn(int64_t, int64Val, Int64) emitVerifyFn(int32_t, int32Val, Int32)
    emitVerifyFn(uint64_t, uint64Val, UInt64)
        emitVerifyFn(uint32_t, uint32Val, UInt32)
            emitVerifyFn(double, float64Val, Float64)
                emitVerifyFn(float, float32Val, Float32)

                    static void verifyConvertFromStr(const char *str,
                                                     Base inputBase,
                                                     int64_t i64,
                                                     int32_t i32,
                                                     uint64_t ui64,
                                                     uint32_t ui32,
                                                     double f64,
                                                     float f32)
{
    verifyConvertFromStrToInt64(str, i64, inputBase);
    verifyConvertFromStrToInt32(str, i32, inputBase);
    verifyConvertFromStrToUInt64(str, ui64, inputBase);
    verifyConvertFromStrToUInt32(str, ui32, inputBase);
    verifyConvertFromStrToFloat64(str, f64, inputBase);
    verifyConvertFromStrToFloat32(str, f32, inputBase);
}

Status
dsTypeConversionTest()
{
    const float64_t Pi = 3.1415926535897932384626433832795028841971693993751;

    Status status = dsTypeConversionTestActual(Pi);
    assert(status == StatusOk);

    const float64_t AvogadrosConstant = 6.022140857e23;
    status = dsTypeConversionTestActual(AvogadrosConstant);
    assert(status == StatusOk);

    verifyConvertFromStrFails("ABC");
    verifyConvertFromStr("1zzz",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("1.0.1",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("1f", BaseCanonicalForm, 1LL, 1L, 1ULL, 1UL, 1.0, 1.0);
    verifyConvertFromStr("1..1",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("12345a",
                         BaseCanonicalForm,
                         12345LL,
                         12345L,
                         12345ULL,
                         12345UL,
                         12345.0,
                         12345.0);
    verifyConvertFromStr("1-2",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("1 23",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("-2-3",
                         BaseCanonicalForm,
                         -2LL,
                         -2L,
                         UINT64_MAX - 1ULL,
                         UINT32_MAX - 1UL,
                         -2.0,
                         -2.0);
    verifyConvertFromStr("6?", BaseCanonicalForm, 6LL, 6L, 6ULL, 6UL, 6.0, 6.0);
    verifyConvertFromStr("1_", BaseCanonicalForm, 1LL, 1L, 1ULL, 1UL, 1.0, 1.0);
    verifyConvertFromStr("9.8.7",
                         BaseCanonicalForm,
                         9LL,
                         9L,
                         9ULL,
                         9UL,
                         9.8,
                         (float) 9.8);
    verifyConvertFromStr("0.0.0",
                         BaseCanonicalForm,
                         0LL,
                         0L,
                         0ULL,
                         0UL,
                         0.0,
                         0.0);
    verifyConvertFromStr("1,2,3,4",
                         BaseCanonicalForm,
                         1LL,
                         1L,
                         1ULL,
                         1UL,
                         1.0,
                         1.0);
    verifyConvertFromStr("00FF00",
                         BaseCanonicalForm,
                         0LL,
                         0L,
                         0ULL,
                         0UL,
                         0.0,
                         0.0);
    verifyConvertFromStr("0xFF00",
                         BaseCanonicalForm,
                         65280LL,
                         65280L,
                         65280ULL,
                         65280UL,
                         65280.0,
                         65280.0);
    verifyConvertFromStr("0x00FF",
                         BaseCanonicalForm,
                         255LL,
                         255L,
                         255ULL,
                         255UL,
                         255.0,
                         255.0);
    verifyConvertFromStr("-0", BaseCanonicalForm, 0LL, 0L, 0ULL, 0UL, 0.0, 0.0);

    return StatusOk;
}

Status
dsSetUp()
{
    Status status;
    status = AppMgr::get()->addBuildInApps();
    verify(status == StatusOk);

    DagLib *dagLib = DagLib::get();
    status = dagLib->createNewDag(128, DagTypes::WorkspaceGraph, NULL, &myDag);
    assert(status == StatusOk);

    // Create default Dht
    status = DhtMgr::get()->dhtCreateDefaultDhts();
    assert(status == StatusOk);

    return StatusOk;
}

Status
dsTearDown()
{
    DagLib *dagLib = DagLib::get();

    verifyOk(dagLib->destroyDag(myDag, DagTypes::DestroyDeleteAndCleanNodes));

    AppMgr::get()->removeBuiltInApps();

    DhtMgr::get()->dhtDeleteDefaultDhts();
    // wait for all messages to come back
    sysSleep(1);

    return StatusOk;
}

Status
dsAccessorTest()
{
    constexpr const int maxAccessorNameLen = 50;
    struct TestAccessor {
        AccessorName::Type type;
        union {
            const char *fieldName;
            int subscript;
        };
    };
    struct TestCase {
        const char *inStr;
        StatusCode outStatus;
        int numAccessors;
        TestAccessor accessors[maxAccessorNameLen];
    };

    constexpr const TestCase tests[] = {
        {"simple", StatusCodeOk, 1, {{AccessorName::Type::Field, {"simple"}}}},

        {"nested.field",
         StatusCodeOk,
         2,
         {
             {AccessorName::Type::Field, {"nested"}},
             {AccessorName::Type::Field, {"field"}},
         }},

        {"double.nested.field",
         StatusCodeOk,
         3,
         {
             {AccessorName::Type::Field, {"double"}},
             {AccessorName::Type::Field, {"nested"}},
             {AccessorName::Type::Field, {"field"}},
         }},

        {"array[0]",
         StatusCodeOk,
         2,
         {
             {AccessorName::Type::Field, {"array"}},
             {AccessorName::Type::Subscript, {0}},
         }},

        {"array[0][0]",
         StatusCodeOk,
         3,
         {
             {AccessorName::Type::Field, {"array"}},
             {AccessorName::Type::Subscript, {0}},
             {AccessorName::Type::Subscript, {0}},
         }},

        {"array[5].nested.field[10]",
         StatusCodeOk,
         5,
         {
             {AccessorName::Type::Field, {"array"}},
             {AccessorName::Type::Subscript, {.subscript = 5}},
             {AccessorName::Type::Field, {"nested"}},
             {AccessorName::Type::Field, {"field"}},
             {AccessorName::Type::Subscript, {.subscript = 10}},
         }},

        {"not\\.nested",
         StatusCodeOk,
         1,
         {
             {AccessorName::Type::Field, {"not.nested"}},
         }},

        {"not\\.nested\\[\\]\\\\",
         StatusCodeOk,
         1,
         {
             {AccessorName::Type::Field, {"not.nested[]\\"}},
         }},

        {"unclosedbracked[", StatusCodeAstMalformedEvalString, 0, {}},

        {"", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access[]", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access[[", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access][", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access[a]", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access[-1]", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.access[9999999999999999999999999999999999]",
         StatusCodeAstMalformedEvalString,
         0,
         {}},

        {"bad.escape\\a", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.escape\\", StatusCodeAstMalformedEvalString, 0, {}},

        {".", StatusCodeAstMalformedEvalString, 0, {}},

        {"bad.[1]", StatusCodeAstMalformedEvalString, 0, {}},

        {".falsestart", StatusCodeAstMalformedEvalString, 0, {}},

    };

    for (int ii = 0; ii < (int) ArrayLen(tests); ii++) {
        const TestCase *test = &tests[ii];
        AccessorNameParser parser;
        Accessor accessor;

        Status status = parser.parseAccessor(test->inStr, &accessor);
        assert(status.code() == test->outStatus);
        if (status != StatusOk) {
            continue;
        }
        assert(accessor.nameDepth == test->numAccessors);
        assert(accessor.names != NULL);
        for (int jj = 0; jj < accessor.nameDepth; jj++) {
            const TestAccessor *tAccess = &test->accessors[jj];
            const AccessorName *access = &accessor.names[jj];
            assert(tAccess->type == access->type);
            if (tAccess->type == AccessorName::Type::Field) {
                assert(strcmp(tAccess->fieldName, access->value.field) == 0);
            } else {
                assert(tAccess->type == AccessorName::Type::Subscript);
                assert(tAccess->subscript == access->value.subscript);
            }
        }
    }

    return StatusOk;
}
