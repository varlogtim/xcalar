// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include <new>
#include <stdint.h>
#include <cstdlib>
#include <pthread.h>
#include <libgen.h>
#include "StrlFunc.h"
#include "UdfPythonImpl.h"
#include "UdfPyFunction.h"
#include "util/MemTrack.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "operators/XcalarEval.h"
#include "operators/Xdf.h"
#include "common/InitTeardown.h"
#include "strings/String.h"
#include "runtime/Tls.h"
#include "libapis/LibApisCommon.h"

#include "test/QA.h"  // Must be last.

//
// This file defines unit tests for python UDF parsing and execution.
//

enum {
    RefCountBugIterCount = 250,
    UdfPyTestMaxArgc = 32,
};

////////////////////////////////////////////////////////////////////////////////

struct UdfPyTestModule {
    const char *module;
    const char *source;
};

union GenericArgv {
    char *argvStr[UdfPyTestMaxArgc];
    int argvInt[UdfPyTestMaxArgc];
    bool argvBool[UdfPyTestMaxArgc];
    float argvFloat[UdfPyTestMaxArgc];
};

typedef void (*ConvertToScalarsFn)(GenericArgv *, Scalar *[], unsigned);

// Test modules whose adds are expected to succeed.
static UdfPyTestModule testModules[] = {
    {.module = "simple",
     .source = "def simple():\n return 'that was easy'\n"
               "def __hidden():\n return 'this is hidden'\n"},
    {.module = "string",
     .source = "def str_Length( strVal ):\n"
               "\treturn \"%d\" % len(strVal);"
               "\n\n"
               "def concat(*args):\n"
               "\tbuffer=\"\"\n"
               "\tfor arg in args:\n"
               "\t\tbuffer+=arg\n"
               "\treturn buffer\n"},
    {.module = "maths",
     .source = "def addStuff(x, y, *args):\n"
               " sum = int(x) + int(y)\n"
               " for a in args:\n"
               "  sum += int(a)\n"
               " return str(sum)\n"},
    {.module = "buggySource",
     .source = "def divByZero(x, y):\n return (int(x) / int(y))\n"
               "def addToUnset(x, y):\n z += int(x) + int(y)\n return z\n"},
    {.module = "witness",
     .source = "def xc2399(x):\n return str(round(x, 3))\n"
               "def xc2399floatRet(x):\n return round(x, 3)\n"},
    {.module = "NullByte",
     .source = "def sayHello():\n\treturn \"I am a \\x00 byte\"\n"},
    {.module = "nested",
     .source = "def outer():\n def inner():\n  return 'inner'\n"
               " return 'outer'\n"},
    {.module = "soloVarArg", .source = "def func(*myArgList):\n return \"\"\n"},
    {.module = "simpleSamePrefix", .source = "def notsimple(): return 'foo'"},
};

static UdfPythonImpl *udfPy = NULL;

////////////////////////////////////////////////////////////////////////////////

//
// Utility functions.
//

// Generate a dummy module or function name.
void
generateName(char *name, size_t nameSize)
{
    static Atomic64 uniquifier = {0};

    uint64_t num = (uint64_t) atomicInc64(&uniquifier);

    if ((size_t) snprintf(name, nameSize, "n%llu", (unsigned long long) num) >=
        nameSize) {
        name[nameSize - 1] = '\0';
    }
}

// Verify module contains the expected function names.
void
verifyFunctions(XcalarEvalFnDesc *functions,
                size_t functionsCount,
                const char *expected[],
                size_t expectedCount)
{
    size_t iExp, iAct;
    assert(functionsCount == expectedCount);
    for (iExp = 0; iExp < expectedCount; iExp++) {
        bool found = false;
        for (iAct = 0; iAct < functionsCount; iAct++) {
            char *sep = strchr(functions[iAct].fnName, ':');
            assert(sep != NULL);
            sep++;

            if (strcmp(expected[iExp], sep) == 0) {
                found = true;
                break;
            }
        }
        assert(found);
    }
}

void
verifyFunction(XcalarEvalFnDesc *function, const char *args[], int numArgs)
{
    int ii;

    assert(function->numArgs == numArgs);

    if (numArgs < 0) {
        numArgs *= -1;
    }

    for (ii = 0; ii < numArgs; ii++) {
        assert(strcmp(args[ii], function->argDescs[ii].argDesc) == 0);
    }
}

// Verify the given strings are contained within their respective error strings.
void
verifyErrorMatches(UdfError *error, const char *messageSubstring)
{
    if (messageSubstring != NULL) {
        assert(error->message_ != NULL);
        assert(strstr(error->message_, messageSubstring) != NULL);
    }
}

Status
allocModInput(const char *src, const char *modName, UdfModuleSrc **inputRet)
{
    Status status;
    UdfModuleSrc *input = NULL;

    input = (UdfModuleSrc *) memAlloc(sizeof(*input) + strlen(src) + 1);
    BailIfNull(input);

    memset(input, 0, sizeof(*input) + strlen(src) + 1);
    input->type = UdfTypePython;
    strlcpy(input->moduleName, modName, sizeof(input->moduleName));
    input->sourceSize = strlen(src) + 1;
    memcpy(input->source, src, input->sourceSize);

    *inputRet = input;
    input = NULL;

    status = StatusOk;
CommonExit:
    if (input != NULL) {
        memFree(input);
        input = NULL;
    }
    return status;
}

// Verify module add fails with expectedStatus and (optionally) match error
// message substring.
void
verifyInvalid(const char *source,
              Status expectedStatus,
              const char *errorSubstring)
{
    UdfError error;
    Status status;

    char moduleName[32];
    generateName(moduleName, sizeof(moduleName));
    UdfModuleSrc *input = NULL;

    status = allocModInput(source, (const char *) moduleName, &input);
    assert(status == StatusOk);
    assert(input != NULL);

    verify(udfPy->addModule(input, &error) == expectedStatus);
    verifyErrorMatches(&error, errorSubstring);

    memFree(input);
    input = NULL;
}

void
scalarsFromStr(GenericArgv *source, Scalar *target[], unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        target[i] = Scalar::allocScalar(DfMaxFieldValueSize);
        assert(target[i] != NULL);
        verifyOk(xdfMakeStringIntoScalar(target[i],
                                         source->argvStr[i],
                                         strlen(source->argvStr[i])));
    }
}

void
scalarsFromInt(GenericArgv *source, Scalar *target[], unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        target[i] = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        assert(target[i] != NULL);
        verifyOk(xdfMakeInt64IntoScalar(target[i], source->argvInt[i]));
    }
}

void
scalarsFromBool(GenericArgv *source, Scalar *target[], unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        target[i] = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        assert(target[i] != NULL);
        verifyOk(xdfMakeBoolIntoScalar(target[i], source->argvBool[i]));
    }
}

void
scalarsFromFloat(GenericArgv *source, Scalar *target[], unsigned count)
{
    for (unsigned i = 0; i < count; i++) {
        target[i] = Scalar::allocScalar(Scalar::DefaultFieldValsBufSize);
        assert(target[i] != NULL);
        verifyOk(xdfMakeFloatIntoScalar(target[i], source->argvFloat[i]));
    }
}

// Execute a specific function with the given args. Verify status against
// expectedStatus. On success, check return if non-null. On error, match
// error message against errorSubstring (if non-null).
void
pyExec(const char *fqFunctionName,
       ConvertToScalarsFn convertFn,
       unsigned argc,
       GenericArgv *args,
       Status expectedStatus,
       const char *returnExpected,
       const char *errorSubstring)
{
    UdfError error;

    memZero(&error, sizeof(error));

    // Lookup function in XcalarEval.
    LibNsTypes::NsHandle nsHandle;
    XcalarEvalRegisteredFn *fn =
        XcalarEval::get()->getOrResolveRegisteredFn(fqFunctionName,
                                                    NULL,
                                                    &nsHandle);
    if (fn == NULL) {
        assert(expectedStatus == StatusUdfModuleNotFound ||
               expectedStatus == StatusUdfFunctionNotFound ||
               expectedStatus == StatusUdfNotFound);
        return;
    }

    Scalar *returnActual = Scalar::allocScalar(DfMaxFieldValueSize);
    assert(returnActual != NULL);

    Scalar *argv[XcalarEvalMaxNumArgs];

    if (argc != 0) {
        convertFn(args, argv, argc);
    }

    UdfPyFunction *udfPyFunction = (UdfPyFunction *) fn->fnDesc.context;
    assert(udfPyFunction != NULL);

    Status status = udfPyFunction->eval((int) argc, argv, returnActual, &error);
    assert(status == expectedStatus);  // Take a look at error if failed.

    XcalarEval::get()->putFunction(fn, &nsHandle);

    if (status == StatusOk) {
        DfFieldValue val;
        verifyOk(returnActual->getValue(&val));
        if (returnExpected != NULL) {
            assert(strcmp(val.stringVal.strActual, returnExpected) == 0);
        }

        assert(error.message_ == NULL);
    } else {
        verifyErrorMatches(&error, errorSubstring);
    }

    // Free scalars.
    Scalar::freeScalar(returnActual);
    for (unsigned i = 0; i < argc; i++) {
        Scalar::freeScalar(argv[i]);
    }
}

Status
udfPyListModule(const char *moduleName,
                XcalarEvalFnDesc **functions,
                size_t *functionsCount)
{
    Status status;

    XcalarApiOutput *output = NULL;
    XcalarApiListXdfsOutput *listXdfsOutput;

    status = udfPy->listModule(moduleName, &output);
    BailIfFailed(status);

    listXdfsOutput = &output->outputResult.listXdfsOutput;

    *functions = (XcalarEvalFnDesc *) memAlloc(sizeof(**functions) *
                                               listXdfsOutput->numXdfs);
    BailIfNull(*functions);

    memcpy(*functions,
           listXdfsOutput->fnDescs,
           sizeof(**functions) * listXdfsOutput->numXdfs);
    *functionsCount = listXdfsOutput->numXdfs;
    status = StatusOk;

CommonExit:
    if (output != NULL) {
        memFree(output);
    }
    return status;
}

////////////////////////////////////////////////////////////////////////////////

//
// Basic test cases.
//

static Status
udfPyTestLoadModules()
{
    Status status;
    UdfError error;
    memZero(&error, sizeof(error));
    UdfModuleSrc *input = NULL;

    for (size_t i = 0; i < ArrayLen(testModules); i++) {
        status =
            allocModInput(testModules[i].source, testModules[i].module, &input);
        BailIfFailed(status);
        assert(input != NULL);

        status = udfPy->addModule(input, &error);
        if (status != StatusOk) {
            printf("failed to add module: '%s', '%s'",
                   input->moduleName,
                   input->source);
            goto CommonExit;
        }
        assert(error.message_ == NULL);
        memFree(input);
        input = NULL;
    }

    status = StatusOk;

CommonExit:

    return status;
}

static Status
udfPyTestSanity()
{
    UdfError error;
    memZero(&error, sizeof(error));

    XcalarEvalFnDesc *functions = NULL;
    size_t functionsCount = 0;

    pyExec("simple:simple",
           scalarsFromStr,
           0,
           NULL,
           StatusOk,
           "that was easy",
           NULL);

    static GenericArgv argsLen = {.argvStr = {(char *) "hello"}};
    pyExec("string:str_Length",
           scalarsFromStr,
           1,
           &argsLen,
           StatusOk,
           "5",
           NULL);

    static GenericArgv argsAdd = {.argvInt = {9, 9, 1, 2, 3, 4, 5, 6}};
    pyExec("maths:addStuff", scalarsFromInt, 8, &argsAdd, StatusOk, "39", NULL);

    static GenericArgv argsConcat = {
        .argvStr = {(char *) "foo", (char *) "bar"}};
    pyExec("string:concat",
           scalarsFromStr,
           2,
           &argsConcat,
           StatusOk,
           "foobar",
           NULL);

    static GenericArgv argsWrongAdd = {.argvStr = {(char *) "1", (char *) "1"}};
    pyExec("maths:add",
           scalarsFromStr,
           2,
           &argsWrongAdd,
           StatusUdfFunctionNotFound,
           NULL,
           NULL);

    // XXX: old test expected StatusStrEncodingNotSupported - now it's OK
    // why? needs vetting
    pyExec("NullByte:sayHello", scalarsFromStr, 0, NULL, StatusOk, NULL, NULL);

    static const char *nestedFn = "outer";
    verifyOk(udfPyListModule("nested", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, &nestedFn, 1);
    memFree(functions);

    const char *addStuffArgs[] = {"x", "y", "*args"};
    const char *mathsFn = "addStuff";
    verifyOk(udfPyListModule("maths", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, &mathsFn, 1);
    verifyFunction(&functions[0], addStuffArgs, -3);
    memFree(functions);

    const char *funcArgs[] = {"*myArgList"};
    const char *soloVarArgFn = "func";
    verifyOk(udfPyListModule("soloVarArg", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, &soloVarArgFn, 1);
    verifyFunction(&functions[0], funcArgs, -1);
    memFree(functions);

    const char *simpleSamePrefixFn = "notsimple";
    verifyOk(udfPyListModule("simpleSamePrefix", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, &simpleSamePrefixFn, 1);
    memFree(functions);

    return StatusOk;
}

// Test detection and reporting of validation errors during module add/update.
static Status
udfPyTestInvalid()
{
    verifyInvalid("def foo:\n return 7\n", StatusUdfModuleLoadFailed, "syntax");
    verifyInvalid("def syntax(x, y):\n if x = y:\n  return 6\n return 7\n",
                  StatusUdfModuleLoadFailed,
                  "syntax");
    verifyInvalid("", StatusUdfModuleEmpty, NULL);
    verifyInvalid(" \t  \n  ", StatusUdfModuleEmpty, NULL);
    verifyInvalid("print('hullo')\n", StatusUdfModuleEmpty, NULL);
    verifyInvalid("def __ghost(): return 666", StatusUdfModuleEmpty, NULL);

    return StatusOk;
}

// Test different combinations of successful/unsuccessful add and update.
static Status
udfPyTestAddUpdateError()
{
    Status status;
    UdfError error;
    memZero(&error, sizeof(error));
    UdfModuleSrc *input = NULL;

    XcalarEvalFnDesc *functions = NULL;
    size_t functionsCount = 0;

    const char *simpleFunctions[] = {"simple", "s"};
    const char *simpleFunctions2[] = {"s"};

    // Verify "simple" module exists with expected functions.
    verifyOk(udfPyListModule("simple", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, simpleFunctions, 1);
    memFree(functions);

    // "Attempt" bad update and verify it stays the same.
    verify(udfPy->updateModule("simple",
                               "def badFn:\n return 'bad'\n",
                               &error) == StatusUdfModuleLoadFailed);
    // Recovery of failed updates is the responsibility of libudf, not libudfpy.
    verifyOk(udfPyListModule("simple", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, simpleFunctions, 0);
    memFree(functions);

    // Do valid update. Verify new function exists.
    verifyOk(udfPy->updateModule("simple",
                                 "def s():\n return 'change is good'\n",
                                 &error));
    verifyOk(udfPyListModule("simple", &functions, &functionsCount));
    verifyFunctions(functions, functionsCount, simpleFunctions2, 1);
    memFree(functions);
    pyExec("simple:s", NULL, 0, NULL, StatusOk, "change is good", NULL);

    // XXX:
    // The maximum number of arguments supported is set by XcalarEval,
    // not the python support code. XcalarEvalMaxNumArgs sets this limit.
    // As of git sha 0ed7b911, the limit is very high - if needed, a test
    // can be added to validate the limit. The old test is being deleted
    // since it tested the much lower limit in a simple way that doesn't
    // extend to the much larger limit

    udfPy->removeModule("simple");

    pyExec("simple:simple", NULL, 0, NULL, StatusUdfModuleNotFound, NULL, NULL);

    const char *udfsrc = "if True:\n x = 1/0\ndef foo():\n return 0\n";

    status = allocModInput(udfsrc, "unstableTest", &input);
    BailIfFailed(status);

    verify(udfPy->addModule(input, &error) == StatusUdfModuleLoadFailed);
    memFree(input);
    input = NULL;

    status = StatusOk;
CommonExit:
    return status;
}

static Status
udfPyTestRuntimeError()
{
    static GenericArgv argsDivByZero = {
        .argvStr = {(char *) "1", (char *) "0"}};
    static GenericArgv argsAddToUnset = {
        .argvStr = {(char *) "1", (char *) "0"}};

    pyExec("buggySource:divByZero",
           scalarsFromStr,
           2,
           &argsDivByZero,
           StatusUdfExecuteFailed,
           NULL,
           "by zero");

    pyExec("buggySource:addToUnset",
           scalarsFromStr,
           2,
           &argsAddToUnset,
           StatusUdfExecuteFailed,
           NULL,
           "before assignment");

    static GenericArgv args2399True = {.argvBool = {true}};
    static GenericArgv args2399False = {.argvBool = {false}};

    for (unsigned i = 0; i < RefCountBugIterCount; i++) {
        // Bug Xc-2399: Boolean input to a function expecting float caused
        // backend crash. The backend crash occurred because a refcount was not
        // properly incremented on the true  / false PyBool_Type, but was
        // decremented on exit from the internal pyExec function.
        // The expected output is 0.0 (false) or 1.0 (true) - the type is
        // converted automatically by python.
        pyExec("witness:xc2399",
               scalarsFromBool,
               1,
               &args2399True,
               StatusOk,
               NULL,
               "1.0");

        pyExec("witness:xc2399",
               scalarsFromBool,
               1,
               &args2399False,
               StatusOk,
               NULL,
               "0.0");
    }

    for (unsigned i = 0; i < RefCountBugIterCount; i++) {
        static GenericArgv argsFloatRet = {.argvFloat = {2.3543f}};
        pyExec("witness:xc2399floatRet",
               scalarsFromFloat,
               1,
               &argsFloatRet,
               StatusOk,
               NULL,
               "2.354");

        pyExec("witness:xc2399floatRet",
               scalarsFromBool,
               1,
               &args2399True,
               StatusOk,
               NULL,
               "1.0");
    }

    return StatusOk;
}

////////////////////////////////////////////////////////////////////////////////

//
// udfPyTestBig: load a very large python module and execute its functions
// in parallel on several threads.
//

enum {
    // Rough bounds for source size.
    UdfPyTestBigSourceSizeMin = 512 * KB,
    UdfPyTestBigSourceSizeMax = 4 * 1024 * KB,

    UdfPyTestBigFnNameSize = 22,
    UdfPyTestBigExecThreadCount = 19,
    UdfPyTestBigExecIters = 25000,
    UdfPyTestBigFnNamesSampleCount = 40,
    UdfPyTestBigMaxArg = 70,
};

static void *
udfPyTestBigThread(void *arg)
{
    char(*fnNames)[][UdfPyTestBigFnNameSize] =
        (char(*)[][UdfPyTestBigFnNameSize]) arg;

    for (unsigned iter = 0; iter < UdfPyTestBigExecIters; iter++) {
        GenericArgv argv = {.argvInt = {rand() % UdfPyTestBigMaxArg,
                                        rand() % UdfPyTestBigMaxArg}};

        char name[1024];
        snprintf(name,
                 sizeof(name),
                 "udfPyTestBig:%s",
                 (*fnNames)[rand() % UdfPyTestBigFnNamesSampleCount]);

        pyExec(name, scalarsFromInt, 2, &argv, StatusOk, NULL, NULL);
    }

    return NULL;
}

static Status
udfPyTestBig()
{
    Status status;
    srand(7);

    static const char *comments[] = {
        "# 'There are 2 hard problems in computer science: cache invalidation, "
        "naming things, and off-by-1 errors.' - Leon Bambrick\n",

        "# 'Debugging is twice as hard as writing the code in the first place. "
        "Therefore, if you write the code as cleverly as possible, you are, by "
        "definition, not smart enough to debug it. - Brian Kernighan\n",
    };

    static const char *fnBodies[] = {
        "\tif argument1:\n"
        "\t\treturn argument1\n"
        "\tif argument2:\n"
        "\t\treturn argument2\n"
        "\treturn 99\n",  // return something; a None return isn't valid

        "\taccum = 0\n"
        "\tfor i in range(0, 100):\n"
        "\t\taccum += argument1 * argument2\n"
        "\treturn str(accum)\n",
    };

    static const char *fnNameBefore = "def ";
    size_t fnNameBeforeLen = strlen(fnNameBefore);
    static const char *fnNameAfter = "(argument1, argument2):\n";
    size_t fnNameAfterLen = strlen(fnNameAfter);

    size_t goalSize =
        UdfPyTestBigSourceSizeMin +
        (rand() % (UdfPyTestBigSourceSizeMax - UdfPyTestBigSourceSizeMin));

    // Allocate more than needed so we can overshoot target a bit.
    char *source = (char *) memAlloc(goalSize + 1024);
    assert(source != NULL);
    source[0] = '\0';
    size_t sourceLen = 0;

    char fnNamesSample[UdfPyTestBigFnNamesSampleCount][UdfPyTestBigFnNameSize];
    unsigned lastSample = 0;

    while (sourceLen + 1 < goalSize) {
        // Each iteration appends comment, function header, function name, and
        // function body.
        char fnName[UdfPyTestBigFnNameSize];
        generateName(fnName, UdfPyTestBigFnNameSize);
        if (lastSample < UdfPyTestBigFnNamesSampleCount) {
            strlcpy(fnNamesSample[lastSample],
                    fnName,
                    sizeof(fnNamesSample[lastSample]));
            lastSample++;
        }

        unsigned r;
        size_t len;

        // Tack on a comment.
        r = (unsigned) (rand() % ArrayLen(comments));
        len = strlen(comments[r]);
        memcpy(source + sourceLen, comments[r], len + 1);
        sourceLen += len;

        // Create function header.
        memcpy(source + sourceLen, fnNameBefore, fnNameBeforeLen + 1);
        sourceLen += fnNameBeforeLen;

        len = strlen(fnName);
        memcpy(source + sourceLen, fnName, len + 1);
        sourceLen += len;

        memcpy(source + sourceLen, fnNameAfter, fnNameAfterLen + 1);
        sourceLen += fnNameAfterLen;

        // Add function body.
        r = (unsigned) (rand() % ArrayLen(fnBodies));
        len = strlen(fnBodies[r]);
        memcpy(source + sourceLen, fnBodies[r], len + 1);
        sourceLen += len;
    }

    UdfError error;
    UdfModuleSrc *input = NULL;
    memZero(&error, sizeof(error));

    status = allocModInput(source, "udfPyTestBig", &input);
    BailIfFailed(status);

    verifyOk(udfPy->addModule(input, &error));
    memFree(input);
    input = NULL;

    pthread_t execThreads[UdfPyTestBigExecThreadCount];
    unsigned i;
    for (i = 0; i < UdfPyTestBigExecThreadCount; i++) {
        verifyOk(Runtime::get()->createBlockableThread(&execThreads[i],
                                                       NULL,
                                                       udfPyTestBigThread,
                                                       fnNamesSample));
    }

    for (i = 0; i < UdfPyTestBigExecThreadCount; i++) {
        verify(sysThreadJoin(execThreads[i], NULL) == 0);
    }

    memFree(source);

    status = StatusOk;
CommonExit:

    return status;
}

////////////////////////////////////////////////////////////////////////////////

static Status
udfPyTestInit()
{
    udfPy = new (std::nothrow) UdfPythonImpl;
    assert(udfPy != NULL);
    verifyOk(udfPy->init());
    return StatusOk;
}

static TestCase testCases[] = {
    {"libudfpy: load modules", udfPyTestLoadModules, TestCaseEnable, ""},
    {"libudfpy: sanity", udfPyTestSanity, TestCaseEnable, ""},
    {"libudfpy: validation", udfPyTestInvalid, TestCaseEnable, "2350"},
    {"libudfpy: add/update/error", udfPyTestAddUpdateError, TestCaseEnable, ""},
    {"libudfpy: runtime errors", udfPyTestRuntimeError, TestCaseEnable, "2399"},
    // XXX: See SYS-199 - udfPyTestBig periodically wedges on
    // UdfPythonImpl::lockPython - disable it until the issue is root caused
    {"libudfpy: big", udfPyTestBig, TestCaseDisable, ""},
};

static TestCaseOptionMask testCaseOptionMask = (TestCaseOptionMask)(
    TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime | TestCaseSetTxn);

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    verifyOk(InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* myNodeId */,
                                1 /* numActiveNodes */,
                                1,
                                BufferCacheMgr::TypeUsrnode));

    verifyOk(udfPyTestInit());

    int numTestsFailed = qaRunTestSuite(testCases,
                                        (unsigned) ArrayLen(testCases),
                                        testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
