// Copyright 2019 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include "StrlFunc.h"
#include "UdfPyFunction.h"
#include "udf/py/UdfPyError.h"
#include "UdfPythonImpl.h"
#include "operators/XcalarEval.h"
#include "operators/Xdf.h"
#include "datetime.h"
#include "sys/XLog.h"

static constexpr const char *ModuleName = "libudf::pyFunction";

// Uncomment the following if you want debug logs for eval UDF failures
// #define EVAL_DEBUG

//
// Creation and destruction of UdfPyFunctions.
//

UdfPyFunction::UdfPyFunction(UdfPythonImpl *udfPy)
    : pyFunction(NULL), udfPython(udfPy)
{
}

UdfPyFunction::~UdfPyFunction()
{
    assert(pyFunction == NULL);
}

Status
UdfPyFunction::init(PyObject *pyName, PyObject *pyFunc, const char *moduleName)
{
    udfPython->lockPython();
    Status status;

    PyObject *strName = NULL;
    PyObject *codeObject = NULL;
    PyObject *annotations = NULL;
    PyObject *paramCount = NULL;
    PyObject *paramNames = NULL;
    PyObject *flags = NULL;
    DfFieldType funcRetType = DfString;  // default function return type

    char *name;
    ssize_t nameLen;

    PyDateTime_IMPORT;
    memZero(&fnDesc, sizeof(fnDesc));

    // Load function name.
    strName = PyObject_Str(pyName);
    BailIfNullWith(strName, StatusUdfFunctionLoadFailed);

    name = PyUnicode_AsUTF8AndSize(strName, &nameLen);
    if (name == NULL || nameLen == 0) {
        status = StatusUdfFunctionLoadFailed;
        goto CommonExit;
    }
    assert(strlen(name) == (size_t) nameLen);

    if (snprintf(fnDesc.fnName,
                 sizeof(fnDesc.fnName),
                 "%s:%s",
                 moduleName,
                 name) >= (int) sizeof(fnDesc.fnName)) {
        status = StatusUdfFunctionNameTooLong;
        goto CommonExit;
    }

    // Load function object to get remaining properties.
    codeObject = PyObject_GetAttrString(pyFunc, "__code__");
    BailIfNullWith(codeObject, StatusUdfFunctionLoadFailed);

    // Load function annotations to get function return type
    annotations = PyObject_GetAttrString(pyFunc, "__annotations__");

    if (annotations != NULL) {
        Py_ssize_t dictPos = 0;
        PyObject *key = NULL;
        PyObject *value = NULL;
        uint32_t num_annotations = 0;

        while (PyDict_Next(annotations, &dictPos, &key, &value)) {
            if (num_annotations != 0) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "UdfPyFunction init: function %s has more than one "
                        "annotation - ignoring",
                        fnDesc.fnName);
            }
            num_annotations++;
            PyObject *strName = PyObject_Str(key);
            BailIfNullWith(strName, StatusUdfFunctionLoadFailed);
            char *annot_name = PyUnicode_AsUTF8(strName);
            BailIfNullWith(annot_name, StatusUdfFunctionLoadFailed);
            Py_DECREF(strName);

            if (strcmp(annot_name, "return") != 0) {
                // we just care about "return" annotations for return type
                continue;
            }

            if (value == NULL) {
                xSyslog(ModuleName,
                        XlogErr,
                        "UdfPyFunction init: function %s %s annotation is NULL",
                        fnDesc.fnName,
                        annot_name);
                status = StatusUdfFuncAnnotErr;
                goto CommonExit;
            } else if (PyType_Check(value)) {
                if (PyType_IsSubtype((PyTypeObject *) &PyLong_Type,
                                     (PyTypeObject *) value)) {
                    funcRetType = DfInt64;
                } else if (PyType_IsSubtype((PyTypeObject *) &PyFloat_Type,
                                            (PyTypeObject *) value)) {
                    funcRetType = DfFloat64;
                } else if (PyType_IsSubtype((PyTypeObject *) &PyBytes_Type,
                                            (PyTypeObject *) value)) {
                    funcRetType = DfString;
                } else if (PyType_IsSubtype((PyTypeObject *) &PyBool_Type,
                                            (PyTypeObject *) value)) {
                    funcRetType = DfBoolean;
                } else {
                    xSyslog(ModuleName,
                            XlogErr,
                            "UdfPyFunction init: function %s %s type "
                            "is unsupported",
                            fnDesc.fnName,
                            annot_name);
                    status = StatusUdfFuncAnnotErr;
                    goto CommonExit;
                }
            } else {
                xSyslog(ModuleName,
                        XlogErr,
                        "UdfPyFunction init: function %s %s annotation is not "
                        "a Type annotation",
                        fnDesc.fnName,
                        annot_name);
                status = StatusUdfFuncAnnotErr;
                goto CommonExit;
            }
            break;  // quit while loop - done with the 'return' annotation
        }
    }  // else no failure - a func may omit type annotation (default is string)

    // Load and validate parameter count.
    paramCount = PyObject_GetAttrString(codeObject, "co_argcount");

    BailIfNullWith(paramCount, StatusUdfFunctionLoadFailed);
    fnDesc.numArgs = (int) PyLong_AsLong(paramCount);

    if (fnDesc.numArgs > XcalarEvalMaxNumArgs) {
        status = StatusUdfFunctionTooManyParams;
        goto CommonExit;
    }

    paramNames = PyObject_GetAttrString(codeObject, "co_varnames");
    BailIfNullWith(paramNames, StatusUdfFunctionLoadFailed);

    if (!PyTuple_Check(paramNames)) {
        status = StatusUdfFunctionLoadFailed;
        goto CommonExit;
    }

    for (unsigned ii = 0; ii < (unsigned) fnDesc.numArgs; ii++) {
        // Get info about parameter i.
        PyObject *paramNameObj = PyTuple_GetItem(paramNames, (Py_ssize_t) ii);
        BailIfNullWith(paramNameObj, StatusUdfFunctionLoadFailed);

        char *paramNameStr = PyUnicode_AsUTF8(paramNameObj);
        BailIfNullWith(paramNameStr, StatusUdfFunctionLoadFailed);

        if (strlen(paramNameStr) + 1 > sizeof(fnDesc.argDescs[ii].argDesc)) {
            status = StatusUdfVarNameTooLong;
            goto CommonExit;
        }

        verify(strlcpy(fnDesc.argDescs[ii].argDesc,
                       paramNameStr,
                       sizeof(fnDesc.argDescs[ii].argDesc)) <=
               sizeof(fnDesc.argDescs[ii].argDesc));

        // XXX Type restrictions.
        fnDesc.argDescs[ii].typesAccepted = XdfAcceptAll;
        fnDesc.argDescs[ii].isSingletonValue = true;
    }

    flags = PyObject_GetAttrString(codeObject, "co_flags");
    BailIfNullWith(flags, StatusUdfFunctionLoadFailed);

    // Load varargs parameter if present.
    if (PyLong_AsLong(flags) & CO_VARARGS) {
        assert(fnDesc.numArgs >= 0);
        // *args param name after other params.
        size_t index = fnDesc.numArgs;

        if (index >= XcalarEvalMaxNumArgs) {
            status = StatusUdfFunctionTooManyParams;
            goto CommonExit;
        }

        PyObject *paramNameObj =
            PyTuple_GetItem(paramNames, (Py_ssize_t) index);
        BailIfNullWith(paramNameObj, StatusUdfFunctionLoadFailed);

        char *paramNameStr = PyUnicode_AsUTF8(paramNameObj);
        BailIfNullWith(paramNameStr, StatusUdfFunctionLoadFailed);

        if ((unsigned) snprintf(fnDesc.argDescs[index].argDesc,
                                sizeof(fnDesc.argDescs[index].argDesc),
                                "*%s",
                                paramNameStr) >
            sizeof(fnDesc.argDescs[index].argDesc)) {
            status = StatusUdfVarNameTooLong;
            goto CommonExit;
        }

        // XXX Type restrictions.
        fnDesc.argDescs[index].typesAccepted = XdfAcceptAll;
        fnDesc.argDescs[index].isSingletonValue = true;

        // Negate to indicate var args.
        fnDesc.numArgs = (fnDesc.numArgs + 1) * -1;
    }

    // Populate miscellaneous values.
    fnDesc.category = FunctionCategoryUdf;

    fnDesc.outputType = funcRetType;
    fnDesc.isSingletonOutput = true;

    fnDesc.context = (void *) this;

    pyFunction = pyFunc;
    Py_INCREF(pyFunction);

    status = StatusOk;

CommonExit:
    if (strName != NULL) {
        Py_DECREF(strName);
    }
    if (codeObject != NULL) {
        Py_DECREF(codeObject);
    }
    if (annotations != NULL) {
        Py_DECREF(annotations);
    }
    if (paramCount != NULL) {
        Py_DECREF(paramCount);
    }
    if (paramNames != NULL) {
        Py_DECREF(paramNames);
    }
    if (flags != NULL) {
        Py_DECREF(flags);
    }
    udfPython->unlockPython();
    return status;
}

void
UdfPyFunction::destroy()
{
    udfPython->lockPython();
    Py_DECREF(pyFunction);
    pyFunction = NULL;
    udfPython->unlockPython();
}

// Register function with XcalarEval. Lifetime is, henceforth, managed by
// XcalarEval via the below callbacks (and its internal ref counting).
Status
UdfPyFunction::registerFn()
{
    return XcalarEval::get()->addUdf(&fnDesc,
                                     UdfPyFunction::evalWrapper,
                                     UdfPyFunction::destroyWrapper);
}

////////////////////////////////////////////////////////////////////////////////

//
// Random wrappers necessar for registering C-style callbacks.
//

void  // static
UdfPyFunction::destroyWrapper(void *context)
{
    UdfPyFunction *self = (UdfPyFunction *) context;
    self->destroy();
    delete self;
}

Status  // static
UdfPyFunction::evalWrapper(void *context, int argc, Scalar *argv[], Scalar *out)
{
    UdfPyFunction *self = (UdfPyFunction *) context;
    return self->evalMap(argc, argv, out);
}

////////////////////////////////////////////////////////////////////////////////

//
// Execution of UDF functions.
//

// XXX libapp specific eval function. Should not be in libudf.
Status
UdfPyFunction::evalApp(const char *input, PyObject **pyReturn, UdfError *error)
{
    Status status;
    PyObject *returnVal = NULL;
    PyObject *args = NULL;
    PyObject *inStr = NULL;
    PyObject *pyGcMod = NULL;
    PyObject *pyGcRet = NULL;

    udfPython->lockPython();

    args = PyTuple_New(1);
    BailIfNull(args);

    inStr = PyUnicode_FromString(input);
    BailIfNullWith(inStr, StatusNoMem);

    if (PyTuple_SetItem(args, 0, inStr) != 0) {
        Py_DECREF(inStr);
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    returnVal = PyObject_CallObject(pyFunction, args);
    if (returnVal == NULL) {
        // Attempt to populate output with error message.
        udfPyErrorCreate(error, NULL);
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    *pyReturn = returnVal;  // Caller must dec ref.
    status = StatusOk;

CommonExit:
    if (args != NULL) {
        // args deallocator will decref on all objects in tuple.
        Py_DECREF(args);
    }

    // Kick python GC
    // https://docs.python.org/3/library/gc.html
    pyGcMod = PyImport_ImportModule("gc");
    if (pyGcMod != NULL) {
        pyGcRet = PyObject_CallMethod(pyGcMod, "collect", NULL);
        if (pyGcRet != NULL) {
            Py_DECREF(pyGcRet);
            pyGcRet = NULL;
        }
        Py_DECREF(pyGcMod);
        pyGcMod = NULL;
    }

    udfPython->unlockPython();

    return status;
}

// Execute a function that exists within a module given arguments described by
// argc and argv. On success, result is populated with return value. On error,
// error is populated with any exceptions detected by Python.
Status
UdfPyFunction::eval(int argc, Scalar *argv[], Scalar *out, UdfError *error)
{
    Status status;
    PyObject *args = NULL;
    PyObject *returnVal = NULL;

    assert(argc >= 0);
    assert(out != NULL);

    udfPython->lockPython();

    //
    // For each arg, marshall to Python object. Create tuple of args and load in
    // Scalar values.
    //

    args = PyTuple_New(argc);
    BailIfNull(args);

    for (int ii = 0; ii < argc; ii++) {
        PyObject *val = NULL;

        if (argv[ii] != NULL && !xdfIsValidArg(ii, DfUnknown, argc, argv)) {
            status = StatusUdfInval;
            goto CommonExit;
        }

        status = scalarToPyObject(argv[ii], &val);
        BailIfFailed(status);

        if (PyTuple_SetItem(args, ii, val) != 0) {
            Py_DECREF(val);
            status = StatusUdfExecuteFailed;
            goto CommonExit;
        }
    }

    //
    // Execute function!
    //

    returnVal = PyObject_CallObject(pyFunction, args);
    if (returnVal == NULL) {
        udfPyErrorCreate(error, NULL);
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    if (returnVal == Py_None) {
        // In this case, the UDF does not return a string but DfNull. In all
        // other cases, it returns a string currently - see below.
        status = StatusOk;
        out->setValue(DfFieldValueNull, DfNull);
        goto CommonExit;
    }

    //
    // Convert returnVal to desired return type on way out to "out" Scalar
    //

    status = PyObjectToScalar(returnVal, fnDesc.outputType, out, error);

CommonExit:
    if (args != NULL) {
        // args deallocator will decref on all objects in tuple.
        Py_DECREF(args);
    }
    if (returnVal != NULL) {
        // This destroys string pointed to by strVal.
        Py_DECREF(returnVal);
    }

    udfPython->unlockPython();
    return status;
}

// Evaluates function and, on error, provides error value in place of expected
// value.
Status
UdfPyFunction::evalMap(int argc, Scalar *argv[], Scalar *out)
{
    UdfError error;
    Status status = eval(argc, argv, out, &error);
    if (status != StatusOk) {
        if (error.message_ != NULL) {
#ifdef EVAL_DEBUG
            xSyslog(ModuleName,
                    XlogDebug,
                    "UdfPyFunction evalMap failure '%s': '%s'",
                    error.message_,
                    strGetFromStatus(status));
#endif
            // Below allowed to fail silently and empty + original status
            // returned.
            xdfMakeStringIntoScalar(out,
                                    error.message_,
                                    strlen(error.message_));
        } else {
            // If error string wasn't generated by eval, at least report the
            // one from the status code that eval returned
            xdfMakeStringIntoScalar(out,
                                    strGetFromStatus(status),
                                    strlen(strGetFromStatus(status)));
        }
        // Since the failure description from eval() is safely catpured in the
        // 'out' Scalar, the status from UdfPyFunction::evalMap() can change
        // to whatever status is useful to communicate with the upper, generic
        // layers of code (e.g. in XcalarEval::evalInternal() which may need
        // to NULL'ify the 'out' Scalar for other non-UDF code paths - but which
        // would be incorrect for UDF eval).
        status = StatusUdfExecuteFailed;
    }
    return status;
}

// Converts a Python object to Xcalar's internal Scalar object given a type
Status
UdfPyFunction::PyObjectToScalar(PyObject *pyVal,
                                DfFieldType pyType,
                                Scalar *outScalar,
                                UdfError *error)
{
    Status status = StatusUnknown;
    int64_t retInt = 0;
    float64_t retFloat = 0;
    PyObject *asStr = NULL;
    bool hasStrRef = false;

    switch (pyType) {
    case DfInt64:
        retInt = PyLong_AsLongLong(pyVal);
        if (retInt == -1) {
            // NOTE: If a non-NULL message in 'error' is generated, for its
            // insertion into the AST (ultimately, the row subject to the
            // eval), then the status code here almost doesn't matter since
            // evalMap() will stuff the error string into the AST, and return
            // StatusUdfExecuteFailed (over-riding the status used here).
            udfPyErrorCreate(error, NULL);
            status = StatusUdfExecuteFailed;  // any non-ok status is fine here!
        } else {
            status = xdfMakeInt64IntoScalar(outScalar, retInt);
            if (status == StatusOverflow) {
                // This allows a more meaningful status string to be pushed
                // upwards customized to the scalar function context
                status = StatusScalarFunctionFieldOverflow;
            }
        }
        return (status);
        break;
    case DfFloat64:
        retFloat = (float64_t) PyFloat_AsDouble(pyVal);  // XXX: review cast
        if (retFloat == -1) {
            udfPyErrorCreate(error, NULL);
            status = StatusUdfExecuteFailed;
        } else {
            status = xdfMakeFloat64IntoScalar(outScalar, retFloat);
            if (status == StatusOverflow) {
                // This allows a more meaningful status string to be pushed
                // upwards customized to the scalar function context
                status = StatusScalarFunctionFieldOverflow;
            }
        }
        return (status);
        break;
    case DfBoolean:
        if (PyBool_Check(pyVal)) {
            if (pyVal == Py_True) {
                status = xdfMakeBoolIntoScalar(outScalar, true);
            } else {
                status = xdfMakeBoolIntoScalar(outScalar, false);
            }
            if (status == StatusOverflow) {
                // This allows a more meaningful status string to be pushed
                // upwards customized to the scalar function context
                status = StatusScalarFunctionFieldOverflow;
            }
        } else {
            // func ret type annotated as bool, but pyVal isn't
            status = StatusUdfFuncRetBoolInvalid;
        }
        return (status);
        break;
    case DfString:
        asStr = pyVal;
        if (!PyUnicode_Check(pyVal)) {
            asStr = PyObject_Str(pyVal);
            if (asStr == NULL) {
                status = StatusUdfFuncRetStrInvalid;
                return (status);
            }
            hasStrRef = true;
        }
        ssize_t strLength;
        char *strVal;
        strVal = PyUnicode_AsUTF8AndSize(asStr, &strLength);
        if (strVal == NULL) {
            status = StatusUdfFuncRetStrInvalid;
            if (hasStrRef) {
                Py_DECREF(asStr);
                asStr = NULL;
            }
            return (status);
        }

        status = xdfMakeStringIntoScalar(outScalar, strVal, strlen(strVal));
        if (status == StatusOverflow) {
            // This allows a more meaningful status string to be pushed upwards
            // customized to the scalar function context
            status = StatusScalarFunctionFieldOverflow;
        }
        if (hasStrRef) {
            Py_DECREF(asStr);
            asStr = NULL;
        }
        return (status);
        break;
    default:
        assert(0 && "Invalid CSF return type");
        status = StatusUdfFuncRetInvalid;
        return (status);
    }
    return (status);
}

// Converts Xcalar's internal Scalar object to a python object.
Status
UdfPyFunction::scalarToPyObject(Scalar *scalarVal, PyObject **pyVal)
{
    Status status;
    PyObject *val = NULL;
    PyObject *timestamp = NULL, *timeTuple = NULL;

    if (scalarVal == NULL || scalarVal->fieldNumValues == 0 ||
        scalarVal->fieldType == DfNull) {
        *pyVal = Py_None;
        Py_INCREF(Py_None);
        status = StatusOk;
        goto CommonExit;
    }

    switch (scalarVal->fieldType) {
    case DfString: {
        const char *stringVal;
        int stringLen;

        status = xdfGetStringFromArgv(1, &stringVal, &stringLen, &scalarVal);
        BailIfFailed(status);

        val = PyUnicode_FromString(stringVal);
        BailIfNullWith(val, StatusUdfPyConvert);

        break;
    }

    case DfInt32:
    case DfInt64: {
        ssize_t intVal;
        assertStatic(sizeof(intVal) == sizeof(int64_t));

        status = xdfGetInt64FromArgv(1, &intVal, &scalarVal);
        BailIfFailed(status);

        val = PyLong_FromSsize_t(intVal);
        BailIfNullWith(val, StatusUdfPyConvert);

        break;
    }

    case DfUInt32:
    case DfUInt64: {
        size_t uintVal;
        assertStatic(sizeof(uintVal) == sizeof(uint64_t));

        status = xdfGetUInt64FromArgv(1, &uintVal, &scalarVal);
        BailIfFailed(status);

        val = PyLong_FromSize_t(uintVal);
        BailIfNullWith(val, StatusUdfPyConvert);

        break;
    }

    case DfTimespec: {
        ssize_t intVal;

        status = xdfGetInt64FromArgv(1, &intVal, &scalarVal);
        BailIfFailed(status);

        double unixTime = (double) (intVal) / 1000;

        timestamp = PyFloat_FromDouble(unixTime);
        BailIfNullWith(timestamp, StatusUdfPyConvert);

        timeTuple = Py_BuildValue("(O)", timestamp);
        BailIfNullWith(timeTuple, StatusUdfPyConvert);

        val = PyDateTime_FromTimestamp(timeTuple);
        BailIfNullWith(val, StatusUdfPyConvert);
        break;
    }

    case DfFloat32:
    case DfFloat64: {
        double floatVal;

        status = xdfGetFloat64FromArgv(1, &floatVal, &scalarVal);
        BailIfFailed(status);

        val = PyFloat_FromDouble(floatVal);
        BailIfNullWith(val, StatusUdfPyConvert);

        break;
    }

    case DfBoolean: {
        // Warning: The python doc is woefully deficient in describing the
        //          behavior of pValue = PyTrue, etc.  Look at the python
        //          boolobject.h file or dig through the python source.
        bool boolVal;

        status = xdfGetBoolFromArgv(1, &boolVal, &scalarVal);
        BailIfFailed(status);

        // Caller is responsible for decrementing these refs. They will be
        // decremented automatically by that container's deallocator.

        if (boolVal) {
            val = Py_True;
            Py_INCREF(Py_True);
        } else {
            val = Py_False;
            Py_INCREF(Py_False);
        }

        break;
    }

    default:
        status = StatusUdfUnsupportedType;
        goto CommonExit;
    }

    assert(val != NULL);
    *pyVal = val;

    status = StatusOk;

CommonExit:
    if (timestamp) {
        Py_DECREF(timestamp);
    }

    if (timeTuple) {
        Py_DECREF(timeTuple);
    }

    assert(status == StatusOk || val == NULL);
    return status;
}

// Execute requested python function given two params: the full contents of the
// current dataset segment and the full path to the file it came from. Path is
// not accessible to some childnodes.
// XXX This API currently involves the following potentially large copies:
//      - dataset is copied into a python arg.
//      - pyReturn will be copied to ship back to the requesting usrnode.
Status
UdfPyFunction::execOnLoad(const uint8_t *dataset,
                          size_t datasetSize,
                          const char *fullPath,
                          UdfError *error,
                          PyObject **pyReturn)
{
    Status status;

    PyObject *args = NULL;
    PyObject *pyFullPath = NULL;
    PyObject *pyDataset = NULL;
    PyObject *pyReturnObj = NULL;
    PyObject *asStr = NULL;

    udfPython->lockPython();

    // Create tuple of args and load in Scalar values.
    args = PyTuple_New(2);
    BailIfNull(args);

    pyFullPath = PyUnicode_FromString(fullPath);
    BailIfNull(pyFullPath);

    assert(datasetSize > 0);  // Guaranteed way back in loadOneFile.
    pyDataset = PyUnicode_FromStringAndSize((char *) dataset, datasetSize - 1);
    BailIfNull(pyDataset);

    if (PyTuple_SetItem(args, 0, pyDataset) != 0) {
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }
    pyDataset = NULL;  // Ref owned by args.

    if (PyTuple_SetItem(args, 1, pyFullPath) != 0) {
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }
    pyFullPath = NULL;  // Ref owned by args.

    pyReturnObj = PyObject_CallObject(pyFunction, args);
    if (pyReturnObj == NULL) {
        udfPyErrorCreate(error, NULL);
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    // XXX More thoughtful return type.
    asStr = pyReturnObj;
    if (!PyUnicode_Check(pyReturnObj)) {
        asStr = PyObject_Str(pyReturnObj);
        BailIfNullWith(asStr, StatusUdfExecuteFailed);
    }

    *pyReturn = asStr;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (pyReturnObj != NULL) {
            Py_DECREF(pyReturnObj);
        }
    }
    if (args != NULL) {
        Py_DECREF(args);
    }
    if (pyDataset != NULL) {
        Py_DECREF(pyDataset);
    }
    if (pyFullPath != NULL) {
        Py_DECREF(pyFullPath);
    }

    udfPython->unlockPython();
    return status;
}
