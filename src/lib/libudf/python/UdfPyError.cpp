// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include "udf/py/UdfPyError.h"
#include "strings/String.h"

//
// Allows creation of python specific UdfError "objects".
//

static char *
pyObjToString(PyObject *obj, size_t *strSizeOut)
{
    PyObject *uniObj = NULL;
    PyObject *uniConvertedObj = NULL;
    char *str = NULL;
    char *strVal;
    ssize_t strSize = 0;

    if (!PyUnicode_Check(obj)) {
        uniConvertedObj = PyObject_Str(obj);
        if (uniConvertedObj == NULL) {
            goto CommonExit;
        }
        uniObj = uniConvertedObj;
    } else {
        uniObj = obj;
    }

    // At this point we have a unicode object; we need to UTF-8 encode it
    assert(PyUnicode_Check(uniObj));

    strVal = PyUnicode_AsUTF8AndSize(obj, &strSize);
    if (strVal == NULL || strSize == 1) {
        // Special case empty string as NULL.
        goto CommonExit;
    } else {
        str = strAllocAndCopyWithSize(strVal, strSize + 1);
    }

    if (str != NULL && strSizeOut != NULL) {
        *strSizeOut = strSize;
    }

CommonExit:
    if (uniConvertedObj) {
        Py_DECREF(uniConvertedObj);
        uniConvertedObj = NULL;
    }
    return str;
}

// Loads current python exception into UdfError object, if one exists.
// Otherwise, load the error string from nonPyErrStr.
void
udfPyErrorCreate(UdfError *error, char *nonPyErrStr)
{
    PyObject *excType = NULL;
    PyObject *excValue = NULL;
    PyObject *excTraceback = NULL;
    PyObject *traceback = NULL;
    PyObject *formatException = NULL;
    PyObject *arrObj = NULL;
    PyObject *stackIter = NULL;
    PyObject *fullTrace = NULL;

    assert(error != NULL);
    memZero(error, sizeof(*error));

    if (PyErr_Occurred()) {
        PyErr_Fetch(&excType, &excValue, &excTraceback);

        // https://bugs.python.org/issue17413
        // https://hg.python.org/cpython/rev/d18df4c90515
        // Sometimes the exception is messed up; Normalize will fix this
        PyErr_NormalizeException(&excType, &excValue, &excTraceback);
        if (excTraceback != NULL) {
            PyException_SetTraceback(excValue, excTraceback);
        }

        traceback = PyImport_ImportModule("traceback");
        if (traceback == NULL) {
            goto CommonExit;
        }

        if (excTraceback == NULL) {
            formatException = PyUnicode_FromString("format_exception_only");
            if (formatException == NULL) {
                goto CommonExit;
            }

            arrObj = PyObject_CallMethodObjArgs(traceback,
                                                formatException,
                                                excType,
                                                excValue,
                                                NULL);

            if (arrObj == NULL) {
                goto CommonExit;
            }
        } else {
            formatException = PyUnicode_FromString("format_exception");
            if (formatException == NULL) {
                goto CommonExit;
            }

            arrObj = PyObject_CallMethodObjArgs(traceback,
                                                formatException,
                                                excType,
                                                excValue,
                                                excTraceback,
                                                NULL);
            if (arrObj == NULL) {
                if (PyErr_GivenExceptionMatches(excType, PyExc_MemoryError)) {
                    // XXX: Work-around to the issue that
                    // PyObject_CallMethodObjArgs fails when invoking
                    // format_exception method on the traceback object, at
                    // least when excType is PyExc_MemoryError. It's
                    // important to at least report that this is a memory
                    // error since apps may run into XPU resource limits,
                    // such as limits on VA. The buffer size for excBuffer
                    // is very unlikely to be exceeded. If it is, as caught
                    // by the assert, replace with dynamic allocation.
                    //
                    char excBuffer[256];
                    int ret;
                    ret = snprintf(excBuffer,
                                   sizeof(excBuffer),
                                   "Python MemoryError exception due to "
                                   "\"%s\"",
                                   excValue ? pyObjToString(excValue, NULL)
                                            : "Unknown reason");
                    assert(ret < (int) sizeof(excBuffer));
                    error->message_ = strAllocAndCopy(excBuffer);
                }
                goto CommonExit;
            }
        }
        // We now have the backtrace, sliced up into a list of strings.
        // Let's concatenate those strings here.
        if (PyList_Check(arrObj)) {
            fullTrace = PyUnicode_FromString("");
            if (fullTrace == NULL) {
                goto CommonExit;
            }
            stackIter = PyObject_GetIter(arrObj);
            PyObject *frameObj;
            while ((frameObj = PyIter_Next(stackIter))) {
                PyObject *newTrace = PyUnicode_Concat(fullTrace, frameObj);
                Py_DECREF(frameObj);  // We only needed this briefly
                if (newTrace == NULL) {
                    goto CommonExit;
                }
                // Swap fullTrace with the newTrace
                Py_DECREF(fullTrace);
                fullTrace = newTrace;
            }
        }
        error->message_ = pyObjToString(fullTrace, NULL);
    } else if (nonPyErrStr != NULL) {
        // if PyErr_Occurred() is false error should be updated with the
        // non-python error information supplied via nonPyErrStr
        error->message_ = nonPyErrStr;
    }

CommonExit:
    if (excType != NULL) {
        Py_DECREF(excType);
    }
    if (excValue != NULL) {
        Py_DECREF(excValue);
    }
    if (excTraceback != NULL) {
        Py_DECREF(excTraceback);
    }
    if (traceback != NULL) {
        Py_DECREF(traceback);
    }
    if (formatException != NULL) {
        Py_DECREF(formatException);
    }
    if (arrObj != NULL) {
        Py_DECREF(arrObj);
    }
    if (stackIter != NULL) {
        Py_DECREF(stackIter);
    }
    if (fullTrace != NULL) {
        Py_DECREF(fullTrace);
    }

    PyErr_Clear();
}
