// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _UDFPYFUNCTION_H_
#define _UDFPYFUNCTION_H_

#include <Python.h>
#include "primitives/Primitives.h"
#include "udf/UdfError.h"
#include "operators/XcalarEvalTypes.h"

class UdfPythonImpl;

//
// Parses function info out of Python and stores enough to execute the function.
//

class UdfPyFunction
{
  public:
    UdfPyFunction(UdfPythonImpl *udfPy);
    ~UdfPyFunction();

    MustCheck Status init(PyObject *pyName,
                          PyObject *pyFunc,
                          const char *moduleName);
    void destroy();

    MustCheck Status execOnLoad(const uint8_t *dataset,
                                size_t datasetSize,
                                const char *fullPath,
                                UdfError *error,
                                PyObject **pyReturn);

    MustCheck Status registerFn();

    MustCheck Status eval(int argc,
                          Scalar *argv[],
                          Scalar *out,
                          UdfError *error);
    MustCheck Status evalApp(const char *input,
                             PyObject **pyReturn,
                             UdfError *error);

    static void destroyWrapper(void *context);
    static MustCheck Status evalWrapper(void *context,
                                        int argc,
                                        Scalar *argv[],
                                        Scalar *out);

    const char *getFnName() const { return fnDesc.fnName; };

  private:
    XcalarEvalFnDesc fnDesc;
    PyObject *pyFunction;
    UdfPythonImpl *udfPython;

    // Disallow.
    UdfPyFunction(const UdfPyFunction &) = delete;
    UdfPyFunction &operator=(const UdfPyFunction &) = delete;

    MustCheck Status evalMap(int argc, Scalar *argv[], Scalar *out);
    MustCheck Status scalarToPyObject(Scalar *scalarVal, PyObject **pyVal);
    MustCheck Status PyObjectToScalar(PyObject *pyVal,
                                      DfFieldType pyType,
                                      Scalar *outScalar,
                                      UdfError *error);
};

#endif  // _UDFPYFUNCTION_H_
