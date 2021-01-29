// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef UDFPYTHONIMPL_H
#define UDFPYTHONIMPL_H

#include <Python.h>
#include "UdfPyXcalar.h"
#include "udf/UdfError.h"
#include "util/AtomicTypes.h"
#include "runtime/Mutex.h"

struct XcalarApiOutput;

class UdfPyFunction;

//
// Entry point for all Python specific UDF processing. Makes use of XcalarEval
// for storing functions. This module doesn't make any assumptions about which
// binary it's executed in.
//

// XXX There should be non-globally-scoped UDFs that are stored on a per-user or
//     per-group basis. Different groups will need to be in different processes
//     to avoid name collisions.

class UdfPythonImpl final
{
  public:
    UdfPythonImpl();
    ~UdfPythonImpl();

    MustCheck Status init();
    void destroy();

    MustCheck Status addModule(UdfModuleSrc *input, UdfError *error);
    MustCheck Status updateModule(const char *moduleName,
                                  const char *source,
                                  UdfError *error);
    void removeModule(const char *moduleName);
    MustCheck Status listModule(const char *moduleName,
                                XcalarApiOutput **output);

    MustCheck Status execOnLoad(const char *fqFunctionName,
                                const uint8_t *dataset,
                                size_t datasetSize,
                                const char *fullPath,
                                UdfError *error,
                                PyObject **pyReturn);

    void lockPython();
    void unlockPython();

    MustCheck Status parseMainFunction(const char *source,
                                       const char *appName,
                                       UdfPyFunction **mainFnOut,
                                       UdfError *error);

  private:
    static constexpr size_t DummyModuleNameLen = 63;
    static constexpr const char *ModuleName = "libudf::python";
    // string to be used when there's no name for UDF or app
    static constexpr const char *AnonName = "anonymous";

    // type of python code being executed in child: UDF or App? This
    // helps name and hence identify the code in diagnostic messages
    enum class ChildType {
        Invalid = 0,
        UDF,
        App,
    };

    bool isInit;
    PyThreadState *pyMainThreadState;
    Mutex lock;
    PyGILState_STATE gilState;
    UdfPyXcalar xcalarModule;

    // Disallow.
    UdfPythonImpl(const UdfPythonImpl &) = delete;
    UdfPythonImpl &operator=(const UdfPythonImpl &) = delete;

    // when generating a unique name for the code, pass as much information
    // as possible (chType, moduleName, etc.) to generate a meaningful name
    // to help identify it in diagnostic messages
    void generateUniqueName(char *uniqueName,
                            size_t uniqueNameSize,
                            ChildType chType,
                            const char *moduleName,
                            const char *moduleSource);

    MustCheck Status parseModule(const char *moduleName,
                                 const char *modulePath,
                                 const char *source,
                                 UdfError *error);
    MustCheck Status injectXcalarModule();

    // If the message string for 'status' contains '<>', the following routine
    // will return a newly allocated string, with fnName replacing '<>'. The
    // caller is responsible for freeing the returned memory.
    char *strGetWithFnFromStatus(Status status, char *fnName);
};

#endif  // UDFPYTHONIMPL_H
