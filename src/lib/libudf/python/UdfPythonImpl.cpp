// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <Python.h>
#include <new>
#include <stdio.h>
#include "UdfPythonImpl.h"
#include "UdfPyFunction.h"
#include "LibUdfConstants.h"
#include "udf/py/UdfPyError.h"
#include "operators/XcalarEval.h"
#include "sys/XLog.h"
#include <libgen.h>

UdfPythonImpl::UdfPythonImpl()
    : isInit(false), pyMainThreadState(NULL), xcalarModule()
{
}

UdfPythonImpl::~UdfPythonImpl()
{
    assert(!isInit);
}

Status
UdfPythonImpl::init()
{
    Status status;
    wchar_t *wEmpty = NULL;
    wchar_t *fakeArgv[2] = {NULL, NULL};
    assert(!isInit);
    PyObject *xcalarScripts = NULL;

    // This should be called before Py_Initialize
    status = xcalarModule.injectModule();
    BailIfFailed(status);

    Py_Initialize();
    PyEval_InitThreads();

    // Set argv; some libraries rely on this being set

    // Convert empty string to the wchar_t equivalent
    wEmpty = Py_DecodeLocale("", NULL);
    BailIfNull(wEmpty);
    fakeArgv[0] = wEmpty;

    // This will internally make a pylist from our argv, so we can keep it
    // stack allocated here.
    PySys_SetArgv(1, fakeArgv);

    pyMainThreadState = PyThreadState_Swap(NULL);
    PyEval_ReleaseLock();
    isInit = true;

    PyGILState_STATE gilState;
    gilState = PyGILState_Ensure();

    PyObject *path;
    path = NULL;

    const char *xlrDir;
    xlrDir = getenv("XLRDIR");

    path = PySys_GetObject((char *) "path");  // Borrowed ref.

    if (path == NULL) {
        xSyslog(ModuleName, XlogErr, "Failed to lookup sys.path");
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    if (xlrDir == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "$XLRDIR environment variable must point to Xcalar install");
        status = StatusUdfModuleLoadFailed;
        goto CommonExit;
    }

    xcalarScripts = PyUnicode_FromFormat("%s/scripts", xlrDir);
    if (xcalarScripts == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to allocate python string for %s/scripts",
                xlrDir);
        status = StatusNoMem;
        goto CommonExit;
    }

    if (PyList_Append(path, xcalarScripts) != 0) {
        xSyslog(ModuleName, XlogErr, "Failed to append to sys.path");
        status = StatusUdfExecuteFailed;
        goto CommonExit;
    }

    PyGILState_Release(gilState);

    isInit = true;

CommonExit:
    if (xcalarScripts != NULL) {
        Py_DECREF(xcalarScripts);
    }
    if (status != StatusOk) {
        destroy();
    }
    return status;
}

void
UdfPythonImpl::destroy()
{
    if (!isInit) {
        return;
    }

    PyEval_RestoreThread(pyMainThreadState);
    Py_Finalize();

    isInit = false;
}

////////////////////////////////////////////////////////////////////////////////

// Due to versioning, we don't use the actual moduleName when importing source
// into Python. Generate a dummy name.
void
UdfPythonImpl::generateUniqueName(char *uniqueName,
                                  size_t uniqueNameSize,
                                  ChildType chType,
                                  const char *moduleName,
                                  const char *moduleSource)
{
    uint64_t num = hashStringFast(moduleSource);
    verify(snprintf(uniqueName,
                    uniqueNameSize,
                    "xcalar_%s_%s_%llx",
                    chType == ChildType::UDF ? "udf" : "app",
                    moduleName ? moduleName : AnonName,
                    (unsigned long long) num) < (int) uniqueNameSize);
}

// Return newly allocated string insering fnName into the status string.
// Caller must free the returned string
char *
UdfPythonImpl::strGetWithFnFromStatus(Status status, char *fnName)
{
    char statusStr[1024];
    char statusStrWithFname[1024];
    char *postFn = NULL;
    int ret;

    ret =
        snprintf(statusStr, sizeof(statusStr), "%s", strGetFromStatus(status));
    assert(ret < (int) sizeof(statusStr));

    postFn = strchr(statusStr, '<');
    if (postFn != NULL) {
        *postFn = '\0';  // terminate statusStr to section before the '<'
        postFn += 2;     // skip over the closing '>'
        // Now insert the fnName by snprintf'ing into statusStrWithFname
        ret = snprintf(statusStrWithFname,
                       sizeof(statusStrWithFname),
                       "%s%s%s",
                       statusStr,
                       fnName,
                       postFn);
        assert(ret < (int) sizeof(statusStrWithFname));
        return (strAllocAndCopy(statusStrWithFname));
    } else {
        return (strAllocAndCopy(statusStr));
    }
}

// Parse out all functions exposed by this module. Register them under the given
// module name.
Status
UdfPythonImpl::parseModule(const char *moduleName,
                           const char *modulePath,
                           const char *source,
                           UdfError *error)
{
    Status status;
    PyObject *module = NULL;
    PyObject *moduleReloaded = NULL;
    PyObject *dict = NULL;
    PyObject *modpathObj = NULL;
    PyObject *modpathStrObj = NULL;
    unsigned functionPassCount = 0;
    unsigned functionFailed = 0;
    Py_ssize_t dictPos = 0;
    bool pyLocked = false;
    PyObject *compiled = NULL;
    PyObject *modNameObj = NULL;
    char *moduleNameDup = NULL;
    char *modpathStrDup = NULL;
    char *inModulePathDup = NULL;
    // package qualified module name
    char pqmodName[MaxUdfPathLen + 1];
    char *modBaseName = NULL;
    // Just use the full on-disk path-name to the UDF module as its unique name
    // if the on-disk path is available in modulePath
    char uniqueName[4 * LibNsTypes::MaxPathNameLen];
    char fnName[XcalarEvalMaxFnNameLen + 1];

    // In some cases (e.g. map invocation), the modulName
    // is typically fully qualified, hence the basename() invocation to get the
    // base module name
    moduleNameDup = strAllocAndCopy(moduleName);
    BailIfNull(moduleNameDup);
    modBaseName = basename(moduleNameDup);

    if (modulePath != NULL && modulePath[0] != '\0') {
        // even if compiling from source, use of modulePath (if available) to
        // generate the unique name is always a good idea - since it IS unique,
        // AND this allows displaying the full path/info for a UDF, in case of
        // failure, helping diagnosability
        verify(snprintf(uniqueName,
                        sizeof(uniqueName),
                        "%s/%s%s",
                        modulePath,
                        modBaseName,
                        udf::pySourceFileSuffix) < (int) sizeof(uniqueName));
    } else {
        generateUniqueName(uniqueName,
                           sizeof(uniqueName),
                           ChildType::UDF,
                           moduleName,
                           source);
    }

    xSyslog(ModuleName,
            XlogDebug,
            "Callled parseModule for module '%s', at path '%s' (source %s)",
            moduleName,
            modulePath,
            source == NULL ? "absent" : "present");

    lockPython();
    pyLocked = true;

    if (source == NULL) {
        // import the module, if source isn't provided. This is the case when
        // some operator / e.g. map invokes it.
        xSyslog(ModuleName,
                XlogDebug,
                "parseModule will try to import module '%s' (source absent)",
                modBaseName);
        // sys.path inserts for import to work done by caller

        inModulePathDup = strAllocAndCopy(modulePath);
        BailIfNullWith(inModulePathDup, StatusNoMem);

        snprintf(pqmodName,
                 sizeof(pqmodName),
                 "%s.%s",
                 basename(
                     inModulePathDup),  // basename(modulepath) == 'pkg name'
                 modBaseName);

        module = PyImport_ImportModule(pqmodName);
        if (module != NULL) {
            char *modpathStr = NULL;
            char *modpathCurrDir = NULL;

            modpathObj = PyObject_GetAttrString(module, "__file__");
            if (modpathObj == NULL) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "module '%s' no file attr "
                        "requested path is '%s'",
                        modBaseName,
                        modulePath);
                status = StatusUdfModuleLoadFailed;
                goto CommonExit;
            }

            modpathStrObj = PyObject_Str(modpathObj);
            modpathStr = PyUnicode_AsUTF8(modpathStrObj);

            modpathStrDup = strAllocAndCopy(modpathStr);
            modpathCurrDir = dirname(modpathStrDup);

            if (strcmp(modulePath, modpathCurrDir) != 0) {
                // XXX: this needs vetting; if path's changing, probably need
                // to re-exec the module?
                xSyslog(ModuleName,
                        XlogInfo,
                        "module '%s' changing paths - "
                        "old path was '%s', new path is '%s'",
                        modBaseName,
                        modpathCurrDir,
                        modulePath);
            }

            // Reload must always be called - even if the path's not changing,
            // the contents of the UDF may have changed since this XPU was last
            // dispatched and happened to import a module with the same name
            // and path. And ImportModule will not do the reload.
            moduleReloaded = PyImport_ReloadModule(module);
            if (moduleReloaded == NULL) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "module '%s' reload failed",
                        modBaseName);
                status = StatusUdfModuleLoadFailed;
                goto CommonExit;
            }
            Py_DECREF(module);
            // release reference on the old module since PyImport_ReloadModule
            // returns a new reference in moduleReloaded. Now, module var is
            // free to be re-assigned to the new reloaded module.

            module = moduleReloaded;

            xSyslog(ModuleName,
                    XlogDebug,
                    "(re)imported module '%s' at path '%s'",
                    modBaseName,
                    modulePath);
        } else {
            xSyslog(ModuleName,
                    XlogDebug,
                    "failed to import module '%s' with full path '%s'",
                    modBaseName,
                    modulePath);
            status = StatusUdfModuleLoadFailed;
            goto CommonExit;
        }
    } else {
        // use source if it's provided
        xSyslog(ModuleName, XlogDebug, "using source");

        compiled = Py_CompileString(source, uniqueName, Py_file_input);
        BailIfNullWith(compiled, StatusUdfModuleLoadFailed);
        xSyslog(ModuleName, XlogDebug, "compiled source");

        modNameObj = PyUnicode_FromString(uniqueName);
        BailIfNull(modNameObj);

        module =
            PyImport_ExecCodeModuleObject(modNameObj, compiled, NULL, NULL);
        BailIfNullWith(module, StatusUdfModuleLoadFailed);

        xSyslog(ModuleName, XlogDebug, "loaded module from source");
    }

    dict = PyObject_GetAttrString(module, "__dict__");
    BailIfNullWith(dict, StatusUdfModuleLoadFailed);

    // Iterate through list of python functions found in this module.
    PyObject *key;
    PyObject *value;
    dictPos = 0;
    functionPassCount = 0;
    functionFailed = 0;
    while (PyDict_Next(dict, &dictPos, &key, &value)) {
        if (PyFunction_Check(value)) {
            PyObject *strName = PyObject_Str(key);
            BailIfNullWith(strName, StatusUdfFunctionLoadFailed);

            char *name = PyUnicode_AsUTF8(strName);
            if (name == NULL) {
                assert(false);
                status = StatusUdfFunctionLoadFailed;
                goto CommonExit;
            }

            if (name[0] == '_' && name[1] == '_') {
                // Skip functions prefixed with __.
                Py_DECREF(strName);
                continue;
            }
            Py_DECREF(strName);

            // value refers to a function.
            UdfPyFunction *function = new (std::nothrow) UdfPyFunction(this);
            BailIfNull(function);

            // Unlock python for the UdfFunction modifications
            unlockPython();
            pyLocked = false;

            status = function->init(key, value, moduleName);

            if (status != StatusOk) {
                // XXX: note that getFnName may yield junk if the failure
                // occurs before the name is set in function->init. However,
                // can't do better since the only scenario is that name
                // generation from moduleName, and key fails due to resources -
                // which would occur even outside that func
                strlcpy(fnName, function->getFnName(), sizeof(fnName));
                delete function;
                functionFailed++;
                break;
            }

            // We must not hold the gil lock when we call this function, because
            // it will acquire the functiontable hashtable lock, which must
            // always be acquired first
            status = function->registerFn();
            if (status != StatusOk) {
                strlcpy(fnName, function->getFnName(), sizeof(fnName));
                function->destroy();
                delete function;
                functionFailed++;
                break;
            }
            lockPython();
            pyLocked = true;

            functionPassCount++;
        }
    }

    if (functionPassCount == 0 && functionFailed == 0) {
        status = StatusUdfModuleEmpty;
        goto CommonExit;
    }

    xSyslog(ModuleName,
            XlogInfo,
            "Loaded %u functions (but %u failed) from module %s as %s",
            functionPassCount,
            functionFailed,
            moduleName,
            uniqueName);

    if (functionFailed == 0) {
        status = StatusOk;
    }

CommonExit:
    if (!pyLocked) {
        lockPython();
        pyLocked = true;
    }
    if (moduleNameDup) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    if (modpathStrDup) {
        memFree(modpathStrDup);
        modpathStrDup = NULL;
    }
    if (inModulePathDup) {
        memFree(inModulePathDup);
        inModulePathDup = NULL;
    }
    if (modpathObj != NULL) {
        Py_DECREF(modpathObj);
        modpathObj = NULL;
    }
    if (modpathStrObj != NULL) {
        Py_DECREF(modpathStrObj);
        modpathStrObj = NULL;
    }
    if (status != StatusOk) {
        udfPyErrorCreate(error, strGetWithFnFromStatus(status, fnName));

        // Unlock python across deleteUdfsWithPrefix since function desctruction
        // needs the lock (leads to deadlock otherwise)
        unlockPython();
        pyLocked = false;
        XcalarEval::get()->deleteUdfsWithPrefix(moduleName);
        lockPython();
        pyLocked = true;
    }
    if (compiled != NULL) {
        Py_DECREF(compiled);
        compiled = NULL;
    }
    if (module != NULL) {
        Py_DECREF(module);
    }
    if (modNameObj != NULL) {
        Py_DECREF(modNameObj);
        modNameObj = NULL;
    }
    if (dict != NULL) {
        Py_DECREF(dict);
    }
    unlockPython();
    pyLocked = false;
    return status;
}

// Look for a function called "main" in the given module. Return it.
Status
UdfPythonImpl::parseMainFunction(const char *source,
                                 const char *appName,
                                 UdfPyFunction **mainFnOut,
                                 UdfError *error)
{
    Status status;
    PyObject *compiled = NULL;
    PyObject *moduleObj = NULL;
    PyObject *dict = NULL;
    Py_ssize_t dictPos = 0;
    bool pyLocked = false;
    char fnName[XcalarEvalMaxFnNameLen + 1];

    UdfPyFunction *mainFn = NULL;

    char uniqueName[DummyModuleNameLen + 1];
    generateUniqueName(uniqueName,
                       sizeof(uniqueName),
                       ChildType::App,
                       appName,
                       source);
    const char *uniqueAppName = uniqueName;

    lockPython();
    pyLocked = true;
    compiled = Py_CompileString(source, uniqueName, Py_file_input);
    BailIfNullWith(compiled, StatusUdfModuleLoadFailed);

    // Acts as PyImport_ReloadModule if module already exists.
    moduleObj = PyImport_ExecCodeModule(uniqueName, compiled);
    BailIfNullWith(moduleObj, StatusUdfModuleLoadFailed);

    dict = PyObject_GetAttrString(moduleObj, "__dict__");
    BailIfNullWith(dict, StatusUdfModuleLoadFailed);

    // Iterate through list of python functions found in this module.
    PyObject *key;
    PyObject *value;
    dictPos = 0;
    while (PyDict_Next(dict, &dictPos, &key, &value)) {
        if (PyFunction_Check(value)) {
            PyObject *strName = PyObject_Str(key);
            BailIfNullWith(strName, StatusUdfFunctionLoadFailed);

            char *name = PyUnicode_AsUTF8(strName);
            if (name == NULL) {
                assert(false && "we just created this as a string");
                Py_DECREF(strName);
                status = StatusUdfFunctionLoadFailed;
                goto CommonExit;
            }

            if (strcmp("main", name) != 0) {
                Py_DECREF(strName);
                continue;
            }
            Py_DECREF(strName);

            // value refers to a function.
            UdfPyFunction *function = new (std::nothrow) UdfPyFunction(this);
            BailIfNull(function);

            // Unlock python for the UdfFunction modifications
            unlockPython();
            pyLocked = false;

            status = function->init(key, value, uniqueAppName);
            if (status != StatusOk) {
                // XXX: note that getFnName may yield junk if the failure
                // occurs before the name is set in function->init. However,
                // can't do better since the only scenario is that name
                // generation from moduleName, and key fails due to resources -
                // which would occur even outside that func
                strlcpy(fnName, function->getFnName(), sizeof(fnName));
                delete function;
                goto CommonExit;
            }

            lockPython();
            pyLocked = true;

            mainFn = function;
            break;
        }
    }

    if (mainFn == NULL) {
        status = StatusUdfFunctionNotFound;
        goto CommonExit;
    }

    *mainFnOut = mainFn;

CommonExit:
    if (!pyLocked) {
        lockPython();
        pyLocked = true;
    }
    if (status != StatusOk) {
        udfPyErrorCreate(error, strGetWithFnFromStatus(status, fnName));
    }
    if (compiled != NULL) {
        Py_DECREF(compiled);
    }
    if (moduleObj != NULL) {
        Py_DECREF(moduleObj);
    }
    if (dict != NULL) {
        Py_DECREF(dict);
    }
    unlockPython();
    pyLocked = false;
    return status;
}

// Uncomment following for more debug info in logs for addModule
// #define ADDMOD_DEBUG

Status
UdfPythonImpl::addModule(UdfModuleSrc *input, UdfError *error)
{
    Status status;
    PyObject *sysPathList = NULL;
    PyObject *sharedUDFsModPathObj = NULL;
    PyObject *modPkgDirPathObj = NULL;
    char *moduleName = input->moduleName;
    char *modulePath = input->modulePath;
    char *modpathStrDup = NULL;
    char *modPkgDirPath = NULL;
    char *source = input->sourceSize ? input->source : NULL;
    int ret;
#ifdef ADDMOD_DEBUG
    PyObject *objRep = NULL;
    const char *pth = NULL;
#endif

    // insert shareUDFs path and 'modPkgDirPath' into sys.path
    //
    // Note that if the path to the module is /a/b/c (i.e. full path to module
    // is /a/b/c/m.py), then we must insert /a/b in sys.path (not /a/b/c), since
    // the subsequent import of the module must be qualified by the 'package' or
    // directory in which the module lives (in this case, "c.m" would be
    // imported from /a/b instead of 'm' from /a/b/c) - so as to disambiguate
    // from Python built-in modules which may have the same name as the UDF
    // module name
    //

    if (modulePath) {
        modpathStrDup = strAllocAndCopy(modulePath);
        BailIfNull(modpathStrDup);
        modPkgDirPath = dirname(modpathStrDup);
    }

    XcalarEval::get()->deleteUdfsWithPrefix(moduleName);

    PyGILState_STATE gilState;
    gilState = PyGILState_Ensure();

    // sysPathList is a borrowed reference - so don't need to dec-ref it
    sysPathList = PySys_GetObject((char *) "path");  // get sys.path

    // first insert the shared xc root for sharedUDFs - needed whether
    // importing or compiling from source (latter needs it for import statements
    // in user's UDF code which imports from shared UDFs dir).
    //
    // so "import sharedUDFs.<sharedModuleName>" statements
    // can work in user-written UDFs
    //
    // sharedUDFsModPathObj reference is now owned by us - so release it at end
    sharedUDFsModPathObj =
        PyUnicode_FromString(XcalarConfig::get()->xcalarRootCompletePath_);
    BailIfNull(sharedUDFsModPathObj);

    // PyList_Insert doesn't steal the reference so sharedUDFsModPathObj must
    // still be dec-ref'ed by us at end - so leave it non-NULL
    ret = PyList_Insert(sysPathList, 0, sharedUDFsModPathObj);
    if (ret != 0) {
        status = StatusUDFBadPath;
        goto CommonExit;
    }

    // next insert the current module's package dir path into the sys.path
    // modPkgDirPathObj reference is now owned by us - so release it at end
    modPkgDirPathObj = PyUnicode_FromString(modPkgDirPath);
    BailIfNull(modPkgDirPathObj);
    // PyList_Insert doesn't steal so modPkgDirPathObj must still be dec-refed
    // at end
    ret = PyList_Insert(sysPathList, 0, modPkgDirPathObj);
    if (ret != 0) {
        status = StatusUDFBadPath;
        goto CommonExit;
    }
    ret = PySys_SetObject("path", sysPathList);
    if (ret != 0) {
        status = StatusUDFBadPath;
        goto CommonExit;
    }

#ifdef ADDMOD_DEBUG
    // this print code is just for debug - delete in productization
    // ok to over-write sysPathList since it was a borrowed reference
    sysPathList = PySys_GetObject((char *) "path");  // get new sys.path
    objRep = PyObject_Repr(sysPathList);
    // objRep has new reference !! must dec-ref it before leaving this block

    if (objRep) {
        xSyslog(ModuleName, XlogErr, "objRep 2 is non-NULL");
        pth = PyUnicode_AsUTF8(objRep);
        xSyslog(ModuleName, XlogErr, "sys.path 2 is %s", pth);
        Py_DECREF(objRep);  // do it here - this code's being deleted eventually
        objRep = NULL;
    } else {
        xSyslog(ModuleName, XlogErr, "objRep 2 is NULL");
    }
#endif

    PyGILState_Release(gilState);

    status = parseModule(moduleName, modulePath, source, error);

    gilState = PyGILState_Ensure();
    sysPathList = PySys_GetObject((char *) "path");
    // Assume first two elements of sysPathList are the ones inserted earlier
    // and so can be deleted via the set-slice call below. The call to
    // parseModule is supposed to leave the sysPath alone - it just imports
    // or compiles the python module and doesn't execute any python code...so
    // sys.path couldn't have changed
    ret = PyList_SetSlice(sysPathList, 0, 2, NULL);
    if (ret != 0) {
        status = StatusUDFBadPath;
        goto CommonExit;
    }
    ret = PySys_SetObject("path", sysPathList);
    if (ret != 0) {
        status = StatusUDFBadPath;
        goto CommonExit;
    }

        // Below is a 'safer' method since it scans the sys.path, and deletes
        // only those items from the list which we must instead of assuming the
        // items to be deleted from the path are the first two items as the
        // above code assumes. The following though assumes that both paths
        // appear at least once in sys.path (hence the subtraction of 2 from
        // list size). This seems overkill though given the invariants...
        //
        // sysPathListNew = PyList_New(PyList_Size(sysPathList) - 2);
        // int jj = 0;
        // for (int ii = 0; ii < PyList_Size(sysPathList); ii++) {
        //     PyObject *thisPath = PyList_GetItem(sysPathList, ii);
        //     const char *pathStr = PyUnicode_AsUTF8(thisPath);
        //     if ((strcmp(pathStr,
        //                XcalarConfig::get()->xcalarRootCompletePath_) == 0) ||
        //         (strcmp(pathStr, modulePath) == 0)) {
        //         continue;
        //     } else {
        //         assert(jj < (PyList_Size(sysPathList) - 2));
        //         PyList_SET_ITEM(sysPathListNew, jj, thisPath);
        //         jj++;
        //     }
        //  }
        //  ret = PySys_SetObject("path", sysPathListNew);
        //  if (ret != 0) {
        //      status = StatusUDFBadPath;
        //      goto CommonExit;
        //  }
        //  // XXX: Py_DECREF(sysPathListNew) must be done in CommonExit: block
        //

#ifdef ADDMOD_DEBUG
    // this print code is just for debug - delete in productization
    // ok to over-write sysPathList since it was a borrowed reference
    sysPathList = PySys_GetObject((char *) "path");
    objRep = PyObject_Repr(sysPathList);
    // objRep has new reference !! must dec-ref it before leaving this block

    if (objRep) {
        xSyslog(ModuleName, XlogErr, "objRep 3 is non-NULL");
        pth = PyUnicode_AsUTF8(objRep);  // pth doesn't need to be freed
        xSyslog(ModuleName, XlogErr, "sys.path 3 is %s", pth);
        Py_DECREF(objRep);  // do it here - this code's being deleted eventually
        objRep = NULL;
    } else {
        xSyslog(ModuleName, XlogErr, "objRep 3 is NULL");
    }
#endif

    PyGILState_Release(gilState);

CommonExit:
    if (modpathStrDup) {
        memFree(modpathStrDup);
        modpathStrDup = NULL;
    }
    if (sharedUDFsModPathObj) {
        Py_DECREF(sharedUDFsModPathObj);
        sharedUDFsModPathObj = NULL;
    }
    if (modPkgDirPathObj) {
        Py_DECREF(modPkgDirPathObj);
        modPkgDirPathObj = NULL;
    }
    return status;
}

Status
UdfPythonImpl::updateModule(const char *moduleName,
                            const char *source,
                            UdfError *error)
{
    // We don't validate that this module already exists because libudf
    // will do that for us.
    XcalarEval::get()->deleteUdfsWithPrefix(moduleName);
    return parseModule(moduleName, NULL, source, error);
}

void
UdfPythonImpl::removeModule(const char *moduleName)
{
    XcalarEval::get()->deleteUdfsWithPrefix(moduleName);
}

Status
UdfPythonImpl::listModule(const char *moduleName, XcalarApiOutput **output)
{
    size_t moduleNameLen = strlen(moduleName);
    char moduleNamePattern[moduleNameLen + 3];
    verify((size_t) snprintf(moduleNamePattern,
                             moduleNameLen + 3,
                             "%s:*",
                             moduleName) < moduleNameLen + 3);

    size_t outputSize;
    return XcalarEval::get()->getFnList(moduleNamePattern,
                                        "*",
                                        NULL,
                                        output,
                                        &outputSize);
}

////////////////////////////////////////////////////////////////////////////////

// Looks up desired function in XcalarEval and passes off to
// UdfPyFunction::execOnLoad.
Status
UdfPythonImpl::execOnLoad(const char *fqFunctionName,
                          const uint8_t *dataset,
                          size_t datasetSize,
                          const char *fullPath,
                          UdfError *error,
                          PyObject **pyReturn)
{
    LibNsTypes::NsHandle nsHandle;
    XcalarEvalRegisteredFn *fn =
        XcalarEval::get()->getOrResolveRegisteredFn(fqFunctionName,
                                                    NULL,
                                                    &nsHandle);
    if (fn == NULL) {
        return StatusUdfNotFound;
    }

    UdfPyFunction *udfPyFunction = (UdfPyFunction *) fn->fnDesc.context;
    Status status = udfPyFunction->execOnLoad(dataset,
                                              datasetSize,
                                              fullPath,
                                              error,
                                              pyReturn);
    XcalarEval::get()->putFunction(fn, &nsHandle);

    return status;
}

////////////////////////////////////////////////////////////////////////////////

//
// Use fat lock instead of relying on Python's internal locking. This helps
// Python modules with threading issues (see strptime thread safety) and shows
// no perf degradation.
//

void
UdfPythonImpl::lockPython()
{
    this->lock.lock();
    gilState = PyGILState_Ensure();
}

void
UdfPythonImpl::unlockPython()
{
    PyGILState_Release(gilState);
    this->lock.unlock();
}
