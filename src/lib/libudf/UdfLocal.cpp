// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <pthread.h>
#include "StrlFunc.h"
#include "UdfLocal.h"
#include "udf/UserDefinedFunction.h"
#include "UdfPersist.h"
#include "UdfOperation.h"
#include "UdfParent.h"
#include "sys/XLog.h"
#include "operators/XcalarEval.h"
#include "strings/String.h"
#include "stat/Statistics.h"
#include "util/StringHashTable.h"
#include "ParentChild.h"
#include "ns/LibNs.h"

UdfLocal::UdfLocal()
    : udfParent_(NULL),
      udfOperation_(NULL),
      statGroupId_(0),
      statModuleCount_(NULL)
{
}

UdfLocal::~UdfLocal() {}

Status
UdfLocal::init(UdfOperation *udfOperation, UdfParent *udfParent)
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(ModuleName, &statGroupId_, 1);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&statModuleCount_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(statGroupId_,
                                         "TotalUDFModules",
                                         statModuleCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    udfOperation_ = udfOperation;
    udfParent_ = udfParent;

CommonExit:
    return status;
}

void
UdfLocal::destroy()
{
    modulesHTLock_.lock();
    modules_.removeAll(&UdfModule::del);
    modulesHTLock_.unlock();

    // XXX Cleanup stats.
}

//
// Helper functions for add, update and stage.
//

// Validation common to add, update, and stage.
Status
UdfLocal::validateUdf(const char *moduleName,
                      const char *source,
                      size_t sourceSize,
                      UdfType type)
{
    Status status = StatusUnknown;
    char *moduleNameDup = NULL;
    const char *modNameBase = NULL;

    if (type != UdfTypePython) {
        status = StatusUdfModuleInvalidType;
        xSyslog(ModuleName,
                XlogErr,
                "Validate Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (sourceSize != strlen(source) + 1) {
        status = StatusUdfModuleInvalidSource;
        xSyslog(ModuleName,
                XlogErr,
                "Validate Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (sourceSize > (XcalarApiMaxUdfSourceLen + 1)) {
        status = StatusUdfModuleSourceTooLarge;
        // XXX In the future, it probably makes sense for this to be
        // type-specific.
        xSyslog(ModuleName,
                XlogErr,
                "Validate Scalar Function %s failed: %s len: %lu",
                moduleName,
                strGetFromStatus(status),
                sourceSize);
        goto CommonExit;
    }

    if (strlen(moduleName) == 0) {
        status = StatusUdfModuleInvalidName;
        xSyslog(ModuleName,
                XlogErr,
                "Validate Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (strchr(moduleName, '/') != NULL) {
        // path name to a module, not really a moduleName
        moduleNameDup = strAllocAndCopy(moduleName);
        BailIfNull(moduleNameDup);
        modNameBase = basename(moduleNameDup);
    } else {
        modNameBase = moduleName;
    }

    if (strlen(modNameBase) > XcalarApiMaxUdfModuleNameLen) {
        status = StatusUdfModuleInvalidName;
        xSyslog(ModuleName,
                XlogErr,
                "Validate Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (size_t ii = 0; modNameBase[ii] != '\0'; ii++) {
        if (!isalnum(modNameBase[ii]) && !isalpha(modNameBase[ii]) &&
            modNameBase[ii] != '-' && modNameBase[ii] != '_' &&
            modNameBase[ii] != UserDefinedFunction::UdfRetinaPrefixDelim[0]) {
            status = StatusUdfModuleInvalidName;
            xSyslog(ModuleName,
                    XlogErr,
                    "Validate Scalar Function %s failed: %s",
                    moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    status = StatusOk;

CommonExit:
    if (moduleNameDup) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    return status;
}

// Adds this modules functions to eval module. Caller must exclusively hold the
// module's lock.
Status
UdfLocal::populateEval(UdfLocal::UdfModule *module,
                       XcalarEvalFnDesc *functions,
                       size_t functionCount)
{
    Status status = StatusOk;

    for (size_t ii = 0; ii < functionCount; ii++) {
        char fqFnName[UdfVersionedFQFname];
        const char *moduleName;
        const char *fnName;
        const char *moduleVersion;
        int ret;

        ret =
            snprintf(fqFnName, UdfVersionedFQFname, "%s", functions[ii].fnName);
        if (ret >= (int) UdfVersionedFQFname) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Populate Eval %s fn %s failed: %s",
                    module->moduleFullNameVersioned,
                    functions[ii].fnName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = UserDefinedFunction::parseFunctionName(fqFnName,
                                                        &moduleName,
                                                        &moduleVersion,
                                                        &fnName);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Populate Eval parseFunctionName %s fn %s failed: %s",
                    module->moduleFullNameVersioned,
                    functions[ii].fnName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        ret = snprintf(functions[ii].fnName,
                       sizeof(functions[ii].fnName),
                       "%s:%s",
                       module->moduleFullNameVersioned,
                       fnName);
        if (ret >= (int) sizeof(functions[ii].fnName)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Populate Eval %s fn %s failed: %s",
                    module->moduleFullNameVersioned,
                    functions[ii].fnName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = XcalarEval::get()->addUdf(&functions[ii], NULL, NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Populate Eval add Scalar Function %s fn %s failed: %s",
                    module->moduleFullNameVersioned,
                    fqFnName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (status != StatusOk) {
        XcalarEval::get()->deleteUdfsWithPrefix(
            module->moduleFullNameVersioned);
    }
    return status;
}

void
UdfLocal::logError(UdfError *error)
{
    if (error->message_) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function error: %s",
                error->message_);
    }
}

// Functions that add new modules into the system whether for validation
// purposes (stage) or permanently (add).
void
UdfLocal::UdfModule::del()
{
    if (this->input != NULL) {
        memFree(this->input);
    }
    memFree(this);
}

// Local entry point for adding UDF module. Originating node has asked for
// this module to be added to the system.
Status
UdfLocal::addUdf(UdfModuleSrc *input,
                 char *moduleAbsPath,
                 uint64_t consistentVersion)
{
    Status status = StatusOk;
    bool lockHeld = false;
    bool popEval = false;
    // Our local copy of this input.
    UdfModuleSrc *copy = NULL;
    UdfLocal::UdfModule *module = NULL;
    size_t copySize = 0;
    XcalarEvalFnDesc *functions = NULL;
    size_t functionCount = 0;
    // XXX Propagate to originating node.
    UdfError error;
    char moduleFullNameVersioned[LibNsTypes::MaxPathNameLen + 1];
    int ret;

    // Generate the fully qualified and consistent versioned module name
    if (moduleAbsPath == NULL || strlen(moduleAbsPath) == 0) {
        ret = snprintf(moduleFullNameVersioned,
                       sizeof(moduleFullNameVersioned),
                       "%s%s%lu",
                       input->moduleName,
                       UserDefinedFunction::UdfVersionDelim,
                       consistentVersion);
        if (ret < 0 || ret >= (int) sizeof(moduleFullNameVersioned)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed: %s",
                    input->moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        char *modName;
        if (input->moduleName[0] == '/') {
            modName = &input->moduleName[1];
        } else {
            modName = input->moduleName;
        }
        ret = snprintf(moduleFullNameVersioned,
                       sizeof(moduleFullNameVersioned),
                       "%s/%s%s%lu",
                       moduleAbsPath,
                       modName,
                       UserDefinedFunction::UdfVersionDelim,
                       consistentVersion);
        if (ret < 0 || ret >= (int) sizeof(moduleFullNameVersioned)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s/%s failed: %s",
                    moduleAbsPath,
                    modName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Validation
    // Is this needed? The node doing the gvm broadcast already validated it...
    status = validateUdf(input->moduleName,
                         input->source,
                         input->sourceSize,
                         input->type);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s validate failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    modulesHTLock_.lock();
    lockHeld = true;
    if (modules_.find(moduleFullNameVersioned) != NULL) {
        status = StatusUdfModuleAlreadyExists;
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s lookup failed: %s",
                moduleFullNameVersioned,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Add to system.
    // Allocate before doing anything irreversible.
    copySize = udfModuleSrcSize(input);
    copy = (UdfModuleSrc *) memAlloc(copySize);
    if (copy == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s failed: %s",
                moduleFullNameVersioned,
                strGetFromStatus(status));
        goto CommonExit;
    }
    memcpy(copy, input, copySize);

    module = (UdfLocal::UdfModule *) memAlloc(sizeof(*module));
    if (module == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s failed: %s",
                moduleFullNameVersioned,
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(module, sizeof(*module));
    module->input = copy;
    memcpy(&module->moduleFullNameVersioned,
           moduleFullNameVersioned,
           sizeof(moduleFullNameVersioned));

    status = udfParent_->stageAndListFunctions(moduleFullNameVersioned,
                                               module->input,
                                               &error,
                                               &functions,
                                               &functionCount);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s failed, child rejected the functions: "
                "%s",
                moduleFullNameVersioned,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = populateEval(module, functions, functionCount);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s failed, child rejected the functions: "
                "%s",
                moduleFullNameVersioned,
                strGetFromStatus(status));
        goto CommonExit;
    }
    popEval = true;

    modules_.insert(module);
    StatsLib::statAtomicIncr32(statModuleCount_);

CommonExit:
    if (lockHeld == true) {
        modulesHTLock_.unlock();
    }
    if (functions != NULL) {
        memFree(functions);
        functions = NULL;
    }
    if (status != StatusOk) {
        logError(&error);
        if (popEval == true) {
            XcalarEval::get()->deleteUdfsWithPrefix(
                module->moduleFullNameVersioned);
        }
        if (copy != NULL) {
            memFree(copy);
            copy = NULL;
        }
        if (module != NULL) {
            memFree(module);
            module = NULL;
        }
    }
    return status;
}

// Functions for updating an existing UDF module.
// Update source associated with a module. Module must already exist.
Status
UdfLocal::updateUdf(UdfModuleSrc *input,
                    char *moduleAbsPath,
                    uint64_t consistentVersion,
                    uint64_t nextVersion)
{
    Status status;
    XcalarApiUdfDeleteInput delInput;

    snprintf(delInput.moduleName,
             sizeof(delInput.moduleName),
             "%s",
             input->moduleName);

    // Here is an opportunity to garbage collect inconsistent versions.
    status =
        deleteUdf(&delInput, moduleAbsPath, consistentVersion + 1, nextVersion);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed on deletion while "
                "attempting to cleanout: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = addUdf(input, moduleAbsPath, nextVersion);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed to add function: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return status;
}

// See comments in UserDefinedFunction::addUdfInternal and
// UserDefinedFunction::nwClientFlushPath for overall context. This is the
// local routine which does the actual work of flushing the NFS client cache
// for the local node. Also, see https://github.com/andrep/nfsflush for
// general NFS client cache flush background (we emulate this strategy here)
//
// NOTE: payload in "input" is modified;
// this is to avoid alloc'ing new memory in this function which could fail
// (all failures MUST be eliminated here - this routine must not fail)
//

Status
UdfLocal::flushNwCacheUdf(UdfNwFlushPath *input)
{
    char *udfOdFlushPath = NULL;
    char *udfOdPathUser = NULL;
    char *udfOdPyPath = NULL;
    LogLib *logLib = LogLib::get();
    Status status = StatusOk;
    DIR *dir;
    int ret;

    udfOdFlushPath = input->path;
    xSyslog(ModuleName,
            XlogDebug,
            "flushing Scalar Function od path full '%s' for '%s'",
            udfOdFlushPath,
            input->isCreate ? "create Scalar Function"
                            : "update Scalar Function");

    if (input->isCreate) {
        if (strstr(udfOdFlushPath, UserDefinedFunction::SharedUDFsDirPath) ==
            NULL) {
            // udfOdFlushPath = <Root>/workbooks/<user>/<sid>/udfs/python
            udfOdPathUser = udfOdFlushPath + strlen(logLib->wkbkPath_) + 1;
            // udfOdPathUser = <user>/<sid>/udfs/python
            *udfOdPathUser = '\0';  // chop off all after <Root>/workbooks/
            // udfOdFlushPath =  <Root>/workbooks/
            xSyslog(ModuleName,
                    XlogDebug,
                    "flushing chopped Scalar Function od path '%s'",
                    udfOdFlushPath);
        } else {
            // udfOdFlushPath = <Root>/sharedUDFs/python
            udfOdPyPath = udfOdFlushPath + strlen(logLib->sharedUDFsPath_);
            // udfOdPyPath = /python
            *udfOdPyPath =
                '\0';  // chop-off everything after the / before python
            // udfOdFlushPath =  <Root>/sharedUDFs
            xSyslog(ModuleName,
                    XlogDebug,
                    "flushing chopped Scalar Function od path '%s'",
                    udfOdFlushPath);
        }
    }

    dir = opendir(udfOdFlushPath);
    // This dir was created/updated by a prior successful call to commit the UDF
    // (see block comment in call to nwClientFlushPath in
    // UserDefinedFunction::addUdfInternal().
    //
    // So opendir/closedir can't really fail. But just in case it does in
    // non-DEBUG, emit a log and move on - in which case the higher-level UDF
    // operation should fail.

    assert(dir != NULL);
    if (dir == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogCrit,
                "flushNwCacheUdf: open of Scalar Function od path '%s' failed: "
                "'%s'",
                udfOdFlushPath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    ret = closedir(dir);
    assert(ret != -1);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogCrit,
                "flushNwCacheUdf: close of Scalar Function od path '%s' "
                "failed: '%s'",
                udfOdFlushPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return (status);
}

//
// Other UDF functions. Use list XDFs to get a list of UDFs.
//

// Get a UDF. Reads from only a single usrnode.
Status
UdfLocal::getUdf(XcalarApiUdfGetInput *input,
                 char *moduleAbsPath,
                 uint64_t version,
                 XcalarApiOutput **output,
                 size_t *outputSize)
{
    Status status;
    XcalarApiOutput *outputTmp = NULL;
    UdfModuleSrc *getOutput = NULL;
    size_t outputSizeTmp;
    UdfLocal::UdfModule *module;
    char moduleAbsPathVersioned[LibNsTypes::MaxPathNameLen + 1];
    int ret;

    ret = snprintf(moduleAbsPathVersioned,
                   sizeof(moduleAbsPathVersioned),
                   "%s%s%lu",
                   moduleAbsPath,
                   UserDefinedFunction::UdfVersionDelim,
                   version);
    if (ret >= (int) sizeof(moduleAbsPathVersioned)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Get Scalar Function %s failed: %s",
                moduleAbsPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    modulesHTLock_.lock();
    module = modules_.find(moduleAbsPathVersioned);
    if (module == NULL) {
        modulesHTLock_.unlock();
        status = StatusUdfModuleNotFound;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Get Scalar Function %s failed: %s",
                      input->moduleName,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    modulesHTLock_.unlock();

    outputSizeTmp =
        XcalarApiSizeOfOutput(typeof(*getOutput)) + module->input->sourceSize;

    outputTmp = (XcalarApiOutput *) memAlloc(outputSizeTmp);
    if (outputTmp == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Get Scalar Function %s failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    memcpy(&outputTmp->outputResult.udfGetOutput,
           module->input,
           udfModuleSrcSize(module->input));

    *outputSize = outputSizeTmp;
    *output = outputTmp;
    outputTmp = NULL;

    status = StatusOk;

CommonExit:
    return status;
}

// Removes from usrnode. If in use in any childnodes, it will stick around
// until current operation is complete. Lazily removed from childnodes by
// frequent recycling.
void
UdfLocal::deleteUdfInternal(UdfLocal::UdfModule *module)
{
    XcalarEval::get()->deleteUdfsWithPrefix(module->moduleFullNameVersioned);
    module->del();

    assert(StatsLib::statReadUint64(statModuleCount_) > 0);
    StatsLib::statAtomicDecr32(statModuleCount_);
}

// Remove a UDF from the system. Delete may be implemented differently depending
// on UDF type. Accepts * wildcard to delete multiple UDFs.
Status
UdfLocal::deleteUdf(XcalarApiUdfDeleteInput *input,
                    char *moduleAbsPath,
                    uint64_t consistentVersion,
                    uint64_t nextVersion)
{
    Status status = StatusOk;
    int ret = 0;

    // Clean out all the versions of UDF module here.
    for (uint64_t ii = consistentVersion; ii < nextVersion; ii++) {
        char moduleNameTmp[UdfVersionedModuleName + 1];
        ret = snprintf(moduleNameTmp,
                       sizeof(moduleNameTmp),
                       "%s%s%s%s%lu",
                       moduleAbsPath,
                       input->moduleName[0] == '/' ? "" : "/",
                       input->moduleName,
                       UserDefinedFunction::UdfVersionDelim,
                       ii);
        if (ret < 0 || ret >= (int) sizeof(moduleNameTmp)) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Delete Scalar Function %s failed: %s",
                    input->moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        status = deleteUdfByName(moduleNameTmp);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Delete Scalar Function %s failed: %s",
                    moduleNameTmp,
                    strGetFromStatus(status));
            // Not a hard error.
            status = StatusOk;
        }
    }

CommonExit:
    return status;
}

// NOTE: caller must ensure that moduleName is the module's absolute name
Status
UdfLocal::deleteUdfByName(char *moduleName)
{
    Status status;
    modulesHTLock_.lock();
    UdfLocal::UdfModule *module = modules_.find(moduleName);
    if (module != NULL) {
        verify(modules_.remove(moduleName) != NULL);
    }
    modulesHTLock_.unlock();

    if (module != NULL) {
        deleteUdfInternal(module);
        status = StatusOk;
    } else {
        status = StatusUdfNotFound;
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
    }
    return status;
}

//
// Functions to load needed modules into a particular child.
//
Status
UdfLocal::loadModule(ParentChild *child, const char *moduleName)
{
    Status status = StatusOk;
    UdfLocal::UdfModule *module;
    UdfError error;
    memZero(&error, sizeof(error));
    UdfModuleSrc *input = NULL;
    size_t copySize = 0;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    int ret;
    char moduleNameVersioned[UdfVersionedModuleName + 1];
    LibNs *libNs = LibNs::get();
    UserDefinedFunction::UdfRecord *udfRecord = NULL;
    char *udfOnDiskPath = NULL;

    // XXX: udfContainer is NULL here / this is ok as long as moduleName is
    // an absolute name. loadModule should be passed the container if not.
    status = UserDefinedFunction::get()->getUdfName(fullyQualName,
                                                    sizeof(fullyQualName),
                                                    (char *) moduleName,
                                                    NULL,
                                                    false);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Load module (%s, fullName %s) child wth PID %u failed: %s",
                moduleName,
                fullyQualName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = UserDefinedFunction::get()->getUdfOnDiskPath(fullyQualName,
                                                          &udfOnDiskPath);
    BailIfFailed(status);

    // Get Read only access to the UDF module.
    handle = libNs->open(fullyQualName,
                         LibNsTypes::ReaderShared,
                         (NsObject **) &udfRecord,
                         &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound) {
            status = StatusUdfModuleNotFound;
        } else if (status == StatusAccess || status == StatusPendingRemoval) {
            status = StatusUdfModuleInUse;
        } else if (status == StatusNsInvalidObjName) {
            status = StatusUdfModuleInvalidName;
        }
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load module (%s, fullName %s) child wth PID %u failed "
                      "on NS "
                      "open:%s",
                      moduleName,
                      fullyQualName,
                      child->getPid(),
                      strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // Generate the consistent versioned module name
    ret = snprintf(moduleNameVersioned,
                   sizeof(moduleNameVersioned),
                   "%s%s%lu",
                   moduleName,
                   UserDefinedFunction::UdfVersionDelim,
                   udfRecord->consistentVersion_);
    if (ret >= (int) sizeof(moduleNameVersioned)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Load module %s child wth PID %u failed: %s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    modulesHTLock_.lock();
    module = modules_.find(moduleNameVersioned);
    if (module == NULL) {
        modulesHTLock_.unlock();
        status = StatusUdfModuleNotFound;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load module %s child wth PID %u failed: %s",
                      moduleName,
                      child->getPid(),
                      strGetFromStatus(status));
        goto CommonExit;
    }

    if (udfOnDiskPath != NULL) {
        // NOTE: NOTE: if there's an on-disk path available to the UDF module,
        // then the module can be imported (instead of compiling from source) -
        // in this case the source code for the module isn't necessary to be
        // sent - so calculate the copySize to exclude the size of source code
        // by NOT using udfModuleSrcSize()
        copySize = sizeof(*(module->input));
        // no source-code copied in memcpy below (copySize doesn't include it)
    } else {
        copySize = udfModuleSrcSize(module->input);
    }
    input = (UdfModuleSrc *) memAllocExt(copySize, ModuleName);
    if (input == NULL) {
        modulesHTLock_.unlock();
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Load module %s child with PID %u failed: %s",
                moduleName,
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }
    memcpy(input, module->input, copySize);
    modulesHTLock_.unlock();

    if (udfOnDiskPath != NULL) {
        // set sourceSize to 0 since there's no source! And send down the
        // on-disk path to the UDF module so child can import it (instead of
        // compiling src)
        input->sourceSize = 0;
        status = strStrlcpy(input->modulePath,
                            udfOnDiskPath,
                            sizeof(input->modulePath));
        BailIfFailed(status);
    } else {
        input->modulePath[0] = '\0';
    }

    status = udfParent_->addOrUpdate(child, (char *) moduleName, input, &error);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Load module %s child with PID %u failed on parent"
                " addOrUpdate: %s error (%s)",
                moduleName,
                child->getPid(),
                strGetFromStatus(status),
                error.message_);
        goto CommonExit;
    }

CommonExit:
    if (input != NULL) {
        memFree(input);
        input = NULL;
    }
    if (udfOnDiskPath != NULL) {
        memFree(udfOnDiskPath);
        udfOnDiskPath = NULL;
    }
    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Load module %s failed on NS close: %s",
                    moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (udfRecord != NULL) {
        memFree(udfRecord);
        udfRecord = NULL;
    }

    if (error.message_ != NULL) {
        memFree((char *) error.message_);
        error.message_ = NULL;
    }

    return status;
}

Status
UdfLocal::loadModules(ParentChild *child, EvalUdfModuleSet *modules)
{
    Status status = StatusOk;

    EvalUdfModuleSet::iterator it(*modules);

    for (; it.get() != NULL; it.next()) {
        status = loadModule(child, it.get()->getName());
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Load modules %s to child with PID %u failed: %s",
                    it.get()->getName(),
                    child->getPid(),
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Transfer Scalar Function modules to child with PID %u "
                "failed:%s",
                child->getPid(),
                strGetFromStatus(status));
    }
    return status;
}

Status
UdfLocal::getModuleNameVersioned(char *moduleName,
                                 uint64_t version,
                                 char *fnName,
                                 char **retModuleNameVersioned)
{
    char *fqFnNameVersioned = NULL;
    int ret;
    *retModuleNameVersioned = NULL;
    Status status;

    fqFnNameVersioned = (char *) memAlloc(UdfVersionedFQFname);
    if (fqFnNameVersioned == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Udf Local %s getModuleNameVersioned failed:%s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = snprintf(fqFnNameVersioned,
                   UdfVersionedFQFname,
                   "%s%s%lu:%s",
                   moduleName,
                   UserDefinedFunction::UdfVersionDelim,
                   version,
                   fnName);
    if (ret >= (int) UdfVersionedFQFname) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Udf Local %s getModuleNameVersioned failed:%s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    *retModuleNameVersioned = fqFnNameVersioned;
    fqFnNameVersioned = NULL;
    status = StatusOk;

CommonExit:
    if (fqFnNameVersioned != NULL) {
        memFree(fqFnNameVersioned);
    }
    return status;
}
