// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>
#include <string.h>

#include "StrlFunc.h"
#include "udf/UserDefinedFunction.h"
#include "UdfLocal.h"
#include "UdfOperation.h"
#include "UdfPersist.h"
#include "UdfParent.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "operators/Xdf.h"
#include "df/DataFormat.h"
#include "strings/String.h"
#include "util/FileUtils.h"
#include "LibUdfConstants.h"
#include "gvm/Gvm.h"
#include "ns/LibNs.h"
#include "usr/Users.h"
#include "msg/TwoPcFuncDefsClient.h"

static constexpr const char *ModuleName = "libudf";

using namespace udf;

UserDefinedFunction *UserDefinedFunction::instance = NULL;

UserDefinedFunction::UserDefinedFunction()
    : udfLocal_(NULL), udfPersist_(NULL), udfOperation_(NULL), udfParent_(NULL)
{
}

UserDefinedFunction::~UserDefinedFunction()
{
    assert(udfLocal_ == NULL);
    assert(udfPersist_ == NULL);
    assert(udfOperation_ == NULL);
    assert(udfParent_ == NULL);
}

Status
UserDefinedFunction::init(bool skipRestore)
{
    Status status = StatusUnknown;
    int ii;

    instance = new (std::nothrow) UserDefinedFunction;
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed: %s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        return status;
    }

    // The UdfPath determines the search order for UDF modulename resolution
    // (similar to sys.path of Python). Currently this is always set to:
    // [UdfCurrentDirPath, SharedUDFsDirPath] and is not modifiable by the user.
    //
    // Eventually, this will be stored as a modifiable value in each workbook,
    // or even possibly a dataflow, or a node in a dataflow. For now, all
    // execution scenarios will always use this UdfPath.
    //
    // This means that the module-name will be searched first in
    // UdfCurrentDirPath, (the current session in which query graph referencing
    // the UDF executes) and next in SharedUDFsDirPath, which is the shared UDFs
    // directory in the UDF namespace.

    for (ii = 0; ii < UdfPaths::Npaths; ii++) {
        instance->UdfPath[ii] = NULL;
        switch (ii) {
        case UdfPaths::CurDir:
            instance->UdfPath[ii] =
                (char *) memAllocExt(strlen(UdfCurrentDirPath) + 1, ModuleName);
            BailIfNull(instance->UdfPath[ii]);
            verify(snprintf(instance->UdfPath[ii],
                            strlen(UdfCurrentDirPath) + 1,
                            "%s",
                            UdfCurrentDirPath) <
                   (int) strlen(UdfCurrentDirPath) + 1);

            break;
        case UdfPaths::Shared:
            instance->UdfPath[ii] =
                (char *) memAllocExt(strlen(SharedUDFsDirPath) + 1, ModuleName);
            BailIfNull(instance->UdfPath[ii]);

            verify(snprintf(instance->UdfPath[ii],
                            strlen(SharedUDFsDirPath) + 1,
                            "%s",
                            SharedUDFsDirPath) <
                   (int) strlen(SharedUDFsDirPath) + 1);
            break;
        default:
            status = StatusUDFBadPath;
            xSyslog(ModuleName,
                    XlogErr,
                    "Init skipRestore '%s' failed: %s",
                    (skipRestore == true) ? "true" : "false",
                    strGetFromStatus(status));
            goto CommonExit;
            break;
        }
    }

    instance->udfLocal_ = new (std::nothrow) UdfLocal;
    if (instance->udfLocal_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed: %s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->udfPersist_ = new (std::nothrow) UdfPersist;
    if (instance->udfPersist_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed: %s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->udfOperation_ = new (std::nothrow)
        UdfOperation(instance->udfLocal_, instance->udfPersist_);
    if (instance->udfOperation_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed: %s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance->udfParent_ = new (std::nothrow) UdfParent(instance->udfLocal_);
    if (instance->udfParent_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed: %s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = instance->udfLocal_->init(instance->udfOperation_,
                                       instance->udfParent_);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed on Scalar Function local init: "
                "%s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = instance->udfPersist_->init(skipRestore);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Init skipRestore '%s' failed on Scalar Function persist init: "
                "%s",
                (skipRestore == true) ? "true" : "false",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (instance != NULL) {
            instance->destroy();
        }
    }

    return status;
}

void
UserDefinedFunction::destroy()
{
    int ii;

    if (instance == NULL) {
        return;
    }

    for (ii = 0; ii < UdfPaths::Npaths; ii++) {
        if (instance->UdfPath[ii] != NULL) {
            memFree(instance->UdfPath[ii]);
            instance->UdfPath[ii] = NULL;
        }
    }

    if (udfParent_ != NULL) {
        delete udfParent_;
        udfParent_ = NULL;
    }

    if (udfOperation_ != NULL) {
        delete udfOperation_;
        udfOperation_ = NULL;
    }

    if (udfPersist_ != NULL) {
        udfPersist_->destroy();
        delete udfPersist_;
        udfPersist_ = NULL;
    }

    if (udfLocal_ != NULL) {
        udfLocal_->destroy();
        delete udfLocal_;
        udfLocal_ = NULL;
    }

    delete instance;
    instance = NULL;
}

UserDefinedFunction *
UserDefinedFunction::get()
{
    return instance;
}

// Wraps validation and staging of new module.
Status
UserDefinedFunction::stageAndListFunctions(char *moduleNameVersioned,
                                           UdfModuleSrc *input,
                                           UdfError *error,
                                           XcalarEvalFnDesc **functions,
                                           size_t *functionCount)
{
    Status status = udfLocal_->validateUdf(input->moduleName,
                                           input->source,
                                           input->sourceSize,
                                           input->type);
    BailIfFailed(status);

    status = udfParent_->stageAndListFunctions(moduleNameVersioned,
                                               input,
                                               error,
                                               functions,
                                               functionCount);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to load %s Scalar Function module '%s' from path "
                      "'%s'(%s): "
                      "%s",
                      strGetFromUdfType(input->type),
                      input->moduleName,
                      input->modulePath,
                      strGetFromStatus(status),
                      error->message_);
        goto CommonExit;
    }
CommonExit:
    return status;
}

Status
UserDefinedFunction::addDefaultUdfs()
{
    Status status = StatusOk;
    int ret;
    int fd = -1;
    size_t fileSize;
    struct stat fileStat;
    size_t bytesRead;
    UdfModuleSrc *moduleSrc = NULL;
    XcalarApiOutput *apiOutput = NULL;
    size_t outputSize;
    char defaultModulePath[PATH_MAX];

    const char *xlrDir = getenv("XLRDIR");
    if (xlrDir == NULL) {
        xSyslog(ModuleName,
                XlogCrit,
                "%s",
                "XLRDIR environment variable must point to Xcalar install");
        return StatusFailed;
    }

    if (Config::get()->getMyNodeId() != 0) {
        return StatusOk;
    }

    if (snprintf(defaultModulePath,
                 PATH_MAX,
                 "%s/%s",
                 xlrDir,
                 DefaultUDFsPath) >= PATH_MAX) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar Functions open default module '%s/%s' "
                "failed: %s",
                xlrDir,
                DefaultUDFsPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    fd = open(defaultModulePath, O_CLOEXEC | O_RDONLY);
    if (fd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar functions open default module '%s' failed: "
                "%s",
                defaultModulePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = fstat(fd, &fileStat);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar Functions stat module '%s' failed: %s",
                defaultModulePath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    fileSize = fileStat.st_size;

    // Add 1 for the null terminator
    moduleSrc = (UdfModuleSrc *) memAllocExt(sizeof(*moduleSrc) + fileSize + 1,
                                             ModuleName);
    if (moduleSrc == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar Functions '%s' failed: %s",
                defaultModulePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    moduleSrc->type = UdfTypePython;
    ret = strlcpy(moduleSrc->moduleName,
                  DefaultModuleName,
                  sizeof(moduleSrc->moduleName));
    if (ret >= (int) sizeof(moduleSrc->moduleName)) {
        status = StatusNoBufs;
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar Functions '%s' failed: %s",
                defaultModulePath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Attempt to read directly into the modulesrc object
    status =
        FileUtils::convergentRead(fd, moduleSrc->source, fileSize, &bytesRead);
    if (status == StatusOk) {
        assert(bytesRead == fileSize);
    } else if (status == StatusEof) {
        // The file size changed in between our checking and now. Let's still
        // attempt to load it
        // If the file has changed to be -larger- then it will silently
        // miss the rest.
        // The other option would be to assume the results of stat are incorrect
        // and realloc as we go.
        assert(bytesRead <= fileSize);
    } else {
        xSyslog(ModuleName,
                XlogErr,
                "Add default Scalar Functions '%s' read failed: %s",
                defaultModulePath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    moduleSrc->source[bytesRead] = '\0';
    // 1 for the null terminator
    moduleSrc->sourceSize = bytesRead + 1;
    moduleSrc->isBuiltin = true;
    status = this->addUdf(moduleSrc, NULL, &apiOutput, &outputSize);
    if (status != StatusOk) {
        // Include error struct to syslog when there is either a message
        // or a traceback. Otherwise skip as error.data will be pointing
        // to some garbage.
        if (apiOutput &&
            (apiOutput->outputResult.udfAddUpdateOutput.error.messageSize ||
             apiOutput->outputResult.udfAddUpdateOutput.error.tracebackSize)) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Add default Scalar Functions '%s' add Scalar Function "
                    "failed (%s): %s",
                    defaultModulePath,
                    strGetFromStatus(status),
                    apiOutput->outputResult.udfAddUpdateOutput.error.data);
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "Add default Scalar Functions '%s' add Scalar Function "
                    "failed: %s",
                    defaultModulePath,
                    strGetFromStatus(status));
        }
    }

CommonExit:
    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }
    if (moduleSrc) {
        memFree(moduleSrc);
        moduleSrc = NULL;
    }
    if (apiOutput) {
        memFree(apiOutput);
        apiOutput = NULL;
    }
    return status;
}

Status
UserDefinedFunction::deleteDefaultUdfs()
{
    Status status = StatusOk;
    XcalarApiUdfDeleteInput removeInput;
    int ret;

    if (Config::get()->getMyNodeId() != 0) {
        goto CommonExit;
    }

    ret = strlcpy(removeInput.moduleName,
                  DefaultModuleName,
                  sizeof(removeInput.moduleName));
    assert(ret < (int) sizeof(removeInput.moduleName));

    status = deleteUdf(&removeInput, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to delete '%s' Scalar Function: %s",
                DefaultModuleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

// Allocate and populate output for add/update requests.
Status
UserDefinedFunction::addUpdateOutput(const char *moduleName,
                                     Status operationStatus,
                                     const UdfError *error,
                                     XcalarApiOutput **output,
                                     size_t *outputSize)
{
    Status status = StatusOk;
    size_t messageSize = 0;
    size_t tracebackSize = 0;
    if (error->message_ != NULL) {
        messageSize = strlen(error->message_) + 1;
    }

    XcalarApiUdfAddUpdateOutput *addUpdateOutput;
    size_t outputSizeTmp = sizeOfAddUpdateOutput(messageSize, tracebackSize);
    XcalarApiOutput *outputTmp =
        (XcalarApiOutput *) memAllocExt(outputSizeTmp, ModuleName);
    if (outputTmp == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Add update output '%s' failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    addUpdateOutput = &outputTmp->outputResult.udfAddUpdateOutput;
    addUpdateOutput->status = operationStatus.code();
    addUpdateOutput->error.messageSize = messageSize;
    addUpdateOutput->error.tracebackSize = tracebackSize;

    verify(strlcpy(addUpdateOutput->moduleName,
                   moduleName,
                   sizeof(addUpdateOutput->moduleName)) <
           sizeof(addUpdateOutput->moduleName));

    if (messageSize > 0) {
        strlcpy(addUpdateOutput->error.data, error->message_, messageSize);
    }

    *output = outputTmp;
    *outputSize = outputSizeTmp;

CommonExit:
    return status;
}

// Global entry point for add multiple new UDF modules
// Please note that there's no rollback mechansim built into bulkAdd.
// Right now it's up to the module author to implement whatever rollback
// is necessary, and in future we might implement it for this function itself.
// However, right now there's no use case for it, so long as each of udfAdd
// or udfUpdate are transactional. So for example, if I tried to bulkAdd a bunch
// of Udfs, and some of it failed, those that were successfully added continue
// to be working Udfs, but those that were failed either were not added in the
// first place, or the old version remains in the case of an update
Status
UserDefinedFunction::bulkAddUdf(UdfModuleSrc *input[],
                                uint64_t numModules,
                                XcalarApiUdfContainer *udfContainer,
                                XcalarApiOutput **outputArrayOut[],
                                size_t *outputSizeArrayOut[])
{
    Status status = StatusUnknown, statusTmp;
    Status addUdfStatus = StatusOk;
    XcalarApiOutput **outputArray = NULL;
    size_t *outputSizeArray = NULL;
    uint64_t ii;

    assert(input != NULL);
    assert(outputArrayOut != NULL);
    assert(outputSizeArrayOut != NULL);

    // We need to initialize this to NULL because the caller might need to
    // inspect each element of outputArray for the offending UDFs in the event
    // status != StatusOk.
    *outputArrayOut = NULL;
    *outputSizeArrayOut = NULL;

    outputArray =
        (XcalarApiOutput **) memAlloc(sizeof(*outputArray) * numModules);
    if (outputArray == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Bulk add Scalar Functions failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(outputArray, sizeof(*outputArray) * numModules);

    outputSizeArray =
        (size_t *) memAlloc(sizeof(*outputSizeArray) * numModules);
    if (outputSizeArray == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Bulk add Scalar Functions failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    addUdfStatus = StatusOk;
    for (ii = 0; ii < numModules; ii++) {
        if (strcmp(input[ii]->moduleName, DefaultModuleName) == 0) {
            UdfError udfError;
            udfError.message_ =
                strAllocAndCopy("default module already in system");
            BailIfNull(udfError.message_);
            status = addUpdateOutput(input[ii]->moduleName,
                                     StatusUdfModuleAlreadyExists,
                                     &udfError,
                                     &outputArray[ii],
                                     &outputSizeArray[ii]);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Bulk add Scalar Functions addUpdateOutput %s failed: "
                        "%s",
                        input[ii]->moduleName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
            continue;
        }

        // Doesn't matter if one of the udfAdd fails. Keep going for all Udfs.
        statusTmp = addUdf(input[ii],
                           udfContainer,
                           &outputArray[ii],
                           &outputSizeArray[ii]);

        if (statusTmp != StatusOk) {
            addUdfStatus = statusTmp;
            xSyslog(ModuleName,
                    XlogErr,
                    "Bulk add Scalar Functions add Scalar Function %s failed: "
                    "%s",
                    input[ii]->moduleName,
                    strGetFromStatus(addUdfStatus));
        }
    }

    status = StatusOk;
    *outputArrayOut = outputArray;
    outputArray = NULL;
    *outputSizeArrayOut = outputSizeArray;
    outputSizeArray = NULL;
CommonExit:
    if (status != StatusOk) {
        if (outputArray != NULL) {
            for (ii = 0; ii < numModules; ii++) {
                if (outputArray[ii] != NULL) {
                    memFree(outputArray[ii]);
                    outputArray[ii] = NULL;
                }
            }
            memFree(outputArray);
            outputArray = NULL;
        }

        if (outputSizeArray != NULL) {
            memFree(outputSizeArray);
            outputSizeArray = NULL;
        }
    } else {
        status = addUdfStatus;
    }

    assert(outputArray == NULL);
    assert(outputSizeArray == NULL);
    return status;
}

Status
UserDefinedFunction::addUdf(UdfModuleSrc *input,
                            XcalarApiUdfContainer *udfContainer,
                            XcalarApiOutput **output,
                            size_t *outputSize)
{
    return addUdfInternal(input,
                          udfContainer,
                          UdfRecord::StartVersion,
                          output,
                          outputSize);
}

// Add the specified UDF ('newModule') to UDF container ('udfContainer'), but
// ignore a DUP failure to add the UDF, if the module's source is identical.
Status
UserDefinedFunction::addUdfIgnoreDup(UdfModuleSrc *newModule,
                                     XcalarApiUdfContainer *udfContainer,
                                     XcalarApiOutput **output,
                                     size_t *outputSize)
{
    Status status = StatusOk;
    Status statusTmp;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize = 0;
    XcalarApiUdfGetInput getInput;
    UdfModuleSrc *oldModule;

    statusTmp = addUdfInternal(newModule,
                               udfContainer,
                               UdfRecord::StartVersion,
                               output,
                               outputSize);

    if (statusTmp == StatusUdfModuleAlreadyExists) {
        // Hit a DUP; return success if the module source code is identical

        // first, get the module with the same name in the 'udfContainer'
        verify(strlcpy(getInput.moduleName,
                       newModule->moduleName,
                       sizeof(getInput.moduleName)) <
               sizeof(getInput.moduleName));
        status = UserDefinedFunction::get()->getUdf(&getInput,
                                                    udfContainer,
                                                    &getUdfOutput,
                                                    &getUdfOutputSize);
        BailIfFailed(status);

        oldModule = &getUdfOutput->outputResult.udfGetOutput;
        assert(isValidUdfType(oldModule->type));

        // second, compare the source code of the two modules
        if (memcmp(newModule->source,
                   oldModule->source,
                   oldModule->sourceSize) == 0) {
            // return StatusOk since module's identical - ignore the DUP failure
            xSyslog(ModuleName,
                    XlogDebug,
                    "Module '%s' already exists in Scalar Function container "
                    "for workbook "
                    "'%s' and retina '%s'",
                    oldModule->moduleName,
                    udfContainer->sessionInfo.sessionNameLength == 0
                        ? ""
                        : udfContainer->sessionInfo.sessionName,
                    udfContainer->retinaName);
        } else {
            status = statusTmp;
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed to add Scalar Function '%s' to Scalar "
                            "Function container for "
                            "workbook "
                            "'%s' and retina '%s': %s",
                            newModule->moduleName,
                            udfContainer->sessionInfo.sessionNameLength == 0
                                ? ""
                                : udfContainer->sessionInfo.sessionName,
                            udfContainer->retinaName,
                            strGetFromStatus(status));
        }
    } else {
        status = statusTmp;
    }
CommonExit:
    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    return status;
}

bool
UserDefinedFunction::absUDFinEvalStr(const char *evalStr)
{
    if (strstr(evalStr, UdfWorkBookPrefix) != NULL ||
        strstr(evalStr, SharedUDFsDirPath) != NULL ||
        strstr(evalStr, GlobalUDFDirPathForTests) != NULL ||
        strstr(evalStr, UdfGlobalDataFlowPrefix) != NULL) {
        return true;
    } else {
        return false;
    }
}

// Get the fully qualified name for a UDF - basically pre-pending a prefix
// built from the udfContainer onto the moduleOrFuncName. The returned name
// will be the name of the UDF in the libNs name space.
//
// Some nuances:
// - moduleOrFuncName is so named b/c this string can be either a module or a
//   function name. There are four possibilities:
//
//       - A) "moduleName:FuncName"
//       - B) "moduleName"
//       - C) "funcName"
//       - D) "/<abs_path>/modOrFuncName"
//           -> an absolute path for module or function
//           -> must have a SharedUDFsDirPath OR UdfWorkBookPrefix
//
// - If 'parse' is true, the name can't be of form (B) - i.e. a non-delimited,
//   non-absolute name encountered while parsing must be a function-name (a
//   built-in function). This type of name encountered in all other cases (parse
//   is false), must be a moduleName
//
// - If the name is delimited with ModuleDelim (i.e. ":") - i.e. of form (A), it
//   must be as stated in all cases: moduleName:FuncName
//
// - Form D is already in absolute name form, so the routine just returns the
//   supplied name
//   NOTE: Form D must have a SharedUDFsDirPath or UdfWorkBookPrefix prefix or
//   one of those listed in the routine below...
Status
UserDefinedFunction::getUdfName(char *fullyQualName,
                                size_t fullyQnameSize,
                                char *moduleOrFuncName,
                                XcalarApiUdfContainer *udfContainer,
                                bool parse)
{
    Status status = StatusOk;
    assert(fullyQualName != NULL);
    int ret = 0;
    char *modDelimChar;
    char *moduleName = NULL;
    char *funcName = NULL;
    char *moduleOrFuncNameEnd = NULL;
    UserMgr *userMgr = UserMgr::get();
    bool builtIn = false;

    if (moduleOrFuncName[0] == '/') {  // leading '/' i.e. absolute path
        if (strncmp(moduleOrFuncName,
                    UdfWorkBookPrefix,
                    strlen(UdfWorkBookPrefix)) == 0 ||
            strncmp(moduleOrFuncName,
                    SharedUDFsDirPath,
                    strlen(SharedUDFsDirPath)) == 0 ||
            strncmp(moduleOrFuncName,
                    GlobalUDFDirPathForTests,
                    strlen(GlobalUDFDirPathForTests)) == 0 ||
            strncmp(moduleOrFuncName,
                    UdfGlobalDataFlowPrefix,
                    strlen(UdfGlobalDataFlowPrefix)) == 0) {
            // absolute path to workbook, shared UDFs or retina UDFs
            strlcpy(fullyQualName, moduleOrFuncName, fullyQnameSize);
            // done successfully; now exit
            goto CommonExit;
        } else {
            // absolute path must have a prefix which is one of the valid ones
            // listed above in the 'if' - any other prefix is invalid.
            status = StatusUdfModuleInvalidName;
            goto CommonExit;
        }
    }
    moduleName =
        (char *) memAllocExt(XcalarApiMaxUdfModuleNameLen + 1, ModuleName);
    BailIfNull(moduleName);

    funcName = (char *) memAllocExt(XcalarApiMaxUdfFuncNameLen + 1, ModuleName);
    BailIfNull(funcName);

    modDelimChar = strchr(moduleOrFuncName, ModuleDelim);
    if (modDelimChar == NULL) {
        if (!parse) {
            // even if there's no ModuleDelim, the name may be a module name,
            // since this routine is called even to fully qualify module names
            // when parse is false. In fact, if parse is false, this can't be
            // a function name since there's no <moduleName>: preceding it.
            strlcpy(moduleName,
                    moduleOrFuncName,
                    XcalarApiMaxUdfModuleNameLen + 1);
            *funcName = '\0';
        } else {
            // during parsing, a name without a ModuleDelim implies a function
            // name - a built-in function name.
            strlcpy(funcName, moduleOrFuncName, XcalarApiMaxUdfFuncNameLen + 1);
            *moduleName = '\0';
        }
    } else {
        moduleOrFuncNameEnd = moduleOrFuncName + strlen(moduleOrFuncName) - 1;
        strlcpy(moduleName,
                moduleOrFuncName,
                modDelimChar - moduleOrFuncName + 1);
        strlcpy(funcName,
                modDelimChar + 1,
                moduleOrFuncNameEnd - modDelimChar + 1);
    }

    if (strlen(moduleName) != 0 && strcmp(moduleName, "default") == 0) {
        // Default module ends up in SharedScalar FunctionsDirPath (no workbook
        // container)
        ret = snprintf(fullyQualName,
                       fullyQnameSize,
                       "%s/%s",
                       SharedUDFsDirPath,
                       moduleOrFuncName);
        if (ret < 0 || ret >= (int) fullyQnameSize) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed for user %s, workbook %s: "
                    "%s",
                    moduleOrFuncName,
                    udfContainer->userId.userIdName,
                    udfContainer->sessionInfo.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (!strlen(moduleName) && parse) {
        // no module and parsing - so must be a built-in function
        strlcpy(fullyQualName, moduleOrFuncName, fullyQnameSize);
        builtIn = true;
    } else if (udfContainer == NULL) {
        // old style global (for tests like
        // src/lib/tests/libudf/UdfTests.cpp) which use NULL udfContainer)
        ret = snprintf(fullyQualName,
                       fullyQnameSize,
                       "%s/%s",
                       GlobalUDFDirPathForTests,
                       moduleOrFuncName);
        if (ret < 0 || ret >= (int) fullyQnameSize) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed : %s",
                    moduleOrFuncName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (strlen(udfContainer->userId.userIdName) != 0 &&
               (udfContainer->sessionInfo.sessionNameLength != 0 ||
                udfContainer->sessionInfo.sessionId != 0)) {
        // workbook container
        assert(strlen(udfContainer->retinaName) == 0);  // not yet
        if (udfContainer->sessionInfo.sessionId == 0) {
            status = userMgr->getSessionId(&udfContainer->userId,
                                           &udfContainer->sessionInfo);
            BailIfFailed(status);
            // success means udfContainer->sessionInfo.sessionId is now filled
            assert(udfContainer->sessionInfo.sessionId != 0);
        }
        ret = snprintf(fullyQualName,
                       fullyQnameSize,
                       "%s%s/%lX/%s/%s",
                       UdfWorkBookPrefix,
                       udfContainer->userId.userIdName,
                       udfContainer->sessionInfo.sessionId,
                       UdfWorkBookDir,
                       moduleOrFuncName);
        if (ret < 0 || ret >= (int) fullyQnameSize) {
            status = StatusNameTooLong;
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed for user %s, workbook %s: "
                    "%s",
                    moduleOrFuncName,
                    udfContainer->userId.userIdName,
                    udfContainer->sessionInfo.sessionName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else if (udfContainer->sessionInfo.sessionNameLength == 0) {
        // no workbook
        if (strlen(udfContainer->retinaName) != 0) {
            // retina container
            ret = snprintf(fullyQualName,
                           fullyQnameSize,
                           "%s%s/%s/%s",
                           UdfGlobalDataFlowPrefix,
                           udfContainer->retinaName,
                           UdfWorkBookDir,
                           moduleOrFuncName);
            if (ret < 0 || ret >= (int) fullyQnameSize) {
                status = StatusNameTooLong;
                xSyslog(ModuleName,
                        XlogErr,
                        "Add Scalar Function %s failed for retina %s: %s",
                        moduleOrFuncName,
                        udfContainer->retinaName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            // no workbook, no retina, empty udfContainer (similar to NULL)
            // user for new style global (in same name space as default module)
            ret = snprintf(fullyQualName,
                           fullyQnameSize,
                           "%s/%s",
                           SharedUDFsDirPath,
                           moduleOrFuncName);
            if (ret < 0 || ret >= (int) fullyQnameSize) {
                status = StatusNameTooLong;
                xSyslog(ModuleName,
                        XlogErr,
                        "Add Scalar Function %s failed in global scope: %s",
                        moduleOrFuncName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
    } else {
        assert(0 && "Unexpected Scalar Function container");
        status = StatusInval;
        xSyslog(ModuleName,
                XlogErr,
                "Invalid Scalar Function container was provided, User '%s', "
                "session "
                "name '%s' (length %lu), session ID '%lX'",
                udfContainer->userId.userIdName,
                udfContainer->sessionInfo.sessionName,
                udfContainer->sessionInfo.sessionNameLength,
                udfContainer->sessionInfo.sessionId);
        goto CommonExit;
    }

CommonExit:
    if (moduleName != NULL) {
        memFree(moduleName);
    }
    if (funcName != NULL) {
        memFree(funcName);
    }
    return status;
}

// Flush NFS client cache on all nodes for this UDF (at udfOdPath). This work
// is done in two phases (also see comment in addUdfInternal() at call-site of
// this function on the 2 phases):
//
// Phase-1: prep phase - gFlushPayloadInOut is NULL (used to return memory)
// Phase-2: commit phase - gFlushPayloadInOut is non-NULL (to do real GVM)
//
// In detail, if gFlushPayloadInOut
//
//    -> is NULL: then this is phase-1; memory allocated in this routine
//       and returned in this param - it must be freed by caller
//
//    -> is non-NULL: then this is phase-2- no need to allocate memory
//       just use passed in memory (will be freed by caller of course)
//
Status
UserDefinedFunction::nwClientFlushPath(char *udfOdPath,
                                       bool isCreate,
                                       Gvm::Payload **gFlushPayloadInOut)
{
    Status status = StatusUnknown;
    Status *nodeStatus = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Gvm::Payload *gFlushPayload = NULL;
    UdfNwFlushPath *udfGvmFlushPayload = NULL;
    char *udfGvmPathStr = NULL;
    bool phase_one = false;
    int64_t flushRetries = -1;
    unsigned nodeNum = 0;
    bool gvm_invoked = false;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    if (*gFlushPayloadInOut == NULL) {
        phase_one = true;
        gFlushPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                                  sizeof(UdfNwFlushPath) +
                                                  strlen(udfOdPath) + 1);
        BailIfNull(gFlushPayload);

        // gFlushPayload is freed by caller
        udfGvmFlushPayload = (UdfNwFlushPath *) gFlushPayload->buf;
        udfGvmFlushPayload->pathSize = strlen(udfOdPath) + 1;
        udfGvmFlushPayload->isCreate = isCreate;
        udfGvmPathStr = udfGvmFlushPayload->path;
        status = strStrlcpy(udfGvmPathStr, udfOdPath, strlen(udfOdPath) + 1);
        BailIfFailed(status);

        gFlushPayload->init(udfOperation_->getGvmIndex(),
                            (uint32_t) UdfOperation::Action::FlushNwCache,
                            sizeof(UdfNwFlushPath) + strlen(udfOdPath) + 1);
        *gFlushPayloadInOut = gFlushPayload;
    } else {
        phase_one = false;
        gFlushPayload = *gFlushPayloadInOut;
    }

    do {
        nodeNum = 0;
        flushRetries++;
        status = Gvm::get()->invoke(gFlushPayload, nodeStatus);
        //
        // The GVM cross-node operation is highly likely to succeed in phase-2,
        // since phase-1 did the following:
        //
        // - A) alloced memory and invoked the same GVM broadcast successfully
        //      (via this very routine with a NULL gFlushPayloadInOut)
        // - B) commited the UDF change (create/update) successfully
        //
        // So there can't be any failure scenarios left. However, if in
        // non-DEBUG, the unexpected happens (due to some bug or say a network
        // issue), then log and re-try - note that in case the failure persists,
        // this routine shouldn't try forever - at the risk of having this
        // operation loop endlessly and hang some API - and so quit after
        // NwClientFlushMaxRetries.
        //
        // If the retry count is exceeded, this may be due to an unrelated
        // serious error (UDF change has been committed but flush fails despite
        // phase-1 work to ensure success)- however, the side-effect isn't
        // catastrophic- in this case, the user sees a failure even though UDF
        // change is through (so the user himself/herself is likely to retry) -
        // this is indicated via a special error code. This is highly unlikely
        // to occur in production, as testing has proven. Also, client caches
        // automatically get flushed after some time period so the side-effect
        // (committed UDF change isn't visible) wouldn't last for the user.
        //

        assert(phase_one || status == StatusOk);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogEmerg,
                    "%s Scalar Function nwClientFlushPath (on-disk path %s) "
                    "failed "
                    "unexpectedly: %s, phase-%s, retry count %ld",
                    isCreate ? "Create" : "Update",
                    udfOdPath,
                    strGetFromStatus(status),
                    phase_one ? "1" : "2",
                    flushRetries);
            continue;  // retry in both phases
        }

        gvm_invoked = true;

        for (; nodeNum < nodeCount; nodeNum++) {
            if (nodeStatus[nodeNum] != StatusOk) {
                status = nodeStatus[nodeNum];
                break;
            }
        }
        assert(phase_one || status == StatusOk);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogEmerg,
                    "%s Scalar Function nwClientFlushPath (on-disk path %s) "
                    "failed "
                    "unexpectedly on node %d: %s, phase-%s, retry count %ld",
                    isCreate ? "Create" : "Update",
                    udfOdPath,
                    nodeNum,
                    strGetFromStatus(status),
                    phase_one ? "1" : "2",
                    flushRetries);
            // sleep each iteration of the loop; this way, even if this
            // eventually fails, NwClientFlushSleepInterval *
            // NwClientFlushMaxRetries secs would've elapsed - which should be
            // well over the NFS client cache timeout period, making the
            // visibility a non-issue
            sysSleep(NwClientFlushSleepInterval);
        }
    } while (status != StatusOk &&
             (uint64_t) flushRetries < NwClientFlushMaxRetries);

    if (status != StatusOk &&
        (uint64_t) flushRetries >= NwClientFlushMaxRetries) {
        // phase-2 related comment:
        // log messages above indicated the reason for the original failure;
        // so this isn't lost; but higher-layers need to understand that
        // UDF create/update succeeded but flush failed, via a new status code
        if (!phase_one || gvm_invoked) {
            // return the new status-code only in phase2 OR in phase1 only if
            // gvm has been invoked. If gvm_invoke fails in phase1, we shouldn't
            // proceed to phase2 and commit the UDF change since there seems to
            // be something seriously wrong, and better not to commit the UDF
            // change. So not returning this status in this case, achieves this.
            status = StatusUdfFlushFailed;
        }
    }

CommonExit:

    delete[] nodeStatus;
    return (status);
}

// Global entry point for adding new UDF module.
Status
UserDefinedFunction::addUdfInternal(UdfModuleSrc *input,
                                    XcalarApiUdfContainer *udfContainer,
                                    uint64_t curVersion,
                                    XcalarApiOutput **output,
                                    size_t *outputSize)
{
    Status status;
    XcalarApiOutput *preAllocOutput = NULL;
    size_t preAllocOutputSize;
    LibNs *libNs = LibNs::get();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
    bool nsPublished = false;
    bool gvmIssued = false;
    UdfRecord udfRecord(curVersion, curVersion + 1, false);
    UdfError error;
    memZero(&error, sizeof(error));
    UdfOperation::UdfModuleSrcVersion *udfGvmPayload = NULL;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    char *fullyQNameDup = NULL;
    char *userName = NULL;
    char *sessionName = NULL;
    Gvm::Payload *gPayload = NULL;
    Gvm::Payload *gFlushPayload = NULL;
    char *udfOdPath = NULL;

    assert(input != NULL);
    assert(output != NULL);
    assert(outputSize != NULL);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    userName = udfContainer ? udfContainer->userId.userIdName : NULL;
    sessionName = udfContainer ? udfContainer->sessionInfo.sessionName : NULL;

    status = addUpdateOutput(input->moduleName,
                             StatusOk,
                             &error,
                             &preAllocOutput,
                             &preAllocOutputSize);
    if (status != StatusOk) {
        // Cleanup path requires this to be non-NULL.
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s addUpdateOutput for user %s, session "
                "%s failed: %s",
                input->moduleName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        return status;
    }

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        input->moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    status = getUdfOnDiskPath(fullyQualName, &udfOdPath);
    BailIfFailed(status);

    if (udfOdPath == NULL) {
        // e.g. test Scalar Functions in GlobalUDFDirPathForTests aren't
        // persisted
        input->modulePath[0] = '\0';
    } else {
        status =
            strStrlcpy(input->modulePath, udfOdPath, sizeof(input->modulePath));
        BailIfFailed(status);
    }

    // Try to list functions in the module.
    // Validate Udf and send to XPU for syntax validation, etc.
    status =
        stageAndListFunctions(input->moduleName, input, &error, NULL, NULL);
    if (status != StatusOk) {
        if (status == StatusConnReset) {
            status = StatusXpuDeath;
        }
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s user %s,session %s "
                "stageAndListFunctions failed:%s",
                input->moduleName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Publish UdfRecord::StartVersion as the consistent version for this UDF
    // module.
    nsId = libNs->publish(fullyQualName, &udfRecord, &status);
    if (status != StatusOk) {
        if (status == StatusExist) {
            status = StatusUdfModuleAlreadyExists;
            // Special case common error where UDF module is being copied to the
            // sharedUDF space.  Very targeted but UX ROI justifies it.
            if (strncmp(fullyQualName,
                        SharedUDFsDirPath,
                        strlen(SharedUDFsDirPath)) == 0) {
                // Special case handling
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Failed to add Scalar Function module '%s' to "
                              "the "
                              "'%s' folder: %s",
                              input->moduleName,
                              SharedUDFsDirPath,
                              strGetFromStatus(status));
                goto CommonExit;
            }
        }
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to publish Scalar Function '%s' in namespace "
                      "'%s', user "
                      "'%s', session '%s': %s",
                      input->moduleName,
                      fullyQualName,
                      userName ? userName : "<no userName>",
                      sessionName ? sessionName : "<no sessionName>",
                      strGetFromStatus(status));
        goto CommonExit;
    }
    nsPublished = true;

    handle = libNs->open(fullyQualName, LibNsTypes::WriterExcl, &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound) {
            status = StatusUdfModuleNotFound;
        } else if (status == StatusAccess || status == StatusPendingRemoval) {
            status = StatusUdfModuleAlreadyExists;
        } else if (status == StatusNsInvalidObjName) {
            status = StatusUdfModuleInvalidName;
        }
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to open Scalar Function '%s' in namespace '%s', "
                      "user '%s', "
                      "session '%s': %s",
                      input->moduleName,
                      fullyQualName,
                      userName ? userName : "<no userName>",
                      sessionName ? sessionName : "<no sessionName>",
                      strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(UdfOperation::UdfModuleSrcVersion) +
        input->sourceSize);
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s (full %s) failed for user %s, "
                "session %s: %s",
                input->moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }
    udfGvmPayload = (UdfOperation::UdfModuleSrcVersion *) gPayload->buf;

    // UdfRecord::StartVersion is the first consistent version.
    udfGvmPayload->consistentVersion = udfRecord.consistentVersion_;
    udfGvmPayload->nextVersion = udfRecord.nextVersion_;
    if (strchr(input->moduleName, '/') == NULL) {
        fullyQNameDup = strAllocAndCopy(fullyQualName);
        BailIfNull(fullyQNameDup);
        strlcpy(udfGvmPayload->modulePrefix,
                dirname(fullyQNameDup),
                sizeof(udfGvmPayload->modulePrefix));
    } else {
        // XXX: allow absolute path names for UDFs for now. This is for
        // stuff like custom target tests to work. XXX: The previous XXX comment
        // is probably invalid now - check and fix. Cleanup during testing.
        //
        // XD will disallow
        // users from using / in the UDF names lest they start messing
        // with the global UDF name space and start adding new UDFs into
        // other users' workbooks!
        //
        // NOTE: absolute path names are allowed for shared UDFs - users
        // can't supply this path directly but XD/front-end will supply it
        udfGvmPayload->modulePrefix[0] = '\0';
    }

    memcpy(&udfGvmPayload->addUpdateInput,
           input,
           sizeof(*input) + input->sourceSize);

    // We optimistically attempt to add a new UDF module here. Only upon error,
    // clean out code kicks in.
    gvmIssued = true;
    gPayload->init(udfOperation_->getGvmIndex(),
                   (uint32_t) UdfOperation::Action::Add,
                   sizeof(UdfOperation::UdfModuleSrcVersion) +
                       input->sourceSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Add Scalar Function %s (full %s), user %s, session %s failed "
                "on GVM "
                "invoke to add: %s",
                input->moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // allocate before committing so can back-out in case of memory failure

    assert(
        !UserDefinedFunction::get()->udfPersist_->needsPersist(input,
                                                               udfContainer) ||
        udfOdPath != NULL);

    if (UserDefinedFunction::get()->udfPersist_->needsPersist(input,
                                                              udfContainer)) {
        // Phase-one paves the path for phase-two success - it allocs memory
        // and issues a dummy GVM - if all succeeds, then the UDF change is
        // committed to on-disk and then phase-two is done to flush the n/w
        // client cache so the on-disk update is made visible on all nodes
        status = nwClientFlushPath(udfOdPath, true, &gFlushPayload);
        if (status == StatusUdfFlushFailed) {
            // in phase-1 this status indicates everything succeeded except
            // flush ...in which case it's safe to move to phase-2 which will
            // retry the flushes (by which time NFS client cache timeout period
            // would've made visibility a non-issue)
            status = StatusOk;
        }
        BailIfFailed(status);
    }

    // See large block comment in updateUdf() for atomicity of the libNs
    // and on-disk UDF store update done via the udfUpdateCommit() call
    // below.

    status = udfOperation_->udfUpdateCommit(udfGvmPayload,
                                            UdfOperation::Action::Add,
                                            fullyQualName,
                                            udfContainer,
                                            handle);
    BailIfFailed(status);

    if (gFlushPayload != NULL) {  // would be NULL (if needsPersist is false)
        // Phase-two:
        status = nwClientFlushPath(udfOdPath, true, &gFlushPayload);

        // if the flush above fails (in a highly improbable and unlikely case),
        // a special error code indicating that the UDF update/creation was
        // committed to disk, but client cache flush failed, is returned. This
        // is logged in detail inside nwClientFlushPath..so the details are in
        // the logs.
        //
        // This is like a "soft failure" - the client could retry the UDF
        // update/creation again or just wait for the client caches to be
        // flushed on their own (typically, they'll get flushed in a short time
        // duration).
        //
        // Since the UDF change has been basically successful for all intents
        // and purposes, we should not let this status be returned all the way
        // up...(we can revisit this if needed, in the future).
        //
        // This is because if we let this fail, the in-core state and on-disk
        // state will become inconsistent - the failure will clean out the
        // in-core state but the on-disk state will remain - if we decide to
        // clean-out the on-disk state also, then the attempt to remove the
        // on-disk state could fail too...so the code to handle this can go on
        // forever - this is highly improbable and so not worth trying to
        // address currently.
        //

        status = StatusOk;
    }

    assert(status == StatusOk);
CommonExit:
    *output = NULL;

    if (error.message_ != NULL) {
        // Failure dealt with by using preallocated output below.
        // XXX - we are relying on this keeping *output == NULL in error cases
        Status status2 = addUpdateOutput(input->moduleName,
                                         status,
                                         &error,
                                         output,
                                         outputSize);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed on addUpdateOutput: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
        if (error.message_ != NULL) {
            memFree((char *) error.message_);
            error.message_ = NULL;
        }
    }

    if (*output == NULL) {
        preAllocOutput->outputResult.udfAddUpdateOutput.status = status.code();
        assertStatic(
            sizeof(input->moduleName) ==
            sizeof(preAllocOutput->outputResult.udfAddUpdateOutput.moduleName));
        strlcpy(preAllocOutput->outputResult.udfAddUpdateOutput.moduleName,
                input->moduleName,
                sizeof(preAllocOutput->outputResult.udfAddUpdateOutput
                           .moduleName));

        *output = preAllocOutput;
        *outputSize = preAllocOutputSize;
        preAllocOutput = NULL;
    }

    if (preAllocOutput != NULL) {
        memFree(preAllocOutput);
    }

    if (fullyQNameDup != NULL) {
        memFree(fullyQNameDup);
    }

    if (status != StatusOk && gvmIssued == true) {
        // Clean up the failed version here.
        cleanoutHelper(input->moduleName,
                       udfRecord.consistentVersion_,
                       udfRecord.nextVersion_,
                       udfContainer);
    }

    if (status != StatusOk && nsPublished == true) {
        // Since we failed to add Scalar Function, remove the module from
        // Namespace.
        Status status2 = libNs->remove(nsId, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed on NS remove: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Add Scalar Function %s failed on NS close: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (gFlushPayload != NULL) {
        memFree(gFlushPayload);
        gFlushPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (udfOdPath != NULL) {
        memFree(udfOdPath);
        udfOdPath = NULL;
    }

    return status;
}

// Global entry point for updating existing UDF module.
Status
UserDefinedFunction::updateUdf(UdfModuleSrc *input,
                               XcalarApiUdfContainer *udfContainer,
                               XcalarApiOutput **output,
                               size_t *outputSize)
{
    Status status;
    XcalarApiOutput *preAllocOutput = NULL;
    size_t preAllocOutputSize;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    bool gvmIssued = false;
    UdfRecord *udfRecord = NULL;
    UdfError error;
    memZero(&error, sizeof(error));
    UdfOperation::UdfModuleSrcVersion *udfGvmPayload = NULL;
    int ret;
    LibNs *libNs = LibNs::get();
    char moduleNameVersioned[UdfVersionedModuleName + 1];
    uint64_t obsoleteConsistentVersion = UdfRecord::InvalidVersion;
    uint64_t obsoleteNextVersion = UdfRecord::InvalidVersion;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    char *fullyQnameDup = NULL;
    Gvm::Payload *gPayload = NULL;
    Gvm::Payload *gFlushPayload = NULL;
    char *udfOdPath = NULL;

    assert(input != NULL);
    assert(output != NULL);
    assert(outputSize != NULL);

    status = addUpdateOutput(input->moduleName,
                             StatusOk,
                             &error,
                             &preAllocOutput,
                             &preAllocOutputSize);
    if (status != StatusOk) {
        // Cleanup path requires this to be non-NULL.
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s addUpdateOutput failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        return status;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        input->moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    status = getUdfOnDiskPath(fullyQualName, &udfOdPath);
    BailIfFailed(status);

    // Since this is an update, it requires exclusive access to module. If this
    // fails, we will error out, since the Scalar Function is currently in use
    // elsewhere and cannot be updated.
    // XXX In the future, we could consider better workflows which
    // suspend/resume the updates to reach the window of opportunity where
    // exclusive access is available.
    handle = libNs->open(fullyQualName,
                         LibNsTypes::WriterExcl,
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
                      "Update Scalar Function %s open failed: %s",
                      input->moduleName,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // Promote the next version number to be used for each update. This way,
    // in the event of a failed update Txn, the version number is never reused
    // and the Txn cleanout can garbage collect the state left back by a
    // failed Txn.
    // XXX: maybe this should also use the getRefToObject API here instead of
    // updateNsObject but it's not really necessary
    udfRecord->nextVersion_ += 1;
    handle = libNs->updateNsObject(handle, udfRecord, &status);
    udfRecord->nextVersion_ -= 1;
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed on update NS: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Generate the update version module name
    ret = snprintf(moduleNameVersioned,
                   UdfVersionedModuleName,
                   "%s%s%lu",
                   input->moduleName,
                   UdfVersionDelim,
                   udfRecord->nextVersion_);
    if (ret >= (int) sizeof(moduleNameVersioned)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Try to list functions in the module.
    status =
        stageAndListFunctions(moduleNameVersioned, input, &error, NULL, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s stageAndListFunctions failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(UdfOperation::UdfModuleSrcVersion) +
        input->sourceSize);
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    udfGvmPayload = (UdfOperation::UdfModuleSrcVersion *) gPayload->buf;
    obsoleteConsistentVersion = udfRecord->consistentVersion_;
    obsoleteNextVersion = udfRecord->nextVersion_;
    udfGvmPayload->consistentVersion = obsoleteConsistentVersion;
    udfGvmPayload->nextVersion = obsoleteNextVersion;

    if (strchr(input->moduleName, '/') == NULL) {
        fullyQnameDup = strAllocAndCopy(fullyQualName);
        BailIfNull(fullyQnameDup);
        strlcpy(udfGvmPayload->modulePrefix,
                dirname(fullyQnameDup),
                sizeof(udfGvmPayload->modulePrefix));
    } else {
        // For absolute pathnames (API for shared UDFs explicitly requires the
        // caller to supply an absolute pathname), no prefix required.  See
        // comment for the same issue in addUdfInternal
        udfGvmPayload->modulePrefix[0] = '\0';
    }
    memcpy(&udfGvmPayload->addUpdateInput,
           input,
           sizeof(*input) + input->sourceSize);

    // We optimistically update UDFs here. Upon error, clean out code kicks in
    // to do the transaction recovery.
    gvmIssued = true;
    gPayload->init(udfOperation_->getGvmIndex(),
                   (uint32_t) UdfOperation::Action::Update,
                   sizeof(UdfOperation::UdfModuleSrcVersion) +
                       input->sourceSize);
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Update Scalar Function %s failed on GVM invoke to update: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Now that all the GVM updates succeeded, update the consistent version
    // number for the UDF name in libNs and store the UDF persistently - do
    // both in an atomic manner (both succeed or both fail).
    //
    // For the atomicity requirement, any libNs updates (to bump up the version
    // or revert the version in case of failure) must NOT fail - so the libNs
    // work is done on the libNs DLM node for the UDF (where the libNs object
    // is allocated) so that the update can be done using an in-memory pointer
    // to the udfRecord, atomically with the persistent UDF update to the UDF
    // on-disk store  - for this, the udfUpdateCommit() call is made below -
    // which does a directed one-node, 2PC to the DLM node - see
    // TwoPcMsg2pcAddOrUpdateUdfDlm1::schedLocalWork() which performs the work.

    // XXX Update UDF should not be called from restore code path kicked
    // during bootstrapping? Anyway, let's poke around that later.

    assert(
        !UserDefinedFunction::get()->udfPersist_->needsPersist(input,
                                                               udfContainer) ||
        udfOdPath != NULL);

    if (UserDefinedFunction::get()->udfPersist_->needsPersist(input,
                                                              udfContainer)) {
        // Phase-one: see comment in addUdfInternal (which does the same thing)
        status = nwClientFlushPath(udfOdPath, false, &gFlushPayload);
        if (status == StatusUdfFlushFailed) {
            // in phase-1 this status indicates everything succeeded except
            // flush ...in which case it's safe to move to phase-2 which will
            // retry the flushes (by which time NFS client cache timeout period
            // would've made visibility a non-issue)
            status = StatusOk;
        }
        BailIfFailed(status);
    }

    status = udfOperation_->udfUpdateCommit(udfGvmPayload,
                                            UdfOperation::Action::Update,
                                            fullyQualName,
                                            udfContainer,
                                            handle);
    BailIfFailed(status);

    if (gFlushPayload != NULL) {  // would be NULL (if needsPersist is false)
        // Phase-two: see comment in addUdfInternal (which does the same thing)
        status = nwClientFlushPath(udfOdPath, false, &gFlushPayload);
        assert(status == StatusOk);
        status = StatusOk;  // see comment in addUdfInternal
    }

    // ************************************************
    // NOTE: a failure in udfUpdateCommit doesn't necessarily mean the UDF
    // didn't actually get committed to libNs and on-disk - since the failure
    // could've occurred in the rest of the 2pc path after the two-pc receiver
    // function successfully committed - highly unlikely but possible...
    // however, this is acceptable if an error from this routine (i.e. the
    // add/update UDF API's failure) does not imply that the UDF couldn't have
    // been committed. No new code that could fail, should be added to this
    // section of code after the call to udfUpdateCommit above.
    //
    // Note that if the on-disk state was committed but the udfUpdateCommit()
    // failed after that, this suffers from the same issue discussed in
    // the comment for nwClientFlushPath() failure in addUdfInternal - possible
    // inconsistency that arises between in-core and on-disk state if the
    // udfUpdateCommit() fails in this manner - since the failure cleanout would
    // remove the libNs/in-core state below, but the on-disk state has been
    // committed...
    // ...addressing this by also reverting the on-disk state, may itself fail
    // - leading to a never ending cycle.
    //
    // We assume these sort of errors don't happen - if they do, we have bigger
    // issues going on in the cluster!
    //
    // ************************************************

    // libNsUdfRecord->nextVersion_ += 1; not needed; already done at the very
    // start in the call to updateNsObject

    // Clean up the obsolete consistent version now. Generate the obsolete
    // consistent version module name. Also failure to cleanout is not a
    // hard failure.
    cleanoutHelper(input->moduleName,
                   obsoleteConsistentVersion,
                   obsoleteNextVersion,
                   udfContainer);

CommonExit:
    *output = NULL;

    if (error.message_ != NULL) {
        // Failure dealt with by using preallocated output below.
        // XXX - we are relying on this keeping *output == NULL in error cases
        Status status2 = addUpdateOutput(input->moduleName,
                                         status,
                                         &error,
                                         output,
                                         outputSize);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Update Scalar Function %s failed on addUpdateOutput: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
        if (error.message_ != NULL) {
            memFree((char *) error.message_);
            error.message_ = NULL;
        }
    }

    if (*output == NULL) {
        preAllocOutput->outputResult.udfAddUpdateOutput.status = status.code();
        assertStatic(
            sizeof(input->moduleName) ==
            sizeof(preAllocOutput->outputResult.udfAddUpdateOutput.moduleName));
        strlcpy(preAllocOutput->outputResult.udfAddUpdateOutput.moduleName,
                input->moduleName,
                sizeof(preAllocOutput->outputResult.udfAddUpdateOutput
                           .moduleName));
        *output = preAllocOutput;
        *outputSize = preAllocOutputSize;
        preAllocOutput = NULL;
    }

    if (preAllocOutput != NULL) {
        memFree(preAllocOutput);
    }

    if (fullyQnameDup != NULL) {
        memFree(fullyQnameDup);
    }

    if (status != StatusOk && gvmIssued == true) {
        // Clean up the failed update version here.
        cleanoutHelper(input->moduleName,
                       obsoleteNextVersion,
                       obsoleteNextVersion + 1,
                       udfContainer);
    }

    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Update Scalar Function %s failed on NS close: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (udfRecord != NULL) {
        memFree(udfRecord);
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (gFlushPayload != NULL) {
        memFree(gFlushPayload);
        gFlushPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (udfOdPath != NULL) {
        memFree(udfOdPath);
        udfOdPath = NULL;
    }

    return status;
}

// Return the resolution of a UDF given its moduleName and udfContainer. The
// moduleName is searched in the set of paths in UserDefinedFunction::UdfPath[]
// and the fully qualified path for the first hit found, is returned.
Status
UserDefinedFunction::getResolutionUdf(char *moduleName,
                                      XcalarApiUdfContainer *udfContainer,
                                      char **fullyQName,
                                      size_t *fullyQNameSize)
{
    Status status;
    assert(fullyQName != NULL);
    assert(fullyQNameSize != NULL);
    UdfRecord *udfRecord = NULL;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    UserDefinedFunction *udf = UserDefinedFunction::get();
    LibNs *libNs = LibNs::get();
    size_t fullyQNameSizeLocal = LibNsTypes::MaxPathNameLen + 1;
    char fullyQualNameLocal[LibNsTypes::MaxPathNameLen + 1];
    char prefixedSym[LibNsTypes::MaxPathNameLen + 1];
    int udfPathNpaths = 0;
    int ii;
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize = 0;
    bool udfFound = false;

    // init return values first
    *fullyQName = NULL;
    *fullyQNameSize = 0;

    udfPathNpaths = UserDefinedFunction::UdfPaths::Npaths;
    assert(udfPathNpaths == (sizeof(udf->UdfPath) / sizeof(char *)));

    // For each path in UserDefinedFunction::UdfPath[], check if moduleName
    // is present - quit on the first hit, and construct the fully qualified
    // module name from this, for return to caller
    for (ii = 0; ii < udfPathNpaths; ii++) {
        switch (ii) {
        case UserDefinedFunction::UdfPaths::CurDir:
            assert(strncmp(udf->UdfPath[ii],
                           UserDefinedFunction::UdfCurrentDirPath,
                           strlen(UserDefinedFunction::UdfCurrentDirPath)) ==
                   0);
            verify(
                snprintf(prefixedSym, sizeof(prefixedSym), "%s", moduleName) <
                (int) sizeof(prefixedSym));
            break;
        default:
            // NOT current workbook; prefix the current path to moduleName
            // so full path can be searched
            verify(snprintf(prefixedSym,
                            sizeof(prefixedSym),
                            "%s/%s",
                            udf->UdfPath[ii],
                            moduleName) < (int) sizeof(prefixedSym));
            break;
        }
        xSyslog(ModuleName,
                XlogDebug,
                "Get Scalar Function container u=%s, w=%s,%lX",
                udfContainer->userId.userIdName,
                udfContainer->sessionInfo.sessionName,
                udfContainer->sessionInfo.sessionId);

        status = udf->getUdfName(fullyQualNameLocal,
                                 sizeof(fullyQualNameLocal),
                                 prefixedSym,
                                 udfContainer,
                                 false);
        BailIfFailed(status);

        // Get Read only access to the UDF module.
        handle = libNs->open(fullyQualNameLocal,
                             LibNsTypes::ReaderShared,
                             (NsObject **) &udfRecord,
                             &status);
        if (status != StatusOk) {
            if (status == StatusNsNotFound ||
                status == StatusNsInvalidObjName) {
                status = StatusUdfModuleNotFound;
            } else if (status == StatusAccess ||
                       status == StatusPendingRemoval) {
                status = StatusUdfModuleInUse;
            }
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Get Scalar Function %s open failed: %s",
                          fullyQualNameLocal,
                          strGetFromStatus(status));
            continue;  // go to next path in UdfPath[] array
        } else {
            handleValid = true;
        }

        strlcpy(udfGetInput.moduleName,
                moduleName,
                sizeof(udfGetInput.moduleName));

        // Don't really need the UDF contents but this just validates that
        // udfLocal_ has the UDF that libNs claims it should
        status = udfLocal_->getUdf(&udfGetInput,
                                   fullyQualNameLocal,
                                   udfRecord->consistentVersion_,
                                   &getUdfOutput,
                                   &getUdfOutputSize);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Get Scalar Function %s failed: %s",
                    moduleName,
                    strGetFromStatus(status));
        } else {
            // this UDF module's valid and accessible
            udfFound = true;
            break;
        }
    }

    if (udfFound) {
        *fullyQName = (char *) memAllocExt(fullyQNameSizeLocal, ModuleName);
        BailIfNull(*fullyQName);
        *fullyQNameSize = strlen(fullyQualNameLocal) >= fullyQNameSizeLocal
                              ? fullyQNameSizeLocal - 1
                              : strlen(fullyQualNameLocal);
        strlcpy(*fullyQName, fullyQualNameLocal, fullyQNameSizeLocal);
    }

CommonExit:
    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Get Scalar Function %s failed on NS close: %s",
                    moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (udfRecord != NULL) {
        memFree(udfRecord);
        udfRecord = NULL;
    }

    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    return status;
}

Status
UserDefinedFunction::getUdfOnDiskPath(const char *udfLibNsPath,
                                      char **udfOnDiskPath)
{
    return udfPersist_->getUdfOnDiskPath(udfLibNsPath, udfOnDiskPath);
}

Status
UserDefinedFunction::getUdf(XcalarApiUdfGetInput *input,
                            XcalarApiUdfContainer *udfContainer,
                            XcalarApiOutput **output,
                            size_t *outputSize)
{
    assert(input != NULL);
    assert(output != NULL);
    assert(outputSize != NULL);
    UdfRecord *udfRecord = NULL;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    char fullyQualName[LibNsTypes::MaxPathNameLen + 1];
    Status status;
    LibNs *libNs = LibNs::get();

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        input->moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    // Get Read only access to the Scalar Function module.
    handle = libNs->open(fullyQualName,
                         LibNsTypes::ReaderShared,
                         (NsObject **) &udfRecord,
                         &status);
    if (status != StatusOk) {
        if (status == StatusNsNotFound || status == StatusNsInvalidObjName) {
            status = StatusUdfModuleNotFound;
        } else if (status == StatusAccess || status == StatusPendingRemoval) {
            status = StatusUdfModuleInUse;
        }
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Get Scalar Function %s open failed: %s",
                      fullyQualName,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    // Only bother to return consistent version of UDF here. Nothing else is
    // trust worthy.
    status = udfLocal_->getUdf(input,
                               fullyQualName,
                               udfRecord->consistentVersion_,
                               output,
                               outputSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Get Scalar Function %s failed: %s",
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (handleValid) {
        Status status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Get Scalar Function %s failed on NS close: %s",
                    input->moduleName,
                    strGetFromStatus(status2));
        }
    }

    if (udfRecord != NULL) {
        memFree(udfRecord);
    }
    return status;
}

Status
UserDefinedFunction::deleteUdf(XcalarApiUdfDeleteInput *input,
                               XcalarApiUdfContainer *udfContainer)
{
    Status status;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    size_t getPathInfoSize;
    LibNsTypes::PathInfoOut *pathInfoOut = NULL;
    LibNs *libNs = LibNs::get();

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        input->moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    getPathInfoSize = libNs->getPathInfo(fullyQualName, &pathInfoOut);
    if (getPathInfoSize == 0) {
        status = StatusUdfModuleNotFound;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Delete Scalar Function %s failed on get path info: %s",
                      fullyQualName,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    for (size_t ii = 0; ii < getPathInfoSize; ii++) {
        char *moduleNameTmp = pathInfoOut[ii].pathName;
        status = deleteUdfInternal(moduleNameTmp, udfContainer, false);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Delete Scalar Function %s failed: %s",
                    moduleNameTmp,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }
    status = StatusOk;

CommonExit:
    if (pathInfoOut != NULL) {
        memFree(pathInfoOut);
    }
    return status;
}

// Routine to delete the UDF from cluster and persistently.

// NOTE: legacy flag needed to handle upgrade. Turning the flag on triggers
// deletion of the legacy UDF metadata and versioned UDF persistent files, as
// opposed to deletion of the new persistent layout.

Status
UserDefinedFunction::deleteUdfInternal(char *moduleName,
                                       XcalarApiUdfContainer *udfContainer,
                                       bool legacy)
{
    Status status;
    LibNsTypes::NsHandle handle;
    bool handleValid = false;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen + 1];
    UdfRecord *udfRecord = NULL;
    UdfOperation::XcalarApiUdfDeleteInputVersion *udfGvmPayload = NULL;
    LibNs *libNs = LibNs::get();
    XcalarApiUdfGetInput getUdfInput;
    XcalarApiOutput *getUdfApiOutput = NULL;
    size_t outputSize;
    UdfModuleSrc *udfModule = NULL;
    char moduleFullNameVersioned[LibNsTypes::MaxPathNameLen + 1];
    bool removeUdf = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    char *fullyQNameDup = NULL;
    char *userName = NULL;
    char *sessionName = NULL;
    char *moduleNameDup = NULL;

    userName = udfContainer ? udfContainer->userId.userIdName : NULL;
    sessionName = udfContainer ? udfContainer->sessionInfo.sessionName : NULL;
    Gvm::Payload *gPayload = NULL;

    if (strlen(moduleName) > MaxUdfPathLen) {
        status = StatusUdfModuleInvalidName;
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    // Open in exclusive access. This UDF module is earmarked for deletion.
    handle = libNs->open(fullyQualName,
                         LibNsTypes::WriterExcl,
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
                      "Delete Scalar Function %s open failed: %s",
                      moduleName,
                      strGetFromStatus(status));
        goto CommonExit;
    }
    handleValid = true;

    if (udfRecord->isCreated_ == false) {
        status = StatusUdfModuleInUse;
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s (full %s), user %s, session %s "
                "failed on "
                "pending create: %s",
                moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    ret = snprintf(moduleFullNameVersioned,
                   sizeof(moduleFullNameVersioned),
                   "%s%s%lu",
                   fullyQualName,
                   UdfVersionDelim,
                   udfRecord->consistentVersion_);
    if (ret >= (int) sizeof(moduleFullNameVersioned)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s (full %s) user %s "
                "session %s failed: %s",
                moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Only bother to return consistent version of UDF here. Nothing else is
    // trust worthy.
    snprintf(getUdfInput.moduleName,
             sizeof(getUdfInput.moduleName),
             "%s",
             moduleName);
    status = udfLocal_->getUdf(&getUdfInput,
                               fullyQualName,
                               udfRecord->consistentVersion_,
                               &getUdfApiOutput,
                               &outputSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s (full %s), user %s "
                "session %s get failed: %s",
                moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) +
        sizeof(UdfOperation::XcalarApiUdfDeleteInputVersion));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s failed: %s",
                moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    udfGvmPayload =
        (UdfOperation::XcalarApiUdfDeleteInputVersion *) gPayload->buf;
    udfGvmPayload->consistentVersion = udfRecord->consistentVersion_;
    udfGvmPayload->nextVersion = udfRecord->nextVersion_;

    if (strchr(moduleName, '/') == NULL) {
        fullyQNameDup = strAllocAndCopy(fullyQualName);
        BailIfNull(fullyQNameDup);
        strlcpy(udfGvmPayload->modulePrefix,
                dirname(fullyQNameDup),
                sizeof(udfGvmPayload->modulePrefix));
    } else {
        // allow absolute name; so no prefix for UdfLocal
        // see similar code and comments for this in addUdfInternal
        udfGvmPayload->modulePrefix[0] = '\0';
    }

    snprintf(udfGvmPayload->deleteInput.moduleName,
             sizeof(udfGvmPayload->deleteInput.moduleName),
             "%s",
             moduleName);

    // Delete all the versions on this Scalar Function. This includes consistent
    // version to less than the next version.
    removeUdf = true;
    gPayload->init(udfOperation_->getGvmIndex(),
                   (uint32_t) UdfOperation::Action::Delete,
                   sizeof(UdfOperation::XcalarApiUdfDeleteInputVersion));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s (full %s) user %s, session %s "
                "failed on "
                "GVM invoke: %s",
                moduleName,
                fullyQualName,
                userName ? userName : "<no userName>",
                sessionName ? sessionName : "<no sessionName>",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Delete the UDF persistent copy.
    udfModule = &getUdfApiOutput->outputResult.udfGetOutput;

    if (legacy) {
        // In legacy, persisted UDF files have version number - so use the
        // moduleFullNameVersioned here
        moduleNameDup = strAllocAndCopy(moduleFullNameVersioned);
        BailIfNull(moduleNameDup);
        // needsPersist() can't be used here since deletion occurs only during
        // boot for legacy, and needsPersist() always returns false during boot
        // since udfPersist_->finishedRestoring() is false. It's always ok to
        // persistently delete for legacy boot...
        udfPersist_->legacyDel(basename(moduleNameDup),
                               udfContainer,
                               udfModule->type);
    } else {
        if (udfPersist_->needsPersist(udfModule, udfContainer)) {
            // No version number in persisted files - so just use moduleName
            udfPersist_->del(moduleName, udfContainer, udfModule->type);
        }
    }

CommonExit:
    if (handleValid) {
        // Now mark the Module for removal. This means all subsequent attempts
        // to open will fail.
        Status status2;
        if (removeUdf) {
            status2 = libNs->remove(fullyQualName, NULL);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Delete Scalar Function %s failed on NS remove: %s",
                        moduleName,
                        strGetFromStatus(status2));
            }
        }

        // With close of handle, the module will be tossed out of the namespace.
        status2 = libNs->close(handle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Delete Scalar Function %s failed on NS close: %s",
                    moduleName,
                    strGetFromStatus(status2));
        }
    }
    if (udfRecord != NULL) {
        memFree(udfRecord);
    }
    if (getUdfApiOutput != NULL) {
        memFree(getUdfApiOutput);
    }
    if (fullyQNameDup != NULL) {
        memFree(fullyQNameDup);
    }
    if (moduleNameDup != NULL) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

size_t  // static
UserDefinedFunction::sizeOfAddUpdateOutput(size_t messageSize,
                                           size_t tracebackSize)
{
    XcalarApiUdfAddUpdateOutput *addUpdateOutput;
    return XcalarApiSizeOfOutput(typeof(*addUpdateOutput)) + messageSize +
           tracebackSize;
}

//
// On-load execution of UDFs.
// XXX: WARNING: fullyQualifiedFnName is modified!! This MUST BE FIXED!!
//
Status
UserDefinedFunction::parseFunctionName(char *fullyQualifiedFnName,
                                       const char **moduleName,
                                       const char **moduleVersion,
                                       const char **functionName)
{
    Status status;
    char *delim;
    char *modNameDup = NULL;

    assert(moduleName != NULL);
    *moduleName = fullyQualifiedFnName;
    if (moduleVersion != NULL) {
        *moduleVersion = NULL;
    }
    if (functionName != NULL) {
        *functionName = NULL;
    }

    // Figure out the functionName
    if (functionName != NULL) {
        delim = strchr(fullyQualifiedFnName, ModuleDelim);
        if (delim == NULL) {
            status = StatusUdfInval;
            goto CommonExit;
        }
        *delim = '\0';
        *functionName = delim + 1;
    }

    // Figure out the moduleVersion
    if (moduleVersion != NULL) {
        if (strchr((*moduleName), '/') != NULL) {
            // If moduleName has absolute path, and the container is in a retina
            // made synthetically from an execute-retina dag node, the retina
            // name may have a special char which may be the same as that in
            // UdfVersionDelim[0]. So the check for version delim must be made
            // on the basename of the full absolute moduleName, not the absolute
            // name, lest it find the udf version delim in the retina name.
            modNameDup = strAllocAndCopy((*moduleName));
            if (strchr(basename(modNameDup), UdfVersionDelim[0]) != NULL) {
                // Use 'rindex' to find the last occurrence since first
                // occurrence may land earlier in the path than in the
                // actual moduleName. Since we know that strchr in the basename
                // is not NULL, we know that rindex() must yield non-NULL, hence
                // the assert that delim must be non-NULL.
                delim = rindex((char *) (*moduleName), UdfVersionDelim[0]);
                assert(delim != NULL);
                *delim = '\0';
                *moduleVersion = delim + 1;
            } else {
                *moduleVersion = NULL;
            }
        } else {
            delim = rindex((char *) (*moduleName), UdfVersionDelim[0]);
            if (delim != NULL) {
                *delim = '\0';
                *moduleVersion = delim + 1;
            } else {
                *moduleVersion = NULL;
            }
        }
    }
    status = StatusOk;

CommonExit:
    if (modNameDup != NULL) {
        memFree(modNameDup);
    }
    return status;
}

// XXX: upgradeToDionysus flag may not be needed - use false for now.
// Decision TBD until upgrade work's done
Status
UserDefinedFunction::unpersistAll(bool upgradeToDionysus)
{
    Status status = udfPersist_->addAll(upgradeToDionysus);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function unpersist all failed: %s",
                strGetFromStatus(status));
    }
    return status;
}

Status
UserDefinedFunction::transferModules(ParentChild *child,
                                     EvalUdfModuleSet *modules,
                                     const char *userIdName,
                                     uint64_t sessionId)
{
    Status status;
    ChildUdfInitRequest udfinit;
    LocalConnection::Response *response = NULL;
    const ProtoResponseMsg *responseMsg;
    ProtoChildRequest childRequest;

    status = udfLocal_->loadModules(child, modules);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function transfer modules failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    try {
        udfinit.set_username(userIdName);
        udfinit.set_sessionid(sessionId);
        childRequest.set_func(ChildFuncUdfInit);
        childRequest.set_allocated_childudfinit(&udfinit);
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function transfer modules failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = child->sendAsync(&childRequest, &response);
    childRequest.release_childudfinit();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function transferUserIdName failed to send to child "
                "(pid: %u):%s",
                child->getPid(),
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = response->wait(TimeoutUdfInitUsecs);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function transferUserIdName failed to get a response "
                "from child"
                " (pid: %u) (timeout: %lu microsecs):%s",
                child->getPid(),
                TimeoutUdfInitUsecs,
                strGetFromStatus(status));
        goto CommonExit;
    }
    responseMsg = response->get();
    status.fromStatusCode((StatusCode) responseMsg->status());
    if (status != StatusOk) {
        const char *errorMsg = "None";
        if (!responseMsg->error().empty()) {
            errorMsg = responseMsg->error().c_str();
        }
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function transferUserIdName to child (pid: %u) failed: "
                " status %s, error %s",
                child->getPid(),
                strGetFromStatus(status),
                errorMsg);
    }

CommonExit:
    if (response != NULL) {
        response->refPut();
    }
    return status;
}

Status
UserDefinedFunction::openUdfHandle(char *fullyQualifiedFnName,
                                   XcalarApiUdfContainer *udfContainer,
                                   LibNsTypes::NsHandle *retNsHandle,
                                   char **retFullyQualFnNameVersioned)
{
    *retFullyQualFnNameVersioned = NULL;
    bool handleValid = false;
    int ret;
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    Status status;
    LibNs *libNs = LibNs::get();
    char fqFnName[UdfVersionedFQFname];
    const char *fnName, *moduleName, *moduleVersion;
    UdfRecord *udfRecord = NULL;
    char *fqFnNameVersioned = NULL;

    ret = snprintf(fqFnName, sizeof(fqFnName), "%s", fullyQualifiedFnName);
    if (ret >= (int) sizeof(fqFnName)) {
        status = StatusNameTooLong;
        xSyslog(ModuleName,
                XlogErr,
                "Open Scalar Function handle %s failed: %s",
                fullyQualifiedFnName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = parseFunctionName(fqFnName, &moduleName, &moduleVersion, &fnName);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Open Scalar Function handle %s failed on parseFunctionName: "
                "%s",
                fullyQualifiedFnName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        (char *) moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    // Get Read only access to the UDF module.
    *retNsHandle = libNs->open(fullyQualName,
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
                      "Unable to open Scalar Function handle %s",
                      fullyQualName);
        goto CommonExit;
    }
    handleValid = true;

    status = udfLocal_->getModuleNameVersioned((char *) moduleName,
                                               udfRecord->consistentVersion_,
                                               (char *) fnName,
                                               &fqFnNameVersioned);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Open Scalar Function handle %s failed: %s",
                fullyQualifiedFnName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    *retFullyQualFnNameVersioned = fqFnNameVersioned;
    fqFnNameVersioned = NULL;

CommonExit:
    if (handleValid && status != StatusOk) {
        Status status2 = libNs->close(*retNsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Get Scalar Function %s failed on NS close: %s",
                    fullyQualifiedFnName,
                    strGetFromStatus(status2));
        }
    }
    if (udfRecord != NULL) {
        memFree(udfRecord);
    }
    if (fqFnNameVersioned != NULL) {
        memFree(fqFnNameVersioned);
    }
    return status;
}

Status
UserDefinedFunction::closeUdfHandle(char *fullyQualifiedFnName,
                                    LibNsTypes::NsHandle nsHandle)
{
    LibNs *libNs = LibNs::get();
    Status status = libNs->close(nsHandle, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Close Scalar Function %s failed on NS close: %s",
                fullyQualifiedFnName,
                strGetFromStatus(status));
    }
    return status;
}

// Init the UDF container with the supplied container components: username,
// workbook name, and retinaName.
// Currently, retinaName is supported when user/workbook are absent. The case
// of retinaName inside a workbook will occur in the future.
Status
UserDefinedFunction::initUdfContainer(XcalarApiUdfContainer *udfContainer,
                                      XcalarApiUserId *userId,
                                      XcalarApiSessionInfoInput *sessionInfo,
                                      const char *retinaName)
{
    Status status = StatusOk;
    assert(udfContainer != NULL);

    if (udfContainer == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memset(udfContainer, 0, sizeof(XcalarApiUdfContainer));
    if (userId != NULL) {
        memcpy(&udfContainer->userId, userId, sizeof(XcalarApiUserId));
    }
    if (sessionInfo != NULL) {
        memcpy(&udfContainer->sessionInfo,
               sessionInfo,
               sizeof(XcalarApiSessionInfoInput));
    }
    if (retinaName != NULL) {
        status = strStrlcpy(udfContainer->retinaName,
                            retinaName,
                            sizeof(udfContainer->retinaName));
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// initUdfContainerFromScope() is intended as a general utility function for
// any API using the new API infrastructure in XCE, to initialize the UDF
// container that may be needed by the API given the API's scope param (which
// all APIs must use in the new infrastructure, to supply the session info (or
// not, as the case may be) for the API.
//
// This routine translates a protobuf scope specifier in "scope" to its C
// equivalent UDF container. If the "scope" is global, the UDF container is
// empty (zero'ed out), but if the "scope" is Workbook, the UDF container is
// initialized with the corresponding user and session info extracted from the
// workbook specifier protobuf container.
Status
UserDefinedFunction::initUdfContainerFromScope(
    XcalarApiUdfContainer *udfContainer,
    const xcalar::compute::localtypes::Workbook::WorkbookScope *scope)
{
    Status status;
    XcalarApiUserId user;
    XcalarApiSessionInfoInput sessInput;
    UserMgr *userMgr_ = UserMgr::get();

    memset(&user, 0, sizeof(XcalarApiUserId));
    memset(&sessInput, 0, sizeof(XcalarApiSessionInfoInput));

    switch (scope->specifier_case()) {
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kGlobl:
        xSyslog(ModuleName,
                XlogDebug,
                "initUdfContainerFromScope: global scope");
        break;
    case xcalar::compute::localtypes::Workbook::WorkbookScope::kWorkbook: {
        auto workbookSpec = scope->workbook();
        status =
            userMgr_->getUserAndSessInput(&workbookSpec, &user, &sessInput);
        BailIfFailed(status);
        xSyslog(ModuleName,
                XlogDebug,
                "initUdfContainerFromScope: local scope: "
                "user %s, session %s,%lX",
                user.userIdName,
                sessInput.sessionName,
                sessInput.sessionId);
        break;
    }
    default:
        status = StatusInval;
        goto CommonExit;
    }
    status = initUdfContainer(udfContainer, &user, &sessInput, NULL);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
UserDefinedFunction::cleanoutHelper(char *moduleName,
                                    uint64_t consistentVersion,
                                    uint64_t nextVersion,
                                    XcalarApiUdfContainer *udfContainer)
{
    UdfOperation::XcalarApiUdfDeleteInputVersion *udfGvmPayloadTmp = NULL;
    Gvm::Payload *gPayload = NULL;
    Status status = StatusOk;
    Status *nodeStatus = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    char fullyQualName[LibNsTypes::MaxPathNameLen];
    char *fullyQNameDup = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    status = getUdfName(fullyQualName,
                        sizeof(fullyQualName),
                        moduleName,
                        udfContainer,
                        false);
    BailIfFailed(status);

    gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) +
        sizeof(UdfOperation::XcalarApiUdfDeleteInputVersion));
    if (gPayload == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Cleanout Scalar Function %s consistent version %lu next "
                "version %lu"
                " failed: %s",
                moduleName,
                consistentVersion,
                nextVersion,
                strGetFromStatus(status));
        goto CommonExit;
    }

    udfGvmPayloadTmp =
        (UdfOperation::XcalarApiUdfDeleteInputVersion *) gPayload->buf;
    udfGvmPayloadTmp->consistentVersion = consistentVersion;
    udfGvmPayloadTmp->nextVersion = nextVersion;

    if (strchr(moduleName, '/') == NULL) {
        fullyQNameDup = strAllocAndCopy(fullyQualName);
        BailIfNull(fullyQNameDup);
        strlcpy(udfGvmPayloadTmp->modulePrefix,
                dirname(fullyQNameDup),
                sizeof(udfGvmPayloadTmp->modulePrefix));
    } else {
        // allow absolute name; so no prefix for UdfLocal
        // see similar code and comments for this in addUdfInternal
        udfGvmPayloadTmp->modulePrefix[0] = '\0';
    }

    snprintf(udfGvmPayloadTmp->deleteInput.moduleName,
             sizeof(udfGvmPayloadTmp->deleteInput.moduleName),
             "%s",
             moduleName);

    gPayload->init(udfOperation_->getGvmIndex(),
                   (uint32_t) UdfOperation::Action::Delete,
                   sizeof(UdfOperation::XcalarApiUdfDeleteInputVersion));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Delete Scalar Function %s failed on GVM invoke: %s",
                moduleName,
                strGetFromStatus(status));
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (fullyQNameDup != NULL) {
        memFree(fullyQNameDup);
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
}

void
UserDefinedFunction::setFinishedRestoring(bool newValue)
{
    udfPersist_->setFinishedRestoring(newValue);
}

Status
UserDefinedFunction::copyUdfToWorkbook(const char *fullyQName,
                                       XcalarApiUdfContainer *udfContainer)
{
    Status status = StatusOk;
    char fullUdfName[XcalarApiMaxUdfModuleNameLen + 1];
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    XcalarApiUdfGetInput udfGetInput;
    UdfModuleSrc *udfModuleSrc;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize;
    Status status2;
    XcalarApiOutput *getUdfOutput2 = NULL;
    size_t getUdfOutputSize2;
    UdfModuleSrc *udfModuleSrc2;

    if (strncmp(fullyQName, DefaultModuleName, strlen(DefaultModuleName)) ==
        0) {
        goto CommonExit;
    }

    // Copy the Scalar Function name to a local buffer as parseFunctionName
    // modifies it.
    assert(fullyQName[0] == '/');
    strlcpy(fullUdfName, fullyQName, sizeof(fullUdfName));

    status = parseFunctionName(fullUdfName,
                               &udfModuleName,
                               &udfModuleVersion,
                               &udfFunctionName);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse workbook '%s' function '%s' for "
                "user '%s': %s",
                udfContainer->sessionInfo.sessionName,
                fullyQName,
                udfContainer->userId.userIdName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    strlcpy(udfGetInput.moduleName,
            udfModuleName,
            sizeof(udfGetInput.moduleName));

    status = getUdf(&udfGetInput, NULL, &getUdfOutput, &getUdfOutputSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to get Scalar Function source for '%s': %s",
                udfModuleName,
                strGetFromStatus(status));
        goto CommonExit;
    }
    udfModuleSrc = &getUdfOutput->outputResult.udfGetOutput;

    status =
        addUdf(udfModuleSrc, udfContainer, &addUdfOutput, &addUdfOutputSize);
    if (status != StatusOk) {
        if (status == StatusUdfModuleAlreadyExists) {
            // There's already a UDF module in the workbook with the same
            // name.  Compare it to see if it is the same UDF.

            // Convert fully qualified name to relative.
            char *moduleNameCopy = strAllocAndCopy(udfGetInput.moduleName);
            BailIfNull(moduleNameCopy);
            strlcpy(udfGetInput.moduleName,
                    basename(moduleNameCopy),
                    sizeof(udfGetInput.moduleName));
            memFree(moduleNameCopy);
            moduleNameCopy = NULL;

            status2 = getUdf(&udfGetInput,
                             udfContainer,
                             &getUdfOutput2,
                             &getUdfOutputSize2);
            if (status2 == StatusOk) {
                udfModuleSrc2 = &getUdfOutput2->outputResult.udfGetOutput;
                if (getUdfOutputSize == getUdfOutputSize2 &&
                    strlen(udfModuleSrc->source) ==
                        strlen(udfModuleSrc2->source) &&
                    strcmp(udfModuleSrc->source, udfModuleSrc2->source) == 0) {
                    // Same UDF!!!
                    status = StatusOk;
                } else {
                    // Different UDF
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed to copy Scalar Function '%s' into workbook "
                            "'%s' for "
                            "user '%s as a Scalar Function with the same name "
                            "already "
                            "exists in the workbook",
                            udfModuleName,
                            udfContainer->sessionInfo.sessionName,
                            udfContainer->userId.userIdName);
                    goto CommonExit;
                }
            } else {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to determine Scalar Function '%s' for workbook "
                        "'%s' for "
                        "user '%s' is a duplicate: %s",
                        udfModuleName,
                        udfContainer->sessionInfo.sessionName,
                        udfContainer->userId.userIdName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to add source for Scalar Function '%s' for "
                    "workbook '%s' for "
                    "user '%s': %s",
                    udfModuleName,
                    udfContainer->sessionInfo.sessionName,
                    udfContainer->userId.userIdName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:

    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    if (getUdfOutput2 != NULL) {
        memFree(getUdfOutput2);
        getUdfOutput2 = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }

    return status;
}

Status
UserDefinedFunction::deleteWorkbookDirectory(const char *userName,
                                             const uint64_t sessionId)
{
    return udfPersist_->deleteWorkbookDirectory(userName, sessionId);
}

// XXX: upgradeToDionysus flag may not be needed - use false for now.
// Decision TBD until upgrade work's done
void
UserDefinedFunction::processUdfsDir(DIR *udfDir,
                                    char *dirPath,
                                    size_t maxDirNameLen,
                                    XcalarApiUdfContainer *udfContainer,
                                    bool upgradeToDionysus)
{
    udfPersist_->processUdfsDir(udfDir,
                                dirPath,
                                maxDirNameLen,
                                udfContainer,
                                upgradeToDionysus);
}

// XXX Udf Container needs to be a richer object the tells exactly what the
// container was created for. For now, this should do.
bool
UserDefinedFunction::containerForDataflows(
    const XcalarApiUdfContainer *udfContainer)
{
    if (udfContainer->retinaName[0] != '\0') {
        return true;
    } else {
        return false;
    }
}

bool
UserDefinedFunction::containerWithWbScope(
    const XcalarApiUdfContainer *udfContainer)
{
    if (udfContainer->userId.userIdName[0] != '\0' &&
        udfContainer->sessionInfo.sessionName[0] != '\0') {
        return true;
    } else {
        return false;
    }
}

// XXX Rest of the fields in the XcalarApiUdfContainer are dead fields.
bool
UserDefinedFunction::containersMatch(const XcalarApiUdfContainer *udfContainer1,
                                     const XcalarApiUdfContainer *udfContainer2)
{
    // Note that sessionName can change due to a rename, but the sessionId
    // remains the same from it's creation time.
    if (strncmp(udfContainer1->userId.userIdName,
                udfContainer2->userId.userIdName,
                sizeof(udfContainer1->userId.userIdName)) ||
        strncmp(udfContainer1->retinaName,
                udfContainer2->retinaName,
                sizeof(udfContainer1->retinaName)) ||
        udfContainer1->sessionInfo.sessionId !=
            udfContainer2->sessionInfo.sessionId) {
        return false;
    } else {
        return true;
    }
}

// XXX Rest of the fields in the XcalarApiUdfContainer are dead fields.
void
UserDefinedFunction::copyContainers(
    XcalarApiUdfContainer *dstUdfContainer,
    const XcalarApiUdfContainer *srcUdfContainer)
{
    memcpy(dstUdfContainer, srcUdfContainer, sizeof(XcalarApiUdfContainer));
}

// Copies the specified UDF parser to the shared UDF space as the resultant
// dataset resides in global space.
//
// Note that multiple datasets can use the same sUDF so we cannot automatically
// remove it from shared space (at least currently).
Status
UserDefinedFunction::copyUdfParserToSharedSpace(const char *parserFnName,
                                                char **relativeNameOut)
{
    Status status = StatusOk;

    char parserFnNameCopy[UdfVersionedFQFname];
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    char fullQualName[LibNsTypes::MaxPathNameLen + 1];
    XcalarApiUdfContainer sharedUdfContainer;
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize;
    UdfModuleSrc *moduleSrc = NULL;
    char *moduleNameDup = NULL;
    const char *modNameBase = NULL;
    XcalarApiOutput *getUdfOutput2 = NULL;
    size_t getUdfOutputSize2;
    UdfModuleSrc *moduleSrc2 = NULL;
    char *relativeName = NULL;

    if (parserFnName[0] == '\0' ||
        strncmp(parserFnName,
                UserDefinedFunction::SharedUDFsDirPath,
                strlen(UserDefinedFunction::SharedUDFsDirPath)) == 0 ||
        strncmp(parserFnName,
                UserDefinedFunction::DefaultModuleName,
                strlen(UserDefinedFunction::DefaultModuleName)) == 0) {
        // No parser or already in shared space or uses the default...nothing to
        // do
        goto CommonExit;
    }

    // Must have an full path to the UDF as there's no session
    // associated with this operation (and thus no Udf container).
    if (parserFnName[0] != '/') {
        status = StatusUdfModuleFullNameRequired;
        goto CommonExit;
    }

    // Init Udf container for shared UDF space.
    memset(&sharedUdfContainer, 0, sizeof(sharedUdfContainer));

    // Copy parser name as parseFunctionName modifies what is passed to it.
    strlcpy(parserFnNameCopy, parserFnName, sizeof(parserFnNameCopy));
    status = UserDefinedFunction::parseFunctionName(parserFnNameCopy,
                                                    &udfModuleName,
                                                    &udfModuleVersion,
                                                    &udfFunctionName);
    BailIfFailed(status);

    // Get the UDF module.  As a full path name has been specified no Udf
    // container is needed.
    strlcpy(udfGetInput.moduleName,
            udfModuleName,
            sizeof(udfGetInput.moduleName));
    status = getUdf(&udfGetInput, NULL, &getUdfOutput, &getUdfOutputSize);
    BailIfFailed(status);

    moduleSrc = &getUdfOutput->outputResult.udfGetOutput;

    // The UDF module is a full path name.  We want just relative module
    // name.
    moduleNameDup = strAllocAndCopy(udfModuleName);
    BailIfNull(moduleNameDup);
    modNameBase = basename(moduleNameDup);

    // Create the relative name which will be passed back to the caller.
    relativeName = (char *) memAllocExt(UdfVersionedFQFname, ModuleName);
    BailIfNull(relativeName);

    verify(snprintf(relativeName,
                    UdfVersionedFQFname,
                    "%s:%s",
                    modNameBase,
                    udfFunctionName) < (int) UdfVersionedFQFname);

    // Generate fully qualified UDF name for the shared UDF space.
    status = getUdfName(fullQualName,
                        sizeof(fullQualName),
                        (char *) &modNameBase,
                        &sharedUdfContainer,
                        false);
    BailIfFailed(status);

    // Add the UDF to the shared UDF space
    status = addUdf(moduleSrc,
                    &sharedUdfContainer,
                    &addUdfOutput,
                    &addUdfOutputSize);
    if (status != StatusOk) {
        if (status == StatusUdfModuleAlreadyExists) {
            Status status2;
            // See if the existing UDF matches the one we're adding.
            strlcpy(udfGetInput.moduleName,
                    modNameBase,
                    sizeof(udfGetInput.moduleName));
            status2 = getUdf(&udfGetInput,
                             &sharedUdfContainer,
                             &getUdfOutput2,
                             &getUdfOutputSize2);
            if (status2 == StatusOk) {
                moduleSrc2 = &getUdfOutput2->outputResult.udfGetOutput;
                if (getUdfOutputSize == getUdfOutputSize2 &&
                    strlen(moduleSrc->source) == strlen(moduleSrc2->source) &&
                    strcmp(moduleSrc->source, moduleSrc2->source) == 0) {
                    // Same UDF!!!
                    status = StatusOk;
                } else {
                    // Different UDF
                    xSyslogTxnBuf(ModuleName,
                                  XlogErr,
                                  "Failed to copy Scalar Function '%s' into "
                                  "shared Scalar Function "
                                  "space as a Scalar Function with the same "
                                  "name already "
                                  "exists",
                                  modNameBase);
                    goto CommonExit;
                }
            }
        } else {
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Failed to add Scalar Function '%s' to shared space: "
                          "%s",
                          modNameBase,
                          strGetFromStatus(status));
            goto CommonExit;
        }
    }

    *relativeNameOut = relativeName;
    relativeName = NULL;  // Caller must free

CommonExit:

    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    if (moduleNameDup != NULL) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    if (getUdfOutput2 != NULL) {
        memFree(getUdfOutput2);
        getUdfOutput2 = NULL;
    }
    if (relativeName != NULL) {
        memFree(relativeName);
        relativeName = NULL;
    }

    return status;
}

Status
UserDefinedFunction::copyUdfExportToSharedSpace(const char *exportFnName)
{
    Status status = StatusOk;

    char exportFnNameCopy[UdfVersionedFQFname];
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    XcalarApiUdfContainer sharedUdfContainer;
    XcalarApiUdfGetInput udfGetInput;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize;
    UdfModuleSrc *moduleSrc = NULL;
    char *newModuleName = NULL;

    UserMgr *userMgr = UserMgr::get();
    XcalarApiUserId user;
    XcalarApiOutput *sessionListOutputOut = NULL;
    XcalarApiSessionListOutput *sessionListOutput = NULL;
    size_t sessionListOutputSize = 0;
    char fullExportFnName[XcalarApiMaxUdfModuleNameLen];
    uint64_t sessionId = 0;

    // The UDF in export target only has a relative name. The target was likely
    // to be created before Chronos. During Artemis to Chronos upgrade UDFs were
    // copied to the user admin' space, so we are going to find this UDF in one
    // of admin's workbooks.
    if (exportFnName[0] != '/') {
        strlcpy(user.userIdName, AdminUserName, sizeof(user.userIdName));
        user.userIdUnique = 0;
        status = userMgr->list(&user,
                               "*",
                               &sessionListOutputOut,
                               &sessionListOutputSize);
        BailIfFailed(status);

        sessionListOutput =
            &sessionListOutputOut->outputResult.sessionListOutput;
        for (size_t jj = 0; jj < sessionListOutput->numSessions; jj++) {
            sessionId = sessionListOutput->sessions[jj].sessionId;
            snprintf(fullExportFnName,
                     sizeof(fullExportFnName),
                     "%s%s/%lX/%s/%s",
                     UdfWorkBookPrefix,
                     AdminUserName,
                     sessionId,
                     UdfWorkBookDir,
                     exportFnName);

            status = parseFunctionName(fullExportFnName,
                                       &udfModuleName,
                                       &udfModuleVersion,
                                       &udfFunctionName);
            BailIfFailed(status);

            // Get the UDF module. As a full path name has been specified no
            // Udf container is needed.
            strlcpy(udfGetInput.moduleName,
                    udfModuleName,
                    sizeof(udfGetInput.moduleName));
            status =
                getUdf(&udfGetInput, NULL, &getUdfOutput, &getUdfOutputSize);

            if (status == StatusOk) {
                break;
            }
        }
    } else {
        // Copy parser name as parseFunctionName modifies what is passed to it.
        strlcpy(exportFnNameCopy, exportFnName, sizeof(exportFnNameCopy));
        status = parseFunctionName(exportFnNameCopy,
                                   &udfModuleName,
                                   &udfModuleVersion,
                                   &udfFunctionName);
        BailIfFailed(status);

        // Get the UDF module. As a full path name has been specified no Udf
        // container is needed.
        strlcpy(udfGetInput.moduleName,
                udfModuleName,
                sizeof(udfGetInput.moduleName));
        status = getUdf(&udfGetInput, NULL, &getUdfOutput, &getUdfOutputSize);
    }

    BailIfFailed(status);

    moduleSrc = &getUdfOutput->outputResult.udfGetOutput;

    status = generateUserWorkbookUdfName(exportFnName, &newModuleName, false);
    BailIfFailed(status);

    strlcpy(moduleSrc->moduleName,
            newModuleName,
            sizeof(moduleSrc->moduleName));

    // Init Udf container for shared UDF space.
    memset(&sharedUdfContainer, 0, sizeof(sharedUdfContainer));

    // Add the UDF to the shared UDF space
    status = addUdf(moduleSrc,
                    &sharedUdfContainer,
                    &addUdfOutput,
                    &addUdfOutputSize);
    if (status != StatusOk) {
        // We copy UDFs to shared space using names with special characters.
        // Users aren't allowed to use these characters so the UDFs with the
        // same name could only be from here. Since we generate different names
        // for different user and workbooks, if two new UDFs have the same name,
        // they must have been copied from the same source.
        if (status == StatusUdfModuleAlreadyExists) {
            status = StatusOk;
        } else {
            xSyslogTxnBuf(ModuleName,
                          XlogErr,
                          "Failed to add Scalar Function '%s' to shared space: "
                          "%s",
                          newModuleName,
                          strGetFromStatus(status));
            goto CommonExit;
        }
    }

CommonExit:
    if (sessionListOutputOut) {
        memFree(sessionListOutputOut);
        sessionListOutputOut = NULL;
    }

    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    if (newModuleName != NULL) {
        memFree(newModuleName);
        newModuleName = NULL;
    }

    return status;
}

// Prefix a udf's name with a retina's name with hyphen delimiter (so, it'd be
// "<retinaName>-<udfName>") after replacing any UdfVersionDelim chars in the
// retinaName (there can be only one instance of the UdfVersionDelim char in
// the new name). Return the prefixed name in memory allocated by this routine,
// via "newUdfName".
//
// Caller must free the memory returned via "newUdfName".
//
Status
UserDefinedFunction::prefixUdfNameWithRetina(const char *retinaName,
                                             char *udfName,
                                             char **newUdfName)
{
    Status status = StatusUnknown;
    char fixedRetinaName[XcalarApiMaxTableNameLen + 1];
    uint64_t jj = 0;
    char *prefixedModuleName = NULL;

    assert(newUdfName != NULL);
    *newUdfName = NULL;

    prefixedModuleName = (char *) memAlloc(XcalarApiMaxUdfModuleNameLen + 1);
    BailIfNull(prefixedModuleName);

    strlcpy(fixedRetinaName, retinaName, sizeof(fixedRetinaName));

    // The UDF's name can't have more than one UdfVersionDelim[0] occurrence
    // and since a retina's name may have this delimiter, make sure it's
    // changed to something different (the definition of UdfRetinaPrefixDelim
    // must be different from UdfVersionDelim).
    for (jj = 0; jj < strlen(fixedRetinaName); jj++) {
        if (fixedRetinaName[jj] == UserDefinedFunction::UdfVersionDelim[0]) {
            fixedRetinaName[jj] = UserDefinedFunction::UdfRetinaPrefixDelim[0];
        }
    }

    // Now prefix the UDF name with the retina's name (fixed
    // if needed above)

    snprintf(prefixedModuleName,
             XcalarApiMaxUdfModuleNameLen + 1,
             "%s-%s",
             fixedRetinaName,
             udfName);

    *newUdfName = prefixedModuleName;  // caller must free

    status = StatusOk;
CommonExit:
    return status;
}

Status
UserDefinedFunction::generateUserWorkbookUdfName(const char *udfName,
                                                 char **newUdfName,
                                                 bool isFunction)
{
    Status status = StatusOk;
    *newUdfName = (char *) memAlloc(XcalarApiMaxUdfModuleNameLen + 1);
    size_t i = 0;
    size_t j = 0;

    if (strlen(udfName) > XcalarApiMaxUdfModuleNameLen) {
        status = StatusUdfModuleInvalidName;
        goto CommonExit;
    }

    if (strncmp(udfName, UdfWorkBookPrefix, strlen(UdfWorkBookPrefix)) != 0) {
        // For relative path, we insert the admin name to the dst str.
        snprintf(*newUdfName,
                 XcalarApiMaxUdfModuleNameLen + 1,
                 "%s%s",
                 AdminUserName,
                 UdfRetinaPrefixDelim);
        j = strlen(AdminUserName) + 1;
    } else {
        // For absolute path, we skip the workbook prefix of the src str, (so it
        // begins with a user name).
        i = strlen(UdfWorkBookPrefix);
    }

    while (i < strlen(udfName) && (isFunction || udfName[i] != ModuleDelim)) {
        if (!isalnum(udfName[i]) && !isalpha(udfName[i]) && udfName[i] != '-' &&
            udfName[i] != '_' && udfName[i] != ModuleDelim &&
            udfName[i] != UdfRetinaPrefixDelim[0]) {
            (*newUdfName)[j] = UdfRetinaPrefixDelim[0];
        } else {
            (*newUdfName)[j] = udfName[i];
        }

        ++i;
        ++j;
    }

    (*newUdfName)[j] = '\0';

CommonExit:
    return status;
}

// Determine if the UDF module name exists in the namespace (meaning it is
// useable). The specified name may or may not contain the function name.
bool
UserDefinedFunction::udfModuleExistsInNamespace(const char *fullyQualName)
{
    Status status = StatusOk;
    bool found = false;
    char *fullyQualNameDup = NULL;
    char *modDelimChar;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle handle;

    fullyQualNameDup = strAllocAndCopy(fullyQualName);
    BailIfNull(fullyQualNameDup);

    modDelimChar = strchr(fullyQualNameDup, ModuleDelim);
    if (modDelimChar != NULL) {
        // Null terminate the module name as we don't need the function name.
        *modDelimChar = '\0';
    }
    handle = libNs->open(fullyQualNameDup, LibNsTypes::ReaderShared, &status);
    BailIfFailed(status);

    status = libNs->close(handle, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to close '%s': %s",
                fullyQualNameDup,
                strGetFromStatus(status));
        goto CommonExit;
    }

    found = true;

CommonExit:
    if (fullyQualNameDup != NULL) {
        memFree(fullyQualNameDup);
        fullyQualNameDup = NULL;
    }

    return found;
}

// NOTE:
//
// See large comment in updateUdf() about atomicity of on-disk UDF update and
// update of the UDF consistent version in libNs (either both must fail or both
// must succeed). Therefore both operations are carried out on the node where
// the libNs object is allocated so that getRefToObject() can be used to
// retrieve the in-memory pointer to the libNs UdfRecord - updates to which are
// guaranteed to not fail given that it's an in-memory update (this as opposed
// to the use of LibNs::updateNsObject() which would do a 2-pc to the node,
// and thus could fail).
//
// For the atomicity requirement, the order of operations must be:
//
// 0. First, get libNs record pointer - if it fails bail out so both fail
//
// 1. Second, do the on-disk update to the UDF persistent store
//     1.1 if it fails, bail out so both fail
//     1.2 if it succeeds, update libNs in-core pointer (guaranteed to succeed
//     via use of getRefToObject()) - so both succeed

void
TwoPcMsg2pcAddOrUpdateUdfDlm1::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    UdfOperation::UdfUpdateDlmMsg *msg =
        (UdfOperation::UdfUpdateDlmMsg *) payload;
    LibNs *libNs = LibNs::get();
    LibNsTypes::NsHandle handle;
    UserDefinedFunction::UdfRecord *libNsUdfRecord;
    XcalarApiUdfContainer *udfContainer;
    UdfModuleSrc *input;

    handle = msg->udfLibNsHandle;
    udfContainer = &msg->udfContainer;
    input = &msg->udfInfo.addUpdateInput;

    libNsUdfRecord =
        (UserDefinedFunction::UdfRecord *) libNs->getRefToObject(handle,
                                                                 &status);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Scalar Function %s/%s update commit failed on getRefToObject "
                "NS: %s",
                msg->udfInfo.modulePrefix,
                input->moduleName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (UserDefinedFunction::get()->udfPersist_->needsPersist(input,
                                                              udfContainer)) {
        status =
            UserDefinedFunction::get()->udfPersist_->update(input->moduleName,
                                                            udfContainer,
                                                            input->type,
                                                            input->source);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Scalar Function %s/%s update commit failed write: %s",
                    msg->udfInfo.modulePrefix,
                    input->moduleName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (msg->action == UdfOperation::Action::Add) {
        // This is addUdf() so consistent version must be StartVersion
        assert(msg->udfInfo.consistentVersion ==
               UserDefinedFunction::UdfRecord::StartVersion);
        libNsUdfRecord->isCreated_ = true;
        xSyslog(ModuleName,
                XlogInfo,
                "Scalar Function %s/%s created with version %ld",
                msg->udfInfo.modulePrefix,
                input->moduleName,
                UserDefinedFunction::UdfRecord::StartVersion);
    } else {
        // This is for updateUdf()
        assert(msg->action == UdfOperation::Action::Update);
        assert(libNsUdfRecord->isCreated_ == true);

        xSyslog(ModuleName,
                XlogInfo,
                "Scalar Function %s/%s update version change %ld to %ld",
                msg->udfInfo.modulePrefix,
                input->moduleName,
                libNsUdfRecord->consistentVersion_,
                msg->udfInfo.nextVersion);

        libNsUdfRecord->consistentVersion_ = msg->udfInfo.nextVersion;
        // libNsUdfRecord->nextVersion should already have been updated
        // in issuer of 2pc via call to updateNsObject
        assert(libNsUdfRecord->nextVersion_ >
               libNsUdfRecord->consistentVersion_);
    }

CommonExit:
    eph->setAckInfo(status, 0);
}

void
TwoPcMsg2pcAddOrUpdateUdfDlm1::schedLocalCompletion(MsgEphemeral *eph,
                                                    void *payload)
{
    UdfOperation::UdfUpdateDlmMsg *msg =
        (UdfOperation::UdfUpdateDlmMsg *) eph->ephemeral;

    msg->status = eph->status;
}

void
TwoPcMsg2pcAddOrUpdateUdfDlm1::recvDataCompletion(MsgEphemeral *eph,
                                                  void *payload)
{
    schedLocalCompletion(eph, payload);
}
