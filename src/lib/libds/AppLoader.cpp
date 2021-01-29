// Copyright 2016-2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <jansson.h>

#include "StrlFunc.h"
#include "dataset/BackingData.h"
#include "dataset/AppLoader.h"
#include "dataset/Dataset.h"
#include "app/AppMgr.h"
#include "primitives/Primitives.h"
#include "udf/UserDefinedFunction.h"
#include "util/MemTrack.h"
#include "df/DataFormat.h"
#include "runtime/Runtime.h"
#include "strings/String.h"
#include "app/AppMgr.h"
#include "datapage/DataPage.h"
#include "datapage/DataPageIndex.h"
#include "sys/XLog.h"
#include "operators/Operators.h"
#include "util/Base64.h"

const char *SingleFileExtensions[6] =
    {".gz", ".json", ".csv", ".tar", ".zip", ".parq"};

bool
isSingleFile(const char *path)
{
    for (unsigned ii = 0; ii < ArrayLen(SingleFileExtensions); ii++) {
        const char *ext = SingleFileExtensions[ii];

        if (strlen(path) > strlen(ext)) {
            const char *pathExt = path + strlen(path) - strlen(ext);
            if (strcmp(pathExt, ext) == 0) {
                return true;
            }
        }
    }

    return false;
}

// Struct to handle char* cleanups.
struct BuffHolder {
    BuffHolder() { buff = nullptr; }
    ~BuffHolder()
    {
        if (buff != nullptr) {
            memFree(buff);
            buff = nullptr;
        }
    }
    char *buff;
};

Status
AppLoader::loadDataset(const DfLoadArgs *loadArgs,
                       const XcalarApiUdfContainer *udfContainer,
                       OpStatus *opStatus,
                       XcalarApiBulkLoadOutput *loadOutput,
                       bool *retAppInternalError)
{
    Status status;
    const char *loadAppName;
    char *inBlob = NULL;
    App *loadApp = NULL;
    char *udfOnDiskPath = NULL;
    char *udfSource = NULL;
    LibNsTypes::NsHandle handle;
    XcalarApiUserId *user;
    char fullUdfName[sizeof(loadArgs->parseArgs.parserFnName)];
    const char *udfFunctionName = NULL;
    DfLoadArgs *loadArgsCopy = NULL;
    bool xdbLoad = loadArgs->xdbLoadArgs.keyName[0] != '\0';

    *retAppInternalError = false;
    assert(loadArgs);

    loadArgsCopy = (DfLoadArgs *) memAlloc(sizeof(DfLoadArgs));
    BailIfNull(loadArgsCopy);
    memcpy(loadArgsCopy, loadArgs, sizeof(DfLoadArgs));

    // XXX: TODO: this is a hack for detecting single file loads, in the future
    // we would want to spawn 1 XPU by default to check the path, then spawn as
    // many XPUs as we need in order to perform the load
    if (loadArgs->sourceArgsListCount == 1 &&
        isSingleFile(loadArgs->sourceArgsList[0].path)) {
        loadAppName = AppMgr::SingleFileLoadAppName;
    } else {
        loadAppName = AppMgr::LoadAppName;
    }

    user = (XcalarApiUserId *) &udfContainer->userId;

    status = Dataset::get()->updateLoadStatus(dataset_->name_,
                                              dsRefHandle_.nsHandle,
                                              DsLoadStatusInProgress);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed load app %s for dataset %s: %s",
                      loadAppName,
                      dataset_->name_,
                      strGetFromStatus(status));
        goto CommonExit;
    }

    loadApp = AppMgr::get()->openAppHandle(loadAppName, &handle);
    if (loadApp == NULL) {
        status = StatusLoadAppNotExist;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load app %s does not exist",
                      loadAppName);
        goto CommonExit;
    }

    if (loadArgs->parseArgs.parserFnName[0] != '\0') {
        status =
            Operators::convertLoadUdfToAbsolutePath(loadArgsCopy,
                                                    (XcalarApiUdfContainer *)
                                                        udfContainer);
        BailIfFailed(status);

        // By now, parserFnName has been populated with fully qualified
        // libNsPath to the UDF; get the on-disk path to it so load.py can just
        // import the module directly instead of compiling from the source
        // code (which will need to be obtained using latest libNs consistent
        // version). Since the on-disk path is always up-to-date and matches
        // the consistent version, it's safe to just import the module from disk
        status = getUdfOnDiskPath(loadArgsCopy->parseArgs.parserFnName,
                                  &udfOnDiskPath);
        BailIfFailed(status);
        xSyslog(ModuleName,
                XlogDebug,
                "UDF path '%s' converted to '%s'",
                loadArgs->parseArgs.parserFnName,
                udfOnDiskPath);
        if (udfOnDiskPath == NULL) {
            // this udf must be from a published table retina
            const char *udfModuleName;
            const char *udfModuleVersion;
            strlcpy(fullUdfName,
                    loadArgsCopy->parseArgs.parserFnName,
                    sizeof(fullUdfName));
            status = UserDefinedFunction::parseFunctionName(fullUdfName,
                                                            &udfModuleName,
                                                            &udfModuleVersion,
                                                            &udfFunctionName);
            BailIfFailed(status);
            status = getUdfSrc(udfModuleName,
                               (XcalarApiUdfContainer *) udfContainer,
                               &udfSource);
            BailIfFailed(status);
            assert(udfSource);
        }
    }

    assert(loadApp);
    // NOTE: here there are two scenarios:
    // A: udfOnDiskPath is non-NULL, and udfSource is NULL (primary scenario)
    // B: udfOnDiskPath is NULL, and udfSource is not NULL
    //
    // If udfOnDiskPath is non-NULL, load.py will import the UDF directly from
    // the on-disk path (which allows recursive imports from Xcalar UDF
    // namespace).
    //
    // If for whatever reason, udfOnDiskPath is NULL, then the UDF
    // source is sent down, and this is compiled and executed by load.py
    //
    // NOTE: if we ever see issues with import from disk (see ENG-657), then
    // one can revert to always sending down python source here
    //
    status = jsonifyLoadInput(loadArgs, udfOnDiskPath, udfSource, &inBlob);
    BailIfFailed(status);
    assert(inBlob != NULL);

    status = runLoadApp(loadApp,
                        user,
                        opStatus,
                        inBlob,
                        loadOutput,
                        retAppInternalError);
    BailIfFailed(status);

    if (loadOutput->errorString[0] != '\0') {
        xSyslog(ModuleName,
                XlogErr,
                "Encountered error with file '%s': %s",
                loadOutput->errorFile,
                loadOutput->errorString);
    }

    opStatus->atomicOpDetails.numWorkTotal = loadOutput->numBytes;
    opStatus->atomicOpDetails.numWorkCompleted = loadOutput->numBytes;

    xSyslog(ModuleName,
            XlogInfo,
            "Loaded %zu bytes across %zu files from %d sources, including "
            "%s at %s",
            loadOutput->numBytes,
            loadOutput->numFiles,
            loadArgs->sourceArgsListCount,
            loadArgs->sourceArgsList[0].targetName,
            loadArgs->sourceArgsList[0].path);
    if (!xdbLoad && (loadOutput->numFiles == 0 || loadOutput->numBytes == 0)) {
        // We found no 'files'
        status = StatusDsLoadFailed;
        if (loadOutput->errorString[0] == '\0') {
            snprintf(loadOutput->errorString,
                     sizeof(loadOutput->errorString),
                     "All found files are empty");
        }
        goto CommonExit;
    }

CommonExit:
    if (udfSource) {
        memFree(udfSource);
        udfSource = NULL;
    }
    if (udfOnDiskPath) {
        memFree(udfOnDiskPath);
        udfOnDiskPath = NULL;
    }
    if (inBlob) {
        memFree(inBlob);
        inBlob = NULL;
    }
    if (loadApp) {
        AppMgr::get()->closeAppHandle(loadApp, handle);
        loadApp = NULL;
    }
    if (loadArgsCopy) {
        memFree(loadArgsCopy);
        loadArgsCopy = NULL;
    }
    DsLoadStatus loadStatus;
    if (status == StatusCanceled) {
        loadStatus = DsLoadStatusCancelled;
    } else if (status != StatusOk) {
        loadStatus = DsLoadStatusFailed;
    } else {
        loadStatus = DsLoadStatusComplete;
    }

    {
        Status status2 = Dataset::get()->updateLoadStatus(dataset_->name_,
                                                          dsRefHandle_.nsHandle,
                                                          loadStatus);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed load app %s for dataset %s: %s",
                    loadAppName,
                    dataset_->name_,
                    strGetFromStatus(status2));
            if (status == StatusOk) {
                status = status2;
            }
        }
    }

    return status;
}

Status
AppLoader::getUdfSrc(const char *udfModuleName,
                     XcalarApiUdfContainer *udfContainer,
                     char **udfSource)
{
    Status status;
    XcalarApiUdfGetInput getUdfInput;
    UdfModuleSrc *udfModule;
    XcalarApiOutput *getUdfApiOutput = NULL;
    size_t outputSize;
    *udfSource = NULL;

    if (strlcpy(getUdfInput.moduleName,
                udfModuleName,
                sizeof(getUdfInput.moduleName)) >=
        sizeof(getUdfInput.moduleName)) {
        status = StatusNoBufs;
        goto CommonExit;
    }

    status = UserDefinedFunction::get()->getUdf(&getUdfInput,
                                                udfContainer,
                                                &getUdfApiOutput,
                                                &outputSize);
    BailIfFailed(status);
    udfModule = &getUdfApiOutput->outputResult.udfGetOutput;

    *udfSource = (char *) memAlloc(udfModule->sourceSize);
    BailIfNull(*udfSource);

    verify(strlcpy(*udfSource, udfModule->source, udfModule->sourceSize) <
           udfModule->sourceSize);

CommonExit:
    if (getUdfApiOutput) {
        memFree(getUdfApiOutput);
        getUdfApiOutput = NULL;
    }
    if (status != StatusOk) {
        if (*udfSource != NULL) {
            memFree(*udfSource);
            *udfSource = NULL;
        }
    }
    return status;
}

Status
AppLoader::getUdfOnDiskPath(const char *udfLibNsPath, char **udfOnDiskPath)
{
    Status status;

    status = UserDefinedFunction::get()->getUdfOnDiskPath(udfLibNsPath,
                                                          udfOnDiskPath);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
AppLoader::runLoadApp(App *loadApp,
                      const XcalarApiUserId *user,
                      OpStatus *opStatus,
                      const char *inObj,
                      XcalarApiBulkLoadOutput *loadOutput,
                      bool *retAppInternalError)
{
    Status status;
    AppGroup::Id appGroupId;
    char *outStr = NULL;
    char *errorStr = NULL;

    AppGroup::Scope scope = AppGroup::Scope::Global;
    unsigned clusterNumNodes = Config::get()->getActiveNodes();

    assert(loadApp);

    *retAppInternalError = false;
    status = AppMgr::get()->runMyAppAsync(loadApp,
                                          scope,
                                          user ? user->userIdName : "",
                                          0,
                                          inObj,
                                          datasetId_,
                                          &appGroupId,
                                          &errorStr,
                                          retAppInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load app \"%s\" failed to start with error:\"%s\","
                      " output:\"%s\"",
                      loadApp->getName(),
                      errorStr ? errorStr : "",
                      outStr ? outStr : "");

        Status status2 =
            parseLoadResults(outStr, errorStr, clusterNumNodes, loadOutput);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to parse output of load app %s(%s):"
                    " output:\"%s\" error:\"%s\"",
                    loadApp->getName(),
                    strGetFromStatus(status),
                    outStr ? outStr : "",
                    errorStr ? errorStr : "");
            // Note that parse error is just syslogged and is not the
            // primary error.
        }

        if (loadOutput->errorString[0] == '\0') {
            strlcpy(loadOutput->errorString,
                    strGetFromStatus(status),
                    sizeof(loadOutput->errorString));
        }
        goto CommonExit;
    }

    assert(appGroupId != XidInvalid);

    while (true) {
        *retAppInternalError = false;
        status = AppMgr::get()->waitForMyAppResult(appGroupId,
                                                   AppWaitTimeoutUsecs,
                                                   &outStr,
                                                   &errorStr,
                                                   retAppInternalError);
        if (status == StatusOk) {
            break;
        } else if (status == StatusTimedOut) {
            if (opStatus->atomicOpDetails.cancelled) {
                xSyslog(ModuleName,
                        XlogInfo,
                        "Load app \"%s\" aborted, since operation was"
                        " cancelled",
                        loadApp->getName());
                // Given the timeout and Operation/TXN has been cancelled in the
                // interim, let's abort the App and reap it's result.
                status = StatusCanceled;
                Status status2 =
                    AppMgr::get()->abortMyAppRun(appGroupId,
                                                 status,
                                                 retAppInternalError);
                if (status2 != StatusOk) {
                    // When App Abort fails, we should just leak the resources,
                    // since the undelying resources may just be in active use
                    // in the cluster and just a 2PC failed.
                    status = status2;
                    goto CommonExit;
                }
            }
            continue;
        } else {
            assert(status != StatusTimedOut);
            xSyslog(ModuleName,
                    XlogErr,
                    "Load app \"%s\" failed with error:\"%s\", output:\"%s\"",
                    loadApp->getName(),
                    errorStr ? errorStr : "",
                    outStr ? outStr : "");

            Status status2 =
                parseLoadResults(outStr, errorStr, clusterNumNodes, loadOutput);
            if (status2 != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed to parse output of load app %s(%s):"
                        " output:\"%s\" error:\"%s\"",
                        loadApp->getName(),
                        strGetFromStatus(status),
                        outStr ? outStr : "",
                        errorStr ? errorStr : "");
                // Note that parse error is just syslogged and is not the
                // primary error.
            }

            if (loadOutput->errorString[0] == '\0') {
                strlcpy(loadOutput->errorString,
                        strGetFromStatus(status),
                        sizeof(loadOutput->errorString));
            }
            goto CommonExit;
        }
    }

    assert(status == StatusOk);

    // Parse errors here mean we failed to determine the actual load output;
    // this is a fatal error
    status = parseLoadResults(outStr, errorStr, clusterNumNodes, loadOutput);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of load app %s(%s):"
                " output:\"%s\"error:\"%s\"",
                loadApp->getName(),
                strGetFromStatus(status),
                outStr ? outStr : "",
                errorStr ? errorStr : "");
        goto CommonExit;
    }

CommonExit:
    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errorStr) {
        memFree(errorStr);
        errorStr = NULL;
    }
    return status;
}

Status
AppLoader::jsonifyLoadInput(const DfLoadArgs *loadArgs,
                            const char *udfOnDiskPath,
                            const char *udfSource,
                            char **outStr)
{
    Status status = StatusOk;
    int ret;
    json_t *json = NULL;
    json_t *fieldsJson = NULL;
    json_t *fieldJson = NULL;
    json_t *fieldsRequiredJson = NULL;
    const size_t defaultMaxSize =
        XcalarConfig::get()->getMaxInteractiveDataSize();
    json_t *sourceArgsList = NULL;
    json_t *sourceArgs = NULL;
    const ParseArgs *parseArgs = &loadArgs->parseArgs;

    *outStr = NULL;
    fieldsJson = json_array();
    BailIfNull(fieldsJson);

    fieldsRequiredJson = json_array();
    BailIfNull(fieldsRequiredJson);

    for (int ii = 0; ii < (int) loadArgs->parseArgs.fieldNamesCount; ii++) {
        fieldJson = json_pack(
            "{"
            "s:s"  // oldName
            "s:s"  // fieldName
            "s:s"  // type
            "}",
            "oldName",
            loadArgs->parseArgs.oldNames[ii],
            "fieldName",
            loadArgs->parseArgs.fieldNames[ii],
            "type",
            strGetFromDfFieldType(loadArgs->parseArgs.types[ii]));
        BailIfNullWith(fieldJson, StatusJsonError);
        ret = json_array_append_new(fieldsJson, fieldJson);
        if (ret != 0) {
            status = StatusNoMem;
            goto CommonExit;
        }
        fieldJson = NULL;
    }

    for (int ii = 0; ii < (int) loadArgs->xdbLoadArgs.fieldNamesCount; ii++) {
        fieldJson = json_pack(
            "{"
            "s:s"  // required field name
            "s:s"  // required field type
            "}",
            "fieldName",
            loadArgs->xdbLoadArgs.fieldNames[ii],
            "fieldType",
            strGetFromDfFieldType(
                loadArgs->xdbLoadArgs.valueDesc.valueType[ii]));
        BailIfNullWith(fieldJson, StatusJsonError);

        ret = json_array_append_new(fieldsRequiredJson, fieldJson);
        if (ret != 0) {
            status = StatusNoMem;
            goto CommonExit;
        }
        fieldJson = NULL;
    }

    sourceArgsList = json_array();
    BailIfNull(sourceArgsList);

    for (int ii = 0; ii < loadArgs->sourceArgsListCount; ii++) {
        sourceArgs = jsonifySourceArgs(&loadArgs->sourceArgsList[ii]);
        BailIfNull(sourceArgs);

        ret = json_array_append_new(sourceArgsList, sourceArgs);
        if (ret != 0) {
            status = StatusNoMem;
            goto CommonExit;
        }
        sourceArgs = NULL;
    }

    json = json_pack(
        "{"
        "s:s,"   // func
        "s:o"    // sourceArgsList
        "s:{"    // parseArgs
        "s:s,"   // parserFnName
        "s:s?,"  // parserFnPath
        "s:s?,"  // parserFnSource
        "s:s,"   // parserArgJson
        "s:s,"   // fileNameFieldName
        "s:s,"   // recordNumFieldName
        "s:b,"   // allowFileErrors
        "s:b,"   // allowRecordErrors
        "s:o"    // fieldNames
        "s:o"    // fieldsRequired
        "s:s"    // eval string
        "},"
        "s:I,"  // sampleSize (64bit int, called json_int_t)
        "}",
        "func",
        "load",
        "sourceArgsList",
        sourceArgsList,
        "parseArgs",
        "parserFnName",
        parseArgs->parserFnName,
        "parserFnPath",
        udfOnDiskPath,
        "parserFnSource",
        udfSource,
        "parserArgJson",
        parseArgs->parserArgJson,
        "fileNameFieldName",
        parseArgs->fileNameFieldName,
        "recordNumFieldName",
        parseArgs->recordNumFieldName,
        "allowFileErrors",
        parseArgs->allowFileErrors,
        "allowRecordErrors",
        parseArgs->allowRecordErrors,
        "fields",
        fieldsJson,
        "fieldsRequired",
        fieldsRequiredJson,
        "evalString",
        loadArgs->xdbLoadArgs.evalString,
        "sampleSize",
        loadArgs->maxSize ? loadArgs->maxSize : defaultMaxSize);
    if (json == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Unknown error json formatting load input");
        status = StatusInval;
        goto CommonExit;
    }
    fieldsJson = NULL;          // Stolen by the above
    sourceArgsList = NULL;      // Stolen by the above
    fieldsRequiredJson = NULL;  // Stolen by the above

    *outStr = json_dumps(json, 0);
    BailIfNull(*outStr);

CommonExit:
    if (json != NULL) {
        json_decref(json);
        json = NULL;
    }
    if (sourceArgs != NULL) {
        json_decref(sourceArgs);
        sourceArgs = NULL;
    }
    if (sourceArgsList != NULL) {
        json_decref(sourceArgsList);
        sourceArgsList = NULL;
    }
    if (fieldsJson != NULL) {
        json_decref(fieldsJson);
        fieldsJson = NULL;
    }

    if (fieldsRequiredJson != NULL) {
        json_decref(fieldsRequiredJson);
        fieldsRequiredJson = NULL;
    }

    if (fieldJson != NULL) {
        json_decref(fieldJson);
        fieldJson = NULL;
    }
    if (status != StatusOk) {
        if (*outStr) {
            memFree(*outStr);
            *outStr = NULL;
        }
    }

    return status;
}

Status
AppLoader::parseLoadResults(const char *appOut,
                            const char *appErr,
                            unsigned clusterNumNodes,
                            XcalarApiBulkLoadOutput *loadOutput)
{
    Status status;
    size_t numBuffersIgnored = 0;

    status = parseLoadOutStr(appOut,
                             clusterNumNodes,
                             &numBuffersIgnored,
                             &loadOutput->numBytes,
                             &loadOutput->numFiles);
    BailIfFailed(status);

    status = parseLoadErrStr(appErr,
                             clusterNumNodes,
                             loadOutput->errorString,
                             sizeof(loadOutput->errorString),
                             loadOutput->errorFile,
                             sizeof(loadOutput->errorFile));
    BailIfFailed(status);
CommonExit:
    return status;
}

// Parses a string formatted as a series of:
// As defined in the load app
Status
AppLoader::parseLoadOutStr(const char *appOutBlob,
                           unsigned clusterNumNodes,
                           size_t *numBuffers,
                           size_t *numBytes,
                           size_t *numFiles)
{
    Status status = StatusOk;
    size_t numNodes;
    json_t *outTotalJson = NULL;
    json_error_t jsonError;
    json_t *appOutJson = NULL;
    *numBuffers = 0;
    *numBytes = 0;
    *numFiles = 0;

    if (appOutBlob == NULL) {
        goto CommonExit;
    }

    outTotalJson = json_loads(appOutBlob, 0, &jsonError);
    if (outTotalJson == NULL) {
        // The format of this string is dictated by the AppGroup, and thus
        // can be guaranteed to be parseable. However, it's possible to
        // encounter a memory error here, so we still have to handle errors
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output '%s' of load output: %s",
                appOutBlob,
                jsonError.text);
        goto CommonExit;
    }

    assert(json_typeof(outTotalJson) == JSON_ARRAY);

    numNodes = json_array_size(outTotalJson);
    if (numNodes != clusterNumNodes) {
        status = StatusAppOutParseFail;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "App found %zu nodes worth of output; expecting %u",
                      numNodes,
                      clusterNumNodes);
        goto CommonExit;
    }

    for (size_t ii = 0; ii < numNodes; ii++) {
        json_t *nodeJson = json_array_get(outTotalJson, ii);
        assert(nodeJson);
        assert(json_typeof(nodeJson) == JSON_ARRAY);
        int numApps = json_array_size(nodeJson);
        assert(numApps > 0 && "we must have run at least 1 app per node");

        for (int jj = 0; jj < numApps; jj++) {
            json_t *appJson = json_array_get(nodeJson, jj);
            assert(appJson);

            assert(json_typeof(appJson) == JSON_STRING);
            const char *appOutStr = json_string_value(appJson);

            // If the app output is empty, that means this was one of the host
            // apps
            if (appOutStr[0] == '\0') {
                continue;
            }

            // We now have the actual contents of the app output. At this
            // point the app output is user defined.
            appOutJson = json_loads(appOutStr, 0, &jsonError);
            if (appOutJson == NULL) {
                status = StatusAppOutParseFail;
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Failed to parse output '%s' of load output: %s",
                              appOutStr,
                              jsonError.text);
                goto CommonExit;
            }
            if (json_typeof(appOutJson) != JSON_OBJECT) {
                status = StatusAppOutParseFail;
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Load app output isn't a JSON object");
                goto CommonExit;
            }
            json_t *numBuffersJson;
            json_t *numBytesJson;
            json_t *numFilesJson;
            size_t thisNumBuffers;
            size_t thisNumBytes;
            int64_t thisNumFiles;

            numBuffersJson = json_object_get(appOutJson, "numBuffers");
            if (numBuffersJson == NULL) {
                status = StatusAppOutParseFail;
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Load app output doesn't have numBuffers field");
                goto CommonExit;
            }

            numBytesJson = json_object_get(appOutJson, "numBytes");
            if (numBytesJson == NULL) {
                status = StatusAppOutParseFail;
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Load app output doesn't have numBytes field");
                goto CommonExit;
            }

            numFilesJson = json_object_get(appOutJson, "numFiles");
            if (numFilesJson == NULL) {
                status = StatusAppOutParseFail;
                xSyslogTxnBuf(ModuleName,
                              XlogErr,
                              "Load app output doesn't have numFiles field");
                goto CommonExit;
            }

            thisNumBuffers = json_integer_value(numBuffersJson);
            thisNumBytes = json_integer_value(numBytesJson);
            thisNumFiles = json_integer_value(numFilesJson);

            *numBuffers += thisNumBuffers;
            *numBytes += thisNumBytes;
            *numFiles += thisNumFiles;

            json_decref(appOutJson);
            appOutJson = NULL;
        }
    }

CommonExit:
    if (outTotalJson) {
        json_decref(outTotalJson);
        outTotalJson = NULL;
    }
    if (appOutJson) {
        json_decref(appOutJson);
        appOutJson = NULL;
    }
    if (status == StatusAppOutParseFail) {
        // For uncontrolled apps this is nonfatal.
        // XXX - lets just assume 1 buffer; AppInstance will fix this
        *numBuffers = 1;
        *numBytes = 0;
        *numFiles = 0;
    }
    return status;
}

Status
AppLoader::parseLoadErrStr(const char *appErrBlob,
                           unsigned clusterNumNodes,
                           char *errorStringBuf,
                           size_t errorStringBufSize,
                           char *errorFileBuf,
                           size_t errorFileBufSize)
{
    Status status = StatusOk;
    size_t numNodes;
    json_t *errTotalJson = NULL;
    json_t *appErrJson = NULL;
    json_error_t jsonError;
    bool setError = false;
    errorStringBuf[0] = '\0';
    errorFileBuf[0] = '\0';

    errTotalJson = json_loads(appErrBlob, 0, &jsonError);
    if (errTotalJson == NULL) {
        // The format of this string is dictated by the AppGroup, and thus
        // can be guaranteed to be parseable. However, it's possible to
        // encounter a memory error here, so we still have to handle errors
        status = StatusAppOutParseFail;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to parse output of load output: %s",
                      jsonError.text);
        goto CommonExit;
    }

    assert(json_typeof(errTotalJson) == JSON_ARRAY);

    numNodes = json_array_size(errTotalJson);
    if (numNodes != clusterNumNodes) {
        status = StatusAppOutParseFail;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "App found %zu nodes worth of error output; "
                      "expecting %u",
                      numNodes,
                      clusterNumNodes);
        goto CommonExit;
    }

    for (size_t ii = 0; (!setError) && (ii < numNodes); ii++) {
        json_t *nodeJson = json_array_get(errTotalJson, ii);
        assert(nodeJson);
        assert(json_typeof(nodeJson) == JSON_ARRAY);
        int numApps = json_array_size(nodeJson);
        assert(numApps > 0 && "we must have run at least 1 app per node");

        for (int jj = 0; (!setError) && (jj < numApps); jj++) {
            json_t *appJson = json_array_get(nodeJson, jj);
            assert(appJson);

            assert(json_typeof(appJson) == JSON_STRING);
            const char *appErrStr = json_string_value(appJson);

            if (appErrStr[0] == '\0') {
                continue;
            }

            // We now have an actual error string.
            // The error is JSON formatted in this string.

            appErrJson = json_loads(appErrStr, 0, &jsonError);
            if (appErrJson == NULL || json_typeof(appErrJson) != JSON_OBJECT) {
                // This wasn't a formatted output, but it is a valid error,
                // so we won't have a valid file for this one
                strlcpy(errorStringBuf, appErrStr, errorStringBufSize);
            } else {
                // This is a specially formatted error object, constructed by
                // the AppInstance object, rather than by the app itself
                json_t *errorStringJson =
                    json_object_get(appErrJson, "errorString");

                if (errorStringJson == NULL) {
                    json_decref(appErrJson);
                    appErrJson = NULL;
                    continue;
                }

                const char *errorString = json_string_value(errorStringJson);

                if (errorString[0] == '\0') {
                    json_decref(appErrJson);
                    appErrJson = NULL;
                    continue;
                }

                strlcpy(errorStringBuf, errorString, errorStringBufSize);

                // If for some reason we don't have an error file, that's okay
                // (we should always have one here though)
                json_t *errorFileJson =
                    json_object_get(appErrJson, "errorFile");
                if (errorFileJson) {
                    const char *errorFile = json_string_value(errorFileJson);

                    strlcpy(errorFileBuf, errorFile, errorFileBufSize);
                }

                // We only want to copy in a single error; this will exit
                setError = true;
                if (appErrJson) {
                    json_decref(appErrJson);
                    appErrJson = NULL;
                }
            }
            assert(appErrJson == NULL && "must have been freed by here");
        }
    }
    if (setError) {
        assert(errorStringBuf[0] != '\0');
    }

CommonExit:
    if (errTotalJson) {
        json_decref(errTotalJson);
        errTotalJson = NULL;
    }
    if (appErrJson) {
        json_decref(appErrJson);
        appErrJson = NULL;
    }
    return status;
}

json_t *
AppLoader::jsonifySourceArgs(const DataSourceArgs *sourceArgs)
{
    return json_pack(
        "{"
        "s:s,"  // targetName
        "s:s,"  // path
        "s:s,"  // namePattern
        "s:b,"  // recursive
        "}",
        "targetName",
        sourceArgs->targetName,
        "path",
        sourceArgs->path,
        "fileNamePattern",
        sourceArgs->fileNamePattern,
        "recursive",
        sourceArgs->recursive);
}

Status
AppLoader::listFiles(
    const xcalar::compute::localtypes::Connectors::ListFilesRequest *request,
    xcalar::compute::localtypes::Connectors::ListFilesResponse *response)
{
    struct ListFilesApp {
        ListFilesApp() { app = AppMgr::get()->openAppHandle(name, &handle); }
        ~ListFilesApp()
        {
            if (app != nullptr) {
                AppMgr::get()->closeAppHandle(app, handle);
            }
        }
        const char *name = AppMgr::ListFilesAppName;
        LibNsTypes::NsHandle handle;
        App *app;
    } listFiles;

    if (listFiles.app == nullptr) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "App %s does not exist",
                      listFiles.name);
        return StatusLoadAppNotExist;
    }

    // Serialize message to string for app input
    size_t requestBytesSize = request->ByteSizeLong();
    std::unique_ptr<uint8_t[]> requestBytes(new uint8_t[requestBytesSize]);
    bool success =
        request->SerializeToArray(requestBytes.get(), requestBytesSize);
    if (!success) {
        xSyslogTxnBuf(ModuleName, XlogErr, "Failed to stringify request");
        return StatusFailed;
    }

    // Base64 encode bytes.
    BuffHolder encodedBytes;
    size_t encodedBytesSize;
    Status status = base64Encode(requestBytes.get(),
                                 requestBytesSize,
                                 &encodedBytes.buff,
                                 &encodedBytesSize);
    if (!status.ok()) {
        xSyslogTxnBuf(ModuleName, XlogErr, "Failed to base64Encode request");
        return StatusFailed;
    }

    // Run the app
    BuffHolder allAppOutBlob;
    BuffHolder errorBlob;
    bool appInternalError = false;
    status = AppMgr::get()->runMyApp(listFiles.app,
                                     AppGroup::Scope::Global,
                                     "",  // No user, global scope
                                     0,
                                     encodedBytes.buff,
                                     0,
                                     &allAppOutBlob.buff,
                                     &errorBlob.buff,
                                     &appInternalError);
    if (!status.ok()) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to list files on target '%s' path '%s'"
                      "with app '%s' (%s) : %s",
                      request->sourceargs().targetname().c_str(),
                      request->sourceargs().path().c_str(),
                      listFiles.name,
                      strGetFromStatus(status),
                      errorBlob.buff);
        return status;
    }

    // App was successfull, extract single app output
    BuffHolder appOutBlob;
    status = AppMgr::extractSingleAppOut(allAppOutBlob.buff, &appOutBlob.buff);
    if (!status.ok()) {
        return status;
    }

    BuffHolder protoBytes;
    size_t protoBytesSize;
    status = base64Decode((const char *) appOutBlob.buff,
                          strlen(appOutBlob.buff),
                          (uint8_t **) &protoBytes.buff,
                          &protoBytesSize);
    if (!status.ok()) {
        xSyslogTxnBuf(ModuleName, XlogErr, "Failed to base64Decode request");
        return status;
    }

    // Deserialize bytes to proto message
    success = response->ParseFromArray(protoBytes.buff, protoBytesSize);
    if (success) {
        return StatusOk;
    } else {
        xSyslogTxnBuf(ModuleName, XlogErr, "Failed to parse protobuf response");
        return StatusFailed;
    }
}

AppLoader::AppLoader() : datasetId_(0), dataset_(NULL) {}

Status
AppLoader::init(DsDatasetId datasetId, DatasetRefHandle *dsRefHandle)
{
    if (datasetId == XidInvalid) {
        return StatusDsNotFound;
    }
    datasetId_ = datasetId;
    Status status = StatusOk;
    dataset_ = Dataset::get()->getDatasetFromId(datasetId, &status);
    assert(status == StatusOk && dataset_ != NULL && "guaranteed by contract");
    dsRefHandle_ = *dsRefHandle;

    return StatusOk;
}

Status
AppLoader::updateProgress(DsDatasetId datasetId, int64_t incremental)
{
    DsDataset *dataset;
    OpStatus *opStatus;
    Status status = StatusOk;

    assert(datasetId != XidInvalid && "should have been found earlier");

    dataset = Dataset::get()->getDatasetFromId(datasetId, &status);
    assert(dataset != NULL && status == StatusOk &&
           "should have been found earlier");

    verifyOk(DagLib::get()
                 ->getDagLocal(dataset->dagId_)
                 ->getOpStatus(dataset->nodeId_, &opStatus));

    atomicAdd64(&opStatus->atomicOpDetails.numWorkCompletedAtomic, incremental);
    return status;
}

void
AppLoader::setTotalProgress(DsDatasetId datasetId,
                            int64_t totalWork,
                            bool downSampled)
{
    DsDataset *dataset;
    OpStatus *opStatus;
    Status status = StatusOk;

    assert(datasetId != 0 && "should have been found earlier");

    dataset = Dataset::get()->getDatasetFromId(datasetId, &status);
    assert(dataset != NULL && status == StatusOk &&
           "should have been found earlier");

    dataset->downSampled_ = downSampled;

    verifyOk(DagLib::get()
                 ->getDagLocal(dataset->dagId_)
                 ->getOpStatus(dataset->nodeId_, &opStatus));

    opStatus->atomicOpDetails.numWorkTotal = totalWork;
}

Status
AppLoader::addDataPage(AppInstance *parentInstance,
                       DsDatasetId datasetId,
                       int32_t numFiles,
                       int32_t numErrors,
                       bool errorPage,
                       uint8_t *page,
                       bool fixedSchemaPage)
{
    Status status = StatusOk;
    PageLoader *loader = NULL;
    DsDataset *dataset;
    DataPageIndex *index;
    DataPageIndex::RangeReservation *res = NULL;
    int32_t numRecords;
    bool xdbLoad;

    if (datasetId == XidInvalid) {
        status = StatusDsNotFound;
        goto CommonExit;
    }
    dataset = Dataset::get()->getDatasetFromId(datasetId, &status);
    assert(dataset && "this should get caught earlier");

    xdbLoad = dataset->loadArgs_.xdbLoadArgs.keyName[0] != '\0';
    if (!xdbLoad) {
        // data page for datasets
        DataPageReader reader;
        reader.init(page, XdbMgr::bcSize());
        numRecords = reader.getNumRecords();
        // Check if we need to make a reservation in the data page index
        if (numRecords > 0) {
            res = new (std::nothrow) DataPageIndex::RangeReservation();
            BailIfNull(res);

            index = errorPage ? dataset->errorPageIndex_ : dataset->pageIndex_;
            status = index->reserveRecordRange(page, res);
            BailIfFailed(status);
        }
    }

    atomicAdd64(&dataset->numErrors_, numErrors);

    loader = new (std::nothrow) PageLoader(parentInstance);
    BailIfNull(loader);

    loader->init(dataset,
                 res,
                 numFiles,
                 errorPage,
                 page,
                 XdbMgr::bcSize(),
                 fixedSchemaPage);
    BailIfFailed(status);
    res = NULL;

    status = Runtime::get()->schedule(static_cast<Schedulable *>(loader));
    BailIfFailed(status);
    loader = NULL;

CommonExit:
    if (loader) {
        delete loader;
    }
    if (res) {
        delete res;
    }
    return status;
}

PageLoader::PageLoader(AppInstance *parentInstance)
    : Schedulable("PageLoader"),
      parentInstance_(parentInstance),
      dataset_(NULL),
      page_(NULL),
      pageSize_(-1)
{
}

void
PageLoader::init(DsDataset *dataset,
                 DataPageIndex::RangeReservation *reservation,
                 int32_t numFiles,
                 bool errorPage,
                 uint8_t *page,
                 size_t pageSize,
                 bool fixedSchemaPage)
{
    dataset_ = dataset;
    assert(dataset_ != NULL && "should have been found earlier");
    reservation_ = reservation;
    numFiles_ = numFiles;
    errorPage_ = errorPage;
    page_ = page;
    pageSize_ = pageSize;
    fixedSchemaPage_ = fixedSchemaPage;
}

void
PageLoader::run()
{
    Status status = StatusOk;
    int32_t pageSize = XdbMgr::bcSize();
    BufferResult result;

    result.numFiles = numFiles_;
    result.numBytes = 0;

    assert(dataset_->pageIndex_);
    assert(dataset_->errorPageIndex_);

    if (!fixedSchemaPage_) {
        DataPageReader reader;
        reader.init(page_, pageSize);
        assert(reader.validatePage());
        if (reader.getNumRecords() == 0) {
            goto CommonExit;
        }
    }

    // This steals reference to page_ over to loadOneFile
    status = this->loadOnePage(page_, pageSize);
    page_ = NULL;
    BailIfFailed(status);
    result.numBytes = pageSize;

CommonExit:
    if (page_ != NULL) {
        DataFormat::freeXdbPage(page_);
        page_ = NULL;
    }
    result.status = status;
    parentInstance_->bufferLoaded(&result);
}

void
PageLoader::done()
{
    delete this;
}

Status
PageLoader::loadOnePage(uint8_t *page, int32_t pageSize)
{
    Status status = StatusOk;
    DataFormat *df = DataFormat::get();

    assert(page);
    assert(dataset_->pageIndex_);
    assert(dataset_->errorPageIndex_);

    bool xdbLoad = dataset_->loadArgs_.xdbLoadArgs.keyName[0] != '\0';

    if (xdbLoad) {
        // This is an LRQ load; don't insert into index
        assert(reservation_ == NULL && "we don't want a reservation for LRQ");
        // We don't want error pages at all during xdb load
        if (errorPage_) {
            goto CommonExit;
        } else if (fixedSchemaPage_) {
            status = df->loadFixedSchemaPageIntoXdb(dataset_->loadArgs_
                                                        .xdbLoadArgs.dstXdbId,
                                                    page,
                                                    pageSize,
                                                    &dataset_->loadArgs_
                                                         .xdbLoadArgs);
            BailIfFailed(status);

        } else {
            status = df->loadDataPagesIntoXdb(dataset_->loadArgs_.xdbLoadArgs
                                                  .dstXdbId,
                                              1,
                                              &page,
                                              pageSize,
                                              &dataset_->loadArgs_.xdbLoadArgs,
                                              NULL,
                                              NULL);
            BailIfFailed(status);
        }
    } else {
        assert(reservation_ != NULL && "we need a reservation from earlier");
        DataPageIndex *index;
        index = errorPage_ ? dataset_->errorPageIndex_ : dataset_->pageIndex_;

        // This takes ownership of page
        index->addPage(page, reservation_);
        page = NULL;
    }

CommonExit:
    if (page != NULL) {
        DataFormat::freeXdbPage(page);
        page = NULL;
    }
    return status;
}

Status
AppLoader::preview(const XcalarApiPreviewInput *previewInput,
                   const XcalarApiUserId *user,
                   XcalarApiOutput **outputOut,
                   size_t *outputSize)
{
    Status status;
    XcalarApiOutput *output = NULL;
    XcalarApiPreviewOutput *previewOutput;
    const char *loadAppName = AppMgr::ListPreviewAppName;
    App *loadApp = NULL;
    char *appOutBlob = NULL;
    char *appOutput = NULL;
    int outStrLen;
    char *errorStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;

    loadApp = AppMgr::get()->openAppHandle(loadAppName, &handle);
    if (loadApp == NULL) {
        status = StatusLoadAppNotExist;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load app %s does not exist",
                      loadAppName);
        goto CommonExit;
    }

    status = AppMgr::get()->runMyApp(loadApp,
                                     AppGroup::Scope::Global,
                                     user ? user->userIdName : "",
                                     0,
                                     previewInput->inputJson,
                                     0,
                                     &appOutBlob,
                                     &errorStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Load app %s failed with output:%s, error:%s",
                      loadAppName,
                      appOutBlob,
                      errorStr);
        goto CommonExit;
    }

    status = AppMgr::extractSingleAppOut(appOutBlob, &appOutput);
    BailIfFailed(status);

    outStrLen = strlen(appOutput);

    *outputSize = XcalarApiSizeOfOutput(*previewOutput) + outStrLen + 1;
    output = (XcalarApiOutput *) memAllocExt(*outputSize, ModuleName);
    BailIfNull(output);

    previewOutput = &output->outputResult.previewOutput;

    memcpy(previewOutput->outputJson, appOutput, outStrLen + 1);
    assert(previewOutput->outputJson[outStrLen] == '\0');
    previewOutput->outputLen = outStrLen + 1;

    *outputOut = output;
CommonExit:
    if (loadApp) {
        AppMgr::get()->closeAppHandle(loadApp, handle);
        loadApp = NULL;
    }
    if (appOutBlob) {
        memFree(appOutBlob);
        appOutBlob = NULL;
    }
    if (appOutput) {
        memFree(appOutput);
        appOutput = NULL;
    }
    if (errorStr) {
        memFree(errorStr);
        errorStr = NULL;
    }
    if (status != StatusOk) {
        if (output) {
            memFree(output);
            output = NULL;
        }
    }
    return status;
}

Status
AppLoader::removeFile(
    const xcalar::compute::localtypes::Connectors::RemoveFileRequest
        *delRequest,
    const XcalarApiUserId *user)
{
    Status status;
    const char *loadAppName = AppMgr::FileDeleteAppName;
    App *loadApp = NULL;
    char *inBlob = NULL;
    char *appOutput = NULL;
    char *errorBuffer = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    json_t *sourceArgsJson = NULL;
    json_t *inputBlobJson = NULL;

    loadApp = AppMgr::get()->openAppHandle(loadAppName, &handle);
    if (loadApp == NULL) {
        status = StatusLoadAppNotExist;
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Delete file app %s does not exist",
                      loadAppName);
        goto CommonExit;
    }

    sourceArgsJson = json_pack(
        "{"
        "s:s,"  // targetName
        "s:s,"  // path
        "}",
        "targetName",
        (char *) delRequest->target_name().c_str(),
        "path",
        (char *) delRequest->path().c_str());
    BailIfNull(sourceArgsJson);

    inputBlobJson = json_pack(
        "{"
        "s:s,"  // func
        "s:o"   // sourceArgs
        "}",
        "func",
        "deleteFiles",
        "sourceArgs",
        sourceArgsJson);
    if (inputBlobJson == NULL) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Unknown error json formatting remove input");
        status = StatusInval;
        goto CommonExit;
    }
    sourceArgsJson = NULL;  // Stolen by the above

    inBlob = json_dumps(inputBlobJson, 0);
    BailIfNull(inBlob);
    assert(inBlob);

    status = AppMgr::get()->runMyApp(loadApp,
                                     AppGroup::Scope::Global,
                                     user ? user->userIdName : "",
                                     0,
                                     inBlob,
                                     0,
                                     &appOutput,
                                     &errorBuffer,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Failed to remove file/dir on target '%s' path '%s' "
                      "with app '%s' (%s) : %s",
                      (char *) delRequest->target_name().c_str(),
                      (char *) delRequest->path().c_str(),
                      loadAppName,
                      strGetFromStatus(status),
                      errorBuffer);
        goto CommonExit;
    }

CommonExit:
    if (inputBlobJson) {
        json_decref(inputBlobJson);
        inputBlobJson = NULL;
    }
    if (sourceArgsJson) {
        json_decref(sourceArgsJson);
        sourceArgsJson = NULL;
    }
    if (loadApp) {
        AppMgr::get()->closeAppHandle(loadApp, handle);
    }
    if (appOutput) {
        memFree(appOutput);
        appOutput = NULL;
    }
    if (inBlob) {
        memFree(inBlob);
        inBlob = NULL;
    }
    if (errorBuffer) {
        memFree(errorBuffer);
        errorBuffer = NULL;
    }
    return status;
}

Status
AppLoader::schemaLoad(
    const xcalar::compute::localtypes::SchemaLoad::AppRequest *request,
    xcalar::compute::localtypes::SchemaLoad::AppResponse *response)
{
    Status status = StatusOk;
    const char *schemaAppName = AppMgr::SchemaLoadAppName;
    App *schemaApp = NULL;
    LibNsTypes::NsHandle handle;
    char *appInputJsonStr = NULL;
    char *appOutputJsonStr = NULL;
    char *errorBuffer = NULL;
    bool appInternalError = false;
    json_t *allAppOutputJson = NULL;
    json_error_t jsonError;
    size_t numNodes;
    const char *finalSingleOutput = NULL;

    // Load the app
    schemaApp = AppMgr::get()->openAppHandle(schemaAppName, &handle);
    if (schemaApp == NULL) {
        status = StatusLoadAppNotExist;  // hrmm..
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "SchemaLoad app '%s' does not exist",
                      schemaAppName);
        goto CommonExit;
    }

    // Get the JSON string from the proto message
    appInputJsonStr = (char *) request->json().c_str();

    xSyslog(ModuleName, XlogErr, "JSON_IN='%s'", appInputJsonStr);

    // Run the app
    status = AppMgr::get()->runMyApp(schemaApp,
                                     AppGroup::Scope::Global,  // XXX ???
                                     "",                       // user
                                     0,
                                     appInputJsonStr,
                                     0,
                                     &appOutputJsonStr,
                                     &errorBuffer,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "app_name='%s', status='%s', error='%s'",
                      schemaAppName,
                      strGetFromStatus(status),
                      errorBuffer);
        goto CommonExit;
    }

    xSyslog(ModuleName, XlogErr, "JSON_OUT='%s'", appOutputJsonStr);

    // XXX App is return is consolidated on single node inside XPU
    allAppOutputJson = json_loads(appOutputJsonStr, 0, &jsonError);
    if (allAppOutputJson == NULL) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "msg='failed to parse app output', "
                "json_err='%s', output='%s'",
                jsonError.text,
                appOutputJsonStr);
        goto CommonExit;
    }

    numNodes = json_array_size(allAppOutputJson);
    for (size_t ii = 0; ii < numNodes; ii++) {
        json_t *nodeJson = json_array_get(allAppOutputJson, ii);
        size_t numXpus = json_array_size(nodeJson);

        assert(numXpus == 1 && "because of App::FlagInstancePerNode");
        json_t *xpuJson = json_array_get(nodeJson, 0);

        const char *singleAppOutputStr = json_string_value(xpuJson);
        if (singleAppOutputStr[0] == '\0') continue;

        finalSingleOutput = singleAppOutputStr;
    }
    // We should have only a single output from Node 0
    // TODO: write code to validate that all other XPUs return empty string
    if (finalSingleOutput == NULL) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "msg='Could not find single app output', output='%s'",
                appOutputJsonStr);
        goto CommonExit;
    }

    response->set_json(finalSingleOutput);

CommonExit:
    if (schemaApp) {
        AppMgr::get()->closeAppHandle(schemaApp, handle);
    }
    if (appOutputJsonStr) {
        memFree(appOutputJsonStr);
        appOutputJsonStr = NULL;
    }
    if (errorBuffer) {
        memFree(errorBuffer);
        errorBuffer = NULL;
    }
    if (allAppOutputJson) {
        json_decref(allAppOutputJson);
        allAppOutputJson = NULL;
    }
    return (status);
}
