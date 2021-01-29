// Copyright 2016-2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "util/SystemPathVerify.h"
#include "app/AppMgr.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

Status
PathVerify::verifyAll(const char **paths, int numPaths, bool *allShared)
{
    Status status = StatusOk;
    PathVerify pv;

    status = pv.init(paths, numPaths);
    BailIfFailed(status);

    status = pv.run(allShared);
    BailIfFailed(status);

CommonExit:
    return status;
}

Status
PathVerify::verifyPath(const char *path, bool *isShared)
{
    Config *config = Config::get();
    bool isMaster =
        (BigPrime % config->getActiveNodes()) == config->getMyNodeId();
    if (strcmp(path, XcalarConfig::get()->xcalarRootCompletePath_) == 0 &&
        !isMaster) {
        // XXX May be pick some other node for this.
        *isShared = true;
        return StatusOk;
    }
    return verifyAll(&path, 1, isShared);
}

Status
PathVerify::init(const char *paths[], int numPaths)
{
    Status status = StatusOk;

    if (numPaths <= 0) {
        return StatusInval;
    }
    numPaths_ = numPaths;

    for (int ii = 0; ii < numPaths_; ii++) {
        if (paths[ii] == NULL) {
            return StatusInval;
        }
    }
    paths_ = paths;

    return status;
}

Status
PathVerify::run(bool *allShared)
{
    Status status = StatusOk;
    App *verifyApp = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    char *inObj = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;

    verifyApp = AppMgr::get()->openAppHandle(AppMgr::VerifyAppName, &handle);
    if (verifyApp == NULL) {
        status = StatusAppDoesNotExist;
        xSyslog(ModuleName,
                XlogErr,
                "Verify app %s does not exist",
                AppMgr::VerifyAppName);
        goto CommonExit;
    }

    // Construct the input string to the app
    status = jsonifyInput(paths_, numPaths_, &inObj);
    BailIfFailed(status);

    assert(inObj);

    // Actually run the app across the cluster
    status = AppMgr::get()->runMyApp(verifyApp,
                                     AppGroup::Scope::Global,
                                     "",
                                     0,
                                     inObj,
                                     0,
                                     &outStr,
                                     &errStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslogTxnBuf(ModuleName,
                      XlogErr,
                      "Verify app \"%s\" failed with status:\"%s\", "
                      "error:\"%s\", output\"%s\"",
                      verifyApp->getName(),
                      strGetFromStatus(status),
                      errStr ? errStr : "",
                      outStr ? outStr : "");
        goto CommonExit;
    }

    // Now we need to check the output to see if the paths were shared globally
    status = parseOutput(outStr, errStr, allShared);
    BailIfFailed(status);

CommonExit:
    if (verifyApp) {
        AppMgr::get()->closeAppHandle(verifyApp, handle);
        verifyApp = NULL;
    }
    if (inObj) {
        memFree(inObj);
        inObj = NULL;
    }
    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }
    return status;
}

Status
PathVerify::jsonifyInput(const char *paths[], int numPaths, char **outStr)
{
    Status status = StatusOk;
    int ret;
    json_t *obj = NULL;
    json_t *pathArray = NULL;
    json_t *pathStr = NULL;
    const char *pathKey = "paths";
    uint64_t seed;
    *outStr = NULL;

    seed = time(NULL);

    obj = json_pack(
        "{"
        "s:s,"  // func
        "s:I,"  // seed
        "}",
        "func",
        "verifyShared",
        "seed",
        seed);
    BailIfNull(obj);

    // Now we can dynamically construct the paths array

    pathArray = json_array();
    BailIfNull(pathArray);

    for (int ii = 0; ii < numPaths; ii++) {
        pathStr = json_string(paths[ii]);
        BailIfNull(pathStr);

        // Steals pathStr ref
        ret = json_array_append_new(pathArray, pathStr);
        if (ret != 0) {
            status = StatusNoMem;
            goto CommonExit;
        }
        pathStr = NULL;
    }

    ret = json_object_set_new(obj, pathKey, pathArray);
    if (ret != 0) {
        status = StatusNoMem;
        goto CommonExit;
    }
    pathArray = NULL;

    *outStr = json_dumps(obj, 0);
    BailIfNull(*outStr);

CommonExit:
    if (obj) {
        json_decref(obj);
        obj = NULL;
    }
    if (pathStr) {
        json_decref(pathStr);
        pathStr = NULL;
    }
    if (pathArray) {
        json_decref(pathArray);
        pathArray = NULL;
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
PathVerify::parseOutput(const char *outStr, const char *errStr, bool *allShared)
{
    Status status = StatusOk;
    size_t numNodes;
    json_t *outTotalJson = NULL;
    json_error_t jsonError;
    json_t *appOutJson = NULL;
    *allShared = true;

    outTotalJson = json_loads(outStr, 0, &jsonError);
    if (outTotalJson == NULL) {
        // The format of this string is dictated by the AppGroup, and thus
        // can be guaranteed to be parseable. However, it's possible to
        // encounter a memory error here, so we still have to handle errors
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of verify output: %s",
                jsonError.text);
        goto CommonExit;
    }

    assert(json_typeof(outTotalJson) == JSON_ARRAY);

    numNodes = json_array_size(outTotalJson);
    assert(numNodes == Config::get()->getActiveNodes());

    for (size_t ii = 0; ii < numNodes; ii++) {
        json_t *nodeJson = json_array_get(outTotalJson, ii);
        assert(nodeJson);
        assert(json_typeof(nodeJson) == JSON_ARRAY);
        int numApps = json_array_size(nodeJson);
        assert(numApps == 1 && "we must have run 1 app per node");

        json_t *appJson = json_array_get(nodeJson, 0);
        assert(appJson);

        assert(json_typeof(appJson) == JSON_STRING);
        const char *appOutStr = json_string_value(appJson);

        // We now have the actual contents of the app output. At this
        // point the app output is user defined (user being the app).
        bool nodeSeesAllPaths;
        status = parseSingleOutStr(appOutStr, &nodeSeesAllPaths);
        BailIfFailed(status);

        if (!nodeSeesAllPaths) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Node %zu not able to access all shared paths",
                    ii);
            *allShared = false;
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
    return status;
}

Status
PathVerify::parseSingleOutStr(const char *appOutStr, bool *allShared)
{
    Status status = StatusOk;
    json_t *resultObj = NULL;
    const char *path;
    json_t *pathValue;
    json_error_t jsonError;
    *allShared = true;

    resultObj = json_loads(appOutStr, JSON_DECODE_ANY, &jsonError);
    if (resultObj == NULL) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of verify app: %s",
                jsonError.text);
        goto CommonExit;
    }
    if (json_typeof(resultObj) != JSON_OBJECT) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName, XlogErr, "Verify app output isn't a JSON object");
        goto CommonExit;
    }

    json_object_foreach (resultObj, path, pathValue) {
        if (json_typeof(pathValue) != JSON_TRUE &&
            json_typeof(pathValue) != JSON_FALSE) {
            status = StatusAppOutParseFail;
            xSyslog(ModuleName,
                    XlogErr,
                    "Verify app output's path value is not boolean: type '%i'",
                    json_typeof(pathValue));
            goto CommonExit;
        }
        if (json_typeof(pathValue) == JSON_FALSE) {
            // This path is not globally visible
            xSyslog(ModuleName,
                    XlogErr,
                    "Path '%s' not on shared storage",
                    path);
            *allShared = false;
        }
    }

CommonExit:
    if (resultObj) {
        json_decref(resultObj);
        resultObj = NULL;
    }
    return status;
}
