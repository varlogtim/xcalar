// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "util/SystemVersionVerify.h"
#include "app/AppMgr.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

Status
VersionVerify::verifyVersion(const char *version)
{
    Status status = StatusOk;
    Config *config = Config::get();
    VersionVerify vv;

    bool isMaster =
        (BigPrime % config->getActiveNodes()) == config->getMyNodeId();

    if (!isMaster) {
        // Check is driven by node 0
        goto CommonExit;
    }

    status = vv.init(version);
    BailIfFailed(status);

    status = vv.run();
    BailIfFailed(status);

CommonExit:

    return status;
}

Status
VersionVerify::init(const char *version)
{
    Status status = StatusOk;

    version_ = version;

    return status;
}

Status
VersionVerify::run()
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
    status = jsonifyInput(version_, &inObj);
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
    status = parseOutput(outStr, errStr);
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
VersionVerify::jsonifyInput(const char *version, char **outStr)
{
    Status status = StatusOk;
    int ret;
    json_t *obj = NULL;
    json_t *versionArray = NULL;
    json_t *versionStr = NULL;
    const char *versionKey = "version";
    uint64_t seed;
    *outStr = NULL;

    seed = time(NULL);

    obj = json_pack(
        "{"
        "s:s,"  // func
        "s:I,"  // seed
        "}",
        "func",
        "verifyVersion",
        "seed",
        seed);
    BailIfNull(obj);

    // Now we can dynamically construct the version array

    versionArray = json_array();
    BailIfNull(versionArray);

    versionStr = json_string(version);
    BailIfNull(versionStr);

    // Steals versionStr ref
    ret = json_array_append_new(versionArray, versionStr);
    if (ret != 0) {
        status = StatusNoMem;
        goto CommonExit;
    }
    versionStr = NULL;

    ret = json_object_set_new(obj, versionKey, versionArray);
    if (ret != 0) {
        status = StatusNoMem;
        goto CommonExit;
    }
    versionArray = NULL;

    *outStr = json_dumps(obj, 0);
    BailIfNull(*outStr);

CommonExit:
    if (obj) {
        json_decref(obj);
        obj = NULL;
    }
    if (versionStr) {
        json_decref(versionStr);
        versionStr = NULL;
    }
    if (versionArray) {
        json_decref(versionArray);
        versionArray = NULL;
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
VersionVerify::parseOutput(const char *outStr, const char *errStr)
{
    Status status = StatusOk;
    size_t numNodes;
    json_t *outTotalJson = NULL;
    json_error_t jsonError;
    json_t *appOutJson = NULL;

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
        status = parseSingleOutStr(appOutStr);
        BailIfFailed(status);
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
VersionVerify::parseSingleOutStr(const char *appOutStr)
{
    Status status = StatusOk;
    json_t *resultObj = NULL;
    json_error_t jsonError;

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

CommonExit:
    if (resultObj) {
        json_decref(resultObj);
        resultObj = NULL;
    }
    return status;
}
