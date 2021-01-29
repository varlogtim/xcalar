// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "util/CgroupMgr.h"
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "libapis/LibApisRecv.h"
#include "usrnode/UsrNode.h"
#include "ns/LibNs.h"
#include "util/FileUtils.h"
#include "strings/String.h"

CgroupMgr *CgroupMgr::instance = NULL;
int CgroupMgr::CgroupPathCount = 0;
char CgroupMgr::CgroupControllerPaths[CgroupControllerCount]
                                     [CgConfigPathLength];

//
// * Assumption at this point is except XPUs which may come and go,
// everything in XCE especially Xcalar Middleware has been containerized
// upstream by supervisord in xcalar_middleware cgroup.
//
Status
CgroupMgr::init()
{
    Status status = StatusOk;
    NodeId myNodeId = Config::get()->getMyNodeId();
    size_t lastIdx = 0;

    char *controllerPaths = getenv("XCE_CHILDNODE_PATHS");
    // TODO remove this when transition is complete
    if (controllerPaths == NULL) {
        const char *defaultControllerPaths =
            "/sys/fs/cgroup/cpu,cpuacct/xcalar.slice/xcalar-usrnode.service:/"
            "sys/fs/cgroup/memory/xcalar.slice/xcalar-usrnode.service";
        controllerPaths = (char *) defaultControllerPaths;
    }

    size_t controllerPathLen =
        (controllerPaths == NULL) ? 0 : strlen(controllerPaths);
    instance = new (std::nothrow) CgroupMgr();

    BailIfNullMsg(controllerPaths,
                  StatusNoCgroupCtrlPaths,
                  ModuleName,
                  "Failed Cgroup set up: %s",
                  strGetFromStatus(status));

    BailIfNullMsg(instance,
                  StatusNoMem,
                  ModuleName,
                  "Failed Cgroup set up: %s",
                  strGetFromStatus(status));

    if (!enabled()) {
        if (Config::get()->containerized()) {
            xSyslog(ModuleName,
                    XlogInfo,
                    "Cgroups cannot enabled inside docker");
        } else {
            assert(!XcalarConfig::get()->cgroups_);
            xSyslog(ModuleName, XlogInfo, "Cgroups not enabled");
        }
        goto CommonExit;
    }

    if (myNodeId == DlmNode) {
        LibNsTypes::NsId nsId = LibNsTypes::NsInvalidId;
        nsId = LibNs::get()->publish(CgroupNs, &status);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed Cgroup set up: %s",
                        strGetFromStatus(status));

        if (XcalarConfig::get()->cgroupsParamsSetOnBoot_) {
            status = Runtime::get()->schedule(&instance->cgInitFsm_);
            BailIfFailedMsg(ModuleName,
                            status,
                            "Failed Cgroup set up: %s",
                            strGetFromStatus(status));
            instance->cgInitFsmIssued_ = true;
        }
    }

    if (controllerPathLen > 0) {
        for (size_t ii = 0; ii < controllerPathLen; ii++) {
            if (controllerPaths[ii] == CgroupControllerInputDelimit) {
                // this routine is breaking a single large string into
                // several smaller strings, by using
                // CgroupControllerInputDelimit as a field separator.  When you
                // feed the input buffer being parsed to strStrlcpy, it will
                // often fail because the rest of the input string will be much
                // larger than the current single parsed part of that string.
                // So, instead, we explicitly detect that the current part is
                // too big, and fail in that case. We then ignore the error that
                // strStrlcpy produces.

                if ((ii - lastIdx) >= CgConfigPathLength) {
                    char tmpBuf[CgConfigPathLength];

                    status = StatusCgroupCtrlPathLong;
                    // we are in an error handling routine here
                    // and just trying to capture the controller
                    // path that failed rather than every part of
                    // the input string
                    strlcpy(tmpBuf,
                            controllerPaths + lastIdx,
                            CgConfigPathLength);
                    xSyslog(ModuleName,
                            XlogErr,
                            "%s: %s",
                            strGetFromStatus(status),
                            tmpBuf);
                    goto CommonExit;
                }

                status = strStrlcpy(CgroupControllerPaths[CgroupPathCount],
                                    controllerPaths + lastIdx,
                                    ii - lastIdx + 1);

                CgroupPathCount++;
                ii++;  // to skip the field separator
                lastIdx = ii;
            }
        }

        // get the last part the string
        // a:b:c:d -> get 'd'
        // here, the length of the last part of the string should not
        // be ignored, so we use the status returned by strStrlcpy
        status = strStrlcpy(CgroupControllerPaths[CgroupPathCount],
                            controllerPaths + lastIdx,
                            controllerPathLen - lastIdx + 1);

        if (status != StatusOk) {
            char tmpBuf[CgConfigPathLength];

            status = StatusCgroupCtrlPathLong;
            // we are in an error handling routine here
            // and just trying to capture the controller
            // path that failed
            strlcpy(tmpBuf, controllerPaths + lastIdx, CgConfigPathLength);
            xSyslog(ModuleName,
                    XlogErr,
                    "%s: %s",
                    strGetFromStatus(status),
                    tmpBuf);
            goto CommonExit;
        }

        CgroupPathCount++;
    }

CommonExit:
    return status;
}

void
CgroupMgr::destroy()
{
    Status status = StatusOk;
    NodeId myNodeId = Config::get()->getMyNodeId();

    // XXX TODO
    // May be dump some stats for the respective cgroups.

    if (cgInitFsmIssued_) {
        cgInitFsm_.doneSem_.semWait();
        cgInitFsmIssued_ = false;
    }

    if (myNodeId == DlmNode) {
        status = LibNs::get()->removeMatching(CgroupNs);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed destroy: %s",
                    strGetFromStatus(status));
        }
    }

    delete instance;
    instance = NULL;
}

Status
CgroupMgr::process(const char *jsonInput, char **retJsonOutput, Type type)
{
    Status status = StatusOk;
    App *cgroupsApp = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    LibNsTypes::NsHandle handle;
    bool appInternalError = false;
    bool cgRun = false;
    LibNsTypes::NsHandle nsHandle;
    NsObject *obj = NULL;
    LibNs *libNs = LibNs::get();
    bool validHandle = false;

    *retJsonOutput = NULL;

    if (!enabled()) {
        status = StatusCgroupsDisabled;
        xSyslog(ModuleName,
                XlogErr,
                "Failed cgroup API processing for '%s': %s",
                jsonInput,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (!atomicRead64(&cgInitFsm_.done_) &&
        XcalarConfig::get()->cgroupsParamsSetOnBoot_ &&
        type == Type::External) {
        status = StatusAgain;
        xSyslog(ModuleName,
                XlogErr,
                "Failed cgroup API processing for '%s', since cgroup subsystem "
                "is not ready yet",
                jsonInput);
        goto CommonExit;
    }

    if (type == Type::External) {
        nsHandle = libNs->open(CgroupNs, LibNsTypes::WriterExcl, &obj, &status);
        if (status == StatusAccess) {
            // Cgroup App already active.
            status = StatusCgroupInProgress;
        }
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to open handle to cgroup namespace: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }
        validHandle = true;
    }

    if (!runCgroupApp()) {
        status = StatusCgroupAppInProgress;
        xSyslog(ModuleName,
                XlogErr,
                "Failed cgroup API processing for '%s': %s",
                jsonInput,
                strGetFromStatus(status));
        goto CommonExit;
    }
    cgRun = true;

    cgroupsApp = AppMgr::get()->openAppHandle(AppMgr::CgroupsAppName, &handle);
    if (cgroupsApp == NULL) {
        status = StatusAppDoesNotExist;
        xSyslog(ModuleName,
                XlogErr,
                "App %s does not exist: %s",
                AppMgr::CgroupsAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = AppMgr::get()->runMyApp(cgroupsApp,
                                     AppGroup::Scope::Global,
                                     "",
                                     0,
                                     jsonInput,
                                     0,
                                     &outStr,
                                     &errStr,
                                     &appInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App \"%s\" failed with status:\"%s\", "
                "error:\"%s\", output\"%s\"",
                cgroupsApp->getName(),
                strGetFromStatus(status),
                errStr ? errStr : "",
                outStr ? outStr : "");
        goto CommonExit;
    }

    *retJsonOutput = outStr;
    outStr = NULL;

CommonExit:
    if (cgroupsApp) {
        AppMgr::get()->closeAppHandle(cgroupsApp, handle);
        cgroupsApp = NULL;
    }
    if (validHandle) {
        Status status2 = libNs->close(nsHandle, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to close handle to cgroup: %s",
                    strGetFromStatus(status2));
        }
        validHandle = false;
    }

    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }
    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }
    if (cgRun) {
        cgroupAppDone();
    }
    if (obj) {
        memFree(obj);
        obj = NULL;
    }
    return status;
}

Status
CgroupMgr::parseOutput(const char *outStr)
{
    Status status = StatusOk;
    unsigned numNodes = 0;
    json_t *outTotalJson = NULL;
    json_error_t jsonError;
    json_t *appOutJson = NULL;
    const char *cgroupsAppName = AppMgr::CgroupsAppName;

    outTotalJson = json_loads(outStr, 0, &jsonError);
    if (outTotalJson == NULL) {
        // The format of this string is dictated by the AppGroup, and thus
        // can be guaranteed to be parseable. However, it's possible to
        // encounter a memory error here, so we still have to handle errors
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                jsonError.text);
        goto CommonExit;
    }

    if (json_typeof(outTotalJson) != JSON_ARRAY) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                jsonError.text);
        goto CommonExit;
    }

    numNodes = json_array_size(outTotalJson);
    if (numNodes != Config::get()->getActiveNodes()) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (size_t ii = 0; ii < numNodes; ii++) {
        size_t numApps = 0;
        json_t *nodeJson = json_array_get(outTotalJson, ii);
        if (nodeJson == NULL) {
            status = StatusAppOutParseFail;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to parse output of App %s: %s",
                    cgroupsAppName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (json_typeof(nodeJson) != JSON_ARRAY) {
            status = StatusAppOutParseFail;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to parse output of App %s: %s",
                    cgroupsAppName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        numApps = json_array_size(nodeJson);
        if (numApps != 1) {
            status = StatusAppOutParseFail;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to parse output of App %s: %s",
                    cgroupsAppName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        json_t *appJson;
        appJson = json_array_get(nodeJson, 0);
        BailIfNullMsg(appJson,
                      StatusAppOutParseFail,
                      ModuleName,
                      "Failed to parse output of App %s: %s",
                      cgroupsAppName,
                      strGetFromStatus(status));

        if (json_typeof(appJson) != JSON_STRING) {
            status = StatusAppOutParseFail;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to parse output of App %s: %s",
                    cgroupsAppName,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        const char *appOutStr;
        appOutStr = json_string_value(appJson);

        status = parseSingleOutStr(cgroupsAppName, appOutStr);
        BailIfFailedMsg(ModuleName,
                        status,
                        "Failed to parse output of App %s: %s",
                        cgroupsAppName,
                        strGetFromStatus(status));
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
CgroupMgr::parseSingleOutStr(const char *cgroupsAppName, const char *appOutStr)
{
    Status status = StatusOk;
    json_t *resultObj = NULL, *jsonRetStatus = NULL;
    json_error_t jsonError;

    resultObj = json_loads(appOutStr, JSON_DECODE_ANY, &jsonError);
    BailIfNullMsg(resultObj,
                  StatusAppOutParseFail,
                  ModuleName,
                  "Failed to parse output of App %s with error %s: %s",
                  cgroupsAppName,
                  jsonError.text,
                  strGetFromStatus(status));

    if (json_typeof(resultObj) != JSON_OBJECT) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    jsonRetStatus = json_object_get(resultObj, CgroupFuncRetStatusKey);
    BailIfNullMsg(jsonRetStatus,
                  StatusNoMem,
                  ModuleName,
                  "Failed to parse output of App %s: %s",
                  cgroupsAppName,
                  strGetFromStatus(status));

    if (json_typeof(jsonRetStatus) != JSON_STRING) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    const char *jsonRetStatusStr;
    jsonRetStatusStr = json_string_value(jsonRetStatus);
    if (strcmp(jsonRetStatusStr, CgroupFuncRetStatusSuccess)) {
        status = StatusAppOutParseFail;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse output of App %s: %s",
                cgroupsAppName,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (resultObj) {
        json_decref(resultObj);
        resultObj = NULL;
    }
    return status;
}

FsmState::TraverseState
CgroupMgr::CgroupInitFsmStart::doWork()
{
    CgroupInitFsm *fsm = dynamic_cast<CgroupInitFsm *>(getSchedFsm());

    if (usrNodeNormalShutdown()) {
        fsm->setNextState(NULL);
    } else if (usrNodeForceShutdown()) {
        fsm->setNextState(NULL);
    } else if (!apisRecvInitialized()) {
        fsm->waitSem_.timedWait(USecsPerMSec);
    } else {
        fsm->setNextState(&fsm->completion_);
    }
    return TraverseState::TraverseNext;
}

FsmState::TraverseState
CgroupMgr::CgroupInitFsmCompletion::doWork()
{
    Status status = StatusOk;
    json_t *jsonCgInit = NULL, *jsonFunc = NULL;
    char *jsonInput = NULL, *retJsonOutput = NULL;
    int ret = 0;
    CgroupMgr *cgMgr = CgroupMgr::get();
    CgroupInitFsm *fsm = dynamic_cast<CgroupInitFsm *>(getSchedFsm());

    jsonCgInit = json_object();
    BailIfNullMsg(jsonCgInit,
                  StatusNoMem,
                  ModuleName,
                  "Failed Cgroups set up: %s",
                  strGetFromStatus(status));

    jsonFunc = json_string(CgroupFuncInit);
    BailIfNullMsg(jsonFunc,
                  StatusNoMem,
                  ModuleName,
                  "Failed Cgroups set up: %s",
                  strGetFromStatus(status));

    ret = json_object_set_new(jsonCgInit, CgroupFuncNameKey, jsonFunc);
    if (ret != 0) {
        status = StatusJsonError;
        xSyslog(ModuleName,
                XlogErr,
                "Failed Cgroups set up: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    jsonFunc = NULL;

    jsonInput = json_dumps(jsonCgInit, 0);
    BailIfNullMsg(jsonInput,
                  StatusNoMem,
                  ModuleName,
                  "Failed Cgroups set up: %s",
                  strGetFromStatus(status));

    status = cgMgr->process(jsonInput, &retJsonOutput, Type::System);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed Cgroups set up: %s",
                    strGetFromStatus(status));

    status = cgMgr->parseOutput(retJsonOutput);
    BailIfFailedMsg(ModuleName,
                    status,
                    "Failed Cgroups set up: %s",
                    strGetFromStatus(status));

CommonExit:
    if (jsonFunc != NULL) {
        assert(status != StatusOk);
        json_decref(jsonFunc);
        jsonFunc = NULL;
    }
    if (jsonCgInit != NULL) {
        json_decref(jsonCgInit);
        jsonCgInit = NULL;
    }
    if (retJsonOutput != NULL) {
        memFree(retJsonOutput);
        retJsonOutput = NULL;
    }
    fsm->retStatus_ = status;
    return TraverseState::TraverseStop;
}

void
CgroupMgr::CgroupInitFsm::done()
{
    Status status = retStatus_;
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed Cgroups set up, but not a hard error: %s",
                strGetFromStatus(status));
    } else {
        xSyslog(ModuleName, XlogInfo, "Cgroups set up succeeded");
    }
    atomicWrite64(&done_, 1);
    doneSem_.post();
}

bool
CgroupMgr::runCgroupApp()
{
    CgroupAppState cgState =
        (CgroupAppState) atomicCmpXchg32(&cgroupAppInProgress_,
                                         (int32_t) CgroupAppState::None,
                                         (int32_t) CgroupAppState::InProgress);
    if (cgState == CgroupAppState::None) {
        return true;
    } else {
        assert(cgState == CgroupAppState::InProgress);
        return false;
    }
}

void
CgroupMgr::cgroupAppDone()
{
    CgroupAppState cgState =
        (CgroupAppState) atomicCmpXchg32(&cgroupAppInProgress_,
                                         (int32_t) CgroupAppState::InProgress,
                                         (int32_t) CgroupAppState::None);
    assert(cgState == CgroupAppState::InProgress);
}

bool
CgroupMgr::enabled()
{
    return XcalarConfig::get()->cgroups_ && !Config::get()->containerized();
}

Status
CgroupMgr::childNodeClassify(pid_t pid, char *processClass, char *processSched)
{
    char tmpBuf[CgConfigPathLength + PathLength];
    size_t maxPidStrLen = IntTypeToStrlen(int64_t) + 1;
    char pidStr[maxPidStrLen];
    size_t pidStrLen;
    int fd = -1;
    Status status = StatusOk;

    pidStrLen = snprintf(pidStr, maxPidStrLen, "%u", pid);

    for (int ii = 0; ii < CgroupPathCount; ii++) {
        snprintf(tmpBuf,
                 CgConfigPathLength + PathLength,
                 "%s/%s_xpus-%s.scope/cgroup.procs",
                 CgroupControllerPaths[ii],
                 processClass,
                 processSched);
        fd = open(tmpBuf, O_WRONLY | O_CLOEXEC | O_NOCTTY | O_NOFOLLOW);
        if (fd == -1) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to open cgroups proc file '%s': %s",
                    tmpBuf,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status = FileUtils::convergentWrite(fd, pidStr, pidStrLen);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to write %u to '%s': %s",
                    pid,
                    tmpBuf,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        FileUtils::close(fd);
        fd = -1;
    }

CommonExit:

    if (fd != -1) {
        FileUtils::close(fd);
        fd = -1;
    }

    return status;
}
