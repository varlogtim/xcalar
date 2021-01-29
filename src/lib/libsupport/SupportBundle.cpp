// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <new>
#include <stdio.h>
#include "StrlFunc.h"
#include "support/SupportBundle.h"
#include "util/MemTrack.h"
#include "msg/Message.h"
#include "sys/XLog.h"
#include "common/Version.h"
#include "constants/XcalarConfig.h"
#include "LibSupportConstants.h"
#include "gvm/Gvm.h"
#include "LibSupportGvm.h"
#include "libapis/LibApisCommon.h"
#include "strings/String.h"

using namespace support;

struct SupportGenerateMsg {
    bool generateMiniBundle;
    uint64_t supportCaseId;
    char supportId[XcalarApiUuidStrLen + 1];
};

static constexpr const char *moduleName = "libsupport";

SupportBundle *SupportBundle::instance = NULL;

Status  // static
SupportBundle::init()
{
    assert(instance == NULL);
    instance = new (std::nothrow) SupportBundle;
    if (instance == NULL) {
        return StatusNoMem;
    }

    Status status = LibSupportGvm::init();
    if (status != StatusOk) {
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
SupportBundle::destroy()
{
    if (LibSupportGvm::get()) {
        LibSupportGvm::get()->destroy();
    }
    delete instance;
    instance = NULL;
}

SupportBundle *
SupportBundle::get()
{
    return instance;
}

SupportBundle::SupportBundle() {}

SupportBundle::~SupportBundle() {}

// Creates a unique support ID to associate with a support request.
void
SupportBundle::supportNewSupportId(char *supportId, size_t supportIdSize)
{
    Status status = StatusOk;
    const char *uuidPath = "/proc/sys/kernel/random/uuid";
    FILE *fp = NULL;

    errno = 0;
    // @SymbolCheckIgnore
    fp = fopen(uuidPath, "r");
    if (fp == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to fopen '%s': %s",
                uuidPath,
                strGetFromStatus(status));
        goto CommonExit;
    }
    if (fread(supportId, 1, supportIdSize, fp) != supportIdSize) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogErr, "'%s' failed to return results", uuidPath);
        goto CommonExit;
    }
    // The content read has a terminating '\n'...change it.
    supportId[supportIdSize - 1] = '\0';

CommonExit:

    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }
}

// Populates bundlePath with local path to support bundle.
void
SupportBundle::supportBundlePath(const char *supportId,
                                 char *bundlePath,
                                 size_t bundlePathSize)
{
    snprintf(bundlePath,
             bundlePathSize,
             "%s/support/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             supportId);
}

// Dispatches a SupportGenerate request to all usrnodes causing each to generate
// a node-specific support bundle.
Status
SupportBundle::supportDispatch(bool generateMiniBundle,
                               uint64_t supportCaseId,
                               XcalarApiOutput **output,
                               size_t *outputSize)
{
    SupportGenerateMsg *msg = NULL;
    Status status = StatusOk;
    XcalarApiOutput *outputTmp = NULL;
    size_t outputSizeTmp = SupportBundlePathSize;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    XcalarApiSupportGenerateOutput *supportBundleTmp = NULL;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(SupportGenerateMsg));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(LibSupportGvm::get()->getGvmIndex(),
                   (uint32_t) LibSupportGvm::Action::Generate,
                   sizeof(SupportGenerateMsg));
    msg = (SupportGenerateMsg *) gPayload->buf;

    outputTmp = (XcalarApiOutput *) memAllocExt(outputSizeTmp, moduleName);
    BailIfNull(gPayload);

    supportBundleTmp = &outputTmp->outputResult.supportGenerateOutput;

    supportNewSupportId(msg->supportId, sizeof(msg->supportId));
    msg->generateMiniBundle = generateMiniBundle;
    msg->supportCaseId = supportCaseId;

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    if (status == StatusOk || status == StatusSupportBundleNotSent) {
        // Support bundle was successfully created.  But it may not have
        // been sent to Xcalar.
        if (status == StatusSupportBundleNotSent) {
            supportBundleTmp->supportBundleSent = false;
            status = StatusOk;
        } else {
            supportBundleTmp->supportBundleSent = true;
        }
        strlcpy(supportBundleTmp->supportId,
                msg->supportId,
                sizeof(supportBundleTmp->supportId));
        supportBundlePath(msg->supportId,
                          supportBundleTmp->bundlePath,
                          outputSizeTmp -
                              XcalarApiSizeOfOutput(*supportBundleTmp));

        *output = outputTmp;
        *outputSize = outputSizeTmp;
        outputTmp = NULL;
    } else {
        memFree(outputTmp);
        outputTmp = NULL;
    }

CommonExit:
    if (outputTmp != NULL) {
        memFree(outputTmp);
        outputTmp = NULL;
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

// Generate support files by invoking the helper python script.
Status
SupportBundle::supportGenerateInternal(const char *supportId,
                                       bool generateMiniBundle,
                                       uint64_t supportCaseId,
                                       NodeId nodeId)
{
    Status status;
    int res = -1;

    const char *cmdFormatStr =
        "python3.6 %s/scripts/Support.py "
        "%s %llu \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" %llu %llu 2>&1";
    char *cmdStr = NULL;
    size_t cmdStrLen;
    Config *config = Config::get();
    size_t configFileLen = strlen(config->configFilePath_);
    size_t xcalarRootLen = strlen(XcalarConfig::get()->xcalarRootCompletePath_);
    size_t xcalarLogLen = strlen(XcalarConfig::get()->xcalarLogCompletePath_);
    FILE *script;
    bool supportBundleSent = false;
    char generateMiniBundleStr[6];

    assert(strlen(supportId) == XcalarApiUuidStrLen);
    const char *xlrDir = getenv("XLRDIR");

    // Be a bit more forgiving in this path so we can generate an ASUP
    // even when XLRDIR isn't set properly
    if (!xlrDir) {
        xlrDir = PREFIX;
    }

    if (generateMiniBundle) {
        xSyslog(moduleName, XlogWarn, "Generating mini support bundle");
        status = strStrlcpy(generateMiniBundleStr,
                            "true",
                            sizeof(generateMiniBundleStr));
    } else {
        status = strStrlcpy(generateMiniBundleStr,
                            "false",
                            sizeof(generateMiniBundleStr));
    }
    BailIfFailed(status);

    // Add up len of all cmdStr components.
    cmdStrLen = strlen(cmdFormatStr) + XcalarApiUuidStrLen + UInt64MaxStrLen +
                configFileLen + xcalarRootLen + xcalarLogLen +
                XcalarApiVersionBufLen + PATH_MAX +
                strlen(generateMiniBundleStr) + UInt64MaxStrLen;
    cmdStr = (char *) memAllocExt(cmdStrLen + 1, "supportLocal cmdStr");
    BailIfNull(cmdStr);

    snprintf(cmdStr,
             cmdStrLen + 1,
             cmdFormatStr,
             xlrDir,
             supportId,
             nodeId,
             config->configFilePath_,
             XcalarConfig::get()->xcalarRootCompletePath_,
             XcalarConfig::get()->xcalarLogCompletePath_,
             versionGetStr(),
             generateMiniBundleStr,
             supportCaseId,
             nodeId);

    errno = 0;
    script = popen(cmdStr, "r");
    BailIfNullWith(script,
                   (errno == 0 ? StatusNoMem : sysErrnoToStatus(errno)));

    char line[SupportOutputLineLen + 1];
    status = StatusSupportFail;
    while (fgets(line, sizeof(line), script) != NULL) {
        if (strstr(line, "Successfully generated support bundle") != NULL) {
            status = StatusOk;
        } else if (strstr(line, "Successfully uploaded support bundle") !=
                   NULL) {
            supportBundleSent = true;
        }
        xSyslog(moduleName, XlogInfo, "[Support.py] %s", line);
    }

    // Ignore output of pclose. The wait4 performed by pclose may fail due to
    // childnode infrastructure. This means we can't reliably get the exit
    // status of the process.
    // XXX This still races with the waitpid performed in libparent. Execute
    //     this within a childnode.
    res = pclose(script);
    xSyslog(moduleName, XlogWarn, "Exited with code %d", res);

    // Let the user know if the support bundle was successfully generated
    // but not sent to Xcalar.
    if (status == StatusOk && !supportBundleSent) {
        status = StatusSupportBundleNotSent;
    }

CommonExit:
    if (cmdStr != NULL) {
        memFree(cmdStr);
    }
    xSyslog(moduleName,
            XlogWarn,
            "Returning with %s",
            strGetFromStatus(status));
    return status;
}

// GVM only. Local handle for supportDispatch.
Status
SupportBundle::supportGenerateLocal(void *payload)
{
    SupportGenerateMsg *supportMsg = (SupportGenerateMsg *) payload;

    assert(strlen(supportMsg->supportId) == XcalarApiUuidStrLen);
    if (strlen(supportMsg->supportId) != XcalarApiUuidStrLen) {
        return StatusInval;
    }

    Status status = supportGenerateInternal(supportMsg->supportId,
                                            supportMsg->generateMiniBundle,
                                            supportMsg->supportCaseId,
                                            Config::get()->getMyNodeId());
    return status;
}

// Generates new support ID and triggers local generation of support bundle.
// For use when dispatching via usrnode isn't possible.
Status
SupportBundle::supportGenerate(char *supportId,
                               size_t supportIdSize,
                               char *bundlePath,
                               size_t bundlePathSize,
                               NodeId nodeId,
                               uint64_t supportCaseId)
{
    Status status;
    char supportIdTmp[XcalarApiUuidStrLen + 1];

    if (supportIdSize < XcalarApiUuidStrLen + 1) {
        return StatusInval;
    }

    supportNewSupportId(supportIdTmp, sizeof(supportIdTmp));

    status =
        supportGenerateInternal(supportIdTmp, false, supportCaseId, nodeId);
    BailIfFailed(status);

    snprintf(bundlePath,
             bundlePathSize,
             "%s/support/%s",
             XcalarConfig::get()->xcalarRootCompletePath_,
             supportIdTmp);

    strlcpy(supportId, supportIdTmp, supportIdSize);

CommonExit:
    return status;
}
