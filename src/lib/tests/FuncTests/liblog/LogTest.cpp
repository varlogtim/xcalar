// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "LogTestsCommon.h"
#include "log/Log.h"
#include "strings/String.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "util/FileUtils.h"
#include "LibLogFuncTestConfig.h"
#include "util/System.h"

#include "test/QA.h"  // Must be last

static char DataFileDir[QaMaxQaDirLen + 1] = "";
static char moduleName[] = "logStressTest";

static unsigned numLogPerThr = 100;
static uint64_t bigFileSize = 1 * GB;

static uint64_t logTestFuncStressThreads = 3;

static bool testDataAux = false;

Status
logLoadDataFiles()
{
    Status status;
    const char *recoveryIndexPath =
        LogLib::get()->getDirPath(LogLib::RecoveryDirIndex);

    // deletion of all files in the recovery index means that the log functional
    // test and the log sanity can not be run at the same time
    status = FileUtils::unlinkFiles(recoveryIndexPath, "*", NULL, NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to unlink recovery index files in '%s': %s",
                recoveryIndexPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (DataFileDir[0] == '\0') {
        size_t ret;
        ret = snprintf(DataFileDir,
                       sizeof(DataFileDir),
                       "%s/liblog",
                       qaGetQaDir());
        assert(ret <= sizeof(DataFileDir));
    }

    status = FileUtils::copyDirectoryFiles(recoveryIndexPath, DataFileDir);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to copy files from '%s' to '%s': %s",
                DataFileDir,
                recoveryIndexPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    return status;
}

static void *
logTestFunc(void *arg)
{
    verify(logTestSanity() == StatusOk);
    verify(logTestMagicNumbers() == StatusOk);
    verify(logTestBigNumFiles(numLogPerThr) == StatusOk);
    return NULL;
}

Status
logStress()
{
    Status status = StatusOk;
    int ret;
    pthread_attr_t attr;
    uint64_t numThread;

    numThread = logTestFuncStressThreads;
    assert(numThread > 0);

    // Test write big file
    verifyOk(logTestBigFile(bigFileSize));
    // Test edge
    verifyOk(logTestEdge());

    pthread_t thrHandle[numThread];

    for (unsigned ii = 0; ii < numThread; ii++) {
        ret = pthread_attr_init(&attr);
        assert(ret == 0);
        ret = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        assert(ret == 0);
        status = Runtime::get()->createBlockableThread(&thrHandle[ii],
                                                       &attr,
                                                       logTestFunc,
                                                       NULL);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "createBlockableThread failed: %s",
                    strGetFromStatus(status));
        }
        assert(status == StatusOk);
    }

    for (unsigned ii = 0; ii < numThread; ii++) {
        sysThreadJoin(thrHandle[ii], NULL);
    }

    xSyslog(moduleName, XlogDebug, "End log Test");
    return status;
}

Status
logStressParseConfig(Config::Configuration *config,
                     char *key,
                     char *value,
                     bool stringentRules)
{
    Status status = StatusOk;

    if (strcasecmp(key,
                   strGetFromLibLogFuncTestConfig(
                       LibLogTestFuncStressThreads)) == 0) {
        logTestFuncStressThreads = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibLogFuncTestConfig(
                              LibLogTestBigFileSize)) == 0) {
        bigFileSize = strtoll(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibLogFuncTestConfig(
                              LibLogTestNumLogPerThread)) == 0) {
        numLogPerThr = strtol(value, NULL, 0);
    } else if (strcasecmp(key,
                          strGetFromLibLogFuncTestConfig(
                              LibLogTestDataFileDirAux)) == 0) {
        strlcpy(DataFileDir, value, sizeof(DataFileDir));
        testDataAux = true;
    } else if (strcasecmp(key,
                          strGetFromLibLogFuncTestConfig(
                              LibLogTestDataFileDir)) == 0) {
        // XXX: Temporary to support seamless cutover for new binary test data
        // Prefer Aux path if its set (ignore normal path used only by previous
        // versions)
        if (testDataAux) {
            return StatusOk;
        }

        strlcpy(DataFileDir, value, sizeof(DataFileDir));
    } else {
        status = StatusUsrNodeIncorrectParams;
    }

    xSyslog(moduleName,
            XlogDebug,
            "%s changed %s to %s",
            (status == StatusOk) ? "Successfully" : "Unsuccessfully",
            key,
            value);
    return status;
}
