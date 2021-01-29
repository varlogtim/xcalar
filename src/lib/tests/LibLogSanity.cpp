// Copyright 2015 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>
#include <sys/stat.h>

#include "StrlFunc.h"
#include "config/Config.h"
#include "log/Log.h"
#include "common/InitTeardown.h"
#include "LogTestsCommon.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "stat/Statistics.h"

#include "test/QA.h"  // Must be last

char logTestDirectory[PATH_MAX];

static Status
logTestLogDump()
{
    extern const char *testFileDefault;
    char logDump[1024];

    // XXX Assumes xcalar root is on local FS.
    char filePrefix[] = "nfs://";
    size_t filePrefixLen = strlen(filePrefix);

    char *xcalarRootStart = XcalarConfig::get()->xcalarRootCompletePath_;
    if (strncmp(filePrefix, xcalarRootStart, filePrefixLen) == 0) {
        xcalarRootStart += filePrefixLen;
    }

    verify((size_t) snprintf(logDump,
                             sizeof(logDump),
                             "%s/../../misc/logdump/logdump --directory %s/%s "
                             "--prefix %s",
                             logTestDirectory,
                             xcalarRootStart,
                             LogLib::RecoveryDirName,
                             testFileDefault) < sizeof(logDump));

    verify(system(logDump) == 0);
    return StatusOk;
}

static void
loadDataFiles()
{
    char dataFileDir[QaMaxQaDirLen + 1];
    size_t ret;

    ret = snprintf(dataFileDir, sizeof(dataFileDir), "%s/liblog", qaGetQaDir());
    assert(ret <= sizeof(dataFileDir));

    verifyOk(FileUtils::unlinkFiles(LogLib::get()->getDirPath(
                                        LogLib::RecoveryDirIndex),
                                    "*",
                                    NULL,
                                    NULL));
    verifyOk(FileUtils::copyDirectoryFiles(LogLib::get()->getDirPath(
                                               LogLib::RecoveryDirIndex),
                                           dataFileDir));
}

static Status testCircularLog();
static Status logTestLogNew();

static TestCase testCases[] =
    {{"log wrap around test", testCircularLog, TestCaseEnable, "5799"},
     {"log new test", logTestLogNew, TestCaseEnable, "2452"},
     {"log sanity tests", logTestSanity, TestCaseEnable, ""},
     {"log sanity tests 2", logTestSanity2, TestCaseEnable, ""},
     {"log edge case tests", logTestEdge, TestCaseEnable, ""},
     {"log magic number test", logTestMagicNumbers, TestCaseEnable, ""},
     {"log dump test", logTestLogDump, TestCaseDisable, ""},
     {"log expansion test", logTestExpandFiles, TestCaseEnable, ""}};

// Make sure the file will wrap around
static Status
testCircularLog()
{
    Status status;
    char circularTest[XcalarApiMaxPathLen] = "circularTest";
    char bufData[] = "asdfjkldeadbeef";
    size_t bufSize;
    char *buf;
    LogLib::Handle log;

    cleanupPriorTestFiles(circularTest);
    LogLib *logLib = LogLib::get();

    bufSize = sizeof(bufData);
    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                   bufSize);
    assert(buf != NULL);

    // Test create + write.
    verifyOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            circularTest,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    strlcpy(buf, bufData, bufSize);
    LogLib::Cursor lastRecord;

    // write first record
    status = logLib->writeRecord(&log, buf, bufSize);
    assert(status == StatusOk);
    logLib->getLastWritten(&log, &lastRecord);
    status = logLib->resetHead(&log, &lastRecord);
    assert(status == StatusOk);

    // keep on writing until wrap around
    uint64_t loop = LogLib::LogDefaultFileSize / bufSize + 1;
    for (uint64_t ii = 0; ii < loop; ++ii) {
        status = logLib->writeRecord(&log, buf, bufSize);
        assert(status == StatusOk);
        logLib->getLastWritten(&log, &lastRecord);
        status = logLib->resetHead(&log, &lastRecord);
        assert(status == StatusOk);
    }

    // Xc-5799 write some more log records, this time without calling
    // resetHead.  We expect to run out of space and get StatusOverflow.
    loop = LogLib::LogDefaultFileSize / bufSize + 1;
    for (uint64_t ii = 0; ii < loop; ++ii) {
        status = logLib->writeRecord(&log, buf, bufSize);
        assert(status == StatusOk || status == StatusOverflow);
        if (status == StatusOverflow) {
            break;
        }
    }

    logLib->close(&log);
    memAlignedFree(buf);

    return StatusOk;
}

static Status
logTestLogNew()
{
    StatsLib *statsLib = StatsLib::get();

    statsLib->removeGroupIdFromHashTable("liblog");

    verifyOk(LogLib::createSingleton());

    loadDataFiles();  // Needed for future tests.

    return StatusOk;
}

static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime);

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char *directory = dirname(argv[0]);

    verify(strlcpy(logTestDirectory, directory, sizeof(logTestDirectory)) <
           sizeof(logTestDirectory));

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             directory,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::Config,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    verifyOk(Config::get()->loadNodeInfo());

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
