// Copyright 2015 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <dirent.h>

#include "StrlFunc.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "strings/String.h"
#include "util/Random.h"
#include "DurableObject.pb.h"
#include "durable/Durable.h"
#include "common/Version.h"
#include "test/QA.h"

// mustBeOk is similar to mustBeOk except that it preserves the return
// status.  The "info reg rax" workaround doesn't work.  The returned
// status is used in a syslog (otherwise the compiler will complain about
// an unused variable) to hopefully preserve it in a core.
#define mustBeOk(op)                                 \
    do {                                             \
        Status myStatus;                             \
        myStatus = (op);                             \
        if (unlikely(myStatus != StatusOk)) {        \
            xSyslog(moduleName,                      \
                    XlogErr,                         \
                    "Failure in LogTestsCommon: %s", \
                    strGetFromStatus(myStatus));     \
            assert(0 && "LogTestsCommon failure");   \
        }                                            \
    } while (false)

using namespace xcalar::internal::durable;
using namespace xcalar::internal::durable::session;

static constexpr const char *moduleName = "logTestsCommon";

static const char prefix[] = "logTests";

static LogLib::Handle staticLogEdgeTest;
static LogLib::Handle staticLogBigFileTest;
// Test files created by the tests below.
const char *testFileDefault = "LogTests.Default";
static const char *testFileDeleteMe = "LogTests.DeleteMe";
static const char *testFileNonDefaultParams = "LogTests.NonDefaultParams";
static const char *testFileMagicNumbers = "LogTests.MagicNumbers";
static const char *testFileBigNumFiles = "LogTests.BigNumFiles";
static const char *testFileBigFile = "LogTests.BigFile";
static const char *testFileExpand = "LogTests.FileExpand";

// Test files copied over from source control.
static const char *testFileMissing0 = "LogTests.Missing0";
static const char *testFileMissingEnd = "LogTests.MissingEnd";
static const char *testFileMissingFooter = "LogTests.MissingFooter";
static const char *testFileBadChecksum = "LogTests.BadChecksum";

void
cleanupPriorTestFiles(const char *prefix)
{
    unsigned countDeleted;
    unsigned countFailed;

    char pattern[strlen(prefix) + 3];
    snprintf(pattern, sizeof(pattern), "%s-*", prefix);
    mustBeOk(FileUtils::unlinkFiles(LogLib::get()->getDirPath(
                                        LogLib::RecoveryDirIndex),
                                    pattern,
                                    &countDeleted,
                                    &countFailed));
}

static bool
checkFileWithPrefixExists(const char *dirPath, const char *prefix)
{
    DIR *dirIter = NULL;
    struct dirent *curEnt;
    bool result = false;

    const size_t prefixLen = strlen(prefix);

    char search[prefixLen + 2];
    strlcpy(search, prefix, sizeof(search));
    search[prefixLen] = '*';
    search[prefixLen + 1] = '\0';

    dirIter = opendir(dirPath);
    assert(dirIter != NULL);

    // Check to see if file exists matching the pattern
    errno = 0;
    for (curEnt = readdir(dirIter); curEnt != NULL; curEnt = readdir(dirIter)) {
        if (!strMatch(search, curEnt->d_name) ||
            strcmp(curEnt->d_name, ".") == 0 ||
            strcmp(curEnt->d_name, "..") == 0) {
            errno = 0;
            continue;
        }
        result = true;
        break;
    }

    closedir(dirIter);
    dirIter = NULL;

    return result;
}

Status
logTestSanity()
{
    Status status;
    LogLib::Handle log;
    size_t bytesRead;
    char bufData[] = "asdfjkldeadbeef";
    size_t bufSize;
    char *buf;
    // Let these throw
    char *perThrTestFileDefault = new char[XcalarApiMaxPathLen];
    char *perThrTestFileDeleteMe = new char[XcalarApiMaxPathLen];
    char *perThrTestFileNonDefaultParams = new char[XcalarApiMaxPathLen];
    pid_t tid = sysGetTid();
    int ret;
    Config *config = Config::get();
    static const size_t BufReadSize = 3 * KB;
    uint8_t *bufRead = new uint8_t[BufReadSize];

    ret = snprintf(perThrTestFileDefault,
                   XcalarApiMaxPathLen,
                   "%s-Node%d-Thr%d",
                   testFileDefault,
                   config->getMyNodeId(),
                   tid);
    assert((unsigned) ret < XcalarApiMaxPathLen);

    ret = snprintf(perThrTestFileDeleteMe,
                   XcalarApiMaxPathLen,
                   "%s-Node%d-Thr%d",
                   testFileDeleteMe,
                   config->getMyNodeId(),
                   tid);
    assert((unsigned) ret < XcalarApiMaxPathLen);

    ret = snprintf(perThrTestFileNonDefaultParams,
                   XcalarApiMaxPathLen,
                   "%s-Node%d-Thr%d",
                   testFileNonDefaultParams,
                   config->getMyNodeId(),
                   tid);
    assert((unsigned) ret < XcalarApiMaxPathLen);

    LogLib *logLib = LogLib::get();

    // Remove files from previous test to force re-creation.
    cleanupPriorTestFiles(perThrTestFileDefault);
    cleanupPriorTestFiles(perThrTestFileDeleteMe);
    cleanupPriorTestFiles(perThrTestFileNonDefaultParams);

    bufSize = sizeof(bufData);
    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                   bufSize);
    assert(buf != NULL);

    // Test create + write.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    strcpy(buf, bufData);
    mustBeOk(logLib->writeRecord(&log, buf, bufSize));

    memset(buf, 0, bufSize);
    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, bufData) == 0);

    logLib->close(&log);

    // Read what was just written.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(strcmp(buf, bufData) == 0);

    logLib->close(&log);

    // Write again past end.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    strcpy(buf, bufData);
    mustBeOk(logLib->writeRecord(&log, buf, bufSize));

    logLib->close(&log);

    // Read both previously written blocks.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    memset(buf, 0, bufSize);
    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(strcmp(buf, bufData) == 0);

    memset(buf, 0, bufSize);
    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(strcmp(buf, bufData) == 0);

    // Attempt to read past end. Should be notified that we're at the end.
    status =
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext);
    assert(status == StatusEof);
    assert(log.positionedAtEnd);

    // Write after enough to wrap to next file.
    memAlignedFree(buf);
    bufSize = LogLib::LogDefaultFileSize / 2;
    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                   bufSize);
    memset(buf, 'A', bufSize);
    mustBeOk(logLib->writeRecord(&log, buf, bufSize));
    mustBeOk(logLib->writeRecord(&log, buf, bufSize));

    assert(log.backingFileCount == LogLib::LogDefaultFileCount);
    assert(log.backingFileSize == LogLib::LogDefaultFileSize);

    logLib->close(&log);

    // Attempt to read back full log.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(bytesRead == sizeof(bufData));
    assert(strcmp(buf, bufData) == 0);

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(bytesRead == sizeof(bufData));
    assert(strcmp(buf, bufData) == 0);

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(bytesRead == bufSize);
    assert(memcmp(buf, "A", 1) == 0);

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(bytesRead == bufSize);
    assert(memcmp(buf, "A", 1) == 0);

    status =
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext);
    assert(status == StatusEof);

    logLib->close(&log);

    // Test LogLib::FileSeekToLogicalEnd flag's ability to wrap files.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    strcpy(buf, "abcdefg");
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));

    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    for (unsigned i = 0; i < 4; i++) {
        mustBeOk(logLib->readRecord(&log,
                                    buf,
                                    bufSize,
                                    &bytesRead,
                                    LogLib::ReadNext));
    }

    mustBeOk(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext));
    assert(strcmp(buf, "abcdefg") == 0);

    logLib->close(&log);

    // Test logLib->readLastLogicalRecord.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    memset(buf, 0, bufSize);
    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufSize, &bytesRead));
    assert(strcmp(buf, "abcdefg") == 0);

    // Reset log head to a later value.
    strcpy(buf, "A new beginning");
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));

    LogLib::Cursor logCursor;
    logLib->getLastWritten(&log, &logCursor);
    mustBeOk(logLib->resetHead(&log, &logCursor));
    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    memset(buf, 0, bufSize);
    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, "A new beginning") == 0);
    assert(
        logLib->readRecord(&log, buf, bufSize, &bytesRead, LogLib::ReadNext) ==
        StatusEof);

    strcpy(buf, "... ...");
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));

    // Create another cursor.
    const char *secondCursor = (const char *) " < Important > ";
    strcpy(buf, secondCursor);
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    logLib->getLastWritten(&log, &logCursor);

    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, "A new beginning") == 0);

    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufSize, &bytesRead));
    assert(strcmp(buf, secondCursor) == 0);

    strcpy(buf, "... ...");
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));

    mustBeOk(logLib->resetHead(&log, &logCursor));
    mustBeOk(logLib->resetHead(&log, &logCursor));

    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, secondCursor) == 0);

    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    logLib->getLastWritten(&log, &logCursor);
    mustBeOk(logLib->resetHead(&log, &logCursor));
    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, secondCursor) == 0);
    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufSize, &bytesRead));

    strcpy(buf, "... ...");
    do {
        mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
        logLib->getLastWritten(&log, &logCursor);
    } while (logCursor.fileNum == 1);
    assert(logCursor.fileNum == 0);

    strcpy(buf, "Back to file 0 ");
    mustBeOk(logLib->writeRecord(&log, buf, strlen(buf) + 1));
    logLib->getLastWritten(&log, &logCursor);
    mustBeOk(logLib->resetHead(&log, &logCursor));

    mustBeOk(logLib->readRecord(&log,
                                buf,
                                bufSize,
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(strcmp(buf, "Back to file 0 ") == 0);

    logLib->close(&log);
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    assert(log.backingFileCount == LogLib::LogDefaultFileCount);
    assert(log.backingFileSize == LogLib::LogDefaultFileSize);

    logLib->close(&log);

    mustBeOk(
        logLib->fileDelete(LogLib::RecoveryDirIndex, perThrTestFileDefault));

    // Test log close and delete.
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDeleteMe,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    assert(checkFileWithPrefixExists(LogLib::get()->getDirPath(
                                         LogLib::RecoveryDirIndex),
                                     perThrTestFileDeleteMe));

    mustBeOk(logLib->closeAndDelete(&log));
    assert(!checkFileWithPrefixExists(LogLib::get()->getDirPath(
                                          LogLib::RecoveryDirIndex),
                                      perThrTestFileDeleteMe));

    // Test with non-default backing file size/count.
    unsigned backingFileCount = 12;
    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileNonDefaultParams,
                            LogLib::FileSeekToStart,
                            backingFileCount,
                            4 * KB));
    // Fill most of each backing file with P so the next write will have to wrap
    // around. 1KB should be plenty for metadata.
    memset(buf, 'P', 3 * KB);
    for (unsigned i = 0; i < backingFileCount; i++) {
        mustBeOk(logLib->writeRecord(&log, buf, 3 * KB));
    }

    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileNonDefaultParams,
                            LogLib::FileSeekToStart,
                            backingFileCount,
                            4 * KB));

    for (unsigned i = 0; i < backingFileCount; i++) {
        mustBeOk(logLib->readRecord(&log,
                                    bufRead,
                                    BufReadSize,
                                    &bytesRead,
                                    LogLib::ReadNext));
        assert(bytesRead == BufReadSize);
        assert(memcmp(buf, bufRead, BufReadSize) == 0);
    }

    logLib->close(&log);
    mustBeOk(logLib->fileDelete(LogLib::RecoveryDirIndex,
                                perThrTestFileNonDefaultParams));

    memAlignedFree(buf);
    delete[] perThrTestFileDefault;
    delete[] perThrTestFileDeleteMe;
    delete[] perThrTestFileNonDefaultParams;
    delete[] bufRead;
    return StatusOk;
}

Status
logTestSanity2()
{
    size_t bytesRead;
    Status status;
    char bufData[] = "asdfjkldeadbeef";
    LogLib::Handle log;
    char *perThrTestFileNonDefaultParams = new char[XcalarApiMaxPathLen];
    Config *config = Config::get();
    static const size_t BufReadSize = 3 * KB;
    uint8_t *bufRead = new uint8_t[BufReadSize];
    int ret;
    pid_t tid = sysGetTid();
    ret = snprintf(perThrTestFileNonDefaultParams,
                   XcalarApiMaxPathLen,
                   "%s-Node%d-Thr%d",
                   testFileNonDefaultParams,
                   config->getMyNodeId(),
                   tid);
    assert((unsigned) ret < XcalarApiMaxPathLen);

    LogLib *logLib = LogLib::get();
    size_t bufSize;
    char *buf;
    bufSize = sizeof(bufData);
    bufSize = LogLib::LogDefaultFileSize / 2;
    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                   bufSize);
    assert(buf != NULL);

    // Remove files from previous test to force re-creation.
    cleanupPriorTestFiles(perThrTestFileNonDefaultParams);

    // now test LogLib::FileDontPreallocate

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileNonDefaultParams,
                            LogLib::FileReturnErrorIfExists |
                                LogLib::FileSeekToStart |
                                LogLib::FileDontPreallocate,
                            1,
                            0));

    memset(buf, 'H', 3 * KB);
    mustBeOk(logLib->writeRecord(&log, buf, 3 * KB));
    memset(buf, 'J', 3 * KB);
    mustBeOk(logLib->writeRecord(&log, buf, 3 * KB));
    logLib->close(&log);

    mustBeOk(
        logLib->create(&log,
                       LogLib::RecoveryDirIndex,
                       perThrTestFileNonDefaultParams,
                       LogLib::FileSeekToStart | LogLib::FileDontPreallocate,
                       1,
                       0));

    mustBeOk(
        logLib->readLastLogicalRecord(&log, bufRead, BufReadSize, &bytesRead));
    assert(bytesRead == BufReadSize);
    assert(memcmp(buf, bufRead, BufReadSize) == 0);
    logLib->close(&log);

    mustBeOk(
        logLib->create(&log,
                       LogLib::RecoveryDirIndex,
                       perThrTestFileNonDefaultParams,
                       LogLib::FileSeekToStart | LogLib::FileDontPreallocate,
                       1,
                       0));
    mustBeOk(logLib->readRecord(&log,
                                bufRead,
                                BufReadSize,
                                &bytesRead,
                                LogLib::ReadNext));
    memset(buf, 'H', 3 * KB);
    assert(memcmp(buf, bufRead, BufReadSize) == 0);

    logLib->close(&log);

    mustBeOk(logLib->fileDelete(LogLib::RecoveryDirIndex,
                                perThrTestFileNonDefaultParams));

    // DurableObject logging.
    mustBeOk(
        logLib->create(&log,
                       LogLib::RecoveryDirIndex,
                       perThrTestFileNonDefaultParams,
                       LogLib::FileSeekToStart | LogLib::FileDontPreallocate,
                       1,
                       0));

    // TODO: Make a non-trivial libdurable functional test module. For now
    // though just piggyback on the log FT witha simple write/read cycle
    // XXX: Test is currently trivial
    status = LibDurable::init();
    assert(status == StatusOk);
    LibDurable *libDur = LibDurable::get();
    LibDurable::Handle dhSer(&log, "session");
    DurableSessionV2 *pbSes;
    status =
        dhSer.getMutable<DurableSession,
                         DurableSessionV2,
                         Ver>(DurableSessionType,
                              V2,
                              &pbSes,
                              DurableVersions::pbGetCurrSha_DurableSessionV2());

    assert(status == StatusOk);
    assert(pbSes != NULL);
    const char *testStr = "TRIVIAL TEST";

    pbSes->mutable_sessionname()->set_value(
        std::string(testStr, strlen(testStr)));

    mustBeOk(libDur->serialize(&dhSer));

    LibDurable::Handle dhDes(&log, "session");

    mustBeOk(libDur->deserialize(&dhDes));
    status = dhDes.getMutable<DurableSession,
                              DurableSessionV2,
                              Ver>(DurableSessionType, V2, &pbSes);

    assert(status == StatusOk);
    assert(pbSes != NULL);
    assert(strcmp(testStr, pbSes->sessionname().value().c_str()) == 0);
    assert(dhDes.getType() == DurableSessionType);

    char verXce[80];
    status = dhDes.getVerXce(verXce, 80);
    assert(status == StatusOk);
    assert(strcmp(verXce, versionGetStr()) == 0);

    char sha[41];  // ASCII git sha is 40 characters
    memset(sha, 0, sizeof(sha));
    status = dhDes.getSha<DurableSession>(sha, sizeof(sha));
    assert(status == StatusOk);

    // Temporary workaround for Xc-7284
    assert(strlen(sha) == 40 || strlen(sha) == 0);

    mustBeOk(logLib->fileDelete(LogLib::RecoveryDirIndex,
                                perThrTestFileNonDefaultParams));

    logLib->close(&log);
    delete[] perThrTestFileNonDefaultParams;
    memAlignedFree(buf);
    delete[] bufRead;
    return StatusOk;
}

// Tests various edge cases. Uses files loaded by loadDataFiles.
Status
logTestEdge()
{
    LogLib::Handle log;
    int bufBytes = 60000;
    char *buf = new char[bufBytes];  // let throw
    size_t bytesRead;
    LogLib *logLib = LogLib::get();
    pid_t tid = sysGetTid();
    char perThrTestFileDefault[XcalarApiMaxPathLen];
    Config *config = Config::get();

    snprintf(perThrTestFileDefault,
             sizeof(perThrTestFileDefault),
             "%s-Node%d-Thr%d",
             testFileDefault,
             config->getMyNodeId(),
             tid);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    assert(log.backingFileCount == LogLib::LogDefaultFileCount);
    assert(log.backingFileSize == LogLib::LogDefaultFileSize);

    logLib->close(&log);

    // Test "ReturnErrorIfExists" flag.
    verify(logLib->create(&log,
                          LogLib::LogLib::RecoveryDirIndex,
                          perThrTestFileDefault,
                          LogLib::FileSeekToStart |
                              LogLib::FileReturnErrorIfExists,
                          LogLib::LogDefaultFileCount,
                          LogLib::LogDefaultFileSize) == StatusExist);

    // XXX Test behavior when specifying different FileCount/FileSize from what
    //     exists.

    // Test loadBackingFiles error detection and cleanup.
    // @SymbolCheckIgnore
    log.backingFilesList = NULL;

    verify(
        logLib->loadBackingFiles(logLib->getDirPath(LogLib::RecoveryDirIndex),
                                 testFileMissing0,
                                 &log.backingFilesList,
                                 LogLib::FileSeekToStart,
                                 NULL,
                                 NULL) == StatusLogCorrupt);
    assert(log.backingFilesList == NULL);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            testFileMissingFooter,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufBytes, &bytesRead));
    assert(strcmp(buf, "RecordNumber_000000006") == 0);
    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            testFileMissingEnd,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufBytes, &bytesRead));
    assert(strcmp(buf, "RecordNumber_000000040") == 0);
    logLib->close(&log);

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            testFileBadChecksum,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));
    mustBeOk(logLib->readLastLogicalRecord(&log, buf, bufBytes, &bytesRead));
    assert(strcmp(buf, "RecordNumber_000000010") == 0);
    logLib->close(&log);

    // Open log to be closed by logDelete. Must be at end.
    mustBeOk(logLib->create(&staticLogEdgeTest,
                            LogLib::RecoveryDirIndex,
                            perThrTestFileDefault,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    logLib->close(&staticLogEdgeTest);
    mustBeOk(
        logLib->fileDelete(LogLib::RecoveryDirIndex, perThrTestFileDefault));
    delete[] buf;
    return StatusOk;
}

// Array of magic numbers to use in below magic numbers test.
static const uint64_t MagicNums[] = {LogLib::LogHeaderMagic,
                                     LogLib::LogFooterMagic,
                                     LogLib::LogEndMagic,
                                     LogLib::LogNextFileMagic};
static const size_t MagicNumsCount = sizeof(MagicNums) / sizeof(MagicNums[0]);

static inline bool
isMagicNumber(uint64_t num)
{
    unsigned magicNumMatchIdx;

    for (magicNumMatchIdx = 0; magicNumMatchIdx < MagicNumsCount;
         magicNumMatchIdx++) {
        if (num == MagicNums[magicNumMatchIdx]) {
            return true;
        }
    }

    return false;
}

// Fills data portion of log with different magic numbers. Verifies we're still
// able to traverse log.
Status
logTestMagicNumbers()
{
    enum { MagicNumTestCountWrites = 100, MagicNumTestMaxNums = 128 };

    LogLib::Handle log;

    unsigned magicNumIdx;
    unsigned writeIdx;
    char perThrtestFileMagicNumbers[XcalarApiMaxPathLen];
    pid_t tid = sysGetTid();
    Config *config = Config::get();

    snprintf(perThrtestFileMagicNumbers,
             sizeof(perThrtestFileMagicNumbers),
             "%s-Node%d-Thr%d",
             testFileMagicNumbers,
             config->getMyNodeId(),
             tid);
    // Use same seed each time for reproducibility.
    srand(7);

    cleanupPriorTestFiles(perThrtestFileMagicNumbers);
    LogLib *logLib = LogLib::get();

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrtestFileMagicNumbers,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    for (writeIdx = 0; writeIdx < MagicNumTestCountWrites; writeIdx++) {
        // Construct a block full of magic numbers and write it to log file.
        uint64_t *buf;
        size_t magicNumsToWrite = rand() % (MagicNumTestMaxNums - 1) + 1;

        buf = (uint64_t *) memAlloc(sizeof(uint64_t) * magicNumsToWrite);
        assert(buf != NULL);

        for (magicNumIdx = 0; magicNumIdx < magicNumsToWrite; magicNumIdx++) {
            buf[magicNumIdx] = MagicNums[rand() % MagicNumsCount];
        }

        mustBeOk(logLib->writeRecord(&log,
                                     buf,
                                     sizeof(uint64_t) * magicNumsToWrite));

        memFree(buf);
    }

    logLib->close(&log);

    uint64_t readBuf[MagicNumTestMaxNums];
    size_t bytesRead;

    mustBeOk(logLib->create(&log,
                            LogLib::RecoveryDirIndex,
                            perThrtestFileMagicNumbers,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            LogLib::LogDefaultFileSize));

    // For each block that was written ...
    for (writeIdx = 0; writeIdx < MagicNumTestCountWrites; writeIdx++) {
        mustBeOk(logLib->readRecord(&log,
                                    readBuf,
                                    sizeof(readBuf),
                                    &bytesRead,
                                    LogLib::ReadNext));
        assert(bytesRead % sizeof(uint64_t) == 0);

        // For every number in the block ...
        for (magicNumIdx = 0; magicNumIdx < bytesRead / sizeof(uint64_t);
             magicNumIdx++) {
            // Make sure it matches one of the magic numbers.
            assert(isMagicNumber(readBuf[magicNumIdx]));
        }
    }

    verify(logLib->readRecord(&log,
                              readBuf,
                              sizeof(readBuf),
                              &bytesRead,
                              LogLib::ReadNext) == StatusEof);

    mustBeOk(logLib->readRecord(&log,
                                readBuf,
                                sizeof(readBuf),
                                &bytesRead,
                                LogLib::ReadFromStart));
    assert(bytesRead >= sizeof(uint64_t));
    assert(isMagicNumber(readBuf[0]));

    mustBeOk(logLib->readLastLogicalRecord(&log,
                                           readBuf,
                                           sizeof(readBuf),
                                           &bytesRead));
    assert(bytesRead >= sizeof(uint64_t));
    assert(isMagicNumber(readBuf[0]));

    mustBeOk(logLib->readRecord(&log,
                                readBuf,
                                sizeof(readBuf),
                                &bytesRead,
                                LogLib::ReadFromStart));

    mustBeOk(logLib->logSeekToLogicalEnd(&log, NULL, NULL));

    logLib->close(&log);

    mustBeOk(logLib->fileDelete(LogLib::RecoveryDirIndex,
                                perThrtestFileMagicNumbers));
    return StatusOk;
}

Status
logTestBigNumFiles(unsigned numFiles)
{
    char perThrBigNumFiles[XcalarApiMaxPathLen];

    char bufData[] = "asdfjkldeadbeef";
    size_t bufSize;
    char *buf;
    LogLib::Handle log;
    LogLib *logLib = LogLib::get();
    size_t bytesRead;
    Config *config = Config::get();

    pid_t tid = sysGetTid();

    bufSize = sizeof(bufData);
    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                   bufSize);
    assert(buf != NULL);

    strcpy(buf, bufData);

    for (unsigned ii = 0; ii < numFiles; ++ii) {
        snprintf(perThrBigNumFiles,
                 sizeof(perThrBigNumFiles),
                 "%s-Node%d-Thr%d-%u",
                 testFileBigNumFiles,
                 config->getMyNodeId(),
                 tid,
                 ii);

        // Test create + write.
        mustBeOk(logLib->create(&log,
                                LogLib::RecoveryDirIndex,
                                perThrBigNumFiles,
                                LogLib::FileSeekToLogicalEnd,
                                LogLib::LogDefaultFileCount,
                                LogLib::LogDefaultFileSize));

        mustBeOk(logLib->writeRecord(&log, buf, bufSize));

        logLib->close(&log);
    }

    for (unsigned ii = 0; ii < numFiles; ++ii) {
        snprintf(perThrBigNumFiles,
                 sizeof(perThrBigNumFiles),
                 "%s-Node%d-Thr%d-%u",
                 testFileBigNumFiles,
                 config->getMyNodeId(),
                 tid,
                 ii);

        mustBeOk(logLib->create(&log,
                                LogLib::RecoveryDirIndex,
                                perThrBigNumFiles,
                                LogLib::FileSeekToStart,
                                LogLib::LogDefaultFileCount,
                                LogLib::LogDefaultFileSize));

        memset(buf, 0, bufSize);
        mustBeOk(logLib->readRecord(&log,
                                    buf,
                                    bufSize,
                                    &bytesRead,
                                    LogLib::ReadNext));
        assert(strcmp(buf, bufData) == 0);

        logLib->close(&log);

        mustBeOk(
            logLib->fileDelete(LogLib::RecoveryDirIndex, perThrBigNumFiles));
    }

    return StatusOk;
}

Status
logTestBigFile(size_t size)
{
    char *guardData;
    size_t guardDataSize = 1 * KB;
    char *buf;
    char *readBuf;
    size_t bytesRead;
    RandHandle rndHdl;
    LogLib *logLib = LogLib::get();
    Status status;
    size_t logFileSize = size + 2 * guardDataSize + 2 * MB;
    pid_t tid = sysGetTid();
    char perThrTestFile[XcalarApiMaxPathLen];
    Config *config = Config::get();

    rndInitHandle(&rndHdl, sysGetTid());

    buf = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(), size);
    assert(buf != NULL);

    for (uint64_t ii = 0; ii < size; ++ii) {
        buf[ii] = rndGenerate32(&rndHdl);
    }

    readBuf =
        (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(), size);
    assert(readBuf != NULL);

    int ret = snprintf(perThrTestFile,
                       sizeof(perThrTestFile),
                       "%s-Node%d-Thr%d",
                       testFileBigFile,
                       config->getMyNodeId(),
                       tid);
    assert((unsigned) ret < sizeof(perThrTestFile));

    // Test create + write.
    mustBeOk(logLib->create(&staticLogBigFileTest,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            logFileSize));

    guardData = (char *) memAllocAligned(logLib->getMinLogicalBlockAlignment(),
                                         guardDataSize);

    // write header guard data
    memset(guardData, 'A', guardDataSize);
    mustBeOk(
        logLib->writeRecord(&staticLogBigFileTest, guardData, guardDataSize));

    // write random data
    status = logLib->writeRecord(&staticLogBigFileTest, buf, size);
    assert(status == StatusOk);

    // write footer guard data
    memset(guardData, 'Z', guardDataSize);
    mustBeOk(
        logLib->writeRecord(&staticLogBigFileTest, guardData, guardDataSize));

    logLib->close(&staticLogBigFileTest);

    status = logLib->create(&staticLogBigFileTest,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToStart |
                                LogLib::FileReturnErrorIfExists,
                            LogLib::LogDefaultFileCount,
                            logFileSize);

    assert(status == StatusExist);

    status = logLib->create(&staticLogBigFileTest,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            logFileSize);

    // verify header
    memset(guardData, 0, guardDataSize);

    mustBeOk(logLib->readRecord(&staticLogBigFileTest,
                                guardData,
                                guardDataSize,
                                &bytesRead,
                                LogLib::ReadNext));

    for (uint64_t ii = 0; ii < bytesRead; ++ii) {
        assert(guardData[ii] == 'A');
    }

    // verify random
    status = logLib->readRecord(&staticLogBigFileTest,
                                readBuf,
                                size,
                                &bytesRead,
                                LogLib::ReadNext);
    assert(status == StatusOk);
    assert(bytesRead == size);

    assert(memcmp(buf, readBuf, size) == 0);

    // verify footer
    mustBeOk(logLib->readRecord(&staticLogBigFileTest,
                                guardData,
                                guardDataSize,
                                &bytesRead,
                                LogLib::ReadNext));
    assert(bytesRead == guardDataSize);

    for (uint64_t ii = 0; ii < bytesRead; ++ii) {
        assert(guardData[ii] == 'Z');
    }

    logLib->close(&staticLogBigFileTest);
    mustBeOk(logLib->fileDelete(LogLib::RecoveryDirIndex, perThrTestFile));

    memAlignedFree(buf);
    memAlignedFree(guardData);
    memAlignedFree(readBuf);

    return StatusOk;
}

// Check that the reallocate to expand backing files option works.
Status
logTestExpandFiles()
{
    Status status;
    LogLib::Handle handle;
    LogLib::Cursor lastRecord;
    LogLib *logLib = LogLib::get();  // If this isn't ugly....
    size_t bytesRead;
    size_t blockAlign;
    size_t bufSize = 6144;  // 6K
    size_t SmallFileSize = 4096;
    char *buf;
    char perThrTestFile[XcalarApiMaxPathLen];
    pid_t tid = sysGetTid();
    int ret;
    Config *config = Config::get();

    memset(&perThrTestFile, 0, XcalarApiMaxPathLen);

    ret = snprintf(perThrTestFile,
                   sizeof(perThrTestFile),
                   "%s-Node%d-Thr%d",
                   testFileExpand,
                   config->getMyNodeId(),
                   tid);
    assert((unsigned) ret < sizeof(perThrTestFile));

    blockAlign = logLib->getMinLogicalBlockAlignment();

    // Remove files from previous test to force re-creation.
    cleanupPriorTestFiles(perThrTestFile);

    buf = (char *) memAllocAligned(blockAlign, bufSize);
    assert(buf != NULL);
    memset(buf, 'z', bufSize);

    // Verify that we can't write a record larger than a log file if we
    // do not allow reallocation for expansion.
    status = logLib->create(&handle,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToLogicalEnd,
                            LogLib::LogDefaultFileCount,
                            SmallFileSize);
    assert(status == StatusOk);

    // Attempt to write the buffer - it should fail

    status = logLib->writeRecord(&handle, buf, bufSize);
    assert(status == StatusLogMaximumEntrySizeExceeded);

    status = logLib->closeAndDelete(&handle);
    assert(status == StatusOk);

    // Verify that we can write a record larger than a log file if we
    // allow reallocation for expansion.
    status = logLib->create(&handle,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToLogicalEnd |
                                LogLib::FileExpandIfNeeded,
                            LogLib::LogDefaultFileCount,
                            SmallFileSize);
    assert(status == StatusOk);

    // Attempt to write the buffer - it should work this time

    status = logLib->writeRecord(&handle, buf, bufSize);
    assert(status == StatusOk);

    logLib->close(&handle);

    // Read the file back.  Verify we get the entire 6K.  This also tests
    // an edge case - the very first record does not fit and no other records
    // were written.

    status = logLib->create(&handle,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            SmallFileSize);
    assert(status == StatusOk);

    memset(buf, 0, bufSize);
    status =
        logLib->readRecord(&handle, buf, bufSize, &bytesRead, LogLib::ReadNext);
    assert(status == StatusOk);
    assert(memcmp(buf, "zzzz", 4) == 0);
    assert(memcmp(buf + bufSize - 4, "zzzz", 4) == 0);

    status = logLib->closeAndDelete(&handle);
    assert(status == StatusOk);

    // Kick it up a notch. Make both files expand.

    status = logLib->create(&handle,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToLogicalEnd |
                                LogLib::FileExpandIfNeeded,
                            LogLib::LogDefaultFileCount,
                            SmallFileSize);
    assert(status == StatusOk);

    // Attempt to write the buffer - it should work and expand one file

    status = logLib->writeRecord(&handle, buf, bufSize);
    assert(status == StatusOk);

    logLib->getLastWritten(&handle, &lastRecord);

    status = logLib->resetHead(&handle, &lastRecord);
    assert(status == StatusOk);

    // Write the buffer again after altering it's contents - this should
    // expand the other file
    memset(buf, 'y', bufSize);
    status = logLib->writeRecord(&handle, buf, bufSize);
    assert(status == StatusOk);

    logLib->close(&handle);

    // Read the log back.  Verify we get both the zzz and yyy records.

    status = logLib->create(&handle,
                            LogLib::RecoveryDirIndex,
                            perThrTestFile,
                            LogLib::FileSeekToStart,
                            LogLib::LogDefaultFileCount,
                            SmallFileSize);
    assert(status == StatusOk);

    memset(buf, 0, bufSize);
    status =
        logLib->readRecord(&handle, buf, bufSize, &bytesRead, LogLib::ReadNext);
    assert(status == StatusOk);
    assert(memcmp(buf, "zzzz", 4) == 0);
    assert(memcmp(buf + bufSize - 4, "zzzz", 4) == 0);

    memset(buf, 0, bufSize);
    status =
        logLib->readRecord(&handle, buf, bufSize, &bytesRead, LogLib::ReadNext);
    assert(status == StatusOk);
    assert(memcmp(buf, "yyyy", 4) == 0);
    assert(memcmp(buf + bufSize - 4, "yyyy", 4) == 0);

    status = logLib->closeAndDelete(&handle);
    assert(status == StatusOk);

    // Yay!

    memAlignedFree(buf);

    return StatusOk;
}
