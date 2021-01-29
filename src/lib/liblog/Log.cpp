// Copyright 2013 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <pthread.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <google/protobuf/util/time_util.h>

#include "StrlFunc.h"
#include "log/Log.h"
#include "config/Config.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "hash/Hash.h"
#include "stat/Statistics.h"
#include "constants/XcalarConfig.h"
#include "util/FileUtils.h"
#include "common/InitTeardown.h"
#include "common/Version.h"
#include "DurableObject.pb.h"
#include "DurableVersions.h"
#include "durable/Durable.h"

// This module allows creation, read/write and management of persistent logs.
// Log files can be sequentially written or read. Writes must only append to
// the end of the log while reads typically start from the beginning of the
// log (although it's possible to seek to the last record).

// All reads and writes are done in terms of log records who's content is known
// to the caller. Records are braced by a header and a footer. If a corruption
// is encountered in the log, the last intact record before the corrupt is
// considered the "last" record in the log.

// This module is not thread safe. It is assumed that all thread synchronization
// is done by the caller.

// minLogicalBlockAlignment is the alignment in bytes that is required for an
// in-memory logical block being written to a log. For now all
// logging assume a 64 bit alignment to optimize crc32 based checksums.
// This requires the user to do a getMinLogicalBlockAlignment() for the
// memory they are allocating for their logical blocks.

bool LogLib::logIsInit = false;
LogLib *LogLib::logLib = NULL;

Status
LogLib::createSingleton()
{
    Status status;
    void *ptr = NULL;
    // This is needed as the log tests call this routine repeatedly
    // without a deleteSingleton...effectively stomping on the prior
    // logLib.  The tests have changed the sessionDirName prior to
    // calling this so use that name.
    char savedSessionDirName[XcalarApiMaxPathLen + 1];
    bool savedName = false;

    if (logLib != NULL) {
        strlcpy(savedSessionDirName,
                logLib->getSessionDirName(),
                XcalarApiMaxPathLen);
        savedName = true;
    } else {
        ptr = memAllocExt(sizeof(LogLib), __PRETTY_FUNCTION__);
        if (ptr == NULL) {
            return StatusNoMem;
        }
        LogLib::logLib = new (ptr) LogLib();
    }

    if (savedName) {
        // Use name from prior instance
        logLib->setSessionDirName(savedSessionDirName);
    } else {
        // Use the default name
        logLib->setSessionDirName(SessionDirNameDefault);
    }

    status = LogLib::logLib->createInternal();

    if (status != StatusOk) {
        if (ptr != NULL) {
            LogLib::logLib->~LogLib();
            memFree(ptr);
            LogLib::logLib = NULL;
        }
    }

    return status;
}

LogLib *
LogLib::get()
{
    assert(logLib);
    return logLib;
}

//
// Helper functions.
//

void
LogLib::appendBackingFileList(BackingFile **listHead, BackingFile *element)
{
    assert(element != NULL);

    if (*listHead == NULL) {
        element->prev = element->next = element;
        *listHead = element;
    } else {
        element->prev = (*listHead)->prev;
        element->next = (*listHead);
        (*listHead)->prev->next = element;
        (*listHead)->prev = element;
    }
}

void
LogLib::insertBeforeBackingFileList(BackingFile **listHead,
                                    BackingFile *curElt,
                                    BackingFile *newElt)
{
    assert(*listHead != NULL && curElt != NULL && newElt != NULL);

    newElt->next = curElt;
    newElt->prev = curElt->prev;
    curElt->prev->next = newElt;
    curElt->prev = newElt;

    if (curElt == *listHead) {
        *listHead = newElt;
    }
}

LogLib::BackingFile *
LogLib::removeHeadFromBackingFileList(BackingFile **listHead)
{
    BackingFile *ret = NULL;

    if (*listHead != NULL) {
        ret = (*listHead);

        if ((*listHead)->next == (*listHead)) {
            // last element
            *listHead = NULL;
        } else {
            *listHead = (*listHead)->next;
            ret->next->prev = ret->prev;
            ret->prev->next = ret->next;
        }
    }

    return ret;
}

LogLib::Handle *
LogLib::removeHeadFromLogHandleList(Handle **listHead)
{
    Handle *ret = NULL;

    if (*listHead != NULL) {
        ret = (*listHead);

        if ((*listHead)->next == (*listHead)) {
            // last element
            *listHead = NULL;
        } else {
            *listHead = (*listHead)->next;
            ret->next->prev = ret->prev;
            ret->prev->next = ret->next;
        }
    }

    return ret;
}

void
LogLib::removeFromLogHandleListWithLock(Handle **listHead, Handle *element)
{
    assert(*listHead != NULL);
    assert(element != NULL);

    openHdlLock.lock();
    if (*listHead == element) {
        // target is the head
        if (element->next == element) {
            assert(element->prev == element);
            // target is the only element on list
            *listHead = NULL;
        } else {
            *listHead = element->next;
            element->prev->next = element->next;
            element->next->prev = element->prev;
        }
    } else {
        element->prev->next = element->next;
        element->next->prev = element->prev;
    }

    StatsLib::statNonAtomicDecr(this->statOpenLogCount);
    openHdlLock.unlock();
}

// Close log without removing from list of open logs. Closes any open log set
// files and destroys backing file list.
void
LogLib::logCloseInt(Handle *log)
{
    BackingFile *listElt;
    while ((listElt = removeHeadFromBackingFileList(&log->backingFilesList))) {
        if (listElt->fd != -1) {
            FileUtils::close(listElt->fd);
        }
        memFree(listElt);
    }

    memBarrier();
    assert(log->backingFilesList == NULL);
    log->currentFile = NULL;

    if (log->metaFd != -1) {
        FileUtils::close(log->metaFd);
        log->metaFd = -1;
    }

    if (log->filePrefix != NULL) {
        memFree(log->filePrefix);
        log->filePrefix = NULL;
    }
}

//
// Module initialization/teardown.
//

Status
LogLib::createXcalarDirs()
{
    Status status = StatusOk;
    char tempPath[LogDirLength + 1];
    const char *fullPath;
    int ret;

    bool syslogPaths =
        (InitTeardown::get() != NULL &&
         InitTeardown::get()->getInitLevel() >= InitLevel::UsrNode);

    // Each legit enum in dirIndex must create a dir below as all
    // Xcalar dirs for logging are created statically to keep things
    // easy for system tools and 3rd party apps to use XcalarRootDirIndex
    // (index 0) has already been created and is hence omitted.
    for (unsigned dirIndex = (unsigned) SessionsDirIndex;
         dirIndex < (unsigned) MaxDirsDoNotAddAfterThisEnum;
         ++dirIndex) {
        // Construct and store the full path for this log
        switch ((DirIndex) dirIndex) {
        case SessionsDirIndex:
            // Construct path so it can be used by upgrade but don't create dir
            // XXX: This code could be deleted in Eos
            snprintf(tempPath,
                     sizeof(tempPath),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     getSessionDirName());
            if (syslogPaths) {
                xSyslog(moduleName, XlogInfo, "Sessions Path %s", tempPath);
            }
            fullPath = tempPath;
            break;

        case ResultSetExportDirIndex:
            snprintf(resultSetExportPath_,
                     sizeof(resultSetExportPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     ResultSetExportDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "ResultSetExportPath %s",
                        resultSetExportPath_);
            }
            fullPath = resultSetExportPath_;
            break;

        case WorkBookDirIndex:
            // For UDFs in workbook scope - use WorkBookDirName here
            // Eventually, other components (kvs, sessions, etc.)
            // should move to WorkBookDirName too. For now, we start with UDFs
            snprintf(wkbkPath_,
                     sizeof(wkbkPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     WorkBookDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Workbook Udf Path %s",
                        wkbkPath_);
            }
            fullPath = wkbkPath_;
            break;

        case SharedUDFsDirIndex:
            // For UDFs not in workbook scope (i.e. shared between workbooks)
            snprintf(sharedUDFsPath_,
                     sizeof(sharedUDFsPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     SharedUDFsDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Shared Udf Path %s",
                        sharedUDFsPath_);
            }
            fullPath = sharedUDFsPath_;
            break;

        case RecoveryDirIndex:
            snprintf(recoveryLogPath_,
                     sizeof(recoveryLogPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     RecoveryDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "RecoveryLogPath %s",
                        recoveryLogPath_);
            }
            fullPath = recoveryLogPath_;
            break;

        case TargetDirIndex:
            snprintf(targetPath_,
                     sizeof(targetPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     TargetDirName);
            if (syslogPaths) {
                xSyslog(moduleName, XlogInfo, "TargetPath %s", targetPath_);
            }
            fullPath = targetPath_;
            break;

        case AppPersistDirIndex:
            snprintf(appPersistPath_,
                     sizeof(appPersistPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     AppPersistDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "AppPersistPath %s",
                        appPersistPath_);
            }
            fullPath = appPersistPath_;
            break;

        case PublishedDirIndex:
            snprintf(publishedTablePath_,
                     sizeof(publishedTablePath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     PublishedDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "PublishDirName %s",
                        publishedTablePath_);
            }
            fullPath = publishedTablePath_;
            break;

        case LicenseDirIndex:
            snprintf(licenseDirPath_,
                     sizeof(licenseDirPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     LicenseDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "LicenseDirPath %s",
                        licenseDirPath_);
            }
            fullPath = licenseDirPath_;
            break;

        case JupyterDirIndex:
            snprintf(jupyterPath_,
                     sizeof(jupyterPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     JupyterDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Jupyter notebooks path %s",
                        jupyterPath_);
            }
            fullPath = jupyterPath_;
            break;

        case XcalarRootDirIndex:
            continue;

        case DataflowStatsHistoryDirIndex:
            snprintf(dfStatsDirPath_,
                     sizeof(dfStatsDirPath_),
                     "%s/%s",
                     XcalarConfig::get()->xcalarRootCompletePath_,
                     DfStatsHistoryDirName);
            if (syslogPaths) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Dataflow stats history path %s",
                        dfStatsDirPath_);
            }
            fullPath = dfStatsDirPath_;
            break;

        default:
            assert(false);
            status = StatusFailed;
            goto CommonExit;
            break;
        }

        // Copy in this log's directory to the array
        if (strlcpy(this->logDirPaths[dirIndex],
                    fullPath,
                    sizeof(this->logDirPaths[0])) >=
            sizeof(this->logDirPaths[0])) {
            xSyslog(moduleName,
                    XlogErr,
                    "Log directory name %s too long",
                    fullPath);
            status = StatusNoBufs;
            goto CommonExit;
        }
        if (((DirIndex) dirIndex) == SessionsDirIndex) {
            // skip dir creation since this dir path is constructed only
            // for use by upgrade but not to create this dir in new installs
            // XXX: should delete this (sessions dir code) in Eos
            continue;
        }
        ret = mkdir(fullPath, S_IRWXU);
        if (ret == -1 && errno != EEXIST) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Cannot create '%s' directory in Xcalar root",
                    fullPath);
            goto CommonExit;
        }

        // Check r/w permissions on folder
        ret = access(fullPath, R_OK | W_OK);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Read/write access not permitted for %s",
                    fullPath);
            goto CommonExit;
        }
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogCrit,
                "Log initialization failed: %s.",
                strGetFromStatus(status));
    }
    return status;
}

Status
LogLib::logInitStats()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup(moduleName, &this->statGroupId, 7);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->statOpenLogCount);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statOpenLogCountMaxReached);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statLogWrites);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statLogReads);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statLogHeadResets);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statLogFailedReads);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->statLogFailedWrites);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "openLogHandles",
                                         this->statOpenLogCount,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "openLogHandlesMaxReached",
                                         this->statOpenLogCountMaxReached,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "logRecordReads",
                                         this->statLogReads,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "logRecordWrites",
                                         this->statLogWrites,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "logHeadResets",
                                         this->statLogHeadResets,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "failedLogRecordReads",
                                         this->statLogFailedReads,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->statGroupId,
                                         "failedLogRecordWrites",
                                         this->statLogFailedWrites,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    // XXX Cleanup on failure.
    return status;
}

// Called once to initialize the log module during init.
// Create all dirs (for example sessions dir) under xcalarRootPath at startup.
// Individual files within these dirs can be created whenever.
Status
LogLib::createInternal()
{
    assertStatic(sizeof(LogHeader) == 64);
    assertStatic(sizeof(LogFooter) == 64);
    // Assume -D_FILE_OFFSET_BITS=64 or further checks are necessary.
    assertStatic(sizeof(off_t) == sizeof(off64_t));

    Status status;

    // Increase the limit on the number of fds this process can create.
    struct rlimit fdLimit;
    memZero(&fdLimit, sizeof(fdLimit));

    if (getrlimit(RLIMIT_NOFILE, &fdLimit) != 0) {
        return sysErrnoToStatus(errno);
    }

    fdLimit.rlim_cur = fdLimit.rlim_max;
    if (setrlimit(RLIMIT_NOFILE, &fdLimit) != 0) {
        return sysErrnoToStatus(errno);
    }

    status = logInitStats();
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogInfo,
            "xcalarRootPath %s",
            XcalarConfig::get()->xcalarRootCompletePath_);

    this->openHandleList = NULL;

    if (strlcpy(this->logDirPaths[XcalarRootDirIndex],
                XcalarConfig::get()->xcalarRootCompletePath_,
                sizeof(this->logDirPaths[0])) >= sizeof(this->logDirPaths[0])) {
        status = StatusNoBufs;
        xSyslog(moduleName,
                XlogErr,
                "Failed to access Xcalar root (%s): "
                "must be less than %lu chars long",
                XcalarConfig::get()->xcalarRootCompletePath_,
                sizeof(this->logDirPaths[0]));
    }

    try {
        xceVersion = versionGetStr();
    } catch (std::exception &e) {
        status = StatusNoMem;
        goto CommonExit;
    }

    // Create all the system directories needed under xcalar root path.
    status = createXcalarDirs();
    BailIfFailed(status);

    LogLib::logIsInit = true;

CommonExit:
    return status;
}

// Called once during shutdown.
void
LogLib::deleteSingleton()
{
    Handle *handle;
    if (!logIsInit) {
        return;
    }

    LogLib *logLib = LogLib::get();

    logLib->openHdlLock.lock();

    while ((handle = logLib->removeHeadFromLogHandleList(
                &logLib->openHandleList)) != NULL) {
        logLib->logCloseInt(handle);
        StatsLib::statNonAtomicDecr(logLib->statOpenLogCount);
    }

    assert(logLib->openHandleList == NULL);

    logLib->openHdlLock.unlock();

    logLib->logIsInit = false;

    logLib->~LogLib();
    memFree(logLib);
}

//
// Functions for opening and closing log files.
//

// Assumes log is at beginning. Checks to see if there's a header at the start
// of the log. Returns StatusEof if there isn't (meaning there's no meaningful
// data already in the log).  There is also an assmption that this is done as
// part of log open, so it's o.k. to move to the next datast if necessary.
Status
LogLib::logCheckForHeader(Handle *log)
{
    Status status;

    off_t restorePos = -1;

    uint64_t magic = 0;
    bool triedNext = false;
    bool success = false;
    assert(log->currentFile->fileNum == log->head.fileNum);

    restorePos = lseek(log->currentFile->fd, 0, SEEK_CUR);
    if (restorePos == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    do {
        status = FileUtils::convergentRead(log->currentFile->fd,
                                           &magic,
                                           sizeof(magic),
                                           NULL);
        BailIfFailed(status);

        if (magic == LogHeaderMagic) {
            success = true;
        } else {
            if (magic == 0) {
                status = StatusEof;
                goto CommonExit;
            } else {
                // If the first record written would not fit in the dataset,
                // there may be nothing but a "next" indictor.  Try the next
                // log file, if there is one.
                if (magic == LogNextFileMagic && log->backingFileCount > 1 &&
                    !triedNext) {
                    off_t resultPos;

                    resultPos =
                        lseek(log->currentFile->fd, restorePos, SEEK_SET);
                    if (resultPos == -1) {
                        restorePos = -1;
                        status = sysErrnoToStatus(errno);
                        goto CommonExit;
                    }
                    status = logSeekToNextFile(log, 0);
                    BailIfFailed(status);
                    triedNext = true;
                } else {
                    xSyslog(moduleName,
                            XlogWarn,
                            "Invalid header 0x%16lx found while reading"
                            " log file %s",
                            magic,
                            log->currentFile->fileName);
                    status = StatusLogCorrupt;
                    goto CommonExit;
                }
            }
        }
    } while (!triedNext && !success);

CommonExit:
    if (restorePos != -1) {
        off_t resetPos;
        resetPos = lseek(log->currentFile->fd, restorePos, SEEK_SET);
        if (resetPos == -1 && (status == StatusOk || status == StatusEof)) {
            // Fail log creation if this seek fails.
            status = sysErrnoToStatus(errno);
        }
    }

    return status;
}

// metaFileName consists of "<filePrefix>-<metaStr>".
size_t
LogLib::calculateMetaFileNameLen(const char *filePrefix)
{
    return strlen(filePrefix) + strlen(metaStr) + 1;
}

// Verifies that a file operation doesn't overflow the expected boundaries
// of the current backing file (this ensures that the code is wrapping log
// files when it should).
void
LogLib::assertDontOverflowFile(Handle *log, size_t byteCount)
{
#ifdef DEBUG

    off_t pos;
    struct stat attr;

    if (log->backingFileSize != 0) {
        pos = lseek(log->currentFile->fd, 0, SEEK_CUR);
        assert(pos != -1);
        assert(fstat(log->currentFile->fd, &attr) == 0);

        assert((off_t)(pos + byteCount) <= attr.st_size);
    }

#endif  // DEBUG
}

// Loads or creates on-disk log set backing files and initializes in memory log
// set representation.
Status
LogLib::logCreateInt(Handle *log,
                     const char *dirPath,
                     const char *filePrefix,
                     FileCreateModeMask modeMask,
                     uint32_t createFileCount,
                     size_t createFileSize)
{
    Status status;
    LogSetMeta logSetMeta;
    bool backingFilesInited = false;
    int dirPathLen = strlen(dirPath);
    // 1 for '/', 1 for '\0'
    int metaFileNameLen = dirPathLen + 1 + calculateMetaFileNameLen(filePrefix);
    char metaFileName[metaFileNameLen + 1];

    // This must fit in just the dirPath space
    verify((int) strlcpy(metaFileName, dirPath, sizeof(metaFileName)) <
           dirPathLen + 1);
    metaFileName[dirPathLen] = '/';
    status = logGetMetaFileName(filePrefix,
                                metaFileName + dirPathLen + 1,
                                sizeof(metaFileName) - dirPathLen - 1);
    BailIfFailed(status);

    log->backingFilesList = NULL;
    backingFilesInited = true;

    status = logLoadOrCreateFiles(log,
                                  dirPath,
                                  filePrefix,
                                  modeMask,
                                  &logSetMeta,
                                  createFileCount,
                                  createFileSize);
    BailIfFailed(status);
    // Keep open handle to meta file.
    log->metaFd = open(metaFileName, O_CLOEXEC | O_RDWR);
    if (log->metaFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // Create cursor to beginning of log.
    log->head.fileNum = logSetMeta.logSetStartFile;
    log->head.fileOffset = logSetMeta.logSetStartOffset;

    // Initialize lastWritten to head so that resetHead(lastWritten) will
    // have no effect.
    log->lastWritten = log->head;

    status = seekToBeginning(log);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        if (backingFilesInited) {
            // Close any open files, destroy list.
            logCloseInt(log);
        }
    }

    return status;
}

// Create a new log or open an existing log in the directory given by
// DirIndex with the prefix filePrefix (suffixes implementation dependent).
// log will be initialized with log properties on success and must be passed
// to close before being freed. createFileSize and createFileCount only apply
// if the log doesn't already exist (see FileReturnErrorIfExists). They give
// size and file count of new log backing files.
Status
LogLib::create(Handle *log,
               DirIndex dirIndex,
               const char *filePrefix,
               FileCreateModeMask modeMask,
               uint32_t createFileCount,
               size_t createFileSize)
{
    Status status;
    bool closeLogOnFailure = false;
    uint64_t maxCountReached;
    uint64_t currentCount = 0;

    memZero(log, sizeof(*log));

    log->metaFd = -1;
    log->dirIndex = dirIndex;
    log->nextWriteSeqNum = LogInitNextWriteSeqNum;
    log->lastReadSeqNum = LogInitLastReadSeqNum;

    const size_t filePrefixSize = strlen(filePrefix) + 1;
    log->filePrefix = (char *) memAllocExt(filePrefixSize, __PRETTY_FUNCTION__);
    BailIfNull(log->filePrefix);
    strlcpy(log->filePrefix, filePrefix, filePrefixSize);

    status = logCreateInt(log,
                          this->logDirPaths[dirIndex],
                          filePrefix,
                          modeMask,
                          createFileCount,
                          createFileSize);
    BailIfFailed(status);
    closeLogOnFailure = true;

    if (modeMask & LogLib::FileSeekToStart) {
        assert((modeMask & LogLib::FileSeekToLogicalEnd) == 0);
        status = seekToBeginning(log);
        BailIfFailed(status);

        status = logCheckForHeader(log);
        if (status == StatusEof) {
            log->positionedAtEnd = true;
            status = StatusOk;
        } else if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        assert(modeMask & LogLib::FileSeekToLogicalEnd);
        status = logSeekToLogicalEnd(log, NULL, NULL);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (modeMask & LogLib::FileExpandIfNeeded) {
        log->expandIfNeeded = true;
    } else {
        log->expandIfNeeded = false;
    }

    assert(log != NULL);
    this->openHdlLock.lock();

    if (this->openHandleList == NULL) {
        log->prev = log->next = log;
        this->openHandleList = log;
    } else {
        log->prev = this->openHandleList->prev;
        log->next = this->openHandleList;
        this->openHandleList->prev->next = log;
        this->openHandleList->prev = log;
    }

    StatsLib::statNonAtomicIncr(this->statOpenLogCount);

    currentCount = StatsLib::statReadUint64(this->statOpenLogCount);

    // XXX Because this isn't atomic, it is only an approximation.
    maxCountReached =
        StatsLib::statReadUint64(this->statOpenLogCountMaxReached);
    if (currentCount > maxCountReached) {
        StatsLib::statNonAtomicSet64(this->statOpenLogCountMaxReached,
                                     currentCount);
    }

    this->openHdlLock.unlock();

CommonExit:
    if (status != StatusOk) {
        if (closeLogOnFailure) {
            logCloseInt(log);
        }
    }

    // XXX Caller should detect out-of-disk-space errors and log a message (with
    //     more detail than we can give here.

    return status;
}

// Close a log file.  Log files are automatically closed during shutdown and,
// therefore, it is not normally not necessary to call log close.
//
// This function exists primarily for those cases where multiple logs are
// read and processed by one thread that will not be doing subsequent writes
// or when the desginated writer is changing nodes.
void
LogLib::close(Handle *logFileHandle)
{
    logCloseInt(logFileHandle);
    removeFromLogHandleListWithLock(&this->openHandleList, logFileHandle);
}

// Delete all files associated with a log.
Status
LogLib::fileDelete(DirIndex dirIndex, const char *filePrefix)
{
    Status status;
    unsigned countDeleted;
    unsigned countFailed;

    if (dirIndex >= MaxDirsDoNotAddAfterThisEnum) {
        assert(false);
        return StatusLogHandleInvalid;
    }

    char pattern[strlen(filePrefix) + 3];
    snprintf(pattern, sizeof(pattern), "%s-*", filePrefix);

    status = FileUtils::unlinkFiles(this->logDirPaths[dirIndex],
                                    pattern,
                                    &countDeleted,
                                    &countFailed);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Could not delete all log files with prefix %s. "
                "%d were deleted successfully while %d failed. "
                "Error '%s' was encountered.",
                filePrefix,
                countDeleted,
                countFailed,
                strGetFromStatus(status));
    }

    return status;
}

// Close and delete a log.
Status
LogLib::closeAndDelete(Handle *log)
{
    const size_t filePrefixSize = strlen(log->filePrefix) + 1;
    char filePrefix[filePrefixSize];
    strlcpy(filePrefix, log->filePrefix, filePrefixSize);

    assert(log->dirIndex < MaxDirsDoNotAddAfterThisEnum);
    logCloseInt(log);

    removeFromLogHandleListWithLock(&this->openHandleList, log);

    return fileDelete(log->dirIndex, filePrefix);
}

//
// Functions for writing log records.
//

// Gets bytes remaining for write in current log backing file.
Status
LogLib::logFileGetRemainingBytes(Handle *log, size_t *bytes)
{
    struct stat attr;
    off_t pos;
    int ret;

    if (log->backingFileSize == 0) {
        // UINT64_MAX may lead to arithmetic errors depending on what someone
        // does with the size returned, so I'm suggesting we return
        // UINT64_MAX/2 instead.
        *bytes = UINT64_MAX / 2;
        return StatusOk;
    }

    ret = fstat(log->currentFile->fd, &attr);
    if (ret == -1) {
        *bytes = UINT64_MAX / 2;
        return sysErrnoToStatus(errno);
    }

    pos = lseek(log->currentFile->fd, 0, SEEK_CUR);
    if (pos == -1) {
        *bytes = UINT64_MAX / 2;
        return sysErrnoToStatus(errno);
    }

    if (log->head.fileNum == log->currentFile->fileNum &&
        log->head.fileOffset > pos) {
        assert((attr.st_size - log->head.fileOffset) + pos <= attr.st_size);
        *bytes = attr.st_size - (attr.st_size - log->head.fileOffset) - pos;
    } else {
        *bytes = attr.st_size - pos;
    }
    return StatusOk;
}

// Writes raw data at current position. Wraps to next file in log set if out of
// space in current file.
Status
LogLib::logWriteBytes(Handle *log, uint8_t *buf, size_t bufSize)
{
    Status status;
    off_t ret;

    // Is there enough room in any file for buf, or can we re-allocate?
    if (log->backingFileSize > 0 &&
        bufSize + LogOverheadSize > log->backingFileSize &&
        !log->expandIfNeeded) {
        status = StatusOverflow;
        goto CommonExit;
    }

    // Is there enough room remaining in this file?
    size_t remaining;
    status = logFileGetRemainingBytes(log, &remaining);
    BailIfFailed(status);

    if (bufSize > remaining) {
        // Not enough room in current file. Wrap to next.
        uint64_t continuationRecord = LogNextFileMagic;
        status = FileUtils::convergentWrite(log->currentFile->fd,
                                            &continuationRecord,
                                            sizeof(continuationRecord));
        BailIfFailed(status);

        status = logSeekToNextFile(log, bufSize);
        BailIfFailed(status);

        status = logFileGetRemainingBytes(log, &remaining);
        BailIfFailed(status);

        if (bufSize > remaining) {
            // Caller will likely undo seek.
            status = StatusOverflow;
            goto CommonExit;
        }
    }
    status = logSavePosition(log, &log->lastWritten);
    BailIfFailed(status);

    status = FileUtils::convergentWrite(log->currentFile->fd, buf, bufSize);
    BailIfFailed(status);

    assertDontOverflowFile(log, 0);

    // backup sizeof(uint64_t) bytes to position next write to overwrite
    // LogEndMagic
    ret = lseek(log->currentFile->fd, -sizeof(uint64_t), SEEK_CUR);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    BailIfFailed(status);

CommonExit:
    return status;
}

// Append new log record to end of log containing the data in bufIn.
Status
LogLib::writeRecord(Handle *log,
                    void *bufIn,
                    size_t bufSize,
                    LogLib::WriteOptionsMask optionsMask)
{
    //
    // Validate input.
    //

    if (!(optionsMask & WriteOverwrite) && !log->positionedAtEnd) {
        assert(false);
        return StatusInval;
    }

    const size_t entrySize =
        sizeof(LogHeader) + sizeof(LogFooter) + sizeof(uint64_t) + bufSize;

    if (log->backingFileSize > 0 && entrySize > log->backingFileSize &&
        !log->expandIfNeeded) {
        return StatusLogMaximumEntrySizeExceeded;
    }

    //
    // Save current position to restore on failure.
    //

    Cursor restorePos;
    Cursor restoreLastWritten;

    Status status = logSavePosition(log, &restorePos);
    if (status != StatusOk) {
        return status;
    }

    restoreLastWritten = log->lastWritten;

    //
    // Allocate and populate a buffer to be written to disk. As in traditional
    // database logging, copy callers data onto our heap.
    //

    uint8_t *bufTemp =
        (uint8_t *) memAllocAlignedExt(xcMax((size_t) PageSize,
                                             this->minLogicalBlockAlignment),
                                       entrySize,
                                       __PRETTY_FUNCTION__);
    if (bufTemp == NULL) {
        return StatusNoMem;
    }

    LogHeader *header = (LogHeader *) bufTemp;
    uint8_t *data = &bufTemp[sizeof(LogHeader)];
    LogFooter *footer = (LogFooter *) &data[bufSize];

    // Populate header.
    // TODO Don't directly write structs to disk.
    header->magic = LogHeaderMagic;
    header->majVersion = LogMajorVersion4;
    header->headerSize = sizeof(LogHeader);
    header->dataSize = bufSize;
    header->footerSize = sizeof(LogFooter);
    header->pid = getpid();
    header->seqNum = log->nextWriteSeqNum;  // Incremented on success below.
    memZero(header->reserved, sizeof(header->reserved));

    // Populate data.
    memcpy(data, bufIn, bufSize);

    // Populate footer.
    footer->magic = LogFooterMagic;
    footer->entrySize =
        header->headerSize + header->dataSize + header->footerSize;
    assert(footer->entrySize == entrySize - sizeof(uint64_t));
    memZero(footer->reserved, sizeof(footer->reserved));

    // CRC32c appears to behave differently than other crc and checksum
    // algorithms w.r.t. to incremental calculation.  Unlike those,
    //
    // hashCrc32c(0, buf, bufSize) is *not* equivalent to
    // hashCrc32c(hashCrc32c(0, buf, bufSize - offset), &buf[offset], offset)
    //
    // so we calculate in 3 steps here in order to replicate the calculation
    // in isValidFooter()
    footer->checksum = hashCrc32c(0, header, header->headerSize);
    footer->checksum = hashCrc32c(footer->checksum, data, header->dataSize);
    footer->checksum =
        hashCrc32c(footer->checksum,
                   footer,
                   header->footerSize - sizeof(footer->checksum));

    // Place LogEndMagic denoting the log's end after this record.
    uint64_t *endMagic = (uint64_t *) &footer[1];
    *endMagic = LogEndMagic;

    //
    // Write to disk.
    //

    status = logWriteBytes(log, bufTemp, entrySize);
    BailIfFailed(status);
    log->positionedAtEnd = true;

    StatsLib::statNonAtomicIncr(this->statLogWrites);

CommonExit:

    if (bufTemp != NULL) {
        memAlignedFree(bufTemp);
        bufTemp = NULL;
    }

    if (status == StatusOk) {
        log->nextWriteSeqNum++;
    } else {
        // StatusEof is a normal and expected condition, so do not consider it
        // a failure for diagnostic purposes
        StatsLib::statNonAtomicIncr(this->statLogFailedWrites);

        // Attempt to restore previous state on failure.
        log->lastWritten = restoreLastWritten;  // Header of last real record.
        Status restoreStatus = logSeek(log, &restorePos);
        if (restoreStatus != StatusOk) {
            xSyslog(moduleName,
                    XlogCrit,
                    "Failed to restore log state after failure: %s.",
                    strGetFromStatus(restoreStatus));
        }
    }

    return status;
}

//
// Functions for reading log records.
//

// Reads raw bytes from log set. Caller must guarantee that the entire range
// is contained within the current backing file.
Status
LogLib::logSetReadBytes(Handle *log,
                        uint8_t *buf,
                        size_t bufSize,
                        size_t *bytesReadOut)
{
    Status status;
    assertDontOverflowFile(log, bufSize);

    status = FileUtils::convergentRead(log->currentFile->fd,
                                       buf,
                                       bufSize,
                                       bytesReadOut);
    return status;
}

// Read a record from the current seek offset (or beginning of file if
// LogLib::ReadFromStart is specified) and previously caller supplied data
// into bufIn. Returns StatusOverflow if bufSize isn't large enough.
Status
LogLib::readRecord(Handle *log,
                   void *bufIn,
                   size_t bufSize,
                   size_t *bytesReadOut,
                   ReadMode logReadMode)
{
    Status status = readRecordInternal(log, &bufIn, &bufSize, logReadMode);
    *bytesReadOut = bufSize;
    return status;
}

// This internal function, that does the actual "read record" work, has an
// awkward signature to support both flavors of readRecord.
Status
LogLib::readRecordInternal(Handle *log,
                           void **buf,
                           size_t *bufSize,
                           ReadMode logReadMode)
{
    assert(buf != NULL && "Caller must provide out param");
    assert(bufSize != NULL && "Caller must provide out param");
    assert((logReadMode == ReadFromStart || logReadMode == ReadNext) &&
           "Invalid logReadMode");

    // Build restore point.
    bool restorePositionedAtEnd = log->positionedAtEnd;
    uint64_t restoreReadSeqNum = log->lastReadSeqNum;

    Cursor restorePos;
    Status status = logSavePosition(log, &restorePos);
    if (status != StatusOk) {
        return status;
    }

    LogHeader header;
    LogFooter footer;
    size_t bytesRead;
    bool freeBufOnFail = false;

    if (logReadMode == ReadFromStart) {
        status = seekToBeginning(log);
        BailIfFailed(status);
    }

    // Read and validate header.
    status = logReadNextHeader(log, &header);
    BailIfFailed(status);

    if (!logIsValidHeader(log, &header)) {
        status = StatusEof;
        goto CommonExit;
    }

    log->lastReadSeqNum = header.seqNum;

    if (*buf != NULL) {
        // Caller allocated buffer.
        if (*bufSize < header.dataSize) {
            *bufSize = header.dataSize;
            status = StatusOverflow;
            goto CommonExit;
        }
        *bufSize = header.dataSize;
    } else {
        // Callee allocated buffer.
        *bufSize = header.dataSize;
        *buf = memAlloc(header.dataSize);
        BailIfNull(*buf);
        freeBufOnFail = true;
    }

    // Read data.
    status = logSetReadBytes(log,
                             static_cast<uint8_t *>(*buf),
                             header.dataSize,
                             &bytesRead);
    BailIfFailed(status);

    if (bytesRead < header.dataSize) {
        // Log record cut off.
        status = StatusEof;
        goto CommonExit;
    }

    // Read header.
    status =
        logSetReadBytes(log, (uint8_t *) &footer, sizeof(footer), &bytesRead);
    BailIfFailed(status);

    if (bytesRead < sizeof(footer) ||
        !logIsValidFooter(&footer, &header, static_cast<uint8_t *>(*buf))) {
        status = StatusEof;
        goto CommonExit;
    }

    log->nextWriteSeqNum = xcMax(header.seqNum + 1, log->nextWriteSeqNum);
    StatsLib::statNonAtomicIncr(this->statLogReads);

CommonExit:
    if (status != StatusOk) {
        if (*buf != NULL && freeBufOnFail) {
            memFree(*buf);
            *buf = NULL;
        }

        // StatusEof is a normal and expected condition, so do not consider it
        // a failure for diagnostic purposes
        if (status != StatusEof) {
            StatsLib::statNonAtomicIncr(this->statLogFailedReads);
        }
        Status statusRestore = logSeek(log, &restorePos);
        if (statusRestore == StatusOk) {
            log->positionedAtEnd = restorePositionedAtEnd;
            log->lastReadSeqNum = restoreReadSeqNum;
        } else {
            xSyslog(moduleName,
                    XlogCrit,
                    "Unable to restore log state after failed read.");
        }
    }

    if (status == StatusEof) {
        log->positionedAtEnd = true;
    }

    return status;
}

// Read the last correct (uncorrupted/unfractured) logical block written.
// buf and bufLen define the buffer into which the log data will be read.
Status
LogLib::readLastLogicalRecord(Handle *log,
                              void *bufIn,
                              size_t bufSize,
                              size_t *bytesReadOut)
{
    Status status;

    status = readLastLogicalRecordInt(log);
    if (status != StatusOk) {
        return status;
    }

    status = readRecord(log, bufIn, bufSize, bytesReadOut, ReadNext);
    if (status != StatusOk) {
        return status;
    }
    log->positionedAtEnd = true;

    return status;
}

Status
LogLib::readLastLogicalRecordInt(Handle *log)
{
    Status status;
    Cursor lastRecord;
    bool foundLastRecord;

    status = logSeekToLogicalEnd(log, &lastRecord, &foundLastRecord);
    if (status != StatusOk) {
        return status;
    } else if (!foundLastRecord) {
        return StatusEof;
    }

    status = logSeek(log, &lastRecord);
    if (status != StatusOk) {
        return status;
    }

    // lastReadSeqNum currently points to the last record. Subtract 1 to expect
    // that record.
    log->lastReadSeqNum -= 1;

    return status;
}

// Return the required memory alignment required for log writes
size_t
LogLib::getMinLogicalBlockAlignment()
{
    return this->minLogicalBlockAlignment;
}

// Get a Cursor pointing to the last written log record.
void
LogLib::getLastWritten(Handle *log, Cursor *lastWritten)
{
    *lastWritten = log->lastWritten;
}

// Set the log head to the given cursor.
Status
LogLib::resetHead(Handle *log, Cursor *head)
{
    Status status;
    LogSetMeta meta;
    off_t ret;

    assert(log->backingFileSize > 0);

    ret = lseek(log->metaFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = FileUtils::convergentRead(log->metaFd, &meta, sizeof(meta), NULL);
    BailIfFailed(status);

    meta.logSetStartFile = head->fileNum;
    meta.logSetStartOffset = head->fileOffset;

    ret = lseek(log->metaFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = FileUtils::convergentWrite(log->metaFd, &meta, sizeof(meta));
    BailIfFailed(status);

    log->head = *head;

    StatsLib::statNonAtomicIncr(this->statLogHeadResets);

CommonExit:
    return status;
}

const char *
LogLib::getDirPath(DirIndex dirIndex)
{
    assert(dirIndex < MaxDirsDoNotAddAfterThisEnum);
    return logDirPaths[dirIndex];
}

const char *
LogLib::getSessionDirName()
{
    return sessionDirName_;
}

void
LogLib::setSessionDirName(const char *newDirName)
{
    strlcpy(sessionDirName_, newDirName, XcalarApiMaxPathLen);
}
