// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Functions for manipulating and creating individual log set files.

#include <fcntl.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/types.h>
#include "StrlFunc.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "hash/Hash.h"
#include "log/Log.h"
#include "util/FileUtils.h"
#include "strings/String.h"

// Builds a string that matches the metadata file which describes our backing
// file set. Writes to metaFileNameOut out parameter.
Status
LogLib::logGetMetaFileName(const char *filePrefix,
                           char *metaFileNameOut,
                           size_t metaFileNameOutSize)
{
    size_t filePrefixLen = strlen(filePrefix);
    size_t metaFileNameLen = calculateMetaFileNameLen(filePrefix);
    if (metaFileNameOutSize < metaFileNameLen + 1) {
        assert(false);
        return StatusNameTooLong;
    }

    strlcpy(metaFileNameOut, filePrefix, metaFileNameOutSize);
    metaFileNameOut[filePrefixLen] = '-';
    strlcpy(metaFileNameOut + filePrefixLen + 1,
            metaStr,
            metaFileNameOutSize - filePrefixLen - 1);

    return StatusOk;
}

// Create log set metadata file. Must not already exist.
Status
LogLib::createMetaFile(const char *dirPath,
                       const char *filePrefix,
                       LogSetMeta *logSetMetaOut)
{
    Status status;
    int metaFd = -1;
    int dirPathLen = strlen(dirPath);
    int metaFileNameLen = dirPathLen + 1 + calculateMetaFileNameLen(filePrefix);
    char metaFileName[metaFileNameLen + 1];
    // This must fit in just the dirPath space
    verify((int) strlcpy(metaFileName, dirPath, sizeof(metaFileName)) <
           dirPathLen + 1);

    metaFileName[dirPathLen] = '/';

    // Add 1 to account for the slash we just put
    status = logGetMetaFileName(filePrefix,
                                metaFileName + dirPathLen + 1,
                                sizeof(metaFileName) - dirPathLen - 1);
    // We allocated based off of name length, this cannot fail
    assert(status == StatusOk);

    LogSetMeta logSetMeta;
    memset(&logSetMeta, 0, sizeof(logSetMeta));
    logSetMeta.logSetVersion = LogSetVersion1;
    logSetMeta.logSetStartFile = 0;
    logSetMeta.logSetStartOffset = 0;

    xSyslog(moduleName, XlogInfo, "Creating %s", metaFileName);
    metaFd = open(metaFileName,
                  O_CREAT | O_EXCL | O_CLOEXEC | O_WRONLY,
                  S_IRWXU | S_IRWXG | S_IRWXO);
    if (metaFd == -1) {
        // This will also bail on StatusExist because this function shouldn't
        // be used to open an existing meta file.
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to create %s: %s",
                metaFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        FileUtils::convergentWrite(metaFd, &logSetMeta, sizeof(logSetMeta));
    BailIfFailed(status);

    memcpy(logSetMetaOut, &logSetMeta, sizeof(*logSetMetaOut));

CommonExit:
    if (metaFd != -1) {
        FileUtils::close(metaFd);
        metaFd = -1;
    }

    return status;
}

Status
LogLib::loadMetaFile(const char *dirPath,
                     const char *filePrefix,
                     LogSetMeta *logSetMetaOut)
{
    Status status = StatusOk;
    off_t ret;
    int metaFd = -1;
    int dirPathLen = strlen(dirPath);
    // Add 1 for the '/'
    int fullMetaFileNameLength =
        dirPathLen + 1 + calculateMetaFileNameLen(filePrefix);
    char fullMetaFileName[fullMetaFileNameLength + 1];
    // This must fit in just the dirPath space
    verify((int) strlcpy(fullMetaFileName, dirPath, sizeof(fullMetaFileName)) <
           dirPathLen + 1);

    fullMetaFileName[dirPathLen] = '/';

    // Add 1 to account for the slash we just put
    status = logGetMetaFileName(filePrefix,
                                fullMetaFileName + dirPathLen + 1,
                                sizeof(fullMetaFileName) - dirPathLen - 1);
    // We allocated based off of name length, this cannot fail
    assert(status == StatusOk);

    uint32_t metaFileVersion;

    metaFd = open(fullMetaFileName, O_CLOEXEC | O_RDWR);
    if (metaFd == -1) {
        status = sysErrnoToStatus(errno);
    }

    if (status != StatusOk) {
        // This may return StatusNoEnt indicating it's okay to create LogSet.
        xSyslog(moduleName,
                XlogDebug,
                "Couldn't find log set metadata file '%s': %s.",
                fullMetaFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Read in meta file version. Make sure it's one this version of xcalar can
    // understand.
    status = FileUtils::convergentRead(metaFd,
                                       &metaFileVersion,
                                       sizeof(metaFileVersion),
                                       NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Failed to read log metadata file: %s.",
                strGetFromStatus(status));
        goto CommonExit;
    }
    switch (metaFileVersion) {
    case LogSetVersion1:
        break;

    default:
        xSyslog(moduleName,
                XlogErr,
                "Log version %u not supported.",
                metaFileVersion);
        status = StatusLogVersionMismatch;
        goto CommonExit;
    }

    ret = lseek(metaFd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // Read in full LogSetMeta for this version (this may dirty logSetMeta
    // on failure).
    status = FileUtils::convergentRead(metaFd,
                                       logSetMetaOut,
                                       sizeof(*logSetMetaOut),
                                       NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Failed to read log metadata file: %s.",
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (metaFd != -1) {
        FileUtils::close(metaFd);
        metaFd = -1;
    }

    return status;
}

//
// Functions for loading/creating backing file set.
//

// When given a file with the desired prefix, validates file name and parses
// out file num.
Status
LogLib::parseBackingFileName(const char *filePrefixWithDelim,
                             const char *fileName,
                             uint32_t *fileNumOut)
{
    Status status;

    const size_t filePrefixWithDelimLen = strlen(filePrefixWithDelim);
    const size_t fileNameLen = strlen(fileName);
    const char *fileNumPtr;
    char *fileNumPtrEnd;
    uint32_t fileNum;

    if (strncmp(fileName, filePrefixWithDelim, filePrefixWithDelimLen) != 0) {
        xSyslog(moduleName,
                XlogWarn,
                "File name %s not prefixed with %s of len %lu",
                fileName,
                filePrefixWithDelim,
                filePrefixWithDelimLen);
        status = StatusInval;
        goto CommonExit;
    }

    if (fileNameLen == filePrefixWithDelimLen) {
        xSyslog(moduleName,
                XlogWarn,
                "File name %s prefixed with %s invalid lengths (%lu != %lu)",
                fileName,
                filePrefixWithDelim,
                fileNameLen,
                filePrefixWithDelimLen);
        // Must be characters after prefixWithDelim.
        status = StatusInval;
        goto CommonExit;
    }

    // We know this addition isn't an overflow because of above strncmp.
    fileNumPtr = fileName + filePrefixWithDelimLen;

    // Parse the number at the end of the string.
    fileNum = (uint32_t) strtoul(fileNumPtr, &fileNumPtrEnd, 10);
    if (fileNumPtrEnd != (fileName + fileNameLen)) {
        xSyslog(moduleName,
                XlogWarn,
                "File name %s (%p) len %lu, num %s (%p) invalid end (%p)",
                fileName,
                fileName,
                fileNameLen,
                fileNumPtr,
                fileNumPtr,
                fileNumPtrEnd);
        status = StatusInval;
        goto CommonExit;
    }

    assert(fileNumPtrEnd != fileNumPtr);  // See above check of lengths.

    *fileNumOut = fileNum;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogWarn,
                "Invalid log file, '%s', found in log directory.",
                fileName);
    }
    return status;
}

// Creates the configured number of log set backing files to be used for a new
// log set. Files must not already exist. This function creates a LogSetVersion1
// formatted log set. New versions should create new functions as needed.
Status
LogLib::createBackingFiles(const char *dirPath,
                           const char *filePrefix,
                           BackingFile **logSetBackingFiles,
                           uint32_t createFileCount,
                           size_t createFileSize,
                           FileCreateModeMask modeMask)
{
    Status status = StatusOk;
    int newFd = -1;

    size_t dirPathLen = strlen(dirPath);
    size_t filePrefixLen = strlen(filePrefix);

    assert(createFileCount > 0);
    assert((createFileSize > 0) || (modeMask & LogLib::FileDontPreallocate));
    assert(!((createFileSize > 0) && (modeMask & LogLib::FileDontPreallocate)));

    createFileSize = roundUp(createFileSize, LogFileSizeQuanta);

    uint32_t fileNum;
    for (fileNum = 0; fileNum < createFileCount; fileNum++) {
        size_t fileNameSize =
            dirPathLen + 1 + filePrefixLen + 1 + UInt64MaxStrLen + 1;
        char fileName[fileNameSize];
        verify(snprintf(fileName,
                        fileNameSize,
                        "%s/%s-%u",
                        dirPath,
                        filePrefix,
                        fileNum) < (int) fileNameSize);

        BackingFile *newNode =
            (BackingFile *) memAlloc(sizeof(*newNode) + fileNameSize);
        BailIfNull(newNode);

        newNode->fileNum = fileNum;
        newNode->fd = -1;
        strlcpy(newNode->fileName, fileName, fileNameSize);

        xSyslog(moduleName, XlogNote, "Creating %s", fileName);
        newFd = open(fileName,
                     O_CREAT | O_EXCL | O_CLOEXEC | O_RDWR,
                     S_IRWXU | S_IRWXG | S_IRWXO);
        if (newFd == -1) {
            // This will also bail on StatusExist because this function
            // shouldn't be used to open an existing meta file.
            status = sysErrnoToStatus(errno);
            memFree(newNode);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to create %s: %s",
                    fileName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        newNode->fd = newFd;

        // Add to list as soon as file's created so that it's cleaned up.
        appendBackingFileList(logSetBackingFiles, newNode);

        if (!(modeMask & LogLib::FileDontPreallocate)) {
            status = FileUtils::fallocate(newNode->fd, 0, 0, createFileSize);
            BailIfFailed(status);
            newNode->fileSize = createFileSize;
        } else {
            assert(createFileSize == 0);
            newNode->fileSize = 0;
        }
    }

CommonExit:
    if (status != StatusOk) {
        // On failure, destroy everything this function created.
        BackingFile *fileToRemove;
        while ((fileToRemove = removeHeadFromBackingFileList(
                    logSetBackingFiles)) != NULL) {
            if (fileToRemove->fd != -1) {
                FileUtils::close(fileToRemove->fd);
                fileToRemove->fd = -1;
            }

            unlink(fileToRemove->fileName);
            memFree(fileToRemove);
        }
    }

    return status;
}

// Load existing log set backing files. This function assumes a LogSetVersion1
// formatted log set. New versions should create new functions as needed.
// fileCountOut and fileSizeOut are populated with the encountered file size and
// count respectively.
Status
LogLib::loadBackingFiles(const char *dirPath,
                         const char *filePrefix,
                         BackingFile **logSetBackingFilesList,
                         FileCreateModeMask modeMask,
                         uint32_t *fileCountOut,
                         size_t *fileSizeOut)
{
    Status status;
    DIR *dirIter = NULL;
    BackingFile *currentNode;

    const size_t filePrefixLen = strlen(filePrefix);
    const size_t filePatternLen = filePrefixLen + 2;
    const size_t filePrefixWithDelimLen = filePatternLen - 1;
    BackingFile *fileToRemove;
    uint32_t fileNum;
    size_t fileSize = 0;

    // Build filePattern to allow matching of desired log files
    // filePattern should be "<filePrefix>-*".
    char filePattern[filePatternLen + 1];

    strlcpy(filePattern, filePrefix, sizeof(filePattern));
    filePattern[filePrefixLen] = '-';
    filePattern[filePrefixLen + 1] = '*';
    filePattern[filePrefixLen + 2] = '\0';

    char filePrefixWithDelim[filePrefixWithDelimLen + 1];
    strlcpy(filePrefixWithDelim, filePattern, sizeof(filePrefixWithDelim));
    filePrefixWithDelim[filePrefixWithDelimLen] = '\0';
    uint32_t nextFileNum = 0;

    char metaFileName[calculateMetaFileNameLen(filePrefix) + 1];
    status = logGetMetaFileName(filePrefix, metaFileName, sizeof(metaFileName));
    BailIfFailed(status);

    // Open directory iterator that iterates all files in the caller's directory
    dirIter = opendir(dirPath);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogDebug,
                "Cannot access log directory: %s.",
                strGetFromStatus(status));
        goto CommonExit;
    }

    while (true) {
        int ret;
        // See glibc docs for correct use of readdir/readdir_r/errno and threads
        errno = 0;
        struct dirent *currentFile = readdir(dirIter);
        if (currentFile == NULL) {
            // The only error is EBADF, which should exclusively be a program
            // error
            assert(errno == 0);
            // End of directory.
            break;
        }

        if (strcmp(currentFile->d_name, metaFileName) == 0) {
            // Skip the meta file. Already saw that.
            continue;
        }

        if ((strcmp(currentFile->d_name, ".") == 0) ||
            (strcmp(currentFile->d_name, "..") == 0)) {
            // Ignore non-real directories
            continue;
        }

        if (!strMatch(filePattern, currentFile->d_name)) {
            continue;
        }

        size_t currentFileNameLen = strlen(currentFile->d_name);
        int fullFileLen = strlen(dirPath) + 1 + currentFileNameLen;
        char fullFilePath[fullFileLen + 1];
        verify(snprintf(fullFilePath,
                        sizeof(fullFilePath),
                        "%s/%s",
                        dirPath,
                        currentFile->d_name) < (int) sizeof(fullFilePath));

        struct stat fileStat;

        ret = stat(fullFilePath, &fileStat);
        if (ret == -1) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        }

        if (S_ISDIR(fileStat.st_mode)) {
            xSyslog(moduleName,
                    XlogWarn,
                    "Log directory cannot contain subdirectories (%s).",
                    fullFilePath);
            status = StatusLogCorrupt;
            goto CommonExit;
        }

        // Backing files for a log file set are normally all the same size,
        // but they don't have to be.  Keep track of the largest size.
        if (!(modeMask & LogLib::FileDontPreallocate)) {
            // preallocated log files are never zero size; others might be
            if (fileStat.st_size == 0) {
                xSyslog(moduleName,
                        XlogWarn,
                        "Empty log file, '%s', encountered.",
                        currentFile->d_name);
                status = StatusLogCorrupt;
                goto CommonExit;
            }

            if (fileSize < (size_t) fileStat.st_size) {
                fileSize = fileStat.st_size;
            }
        }

        status = parseBackingFileName(filePrefixWithDelim,
                                      currentFile->d_name,
                                      &fileNum);
        if (status == StatusInval) {
            // Doesn't appear to be a valid log file name
            status = StatusOk;
            continue;
        }
        BailIfFailed(status);

        // Filename is valid. Add to list.
        BackingFile *newBackingFile;
        newBackingFile = (BackingFile *) memAllocExt(sizeof(*newBackingFile) +
                                                         fullFileLen + 1,
                                                     __PRETTY_FUNCTION__);
        BailIfNull(newBackingFile);

        newBackingFile->fileNum = fileNum;
        newBackingFile->fileSize = fileStat.st_size;
        newBackingFile->fd = -1;
        strlcpy(newBackingFile->fileName, fullFilePath, fullFileLen + 1);

        newBackingFile->fd = open(newBackingFile->fileName, O_CLOEXEC | O_RDWR);
        if (newBackingFile->fd == -1) {
            status = sysErrnoToStatus(errno);
            memFree(newBackingFile);
            newBackingFile = NULL;
            goto CommonExit;
        }

        currentNode = *logSetBackingFilesList;
        while (currentNode != NULL) {
            if (newBackingFile->fileNum < currentNode->fileNum) {
                insertBeforeBackingFileList(logSetBackingFilesList,
                                            currentNode,
                                            newBackingFile);
                newBackingFile = NULL;
                break;
            }

            currentNode = currentNode->next;
            if (currentNode == *logSetBackingFilesList) {
                break;
            }
        }

        if (newBackingFile != NULL) {
            appendBackingFileList(logSetBackingFilesList, newBackingFile);
            newBackingFile = NULL;
        }
    }

    currentNode = *logSetBackingFilesList;
    while (currentNode != NULL) {
        if (currentNode->fileNum != nextFileNum) {
            xSyslog(moduleName,
                    XlogWarn,
                    "Missing log file %s%d (got %s%d instead).",
                    filePrefixWithDelim,
                    nextFileNum,
                    filePrefixWithDelim,
                    currentNode->fileNum);
            status = StatusLogCorrupt;
            goto CommonExit;
        }

        nextFileNum++;

        currentNode = currentNode->next;
        if (currentNode == *logSetBackingFilesList) {
            break;
        }
    }

    if (fileCountOut != NULL) {
        *fileCountOut = nextFileNum;
    }
    if (fileSizeOut != NULL) {
        *fileSizeOut = fileSize;
    }

CommonExit:
    // General cleanup.
    if (dirIter != NULL) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }

    if (status != StatusOk) {
        // Failure specific cleanup.
        while ((fileToRemove = removeHeadFromBackingFileList(
                    logSetBackingFilesList)) != NULL) {
            if (fileToRemove->fd != -1) {
                FileUtils::close(fileToRemove->fd);
                fileToRemove->fd = -1;
            }

            memFree(fileToRemove);
        }
    }

    return status;
}

// Attempts to load existing log set from disk. Certain errors (e.g. an
// unsupported version) will cause log load to stop immediately.
Status
LogLib::logLoadOrCreateFiles(Handle *log,
                             const char *dirPath,
                             const char *filePrefix,
                             FileCreateModeMask modeMask,
                             LogSetMeta *logSetMetaOut,
                             uint32_t createFileCount,
                             size_t createFileSize)
{
    Status status;

    LogSetMeta logSetMeta;

    uint32_t loadFileCount;
    size_t loadFileSize;

    status = loadMetaFile(dirPath, filePrefix, &logSetMeta);
    if (status == StatusOk) {
        if (modeMask & LogLib::FileReturnErrorIfExists) {
            status = StatusExist;
            goto CommonExit;
        }

        status = loadBackingFiles(dirPath,
                                  filePrefix,
                                  &log->backingFilesList,
                                  modeMask,
                                  &loadFileCount,
                                  &loadFileSize);
        if (status == StatusOk) {
            // loadFileCount/Size may not equal createFileCount/Size
            log->backingFileCount = loadFileCount;
            log->backingFileSize = loadFileSize;
        } else {
            // Something prevented the loading of the backing files.
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to load backing files: path '%s', prefix '%s': %s",
                    dirPath,
                    filePrefix,
                    strGetFromStatus(status));
        }

        goto CommonExit;
    }

    if (!(modeMask & LogLib::FileNoCreate)) {
        // Create the files.  These fail if the files already exist.
        status = createMetaFile(dirPath, filePrefix, &logSetMeta);
        BailIfFailed(status);

        status = createBackingFiles(dirPath,
                                    filePrefix,
                                    &log->backingFilesList,
                                    createFileCount,
                                    createFileSize,
                                    modeMask);
        BailIfFailed(status);

        log->backingFileCount = createFileCount;
        log->backingFileSize = createFileSize;
    }

CommonExit:
    if (status == StatusOk) {
        memcpy(logSetMetaOut, &logSetMeta, sizeof(*logSetMetaOut));
    }

    return status;
}
