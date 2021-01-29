// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ftw.h>
#include <stdio.h>

#include "LibUtilConstants.h"
#include "util/FileUtils.h"
#include "util/System.h"
#include "strings/String.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "FileUtils";

using namespace util;

Status
FileUtils::convergentRead(int fd,
                          void *bufIn,
                          size_t numBytes,
                          size_t *bytesRead)
{
    Status status = StatusOk;
    uint8_t *buf = (uint8_t *) bufIn;
    ssize_t totalRead = 0;
    ssize_t thisRead;

    do {
        thisRead = read(fd, &buf[totalRead], numBytes - totalRead);
        if (thisRead > 0) {
            totalRead += thisRead;
        } else {
            status = StatusEof;
            break;
        }
    } while (((thisRead == -1 && errno == EINTR) || thisRead > 0) &&
             (totalRead < (ssize_t) numBytes));
    if (thisRead == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

CommonExit:
    if (bytesRead) {
        assert(totalRead >= 0);
        *bytesRead = (size_t) totalRead;
    }
    return status;
}

Status
FileUtils::convergentWrite(int fd, const void *bufIn, size_t count)
{
    uint8_t *buf = (uint8_t *) bufIn;
    ssize_t totalWritten = 0;
    ssize_t thisWritten;

    do {
        thisWritten = write(fd, &buf[totalWritten], count - totalWritten);
        if (thisWritten > 0) {
            totalWritten += thisWritten;
        }
    } while (((thisWritten == -1 && errno == EINTR) || thisWritten >= 0) &&
             (totalWritten < (ssize_t) count));
    if (thisWritten == -1) {
        return sysErrnoToStatus(errno);
    }

    return StatusOk;
}

Status
FileUtils::unlinkFiles(const char *dirPath,
                       const char *namePattern,
                       unsigned *numDeleted,
                       unsigned *numFailed)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    int dirPathLen = strlen(dirPath);
    struct dirent *curEnt;
    unsigned countDeleted = 0;
    unsigned countFailed = 0;

    dirIter = opendir(dirPath);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // We need to set errno before every execution of this in order to catch
    // errors this can be avoided by using readdir_r
    errno = 0;
    for (curEnt = readdir(dirIter); curEnt != NULL; curEnt = readdir(dirIter)) {
        if (!strMatch(namePattern, curEnt->d_name) ||
            strcmp(curEnt->d_name, ".") == 0 ||
            strcmp(curEnt->d_name, "..") == 0) {
            errno = 0;
            continue;
        }
        int fullFileLen = dirPathLen + 1 + strlen(curEnt->d_name);
        char fullFilePath[fullFileLen + 1];
        verify(snprintf(fullFilePath,
                        sizeof(fullFilePath),
                        "%s/%s",
                        dirPath,
                        curEnt->d_name) < (int) sizeof(fullFilePath));

        int ret;
        struct stat fileStat;
        ret = stat(fullFilePath, &fileStat);
        if (ret == -1) {
            ++countFailed;
            if (status == StatusOk) {
                status = sysErrnoToStatus(errno);
            }
            xSyslog(moduleName,
                    XlogWarn,
                    "Failed to stat %s: %s",
                    fullFilePath,
                    strGetFromStatus(status));
            errno = 0;
            continue;
        }

        if (!S_ISDIR(fileStat.st_mode)) {
            ret = unlink(fullFilePath);
            if (ret == -1) {
                ++countFailed;
                if (status == StatusOk) {
                    status = sysErrnoToStatus(errno);
                }
                xSyslog(moduleName,
                        XlogWarn,
                        "Failed to unlink %s: %s",
                        fullFilePath,
                        strGetFromStatus(status));
            }
            // Required by POSIX.1-2008 after unlink
            rewinddir(dirIter);
            xSyslog(moduleName,
                    XlogDebug,
                    "Deleting %s: %s",
                    fullFilePath,
                    strGetFromStatus(status));
            errno = 0;
            continue;
        }

        ++countDeleted;
        // Reset errno so we can determine an error with readdir
        errno = 0;
    }
    if (errno && status == StatusOk) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
CommonExit:
    if (dirIter) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }
    if (numDeleted) {
        *numDeleted = countDeleted;
    }
    if (numFailed) {
        *numFailed = countFailed;
    }
    return status;
}

Status
FileUtils::copyFile(const char *dstPath, const char *srcPath)
{
    Status status;
    char buf[CopyFileBufLen];
    int srcFd = -1;
    int dstFd = -1;
    size_t bytesRead;

    srcFd = open(srcPath, O_CLOEXEC | O_RDONLY);
    if (srcFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    dstFd = open(dstPath,
                 O_CREAT | O_EXCL | O_CLOEXEC | O_WRONLY,
                 S_IRWXU | S_IRWXG | S_IRWXO);
    if (dstFd == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    while (true) {
        status = convergentRead(srcFd, buf, sizeof(buf), &bytesRead);
        if (status != StatusOk && status != StatusEof) {
            goto CommonExit;
        }
        status = StatusOk;
        if (!bytesRead) {
            break;
        }

        status = convergentWrite(dstFd, buf, bytesRead);
        BailIfFailed(status);
    };

CommonExit:
    if (srcFd != -1) {
        FileUtils::close(srcFd);
        srcFd = -1;
    }
    if (dstFd != -1) {
        FileUtils::close(dstFd);
        dstFd = -1;
    }
    return status;
}

Status
FileUtils::copyDirectoryFiles(const char *dstDirPath, const char *srcDirPath)
{
    Status status = StatusOk;
    DIR *dirIter = NULL;
    int srcDirLen = strlen(srcDirPath);
    int dstDirLen = strlen(dstDirPath);
    struct dirent *curEnt;

    dirIter = opendir(srcDirPath);
    if (dirIter == NULL) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // We need to set errno before every execution of this in order to catch
    // errors this can be avoided by using readdir_r
    errno = 0;
    for (curEnt = readdir(dirIter); curEnt != NULL; curEnt = readdir(dirIter)) {
        int srcFileLen = srcDirLen + 1 + strlen(curEnt->d_name);
        int dstFileLen = dstDirLen + 1 + strlen(curEnt->d_name);
        char srcFilePath[srcFileLen + 1];
        char dstFilePath[dstFileLen + 1];
        verify(snprintf(srcFilePath,
                        sizeof(srcFilePath),
                        "%s/%s",
                        srcDirPath,
                        curEnt->d_name) < (int) sizeof(srcFilePath));
        verify(snprintf(dstFilePath,
                        sizeof(dstFilePath),
                        "%s/%s",
                        dstDirPath,
                        curEnt->d_name) < (int) sizeof(dstFilePath));

        int ret;
        struct stat fileStat;
        ret = stat(srcFilePath, &fileStat);
        if (ret == -1) {
            if (status == StatusOk) {
                status = sysErrnoToStatus(errno);
            }
            errno = 0;
            continue;
        }

        if (S_ISDIR(fileStat.st_mode)) {
            errno = 0;
            continue;
        }

        status = copyFile(dstFilePath, srcFilePath);
        BailIfFailed(status);

        // Reset errno so we can determine an error with readdir
        errno = 0;
    }
    if (errno && status == StatusOk) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
CommonExit:
    if (dirIter) {
        verify(closedir(dirIter) == 0);
        dirIter = NULL;
    }
    return status;
}

void
FileUtils::close(int fd)
{
    int ret;
    do {
        ret = ::close(fd);
    } while ((ret == -1) && (errno == EINTR));
}

static Status
fallocateFake(int fd, off_t offset, off_t len)
{
    uint8_t zeroBuf[16 * KB] = {0};  // Should fit within most caches.

    off_t bytesWrittenTotal = 0;

    if (len <= 0) {
        return StatusInval;
    }

    int ret = lseek(fd, 0, SEEK_SET);
    if (ret == -1) {
        return sysErrnoToStatus(errno);
    }

    while (bytesWrittenTotal < len) {
        Status status;
        size_t writeLength =
            xcMin(sizeof(zeroBuf), (size_t) len - bytesWrittenTotal);
        status = FileUtils::convergentWrite(fd, zeroBuf, writeLength);
        if (status != StatusOk) {
            return status;
        }
        bytesWrittenTotal += writeLength;
    }
    assert(bytesWrittenTotal == len);
    return StatusOk;
}

Status
FileUtils::fallocate(int fd, int mode, off_t offset, off_t len)
{
    int ret = ::fallocate(fd, mode, offset, len);
    if (ret == -1) {
        if (errno == EOPNOTSUPP) {
            return fallocateFake(fd, offset, len);
        } else {
            return sysErrnoToStatus(errno);
        }
    }
    return StatusOk;
}

static int
ftwRmDir(const char *fpath,
         const struct stat *sb,
         int typeflag,
         struct FTW *ftwbuf)
{
    int ret;
    switch (typeflag) {
    case (FTW_F):
    case (FTW_D):
    case (FTW_DP):
        ret = remove(fpath);
        break;
    case (FTW_DNR):
        // This isn't a readable directory so we must fail
        ret = -1;
        break;
    default:
        ret = -1;
        break;
    }
    return ret;
}

Status
FileUtils::rmDirRecursive(const char *dirPath)
{
    int ret;
    const int nopenfd = 20;  // max number of FDs ftw will consume at once

    if (strlen(dirPath) == 1 && dirPath[0] == '/') {
        assert(false);
        return StatusInval;
    }

    errno = 0;
    // Recursively traverse dir tree from the bottom up, deleting everything
    // ftw stands for File Tree Walk, FTW_DEPTH causes it to be bottom up
    ret = nftw(dirPath, ftwRmDir, nopenfd, FTW_DEPTH | FTW_PHYS);
    if (ret != 0) {
        Status status = sysErrnoToStatus(errno);
        if (status == StatusOk) {
            // Apparently its possible for some implementations to not set
            // errno on error
            return StatusFailed;
        } else {
            return status;
        }
    }
    return StatusOk;
}

// Requires dirPath be under Xcalar root
// Protect against something like rm -rf /
Status
FileUtils::rmDirRecursiveSafer(const char *dirPath)
{
    const char *xlrRoot = XcalarConfig::get()->xcalarRootCompletePath_;

    if (strncmp(xlrRoot, dirPath, strlen(xlrRoot))) {
        assert(false);
        return StatusInval;
    } else {
        return rmDirRecursive(dirPath);
    }
}

Status
FileUtils::recursiveMkdir(const char *dirPath, mode_t mode)
{
    assert(strlen(dirPath) < PATH_MAX);
    int dirPathLen = strlen(dirPath);
    char path[dirPathLen + 1];
    // If this is an absolute path, we don't want to try to make "/"
    bool dirMade = dirPath[0] == '/';

    verify(strlcpy(path, dirPath, sizeof(path)) == sizeof(path) - 1);

    // Walk the string, whenever we see a '/', try a mkdir on it.
    // Watch for repeated '/', since that doesn't represent a new directory
    char *p = path;
    bool done = false;
    while (!done) {
        // dirMade is so we don't try to make "/tmp/myDir" and "/tmp/myDir/"
        if ((*p == '/' || *p == '\0') && (!dirMade)) {
            int ret;
            // Temporarily make the path contain only up till this directory
            if (*p == '/') {
                *p = '\0';
                ret = mkdir(path, mode);
                *p = '/';
            } else {
                ret = mkdir(path, mode);
            }

            if (ret == -1 && errno != EEXIST) {
                return sysErrnoToStatus(errno);
            }

            dirMade = true;
        } else if (*p != '/') {
            dirMade = false;
        }

        // Note that we do this AFTER processing, because the last character
        // might not be a '/', but we still need to mkdir
        if (*p == '\0') {
            done = true;
        }
        ++p;
    }
    return StatusOk;
}

bool
FileUtils::isDirectoryEmpty(const char *dirPath)
{
    assert(strlen(dirPath) < PATH_MAX);
    DIR *dirIter = NULL;
    struct dirent *curEnt;
    unsigned fileCount = 0;

    dirIter = opendir(dirPath);
    if (dirIter == NULL) {
        return true;
    }

    for (curEnt = readdir(dirIter); curEnt != NULL; curEnt = readdir(dirIter)) {
        if (++fileCount > 2) {
            // more than . and ..
            break;
        }
    }
    verify(closedir(dirIter) == 0);
    dirIter = NULL;

    return fileCount <= 2;
}
