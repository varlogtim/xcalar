// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
#include <dirent.h>
#include <fnmatch.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <stdio.h>
#include <linux/limits.h>
#include <unistd.h>

#include "xdb/Xdb.h"
#include "xdb/XdbInt.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "util/FileUtils.h"
#include "util/WorkQueue.h"
#include "xccompress/XcSnappy.h"
#include "msg/Xid.h"
#include "runtime/Tls.h"

static constexpr const char *moduleName = "libxdb";
//
// Create a new paging file with number fileNum.
//
Status
XdbMgr::openPagingFile(uint32_t fileNum)
{
    Status status = StatusOk;
    assert(fileNum <= maxPagingFileCount_);
    char fname[XcalarApiMaxPathLen];
    char pagingFileName[XcalarApiMaxPathLen];

    status = xdbGetSerDesFname(fileNum, fname, sizeof(fname));
    BailIfFailed(status);
    status = strSnprintf(pagingFileName,
                         sizeof(pagingFileName),
                         "%s/%s",
                         xdbSerPath_,
                         fname);
    BailIfFailed(status);

    xSyslog(moduleName,
            XlogInfo,
            "Opening paging file %d: %s",
            fileNum,
            pagingFileName);
    pagingFileFds_[fileNum] =
        open(pagingFileName, O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (pagingFileFds_[fileNum] < 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to allocate pagingFileName : %s",
                strGetFromStatus(status));
    }
    return (status);
}

#define MOUNT_INFO_FILE "/proc/self/mounts"

//
// Get the file system type from the mounts proc node.
//
Status
getFilesystemType(char *searchPath,
                  std::string &mountPoint,
                  std::string &fsType)
{
    Status status = StatusOk;
    char *line = NULL;
    FILE *fp = fopen(MOUNT_INFO_FILE, "r");

    if (fp == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to open %s: %s",
                MOUNT_INFO_FILE,
                strGetFromStatus(status));
        status = StatusNoMem;
        goto CommonExit;
    }

    line = (char *) malloc(PATH_MAX);
    BailIfNull(line);

    while (fgets(line, PATH_MAX, fp) != NULL) {
        //
        // Format is: device mount-point filesystem-type other-stuff
        //
        strtok(line, " ");
        const char *mountPointStr = strtok(NULL, " ");
        if (mountPointStr == NULL) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Malformed line in %s: %s",
                    MOUNT_INFO_FILE,
                    line);
            goto CommonExit;
        }
        if (strncmp(searchPath, mountPointStr, strlen(mountPointStr)) == 0) {
            const char *fsTypeStr = strtok(NULL, " ");
            if (fsTypeStr == NULL) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "Malformed line in %s: %s",
                        MOUNT_INFO_FILE,
                        line);
                goto CommonExit;
            }
            if (mountPoint.empty() ||
                (strlen(mountPointStr) > mountPoint.size())) {
                mountPoint = mountPointStr;
                fsType = fsTypeStr;
            }
        }
    }

    if (status == StatusOk && mountPoint.size() == 0) {
        return StatusNoEnt;
    } else {
        return status;
    }

CommonExit:
    if (line != NULL) {
        free(line);
        line = NULL;
    }

    if (fp != NULL) {
        free(fp);
        fp = NULL;
    }

    return (status);
}

Status
XdbMgr::xdbSerDesInitSparse()
{
    Status status;
    const XcalarConfig *xconfig = XcalarConfig::get();
    //
    // Get the block size of the paging file system.
    //
    struct stat stats;
    if (stat(xdbSerPath_, &stats) < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to stat %s: %s",
                xdbSerPath_,
                strGetFromStatus(status));
        goto CommonExit;
    }
    pagingFileBlockSize_ = stats.st_blksize;
    maxPagingFileSize_ = xconfig->xdbMaxPagingFileSize_;
    xSyslog(moduleName, XlogInfo, "maxPagingFileSize_=%ld", maxPagingFileSize_);
    if (maxPagingFileSize_ == 0) {
        maxPagingFileSize_ = XcalarConfig::XdbMaxPagingFileSizeDefault;
        std::string mountPoint;
        std::string fsType;
        //
        // Get the file system type so we can set the max file size
        // properly.
        //
        if (getFilesystemType(xdbSerPath_, mountPoint, fsType) == StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Paging file system: %s %s",
                    mountPoint.c_str(),
                    fsType.c_str());
            if (strcasecmp(fsType.c_str(), "ext4") == 0) {
                xSyslog(moduleName, XlogInfo, "Paging to ext4 file system");
                //
                // ext4 file system maximum file size is 16 TB minus one
                // based on a test program that I wrote.
                //
                maxPagingFileSize_ = ((uint64_t) 16 * TB) - 1;
            } else if (strcasecmp(fsType.c_str(), "xfs") == 0) {
                xSyslog(moduleName, XlogInfo, "Paging to xfs file system");
                //
                // xfs file system maximum file size is 100 TB for RHEL5 and
                // higher in RHEL6, 7, and 8. See
                // https://access.redhat.com/articles/rhel-limits
                //
                // I have been unable to get a file too large error on
                // Ubuntu 18 even at 16 PB.
                //
                maxPagingFileSize_ = ((uint64_t) 100 * TB);
            } else {
                xSyslog(moduleName,
                        XlogInfo,
                        "Paging file system: %s %s not supporte",
                        mountPoint.c_str(),
                        fsType.c_str());
            }
        }
    }
    //
    // Open the first paging file.
    //
    status = openPagingFile(1);
    BailIfFailed(status);
    pagingFileCount_ = 1;
    nextPagingFileOffset_ = 0;

CommonExit:
    return (status);
}

//
// Decode the file number, block number, and block count from the batchId.
//
Status
XdbMgr::batchIdToPageInfo(uint64_t batchId,
                          uint64_t &fileNum,
                          uint64_t &blockNum,
                          uint32_t &blockCount)
{
    fileNum = batchId >> 52;
    blockCount = (batchId >> 40) & 0xfff;
    blockNum = batchId & 0xffffffff;
    if (fileNum > maxPagingFileCount_) {
        // TODO: Roll back around
        return StatusXdbDesError;
    } else {
        return StatusOk;
    }
}

//
// Encode the file number, block number, and block count into a batchId.
//
Status
XdbMgr::pageInfoToBatchId(uint64_t &batchId,
                          uint64_t fileNum,
                          uint64_t blockNum,
                          uint32_t blockCount)
{
    if (fileNum > maxPagingFileCount_ || fileNum > 0xfff) {
        xSyslog(moduleName, XlogErr, "Invalid fileNum %lu", fileNum);
        assert(false);
        return (StatusXdbSerError);
    }

    if (blockCount > 0xfff) {
        xSyslog(moduleName, XlogErr, "Invalid blockCount %u", blockCount);
        assert(false);
        return (StatusXdbSerError);
    }

    if (blockNum > 0xffffffffff) {
        xSyslog(moduleName, XlogErr, "Invalid blockNum %lu", blockNum);
        assert(false);
        return (StatusXdbSerError);
    }

    batchId = (fileNum << 52) | ((uint64_t) blockCount << 40) | blockNum;
    return (StatusOk);
}

//
// Open the next paging file unless we've hit the maximum number of files.
//
Status
XdbMgr::openNextPagingFile()
{
    Status status;
    if (pagingFileCount_ == maxPagingFileCount_) {
        xSyslog(moduleName, XlogInfo, "Hit max paging file count");
        status = StatusNoMem;
        goto CommonExit;
    } else {
        status = openPagingFile(pagingFileCount_ + 1);
        BailIfFailed(status);
        pagingFileCount_ += 1;
        nextPagingFileOffset_ = 0;
    }
CommonExit:
    return status;
}

//
// Punch a hole in the current paging file to release backing storage.
//
Status
XdbMgr::deallocateBlock(int fd, uint64_t fileOffset, uint32_t blockSize)
{
    int retVal = fallocate(fd,
                           FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                           fileOffset,
                           blockSize);
    if (retVal < 0) {
        return sysErrnoToStatus(errno);
    } else {
        return StatusOk;
    }
}

#define ROUND_UP(length, blockSize) \
    ((length + blockSize - 1) & ~(blockSize - 1))

//
// Page out to a paging file or page in from a paging file.  The batchId encodes
// where to read a page from if we paging in a page and we encode into the
// batchId where we wrote the page if we are paging out a page.
//
Status
XdbMgr::pageToOrFromLocalFile(Xid &batchId,
                              struct iovec *iov,
                              ssize_t iovcnt,
                              bool writingPage)
{
    uint64_t totalLength = 0;
    Status status;

    for (ssize_t i = 0; i < iovcnt; i++) {
        totalLength += iov[i].iov_len;
    }
    uint64_t dataBlockSize = ROUND_UP(totalLength, pagingFileBlockSize_);

    if (writingPage) {
        while (true) {
            pagingFileLock_.lock();
            uint64_t fileNum = pagingFileCount_;
            uint64_t fileOffset = nextPagingFileOffset_;
            nextPagingFileOffset_ += dataBlockSize;
            if (nextPagingFileOffset_ > maxPagingFileSize_) {
                //
                // We are writing past the maximum size for the file so try to
                // open a new one.
                //
                status = openNextPagingFile();
                if (status == StatusOk) {
                    pagingFileLock_.unlock();
                    continue;
                }
            }
            pagingFileLock_.unlock();
#ifdef PAGE_TO_FILE_DEBUG
            xSyslog(moduleName,
                    XlogInfo,
                    "Writing page with length %ld/%ld to file %ld @ offset %ld",
                    totalLength,
                    dataBlockSize,
                    fileNum,
                    fileOffset);
#endif
            ssize_t len =
                pwritev(pagingFileFds_[fileNum], iov, iovcnt, fileOffset);
            if ((size_t) len == totalLength) {
                //
                // Successfully wrote the block so encode the block in the
                // batchId. This is a hack to try out this new way of paging
                // without changing anything outside this function.
                //
                status =
                    pageInfoToBatchId(batchId,
                                      fileNum,
                                      fileOffset / pagingFileBlockSize_,
                                      dataBlockSize / pagingFileBlockSize_);
                BailIfFailed(status);
#ifdef PAGE_TO_FILE_DEBUG
                xSyslog(moduleName,
                        XlogInfo,
                        "Returning batchId 0x%lx",
                        batchId);
#endif
                goto CommonExit;
            } else if (len < 0) {
                if (errno == EFBIG) {
                    xSyslog(moduleName,
                            XlogInfo,
                            "Write failed with file too big");
                    status = StatusOk;
                    pagingFileLock_.lock();
                    if (fileNum == pagingFileCount_) {
                        //
                        // The file number hasn't changed since we tried to
                        // write the file so we can proceed with opening the
                        // next one if we haven't hit the max file count.  If
                        // the file number has changed then will go around the
                        // loop and try again.
                        //
                        status = openNextPagingFile();
                    }
                    pagingFileLock_.unlock();
                    BailIfFailed(status);
                } else {
                    status = sysErrnoToStatus(errno);
                    xSyslog(moduleName,
                            XlogInfo,
                            "Failed to write page with status %s",
                            status.message());
                    goto CommonExit;
                }
            } else if (len == 0) {
                status = StatusNoSpc;
                goto CommonExit;
            } else {
                //
                // We got a short write.  On the EXT4 file system this can
                // happen because the actual largest size of a file is 16TB - 1
                // instead of 16 TB.  So when we try to write the last full
                // block we will get a truncated write. So we hope that the
                // maximum file size is set properly in the config file.
                //
                status = StatusNoSpc;
                goto CommonExit;
            }
        }
    } else {
        uint64_t fileNum;
        uint64_t blockNum;
        uint32_t blockCount;
        status = batchIdToPageInfo(batchId, fileNum, blockNum, blockCount);
        BailIfFailed(status);
        assert(blockCount * pagingFileBlockSize_ == dataBlockSize);
        uint64_t fileOffset = blockNum * pagingFileBlockSize_;
#ifdef PAGE_TO_FILE_DEBUG
        xSyslog(moduleName,
                XlogInfo,
                "Reading in block %ld @ %ld from file %ld for len %ld",
                blockNum,
                fileOffset,
                fileNum,
                totalLength);
#endif
        ssize_t len = preadv(pagingFileFds_[fileNum], iov, iovcnt, fileOffset);
        if ((size_t) len == totalLength) {
            status = deallocateBlock(pagingFileFds_[fileNum],
                                     fileOffset,
                                     dataBlockSize);
            goto CommonExit;
        } else if (len < 0) {
            status = sysErrnoToStatus(errno);
            goto CommonExit;
        } else {
            assert(0);
            status = StatusNoMem;
            goto CommonExit;
        }
    }

CommonExit:
    return (status);
}
