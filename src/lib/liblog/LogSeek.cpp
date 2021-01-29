// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// Functions for moving around within a log set.

#include <fcntl.h>
#include <stdio.h>

#include "sys/XLog.h"
#include "hash/Hash.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "StrlFunc.h"
#include "util/FileUtils.h"

// Saves log's current position within the log set into a Cursor. Allows
// caller to attempt to restore to this position at a later time.
Status
LogLib::logSavePosition(Handle *log, Cursor *cursorOut)
{
    Status status = StatusOk;

    off_t offset = lseek(log->currentFile->fd, 0, SEEK_CUR);
    if (offset == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }
    cursorOut->fileOffset = offset;

    cursorOut->fileNum = log->currentFile->fileNum;

CommonExit:
    return status;
}

// Reallocate the input log file to a larger size.  In a perfect world,
// this would be done by calling truncate, but we don't have that support.
// Instead, we will create a new file of the requested size, substitute it
// for input file and delete the old file.
Status
LogLib::fileReallocate(Handle *log, BackingFile *file, size_t newSize)
{
    Status status = StatusOk;
    int ret;
    bool needToDelete = false;
    int newFd = -1;
    int oldFd = -1;
    size_t baseFileLen = strlen(file->fileName);
    // Add 4 for 'temp', but sub 1 because we overwrite the 1 char num
    size_t tempFileNameLen = baseFileLen + 3;
    // Add 3 for 'old', but sub 1 because we overwrite the 1 char num
    size_t oldFileNameLen = baseFileLen + 2;
    char oldFile[oldFileNameLen + 1];
    char tempFile[tempFileNameLen + 1];
    Status status2 = StatusOk;

    strcpy(oldFile, file->fileName);
    strcpy(tempFile, file->fileName);

    // overwrite numerical suffix to create the temp file name
    strcpy(tempFile + (baseFileLen - 1), "temp");

    // and create a name for our old file
    strcpy(oldFile + (baseFileLen - 1), "old");

    xSyslog(moduleName, XlogInfo, "Creating %s", tempFile);
    newFd = open(tempFile,
                 O_CREAT | O_EXCL | O_CLOEXEC | O_RDWR,
                 S_IRWXU | S_IRWXG | S_IRWXO);
    if (newFd == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to create %s: %s",
                tempFile,
                strGetFromStatus(status));
        goto CommonExit;
    }

    needToDelete = true;

    status = FileUtils::fallocate(newFd, 0, 0, newSize);

    oldFd = file->fd;

    // rename the old file xxx-n  =>  xxx-old
    ret = rename(file->fileName, oldFile);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // switch files in the BackingFile struct
    file->fd = newFd;
    file->fileSize = newSize;
    log->backingFileSize = newSize;

    // There is no going back now.  We will use the new file.
    needToDelete = false;

    // rename the new file nnn-temp => xxx-n
    ret = rename(tempFile, file->fileName);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
    }
    // XXX Presumably, if this rename failed, we are sunk since we probably
    //     can't rename the old file back... we will copy the name over to
    //     the backing file struct so that we don't have to kill the user.
    //     This will allow the user to continue a little longer and eventually
    //     a sysadmin can fix it.
    if (status != StatusOk) {
        // The fileName begins after the directory and the '/'
        xSyslog(moduleName,
                XlogWarn,
                "Unable to rename log file from %s to %s.  Action required.",
                tempFile,
                file->fileName);
        strcpy(file->fileName, tempFile);
    }

    FileUtils::close(oldFd);

    errno = 0;
    status2 = StatusOk;
    ret = unlink(oldFile);
    if (ret != 0) {
        status2 = sysErrnoToStatus(errno);
    }
    xSyslog(moduleName,
            XlogInfo,
            "Deleting %s: %s",
            oldFile,
            strGetFromStatus(status2));

CommonExit:
    // delete new file on failure
    if (needToDelete) {
        FileUtils::close(newFd);

        errno = 0;
        status2 = StatusOk;
        ret = unlink(tempFile);
        if (ret != 0) {
            status2 = sysErrnoToStatus(errno);
        }
        xSyslog(moduleName,
                XlogInfo,
                "Deleting %s: %s",
                tempFile,
                strGetFromStatus(status2));
    }

    return status;
}

// Sets a new "current file" for this log set based on fileNum, an index into
// the list of log set files.
Status
LogLib::setCurrentFile(Handle *log, uint32_t fileNum)
{
    Status status = StatusOk;
    BackingFile *newCurrentFile = NULL;
    BackingFile *fileTmp;
    off_t ret;

    if (log->currentFile != NULL && log->currentFile->fileNum == fileNum) {
        status = StatusOk;
        goto CommonExit;
    }

    fileTmp = log->backingFilesList;

    while (fileTmp != NULL) {
        if (fileNum == fileTmp->fileNum) {
            newCurrentFile = fileTmp;
            break;
        }

        fileTmp = fileTmp->next;
        if (fileTmp == log->backingFilesList) {
            break;
        }
    }

    if (newCurrentFile == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Backing file num %u is NULL (log->dirIndex: %u), prefix: %s",
                fileNum,
                log->dirIndex,
                log->filePrefix);
        status = StatusLogCorrupt;
        goto CommonExit;
    }

    ret = lseek(newCurrentFile->fd, 0, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    log->currentFile = newCurrentFile;

CommonExit:
    return status;
}

// Move to the next log file if it is big enough to write the caller's
// log record.  If the record is too large to fit in the next log file
// we may reallocate, depending on what was specifed for log open.
//
// Expansion criteria: if expandIfNeeded is set in the log handle and
// the log record will not fit in the next dataset, we determine a new
// size and attempt to reallocate the log dataset.  To avoid expanding
// by too much, we will expand to the size of the largest dataset in the
// log set if it is large enough.  Otherwise, we will expand to buffer
// size + 25% under the assumption that subsequent log records will not
// grow too much on each write attempt.
Status
LogLib::logSeekToNextFile(Handle *log, size_t bufferSize)
{
    size_t newSize;
    Status status;

    BackingFile *nextFile = log->currentFile->next;
    assert(nextFile != NULL);

    if (nextFile->fileNum == log->head.fileNum) {
        // XXX About to wrap back to start file. Insert new file.

        // XXX Current log format doesn't lend itself well to inserting new
        //     files. Consider embedding "pointers" to next file instead of
        //     identifying next file by fileNum + 1.
        // XXX Adding a beginning of file log record will allow the datasets
        //     to be sequenced.  Adding a new log file (if needed) to the
        //     chain of files could be done here.
        return StatusOverflow;
    }

    // See if the log record fits.  If not, see if we should try to allocate
    // a larger dataset to contain it.
    if (nextFile->fileSize < bufferSize) {
        if (log->expandIfNeeded) {
            if (log->backingFileSize >= bufferSize) {
                newSize = log->backingFileSize;
            } else {
                newSize = bufferSize + bufferSize / LogIncreaseFactor;
                newSize = roundUp(newSize, LogFileSizeQuanta);
                assert(newSize > bufferSize);
            }

            // We don't have dsTruncate to make the file larger, so we
            // will create a new file (hopefully) with the larger size
            status = fileReallocate(log, nextFile, newSize);
            if (status != StatusOk) {
                return status;
            }
        } else {
            // Record > file size && no expansion was checked in a higher
            // level function, but we have it here just in case.
            return StatusLogMaximumEntrySizeExceeded;
        }
    }

    return setCurrentFile(log, nextFile->fileNum);
}

// Seek to a position within the log set. Allows movement between files. Caller
// is responsible for rolling back to original state on failure. Seek is also
// primarily responsible for keeping the log's lastWritten pointer up to date.
Status
LogLib::logSeek(Handle *log, Cursor *cursor)
{
    Status status = StatusOk;
    off_t ret;

    if (log->currentFile == NULL ||
        log->currentFile->fileNum != cursor->fileNum) {
        status = setCurrentFile(log, cursor->fileNum);
        if (status != StatusOk) {
            return status;
        }
    }

    // Implied by success of setCurrentFile.
    assert(log->currentFile != NULL);

    ret = lseek(log->currentFile->fd, cursor->fileOffset, SEEK_SET);
    if (ret == -1) {
        status = sysErrnoToStatus(errno);
    }
    return status;
}

// Switch files to the start file of this log set (read from log metadata file)
// and seek to the beginning of that file.
Status
LogLib::seekToBeginning(Handle *log)
{
    Status status;

    status = logSeek(log, &log->head);
    if (status != StatusOk) {
        return status;
    }

    log->positionedAtEnd = false;
    log->lastReadSeqNum = 0;

    return status;
}

// Read next header at or after current position.
Status
LogLib::logReadNextHeader(Handle *log, LogHeader *header)
{
    Status status;

    while (true) {
        size_t bytesRead;

        status = FileUtils::convergentRead(log->currentFile->fd,
                                           header,
                                           sizeof(*header),
                                           &bytesRead);

        if (status != StatusOk && status != StatusEof) {
            // Error! Propagate to caller.
            break;
        }

        if (bytesRead < sizeof(header->magic)) {
            // Didn't read anything good.
            status = StatusEof;
            break;
        }

        if (header->magic != LogNextFileMagic) {
            // This could be a legit file or garbage. Pass to caller.
            break;
        }

        // This is a next file pointer.
        status = logSeekToNextFile(log, 0);
        if (status != StatusOk) {
            break;
        }
    }

    return status;
}

// Loops through log set until either log end (LogEndMagic) is found or an
// invalid log entry is reached. If an invalid entry is reached, seek to
// the end of the last valid entry. Returns a Cursor pointing to the start
// of the header of the last valid record via lastRecordOut. If no valid
// records exist, foundLastRecordOut is set to false.
Status
LogLib::logSeekToLogicalEnd(Handle *log,
                            Cursor *lastRecordOut,
                            bool *foundLastRecordOut)
{
    Status status;
    uint8_t *data = NULL;
    uint64_t lastSeqNum = LogInitLastReadSeqNum;
    bool validRecordFound = false;

    if (log->positionedAtEnd && lastRecordOut == NULL) {
        return StatusOk;
    }

    //
    // First, save the position we started at. If something fails, we'll *try*
    // to return here.
    //

    Cursor originalPos;
    status = logSavePosition(log, &originalPos);
    BailIfFailed(status);

    status = logLib->seekToBeginning(log);
    BailIfFailed(status);

    //
    // Traverse the log set until we find LogEndMagic or a partially written
    // record. Return to the end of the last stable point (i.e. the next place
    // to write.
    //

    // Data must be read to compute checksum.
    size_t dataSize;
    data = (uint8_t *) memAllocExt(16 * KB, __PRETTY_FUNCTION__);
    BailIfNull(data);
    dataSize = 16 * KB;

    // Stores the last valid record so far.
    Cursor lastValidRecord;

    // Beginning of block we're currently examining. Position to return to if
    // remaining data is corrupt.
    Cursor potentialRecord;

    while (true) {
        LogHeader header;

        status = logSavePosition(log, &potentialRecord);
        BailIfFailed(status);

        //
        // Read in and validate log header.
        //

        status = logReadNextHeader(log, &header);
        if (status == StatusEof) {
            // There isn't enough room for a header here but there's no
            // LogNextFileMagic or LogEndMagic meaning this is garbage
            // data.
            break;
        }
        BailIfFailed(status);

        if (header.magic == LogNextFileMagic) {
            status = logSeekToNextFile(log, 0);
            BailIfFailed(status);
            continue;
        }

        if (header.magic == LogEndMagic) {
            // Found end. The seek below will not change position.
            break;
        }

        if (!logIsValidHeader(log, &header)) {
            // Invalid data. Restore to last valid data.
            break;
        }

        //
        // Read in data and footer. Validate checksum.
        //

        if (dataSize < header.dataSize) {
            memFree(data);
            dataSize = 0;

            data =
                (uint8_t *) memAllocExt(header.dataSize, __PRETTY_FUNCTION__);
            BailIfNull(data);
            dataSize = header.dataSize;
        }

        status = FileUtils::convergentRead(log->currentFile->fd,
                                           data,
                                           header.dataSize,
                                           NULL);
        if (status == StatusEof) {
            break;
        }
        BailIfFailed(status);

        LogFooter footer;
        status = FileUtils::convergentRead(log->currentFile->fd,
                                           &footer,
                                           sizeof(footer),
                                           NULL);
        if (status == StatusEof) {
            break;
        }
        BailIfFailed(status);

        if (!logIsValidFooter(&footer, &header, data)) {
            break;
        }

        // This is the only branch that results in finding a valid record.
        lastSeqNum = header.seqNum;
        lastValidRecord = potentialRecord;
        if (!validRecordFound) {
            validRecordFound = true;
        }
    }

    //
    // Loop reached a place with either an invalid/incomplete entry or the log's
    // end. Seek to whatever it deemed the last valid position.
    //

    status = logSeek(log, &potentialRecord);
    BailIfFailed(status);

    log->lastReadSeqNum = lastSeqNum;
    log->nextWriteSeqNum = lastSeqNum + 1;
    log->positionedAtEnd = true;

    if (validRecordFound && lastRecordOut != NULL) {
        assert(foundLastRecordOut != NULL);
        *foundLastRecordOut = true;
        *lastRecordOut = lastValidRecord;
    } else if (!validRecordFound && foundLastRecordOut != NULL) {
        *foundLastRecordOut = false;
    }

CommonExit:
    if (data != NULL) {
        memFree(data);
    }

    if (status != StatusOk) {
        // Attempt to restore to previous state.
        Status statusRestore = logSeek(log, &originalPos);
        if (statusRestore != StatusOk) {
            xSyslog(moduleName,
                    XlogCrit,
                    "Unable to restore log state after failed read.");
        }
    }

    return status;
}

bool
LogLib::logIsValidFooter(const LogFooter *footerPtr,
                         const LogHeader *headerPtr,
                         const uint8_t *dataPtr)
{
    if (footerPtr->magic != LogFooterMagic) {
        return false;
    }
    if (footerPtr->entrySize !=
        headerPtr->headerSize + headerPtr->dataSize + headerPtr->footerSize) {
        return false;
    }

    // we have to calculate crc in 3 steps as footerPtr, headerPtr, and
    // dataPtr are not a single virtually contiguous memory region.
    uint64_t checksum = hashCrc32c(0, headerPtr, headerPtr->headerSize);
    checksum = hashCrc32c(checksum, dataPtr, headerPtr->dataSize);
    checksum = hashCrc32c(checksum,
                          footerPtr,
                          headerPtr->footerSize - sizeof(footerPtr->checksum));
    if (checksum != footerPtr->checksum) {
        return false;
    }

    return true;
}

bool
LogLib::logIsValidHeader(Handle *log, const LogHeader *headerPtr)
{
    if (headerPtr->magic != LogHeaderMagic) {
        return false;
    }
    if (headerPtr->majVersion != LogMajorVersion4) {
        return false;
    }
    if (headerPtr->headerSize != sizeof(LogHeader)) {
        return false;
    }
    if (headerPtr->footerSize != sizeof(LogFooter)) {
        return false;
    }
    const size_t entrySize =
        headerPtr->headerSize + headerPtr->dataSize + headerPtr->footerSize;
    if (entrySize > LogMaximumEntrySize) {
        return false;
    }
    if (headerPtr->seqNum <= log->lastReadSeqNum) {
        return false;
    }

    return true;
}
