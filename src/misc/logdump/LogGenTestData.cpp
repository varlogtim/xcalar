// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <assert.h>
#include <err.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/mman.h>
#include <libgen.h>
#include <stdio.h>
#include <stdlib.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "hash/Hash.h"
#include "common/InitTeardown.h"
#include "util/MemTrack.h"
#include "log/Log.h"
#include "LogGenTestData.h"

Status
LogGenTestData::genCleanLog(LogLib::Handle *handle,
                            const char *const prefix,
                            const size_t logFileSize)
{
    Status status = StatusOk;
    size_t recordNum = 0;
    const char *const dataPrefix = "RecordNumber_";
    char buf[logBufSize];
    constexpr size_t entSize =
        sizeof(LogLib::LogHeader) + sizeof(LogLib::LogFooter) + sizeof(buf);
    LogLib *logLib = LogLib::get();
    size_t numEnts = LogLib::LogDefaultFileCount * logFileSize / entSize;

    status = logLib->create(handle,
                            LogLib::XcalarRootDirIndex,
                            prefix,
                            LogLib::FileSeekToLogicalEnd |
                                LogLib::FileReturnErrorIfExists |
                                LogLib::FileExpandIfNeeded,
                            LogLib::LogDefaultFileCount,
                            logFileSize);

    BailIfFailed(status);

    // XXX: Fill out buffer with variable length pseudorandom ASCII
    memset(buf, 0, sizeof(buf));
    for (recordNum = 0; recordNum < numEnts - 1; recordNum++) {
        snprintf(buf, sizeof(buf), "%s%09lu", dataPrefix, recordNum);
        status = logLib->writeRecord(handle,
                                     buf,
                                     sizeof(buf),
                                     LogLib::WriteOptionsMaskDefault);
        BailIfFailed(status);
    }

    status = logLib->seekToBeginning(handle);
    BailIfFailed(status);

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr,
                "Error generating test data: %s\n",
                strGetFromStatus(status));
    }

    return status;
}

Status
LogGenTestData::ffToRecord(LogLib::Handle *handle, const size_t pos)
{
    Status status = StatusOk;
    LogLib *logLib = LogLib::get();

    status = logLib->seekToBeginning(handle);
    BailIfFailed(status);

    for (size_t i = 0; i < pos; i++) {
        char buf[logBufSize];
        size_t bytesRead;
        status = logLib->readRecord(handle,
                                    buf,
                                    sizeof(buf),
                                    &bytesRead,
                                    LogLib::ReadNext);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

// Assumes file position at beginning of record header
// Leaves file position at beginnning of next record's header
Status
LogGenTestData::getSetFooter(LogLib::Handle *handle,
                             LogLib::LogFooter *footer,
                             const bool isWrite)
{
    Status status = StatusOk;
    LogLib::LogHeader header;
    LogLib *logLib = LogLib::get();
    off_t retval;

    status = logLib->logReadNextHeader(handle, &header);
    BailIfFailed(status);
    // logLib->logIsValidHeader(handle, &header) may be false here due to
    // expected seqNum mismatch.
    assert(header.magic == LogLib::LogHeaderMagic);
    retval = lseek(handle->currentFile->fd, header.dataSize, SEEK_CUR);
    assert(retval != -1);
    if (isWrite) {
        status =
            logLib->logWriteBytes(handle, (uint8_t *) footer, sizeof(*footer));
        BailIfFailed(status);
    } else {
        size_t bytesRead;
        status = logLib->logSetReadBytes(handle,
                                         (uint8_t *) footer,
                                         sizeof(*footer),
                                         &bytesRead);
        BailIfFailed(status);
    }

CommonExit:
    return status;
}

Status
LogGenTestData::dumpTestData(LogLib::Handle *handle)
{
    char buf[logBufSize];
    int numRecords = 0;
    size_t bytesRead;
    LogLib *logLib = LogLib::get();
    LogLib::LogHeader header;
    LogLib::LogFooter footer;

    Status status = logLib->seekToBeginning(handle);
    BailIfFailed(status);

    while (status == StatusOk) {
        LogLib::Cursor logCursor;
        status = logLib->logSavePosition(handle, &logCursor);
        BailIfFailed(status);
        status = logLib->logReadNextHeader(handle, &header);
        BailIfFailed(status);
        status = logLib->logSeek(handle, &logCursor);
        BailIfFailed(status);
        status = logLib->readRecord(handle,
                                    buf,
                                    sizeof(buf),
                                    &bytesRead,
                                    LogLib::ReadNext);
        if (status != StatusOk) {
            status = StatusOk;
            break;
        }
        status = logLib->logSeek(handle, &logCursor);
        BailIfFailed(status);
        status = getSetFooter(handle, &footer, false);
        BailIfFailed(status);
        printf(
            "hmagic: 0x%lx, hseq: %5lu, dataSize: %5lu, fmagic: 0x%lx, "
            "fcksum: 0x%016lx, ASCII data: %s\n",
            header.magic,
            header.seqNum,
            header.dataSize,
            footer.magic,
            footer.checksum,
            buf);
        numRecords++;
    }

CommonExit:
    printf("Read %d records\n", numRecords);
    return status;
}

Status
LogGenTestData::genFuncTestData(const char *const prefix, const bool verbose)
{
    Status status = StatusOk;
    LogLib::Handle handle;
    LogLib *logLib = LogLib::get();
    LogLib::Cursor logCursor;
    LogLib::LogFooter footer;
    char buf[logBufSize];
    bool found;
    uint64_t logEnd;
    int retval;

    // Generate test case with a bad checksum
    snprintf(buf, sizeof(buf), "%s.BadChecksum", prefix);
    status = genCleanLog(&handle, buf, 8 * KB);
    BailIfFailed(status);
    // Arbitrarly choose to corrupt this record number
    status = ffToRecord(&handle, 11);
    BailIfFailed(status);
    status = logLib->logSavePosition(&handle, &logCursor);
    BailIfFailed(status);
    status = getSetFooter(&handle, &footer, false);
    BailIfFailed(status);
    status = logLib->logSeek(&handle, &logCursor);
    BailIfFailed(status);
    assert(footer.checksum != 0xbadc0de);
    footer.checksum = 0xbadc0de;
    status = getSetFooter(&handle, &footer, true);
    BailIfFailed(status);
    if (verbose) {
        status = dumpTestData(&handle);
        BailIfFailed(status);
    }
    logLib->close(&handle);

    // Generate test case with a "missing" footer
    snprintf(buf, sizeof(buf), "%s.MissingFooter", prefix);
    status = genCleanLog(&handle, buf, 8 * KB);
    BailIfFailed(status);
    // Arbitrarly choose to corrupt this record number
    status = ffToRecord(&handle, 7);
    BailIfFailed(status);
    // XXX: The existing test case just zeros the footer.  We should probably
    // add a case where the footer is actually missing.
    memset(&footer, 0x0, sizeof(footer));
    status = getSetFooter(&handle, &footer, true);
    BailIfFailed(status);
    if (verbose) {
        status = dumpTestData(&handle);
        BailIfFailed(status);
    }
    logLib->close(&handle);

    // Generate test case with missing log end
    snprintf(buf, sizeof(buf), "%s.MissingEnd", prefix);
    status = genCleanLog(&handle, buf, 8 * KB);
    BailIfFailed(status);
    status = logLib->logSeekToLogicalEnd(&handle, &logCursor, &found);
    BailIfFailed(status);
    assert(found);
    logEnd = 0;
    status =
        logLib->logWriteBytes(&handle, (uint8_t *) &logEnd, sizeof(logEnd));
    BailIfFailed(status);
    if (verbose) {
        status = dumpTestData(&handle);
        BailIfFailed(status);
    }
    logLib->close(&handle);

    // Generate test case with missing backing file
    snprintf(buf, sizeof(buf), "%s.Missing0", prefix);
    status = genCleanLog(&handle, buf, 8 * KB);
    BailIfFailed(status);
    snprintf(buf,
             sizeof(buf),
             "%s/%s.Missing0-0",
             XcalarConfig::get()->xcalarRootCompletePath_,
             prefix);
    retval = unlink(buf);
    assert(retval == 0);
    logLib->close(&handle);

CommonExit:

    if (status != StatusOk) {
        fprintf(stderr,
                "Error generating test data: %s\n",
                strGetFromStatus(status));
    }

    return status;
}
