// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <errno.h>

#include "util/XcalarProcFsHelper.h"
#include "util/System.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "XcalarProcFsHelper";

XcProcFsHelper::XcProcFsHelper()
{
    memInfoIdxUpdated_ = false;
    memInfoFileDesc_ = InvalidDescValue;
    bufferForRead_ = NULL;
}

XcProcFsHelper::~XcProcFsHelper()
{
    if (memInfoFileDesc_ != InvalidDescValue) {
        close(memInfoFileDesc_);
    }

    if (bufferForRead_ != NULL) {
        memFree(bufferForRead_);
    }
}

Status
XcProcFsHelper::getSysFreeResourceSize(uint64_t *sysFreeResSizeInBytes)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *sysFreeResSizeInBytes =
        (getFreeMemSizeInBytes() +
         procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapFree]);

CommonExit:

    return status;
}

Status
XcProcFsHelper::getSysTotalResourceSize(uint64_t *sysTotalResSizeInBytes)
{
    Status status = StatusOk;

    XcalarConfig *xcConfig = XcalarConfig::get();

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *sysTotalResSizeInBytes =
        (procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::MemTotal] +
         (procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapTotal] *
          xcConfig->swapUsagePercent_) /
             100);

CommonExit:

    return status;
}

Status
XcProcFsHelper::getFreeMemSizeInBytes(size_t *freeMemSize)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *freeMemSize = getFreeMemSizeInBytes();

CommonExit:

    return status;
}

Status
XcProcFsHelper::getTotalMemoryInBytes(uint64_t *totalMemoryInBytes)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *totalMemoryInBytes =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::MemTotal];

CommonExit:

    return status;
}

// populateMembers() has to be invoked before invoking this func
uint64_t
XcProcFsHelper::getFreeMemSizeInBytes()
{
    // https://www.kernel.org/doc/Documentation/filesystems/proc.txt
    // Cached mem in htop is calculated as
    // this->cachedMem = this->cachedMem + sreclaimable - shmem;
    // we use cached mem to calculate free memory
    // SwapCached will not be included in Cached:
    // "real free ram" = (free ram + swapcached + buffers + cached - shmem)

    size_t totalFreeMemoryInBytes =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::MemFree] +
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapCached] +
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::Buffers] +
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SReclaimable];

    assert(memInfoIdxUpdated_ == true);

    if (procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::Cached] >
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::Shmem]) {
        totalFreeMemoryInBytes +=
            (procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::Cached] -
             procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::Shmem]);
    }

    return totalFreeMemoryInBytes;
}

Status
XcProcFsHelper::getMemInfo(uint64_t *totalMemory,
                           uint64_t *freeMemory,
                           uint64_t *totalSwap,
                           uint64_t *freeSwap)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *totalMemory =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::MemTotal];
    // this is used in top api, lets not add confusion
    // just return what meminfo has for MemFree
    *freeMemory =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::MemFree];
    *totalSwap =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapTotal];
    *freeSwap = procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapFree];

CommonExit:

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s() Failure: %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

Status
XcProcFsHelper::getFreeMemAndFreeSwapSizeInBytes(uint64_t *freeMemSize,
                                                 uint64_t *freeSwapSize)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *freeMemSize = getFreeMemSizeInBytes();
    *freeSwapSize =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapFree];

CommonExit:

    return status;
}

Status
XcProcFsHelper::getFreeMemAndSwapSizesInBytes(uint64_t *freeMemSizeInBytes,
                                              uint64_t *freeSwapSizeInBytes,
                                              uint64_t *totalSwapSizeInBytes)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    status = populateMembers();
    BailIfFailed(status);

    *freeMemSizeInBytes = getFreeMemSizeInBytes();
    *freeSwapSizeInBytes =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapFree];
    *totalSwapSizeInBytes =
        procMemInfoStatsInBytes_[(size_t) ProcMemInfoMembers::SwapTotal];

CommonExit:

    return status;
}

Status
XcProcFsHelper::parseLineFromProcMemInfo(const char *line,
                                         const char *nameString,
                                         size_t nameStringLen,
                                         uint64_t *valueInBytes)
{
    Status status = StatusOk;

    assert(memInfoIdxUpdated_ == true);

    static const char KbString[] = " kB\n";
    char *valueInUnitsString = NULL;

    const char *ptrToValue = NULL;
    uint64_t valueInKb = 0;

    assert(line != NULL);
    assert(valueInBytes != NULL);
    *valueInBytes = 0;

    if (strncmp(line, nameString, nameStringLen) != 0) {
        status = StatusUnknownProcMemInfoFileFormat;
        goto CommonExit;
    }

    ptrToValue = strchr(line, ' ');
    if (ptrToValue == NULL) {
        status = StatusUnknownProcMemInfoFileFormat;
        goto CommonExit;
    }

    valueInKb = strtoull(ptrToValue, &valueInUnitsString, 10);
    if (valueInKb == ULLONG_MAX) {
        status = sysErrnoToStatus(errno);
        assert(status != StatusOk);
        goto CommonExit;
    }

    if ((valueInUnitsString == NULL) || (*valueInUnitsString == '\0') ||
        (strnlen(valueInUnitsString, sizeof(KbString) - 1) !=
         (sizeof(KbString) - 1)) ||
        (strncmp(valueInUnitsString, KbString, sizeof(KbString) - 1) != 0)) {
        status = StatusUnknownProcMemInfoFileFormat;
        goto CommonExit;
    }

    *valueInBytes = (valueInKb * KB);

CommonExit:

    // this error case happens only if proc/meminfo format changed
    assert(status != StatusUnknownProcMemInfoFileFormat);

    return status;
}

Status
XcProcFsHelper::populateMembers()
{
    Status status = StatusOk;

    char *lineToProcess = NULL;
    ssize_t numCharsRead = -1;
    unsigned numCountersUpdated = 0;
    unsigned currentLineNumber = 0;
    unsigned currentIdx = 0;

    assertStatic(ArrayLen(memInfoStrings) ==
                 (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax);

    assert(memInfoIdxUpdated_ == true);
    assert(memInfoFileDesc_ != InvalidDescValue);

    memInfoProtector_.lock();

    if (lseek(memInfoFileDesc_, 0L, SEEK_SET) != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    assert(bufferForRead_ != NULL);
    memZero(bufferForRead_, bufferSizeForRead);

    // we try to read the whole file contents at once and parse it
    numCharsRead = read(memInfoFileDesc_, bufferForRead_, bufferSizeForRead);
    if (!(numCharsRead > 0)) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (*bufferForRead_ == '\0') {
        status = StatusUnknownProcMemInfoFileFormat;
        goto CommonExit;
    }

    lineToProcess = bufferForRead_;
    currentLineNumber = 0;
    numCountersUpdated = 0;
    while (true) {
        bool found = false;
        for (currentIdx = (size_t) ProcMemInfoMembers::MemTotal;
             currentIdx < (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax;
             currentIdx++) {
            if (currentLineNumber == procMemInfoCounterIndexes_[currentIdx]) {
                found = true;
                break;
            }
        }

        if (found == true) {
            status =
                parseLineFromProcMemInfo(lineToProcess,
                                         memInfoStrings[currentIdx],
                                         strlen(memInfoStrings[currentIdx]),
                                         &procMemInfoStatsInBytes_[currentIdx]);
            BailIfFailed(status);

            numCountersUpdated++;
            if (numCountersUpdated ==
                (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax) {
                break;
            }
        }

        lineToProcess = strchr(lineToProcess, '\n');
        if ((lineToProcess == NULL) || (*lineToProcess == '\0')) {
            status = StatusUnknownProcMemInfoFileFormat;
            goto CommonExit;
        }

        // skip '\n'
        lineToProcess++;
        // this should not be the last line
        if (*lineToProcess == '\0') {
            status = StatusUnknownProcMemInfoFileFormat;
            goto CommonExit;
        }

        currentLineNumber++;
    }

    assert(numCountersUpdated ==
           (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax);

CommonExit:
    memInfoProtector_.unlock();

    return status;
}

Status
XcProcFsHelper::updateMemInfoIndexes()
{
    Status status = StatusOk;
    char *lineToProcess = NULL;
    ssize_t numCharsRead = -1;
    unsigned currentIdx = 0;
    unsigned currentLineNumber = 0;
    unsigned numIdxFound = 0;

    assertStatic(ArrayLen(memInfoStrings) ==
                 (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax);

    assert(bufferForRead_ != NULL);
    memZero(bufferForRead_, bufferSizeForRead);

    // we try to read the whole file contents at once and parse it
    numCharsRead = read(memInfoFileDesc_, bufferForRead_, bufferSizeForRead);
    if (!(numCharsRead > 0)) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    if (*bufferForRead_ == '\0') {
        status = StatusUnknownProcMemInfoFileFormat;
        goto CommonExit;
    }

    lineToProcess = bufferForRead_;
    currentLineNumber = 0;

    while (true) {
        for (currentIdx = (size_t) ProcMemInfoMembers::MemTotal;
             currentIdx < (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax;
             currentIdx++) {
            if (strlen(lineToProcess) < strlen(memInfoStrings[currentIdx])) {
                status = StatusUnknownProcMemInfoFileFormat;
                goto CommonExit;
            }

            if (strncmp(lineToProcess,
                        memInfoStrings[currentIdx],
                        strlen(memInfoStrings[currentIdx])) == 0) {
                assert(currentIdx <
                       (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax);
                procMemInfoCounterIndexes_[currentIdx] = currentLineNumber;
                numIdxFound++;
                break;
            }
        }

        if (numIdxFound == (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax) {
            break;
        }

        lineToProcess = strchr(lineToProcess, '\n');
        if ((lineToProcess == NULL) || (*lineToProcess == '\0')) {
            status = StatusUnknownProcMemInfoFileFormat;
            goto CommonExit;
        }

        // skip '\n'
        lineToProcess++;
        // this should not be the last line
        if (*lineToProcess == '\0') {
            status = StatusUnknownProcMemInfoFileFormat;
            goto CommonExit;
        }

        currentLineNumber++;
    }

    assert(numIdxFound == (size_t) ProcMemInfoMembers::ProcMemInfoIdxMax);

CommonExit:

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s() Failure: %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

Status
XcProcFsHelper::initProcFsHelper()
{
    Status status = StatusOk;
    char *sameHwContainer = NULL;

    // default meminfo location
    const char *memInfoFileAbsPath = "/proc/meminfo";

    // MEMINFO may contain the path to a user defined meminfo.txt file.
    // It should only be set if all cluster nodes exist on the same physical
    // machine (same HW).
    sameHwContainer = getenv("CLUSTER_SAME_PHYSICAL_NODE");
    if (sameHwContainer && strcmp(sameHwContainer, "true") == 0) {
        if (char *env = getenv("MEMINFO")) {
            memInfoFileAbsPath = env;
            xSyslog(moduleName,
                    XlogInfo,
                    "Meminfo file location set to user supplied path: %s",
                    memInfoFileAbsPath);
        }
    }

    memInfoFileDesc_ = open(memInfoFileAbsPath, O_RDONLY, O_CLOEXEC);
    if (memInfoFileDesc_ == InvalidDescValue) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    bufferForRead_ = (char *) memAllocExt(bufferSizeForRead, moduleName);
    if (bufferForRead_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = updateMemInfoIndexes();
    BailIfFailed(status);

    memInfoIdxUpdated_ = true;

CommonExit:

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s() Failure: %s",
                __func__,
                strGetFromStatus(status));

        if (bufferForRead_ != NULL) {
            memFree(bufferForRead_);
            bufferForRead_ = NULL;
        }

        if (memInfoFileDesc_ != InvalidDescValue) {
            close(memInfoFileDesc_);
            memInfoFileDesc_ = InvalidDescValue;
        }
    }

    return status;
}
