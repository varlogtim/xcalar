// Copyright 2013-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALAR_PROC_FS_HELPER_
#define _XCALAR_PROC_FS_HELPER_

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "primitives/Macros.h"
#include "primitives/Status.h"
#include "primitives/Assert.h"
#include "runtime/Mutex.h"

class XcProcFsHelper
{
  public:
    XcProcFsHelper();
    ~XcProcFsHelper();

    MustCheck Status initProcFsHelper();

    MustCheck Status getMemInfo(uint64_t *totalMemory,
                                uint64_t *freeMemory,
                                uint64_t *totalSwap,
                                uint64_t *freeSwap);

    // the following apis' are exposed to reduce number of meminfo file open

    MustCheck Status getFreeMemSizeInBytes(uint64_t *totalFreeMemoryInBytes);

    MustCheck Status getFreeMemAndFreeSwapSizeInBytes(uint64_t *freeMemSize,
                                                      uint64_t *freeSwapSize);

    MustCheck Status getSysFreeResourceSize(uint64_t *sysFreeResSizeInBytes);

    MustCheck Status getTotalMemoryInBytes(uint64_t *totalMemoryInBytes);

    MustCheck Status
    getFreeMemAndSwapSizesInBytes(uint64_t *freeMemSizeInBytes,
                                  uint64_t *freeSwapSizeInBytes,
                                  uint64_t *totalSwapSizeInBytes);

    MustCheck Status getSysTotalResourceSize(uint64_t *sysTotalResSizeInBytes);

  private:
    static constexpr const int InvalidDescValue = -1;

    int memInfoFileDesc_ = InvalidDescValue;

    char *bufferForRead_ = NULL;

    // protects fd and buffer
    Mutex memInfoProtector_;

    // size of meminfo is a little over 1K, allocating 3 KB more
    // so that it will fit, if it doesnt, boot strap will fail
    // and we should catch in interal testing
    static constexpr const size_t bufferSizeForRead = 4096;

    // keep this in sync with memInfoStrings
    enum class ProcMemInfoMembers {
        MemTotal,
        MemFree,
        Buffers,
        Cached,
        SwapCached,
        SwapTotal,
        SwapFree,
        Shmem,
        SReclaimable,

        ProcMemInfoIdxMax,
    };

    static constexpr size_t numProcMemInfoMembers =
        ((size_t) ProcMemInfoMembers::ProcMemInfoIdxMax -
         (size_t) ProcMemInfoMembers::MemTotal);

    // https://www.kernel.org/doc/Documentation/filesystems/proc.txt
    const char *memInfoStrings[numProcMemInfoMembers] = {
        "MemTotal:",
        "MemFree:",
        "Buffers:",
        "Cached:",
        "SwapCached",
        "SwapTotal:",
        "SwapFree:",
        "Shmem:",
        "SReclaimable:",
    };

    unsigned procMemInfoCounterIndexes_[numProcMemInfoMembers] = {0};

    bool memInfoIdxUpdated_ = false;
    // the above indexes are different depending on kernel version
    // we find out all the indexes we want during boot
    MustCheck Status updateMemInfoIndexes();

    uint64_t procMemInfoStatsInBytes_[numProcMemInfoMembers] = {0};

    MustCheck Status parseLineFromProcMemInfo(const char *line,
                                              const char *nameString,
                                              size_t nameStringLen,
                                              uint64_t *valueInBytes);

    MustCheck uint64_t getFreeMemSizeInBytes();

    MustCheck Status populateMembers();
};

#endif  // _XCALAR_PROC_FS_HELPER_
