// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/mman.h>
#include <sys/resource.h>
#include <stdio.h>
#include <sys/mman.h>
#ifdef XLR_VALGRIND
#include <valgrind/valgrind.h>
#endif
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <setjmp.h>

#include "bc/BufCacheMemMgr.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "strings/String.h"
#include "constants/XcalarConfig.h"
#include "bc/BufferCache.h"
#include "util/XcalarSysHelper.h"

static constexpr const char *moduleName = "libbc";
BufCacheMemMgr *BufCacheMemMgr::instance = NULL;

Status
BufCacheMemMgr::init(BufferCacheMgr::Type type, uint64_t bufCacheSize)
{
    static constexpr const size_t BufSize = 255;
    Status status = StatusOk, status2;
    int ret;
    struct rlimit vaLimit;
    XcProcFsHelper *procFsHelper = NULL;

    size_t totalSwapSize;
    size_t physicalMemSize;
    size_t freeMemSize;
    size_t freeSwapSize;
    size_t totalSystemMemory;

    char vaLimitHuman[BufSize];
    char vaMinHuman[BufSize];
    char swapSizeHuman[BufSize];
    char physicalMemSizeHuman[BufSize];
    char freeMemSizeHuman[BufSize];
    char freeSwapSizeHuman[BufSize];

    instance =
        (BufCacheMemMgr *) memAllocExt(sizeof(BufCacheMemMgr), moduleName);
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "BufCacheMemMgr::init failed, type %u,"
                " bufCacheSize %lu bytes: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        return status;
    }
    instance = new (instance) BufCacheMemMgr();

    procFsHelper = XcSysHelper::get()->getProcFsHelper();

    instance->bufCacheSize_ = roundDown(bufCacheSize, PageSize);

    if (type == BufferCacheMgr::TypeUsrnode) {
        instance->type_ = TypeContigAlloc;
    } else {
        instance->type_ = TypeMalloc;
    }

    status = procFsHelper->getFreeMemAndSwapSizesInBytes(&freeMemSize,
                                                         &freeSwapSize,
                                                         &totalSwapSize);
    BailIfFailed(status);

    physicalMemSize = XcSysHelper::get()->getPhysicalMemorySizeInBytes();

    status =
        XcSysHelper::get()->getSysTotalResourceSizeInBytes(&totalSystemMemory);
    BailIfFailed(status);

    status2 = strHumanSize(swapSizeHuman, sizeof(swapSizeHuman), totalSwapSize);
    if (status2 == StatusOk) {
        status2 = strHumanSize(physicalMemSizeHuman,
                               sizeof(physicalMemSizeHuman),
                               physicalMemSize);
    }

    if (status2 == StatusOk) {
        status2 = strHumanSize(freeMemSizeHuman,
                               sizeof(freeMemSizeHuman),
                               freeMemSize);
    }

    if (status2 == StatusOk) {
        status2 = strHumanSize(freeSwapSizeHuman,
                               sizeof(freeSwapSizeHuman),
                               freeSwapSize);
    }

    if (status2 == StatusOk) {
        xSyslog(moduleName,
                XlogInfo,
                "Swap size: %s (%lu bytes), Physical RAM: %s (%lu bytes), "
                "Free memory size: %s (%lu bytes), Free Swap Size: %s "
                "(%lu bytes)",
                swapSizeHuman,
                totalSwapSize,
                physicalMemSizeHuman,
                physicalMemSize,
                freeMemSizeHuman,
                freeMemSize,
                freeSwapSizeHuman,
                freeSwapSize);
    } else {
        xSyslog(moduleName,
                XlogInfo,
                "Swap size: %lu bytes Physical RAM: %lu bytes",
                totalSwapSize,
                physicalMemSize);
    }

    if (instance->type_ == TypeMalloc) {
        goto CommonExit;
    }

    assert(instance->type_ == TypeContigAlloc);
    status = instance->setUpShm();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "BufCacheMemMgr::init failed, type %u,"
                " bufCacheSize %lu bytes: %s",
                type,
                bufCacheSize,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // This sets the upper limit on the virtual address that this process
    // can allocate. Note that this does not guarantee that we will not
    // get OOM killed, because other applications might already be hoarding
    // a lot of memory. The alternative is to set /proc/sys/vm/overcommit_memory
    // to 2, which will cause system-wide malloc to fail if total virtual memory
    // allocated > RAM + swap. This causes several applications, including
    // Chrome, to crash. This is because several applications allocate a large
    // virtual address range, but never actually use them. Hence, in the usual
    // case, these virtual addresses never need to be backed at all.
    if (XcalarConfig::get()->enforceVALimit_) {
        // Get rlim_max from the environment; we preserve it so rlim_cur can
        // be raised later to support mappings not backed by physical memory
        if (getrlimit(RLIMIT_AS, &vaLimit)) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "BufCacheMemMgr::init failed, type %u,"
                    " bufCacheSize %lu bytes: %s",
                    type,
                    bufCacheSize,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        vaLimit.rlim_cur =
            totalSystemMemory / Config::get()->getActiveNodesOnMyPhysicalNode();
        if (vaLimit.rlim_cur < BufferCacheMgr::MinTotalMem) {
            Status tmpStatus;

            tmpStatus = strHumanSize(vaMinHuman,
                                     sizeof(vaMinHuman),
                                     BufferCacheMgr::MinTotalMem);

            if (totalSystemMemory < BufferCacheMgr::MinTotalMem) {
                // We haven't even divided by numLocalNodes and we're already
                // below what's required.
                char sysMemHuman[BufSize];

                if (tmpStatus == StatusOk) {
                    tmpStatus = strHumanSize(sysMemHuman,
                                             sizeof(sysMemHuman),
                                             totalSystemMemory);
                }

                if (tmpStatus == StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Warning! Total amount of system memory %s "
                            "(%lu bytes) is less than recommended amount of "
                            "%s (%lu bytes)",
                            sysMemHuman,
                            totalSystemMemory,
                            vaMinHuman,
                            BufferCacheMgr::MinTotalMem);
                } else {
                    xSyslog(moduleName,
                            XlogErr,
                            "Warning! Total amount of system memory %lu bytes "
                            "is less than recommended amount of %lu bytes",
                            totalSystemMemory,
                            BufferCacheMgr::MinTotalMem);
                }

                // We can choose to return error if we want to. But now let's
                // proceed
                vaLimit.rlim_cur = totalSystemMemory;
            } else {
                if (tmpStatus == StatusOk) {
                    tmpStatus = strHumanSize(vaLimitHuman,
                                             sizeof(vaLimitHuman),
                                             vaLimit.rlim_cur);
                }

                // Consider running with fewer usrnodes instead
                if (tmpStatus == StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "%s (%lu bytes) per usrnode is too small. "
                            "Defaulting to %s (%lu bytes) per usrnode",
                            vaLimitHuman,
                            vaLimit.rlim_cur,
                            vaMinHuman,
                            BufferCacheMgr::MinTotalMem);
                } else {
                    xSyslog(moduleName,
                            XlogErr,
                            "%lu bytes per usrnode is too small. Defaulting to "
                            "%lu bytes per usrnode",
                            vaLimit.rlim_cur,
                            BufferCacheMgr::MinTotalMem);
                }

                vaLimit.rlim_cur = BufferCacheMgr::MinTotalMem;
            }
        }

        Status tmpStatus;
        tmpStatus =
            strHumanSize(vaLimitHuman, sizeof(vaLimitHuman), vaLimit.rlim_cur);
        if (tmpStatus == StatusOk) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Setting address space limit to %s (%lu bytes)",
                    vaLimitHuman,
                    vaLimit.rlim_cur);
        } else {
            xSyslog(moduleName,
                    XlogInfo,
                    "Setting address space limit to %lu bytes",
                    vaLimit.rlim_cur);
        }

        errno = 0;
        ret = setrlimit(RLIMIT_AS, &vaLimit);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "BufCacheMemMgr::init failed, type %u,"
                    " bufCacheSize %lu bytes: %s",
                    type,
                    bufCacheSize,
                    strGetFromStatus(status));
            goto CommonExit;
        }

#ifdef DEBUG
        void *a;
        a = memAlloc(vaLimit.rlim_cur + 1);
        if (a != NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Attempt to allocate beyond address space"
                    " limit returns non NULL value: %p",
                    a);
        }
#endif
    }

CommonExit:

    return status;
}

void
BufCacheMemMgr::destroy()
{
    if (type_ == TypeContigAlloc) {
        tearDownShm();
    } else {
        assert(type_ == TypeMalloc);
        // NOOP
    }

    bufCacheSize_ = 0;
    nextOffset_ = 0;
    startAddr_ = NULL;
    type_ = TypeMin;
    instance->~BufCacheMemMgr();
    memFree(instance);
    instance = NULL;
}

uint64_t
BufCacheMemMgr::getSize()
{
    assert(bufCacheSize_ % PageSize == 0);
    return bufCacheSize_;
}

uint64_t
BufCacheMemMgr::getMlockedSize()
{
    uint64_t mlockedBufCacheSize;
    XcalarConfig *xc = XcalarConfig::get();
    if (xc->bufCacheMlock_) {
        mlockedBufCacheSize =
            roundDown((getSize() * xc->bufferCacheMlockPercent_) / 100,
                      PageSize);
    } else {
        mlockedBufCacheSize = 0;
    }
    assert(mlockedBufCacheSize % PageSize == 0);
    return mlockedBufCacheSize;
}

void *
BufCacheMemMgr::getStartAddr()
{
    assert(startAddr_ != NULL);
    return startAddr_;
}

uint64_t
BufCacheMemMgr::getBufCacheOffset(void *ptr)
{
    assert(startAddr_ != NULL);

    return (uint64_t) ptr - (uint64_t) startAddr_;
}

BufCacheMemMgr::Type
BufCacheMemMgr::getType()
{
    return type_;
}

BufCacheMemMgr *
BufCacheMemMgr::get()
{
    return instance;
}

uint64_t
BufCacheMemMgr::getNextOffset()
{
    return nextOffset_;
}

void
BufCacheMemMgr::setNextOffset(uint64_t offset)
{
    assert(offset <= getSize());
    nextOffset_ = offset;
}

SharedMemory::Type
BufCacheMemMgr::shmType()
{
    SharedMemory::Type type = SharedMemory::Type::Invalid;
    XcalarConfig *xcConf = XcalarConfig::get();

    if (xcConf->bufCacheMlock_) {
        // Buffer Cache memory needs to be pinned.
        // Note that BufferCachePercentOfTotalMem cannot exceed available
        // memory.
        if (xcConf->bufCacheNonTmpFs_) {
            // Buffer Cache memory uses SHM segments instead of TmpFs.
            type = SharedMemory::Type::ShmSegment;
        } else if (xcConf->bufferCachePercentOfTotalMem_ <=
                   XcalarConfig::BufferCachePercentOfTotalMemDefault) {
            // Buffer Cache memory uses TmpFs. So TmpFs needs to be mounted
            // appropriately with enough available memory as specified by
            // BufferCachePercentOfTotalMem.
            type = SharedMemory::Type::ShmTmpFs;
        } else {
            // Buffer Cache memory uses regular filesystem file.
            type = SharedMemory::Type::NonShm;
        }
    } else {
        // Buffer Cache memory to not be pinned.
        // Note that BufferCachePercentOfTotalMem can exceed available
        // memory, i.e. can be > 100.
        if (xcConf->bufCacheNonTmpFs_) {
            // Buffer Cache memory uses SHM segments
            type = SharedMemory::Type::ShmSegment;
        } else {
            // Buffer Cache memory uses regular filesystem file.
            type = SharedMemory::Type::NonShm;
        }
    }
    return type;
}

Status
BufCacheMemMgr::setUpShm()
{
    Status status = StatusOk;
    char bcFileName[SharedMemory::NameLength + 1];
    Config *config = Config::get();
    SharedMemory::Type type = shmType();
    XcalarConfig *xcConf = XcalarConfig::get();

    if ((size_t) snprintf(bcFileName,
                          sizeof(bcFileName),
                          BufferCacheMgr::FileNamePattern,
                          SharedMemory::XcalarShmPrefix,
                          (unsigned) config->getMyNodeId()) >=
        sizeof(bcFileName)) {
        status = StatusOverflow;
        xSyslog(moduleName,
                XlogErr,
                "setUpShm failed because the generated file name"
                " was longer than %lu bytes: %s",
                sizeof(bcFileName),
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = shm_.create(bcFileName, getSize(), type);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "setUpShm create file %s failed: %s",
                bcFileName,
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "BufferCache created file %s size %lu type %u",
            bcFileName,
            getSize(),
            type);

    startAddr_ = shm_.getAddr();

#ifdef XLR_VALGRIND
    VALGRIND_CREATE_MEMPOOL(startAddr_, 0, 0);
#endif

    if (xcConf->bufCacheMlock_) {
        uint64_t mlockSize = getMlockedSize();
        struct rlimit curRlimit;
        int ret = getrlimit(RLIMIT_MEMLOCK, &curRlimit);
        if (ret < 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(moduleName,
                    XlogErr,
                    "Failed getrlimit for RLIMIT_MEMLOCK: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (curRlimit.rlim_cur < mlockSize) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "RLIMIT_MEMLOCK rlim_cur %lu Bytes is less than buffer "
                    "cache mlock size %lu Bytes, so failing BufCacheMemMgr "
                    "init. Try again with reduced XcalarConfig "
                    "BufferCachePercentOfTotalMem",
                    curRlimit.rlim_cur,
                    mlockSize);
            goto CommonExit;
        }

        if (!xcConf->bufferCacheLazyMemLocking_) {
            // Buf$ with Mlocking ON, Lazy Mlocking OFF.
            int ret = mlock(startAddr_, mlockSize);
            if (ret != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "setUpShm mlock file %s failed: %s",
                        bcFileName,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
    }

CommonExit:
    return status;
}

void
BufCacheMemMgr::tearDownShm()
{
#ifdef XLR_VALGRIND
    VALGRIND_DESTROY_MEMPOOL(startAddr_);
#endif

    if (shm_.isBacked()) {
        if (XcalarConfig::get()->bufCacheMlock_) {
            int ret = munlock(startAddr_, getMlockedSize());
            if (ret != 0) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed munlock file %s: %s",
                        shm_.getName(),
                        strGetFromStatus(sysErrnoToStatus(errno)));
            }
        }
        shm_.del();
    }

    startAddr_ = NULL;
}
