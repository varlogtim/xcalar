// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <stdio.h>
#include <sys/time.h>
#include <sys/resource.h>
#include "FiberCache.h"
#include "runtime/Runtime.h"
#include "RuntimeStats.h"
#include "FiberSchedThread.h"
#include "stat/Statistics.h"
#include "sys/XLog.h"
#include "util/XcalarSysHelper.h"

#define stats (Runtime::get()->getStats())

FiberCache::FiberCache()
    : fibers_(NULL), fibersCount_(0), nextFiberId_(0), virtualBytesLimit_(0)
{
}

FiberCache::~FiberCache()
{
    assert(fibers_ == NULL);
    assert(fibersCount_ == 0);
}

Status
FiberCache::alloc(Fiber **fiberOut)
{
    Status status;

    lock_.lock();
    uint64_t fiberId = nextFiberId_++;
    lock_.unlock();

    Fiber *fiber = new (std::nothrow) Fiber(fiberId);
    BailIfNull(fiber);

    status = fiber->make(&FiberSchedThread::main);
    if (status != StatusOk) {
        delete fiber;
        fiber = NULL;
    }

CommonExit:
    if (fiber != NULL) {
        *fiberOut = fiber;
    } else {
        StatsLib::statAtomicIncr64(stats->fiberAllocFails_);
        if (StatsLib::statReadUint64(stats->fiberAllocFails_) %
                AllocFailLogInterval ==
            1) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Fiber allocation failed."
                    "Low memory threshold %llu bytes",
                    (unsigned long long) lowMemThreshold_);
        }
    }
    return status;
}

Status
FiberCache::init(InitLevel initLevel)
{
    Status status = StatusOk;
    size_t totalSystemMemory;

    size_t physicalMemSize = XcSysHelper::get()->getPhysicalMemorySizeInBytes();

    size_t fiberSize = (size_t)(Runtime::get()->getStackBytes() +
                                Runtime::get()->getStackGuardBytes());

    status =
        XcSysHelper::get()->getSysTotalResourceSizeInBytes(&totalSystemMemory);
    BailIfFailed(status);

    // Take the smaller of two fiber counts.
    fibersLimit_ = ((CachePercentSysMem * physicalMemSize) / fiberSize) / 100;
    if (initLevel <= InitLevel::ChildNode) {
        fibersLimit_ = 0;
    }

    // Take the lower of the two mem limits.
    lowMemThreshold_ = (LowMemPercentSysMem * totalSystemMemory) / 100;

    for (size_t ii = 0; ii < fibersLimit_; ii++) {
        Fiber *fiber;
        status = alloc(&fiber);
        if (status != StatusOk) {
            destroy();
            goto CommonExit;
        }

        lock_.lock();
        fiber->nextCache = fibers_;
        fibers_ = fiber;
        fibersCount_++;
        StatsLib::statAtomicIncr64(stats->fibersInCache_);
        lock_.unlock();
    }

    xSyslog(ModuleName,
            XlogInfo,
            "FiberCache caching %llu Fibers (%llu bytes)",
            (unsigned long long) fibersLimit_,
            (unsigned long long) (fibersLimit_ * (size_t) fiberSize));
    xSyslog(ModuleName,
            XlogInfo,
            "FiberCache low memory threshold is %llu bytes "
            "(%llu megabytes)",
            (unsigned long long) lowMemThreshold_,
            (unsigned long long) (lowMemThreshold_ / MB));

CommonExit:

    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogCrit,
                "%s() failure: %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

void
FiberCache::destroy()
{
    lock_.lock();
    Fiber *next;
    for (Fiber *fiber = fibers_; fiber != NULL; fiber = next) {
        next = fiber->nextCache;
        delete fiber;
        fibersCount_--;
        StatsLib::statAtomicDecr64(stats->fibersInCache_);
    }
    fibers_ = NULL;
    lock_.unlock();
}

Status
FiberCache::get(Fiber **fiberOut)
{
    lock_.lock();
    Fiber *fiber = fibers_;
    if (fiber != NULL) {
        fibers_ = fiber->nextCache;
        fibersCount_--;
        lock_.unlock();

        StatsLib::statAtomicDecr64(stats->fibersInCache_);
        StatsLib::statAtomicIncr64(stats->fibersInUse_);
        StatsLib::statAtomicIncr64(stats->cacheHits_);

        *fiberOut = fiber;
        return StatusOk;
    }
    lock_.unlock();

    *fiberOut = fiber;
    return StatusNoMem;
}

Status
FiberCache::getWithAlloc(Fiber **fiberOut)
{
    Status status = get(fiberOut);
    if (status == StatusNoMem) {
        StatsLib::statAtomicIncr64(stats->cacheMisses_);
        status = alloc(fiberOut);
        if (status == StatusOk) {
            StatsLib::statAtomicIncr64(stats->fibersInUse_);
        }
    }
    return status;
}

void
FiberCache::put(Fiber *fiber)
{
    StatsLib::statAtomicDecr64(stats->fibersInUse_);
    if (fibersCount_ >= fibersLimit_) {
        delete fiber;
        StatsLib::statAtomicIncr64(stats->fiberDeletes_);
    } else {
        fiber->reset();

        lock_.lock();
        fiber->nextCache = fibers_;
        fibers_ = fiber;
        fibersCount_++;
        lock_.unlock();

        StatsLib::statAtomicIncr64(stats->fibersInCache_);
    }
}

bool
FiberCache::isMemoryLow() const
{
    if (virtualBytesLimit_ == 0) {
        return false;
    }

    size_t consumed = getConsumedVirtualBytes();
    return consumed >= lowMemThreshold_;
}

// XXX This is more hacky than is ideal. The RLIMIT_AS limit is what's governing
//     our memory usage and this seems to be the only way to gauge how close we
//     are to it.
size_t
FiberCache::getConsumedVirtualBytes() const
{
    size_t consumed = 0;

    static const char *vmSizeKey = "VmSize:\t";
    size_t vmSizeKeyLen = strlen(vmSizeKey);

    // Prefer /proc/self/status to /proc/self/stats because it seems slightly
    // more portable.
    // @SymbolCheckIgnore
    FILE *procSelfStatus = fopen("/proc/self/status", "r");
    if (procSelfStatus == NULL) {
        return consumed;
    }

    char line[512];
    while (fgets(line, sizeof(line), procSelfStatus) != NULL) {
        if (strncmp(line, vmSizeKey, vmSizeKeyLen) == 0) {
            if (sscanf(&line[vmSizeKeyLen], "%lu", &consumed) == 1) {
                consumed *= KB;
            }
            break;
        }
    }

    // @SymbolCheckIgnore
    fclose(procSelfStatus);
    return consumed;
}
