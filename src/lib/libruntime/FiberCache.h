// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef FIBERCACHE_H
#define FIBERCACHE_H

#include "primitives/Primitives.h"
#include "runtime/Spinlock.h"
#include "Fiber.h"
#include "common/InitLevel.h"

class FiberCache final
{
  public:
    FiberCache();
    ~FiberCache();

    Status init(InitLevel initLevel);
    void destroy();

    // this only attempts to pull from the cache
    Status get(Fiber **fiberOut);

    // this attempts to pull from the cache, and mallocs a new fiber if the
    // cache is empty
    Status getWithAlloc(Fiber **fiberOut);
    void put(Fiber *fiber);

    bool isMemoryLow() const;

    size_t getSize() const { return fibersCount_; }

    size_t getLimit() const { return fibersLimit_; }

  private:
    static constexpr const char *ModuleName = "libruntime";
    static constexpr uint64_t AllocFailLogInterval = 1000;

    // XXX May want to expose these externally.

    // Used to determine how many Fibers to hold in cache.
    static constexpr size_t CachePercentSysMem = 1;

    // Determines when low memory event is triggered.
    static constexpr size_t LowMemPercentSysMem = 90;

    Status alloc(Fiber **fiberOut);
    size_t getConsumedVirtualBytes() const;

    // Disallow.
    FiberCache(const FiberCache &) = delete;
    FiberCache &operator=(const FiberCache &) = delete;

    mutable Spinlock lock_;

    // Stack of cached fibers.
    Fiber *fibers_;
    size_t fibersCount_;
    uint64_t nextFiberId_;

    // Cache, at most, this many Fibers.
    size_t fibersLimit_;
    // Stop ingesting work when free system memory goes down to this.
    size_t lowMemThreshold_;

    size_t virtualBytesLimit_;  // Configured by libbc.
};

#endif  // FIBERCACHE_H
