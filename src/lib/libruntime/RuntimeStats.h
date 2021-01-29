// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef RUNTIMESTATS_H
#define RUNTIMESTATS_H

#include "primitives/Primitives.h"
#include "stat/Statistics.h"

//
// Handles reporting of all libruntime stats.
//

class RuntimeStats final
{
  public:
    RuntimeStats();
    ~RuntimeStats();

    Status init();

    // XXX These, ideally, wouldn't be public. But I really don't want to write
    //     a wrapper method for each op on each one.
    StatHandle fiberBytes_;
    StatHandle fibersInUse_;
    StatHandle fibersInCache_;
    StatHandle fiberDeletes_;
    StatHandle fiberAllocFails_;
    StatHandle cacheHits_;
    StatHandle cacheMisses_;
    StatHandle makeRunnableFails_;
    StatHandle threadsJoinable_;
    StatHandle threadsDetachable_;
    StatHandle kickSchedsTxnAbort_;
    StatHandle changeThreadsCount_;
    StatHandle fiberSchedThreadTxnAbort_;
    StatHandle threadsAdded_;
    StatHandle threadsRemoved_;

  private:
    static constexpr unsigned CountStats = 15;

    // Disallow.
    RuntimeStats(const RuntimeStats&) = delete;
    RuntimeStats(const RuntimeStats&&) = delete;
    RuntimeStats& operator=(const RuntimeStats&) = delete;
    RuntimeStats& operator=(const RuntimeStats&&) = delete;

    StatGroupId groupId_;
};

#endif  // RUNTIMESTATS_H
