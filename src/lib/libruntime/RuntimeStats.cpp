// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "RuntimeStats.h"

RuntimeStats::RuntimeStats() {}

RuntimeStats::~RuntimeStats() {}

Status
RuntimeStats::init()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("libruntime", &groupId_, CountStats);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&fiberBytes_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&fibersInUse_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&fibersInCache_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&fiberDeletes_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&fiberAllocFails_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&cacheHits_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&cacheMisses_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&makeRunnableFails_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&threadsJoinable_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&threadsDetachable_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&kickSchedsTxnAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&changeThreadsCount_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&fiberSchedThreadTxnAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&threadsAdded_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&threadsRemoved_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fiber.bytes",
                                         fiberBytes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fibers.inUse",
                                         fibersInUse_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fibers.inCache",
                                         fibersInCache_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fibers.deleted",
                                         fiberDeletes_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fiber.allocFails",
                                         fiberAllocFails_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fiberCache.hits",
                                         cacheHits_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "fiberCache.misses",
                                         cacheMisses_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "schedObj.makeRunnableFails",
                                         makeRunnableFails_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "threads.joinable",
                                         threadsJoinable_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "threads.detachable",
                                         threadsDetachable_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "runtime.kickSchedsTxnAbort",
                                         kickSchedsTxnAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "runtime.changeThreadsCount",
                                         changeThreadsCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "threads.txnAbort",
                                         fiberSchedThreadTxnAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "runtime.threadsAdded",
                                         threadsAdded_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "runtime.threadsRemoved",
                                         threadsRemoved_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}
