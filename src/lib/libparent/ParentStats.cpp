// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "ParentStats.h"
#include "util/MemTrack.h"

//
// Creation of ParentStats. Happens once.
//

Status
ParentStats::create(ParentStats **parentStatsOut)
{
    Status status;
    void *ptr = NULL;
    ParentStats *parentStats = NULL;

    ptr = memAllocExt(sizeof(*parentStats), __PRETTY_FUNCTION__);
    BailIfNull(ptr);

    parentStats = new (ptr) ParentStats();

    status = parentStats->createInternal();
    BailIfFailed(status);

    *parentStatsOut = parentStats;

CommonExit:
    if (status != StatusOk) {
        if (ptr != NULL) {
            assert(parentStats != NULL);  // Constructor cannot fail.
            parentStats->~ParentStats();
            memFree(ptr);
        }
    }
    return status;
}

Status
ParentStats::createInternal()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status =
        statsLib->initNewStatGroup("libparent.parentStats", &this->groupId_, 6);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&this->childDeaths_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->childCreates_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->childWaiting_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->signals_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->waitMilliseconds_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&this->waitCount_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.terminated",
                                         this->childDeaths_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.created",
                                         this->childCreates_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.waitingThreads",
                                         this->childWaiting_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.sigchldSignals",
                                         this->signals_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.waitMilliseconds",
                                         this->waitMilliseconds_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(this->groupId_,
                                         "childnodes.waitCount",
                                         this->waitCount_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    // XXX Cleanup stats on failure.
    return status;
}

ParentStats::ParentStats() {}

//
// Destruction of ParentStats. Happens once.
//
void
ParentStats::del()
{
    this->~ParentStats();
    memFree(this);
}

ParentStats::~ParentStats() {}

//
// Events on which stats fire.
//
void
ParentStats::onChildCreate()
{
    StatsLib::statAtomicIncr32(this->childCreates_);
}

void
ParentStats::onChildDeath()
{
    StatsLib::statAtomicIncr32(this->childDeaths_);
}

void
ParentStats::startWaiting()
{
    StatsLib::statAtomicIncr32(this->childWaiting_);
}

void
ParentStats::stopWaiting(uint64_t elapsedMilliseconds)
{
    StatsLib::statAtomicIncr64(this->waitCount_);
    StatsLib::statAtomicAdd64(this->waitMilliseconds_, elapsedMilliseconds);
    StatsLib::statAtomicDecr32(this->childWaiting_);
}

void
ParentStats::onSignal()
{
    StatsLib::statAtomicIncr32(this->signals_);
}
