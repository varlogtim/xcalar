// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "util/TrackHelpers.h"
#include "util/MemTrack.h"
#include "util/Atomics.h"
#include "runtime/Runtime.h"

static constexpr const char *moduleName = "TrackHelpers";

TrackHelpers::TrackHelpers() {}

TrackHelpers::~TrackHelpers()
{
    assert(this->helpersActive == 0);
    assert(this->helpersOutstanding == 0);

    delete[] this->workUnitsState;
    this->workUnitsState = NULL;
}

Status
TrackHelpers::helperStart()
{
    Status status = StatusUnknown;

    this->lock.lock();

    if (this->workDone) {
        // Work is already done. Helper is just showing up late here.
        status = StatusAllWorkDone;
    } else {
        // Helper has showed up for work!
        status = StatusOk;
    }

    assert(this->helpersActive < this->helpersTotal);
    this->helpersActive++;

    this->lock.unlock();

    return status;
}

uint64_t
TrackHelpers::getWorkUnitsTotal()
{
    return this->workUnitsTotal;
}

uint64_t
TrackHelpers::getHelpersTotal()
{
    return this->helpersTotal;
}

Status
TrackHelpers::getSavedWorkStatus()
{
    this->lock.lock();
    Status ret = savedWorkStatus;
    this->lock.unlock();
    return ret;
}

TrackHelpers *
TrackHelpers::setUp(Status *status,
                    uint64_t totalHelpers,
                    uint64_t totalWorkUnits)
{
    TrackHelpers *trackHelpers = new (std::nothrow) TrackHelpers();
    if (trackHelpers == NULL) {
        return NULL;
    }

    trackHelpers->workStatus = status;
    trackHelpers->helpersTotal = totalHelpers;

    // Account here for the caller that's recruiting totalHelpers.
    trackHelpers->helpersOutstanding = totalHelpers + 1;
    trackHelpers->helpersActive = 0;
    trackHelpers->workUnitsTotal = totalWorkUnits;
    trackHelpers->workUnitsOutstanding = totalWorkUnits;

    trackHelpers->workUnitsState = new (std::nothrow) Atomic32[totalWorkUnits];
    if (trackHelpers->workUnitsState == NULL) {
        delete trackHelpers;
        return NULL;
    }

    for (uint64_t ii = 0; ii < totalWorkUnits; ii++) {
        atomicWrite32(&trackHelpers->workUnitsState[ii], WorkUnitRunnable);
    }
    return trackHelpers;
}

void
TrackHelpers::tearDown(TrackHelpers **trackHelpers)
{
    (*trackHelpers)->lock.lock();

    assert((*trackHelpers)->helpersOutstanding > 0);
    (*trackHelpers)->helpersOutstanding--;
    if ((*trackHelpers)->helpersOutstanding > 0) {
        (*trackHelpers)->lock.unlock();
        return;
    }

    (*trackHelpers)->lock.unlock();

    // All outstanding helpers have completed, so it's time to destruct the
    // trackHelper.
    delete (*trackHelpers);
    (*trackHelpers) = NULL;
}

bool
TrackHelpers::workUnitRunnable(uint64_t workUnitIdx)
{
    while (true) {
        TrackHelpers::WorkUnitState workUnitState =
            (TrackHelpers::WorkUnitState) atomicRead32(
                &this->workUnitsState[workUnitIdx]);
        if (workUnitState != TrackHelpers::WorkUnitRunnable) {
            return false;
        }
        if (TrackHelpers::WorkUnitRunnable ==
            atomicCmpXchg32(&this->workUnitsState[workUnitIdx],
                            TrackHelpers::WorkUnitRunnable,
                            TrackHelpers::WorkUnitRunning)) {
            return true;
        }
    }
    // Never reached.
}

void
TrackHelpers::workUnitComplete(uint64_t workUnitIdx)
{
    assert(TrackHelpers::WorkUnitRunning ==
           atomicRead32(&this->workUnitsState[workUnitIdx]));
    atomicWrite32(&this->workUnitsState[workUnitIdx],
                  TrackHelpers::WorkUnitComplete);

    this->lock.lock();
    assert(this->workUnitsOutstanding > 0);
    this->workUnitsOutstanding--;
    this->lock.unlock();
}

void
TrackHelpers::helperDone(Status status)
{
    bool cleanup = false;

    this->lock.lock();

    // Only update workStatus, if work is outstanding.
    if (this->workStatus && *this->workStatus == StatusOk &&
        status != StatusOk) {
        *this->workStatus = status;
        savedWorkStatus = status;
        // Only track the first instance of error here.
        this->workStatus = NULL;
        assert(this->workDone == false);
    }

    assert(this->helpersActive > 0);
    this->helpersActive--;
    if (this->workDone == false && this->workUnitsOutstanding == 0 &&
        this->helpersActive == 0) {
        this->workDone = true;
        this->workStatus = NULL;

        // All work is done, so let the waiter know about this. Notice
        // here that there is no need to stall the waiter until all
        // the outstanding helpers are done.
        this->cv.broadcast();
    }

    assert(this->helpersOutstanding > 0);
    this->helpersOutstanding--;
    cleanup = this->helpersOutstanding == 0 ? true : false;
    this->lock.unlock();

    // If all outstanding helpers are done, just cleanout trackHelpers.
    if (cleanup) {
        delete this;
    }
}

void
TrackHelpers::waitForAllWorkDone()
{
    // Note that we just wait for all the work to be done and not necessarily
    // all the helpers to havecompleted.
    this->lock.lock();
    while (!this->workDone) {
        this->cv.wait(&lock);
    }
    this->lock.unlock();
}

Status
TrackHelpers::schedThroughput(Schedulable **scheds, unsigned numScheds)
{
    unsigned numScheduled = 0;
    Status schedStatus;
    Status status;

    // Idea is that we have atleast recruited one worker to do the throughpput
    // work to report success.
    for (unsigned ii = 0; ii < numScheds; ii++) {
        Status tmpStatus = Runtime::get()->schedule(scheds[ii]);
        if (tmpStatus != StatusOk) {
            if (!numScheduled) {
                schedStatus = tmpStatus;
            }
        } else {
            numScheduled++;
            scheds[ii] = NULL;
        }
    }

    // If we did not recruit a single worker, just report the first instance of
    // scheduling failure.
    if (!numScheduled && schedStatus != StatusOk && status == StatusOk) {
        status = schedStatus;
    }

    return status;
}
