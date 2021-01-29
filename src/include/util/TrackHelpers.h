// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TRACKHELPERS_H
#define _TRACKHELPERS_H

#include "primitives/Primitives.h"
#include "runtime/Mutex.h"
#include "runtime/Semaphore.h"
#include "util/AtomicTypes.h"
#include "runtime/CondVar.h"
#include "runtime/Runtime.h"

class TrackHelpers
{
  public:
    enum WorkerType { Master, NonMaster };
    static MustCheck TrackHelpers *setUp(Status *status,
                                         uint64_t totalHelpers,
                                         uint64_t totalWorkUnits);
    static void tearDown(TrackHelpers **trackHelpers);
    MustCheck bool workUnitRunnable(uint64_t workUnitIdx);
    void workUnitComplete(uint64_t workUnitIdx);
    void waitForAllWorkDone();
    void helperDone(Status status);
    MustCheck uint64_t getHelpersTotal();
    MustCheck uint64_t getWorkUnitsTotal();
    MustCheck Status helperStart();
    MustCheck Status schedThroughput(Schedulable **scheds, unsigned numScheds);
    MustCheck Status getSavedWorkStatus();

  private:
    Mutex lock;
    CondVar cv;
    Status savedWorkStatus = StatusOk;
    Status *workStatus = NULL;
    uint64_t helpersTotal = 0;
    uint64_t helpersOutstanding = 0;
    uint64_t helpersActive = 0;
    uint64_t workUnitsTotal = 0;
    uint64_t workUnitsOutstanding;
    bool workDone = false;

    enum WorkUnitState : uint32_t {
        WorkUnitInvalid = 0x0,
        WorkUnitRunnable = 0x1,
        WorkUnitRunning = 0x2,
        WorkUnitComplete = 0x3,
    };
    Atomic32 *workUnitsState = NULL;

    // Keep this private. Use setUp instead.
    TrackHelpers();

    // Keep this private. Use tearDown instead.
    ~TrackHelpers();

    // Sorry folks! No operator overloading allowed here.
    TrackHelpers(const TrackHelpers &) = delete;
    TrackHelpers &operator=(const TrackHelpers &) = delete;
};

#endif  // _TRACKHELPERS_H
