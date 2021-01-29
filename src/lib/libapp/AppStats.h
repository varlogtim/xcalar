// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APPSTATS_H
#define APPSTATS_H

#include "primitives/Primitives.h"
#include "stat/Statistics.h"

//
// Handles reporting of App stats.
//

class AppStats final
{
  public:
    AppStats() {}
    ~AppStats() {}

    Status init();

    void incrCountApps() { StatsLib::statNonAtomicIncr(countApps_); }

    void decrCountApps() { StatsLib::statNonAtomicDecr(countApps_); }

    void incrCountAppGroups() { StatsLib::statNonAtomicIncr(countAppGroups_); }

    void decrCountAppGroups() { StatsLib::statNonAtomicDecr(countAppGroups_); }

    void incrCountAppInstances()
    {
        StatsLib::statAtomicIncr64(countAppInstances_);
    }

    void decrCountAppInstances()
    {
        StatsLib::statAtomicDecr64(countAppInstances_);
    }

    void appStartFail(unsigned long long msecs)
    {
        StatsLib::statAtomicIncr64(countAppStartFail_);
        StatsLib::statNonAtomicSet64(msecsLastAppStartFail_, msecs);
    }

    void appStartSuccess(unsigned long long msecs)
    {
        StatsLib::statAtomicIncr64(countAppStartSuccess_);
        StatsLib::statNonAtomicSet64(msecsLastAppStartSuccess_, msecs);
    }

    void incCountAppWaitForResultTimeout()
    {
        StatsLib::statAtomicIncr64(countAppWaitForResultTimeout_);
    }

    void appGroupAbort() { StatsLib::statAtomicIncr64(appGroupAbort_); }

    void appInstanceAbort() { StatsLib::statAtomicIncr64(appInstanceAbort_); }

    void appInstanceAbortBarrier()
    {
        StatsLib::statAtomicIncr64(appInstanceAbortBarrier_);
    }

    void appInstanceStartPending()
    {
        StatsLib::statAtomicIncr64(appInstanceStartPending_);
    }

  private:
    // Disallow.
    AppStats(const AppStats&) = delete;
    AppStats(const AppStats&&) = delete;
    AppStats& operator=(const AppStats&) = delete;
    AppStats& operator=(const AppStats&&) = delete;

    StatGroupId groupId_;

    StatHandle countApps_;
    StatHandle countAppGroups_;
    StatHandle countAppInstances_;
    StatHandle countAppStartFail_;
    StatHandle countAppStartSuccess_;
    StatHandle msecsLastAppStartFail_;
    StatHandle msecsLastAppStartSuccess_;
    StatHandle countAppWaitForResultTimeout_;
    StatHandle appGroupAbort_;
    StatHandle appInstanceAbort_;
    StatHandle appInstanceAbortBarrier_;
    StatHandle appInstanceStartPending_;

    static constexpr unsigned CountStats = 12;
};

#endif  // APPSTATS_H
