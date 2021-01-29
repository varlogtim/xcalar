// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef PARENTSTATS_H_
#define PARENTSTATS_H_

#include "primitives/Primitives.h"
#include "stat/Statistics.h"

// XXX Move to libparent following Parent.cpp's migration to libparent.

//
// This class exists to avoid stats stuff cluttering the Parent code.
//

class ParentStats
{
  public:
    static Status create(ParentStats **parentStatsOut);
    void del();

    void onChildCreate();
    void onChildDeath();
    void startWaiting();
    void stopWaiting(uint64_t elapsedMilliseconds);
    void onSignal();
    void xpuResourceAcquireRetry();
    void xpuResourceAcquire();
    void xpuResourceRelease();
    void xpuResourceAvailable();

  private:
    ParentStats();
    ~ParentStats();

    Status createInternal();

    StatGroupId groupId_;

    StatHandle childDeaths_;
    StatHandle childCreates_;
    StatHandle childWaiting_;
    StatHandle signals_;
    StatHandle waitMilliseconds_;
    StatHandle waitCount_;
    StatHandle xpuResourceAcquireRetry_;
    StatHandle xpuResourceAcquire_;
    StatHandle xpuResourceRelease_;
    StatHandle xpuResourceAvailable_;
};

#endif  // PARENTSTATS_H_
