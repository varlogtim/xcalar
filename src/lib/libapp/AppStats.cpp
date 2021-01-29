// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "AppStats.h"

Status
AppStats::init()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("libapp", &groupId_, CountStats);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&countApps_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&countAppGroups_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&countAppInstances_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&countAppStartFail_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&countAppStartSuccess_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&msecsLastAppStartFail_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&msecsLastAppStartSuccess_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&countAppWaitForResultTimeout_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&appGroupAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&appInstanceAbort_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&appInstanceAbortBarrier_);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&appInstanceStartPending_);
    BailIfFailed(status);

    status = statsLib->initAndMakeGlobal(groupId_,
                                         "app.count",
                                         countApps_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appGroup.count",
                                         countAppGroups_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.count",
                                         countAppInstances_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.countStartFail",
                                         countAppStartFail_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.countStartSuccess",
                                         countAppStartSuccess_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.msecsLastStartFail",
                                         msecsLastAppStartFail_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.msecsLastStartSuccess",
                                         msecsLastAppStartSuccess_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status =
        statsLib->initAndMakeGlobal(groupId_,
                                    "appInstance.countAppWaitForResultTimeout",
                                    countAppWaitForResultTimeout_,
                                    StatUint64,
                                    StatAbsoluteWithNoRefVal,
                                    StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appGroup.appGroupAbort",
                                         appGroupAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.appInstanceAbort",
                                         appInstanceAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.appInstanceAbortBarrier",
                                         appInstanceAbortBarrier_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(groupId_,
                                         "appInstance.appInstanceStartPending",
                                         appInstanceStartPending_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

CommonExit:
    return status;
}
