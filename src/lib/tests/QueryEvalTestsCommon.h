// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERYEVALTESTSCOMMON_H_
#define _QUERYEVALTESTSCOMMON_H_

#include "primitives/Primitives.h"

static constexpr uint64_t RunsPerThreadDefault = 1;
static constexpr uint64_t ThreadsPerCoreDefault = 1;

Status qeDoEvaluateTest(const char *query);
Status qeDoStepThroughTest(const char *query,
                           const uint32_t threadNum,
                           Runtime::SchedId schedId);

#endif  // _QUERYEVALTESTSCOMMON_H_
