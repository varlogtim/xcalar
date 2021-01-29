// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPTIMIZERCOMMON_H_
#define _OPTIMIZERCOMMON_H_

#include "primitives/Primitives.h"

static constexpr uint64_t RunsPerThreadDefault = 10;
static constexpr uint64_t ThreadsPerCoreDefault = 10;

Status optDoOptimizerTest(uint32_t threadNum);

#endif  // _OPTIMIZERCOMMON_H_
