// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDBTESTS_H_
#define _XDBTESTS_H_

#include "primitives/Primitives.h"
#include "config/Config.h"

Status xdbStress();
Status xdbSortTest(uint64_t numRows);
Status newCursorBenchmark();
Status xdbStressParseConfig(Config::Configuration *config,
                            char *key,
                            char *value,
                            bool stringentRules);

#endif  // _XDBTESTS_H_
