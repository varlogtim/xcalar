// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TABLE_TESTS_H_
#define _TABLE_TESTS_H_

#include "primitives/Primitives.h"

Status TableTestsSanity();
Status TableTestsStress();
Status TableTestsParseConfig(Config::Configuration *config,
                            char *key,
                            char *value,
                            bool stringentRules);

#endif  // _TABLE_TESTS_H_
