// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOG_TEST_H_
#define _LOG_TEST_H_

#include "primitives/Primitives.h"

Status logStress();
Status logStressParseConfig(Config::Configuration *config,
                            char *key,
                            char *value,
                            bool stringentRules);
Status logLoadDataFiles();
#endif  // _LOG_TEST_H
