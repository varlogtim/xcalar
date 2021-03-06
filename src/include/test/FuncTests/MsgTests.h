// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGTESTS_H_
#define _MSGTESTS_H_

#include "primitives/Primitives.h"
#include "config/Config.h"

Status msgStress();
Status msgStressParseConfig(Config::Configuration *config,
                            char *key,
                            char *value,
                            bool stringentRules);

#endif  // _MSGTESTS_H_
