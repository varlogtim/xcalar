// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERYOPTIMIZERTESTS_H_
#define _QUERYOPTIMIZERTESTS_H_

#include "primitives/Primitives.h"

Status optimizerStress();
Status optimizerStressParseConfig(Config::Configuration *config,
                                  char *key,
                                  char *value,
                                  bool stringentRules);

#endif  // _QUERYOPTIMIZERTESTS_H_
