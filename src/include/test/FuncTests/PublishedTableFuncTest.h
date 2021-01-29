// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PUBLISHED_TABLE_FUNC_TEST_H_
#define _PUBLISHED_TABLE_FUNC_TEST_H_

#include "primitives/Primitives.h"
#include "config/Config.h"

extern Status publishedTableFuncTest();
extern Status publishedTableFuncTestParseConfig(Config::Configuration *config,
                                                char *key,
                                                char *value,
                                                bool stringentRules);

#endif  // _PUBLISHED_TABLE_FUNC_TEST_H
