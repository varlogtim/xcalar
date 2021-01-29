// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <strings.h>
#include <assert.h>
#include <unistd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "config/Config.h"
#include "util/System.h"
#include "sys/XLog.h"

#include "util/MemTrack.h"
#include "strings/String.h"
#include "constants/XcalarConfig.h"
#include "MsgTests.h"
#include "XdbTests.h"
#include "BcStressTest.h"
#include "OperatorsFuncTest.h"
#include "KvStoreTests.h"
#include "QueryEvalTests.h"
#include "OptimizerTests.h"
#include "LogTest.h"
#include "LibNsTests.h"
#include "SessionTest.h"
#include "QueryManagerTests.h"
#include "DagTests.h"
#include "RuntimeTests.h"
#include "AppFuncTests.h"
#include "TableTests.h"

#include "test/QA.h"

static constexpr const char *moduleName = "ConfigParseFuncTests";

Status
ConfigModuleFuncTests::parse(Config::Configuration *config,
                             char *key,
                             char *value,
                             bool stringentRules)
{
    Status status;

    if (strcasestr(key, "LibMsg")) {
        status = msgStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibXdb")) {
        status = xdbStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibBc")) {
        status = bcStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibOperators")) {
        status = operatorsStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibKvStore")) {
        status = kvStoreStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibQueryEval")) {
        status = queryEvalStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibOptimizer")) {
        status = optimizerStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibLog")) {
        status = logStressParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibNs")) {
        status = libNsFuncTestParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibSession")) {
        status =
            libsessionFuncTestParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibQm")) {
        status =
            QueryManagerTests::parseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibRuntime")) {
        status = RuntimeTests::parseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibApp")) {
        status = AppFuncTests::parseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibDag")) {
        status = libDagTestParseConfig(config, key, value, stringentRules);
    } else if (strcasestr(key, "LibTable")) {
        status = TableTestsParseConfig(config, key, value, stringentRules);
    } else {
        status = StatusUsrNodeIncorrectParams;
    }
    return status;
}
