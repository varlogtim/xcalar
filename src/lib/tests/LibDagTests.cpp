// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <libgen.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "common/InitTeardown.h"
#include "test/FuncTests/DagTests.h"

#include "test/QA.h"  // Must be last

// Some functional test runs fast enough and is "unit-testy"
// enough to be included as sanity. runtimeTestManyProcs
// and runtimeTestOneProc are examples.
// We could in future create a very long-running runtimeTest,
// which we just don't include in sanity.
static TestCase testCases[] = {
    {"Dag test", dgSanityTest, TestCaseEnable, ""},
};

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];

    char *dirTest = dirname(argv[0]);
    char dirConfig[1024];

    strlcpy(dirConfig, dirTest, sizeof(dirConfig));

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirConfig,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    numTestsFailed =
        qaRunTestSuite(testCases,
                       ArrayLen(testCases),
                       (TestCaseOptionMask)(TestCaseOptDisableIsPass |
                                            TestCaseSetTxn));
    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
