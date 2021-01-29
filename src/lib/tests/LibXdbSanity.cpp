// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>

#include "config/Config.h"
#include "xdb/Xdb.h"
#include "operators/Dht.h"
#include "df/DataFormat.h"
#include "operators/OperatorsHash.h"
#include "operators/Operators.h"
#include "util/MemTrack.h"
#include "common/InitTeardown.h"
#include "xdb/XdbInt.h"
#include "XdbTestsCommon.h"
#include "app/AppMgr.h"

#include "test/QA.h"  // Must be last

static TestCase testCases[] = {
    {"Set Up", xdbSetupRuntime, TestCaseEnable, ""},
#ifndef RUST
    {"xdb basic tests", xdbBasicTests, TestCaseEnable, ""},
    {"xdb cursor tests", xdbCursorTests, TestCaseEnable, ""},
    {"xdb string tests", xdbStringTests, TestCaseEnable, ""},
    {"xdbPgCursor tests", xdbPgCursorThreadStress, TestCaseEnable, ""},
    {"xdb Ordered vs Unordered tests",
     xdbOrderedVsUnorderedStress,
     TestCaseEnable,
     ""},
#endif  // RUST
    {"xdb cursor perf test", xdbPgCursorPerfTest, TestCaseEnable, ""},
    {"xdb create load drop test", xdbCreateLoadDropTest, TestCaseEnable, ""},
    {"xdb sort test", xdbSortSanityTest, TestCaseEnable, ""},
    // Must be last
    {"Tear Down", xdbTeardownRuntime, TestCaseEnable, ""},
};

static TestCaseOptionMask testCaseOptionMask = (TestCaseOptionMask)(
    TestCaseOptDisableIsPass | TestCaseScheduleOnRuntime | TestCaseSetTxn);

int
main(int argc, char *argv[])
{
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];
    int numFailedTests = 0;
    char usrNodePath[1 * KB];
    char *dirTest = dirname(argv[0]);

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirTest,
             cfgFile);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirTest,
             "../../bin/usrnode/usrnode");

    // Set up the test configuration before running the tests.
    unitTestConfig();

    verifyOk(InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode));

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
