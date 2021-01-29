// Copyright 2015-2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <libgen.h>

#include "config/Config.h"
#include "kvstore/KvStore.h"
#include "common/InitTeardown.h"
#include "KvStoreTestsCommon.h"

#include "test/QA.h"  // Must be last

static TestCase testCases[] = {
    {"KvStore tests", kvStoreTestsSanity, TestCaseEnable, ""},
    {"KvStore big message test",
     kvStoreBigMessageTestSanity,
     TestCaseEnable,
     ""},
    {"KvStore insanity", kvStoreBadUserTestSanity, TestCaseEnable, ""},
    {"KvStore random", kvStoreRandomTestSanity, TestCaseEnable, ""},
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

    verifyOk(InitTeardown::init(InitLevel::UsrNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeNone));

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
