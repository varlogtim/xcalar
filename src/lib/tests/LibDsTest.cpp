// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/types.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <cstdlib>
#include <libgen.h>

#include "primitives/Primitives.h"
#include "dataset/Dataset.h"
#include "df/DataFormat.h"
#include "DataFormatConstants.h"
#include "export/DataTarget.h"
#include "config/Config.h"
#include "operators/Dht.h"
#include "DatasetInt.h"
#include "xdb/Xdb.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "usrnode/UsrNode.h"
#include "sys/XLog.h"
#include "table/ResultSet.h"
#include "callout/Callout.h"
#include "strings/String.h"
#include "operators/OperatorsApiWrappers.h"
#include "common/InitTeardown.h"
#include "libapis/LibApisRecv.h"
#include "constants/XcalarConfig.h"
#include "DataSetTestsCommon.h"
#include "parent/Parent.h"

#include "runtime/ShimSchedulable.h"
#include "app/AppMgr.h"

#include "test/QA.h"  // Must be last

using namespace df;

static TestCase testCases[] = {
    {"Set Up", dsSetUp, TestCaseEnable, ""},
    {"Accessor parser tests", dsAccessorTest, TestCaseEnable, ""},
    {"Fat pointer tests", dsFatptrTests, TestCaseEnable, ""},
    {"DHT persistence test", dsDhtPersistTest, TestCaseEnable, ""},
    {"Type conversion test", dsTypeConversionTest, TestCaseEnable, "2250"},

    // MUST BE LAST
    {"Tear Down", dsTearDown, TestCaseEnable, ""},
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

    Status status;

    status = InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                usrNodePath,
                                InitFlagsNone,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode);
    assert(status == StatusOk);

    numFailedTests =
        qaRunTestSuite(testCases, ArrayLen(testCases), testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numFailedTests;
}
