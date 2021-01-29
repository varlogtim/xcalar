// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <libgen.h>
#include <unordered_set>
#include "StrlFunc.h"
#include "common/InitTeardown.h"
#include "parent/Parent.h"
#include "parent/ParentChild.h"
#include "config/Config.h"
#include "udf/UserDefinedFunction.h"
#include "log/Log.h"
#include "UdfParent.h"
#include "test/QA.h"

using std::unordered_set;

static unsigned tooManyChildren = 0;
static unsigned tooManyChildrenThreads = 0;
static unordered_set<unsigned> tooManyChildrenUnique;

// XXX TODO Contruction needs to be init ordered
static Mutex tooManyChildrenUniqueLock;

static void *
parentTestManyChildrenThread(void *ignored)
{
    assert(tooManyChildren > 1);
    assert(tooManyChildrenThreads > 1);
    assert(tooManyChildren % tooManyChildrenThreads == 0);

    unordered_set<unsigned> unique;
    unsigned childrenCount = tooManyChildren / tooManyChildrenThreads;
    ParentChild *children[childrenCount];

    Status status =
        Parent::get()->getChildren(children,
                                   childrenCount,
                                   ParentChild::Level::User,
                                   ParentChild::getXpuSchedId(
                                       Txn::currentTxn().rtSchedId_));
    assert(status == StatusOk);

    for (unsigned ii = 0; ii < childrenCount; ii++) {
        unique.insert(children[ii]->getId());  // Throw -> failure.
        Parent::get()->putChild(children[ii]);
    }

    tooManyChildrenUniqueLock.lock();
    for (const auto &uniqueId : unique) {
        tooManyChildrenUnique.insert(uniqueId);
    }
    tooManyChildrenUniqueLock.unlock();

    return NULL;
}

static Status
parentTestManyChildren()
{
    long logicalProcCount = (unsigned) XcSysHelper::get()->getNumOnlineCores();
    assert(logicalProcCount > 0);

    // Must be many more than we'd create (i.e. bigger than what's used in
    // parentInit).
    tooManyChildrenThreads = logicalProcCount;
    tooManyChildren = tooManyChildrenThreads * 10;

    pthread_t threads[tooManyChildrenThreads];
    for (unsigned ii = 0; ii < tooManyChildrenThreads; ii++) {
        verifyOk(
            Runtime::get()->createBlockableThread(&threads[ii],
                                                  NULL,
                                                  parentTestManyChildrenThread,
                                                  NULL));
    }

    for (unsigned ii = 0; ii < tooManyChildrenThreads; ii++) {
        verify(sysThreadJoin(threads[ii], NULL) == 0);
    }

    // XXX Need a way to query stats from unit tests.

    // Verify *some* upper limit was applied and we didn't create this many
    // actual processes due to combination of caching and limit.
    printf("[%s] Created %lu child processes for %u requests.\n",
           __FUNCTION__,
           tooManyChildrenUnique.size(),
           tooManyChildren);

    tooManyChildrenUnique.clear();

    return StatusOk;
}

static Status
parentTestBasicApi()
{
    for (unsigned jj = 0; jj < 16; jj++) {
        unsigned childrenCount = 128;
        ParentChild *children[childrenCount];

        Status status =
            Parent::get()->getChildren(children,
                                       childrenCount,
                                       ParentChild::Level::User,
                                       ParentChild::getXpuSchedId(
                                           Txn::currentTxn().rtSchedId_));
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < childrenCount; ii++) {
                Parent::get()->putChild(children[ii]);
            }
        }
    }

    return StatusOk;
}

static TestCase testCases[] = {
    {"parent-child test: parent API", parentTestBasicApi, TestCaseEnable, ""},
    {"parent-child test: many children",
     parentTestManyChildren,
     TestCaseEnable,
     ""},
};

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    const char *cfgFile = "test-config.cfg";
    char fullCfgFilePath[255];

    char *dirTest = dirname(argv[0]);
    char dirConfig[1024];
    char dirChildNode[1024];

    strlcpy(dirConfig, dirTest, sizeof(dirConfig));
    strlcpy(dirChildNode, dirTest, sizeof(dirChildNode));

    strlcat(dirConfig, "/../", sizeof(dirConfig));
    strlcat(dirChildNode, "/../../../bin/childnode/", sizeof(dirChildNode));

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s%s",
             dirConfig,
             cfgFile);

    verifyOk(InitTeardown::init(InitLevel::UsrNodeWithChildNode,
                                SyslogFacilityTest,
                                fullCfgFilePath,
                                NULL,
                                argv[0],
                                InitFlagSkipRestoreUdfs,
                                0 /* My node ID */,
                                1 /* Num active */,
                                1 /* Num on physical */,
                                BufferCacheMgr::TypeUsrnode));

    numTestsFailed =
        qaRunTestSuite(testCases,
                       ArrayLen(testCases),
                       (TestCaseOptionMask)(TestCaseOptDisableIsPass |
                                            TestCaseScheduleOnRuntime |
                                            TestCaseSetTxn));

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
