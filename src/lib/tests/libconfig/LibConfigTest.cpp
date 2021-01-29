// Copyright 2013 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <libgen.h>
#include <assert.h>
#include <sys/wait.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "primitives/Macros.h"
#include "config/Config.h"
#include "util/FileUtils.h"
#include "common/InitTeardown.h"

#include "app/AppMgr.h"
#include "runtime/ShimSchedulable.h"
#include "libapis/LibApisRecv.h"
#include "test/QA.h"
#include "util/MemTrack.h"
#include "parent/Parent.h"

static Status configSanity();
static Status configHostnames();
static Status addLoadApp();
static Status configTearDown();

const char *cfgFile = "test-config.cfg";
char fullCfgFilePath[255];

char testMode[] = "TestMode";
char testModeValue[] = "True";
char memorySize[] = "555";
char miniDump[] = "Minidump";
char miniDumpValue[] = "true";

static TestCase testCases[] = {
    {"addLoadApp", addLoadApp, TestCaseEnable, ""},
    {"configSanity", configSanity, TestCaseEnable, ""},
    {"configHostnames", configHostnames, TestCaseEnable, ""},
    // Must be last
    {"Tear Down", configTearDown, TestCaseEnable, ""},
};

// We can't run this test on the runtime, because there is a test
// where we have to let the runnables drain
static TestCaseOptionMask testCaseOptionMask =
    (TestCaseOptionMask)(TestCaseOptDisableIsPass |
                         TestCaseScheduleOnDedicatedThread | TestCaseSetTxn);
static void
testWriteNewConfig()
{
    Status status;
    // 8 extra for "testcopy", 1 for null terminator
    const int pathSize = 255 + 8 + 1;
    char copyCfgFilePath[pathSize];

    Config *cfg = Config::get();
    assert(cfg != NULL);

    // Make a copy of the config file as we don't want to change the
    // original.
    snprintf(copyCfgFilePath, pathSize, "%s-%s", fullCfgFilePath, "testcopy");

    unlink(copyCfgFilePath);

    status = FileUtils::copyFile(copyCfgFilePath, fullCfgFilePath);
    assert(status == StatusOk);

    status = cfg->loadConfigFiles(copyCfgFilePath);
    assert(status == StatusOk);

    status = cfg->setNumActiveNodes(1);
    assert(status == StatusOk);
    cfg->setNumActiveNodesOnMyPhysicalNode(1);

    // Try to set a parameter that is immutable.
    status = cfg->setConfigParam(testMode, testModeValue);
    assert(status == StatusConfigParamImmutable);

    // Set parameter to new value
    status = cfg->setConfigParam(miniDump, miniDumpValue);
    assert(status == StatusOk);

    status = cfg->writeConfigFiles(copyCfgFilePath);
    assert(status == StatusOk);

    // We have to let the runnables drain here because writeConfigFiles
    // uses calls a 2pc, and we have to process the completion before
    // we can unloadConfig
    setUsrNodeForceShutdown();
    Runtime::get()->drainAllRunnables();
    unsetUsrNodeForceShutdown();
    cfg->unloadConfigFiles();

    status = cfg->loadConfigFiles(copyCfgFilePath);
    assert(status == StatusOk);

    // Check that the value we set is still true
    assert(XcalarConfig::get()->minidump_ == true);

    // cfg->unloadConfigFiles();
    unlink(copyCfgFilePath);
}

static Status
configSanity()
{
    Status status;
    unsigned numNodes = 0;

    Config *config = Config::get();

    config->setMyNodeId(0);
    config->unloadConfigFiles();
    verifyOk(config->loadConfigFiles(fullCfgFilePath));

    numNodes = config->getTotalNodes();
    assert(numNodes > 0);

    if (numNodes < 4) {
        goto CommonExit;
    }

    status = config->setNumActiveNodes(4);
    assert(status == StatusOk);
    verifyOk(config->loadNodeInfo());
    assert(config->getActiveNodes() == 4);
    assert(config->getActiveNodesOnMyPhysicalNode() == 4);

    config->unloadConfigFiles();
    status = config->loadConfigFiles(fullCfgFilePath);
    assert(status == StatusOk);

    if (numNodes < 16) {
        goto CommonExit;
    }

    status = config->setNumActiveNodes(16);
    assert(status == StatusOk);
    verifyOk(config->loadNodeInfo());
    assert(config->getActiveNodes() == 16);
    assert(config->getActiveNodesOnMyPhysicalNode() == 16);

    config->unloadConfigFiles();
    status = config->loadConfigFiles(fullCfgFilePath);
    assert(status == StatusOk);

    if (numNodes < 64) {
        goto CommonExit;
    }

    status = config->setNumActiveNodes(64);
    assert(status == StatusOk);
    verifyOk(config->loadNodeInfo());
    assert(config->getActiveNodes() == 64);
    assert(config->getActiveNodesOnMyPhysicalNode() == 64);

CommonExit:
    config->unloadConfigFiles();

    testWriteNewConfig();

    status = StatusOk;
    return status;
}

static Status
configHostnames()
{
    Config *config = Config::get();
    char me[MaxHostName];

    unsigned numNodes = 4;
    Config::ConfigNode nodes[numNodes];
    for (unsigned i = 0; i < numNodes; i++) {
        nodes[i].local = false;
    }
    nodes[0].ipAddr = (char *) "127.0.0.1";
    nodes[1].ipAddr = (char *) "localhost";
    nodes[2].ipAddr = (char *) "";
    nodes[3].ipAddr = (char *) "somebodyelse";

    verify(gethostname(me, sizeof(me)) == 0);

    nodes[2].ipAddr = me;

    verify(config->configIdentifyLocalNodes(nodes,
                                            (unsigned) ArrayLen(nodes)) == 3);
    verify(nodes[0].local);

    nodes[2].local = false;
    verify(config->configIdentifyLocalNodes(&nodes[2], 1) == 1);
    verify(nodes[2].local);

    nodes[0].ipAddr = (char *) "somebodyelse";
    nodes[0].local = false;
    nodes[1].local = false;
    nodes[2].local = false;
    verify(config->configIdentifyLocalNodes(nodes,
                                            (unsigned) ArrayLen(nodes)) == 2);
    verify(!nodes[0].local);
    verify(nodes[1].local);

    return StatusOk;
}

// Load the build in Apps which includes writeFileAllNodes which is required
// to update the config file.
static Status
addLoadApp()
{
    return AppMgr::get()->addBuildInApps();
}

static Status
configTearDown()
{
    // Need to have active nodes
    verifyOk(Config::get()->setNumActiveNodes(1));

    AppMgr::get()->removeBuiltInApps();

    return StatusOk;
}

int
main(int argc, char *argv[])
{
    int numTestsFailed;
    char usrNodePath[1 * KB];

    snprintf(fullCfgFilePath,
             sizeof(fullCfgFilePath),
             "%s/%s",
             dirname(argv[0]),
             cfgFile);
    printf("%s\n", fullCfgFilePath);
    snprintf(usrNodePath,
             sizeof(usrNodePath),
             "%s/%s",
             dirname(argv[0]),
             "../../bin/usrnode/usrnode");

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

    numTestsFailed = qaRunTestSuite(testCases,
                                    (unsigned) ArrayLen(testCases),
                                    testCaseOptionMask);

    if (InitTeardown::get() != NULL) {
        InitTeardown::get()->teardown();
    }

    return numTestsFailed;
}
