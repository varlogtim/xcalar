// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "GetOpt.h"
#include "test/FuncTests/FuncTestDriver.h"

typedef void (*MainFn)(const char *moduleName,
                       int argc,
                       char *argv[],
                       bool prettyPrint,
                       bool interactive);
typedef void (*UsageFn)(const char *moduleName, int argc, char *argv[]);

// list
static void funcTestsList(const char *moduleName,
                          int argc,
                          char *argv[],
                          bool prettyPrint,
                          bool interactive);
static void funcTestsListHelp(const char *moduleName, int argc, char *argv[]);

// run
static void funcTestsRun(const char *moduleName,
                         int argc,
                         char *argv[],
                         bool prettyPrint,
                         bool interactive);
static void funcTestsRunHelp(const char *moduleName, int argc, char *argv[]);

struct SubCmdMappings {
    const char *subCmdString;
    const char *subCmdDesc;
    MainFn main;
    UsageFn usageFn;
};

const SubCmdMappings subCmdMappings[] = {
    {
        .subCmdString = "run",
        .subCmdDesc = "Starts functional tests",
        .main = funcTestsRun,
        .usageFn = funcTestsRunHelp,
    },
    {
        .subCmdString = "list",
        .subCmdDesc = "Lists functional tests",
        .main = funcTestsList,
        .usageFn = funcTestsListHelp,
    },
};

void
cliFuncTestsHelp(int argc, char *argv[])
{
    unsigned ii;
    if (argc > 1) {
        for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
            if (strcmp(argv[1], subCmdMappings[ii].subCmdString) == 0) {
                if (subCmdMappings[ii].usageFn != NULL) {
                    subCmdMappings[ii].usageFn(argv[0], argc - 1, &argv[1]);
                } else {
                    printf("No help found for %s\n",
                           subCmdMappings[ii].subCmdString);
                }
                return;
            }
        }
    }

    printf("Usage: %s <functests-sub-command>\n", argv[0]);
    printf("Possible <functests-sub-command> are:\n");
    for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
        printf("  %s - %s\n",
               subCmdMappings[ii].subCmdString,
               subCmdMappings[ii].subCmdDesc);
    }
}

void
cliFuncTestsMain(int argc,
                 char *argv[],
                 XcalarWorkItem *workItemIn,
                 bool prettyPrint,
                 bool interactive)
{
    unsigned ii;

    assert(workItemIn == NULL);

    if (argc == 1) {
        cliFuncTestsHelp(argc, argv);
        return;
    }

    for (ii = 0; ii < ArrayLen(subCmdMappings); ii++) {
        if (strcmp(argv[1], subCmdMappings[ii].subCmdString) == 0) {
            assert(subCmdMappings[ii].main != NULL);
            subCmdMappings[ii].main(argv[0],
                                    argc - 1,
                                    &argv[1],
                                    prettyPrint,
                                    interactive);
            return;
        }
    }

    fprintf(stderr, "Error: No such command %s\n", argv[1]);
    cliFuncTestsHelp(argc, argv);
}

static void
funcTestsRunHelp(const char *moduleName, int argc, char *argv[])
{
    printf(
        "Usage: %s %s [--parallel] [--allNodes]"
        "[--testCase <testCasePattern> ... --testCase <testCasePattern> ]\n"
        "Note that if no test cases are provided, all test cases shall be "
        "run\n",
        moduleName,
        argv[0]);
}

struct FuncTestRunArgs {
    bool parallel;
    bool runAllTests;
    bool runOnAllNodes;
    unsigned numTestPatterns;
    const char *testNamePatterns[XcalarApiMaxNumFuncTests];
};

static Status
parseFuncTestsRun(int argc, char *argv[], FuncTestRunArgs *funcTestRunArgs)
{
    unsigned numTestPatterns = 0;
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"parallel", no_argument, 0, 'p'},
        {"allNodes", no_argument, 0, 'a'},
        {"testCase", required_argument, 0, 't'},
        {0, 0, 0, 0},
    };
    GetOptDataThr optData;

    funcTestRunArgs->parallel = false;
    funcTestRunArgs->runOnAllNodes = false;
    funcTestRunArgs->runAllTests = true;
    funcTestRunArgs->numTestPatterns = 0;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "pat:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'p':
            funcTestRunArgs->parallel = true;
            break;
        case 'a':
            funcTestRunArgs->runOnAllNodes = true;
            break;
        case 't':
            if (numTestPatterns >= XcalarApiMaxNumFuncTests) {
                fprintf(stderr,
                        "Too many test cases. Maximum is %u\n",
                        XcalarApiMaxNumFuncTests);
                return StatusCliParseError;
            }

            funcTestRunArgs->testNamePatterns[numTestPatterns++] =
                optData.optarg;
            break;
        default:
            return StatusCliParseError;
        }
    }

    if (numTestPatterns > 0) {
        funcTestRunArgs->runAllTests = false;
        funcTestRunArgs->numTestPatterns = numTestPatterns;
    }

    return StatusOk;
}

static void
funcTestsRun(const char *moduleName,
             int argc,
             char *argv[],
             bool prettyPrint,
             bool interactive)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiStartFuncTestOutput *startFuncTestOutput = NULL;
    Status status = StatusUnknown;
    FuncTestRunArgs funcTestRunArgs;
    unsigned ii;

    status = parseFuncTestsRun(argc, argv, &funcTestRunArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem =
        xcalarApiMakeStartFuncTestWorkItem(funcTestRunArgs.parallel,
                                           funcTestRunArgs.runAllTests,
                                           funcTestRunArgs.runOnAllNodes,
                                           funcTestRunArgs.numTestPatterns,
                                           funcTestRunArgs.testNamePatterns);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    startFuncTestOutput = &workItem->output->outputResult.startFuncTestOutput;
    printf("NumTests: %u\n", startFuncTestOutput->numTests);
    for (ii = 0; ii < startFuncTestOutput->numTests; ii++) {
        printf("FUNCTEST_RESULT: \"%s\"\t%s\n",
               startFuncTestOutput->testOutputs[ii].testName,
               strGetFromStatusCode(
                   startFuncTestOutput->testOutputs[ii].status));
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        startFuncTestOutput = NULL;
    }

    if (status == StatusCliParseError) {
        funcTestsRunHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        printf("FUNCTEST_ERROR: %s\n", strGetFromStatus(status));
    }
}

static void
funcTestsListHelp(const char *moduleName, int argc, char *argv[])
{
    printf("Usage: %s %s [testNamePattern]\n", moduleName, argv[0]);
}

static void
funcTestsList(const char *moduleName,
              int argc,
              char *argv[],
              bool prettyPrint,
              bool interactive)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListFuncTestOutput *listFuncTestOutput = NULL;
    Status status = StatusUnknown;
    unsigned ii;
    const char *namePattern = "*";

    if (argc >= 2 && argv[1][0] != '\0') {
        namePattern = argv[1];
    }

    if (strlen(namePattern) > FuncTestTypes::MaxTestNameLen) {
        fprintf(stderr,
                "[testNamePattern] is too long (%lu chars). "
                "Max is %lu chars",
                strlen(argv[1]),
                FuncTestTypes::MaxTestNameLen);
        status = StatusNoBufs;
        goto CommonExit;
    }

    workItem = xcalarApiMakeListFuncTestWorkItem(namePattern);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    listFuncTestOutput = &workItem->output->outputResult.listFuncTestOutput;
    printf("NumTests: %u\n", listFuncTestOutput->numTests);
    for (ii = 0; ii < listFuncTestOutput->numTests; ii++) {
        printf("%s\n", listFuncTestOutput->testName[ii]);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        listFuncTestOutput = NULL;
    }

    if (status == StatusCliParseError) {
        funcTestsListHelp(moduleName, argc, argv);
    } else if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
