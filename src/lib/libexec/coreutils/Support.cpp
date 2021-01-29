// Copyright 2015 Xcalar, Inc. All rights reserved.

#include <stdio.h>

#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"

void
cliSupportHelp(int argc, char *argv[])
{
    printf("Usage: %s --generate\n", argv[0]);
}

void
cliSupportMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    bool printUsage = true;

    if (argc != 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (strcmp(argv[1], "--generate") != 0) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    printUsage = false;

    workItem = xcalarApiMakeSupportGenerateWorkItem(false, 0);
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
    if (status == StatusOk) {
        XcalarApiSupportGenerateOutput *output =
            &workItem->output->outputResult.supportGenerateOutput;
        printf("Generated support ID %s\n", output->supportId);
        printf("Support bundle at %s\n", output->bundlePath);
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
        if (printUsage) {
            cliSupportHelp(argc, argv);
        }
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}
