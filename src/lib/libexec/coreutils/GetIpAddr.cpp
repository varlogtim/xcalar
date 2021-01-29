// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>

#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "sys/XLog.h"

void
cliGetIpAddrHelp(int argc, char *argv[])
{
    printf("Usage: %s <nodeId>\n", argv[0]);
}

void
cliGetIpAddrMain(int argc,
                 char *argv[],
                 XcalarWorkItem *workItemIn,
                 bool prettyPrint,
                 bool interactive)
{
    Status status;
    XcalarWorkItem *workItem = NULL;
    bool printUsage = true;
    NodeId nodeId;
    XcalarApiGetIpAddrOutput *gipaout;

    if (argc != 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }
    nodeId = atoi(argv[1]);

    printUsage = false;

    workItem = xcalarApiMakeGetIpAddrWorkItem(nodeId);
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
    gipaout = &workItem->output->outputResult.getIpAddrOutput;
    printf("%s\n", gipaout->ipAddr);

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
        if (printUsage) {
            cliGetIpAddrHelp(argc, argv);
        }
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}
