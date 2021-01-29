// Copyright 2014 Xcalar, Inc. All rights reserved.
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

void
cliDropHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s <nodeTypes> <name>\n"
        "Possible <nodeTypes> are \"table\" \"dataset\" \"constant\" "
        "\"export\"\n",
        argv[0]);
}

void
cliDropMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    StatusCode *statusOutput = NULL;
    Status status = StatusUnknown;

    status = xcalarApiQueueWork(workItemIn,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    statusOutput = &workItemIn->output->hdr.status;

    if (*statusOutput != StatusOk.code()) {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(*statusOutput));
        goto CommonExit;
    }

    SourceType nodeType;
    unsigned ii;
    XcalarApiDeleteDagNodesOutput *deleteDagNodesOutput;

    nodeType = workItemIn->input->deleteDagNodesInput.srcType;

    deleteDagNodesOutput =
        &workItemIn->output->outputResult.deleteDagNodesOutput;

    if (prettyPrint) {
        if (deleteDagNodesOutput->numNodes == 0) {
            char *namePattern;
            namePattern = workItemIn->input->deleteDagNodesInput.namePattern;
            printf("No %s node found matching \"%s\"\n",
                   strGetFromSourceType(nodeType),
                   namePattern);
        } else {
            printf("%lu %s node were deleted:\n",
                   deleteDagNodesOutput->numNodes,
                   strGetFromSourceType(nodeType));
            printf("%-20s%-20s\n", "Node Name", "Status");
            printf("===============================================\n");
            for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
                printf("%-20s%-20s\n",
                       deleteDagNodesOutput->statuses[ii].nodeInfo.name,
                       strGetFromStatusCode(
                           deleteDagNodesOutput->statuses[ii].status));
            }
            printf("===============================================\n");
        }
    } else {
        printf("Number of %s nodes deleted: %lu\n",
               strGetFromSourceType(nodeType),
               deleteDagNodesOutput->numNodes);
        for (ii = 0; ii < deleteDagNodesOutput->numNodes; ii++) {
            printf("\"%s\"\t\"%s\"\n",
                   deleteDagNodesOutput->statuses[ii].nodeInfo.name,
                   strGetFromStatusCode(
                       deleteDagNodesOutput->statuses[ii].status));
        }
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
