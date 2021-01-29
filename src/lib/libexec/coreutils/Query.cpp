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
#include "querymanager/QueryManager.h"

void
cliQueryHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --xcquery <\"xcalarQuery\"> [--queryname <queryName>] "
        "[--sessionname <sessionName>]\n",
        argv[0]);
}

static void
printQueryState(XcalarApiQueryStateOutput *queryStateOutput,
                const char *queryName)
{
    printf("Query Name: %s\n", queryName);
    printf("Query state: %s\n",
           strGetFromQueryState(queryStateOutput->queryState));

    printf("Number of queued tasks: %lu\n",
           queryStateOutput->numQueuedWorkItem);
    printf("Number of completed tasks: %lu\n",
           queryStateOutput->numCompletedWorkItem);

    if (queryStateOutput->queryState == qrError) {
        printf("Query error status: %s\n",
               strGetFromStatusCode(queryStateOutput->queryStatus));

        printf("Number of failed tasks: %lu\n",
               queryStateOutput->numFailedWorkItem);
    }
}

void
cliQueryInspectHelp(int argc, char *argv[])
{
    printf("Usage: %s [queryId] ...\n", argv[0]);
}

// XXX: This cmd should be integerate with Inspect cmd
void
cliQueryInspectMain(int argc,
                    char *argv[],
                    XcalarWorkItem *workItemIn,
                    bool prettyPrint,
                    bool interactive)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiQueryStateOutput *queryStateOutput;

    if (argc < 2) {
        status = StatusCliParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeQueryStateWorkItem(argv[1]);
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
    assert(status == StatusOk);

    if (workItem->output->hdr.status == StatusOk.code()) {
        queryStateOutput = &workItem->output->outputResult.queryStateOutput;

        printQueryState(queryStateOutput, argv[1]);
    } else if (workItem->output->hdr.status == StatusNsNotFound.code()) {
        printf("Error: Query Name does not exist\n");
    } else {
        status.fromStatusCode(workItem->output->hdr.status);
    }
CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
}
