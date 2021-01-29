// Copyright 2014 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>
#include <cstdlib>
#include <getopt.h>

#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "coreutils/CliCoreUtilsInt.h"

void
cliProjectHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --srctable <tableName> "
        "[--dsttable <tableName>] \n"
        "               <column1> [<column2> ... <columnN>]\n",
        argv[0]);
}

void
cliProjectMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status;
    XcalarApiNewTableOutput *projectOutput = NULL;
    XcalarWorkItem *workItem = workItemIn;

    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    projectOutput = &workItem->output->outputResult.projectOutput;

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", projectOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}
