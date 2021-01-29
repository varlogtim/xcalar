// Copyright 2014 - 2015 Xcalar, Inc. All rights reserved.
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
cliGetRowNumHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --srctable <tableName> "
        "--fieldName <newFieldName> [--dsttable <tableName>]\n",
        argv[0]);
}

void
cliGetRowNumMain(int argc,
                 char *argv[],
                 XcalarWorkItem *workItemIn,
                 bool prettyPrint,
                 bool interactive)
{
    Status status;
    XcalarApiNewTableOutput *getRowNumOutput = NULL;
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

    getRowNumOutput = &workItem->output->outputResult.getRowNumOutput;

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", getRowNumOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}
