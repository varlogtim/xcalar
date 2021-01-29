// Copyright 2014 - 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <cstdlib>
#include <stdio.h>
#include <getopt.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"

void
cliRenameHelp(int argc, char *argv[])
{
    printf(
        "Usage:\n\t%s <column> <dataset> <existingColName> <newColName>\n"
        "Or Usage:\n\t%s node <oldName> <newName> \n",
        argv[0],
        argv[0]);
}

void
cliRenameMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    Status status;
    XcalarWorkItem *workItem = workItemIn;

    // workItem should be parsed by qpParseEditColWorkItem
    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("Rename successfully\n");
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
