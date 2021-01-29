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
cliMapHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --eval <\"evalStr\"> --srctable <tableName> "
        "--fieldName <newFieldName> [--dsttable <tableName>]\n",
        argv[0]);

    if (argc < 2) {
        printf("Use help %s xdfs for list of possible XDFs\n", argv[0]);
        return;
    }

    if (strcmp(argv[1], "xdfs") == 0) {
        if (argc == 2) {
            listXdfs(NULL, NULL);
            printf(
                "Use help %s xdfs <xdfName> for more information about the "
                "XDF\n",
                argv[0]);
            return;
        }

        if (argc > 2) {
            showXdf(argv[2]);
            return;
        }

    } else {
        printf("No help topic found for \"%s\"", argv[1]);
    }
}

void
cliMapMain(int argc,
           char *argv[],
           XcalarWorkItem *workItemIn,
           bool prettyPrint,
           bool interactive)
{
    Status status;
    XcalarApiNewTableOutput *mapOutput = NULL;
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

    mapOutput = &workItem->output->outputResult.mapOutput;

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", mapOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}
