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
#include "coreutils/CliCoreUtilsInt.h"

void
cliFilterHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --eval <\"evalStr\"> --srctable <tableName> "
        "[--dsttable <tableName>]\n",
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
cliFilterMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    Status status = StatusUnknown;
    XcalarApiNewTableOutput *filterOutput = NULL;

    assert(workItemIn != NULL);

    status = xcalarApiQueueWork(workItemIn,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    filterOutput = &workItemIn->output->outputResult.filterOutput;

    if (workItemIn->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", filterOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItemIn->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
