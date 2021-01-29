// Copyright 2014 - 2015 Xcalar, Inc. All rights reserved.
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
cliGroupByHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --srctable <tableName> --eval <\"evalStr\"> "
        "--fieldName <newFieldName> "
        "[--dsttable <groupByTableName>] [--nosample]\n",
        argv[0]);

    if (argc < 2) {
        printf("Use help %s xdfs for list of possible xdfs\n", argv[0]);
        return;
    }

    assert(argc >= 2);

    if (strcmp(argv[1], "xdfs") == 0) {
        if (argc == 2) {
            listXdfs(NULL, "Aggregate*");
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
cliGroupByMain(int argc,
               char *argv[],
               XcalarWorkItem *workItemIn,
               bool prettyPrint,
               bool interactive)
{
    Status status;
    XcalarApiNewTableOutput *groupByOutput = NULL;
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

    if (workItem->output->hdr.status == StatusOk.code()) {
        assert(workItem->outputSize == XcalarApiSizeOfOutput(*groupByOutput));
        groupByOutput = &workItem->output->outputResult.groupByOutput;
        printf("\"%s\" successfully created\n", groupByOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
