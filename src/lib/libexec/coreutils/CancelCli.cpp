// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
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
#include "coreutils/CoreUtilsConstants.h"

using namespace cutil;

void
cliCancelHelp(int argc, char *argv[])
{
    printf("Usage: %s [\"table name\"] \n", argv[0]);
}

void
cliCancelMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;

    if (argc < 2) {
        status = StatusCliParseError;
        cliCancelHelp(argc, argv);
        goto CommonExit;
    }

    if (strlen(argv[1]) > XcalarApiMaxTableNameLen) {
        status = StatusInvalidTableName;
        goto CommonExit;
    }

    workItem = xcalarApiMakeCancelOpWorkItem(argv[1]);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    assert(workItem != NULL);
    workItem->legacyClient = true;

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
        printf("cancel cmd has been sent");
    } else {
        printf("error:status = %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
