// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <stdio.h>
#include <cstdlib>

#include "primitives/Primitives.h"
#include "libapis/LibApisSend.h"
#include "cli/CliCoreUtils.h"
#include "coreutils/CoreUtilsConstants.h"

using namespace cutil;

// The syntax checking is overly complicated for now.  The eventual goal is
// to allow the user to request quiesce and if it does not complete within
// a specified amount of time, to automatically convert to force.  There may
// also be a shutdown with automatic restart specified.

struct ActionStringMapping {
    const char *string;
    ShutdownAction shutdownAction;
};

static const ActionStringMapping actionStringMapping[] = {
    {"force", ShutdownForce},
    {"quiesce", ShutdownQuiesce},
};

void
cliShutdownHelp(int argc, char *argv[])
{
    printf("\nUsage: %s [force | quiesce]\n", argv[0]);
}

void
cliShutdownMain(int argc,
                char *argv[],
                XcalarWorkItem *workItemIn,
                bool prettyPrint,
                bool interactive)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    ShutdownAction shutdownAction = ShutdownQuiesce;
    unsigned ii;

    if (argc > 2) {
        fprintf(stderr, "Unknown option(s)\n");
        status = StatusCliParseError;
        cliShutdownHelp(argc, argv);
        goto CommonExit;
    }

    if (argc != 1) {
        for (ii = 0; ii < ArrayLen(actionStringMapping); ii++) {
            if (strcmp(argv[1], actionStringMapping[ii].string) == 0) {
                shutdownAction = actionStringMapping[ii].shutdownAction;
                break;
            }
        }

        if (ii == ArrayLen(actionStringMapping)) {
            fprintf(stderr, "Unknown action: %s\n", argv[1]);
            status = StatusCliParseError;
            cliShutdownHelp(argc, argv);
            goto CommonExit;
        }
    }

    if (shutdownAction == ShutdownQuiesce) {
        workItem = xcalarApiMakeShutdownWorkItem(false, false);
    } else {
        workItem = xcalarApiMakeShutdownWorkItem(false, true);
    }

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status == StatusOk && workItem->output->hdr.status != StatusOk.code()) {
        // The infrastructure was able to send/receive the work but the
        // processing of the work had han error
        status.fromStatusCode(workItem->output->hdr.status);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    if (status == StatusOk) {
        printf("Shut down initiated\n");
    } else {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
