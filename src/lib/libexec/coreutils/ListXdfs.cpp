// Copyright 2015 Xcalar, Inc. All rights reserved.
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

static void
listAndShowXdfs(const char *fnNamePattern,
                const char *categoryPattern,
                bool showDetails)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiListXdfsOutput *listXdfsOutput = NULL;
    unsigned ii;

    assert(fnNamePattern != NULL);
    assert(categoryPattern != NULL);

    workItem = xcalarApiMakeListXdfsWorkItem(fnNamePattern, categoryPattern);
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

    listXdfsOutput = &workItem->output->outputResult.listXdfsOutput;
    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    for (ii = 0; ii < listXdfsOutput->numXdfs; ii++) {
        printf("%s - %s\n",
               listXdfsOutput->fnDescs[ii].fnName,
               listXdfsOutput->fnDescs[ii].fnDesc);
        if (showDetails) {
            int jj;
            printf("Arguments:\n");
            for (jj = 0; jj < listXdfsOutput->fnDescs[ii].numArgs; jj++) {
                printf("\t%d. %s\n",
                       jj + 1,
                       listXdfsOutput->fnDescs[ii].argDescs[jj].argDesc);
            }
            printf("\n");
        }
    }

    if (showDetails && listXdfsOutput->numXdfs == 0) {
        printf("No such XDF \"%s\" found\n", fnNamePattern);
    }

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        listXdfsOutput = NULL;
        workItem = NULL;
    }

    if (status != StatusOk) {
        fprintf(stderr, "Error: %s\n", strGetFromStatus(status));
    }
}

void
listXdfs(const char *fnNamePattern, const char *categoryPattern)
{
    listAndShowXdfs((fnNamePattern == NULL) ? "*" : fnNamePattern,
                    (categoryPattern == NULL) ? "*" : categoryPattern,
                    DontShowDetails);
}

void
showXdf(const char *fnName)
{
    assert(fnName != NULL);
    listAndShowXdfs(fnName, "*", ShowDetails);
}
