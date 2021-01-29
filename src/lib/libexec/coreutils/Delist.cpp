// Copyright 2016 Xcalar, Inc. All rights reserved.
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
#include "libapis/WorkItem.h"
#include "cli/CliCoreUtils.h"

void
cliDelistHelp(int argc, char *argv[])
{
    printf("Usage: %s <datasetNamePattern>\n", argv[0]);
}

void
cliDelistMain(int argc,
              char *argv[],
              XcalarWorkItem *workItemIn,
              bool prettyPrint,
              bool interactive)
{
    StatusCode *statusOutput = NULL;
    Status status = StatusUnknown;
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

    statusOutput = &workItem->output->hdr.status;

    if (*statusOutput != StatusOk.code()) {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(*statusOutput));
        goto CommonExit;
    }

    unsigned ii;
    XcalarApiDatasetUnloadOutput *datasetUnloadOutput;
    datasetUnloadOutput = &workItem->output->outputResult.datasetUnloadOutput;

    if (prettyPrint) {
        if (datasetUnloadOutput->numDatasets == 0) {
            char *namePattern;
            namePattern =
                workItem->input->datasetUnloadInput.datasetNamePattern;
            printf("No datasets found matching \"%s\"\n", namePattern);
        } else {
            printf("%lu datasets were delisted:\n",
                   datasetUnloadOutput->numDatasets);
            printf("%-20s%-20s\n", "Dataset Name", "Status");
            printf("===============================================\n");
            for (ii = 0; ii < datasetUnloadOutput->numDatasets; ii++) {
                printf("%-20s%-20s\n",
                       datasetUnloadOutput->statuses[ii].dataset.name,
                       strGetFromStatusCode(
                           datasetUnloadOutput->statuses[ii].status));
            }
            printf("===============================================\n");
        }
    } else {
        printf("Number of datasets unloaded: %lu\n",
               datasetUnloadOutput->numDatasets);
        for (ii = 0; ii < datasetUnloadOutput->numDatasets; ii++) {
            printf("\"%s\"\t\"%s\"\n",
                   datasetUnloadOutput->statuses[ii].dataset.name,
                   strGetFromStatusCode(
                       datasetUnloadOutput->statuses[ii].status));
        }
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
