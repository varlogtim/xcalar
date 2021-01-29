// Copyright 2014 Xcalar, Inc. All rights reserved.
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
cliLoadHelp(int argc, char *argv[])
{
    printf(
        "Usage:\n\t%s --url <url|memory> --format json "
        "[--size <sizeToLoad>] [--name <name>] "
        "[--apply <moduleName>:<funcName>] [--recursive] "
        "[--namePattern <namePattern>]\n\t%s --url memory "
        "--format random [--size <sizeToLoad>] [--name <name>]\n\t%s"
        " --url memory:seq --format random [--size <sizeToLoad>] "
        "[--name <name>]\n\t%s "
        "--url <url> --format csv [--size <sizeToLoad>] [--name <name>] "
        "[--recsToSkip <numRecsToSkip] "
        "[--recorddelim <recordDelimiter>] "
        "[--quotedelim <quoteDelimiter>] "
        "[--fielddelim <fieldFelimiter>] [--crlf] [--header]"
        "[--apply <moduleName>:<funcName>] [--recursive] "
        "[--namePattern <namePattern>]\n",
        argv[0],
        argv[0],
        argv[0],
        argv[0]);
}

void
cliLoadMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    Status status;
    XcalarApiBulkLoadOutput *loadOutput = NULL;
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

    loadOutput = &workItem->output->outputResult.loadOutput;
    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("Loaded successfully into dataset: %s\n",
               loadOutput->dataset.name);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
