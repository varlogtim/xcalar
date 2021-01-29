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
cliJoinHelp(int argc, char *argv[])
{
    printf(
        "Usage: %s --leftTable <leftTable> --rightTable <rightTable> "
        "[--joinTable <joinTable>] [--joinType <joinType>] "
        "[--leftRenameMap <old:new:type(0 for fp, 1 for imm) "
        "separated by ;>]\n"
        "[--rightRenameMap <old:new:type(0 for fp, 1 for imm) "
        "separated by ;>]\n",
        argv[0]);
    printf(
        "Possible values for <joinType> are \"innerJoin\", \"leftJoin\", "
        "\"rightJoin\", \"fullOuterJoin\".\n"
        "The default is \"innerJoin\"\n");
}

void
cliJoinMain(int argc,
            char *argv[],
            XcalarWorkItem *workItemIn,
            bool prettyPrint,
            bool interactive)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = workItemIn;
    XcalarApiNewTableOutput *joinOutput = NULL;

    assert(workItem != NULL);

    status = xcalarApiQueueWork(workItem,
                                cliDestIp,
                                cliDestPort,
                                cliUsername,
                                cliUserIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    joinOutput = &workItem->output->outputResult.joinOutput;

    if (workItem->output->hdr.status == StatusOk.code()) {
        printf("\"%s\" successfully created\n", joinOutput->tableName);
    } else {
        printf("Error: server returned: %s\n",
               strGetFromStatusCode(workItem->output->hdr.status));
    }

CommonExit:
    if (status != StatusOk) {
        printf("Error: %s\n", strGetFromStatus(status));
    }
}
