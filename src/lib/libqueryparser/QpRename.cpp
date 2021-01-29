// Copyright 2015 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <stdio.h>
#include <assert.h>

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "queryparser/QueryParser.h"

QpRename::QpRename()
{
    this->isValidCmdParser = true;
}

QpRename::~QpRename()
{
    this->isValidCmdParser = false;
}

Status
QpRename::parseArgs(int argc, char *argv[], RenameArgs *renameArgs)
{
    if (argc < 4) {
        return StatusCliParseError;
    }

    renameArgs->oldName = argv[2];
    if (strlen(renameArgs->oldName) == 0) {
        fprintf(stderr, "%s is not a valid tableName\n", argv[2]);
        return StatusCliParseError;
    }

    renameArgs->newName = argv[3];
    if (strlen(renameArgs->newName) == 0) {
        fprintf(stderr, "%s is not a valid tableName\n", argv[3]);
        return StatusCliParseError;
    }

    return StatusOk;
}

Status
QpRename::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    XcalarWorkItem *workItem = NULL;

    if (strcmp(argv[1], "node") == 0) {
        RenameArgs renameNodeArgs;
        status = parseArgs(argc, argv, &renameNodeArgs);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(status == StatusOk);

        workItem = xcalarApiMakeRenameNodeWorkItem(renameNodeArgs.oldName,
                                                   renameNodeArgs.newName);
    } else {
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}
