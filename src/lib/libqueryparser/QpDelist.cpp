// Copyright 2014 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <cstdlib>
#include <stdio.h>
#include <assert.h>
#include <getopt.h>
#include <string>

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "SourceTypeEnum.h"
#include "queryparser/QueryParser.h"

QpDelist::QpDelist()
{
    this->isValidCmdParser = true;
}

QpDelist::~QpDelist()
{
    this->isValidCmdParser = false;
}

Status
QpDelist::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;

    if (argc != 2) {
        return StatusCliParseError;
    }

    workItem = xcalarApiMakeDatasetUnloadWorkItem(argv[1]);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }
    *workItemOut = workItem;
    return status;
}
