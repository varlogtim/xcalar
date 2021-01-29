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

#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "SourceTypeEnum.h"
#include "queryparser/QueryParser.h"

QpDrop::QpDrop()
{
    this->isValidCmdParser = true;
}

QpDrop::~QpDrop()
{
    this->isValidCmdParser = false;
}

Status
QpDrop::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    SourceType srcType;
    XcalarWorkItem *workItem = NULL;

    if (argc != 3) {
        return StatusCliParseError;
    }

    if ((strcasecmp(argv[1], "tables") == 0) ||
        (strcasecmp(argv[1], "table") == 0)) {
        srcType = SrcTable;
    } else if ((strcasecmp(argv[1], "dataset") == 0) ||
               (strcasecmp(argv[1], "datasets") == 0)) {
        srcType = SrcDataset;
    } else if ((strcasecmp(argv[1], "constant") == 0) ||
               (strcasecmp(argv[1], "constants") == 0)) {
        srcType = SrcConstant;
    } else if (strcasecmp(argv[1], "export") == 0) {
        srcType = SrcExport;
    } else {
        printf("Unknown <dropWhat> \"%s\"\n", argv[1]);
        return StatusCliParseError;
    }

    workItem = xcalarApiMakeDeleteDagNodesWorkItem(argv[2], srcType);
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

Status
QpDrop::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *name = "", *sourceType = "";
    int deleteCompletely = 0;  // jansson treats as int

    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             NameKey,
                             &name,
                             SourceTypeKey,
                             &sourceType,
                             DeleteCompletelyKey,
                             &deleteCompletely);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeDeleteDagNodesWorkItem(name,
                                                   strToSourceType(sourceType),
                                                   NULL,
                                                   (bool) deleteCompletely);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpDrop::reverseParse(const XcalarApiInput *input,
                     json_error_t *err,
                     json_t **argsOut)
{
    const XcalarApiDagNodeNamePatternInput *dropInput =
        &input->deleteDagNodesInput;
    json_t *args = NULL;
    Status status = StatusOk;

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        NameKey,
                        dropInput->namePattern,
                        SourceTypeKey,
                        strGetFromSourceType(dropInput->srcType),
                        DeleteCompletelyKey,
                        (int) dropInput->deleteCompletely);
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    *argsOut = args;

    return status;
}
