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

#include "GetOpt.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "strings/String.h"
#include "queryparser/QueryParser.h"

QpGetRowNum::QpGetRowNum()
{
    this->isValidCmdParser = true;
}

QpGetRowNum::~QpGetRowNum()
{
    this->isValidCmdParser = false;
}

Status
QpGetRowNum::parseArgs(int argc, char *argv[], GetRowNumArgs *getRowNumArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"srctable", required_argument, 0, 's'},
        {"dsttable", required_argument, 0, 't'},
        {"fieldName", required_argument, 0, 'n'},
        {0, 0, 0, 0},
    };

    assert(getRowNumArgs != NULL);

    getRowNumArgs->srcTableName = NULL;
    getRowNumArgs->dstTableName = NULL;
    getRowNumArgs->newFieldName = NULL;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "e:s:t:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 's':
            getRowNumArgs->srcTableName = optData.optarg;
            break;

        case 't':
            getRowNumArgs->dstTableName = optData.optarg;
            break;

        case 'n':
            getRowNumArgs->newFieldName = optData.optarg;
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (getRowNumArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (getRowNumArgs->newFieldName == NULL) {
        fprintf(stderr, "--fieldName <newFieldName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (getRowNumArgs->dstTableName == NULL) {
        // Not an error
        fprintf(stderr,
                "--dstTable <tableName> not specified. "
                "Creating new table\n");
    }

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpGetRowNum::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    GetRowNumArgs getRowNumArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &getRowNumArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGetRowNumWorkItem(getRowNumArgs.srcTableName,
                                              getRowNumArgs.dstTableName,
                                              getRowNumArgs.newFieldName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpGetRowNum::parseJson(json_t *op,
                       json_error_t *err,
                       XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "", *newField = "";

    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             NewFieldKey,
                             &newField);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeGetRowNumWorkItem(source, dest, newField);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpGetRowNum::reverseParse(const XcalarApiInput *input,
                          json_error_t *err,
                          json_t **argsOut)
{
    const XcalarApiGetRowNumInput *getRowNumInput;
    Status status = StatusOk;
    json_t *args = NULL;
    getRowNumInput = &input->getRowNumInput;

    args = json_pack_ex(err,
                        0,
                        JsonFormatString,
                        SourceKey,
                        getRowNumInput->srcTable.tableName,
                        DestKey,
                        getRowNumInput->dstTable.tableName,
                        NewFieldKey,
                        getRowNumInput->newFieldName);
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    *argsOut = args;

    return status;
}
