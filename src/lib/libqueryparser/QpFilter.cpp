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
#include "operators/XcalarEval.h"

QpFilter::QpFilter()
{
    this->isValidCmdParser = true;
}

QpFilter::~QpFilter()
{
    this->isValidCmdParser = false;
}

Status
QpFilter::parseArgs(int argc, char *argv[], FilterArgs *filterArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"eval", required_argument, 0, 'e'},
        {"srctable", required_argument, 0, 's'},
        {"dsttable", required_argument, 0, 't'},
        {0, 0, 0, 0},
    };

    assert(filterArgs != NULL);

    filterArgs->evalStr = NULL;
    filterArgs->srcTableName = NULL;
    filterArgs->dstTableName = NULL;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "e:s:t:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'e':
            filterArgs->evalStr = optData.optarg;
            break;

        case 's':
            filterArgs->srcTableName = optData.optarg;
            break;

        case 't':
            filterArgs->dstTableName = optData.optarg;
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (filterArgs->evalStr == NULL) {
        fprintf(stderr, "--eval <evalStr> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (filterArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <srcTableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (filterArgs->dstTableName == NULL) {
        // Not an error
        fprintf(stderr,
                "--dstTable <srcTableName> not specified. "
                "Creating new table\n");
    }

    if (strlen(filterArgs->evalStr) >= XcalarApiMaxEvalStringLen) {
        status = StatusEvalStringTooLong;
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpFilter::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    FilterArgs filterArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &filterArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeFilterWorkItem(filterArgs.evalStr,
                                           filterArgs.srcTableName,
                                           filterArgs.dstTableName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpFilter::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "", *eval = "";
    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             EvalKey,
                             EvalStringKey,
                             &eval);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    workItem = xcalarApiMakeFilterWorkItem(eval, source, dest);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpFilter::reverseParse(const XcalarApiInput *input,
                       json_error_t *err,
                       json_t **argsOut)
{
    const XcalarApiFilterInput *filterInput;
    Status status = StatusOk;
    json_t *args = NULL;

    filterInput = &input->filterInput;

    args = json_pack_ex(err,
                        0,
                        JsonFormatString,
                        SourceKey,
                        filterInput->srcTable.tableName,
                        DestKey,
                        filterInput->dstTable.tableName,
                        EvalKey,
                        EvalStringKey,
                        filterInput->filterStr);
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    *argsOut = args;

    return status;
}
