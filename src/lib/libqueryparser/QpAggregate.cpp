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
#include "GetOpt.h"
#include "strings/String.h"
#include "queryparser/QueryParser.h"
#include "operators/XcalarEval.h"

QpAggregate::QpAggregate()
{
    this->isValidCmdParser = true;
}

QpAggregate::~QpAggregate()
{
    this->isValidCmdParser = false;
}

Status
QpAggregate::parseArgs(int argc, char *argv[], AggregateArgs *aggregateArgs)
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

    aggregateArgs->srcTableName = NULL;
    aggregateArgs->dstTableName = NULL;
    aggregateArgs->evalStr = NULL;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "e:s:t:n:i",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'e':
            aggregateArgs->evalStr = optData.optarg;
            break;

        case 's':
            aggregateArgs->srcTableName = optData.optarg;
            break;

        case 't':
            aggregateArgs->dstTableName = optData.optarg;
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (aggregateArgs->evalStr == NULL) {
        fprintf(stderr, "--eval <evalStr> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (aggregateArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (aggregateArgs->dstTableName == NULL) {
        // Not an error
        fprintf(stderr,
                "--dstTable <tableName> not specified. "
                "Creating new table\n");
    }

    if (strlen(aggregateArgs->evalStr) >= XcalarApiMaxEvalStringLen) {
        status = StatusEvalStringTooLong;
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpAggregate::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    AggregateArgs aggregateArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &aggregateArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeAggregateWorkItem(aggregateArgs.srcTableName,
                                              aggregateArgs.dstTableName,
                                              aggregateArgs.evalStr);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpAggregate::parseJson(json_t *op,
                       json_error_t *err,
                       XcalarWorkItem **workItemOut)
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

    workItem = xcalarApiMakeAggregateWorkItem(source, dest, eval);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpAggregate::reverseParse(const XcalarApiInput *input,
                          json_error_t *err,
                          json_t **argsOut)
{
    const XcalarApiAggregateInput *aggInput;
    Status status = StatusOk;
    json_t *args = NULL;

    aggInput = &input->aggregateInput;

    args = json_pack_ex(err,
                        0,
                        JsonFormatString,
                        SourceKey,
                        aggInput->srcTable.tableName,
                        DestKey,
                        aggInput->dstTable.tableName,
                        EvalKey,
                        EvalStringKey,
                        aggInput->evalStr);
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    *argsOut = args;

    return status;
}
