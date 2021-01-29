// Copyright 2016 Xcalar, Inc. All rights reserved.
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
#include "GetOpt.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "StrlFunc.h"
#include "util/MemTrack.h"
#include "queryparser/QueryParser.h"
#include "sys/XLog.h"
#include "strings/String.h"

static constexpr const char *moduleName = "QpProject";

QpProject::QpProject()
{
    this->isValidCmdParser = true;
}

QpProject::~QpProject()
{
    this->isValidCmdParser = false;
}

Status
QpProject::parseArgs(int argc, char *argv[], ProjectArgs *projectArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    size_t ret;
    OptionThr longOptions[] = {
        {"srctable", required_argument, 0, 's'},
        {"dsttable", required_argument, 0, 't'},
        {"columnNames", required_argument, 0, 'l'},
        {0, 0, 0, 0},
    };
    int numColumns;

    assert(projectArgs != NULL);

    projectArgs->srcTableName = NULL;
    projectArgs->dstTableName = NULL;
    projectArgs->numColumns = 0;
    projectArgs->columns = NULL;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "s:t:",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 's':
            projectArgs->srcTableName = optData.optarg;
            break;

        case 't':
            projectArgs->dstTableName = optData.optarg;
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (projectArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (projectArgs->dstTableName == NULL) {
        fprintf(stderr, "--dsttable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    // Column name seperated by semicol
    numColumns = argc - optData.optind;
    if (numColumns <= 0) {
        fprintf(stderr, "At least one column required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    projectArgs->numColumns = numColumns;
    projectArgs->columns = (char(*)[DfMaxFieldNameLen + 1])
        memAllocExt(sizeof(*projectArgs->columns) * projectArgs->numColumns,
                    moduleName);
    if (projectArgs->columns == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    int ii;
    for (ii = optData.optind; ii < argc; ii++) {
        ret = strlcpy(projectArgs->columns[ii - optData.optind],
                      argv[ii],
                      sizeof(projectArgs->columns[ii - optData.optind]));
        if (ret >= sizeof(projectArgs->columns[ii - optData.optind])) {
            status = StatusParameterTooLong;
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (projectArgs->columns != NULL) {
            memFree(projectArgs->columns);
            projectArgs->columns = NULL;
        }
    }

    return status;
}

Status
QpProject::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    ProjectArgs projectArgs;
    XcalarWorkItem *workItem = NULL;

    projectArgs.columns = NULL;
    status = parseArgs(argc, argv, &projectArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeProjectWorkItem(projectArgs.numColumns,
                                            projectArgs.columns,
                                            projectArgs.srcTableName,
                                            projectArgs.dstTableName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:

    if (projectArgs.columns != NULL) {
        memFree(projectArgs.columns);
    }

    *workItemOut = workItem;

    return status;
}

Status
QpProject::parseJson(json_t *op,
                     json_error_t *err,
                     XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "";
    json_t *columnsJson = NULL;
    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             ColumnsKey,
                             &columnsJson);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    json_t *col;
    unsigned ii;
    unsigned numCols;

    numCols = json_array_size(columnsJson);

    {
        char columns[numCols][XcalarApiMaxFieldNameLen + 1];

        json_array_foreach (columnsJson, ii, col) {
            const char *colStr = json_string_value(col);
            BailIfNullWith(colStr, StatusJsonQueryParseError);

            status = strStrlcpy(columns[ii], colStr, sizeof(columns[ii]));
            BailIfFailed(status);
        }

        workItem = xcalarApiMakeProjectWorkItem(numCols, columns, source, dest);
        if (workItem == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }

CommonExit:

    *workItemOut = workItem;

    return status;
}

Status
QpProject::reverseParse(const XcalarApiInput *input,
                        json_error_t *err,
                        json_t **argsOut)
{
    const XcalarApiProjectInput *projectInput;
    Status status = StatusOk;
    json_t *columns = NULL, *args = NULL;
    json_t *column = NULL;
    projectInput = &input->projectInput;

    columns = json_array();
    BailIfNull(columns);

    for (unsigned ii = 0; ii < projectInput->numColumns; ii++) {
        int ret;
        assert(column == NULL);
        column = json_string(projectInput->columnNames[ii]);
        BailIfNull(column);

        ret = json_array_append_new(columns, column);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        column = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonFormatString,
                        SourceKey,
                        projectInput->srcTable.tableName,
                        DestKey,
                        projectInput->dstTable.tableName,
                        ColumnsKey,
                        columns);
    columns = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (column != NULL) {
        json_decref(column);
        column = NULL;
    }
    if (columns != NULL) {
        json_decref(columns);
        columns = NULL;
    }

    *argsOut = args;

    return status;
}
