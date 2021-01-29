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
#include "strings/String.h"
#include "sys/XLog.h"

QpSelect::QpSelect()
{
    this->isValidCmdParser = true;
}

QpSelect::~QpSelect()
{
    this->isValidCmdParser = false;
}

Status
QpSelect::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "", *filterString = "",
               *joinTableName = "";
    char *evalJsonStr = NULL;
    int64_t minBatchId = -1, maxBatchId = -1;
    XcalarWorkItem *workItem = NULL;
    json_t *columnsJson = NULL, *evalJson = NULL;
    XcalarApiRenameMap *renameMap = NULL;
    int createIndex = 0;
    int limitRows = 0;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             MinBatchIdKey,
                             &minBatchId,
                             MaxBatchIdKey,
                             &maxBatchId,
                             EvalStringKey,
                             &filterString,
                             ColumnsKey,
                             &columnsJson,
                             EvalKey,
                             &evalJson,
                             CreateIndexKey,
                             &createIndex,
                             LimitRowsKey,
                             &limitRows);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    unsigned numCols;

    numCols = json_array_size(columnsJson);

    if (numCols > TupleMaxNumValuesPerRecord) {
        status = StatusFieldLimitExceeded;
        goto CommonExit;
    }

    if (evalJson) {
        evalJsonStr = json_dumps(evalJson, JSON_COMPACT);
        BailIfNull(evalJsonStr);

        json_t *filterStringJson = json_object_get(evalJson, FilterKey);
        if (filterStringJson) {
            filterString = json_string_value(filterStringJson);
            if (filterString == NULL) {
                filterString = "";
            }
        }

        json_t *joinJson = json_object_get(evalJson, JoinKey);
        if (joinJson) {
            json_t *joinTableJson = json_object_get(joinJson, SourceKey);

            joinTableName = json_string_value(joinTableJson);
            if (joinTableName == NULL) {
                joinTableName = "";
            }
        }
    }

    renameMap = (XcalarApiRenameMap *) memAlloc(numCols * sizeof(*renameMap));
    BailIfNull(renameMap);

    status = parseColumnsArray(columnsJson, err, renameMap);
    BailIfFailed(status);

    workItem = xcalarApiMakeSelectWorkItem(source,
                                           dest,
                                           minBatchId,
                                           maxBatchId,
                                           filterString,
                                           numCols,
                                           renameMap,
                                           limitRows,
                                           evalJsonStr,
                                           joinTableName,
                                           createIndex);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (evalJsonStr) {
        memFree(evalJsonStr);
    }

    memFree(renameMap);
    *workItemOut = workItem;

    return status;
}

Status
QpSelect::reverseParse(const XcalarApiInput *input,
                       json_error_t *err,
                       json_t **argsOut)
{
    const XcalarApiSelectInput *selectInput = &input->selectInput;
    json_t *args = NULL;
    Status status = StatusOk;
    int ret = 0;
    json_t *columns = NULL, *val = NULL, *oldName = NULL, *newName = NULL,
           *type = NULL, *evalJson = NULL;

    selectInput = &input->selectInput;
    if (selectInput->evalInputCount) {
        evalJson = json_loads((char *) selectInput->evalInput, 0, err);
    }

    if (evalJson == NULL) {
        evalJson = json_object();
        BailIfNull(evalJson);
    }

    columns = json_array();
    BailIfNull(columns);

    for (unsigned ii = 0; ii < selectInput->numColumns; ii++) {
        oldName = json_string(selectInput->columns[ii].oldName);
        BailIfNull(oldName);

        newName = json_string(selectInput->columns[ii].newName);
        BailIfNull(newName);

        type =
            json_string(strGetFromDfFieldType(selectInput->columns[ii].type));
        BailIfNull(type);

        val = json_object();
        BailIfNull(val);

        ret = json_object_set_new(val, SourceColumnKey, oldName);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        oldName = NULL;

        ret = json_object_set_new(val, DestColumnKey, newName);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        newName = NULL;

        ret = json_object_set_new(val, ColumnTypeKey, type);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        type = NULL;

        ret = json_array_append_new(columns, val);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        val = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        selectInput->srcTable.tableName,
                        DestKey,
                        selectInput->dstTable.tableName,
                        MinBatchIdKey,
                        selectInput->minBatchId,
                        MaxBatchIdKey,
                        selectInput->maxBatchId,
                        ColumnsKey,
                        columns,
                        EvalKey,
                        evalJson,
                        CreateIndexKey,
                        selectInput->createIndex);
    columns = NULL;
    evalJson = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (columns != NULL) {
        json_decref(columns);
        columns = NULL;
    }

    if (val != NULL) {
        json_decref(val);
        val = NULL;
    }

    if (oldName != NULL) {
        json_decref(oldName);
        oldName = NULL;
    }

    if (newName != NULL) {
        json_decref(newName);
        newName = NULL;
    }

    if (type != NULL) {
        json_decref(type);
        type = NULL;
    }

    if (evalJson) {
        json_decref(evalJson);
        evalJson = NULL;
    }

    *argsOut = args;

    return status;
}
