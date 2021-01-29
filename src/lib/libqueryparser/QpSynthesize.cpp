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
#include "sys/XLog.h"

QpSynthesize::QpSynthesize()
{
    this->isValidCmdParser = true;
}

QpSynthesize::~QpSynthesize()
{
    this->isValidCmdParser = false;
}

Status
QpSynthesize::parseJson(json_t *op,
                        json_error_t *err,
                        XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "";
    json_t *columnsJson = NULL;
    XcalarWorkItem *workItem = NULL;
    int sameSession = true;
    XcalarApiRenameMap *renameMap = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             SameSessionKey,
                             &sameSession,
                             ColumnsKey,
                             &columnsJson);
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

    renameMap = (XcalarApiRenameMap *) memAlloc(numCols * sizeof(*renameMap));
    BailIfNull(renameMap);

    status = parseColumnsArray(columnsJson, err, renameMap);
    BailIfFailed(status);

    workItem = xcalarApiMakeSynthesizeWorkItem(source,
                                               dest,
                                               sameSession,
                                               numCols,
                                               renameMap);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    memFree(renameMap);
    *workItemOut = workItem;

    return status;
}

Status
QpSynthesize::reverseParse(const XcalarApiInput *input,
                           json_error_t *err,
                           json_t **argsOut)
{
    const XcalarApiSynthesizeInput *synthesizeInput;
    int ret = 0;
    Status status = StatusOk;
    json_t *columns = NULL, *args = NULL, *val = NULL, *oldName = NULL,
           *newName = NULL, *type = NULL;
    synthesizeInput = &input->synthesizeInput;

    columns = json_array();
    BailIfNull(columns);

    for (unsigned ii = 0; ii < synthesizeInput->columnsCount; ii++) {
        oldName = json_string(synthesizeInput->columns[ii].oldName);
        BailIfNull(oldName);

        newName = json_string(synthesizeInput->columns[ii].newName);
        BailIfNull(newName);

        type = json_string(
            strGetFromDfFieldType(synthesizeInput->columns[ii].type));
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
                        synthesizeInput->source.name,
                        DestKey,
                        synthesizeInput->dstTable.tableName,
                        SameSessionKey,
                        synthesizeInput->sameSession,
                        ColumnsKey,
                        columns);
    columns = NULL;
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

    *argsOut = args;

    return status;
}
