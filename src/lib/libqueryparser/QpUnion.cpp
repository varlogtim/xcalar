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

#include "StrlFunc.h"
#include "GetOpt.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "queryparser/QueryParser.h"
#include "strings/String.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "QpUnion";

QpUnion::QpUnion()
{
    this->isValidCmdParser = true;
}

QpUnion::~QpUnion()
{
    this->isValidCmdParser = false;
}

Status
QpUnion::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *dest = "\0";
    json_t *source = NULL, *renameMapJson = NULL, *keys = NULL;
    int dedup = false;
    UnionOperator unionType;
    const char *unionTypeStr = NULL;
    unsigned numSrcTables;
    XcalarWorkItem *workItem = NULL;
    const char **srcNames = NULL;
    unsigned *renameMapSizes = NULL;
    XcalarApiRenameMap **renameMap = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             KeyFieldKey,
                             &keys,
                             DedupKey,
                             &dedup,
                             UnionTypeKey,
                             &unionTypeStr,
                             ColumnsKey,
                             &renameMapJson);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    if (source != NULL) {
        numSrcTables = json_array_size(source);
    } else {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    if (numSrcTables == 0 || renameMapJson == NULL ||
        json_array_size(renameMapJson) != numSrcTables) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    srcNames = (const char **) memAlloc(sizeof(*srcNames) * numSrcTables);
    BailIfNull(srcNames);

    renameMapSizes =
        (unsigned *) memAlloc(sizeof(*renameMapSizes) * numSrcTables);
    BailIfNull(renameMapSizes);

    renameMap =
        (XcalarApiRenameMap **) memAlloc(sizeof(*renameMap) * numSrcTables);
    BailIfNull(renameMap);
    memZero(renameMap, sizeof(*renameMap) * numSrcTables);

    {
        unsigned ii;
        json_t *val1, *val2;

        json_array_foreach (source, ii, val1) {
            srcNames[ii] = json_string_value(val1);
            BailIfNullWith(srcNames[ii], StatusJsonQueryParseError);
        }

        json_array_foreach (renameMapJson, ii, val1) {
            renameMapSizes[ii] = json_array_size(val1);
            if (renameMapSizes[ii] == 0) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }

            if (renameMapSizes[ii] > TupleMaxNumValuesPerRecord) {
                status = Status2Big;
                goto CommonExit;
            }

            renameMap[ii] = (XcalarApiRenameMap *) memAlloc(
                sizeof(*renameMap[ii]) * renameMapSizes[ii]);
            BailIfNull(renameMap[ii]);

            status = parseColumnsArray(val1, err, renameMap[ii]);
            BailIfFailed(status);
        }

        if (keys != NULL) {
            if (json_array_size(renameMapJson) != numSrcTables) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }

            unsigned tableIdx, keyIdx;
            json_array_foreach (keys, tableIdx, val1) {
                json_array_foreach (val1, keyIdx, val2) {
                    const char *keyName = json_string_value(val2);
                    BailIfNullWith(keyName, StatusJsonQueryParseError);

                    for (unsigned colIdx = 0; colIdx < renameMapSizes[tableIdx];
                         colIdx++) {
                        if (strcmp(keyName,
                                   renameMap[tableIdx][colIdx].oldName) == 0) {
                            renameMap[tableIdx][colIdx].isKey = true;
                            break;
                        }
                    }
                }
            }
        }

        if (unionTypeStr == NULL) {
            // Old queries don't specify a unionType
            unionType = UnionStandard;
        } else {
            unionType = strToUnionOperator(unionTypeStr);
            if (!isValidUnionOperator(unionType)) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }
        }

        workItem = xcalarApiMakeUnionWorkItem(numSrcTables,
                                              srcNames,
                                              dest,
                                              renameMapSizes,
                                              renameMap,
                                              dedup,
                                              unionType);
        if (workItem == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }

CommonExit:
    if (srcNames) {
        memFree(srcNames);
        srcNames = NULL;
    }

    if (renameMapSizes) {
        memFree(renameMapSizes);
        renameMapSizes = NULL;
    }

    if (renameMap) {
        for (unsigned ii = 0; ii < numSrcTables; ii++) {
            if (renameMap[ii]) {
                memFree(renameMap[ii]);
                renameMap[ii] = NULL;
            }
        }

        memFree(renameMap);
        renameMap = NULL;
    }

    *workItemOut = workItem;

    return status;
}

Status
QpUnion::reverseParse(const XcalarApiInput *input,
                      json_error_t *err,
                      json_t **argsOut)
{
    const XcalarApiUnionInput *unionInput;
    int ret;
    Status status = StatusOk;
    json_t *args = NULL, *source = NULL, *renameMapJson = NULL,
           *keyArrays = NULL;
    json_t *val = NULL, *renameMap = NULL, *keyArray = NULL;

    unionInput = &input->unionInput;

    source = json_array();
    BailIfNull(source);

    renameMapJson = json_array();
    BailIfNull(renameMapJson);

    keyArrays = json_array();
    BailIfNull(keyArrays);

    for (unsigned ii = 0; ii < unionInput->numSrcTables; ii++) {
        json_t *name = json_string(unionInput->srcTables[ii].tableName);
        BailIfNull(name);

        ret = json_array_append_new(source, name);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        name = NULL;

        renameMap = json_array();
        BailIfNull(renameMap);

        keyArray = json_array();
        BailIfNull(keyArray);

        for (unsigned jj = 0; jj < unionInput->renameMapSizes[ii]; jj++) {
            val = json_pack_ex(err,
                               0,
                               JsonPackColumnFormatString,
                               SourceColumnKey,
                               unionInput->renameMap[ii][jj].oldName,
                               DestColumnKey,
                               unionInput->renameMap[ii][jj].newName,
                               ColumnTypeKey,
                               strGetFromDfFieldType(
                                   unionInput->renameMap[ii][jj].type));
            BailIfNull(val);

            ret = json_array_append_new(renameMap, val);
            BailIfFailedWith(ret, StatusJsonQueryParseError);
            val = NULL;

            if (unionInput->renameMap[ii][jj].isKey) {
                val = json_string(unionInput->renameMap[ii][jj].oldName);
                BailIfNullWith(val, StatusJsonQueryParseError);

                ret = json_array_append_new(keyArray, val);
                BailIfFailedWith(ret, StatusJsonQueryParseError);
                val = NULL;
            }
        }

        ret = json_array_append_new(renameMapJson, renameMap);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        renameMap = NULL;

        ret = json_array_append_new(keyArrays, keyArray);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        keyArray = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        source,
                        DestKey,
                        unionInput->dstTable.tableName,
                        KeyFieldKey,
                        keyArrays,
                        DedupKey,
                        unionInput->dedup,
                        UnionTypeKey,
                        strGetFromUnionOperator(unionInput->unionType),
                        ColumnsKey,
                        renameMapJson);
    source = NULL;
    renameMapJson = NULL;
    keyArrays = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (val != NULL) {
        json_decref(val);
        val = NULL;
    }

    if (source != NULL) {
        json_decref(source);
        source = NULL;
    }

    if (renameMap != NULL) {
        json_decref(renameMap);
        renameMap = NULL;
    }

    if (renameMapJson != NULL) {
        json_decref(renameMapJson);
        renameMapJson = NULL;
    }

    if (keyArray != NULL) {
        json_decref(keyArray);
        keyArray = NULL;
    }

    if (keyArrays != NULL) {
        json_decref(keyArrays);
        keyArrays = NULL;
    }

    *argsOut = args;

    return status;
}
