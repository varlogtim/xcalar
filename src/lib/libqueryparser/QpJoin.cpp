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
#include "QpConstants.h"
#include "queryparser/QueryParser.h"
#include "strings/String.h"
#include "sys/XLog.h"

using namespace qp;
static constexpr const char *moduleName = "QpJoin";

QpJoin::QpJoin()
{
    this->isValidCmdParser = true;
}

QpJoin::~QpJoin()
{
    this->isValidCmdParser = false;
}

Status
QpJoin::parseArgs(int argc, char *argv[], JoinArgs *joinArgs)
{
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"leftTable", required_argument, 0, 'l'},
        {"rightTable", required_argument, 0, 'r'},
        {"joinTable", required_argument, 0, 'j'},
        {"joinType", required_argument, 0, 't'},
        {"leftRenameMap", required_argument, 0, 'e'},
        {"rightRenameMap", required_argument, 0, 'i'},
        {"collisionCheck", no_argument, 0, 'c'},
        {0, 0, 0, 0},
    };

    JoinOperator defaultJoinType = InnerJoin;
    const char *joinType = NULL;
    char *leftRenameMap = NULL;
    char *rightRenameMap = NULL;

    assert(joinArgs != NULL);

    joinArgs->leftTableName = NULL;
    joinArgs->rightTableName = NULL;
    joinArgs->joinTableName = NULL;
    joinArgs->joinType = InnerJoin;
    joinArgs->collisionCheck = false;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "l:r:j:t:e:i:c",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'l':
            joinArgs->leftTableName = optData.optarg;
            break;

        case 'r':
            joinArgs->rightTableName = optData.optarg;
            break;

        case 'j':
            joinArgs->joinTableName = optData.optarg;
            break;

        case 't':
            joinType = optData.optarg;
            break;

        case 'e':
            leftRenameMap = optData.optarg;
            break;

        case 'i':
            rightRenameMap = optData.optarg;
            break;

        case 'c':
            joinArgs->collisionCheck = true;
            break;

        default:
            return StatusCliParseError;
        }
    }

    if (joinType == NULL) {
        fprintf(stderr,
                "joinType not specified. Defaulting to %s\n",
                strGetFromJoinOperator(defaultJoinType));
        joinArgs->joinType = defaultJoinType;
    } else {
        joinArgs->joinType = strToJoinOperator(joinType);
        if (joinArgs->joinType == (JoinOperator) JoinOperatorLen) {
            fprintf(stderr, "Unknown join type: %s\n", joinType);
            return StatusCliParseError;
        }
    }

    if (joinArgs->leftTableName == NULL) {
        fprintf(stderr, "--leftTable <leftTable> required\n");
        return StatusCliParseError;
    }

    if (joinArgs->rightTableName == NULL) {
        fprintf(stderr, "--rightTable <rightTable> required\n");
        return StatusCliParseError;
    }

    unsigned ret, ii = 0;
    char *token, *saveptr;
    char *token2, *saveptr2;

    if (leftRenameMap != NULL) {
        if (ii >= ArrayLen(joinArgs->renameMap)) {
            return Status2Big;
        }

        token = strtok_r(leftRenameMap, fieldDelim, &saveptr);

        while (token != NULL) {
            // oldName
            token2 = strtok_r(token, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret = strlcpy(joinArgs->renameMap[ii].oldName,
                          token2,
                          sizeof(joinArgs->renameMap[ii].oldName));
            if (ret >= sizeof(joinArgs->renameMap[ii].oldName)) {
                return StatusParameterTooLong;
            }

            // newName
            token2 = strtok_r(NULL, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret = strlcpy(joinArgs->renameMap[ii].newName,
                          token2,
                          sizeof(joinArgs->renameMap[ii].newName));
            if (ret >= sizeof(joinArgs->renameMap[ii].newName)) {
                return StatusParameterTooLong;
            }

            // type
            token2 = strtok_r(NULL, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            if (*token2 == fp) {
                joinArgs->renameMap[ii].type = DfFatptr;
            } else if (*token2 == imm) {
                joinArgs->renameMap[ii].type = DfUnknown;
            } else {
                return StatusCliParseError;
            }

            ii++;
            token = strtok_r(NULL, fieldDelim, &saveptr);
        }
    }

    joinArgs->numLeftColumns = ii;

    if (rightRenameMap != NULL) {
        if (ii >= ArrayLen(joinArgs->renameMap)) {
            return Status2Big;
        }

        token = strtok_r(rightRenameMap, fieldDelim, &saveptr);

        while (token != NULL) {
            // oldName
            token2 = strtok_r(token, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret = strlcpy(joinArgs->renameMap[ii].oldName,
                          token2,
                          sizeof(joinArgs->renameMap[ii].oldName));
            if (ret >= sizeof(joinArgs->renameMap[ii].oldName)) {
                return StatusParameterTooLong;
            }

            // newName
            token2 = strtok_r(NULL, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret = strlcpy(joinArgs->renameMap[ii].newName,
                          token2,
                          sizeof(joinArgs->renameMap[ii].newName));
            if (ret >= sizeof(joinArgs->renameMap[ii].newName)) {
                return StatusParameterTooLong;
            }

            // type
            token2 = strtok_r(NULL, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            if (*token2 == fp) {
                joinArgs->renameMap[ii].type = DfFatptr;
            } else if (*token2 == imm) {
                joinArgs->renameMap[ii].type = DfUnknown;
            } else {
                return StatusCliParseError;
            }

            ii++;
            token = strtok_r(NULL, fieldDelim, &saveptr);
        }
    }

    joinArgs->numRightColumns = ii - joinArgs->numLeftColumns;

    if (joinArgs->numRightColumns + joinArgs->numLeftColumns >
        TupleMaxNumValuesPerRecord) {
        return StatusFieldLimitExceeded;
    }

    return StatusOk;
}

Status
QpJoin::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    JoinArgs *joinArgs = new (std::nothrow) JoinArgs();
    BailIfNull(joinArgs);

    status = parseArgs(argc, argv, joinArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeJoinWorkItem(joinArgs->leftTableName,
                                         joinArgs->rightTableName,
                                         joinArgs->joinTableName,
                                         joinArgs->joinType,
                                         joinArgs->collisionCheck,
                                         true,
                                         false,
                                         joinArgs->numLeftColumns,
                                         joinArgs->numRightColumns,
                                         joinArgs->renameMap,
                                         NULL);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;
    if (joinArgs) {
        delete joinArgs;
        joinArgs = NULL;
    }

    return status;
}

Status
QpJoin::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *sourceLeft = "", *sourceRight = "", *joinType = "", *dest = "",
               *filterString = "";
    int keepAll = 1, nullSafe = 0;
    json_t *leftRenameMap = NULL, *rightRenameMap = NULL, *leftKeys = NULL,
           *rightKeys = NULL;
    unsigned leftRenameMapSize = 0, rightRenameMapSize = 0;
    unsigned leftNumKeys = 0, rightNumKeys = 0;
    XcalarApiRenameMap *renameMap = NULL;
    const char *leftKeyNames[TupleMaxNumValuesPerRecord];
    const char *rightKeyNames[TupleMaxNumValuesPerRecord];
    XcalarWorkItem *workItem = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &sourceLeft,
                             &sourceRight,
                             DestKey,
                             &dest,
                             JoinTypeKey,
                             &joinType,
                             KeyFieldKey,
                             &leftKeys,
                             &rightKeys,
                             ColumnsKey,
                             &leftRenameMap,
                             &rightRenameMap,
                             EvalStringKey,
                             &filterString,
                             KeepAllColumnsKey,
                             &keepAll,
                             NullSafeKey,
                             &nullSafe);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    if (leftRenameMap != NULL) {
        leftRenameMapSize = json_array_size(leftRenameMap);
    }

    if (rightRenameMap != NULL) {
        rightRenameMapSize = json_array_size(rightRenameMap);
    }

    if (leftKeys != NULL) {
        leftNumKeys = json_array_size(leftKeys);
        if (leftNumKeys > TupleMaxNumValuesPerRecord) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "leftNumKeys (%u) exceeds max num columns (%u)",
                          leftNumKeys,
                          TupleMaxNumValuesPerRecord);

            status = StatusInval;
            goto CommonExit;
        }
    }

    if (rightKeys != NULL) {
        rightNumKeys = json_array_size(rightKeys);
        if (rightNumKeys > TupleMaxNumValuesPerRecord) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "rightNumKeys (%u) exceeds max num columns (%u)",
                          rightNumKeys,
                          TupleMaxNumValuesPerRecord);

            status = StatusInval;
            goto CommonExit;
        }
    }

    if (leftNumKeys + rightNumKeys > 0) {
        unsigned ii;
        json_t *val;

        json_array_foreach (leftKeys, ii, val) {
            leftKeyNames[ii] = json_string_value(val);
            BailIfNullWith(leftKeyNames[ii], StatusJsonQueryParseError);
        }

        json_array_foreach (rightKeys, ii, val) {
            rightKeyNames[ii] = json_string_value(val);
            BailIfNullWith(rightKeyNames[ii], StatusJsonQueryParseError);
        }
    }

    if (leftRenameMapSize + rightRenameMapSize > 0) {
        unsigned total = leftRenameMapSize + rightRenameMapSize;

        if (total > TupleMaxNumValuesPerRecord) {
            xSyslogTxnBuf(moduleName,
                          XlogErr,
                          "total columns (%u) exceeds max num columns (%u)",
                          total,
                          TupleMaxNumValuesPerRecord);

            status = StatusInval;
            goto CommonExit;
        }

        renameMap = (XcalarApiRenameMap *) memAlloc(total * sizeof(*renameMap));
        BailIfNull(renameMap);

        status = parseColumnsArray(leftRenameMap,
                                   err,
                                   renameMap,
                                   leftNumKeys,
                                   leftKeyNames);
        BailIfFailed(status);

        status = parseColumnsArray(rightRenameMap,
                                   err,
                                   &renameMap[leftRenameMapSize],
                                   rightNumKeys,
                                   rightKeyNames);
        BailIfFailed(status);
    }

    JoinOperator joinOp;

    joinOp = strToJoinOperator(joinType);

    if (!isValidJoinOperator(joinOp)) {
        status = StatusInval;
        BailIfFailedTxnMsg(moduleName,
                           StatusInval,
                           "%s is not a valid join operator",
                           joinType);
    }

    workItem = xcalarApiMakeJoinWorkItem(sourceLeft,
                                         sourceRight,
                                         dest,
                                         joinOp,
                                         true,
                                         keepAll,
                                         nullSafe,
                                         leftRenameMapSize,
                                         rightRenameMapSize,
                                         renameMap,
                                         filterString);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:

    if (renameMap != NULL) {
        memFree(renameMap);
        renameMap = NULL;
    }

    *workItemOut = workItem;

    return status;
}

Status
QpJoin::reverseParse(const XcalarApiInput *input,
                     json_error_t *err,
                     json_t **argsOut)
{
    const XcalarApiJoinInput *joinInput;
    int ret;
    Status status = StatusOk;
    json_t *args = NULL, *leftRenameMap = NULL, *rightRenameMap = NULL,
           *leftKeys = NULL, *rightKeys = NULL;
    json_t *val = NULL;

    joinInput = &input->joinInput;

    leftRenameMap = json_array();
    BailIfNull(leftRenameMap);

    rightRenameMap = json_array();
    BailIfNull(rightRenameMap);

    leftKeys = json_array();
    BailIfNull(leftKeys);

    rightKeys = json_array();
    BailIfNull(rightKeys);

    for (unsigned iter = 0; iter <= 1; iter++) {
        json_t *renameMap;
        json_t *keys;
        unsigned start, end;
        if (iter == 0) {
            renameMap = leftRenameMap;
            keys = leftKeys;
            start = 0;
            end = joinInput->numLeftColumns;
        } else {
            renameMap = rightRenameMap;
            keys = rightKeys;
            start = joinInput->numLeftColumns;
            end = joinInput->numLeftColumns + joinInput->numRightColumns;
        }

        for (unsigned ii = start; ii < end; ii++) {
            val = json_pack_ex(err,
                               0,
                               JsonPackColumnFormatString,
                               SourceColumnKey,
                               joinInput->renameMap[ii].oldName,
                               DestColumnKey,
                               joinInput->renameMap[ii].newName,
                               ColumnTypeKey,
                               strGetFromDfFieldType(
                                   joinInput->renameMap[ii].type));
            BailIfNullWith(val, StatusJsonQueryParseError);

            ret = json_array_append_new(renameMap, val);
            BailIfFailedWith(ret, StatusJsonQueryParseError);
            val = NULL;

            if (joinInput->renameMap[ii].isKey) {
                val = json_string(joinInput->renameMap[ii].oldName);
                BailIfNullWith(val, StatusJsonQueryParseError);

                ret = json_array_append_new(keys, val);
                BailIfFailedWith(ret, StatusJsonQueryParseError);
                val = NULL;
            }
        }
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        joinInput->leftTable.tableName,
                        joinInput->rightTable.tableName,
                        DestKey,
                        joinInput->joinTable.tableName,
                        JoinTypeKey,
                        strGetFromJoinOperator(joinInput->joinType),
                        KeyFieldKey,
                        leftKeys,
                        rightKeys,
                        ColumnsKey,
                        leftRenameMap,
                        rightRenameMap,
                        EvalStringKey,
                        joinInput->filterString,
                        KeepAllColumnsKey,
                        joinInput->keepAllColumns,
                        NullSafeKey,
                        joinInput->nullSafe);
    leftRenameMap = NULL;
    rightRenameMap = NULL;
    leftKeys = NULL;
    rightKeys = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (val != NULL) {
        json_decref(val);
        val = NULL;
    }

    if (leftRenameMap != NULL) {
        json_decref(leftRenameMap);
        leftRenameMap = NULL;
    }

    if (rightRenameMap != NULL) {
        json_decref(rightRenameMap);
        rightRenameMap = NULL;
    }

    if (leftKeys != NULL) {
        json_decref(leftKeys);
        leftKeys = NULL;
    }

    if (rightKeys != NULL) {
        json_decref(rightKeys);
        rightKeys = NULL;
    }

    *argsOut = args;

    return status;
}
