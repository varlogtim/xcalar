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
#include "QpConstants.h"

using namespace qp;

QpGroupBy::QpGroupBy()
{
    this->isValidCmdParser = true;
}

QpGroupBy::~QpGroupBy()
{
    this->isValidCmdParser = false;
}

Status
QpGroupBy::parseArgs(int argc, char *argv[], GroupByArgs *groupByArgs)
{
    (void) argDelim;
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    OptionThr longOptions[] = {
        {"eval", required_argument, 0, 'e'},
        {"srctable", required_argument, 0, 's'},
        {"dsttable", required_argument, 0, 't'},
        {"fieldName", required_argument, 0, 'n'},
        {"newKeyFieldName", required_argument, 0, 'k'},
        {"nosample", no_argument, 0, 'o'},
        {"icv", no_argument, 0, 'i'},
        {0, 0, 0, 0},
    };
    char *evalStrings = NULL;
    char *fieldNames = NULL;
    unsigned numEvals = 0;
    assert(groupByArgs != NULL);

    groupByArgs->srcTableName = NULL;
    groupByArgs->dstTableName = NULL;
    groupByArgs->newKeyFieldName = NULL;
    groupByArgs->includeSrcTableSample = true;
    groupByArgs->icvMode = false;
    groupByArgs->numEvals = 0;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "e:s:t:k:n:io",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'e':
            evalStrings = optData.optarg;
            break;

        case 's':
            groupByArgs->srcTableName = optData.optarg;
            break;

        case 't':
            groupByArgs->dstTableName = optData.optarg;
            break;

        case 'n':
            fieldNames = optData.optarg;
            break;

        case 'o':
            groupByArgs->includeSrcTableSample = false;
            break;

        case 'i':
            groupByArgs->icvMode = true;
            break;

        case 'k':
            groupByArgs->newKeyFieldName = optData.optarg;
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (evalStrings == NULL) {
        fprintf(stderr, "--eval <evalStr> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (groupByArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (fieldNames == NULL) {
        fprintf(stderr, "--fieldName <newFieldName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (groupByArgs->dstTableName == NULL) {
        // Not an error
        fprintf(stderr,
                "--dstTable <tableName> not specified. "
                "Creating new table\n");
    }

    char *evalToken, *evalSaveptr;
    char *fieldToken, *fieldSaveptr;

    evalToken = strtok_r(evalStrings, fieldDelim, &evalSaveptr);
    fieldToken = strtok_r(fieldNames, fieldDelim, &fieldSaveptr);
    while (evalToken != NULL && fieldToken != NULL) {
        if (numEvals >= ArrayLen(groupByArgs->evalStrs)) {
            return Status2Big;
        }

        groupByArgs->evalStrs[numEvals] = evalToken;

        if (strlen(evalToken) >= XcalarApiMaxEvalStringLen) {
            status = StatusEvalStringTooLong;
            goto CommonExit;
        }

        groupByArgs->newFieldNames[numEvals] = fieldToken;

        if (strlen(fieldToken) >= XcalarApiMaxFieldNameLen) {
            status = StatusNameTooLong;
            goto CommonExit;
        }

        numEvals++;
        evalToken = strtok_r(NULL, fieldDelim, &evalSaveptr);
        fieldToken = strtok_r(NULL, fieldDelim, &fieldSaveptr);
    }

    groupByArgs->numEvals = numEvals;

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpGroupBy::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    GroupByArgs groupByArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &groupByArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(status == StatusOk);

    workItem = xcalarApiMakeGroupByWorkItem(groupByArgs.srcTableName,
                                            groupByArgs.dstTableName,
                                            0,
                                            NULL,
                                            groupByArgs.includeSrcTableSample,
                                            groupByArgs.icvMode,
                                            false,
                                            groupByArgs.numEvals,
                                            groupByArgs.evalStrs,
                                            groupByArgs.newFieldNames);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpGroupBy::parseJson(json_t *op,
                     json_error_t *err,
                     XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *keyName = "", *dest = "";

    int includeSample = false, icv = false, groupAll = false;

    json_t *evalsJson = NULL, *keysJson = NULL;
    XcalarWorkItem *workItem = NULL;
    const char **evalStrs = NULL;
    const char **fieldNames = NULL;
    XcalarApiKeyInput *keys = NULL;
    unsigned numKeys = 0;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             SourceKey,
                             &source,
                             DestKey,
                             &dest,
                             EvalKey,
                             &evalsJson,
                             NewKeyFieldKey,
                             &keyName,
                             KeyFieldKey,
                             &keysJson,
                             IncludeSampleKey,
                             &includeSample,
                             IcvKey,
                             &icv,
                             GroupAllKey,
                             &groupAll);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    json_t *eval, *key;
    unsigned ii;
    unsigned numEvals;

    if (keysJson) {
        numKeys = json_array_size(keysJson);
    } else if (strlen(keyName) > 0) {
        // legacy query, only newKeyFieldName was specified
        numKeys = 1;
    }

    keys = (XcalarApiKeyInput *) memAlloc(sizeof(*keys) * numKeys);
    BailIfNull(keys);
    memZero(keys, sizeof(*keys) * numKeys);

    numEvals = json_array_size(evalsJson);
    evalStrs = (const char **) memAlloc(numEvals * sizeof(*evalStrs));
    BailIfNull(evalStrs);
    fieldNames = (const char **) memAlloc(numEvals * sizeof(*fieldNames));
    BailIfNull(fieldNames);

    json_array_foreach (evalsJson, ii, eval) {
        ret = json_unpack_ex(eval,
                             err,
                             0,
                             JsonEvalFormatString,
                             EvalStringKey,
                             &evalStrs[ii],
                             NewFieldKey,
                             &fieldNames[ii]);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
    }

    if (keysJson) {
        json_array_foreach (keysJson, ii, key) {
            const char *keyName = "", *keyFieldName = "", *type = "DfUnknown",
                       *ordering = "Unordered";

            ret = json_unpack_ex(key,
                                 err,
                                 0,
                                 JsonKeyUnpackFormatString,
                                 KeyNameKey,
                                 &keyName,
                                 KeyFieldNameKey,
                                 &keyFieldName,
                                 KeyTypeKey,
                                 &type,
                                 OrderingKey,
                                 &ordering);
            BailIfFailedWith(ret, StatusJsonQueryParseError);

            status =
                strStrlcpy(keys[ii].keyName, keyName, sizeof(keys[ii].keyName));
            BailIfFailed(status);

            status = strStrlcpy(keys[ii].keyFieldName,
                                keyFieldName,
                                sizeof(keys[ii].keyFieldName));
            BailIfFailed(status);

            keys[ii].type = strToDfFieldType(type);
            if (!isValidDfFieldType(keys[ii].type)) {
                status = StatusJsonQueryParseError;
                goto CommonExit;
            }

            keys[ii].ordering = strToOrdering(ordering);
        }
    } else if (strlen(keyName) > 0) {
        // legacy query, only newKeyFieldName was specified
        keys[0].keyName[0] = '\0';
        strlcpy(keys[0].keyFieldName, keyName, sizeof(keys[0].keyFieldName));
    }

    workItem = xcalarApiMakeGroupByWorkItem(source,
                                            dest,
                                            numKeys,
                                            keys,
                                            includeSample,
                                            icv,
                                            groupAll,
                                            numEvals,
                                            evalStrs,
                                            fieldNames);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (evalStrs != NULL) {
        memFree(evalStrs);
        evalStrs = NULL;
    }
    if (fieldNames != NULL) {
        memFree(fieldNames);
        fieldNames = NULL;
    }
    if (keys != NULL) {
        memFree(keys);
        keys = NULL;
    }

    *workItemOut = workItem;

    return status;
}

Status
QpGroupBy::reverseParse(const XcalarApiInput *input,
                        json_error_t *err,
                        json_t **argsOut)
{
    const XcalarApiGroupByInput *groupByInput;
    int ret;
    Status status = StatusOk;
    json_t *evals = NULL, *args = NULL, *keys = NULL;
    json_t *eval = NULL, *key = NULL;

    groupByInput = &input->groupByInput;

    evals = json_array();
    BailIfNull(evals);

    for (unsigned ii = 0; ii < groupByInput->numEvals; ii++) {
        eval = json_pack_ex(err,
                            0,
                            JsonEvalFormatString,
                            EvalStringKey,
                            groupByInput->evalStrs[ii],
                            NewFieldKey,
                            groupByInput->newFieldNames[ii]);
        BailIfNullWith(eval, StatusJsonQueryParseError);

        ret = json_array_append_new(evals, eval);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        eval = NULL;
    }

    keys = json_array();
    BailIfNull(keys);

    for (unsigned ii = 0; ii < groupByInput->numKeys; ii++) {
        key = json_pack_ex(err,
                           0,
                           JsonKeyPackFormatString,
                           KeyNameKey,
                           groupByInput->keys[ii].keyName,
                           KeyFieldNameKey,
                           groupByInput->keys[ii].keyFieldName,
                           KeyTypeKey,
                           strGetFromDfFieldType(groupByInput->keys[ii].type),
                           OrderingKey,
                           strGetFromOrdering(groupByInput->keys[ii].ordering));
        BailIfNullWith(key, StatusJsonQueryParseError);

        ret = json_array_append_new(keys, key);
        BailIfFailedWith(ret, StatusJsonQueryParseError);

        key = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        groupByInput->srcTable.tableName,
                        DestKey,
                        groupByInput->dstTable.tableName,
                        EvalKey,
                        evals,
                        KeyFieldKey,
                        keys,
                        IncludeSampleKey,
                        groupByInput->includeSrcTableSample,
                        IcvKey,
                        groupByInput->icvMode,
                        GroupAllKey,
                        groupByInput->groupAll);
    evals = NULL;
    keys = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (keys != NULL) {
        json_decref(keys);
        keys = NULL;
    }

    if (key != NULL) {
        json_decref(key);
        key = NULL;
    }

    if (evals != NULL) {
        json_decref(evals);
        evals = NULL;
    }

    if (eval != NULL) {
        json_decref(eval);
        eval = NULL;
    }

    *argsOut = args;

    return status;
}
