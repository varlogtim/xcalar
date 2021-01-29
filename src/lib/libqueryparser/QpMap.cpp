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
#include "QpConstants.h"

using namespace qp;

QpMap::QpMap()
{
    this->isValidCmdParser = true;
}

QpMap::~QpMap()
{
    this->isValidCmdParser = false;
}

Status
QpMap::parseArgs(int argc, char *argv[], MapArgs *mapArgs)
{
    (void) argDelim;
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    char *evalStrings = NULL;
    char *fieldNames = NULL;
    unsigned numEvals = 0;
    OptionThr longOptions[] = {
        {"eval", required_argument, 0, 'e'},
        {"srctable", required_argument, 0, 's'},
        {"dsttable", required_argument, 0, 't'},
        {"fieldName", required_argument, 0, 'n'},
        {"icvMode", no_argument, 0, 'i'},
        {0, 0, 0, 0},
    };

    assert(mapArgs != NULL);

    mapArgs->icvMode = false;
    mapArgs->srcTableName = NULL;
    mapArgs->dstTableName = NULL;
    mapArgs->numEvals = 0;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "e:s:t:i",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'e':
            evalStrings = optData.optarg;
            break;

        case 's':
            mapArgs->srcTableName = optData.optarg;
            break;

        case 't':
            mapArgs->dstTableName = optData.optarg;
            break;

        case 'n':
            fieldNames = optData.optarg;
            break;

        case 'i':
            mapArgs->icvMode = true;
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

    if (mapArgs->srcTableName == NULL) {
        fprintf(stderr, "--srctable <tableName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (fieldNames == NULL) {
        fprintf(stderr, "--fieldName <newFieldName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    if (mapArgs->dstTableName == NULL) {
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
        if (numEvals >= ArrayLen(mapArgs->evalStrs)) {
            return Status2Big;
        }

        mapArgs->evalStrs[numEvals] = evalToken;

        if (strlen(evalToken) >= XcalarApiMaxEvalStringLen) {
            status = StatusEvalStringTooLong;
            goto CommonExit;
        }

        mapArgs->newFieldNames[numEvals] = fieldToken;

        if (strlen(fieldToken) >= XcalarApiMaxFieldNameLen) {
            status = StatusNameTooLong;
            goto CommonExit;
        }

        numEvals++;
        evalToken = strtok_r(NULL, fieldDelim, &evalSaveptr);
        fieldToken = strtok_r(NULL, fieldDelim, &fieldSaveptr);
    }

    mapArgs->numEvals = numEvals;

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpMap::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    MapArgs mapArgs;
    XcalarWorkItem *workItem = NULL;

    status = parseArgs(argc, argv, &mapArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeMapWorkItem(mapArgs.srcTableName,
                                        mapArgs.dstTableName,
                                        mapArgs.icvMode,
                                        mapArgs.numEvals,
                                        mapArgs.evalStrs,
                                        mapArgs.newFieldNames);

    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    *workItemOut = workItem;

    return status;
}

Status
QpMap::parseJson(json_t *op, json_error_t *err, XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *source = "", *dest = "";

    int icv = false;

    json_t *evalsJson = NULL;
    XcalarWorkItem *workItem = NULL;
    const char **evalStrs = NULL;
    const char **fieldNames = NULL;

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
                             IcvKey,
                             &icv);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    json_t *eval;
    unsigned ii;
    unsigned numEvals;

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

    workItem = xcalarApiMakeMapWorkItem(source,
                                        dest,
                                        icv,
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
    *workItemOut = workItem;

    return status;
}

Status
QpMap::reverseParse(const XcalarApiInput *input,
                    json_error_t *err,
                    json_t **argsOut)
{
    const XcalarApiMapInput *mapInput;
    int ret;
    Status status = StatusOk;
    json_t *evals = NULL, *args = NULL;
    json_t *eval = NULL;

    mapInput = &input->mapInput;

    evals = json_array();
    BailIfNull(evals);

    for (unsigned ii = 0; ii < mapInput->numEvals; ii++) {
        eval = json_pack_ex(err,
                            0,
                            JsonEvalFormatString,
                            EvalStringKey,
                            mapInput->evalStrs[ii],
                            NewFieldKey,
                            mapInput->newFieldNames[ii]);
        BailIfNull(eval);

        ret = json_array_append_new(evals, eval);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        eval = NULL;
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        SourceKey,
                        mapInput->srcTable.tableName,
                        DestKey,
                        mapInput->dstTable.tableName,
                        EvalKey,
                        evals,
                        IcvKey,
                        mapInput->icvMode);
    evals = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
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
