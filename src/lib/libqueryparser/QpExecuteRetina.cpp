// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
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
#include "QpConstants.h"
#include "dag/DagLib.h"
#include "sys/XLog.h"
#include "util/Base64.h"

using namespace qp;

static constexpr char const *moduleName = "qpExecuteRetina";

QpExecuteRetina::QpExecuteRetina()
{
    this->isValidCmdParser = true;
}

QpExecuteRetina::~QpExecuteRetina()
{
    this->isValidCmdParser = false;
}

Status
QpExecuteRetina::parseArgs(int argc,
                           char *argv[],
                           ExecuteRetinaArgs *executeRetinaArgs)
{
    Status status = StatusUnknown;
    int optionIndex = 0;
    int flag;
    char *parameterString = NULL;
    OptionThr longOptions[] = {
        {"retinaName", required_argument, 0, 'r'},
        {"schedName", required_argument, 0, 's'},
        {"queryName", required_argument, 0, 'q'},
        {"dstTable", required_argument, 0, 't'},
        {"parameters", required_argument, 0, 'p'},
        {"latencyOptimized", no_argument, 0, 'l'},
        {0, 0, 0, 0},
    };

    assert(executeRetinaArgs != NULL);

    executeRetinaArgs->exportToActiveSession = false;
    executeRetinaArgs->schedName = NULL;
    executeRetinaArgs->retinaName = NULL;
    executeRetinaArgs->queryName = NULL;
    executeRetinaArgs->dstTableName = NULL;
    executeRetinaArgs->numParameters = 0;

    GetOptDataThr optData;

    getOptLongInit();
    while ((flag = getOptLong(argc,
                              argv,
                              "r:t:p:l",
                              longOptions,
                              &optionIndex,
                              &optData)) != -1) {
        switch (flag) {
        case 'r':
            executeRetinaArgs->retinaName = optData.optarg;
            break;

        case 's':
            executeRetinaArgs->schedName = optData.optarg;
            break;

        case 'q':
            executeRetinaArgs->queryName = optData.optarg;
            break;

        case 't':
            executeRetinaArgs->dstTableName = optData.optarg;
            executeRetinaArgs->exportToActiveSession = true;
            break;

        case 'p':
            parameterString = optData.optarg;
            break;

        case 'l':
            break;

        default:
            status = StatusCliParseError;
            goto CommonExit;
        }
    }

    if (executeRetinaArgs->retinaName == NULL) {
        fprintf(stderr, "--retinaName <retinaName> required\n");
        status = StatusCliParseError;
        goto CommonExit;
    }

    unsigned ret, ii;
    char *token, *saveptr;
    char *token2, *saveptr2;

    ii = 0;
    if (parameterString != NULL) {
        if (ii >= ArrayLen(executeRetinaArgs->parameters)) {
            return Status2Big;
        }

        token = strtok_r(parameterString, fieldDelim, &saveptr);

        while (token != NULL) {
            // parameterName
            token2 = strtok_r(token, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret = strlcpy(executeRetinaArgs->parameters[ii].parameterName,
                          token2,
                          sizeof(
                              executeRetinaArgs->parameters[ii].parameterName));
            if (ret >=
                sizeof(executeRetinaArgs->parameters[ii].parameterName)) {
                return StatusParameterTooLong;
            }

            // parameterValue
            token2 = strtok_r(NULL, argDelim, &saveptr2);
            if (token2 == NULL) {
                return StatusCliParseError;
            }

            ret =
                strlcpy(executeRetinaArgs->parameters[ii].parameterValue,
                        token2,
                        sizeof(
                            executeRetinaArgs->parameters[ii].parameterValue));
            if (ret >=
                sizeof(executeRetinaArgs->parameters[ii].parameterValue)) {
                return StatusParameterTooLong;
            }

            ii++;
            token = strtok_r(NULL, fieldDelim, &saveptr);
        }
    }

    executeRetinaArgs->numParameters = ii;

    status = StatusOk;

CommonExit:
    return status;
}

Status
QpExecuteRetina::parse(int argc, char *argv[], XcalarWorkItem **workItemOut)
{
    Status status;
    ExecuteRetinaArgs *executeRetinaArgs = NULL;
    XcalarWorkItem *workItem = NULL;

    executeRetinaArgs =
        (ExecuteRetinaArgs *) memAllocExt(sizeof(*executeRetinaArgs),
                                          moduleName);
    if (executeRetinaArgs == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = parseArgs(argc, argv, executeRetinaArgs);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem =
        xcalarApiMakeExecuteRetinaWorkItem(executeRetinaArgs->retinaName,
                                           executeRetinaArgs->queryName,
                                           executeRetinaArgs
                                               ->exportToActiveSession,
                                           executeRetinaArgs->schedName,
                                           executeRetinaArgs->dstTableName,
                                           executeRetinaArgs->numParameters,
                                           executeRetinaArgs->parameters,
                                           NULL,
                                           NULL);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (executeRetinaArgs != NULL) {
        memFree(executeRetinaArgs);
        executeRetinaArgs = NULL;
    }

    *workItemOut = workItem;

    return status;
}

Status
QpExecuteRetina::parseJson(json_t *op,
                           json_error_t *err,
                           XcalarWorkItem **workItemOut)
{
    Status status = StatusOk;
    const char *retinaName = "", *queryName = "", *dest = "", *schedName = "",
               *udfUserName = "", *udfSessionName = "";

    json_t *parameters = NULL;
    XcalarWorkItem *workItem = NULL;
    bool exportToActiveSession = false;
    char *encRetinaBuf = NULL;
    size_t encRetinaBufSize = 0;
    char *decRetinaBuf = NULL;
    size_t decRetinaBufSize = 0;
    XcalarApiExecuteRetinaInput *oldInput = NULL;
    XcalarApiExecuteRetinaInput *newInputwRetina = NULL;
    size_t newSize;
    size_t oldSize;
    int latencyOptimized = false;
    XcalarApiParameter *params = NULL;
    json_t *resultObj = NULL;

    int ret = json_unpack_ex(op,
                             err,
                             0,
                             JsonUnpackFormatString,
                             RetinaNameKey,
                             &retinaName,
                             RetinaBufKey,
                             &encRetinaBuf,
                             RetinaBufSizeKey,
                             &encRetinaBufSize,
                             QueryNameKey,
                             &queryName,
                             DestKey,
                             &dest,
                             ParametersKey,
                             &parameters,
                             LatencyOptimizedKey,
                             &latencyOptimized,
                             SchedNameKey,
                             &schedName,
                             UdfUserNameKey,
                             &udfUserName,
                             UdfSessionNameKey,
                             &udfSessionName);
    if (ret != 0) {
        status = StatusJsonQueryParseError;
        goto CommonExit;
    }

    json_t *param;
    unsigned ii;
    unsigned numParams;

    exportToActiveSession = (strlen(dest) > 0);
    numParams = json_array_size(parameters);
    if (numParams > XcalarApiMaxNumParameters) {
        status = StatusInval;
        goto CommonExit;
    }

    params = (XcalarApiParameter *) memAlloc(numParams * sizeof(*params));
    BailIfNull(params);

    json_array_foreach (parameters, ii, param) {
        json_t *paramName, *paramValue;

        paramName = json_object_get(param, ParamNameKey);
        BailIfNullWith(paramName, StatusJsonQueryParseError);

        paramValue = json_object_get(param, ParamValueKey);
        BailIfNullWith(paramValue, StatusJsonQueryParseError);

        status = strStrlcpy(params[ii].parameterName,
                            json_string_value(paramName),
                            sizeof(params[ii].parameterName));
        BailIfFailed(status);

        status = strStrlcpy(params[ii].parameterValue,
                            json_string_value(paramValue),
                            sizeof(params[ii].parameterValue));
        BailIfFailed(status);
    }

    workItem = xcalarApiMakeExecuteRetinaWorkItem(retinaName,
                                                  queryName,
                                                  exportToActiveSession,
                                                  schedName,
                                                  dest,
                                                  numParams,
                                                  params,
                                                  udfUserName,
                                                  udfSessionName);
    BailIfNull(workItem);

    // Make room for retina in input by re-allocing input
    // See Xc-12069 and Xc-12051
    if (encRetinaBufSize > 0) {
        // Detect if the retinaBuf payload is a string in JSON layout - this is
        // possible only if a query with embedded exec-retina nodes was reverse
        // parsed and the result sent to be parsed. However, there are no known
        // use cases which need to parse such retinaBuf payloads - so just
        // reject such payloads - first detect this by null-terminating the max
        // possible string payload (if it's not a string but a .tar.gz, it
        // wouldn't be NULL terminated and then json_loads may just crash
        // looking for a NULL) Save the last char (which would be a NULL for a
        // maximal JSON string, and so saving a NULL in the last char will not
        // destroy data; if the string is less than maximal payload, there'd be
        // a NULL before the last char, and so everything after that char would
        // be junk so storing NULL in the last char doesn't destroy data even in
        // this case).
        char saveLastChar = encRetinaBuf[encRetinaBufSize - 1];
        encRetinaBuf[encRetinaBufSize - 1] = '\0';

        resultObj = json_loads(encRetinaBuf, JSON_DECODE_ANY, NULL);
        if (resultObj != NULL) {
            // fail since a valid json object was loaded; encRetinaBuf should
            // have the retina's compressed tarball, not a json string! The
            // reverseParse action below to replace the retinaBuf with a JSON
            // string, if somehow fed back into a parse routine, may cause this
            // but this is illegal and not possible to parse. There are no
            // known use cases for such a scenario.
            status = StatusRetinaParseError;
            xSyslog(moduleName,
                    XlogErr,
                    "Unexpected JSON string found in retinaBuf when parsing "
                    "ExecuteRetina node with name '%s': %s",
                    retinaName,
                    strGetFromStatus(status));
            assert(0);
            goto CommonExit;
        }
        // restore the last char which was NULL terminated above for a
        // safe json_loads
        encRetinaBuf[encRetinaBufSize - 1] = saveLastChar;
        status = base64Decode(encRetinaBuf,
                              encRetinaBufSize,
                              (uint8_t **) &decRetinaBuf,
                              &decRetinaBufSize);
        BailIfFailed(status);

        oldInput = (XcalarApiExecuteRetinaInput *) workItem->input;
        assert(oldInput->exportRetinaBuf == NULL);
        assert(oldInput->exportRetinaBufSize == 0);
        oldSize = workItem->inputSize;
        newSize = oldSize + decRetinaBufSize;

        newInputwRetina = (XcalarApiExecuteRetinaInput *) memAlloc(newSize);
        BailIfNull(newInputwRetina);
        memZero(newInputwRetina, newSize);
        memcpy(newInputwRetina, oldInput, oldSize);

        workItem->input = (XcalarApiInput *) newInputwRetina;
        workItem->inputSize = newSize;
        memFree(oldInput);

        newInputwRetina->exportRetinaBufSize = decRetinaBufSize;
        newInputwRetina->exportRetinaBuf =
            (char *) ((uintptr_t) newInputwRetina->parameters +
                      (numParams * sizeof(*newInputwRetina->parameters)));
        memcpy(newInputwRetina->exportRetinaBuf,
               decRetinaBuf,
               decRetinaBufSize);
    }

CommonExit:

    if (resultObj) {
        json_decref(resultObj);
        resultObj = NULL;
    }
    if (decRetinaBuf != NULL) {
        memFree(decRetinaBuf);
        decRetinaBuf = NULL;
    }
    if (params != NULL) {
        memFree(params);
        params = NULL;
    }
    *workItemOut = workItem;

    return status;
}

Status
QpExecuteRetina::reverseParse(const XcalarApiInput *input,
                              json_error_t *err,
                              json_t **argsOut)
{
    const XcalarApiExecuteRetinaInput *retInput = &input->executeRetinaInput;
    int ret;
    Status status = StatusOk;
    json_t *params = NULL, *args = NULL;
    json_t *param = NULL, *paramName = NULL, *paramValue = NULL;
    char *retinaBuf = NULL;
    size_t retinaBufSize = 0;
    bool latencyOptimized = false;
    json_t *retinaJson = NULL;

    params = json_array();
    BailIfNull(params);

    for (unsigned ii = 0; ii < retInput->numParameters; ii++) {
        param = json_object();
        BailIfNull(param);

        paramName = json_string(retInput->parameters[ii].parameterName);
        BailIfNull(paramName);

        paramValue = json_string(retInput->parameters[ii].parameterValue);
        BailIfNull(paramValue);

        ret = json_object_set_new(param, ParamNameKey, paramName);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        paramName = NULL;

        ret = json_object_set_new(param, ParamValueKey, paramValue);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        paramValue = NULL;

        ret = json_array_append_new(params, param);
        BailIfFailedWith(ret, StatusJsonQueryParseError);
        param = NULL;
    }
    if (retInput->exportRetinaBuf != NULL) {
        // This is mainly for upgrade; an embedded execute retina node's
        // contents in exportRetinaBuf are in compressed tarfile format. The
        // reverse parser should really extract the json out in the spirit of
        // true reverse parsing into json.
        status =
            DagLib::get()->parseRetinaFileToJson(retInput->exportRetinaBuf,
                                                 retInput->exportRetinaBufSize,
                                                 retInput->retinaName,
                                                 &retinaJson);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to parse retina '%s' in embedded ExecuteRetina "
                    "node: %s",
                    retInput->retinaName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        retinaBuf = json_dumps(retinaJson, JSON_INDENT(4) | JSON_ENSURE_ASCII);
        if (retinaBuf == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Failure in json_dumps for retina '%s' in embedded "
                    "ExecuteRetina node: %s",
                    retInput->retinaName,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        retinaBufSize = strlen(retinaBuf);
    }

    args = json_pack_ex(err,
                        0,
                        JsonPackFormatString,
                        RetinaNameKey,
                        retInput->retinaName,
                        RetinaBufKey,
                        retinaBuf,
                        RetinaBufSizeKey,
                        retinaBufSize,
                        QueryNameKey,
                        retInput->queryName,
                        DestKey,
                        retInput->dstTable.tableName,
                        ParametersKey,
                        params,
                        LatencyOptimizedKey,
                        latencyOptimized,
                        SchedNameKey,
                        retInput->schedName);
    params = NULL;
    BailIfNullWith(args, StatusJsonQueryParseError);

CommonExit:
    if (retinaJson) {
        json_decref(retinaJson);
        retinaJson = NULL;
    }
    if (params != NULL) {
        json_decref(params);
        params = NULL;
    }

    if (param != NULL) {
        json_decref(param);
        param = NULL;
    }

    if (paramName != NULL) {
        json_decref(paramName);
        paramName = NULL;
    }

    if (paramValue != NULL) {
        json_decref(paramValue);
        paramValue = NULL;
    }

    if (retinaBuf != NULL) {
        memFree(retinaBuf);
        retinaBuf = NULL;
    }

    *argsOut = args;

    return status;
}
