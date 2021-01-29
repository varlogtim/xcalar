// Copyright 2014 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// XXX: FIXME: Must rethink libapis interface and backward-compat issues

#include <string.h>
#include <cstdlib>
#include <stdio.h>
#include <stddef.h>
#include <assert.h>
#include <unistd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "libapis/WorkItem.h"
#include "sys/Socket.h"
#include "config/Config.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "dag/DagLib.h"
#include "common/Version.h"
#include "udf/UserDefinedFunction.h"
#include "durable/Durable.h"

static constexpr const char *moduleName = "libapis::workItem";

// Helper function to allocate and fill in sessionInfo.
static bool
allocSessionInfo(const char *sessionName, XcalarWorkItem *workItem)
{
    bool succeeded = false;
    XcalarApiSessionInfoInput *sessionInfo = NULL;

    assert(workItem->sessionInfo == NULL);

    if (sessionName != NULL) {
        // Memory freed when the workItem is freed
        sessionInfo = (XcalarApiSessionInfoInput *)
            memCallocExt(1, sizeof(XcalarApiSessionInfoInput), moduleName);
        if (sessionInfo == NULL) {
            goto CommonExit;
        }
        sessionInfo->sessionId = 0;
        strlcpy(sessionInfo->sessionName,
                sessionName,
                sizeof(sessionInfo->sessionName));
        sessionInfo->sessionNameLength = strlen(sessionName);
        workItem->sessionInfo = sessionInfo;
        workItem->sessionInfoSize = sizeof(XcalarApiSessionInfoInput);
        sessionInfo = NULL;  // passed to workItem
    }

    succeeded = true;

CommonExit:

    if (sessionInfo != NULL) {
        memFree(sessionInfo);
        sessionInfo = NULL;
    }

    return succeeded;
}

Status
xcalarApiDeserializeRetinaInput(XcalarApiMakeRetinaInput *retinaInput,
                                size_t inputSize)
{
    uint64_t ii;
    size_t hdrSize =
        xcalarApiSizeOfRetinaInputHdr(retinaInput->numTargetTables);
    size_t bufCursor = (uintptr_t) retinaInput + hdrSize;
    // We need to fix up the pointers
    for (ii = 0; ii < retinaInput->numTargetTables; ii++) {
        if (bufCursor >= (uintptr_t) retinaInput + inputSize) {
            return StatusOverflow;
        }

        if (retinaInput->tableArray[ii] != (RetinaDst *) XcalarApiMagic) {
            return StatusInval;
        }
        retinaInput->tableArray[ii] = (RetinaDst *) bufCursor;
        bufCursor +=
            xcalarApiSizeOfRetinaDst(retinaInput->tableArray[ii]->numColumns);
    }

    if (retinaInput->numColumnHints > 0) {
        retinaInput->columnHints = (Column *) bufCursor;
    } else {
        retinaInput->columnHints = NULL;
    }

    bufCursor +=
        sizeof(*retinaInput->columnHints) * retinaInput->numColumnHints;
    retinaInput->srcTables = (RetinaSrcTable *) bufCursor;

    return StatusOk;
}

// ================ helper function to make specific workItem ================
XcalarWorkItem *
xcalarApiMakeGetQueryWorkItem(XcalarApis api,
                              XcalarApiInput *apiInput,
                              size_t apiInputSize)
{
    Status status = StatusUnknown;
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetQueryInput *getQueryInput = NULL;
    bool error = true;
    size_t inputSize = sizeof(*getQueryInput) + apiInputSize;

    getQueryInput =
        (XcalarApiGetQueryInput *) memCallocExt(1, inputSize, moduleName);
    if (getQueryInput == NULL) {
        goto CommonExit;
    }

    getQueryInput->api = api;
    getQueryInput->inputSize = apiInputSize;
    getQueryInput->input =
        (XcalarApiInput *) ((uintptr_t) getQueryInput + sizeof(*getQueryInput));
    memcpy(getQueryInput->input, apiInput, apiInputSize);

    status = xcalarApiSerializeGetQueryInput(getQueryInput, inputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetQuery,
                                            getQueryInput,
                                            inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (getQueryInput != NULL) {
            memFree(getQueryInput);
            getQueryInput = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListFuncTestWorkItem(const char *namePattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiListFuncTestInput *input = NULL;
    size_t ret;

    input = (XcalarApiListFuncTestInput *) memCalloc(1, sizeof(*input));
    if (input == NULL) {
        xSyslog(moduleName, XlogErr, "Insufficient memory to allocate input");
        goto CommonExit;
    }

    ret = strlcpy(input->namePattern, namePattern, sizeof(input->namePattern));
    if (ret >= sizeof(input->namePattern)) {
        xSyslog(moduleName,
                XlogErr,
                "Name pattern is too long (%lu chars). Max is %lu chars",
                strlen(namePattern),
                sizeof(input->namePattern) - 1);
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListFuncTests,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSetConfigParamWorkItem(const char *paramName,
                                    const char *paramValue)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSetConfigParamInput *input = NULL;
    size_t ret;

    input = (XcalarApiSetConfigParamInput *) memCalloc(1, sizeof(*input));
    if (input == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to set config param work item");
        goto CommonExit;
    }

    ret = strlcpy(input->paramName, paramName, sizeof(input->paramName));
    if (ret >= sizeof(input->paramName)) {
        xSyslog(moduleName,
                XlogErr,
                "Param name is too long (%lu chars).  Max is %lu chars",
                strlen(paramName),
                sizeof(input->paramName) - 1);
        goto CommonExit;
    }
    ret = strlcpy(input->paramValue, paramValue, sizeof(input->paramValue));
    if (ret >= sizeof(input->paramValue)) {
        xSyslog(moduleName,
                XlogErr,
                "Param value is too long (%lu chars).  Max is %lu chars",
                strlen(paramValue),
                sizeof(input->paramValue) - 1);
        goto CommonExit;
    }
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSetConfigParam,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeStartFuncTestWorkItem(bool parallel,
                                   bool runAllTests,
                                   bool runOnAllNodes,
                                   unsigned numTestPatterns,
                                   const char *testNamePatterns[])
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiStartFuncTestInput *input = NULL;
    size_t inputSize = 0;
    unsigned ii;
    size_t ret;

    if (numTestPatterns > XcalarApiMaxNumFuncTests) {
        xSyslog(moduleName,
                XlogErr,
                "Too many functional test requested (%u). Max is %u",
                numTestPatterns,
                XcalarApiMaxNumFuncTests);
        goto CommonExit;
    }

    inputSize =
        sizeof(*input) + (sizeof(input->testNamePatterns[0]) * numTestPatterns);

    input = (XcalarApiStartFuncTestInput *) memCalloc(1, inputSize);
    if (input == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate input "
                "(numTestPatterns: %u)",
                numTestPatterns);
        goto CommonExit;
    }

    input->parallel = parallel;
    input->runAllTests = runAllTests;
    input->runOnAllNodes = runOnAllNodes;
    input->numTestPatterns = numTestPatterns;

    for (ii = 0; ii < numTestPatterns; ii++) {
        ret = strlcpy(input->testNamePatterns[ii],
                      testNamePatterns[ii],
                      sizeof(input->testNamePatterns[ii]));
        if (ret >= sizeof(input->testNamePatterns[ii])) {
            xSyslog(moduleName,
                    XlogErr,
                    "Test name is too long (%lu bytes). Max is %lu bytes",
                    strlen(testNamePatterns[ii]),
                    sizeof(input->testNamePatterns[ii]) - 1);
            goto CommonExit;
        }
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiStartFuncTests, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeCreateDhtWorkItem(const char *dhtName,
                               float64_t upperBound,
                               float64_t lowerBound,
                               Ordering ordering)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiCreateDhtInput *input = NULL;

    if (dhtName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of Dht must be specified");
        goto CommonExit;
    }

    input =
        (XcalarApiCreateDhtInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->dhtName, dhtName, sizeof(input->dhtName));
    input->dhtArgs.upperBound = upperBound;
    input->dhtArgs.lowerBound = lowerBound;
    assert(xcalar::internal::durable::dag::Ordering_IsValid(ordering));
    input->dhtArgs.ordering = ordering;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiCreateDht, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

// Delete DHT request
XcalarWorkItem *
xcalarApiMakeDeleteDhtWorkItem(const char *dhtName, size_t dhtNameLen)
{
    bool error = true;
    XcalarWorkItem *workItem = NULL;
    XcalarApiDeleteDhtInput *input = NULL;

    if (dhtName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of Dht must be specified");
        goto DeleteDhtExit;
    }
    if (dhtNameLen >= DhtMaxDhtNameLen) {
        xSyslog(moduleName,
                XlogErr,
                "Name of Dht is too long (%lu bytes specified, %d bytes "
                "allowed)",
                dhtNameLen,
                DhtMaxDhtNameLen);
        goto DeleteDhtExit;
    }

    input =
        (XcalarApiDeleteDhtInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto DeleteDhtExit;
    }

    strlcpy(input->dhtName, dhtName, sizeof(input->dhtName));
    input->dhtNameLen = dhtNameLen;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiDeleteDht, input, sizeof(*input));
    if (workItem == NULL) {
        goto DeleteDhtExit;
    }

    error = false;

DeleteDhtExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListXdfsWorkItem(const char *fnNamePattern,
                              const char *categoryPattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiListXdfsInput *input = NULL;

    if (fnNamePattern == NULL || categoryPattern == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the function and category pattern must be specified");
        goto CommonExit;
    }

    input =
        (XcalarApiListXdfsInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->fnNamePattern, fnNamePattern, sizeof(input->fnNamePattern));
    strlcpy(input->categoryPattern,
            categoryPattern,
            sizeof(input->categoryPattern));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiListXdfs, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetNumNodesWorkItem()
{
    return xcalarApiMakeGenericWorkItem(XcalarApiGetNumNodes, NULL, 0);
}

XcalarWorkItem *
xcalarApiMakeTopWorkItem(XcalarApiTopRequestType topStatsRequestType,
                         XcalarApiTopType topApiType)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    XcalarApiTopInput *input = NULL;
    XcalarApis api;

    input = (XcalarApiTopInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    input->topStatsRequestType = topStatsRequestType;
    if (topApiType == XcalarApiTopAllNode) {
        api = XcalarApiTop;
    } else {
        assert(topApiType == XcalarApiTopLocalNode);
        api = XcalarApiPerNodeTop;
    }

    workItem = xcalarApiMakeGenericWorkItem(api, input, sizeof(*input));
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }

        if (input != NULL) {
            memFree(input);
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeExportRetinaWorkItem(const char *retinaName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiExportRetinaInput *input = NULL;
    bool error = true;
    size_t ret;

    if (retinaName == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Name of batch dataflow must be specified");
        goto CommonExit;
    }

    input = (XcalarApiExportRetinaInput *) memCallocExt(1,
                                                        sizeof(*input),
                                                        moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    ret = strlcpy(input->retinaName, retinaName, sizeof(input->retinaName));
    if (ret >= sizeof(input->retinaName)) {
        xSyslog(moduleName,
                XlogErr,
                "Input batch dataflow name too long (%lu bytes). Max "
                "is %lu bytes",
                strlen(retinaName) + 1,
                sizeof(input->retinaName));
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiExportRetina,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeImportRetinaWorkItem(const char *retinaName,
                                  bool overwriteExistingUdf,
                                  bool loadRetinaJson,
                                  const char *udfUserName,
                                  const char *udfSessionName,
                                  size_t retinaCount,
                                  uint8_t retina[])
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiImportRetinaInput *input = NULL;
    bool error = true;
    size_t inputSize, ret;

    inputSize = sizeof(*input) + retinaCount;
    input =
        (XcalarApiImportRetinaInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, sizeof(*input));

    ret = strlcpy(input->retinaName, retinaName, sizeof(input->retinaName));
    if (ret >= sizeof(input->retinaName)) {
        xSyslog(moduleName,
                XlogErr,
                "Input batch dataflow name too long (%lu bytes). "
                "Max is %lu bytes",
                strlen(retinaName) + 1,
                sizeof(input->retinaName));
        goto CommonExit;
    }

    memcpy(input->retina, retina, retinaCount);

    input->overwriteExistingUdf = overwriteExistingUdf;
    input->loadRetinaJson = loadRetinaJson;
    input->retinaCount = retinaCount;

    if (udfUserName) {
        strlcpy(input->udfUserName, udfUserName, sizeof(input->udfUserName));
    }

    if (udfSessionName) {
        strlcpy(input->udfSessionName,
                udfSessionName,
                sizeof(input->udfSessionName));
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiImportRetina, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListParametersInRetinaWorkItem(const char *retinaNameIn)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    char *retinaName = NULL;
    const size_t strSize = strlen(retinaNameIn) + 1;

    retinaName = (char *) memCallocExt(1, strSize, moduleName);
    if (retinaName == NULL) {
        goto CommonExit;
    }

    strlcpy(retinaName, retinaNameIn, strSize);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListParametersInRetina,
                                            retinaName,
                                            strSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (retinaName != NULL) {
            memFree(retinaName);
            retinaName = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUpdateRetinaWorkItem(const char *retinaName,
                                  const char *retinaJson,
                                  size_t retinaJsonLen)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiUpdateRetinaInput *input = NULL;
    size_t inputSize = sizeof(*input) + retinaJsonLen;

    input =
        (XcalarApiUpdateRetinaInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->retinaName, retinaName, sizeof(input->retinaName));
    strlcpy(input->retinaJson, retinaJson, retinaJsonLen);
    input->retinaJsonCount = retinaJsonLen;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiUpdateRetina, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

static XcalarWorkItem *
makeRetinaWorkItemWithName(const char *retinaNameIn, XcalarApis api)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    char *retinaName = NULL;
    const size_t strSize = strlen(retinaNameIn) + 1;

    retinaName = (char *) memCallocExt(1, strSize, moduleName);
    if (retinaName == NULL) {
        goto CommonExit;
    }

    strlcpy(retinaName, retinaNameIn, strSize);

    workItem = xcalarApiMakeGenericWorkItem(api, retinaName, strSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (retinaName != NULL) {
            memFree(retinaName);
            retinaName = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetRetinaWorkItem(const char *retinaNameIn)
{
    return makeRetinaWorkItemWithName(retinaNameIn, XcalarApiGetRetina);
}

XcalarWorkItem *
xcalarApiMakeGetRetinaJsonWorkItem(const char *retinaNameIn)
{
    return makeRetinaWorkItemWithName(retinaNameIn, XcalarApiGetRetinaJson);
}

XcalarWorkItem *
xcalarApiMakeDeleteRetinaWorkItem(const char *retinaNameIn)
{
    return makeRetinaWorkItemWithName(retinaNameIn, XcalarApiDeleteRetina);
}

XcalarWorkItem *
xcalarApiMakeExecuteRetinaWorkItem(const char *retinaName,
                                   const char *queryName,
                                   bool exportToActiveSession,
                                   const char *schedName,
                                   const char *newTableName,
                                   unsigned numParameters,
                                   XcalarApiParameter parameters[],
                                   const char *udfUserName,
                                   const char *udfSessionName,
                                   const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiExecuteRetinaInput *input = NULL;
    size_t inputSize;
    bool error = true;
    unsigned ii;
    unsigned ret;

    if (retinaName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of dataflow must be specified");
        goto CommonExit;
    }

    inputSize = sizeof(*input) + (sizeof(input->parameters[0]) * numParameters);
    input =
        (XcalarApiExecuteRetinaInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->retinaName, retinaName, sizeof(input->retinaName));

    if (queryName == NULL || strlen(queryName) == 0) {
        // if no query name specified, use retinaName
        ret = strlcpy(input->queryName, retinaName, sizeof(input->retinaName));
        if (ret > sizeof(input->retinaName)) {
            goto CommonExit;
        }
    } else {
        ret = strlcpy(input->queryName, queryName, sizeof(input->queryName));
        if (ret > sizeof(input->queryName)) {
            goto CommonExit;
        }
    }

    input->exportToActiveSession = exportToActiveSession;

    if (schedName != NULL) {
        ret = strlcpy(input->schedName, schedName, sizeof(input->schedName));
        if (ret > sizeof(input->schedName)) {
            goto CommonExit;
        }
    } else {
        input->schedName[0] = '\0';
    }

    if (newTableName != NULL) {
        ret = strlcpy(input->dstTable.tableName,
                      newTableName,
                      sizeof(input->dstTable.tableName));
        if (ret > sizeof(input->dstTable.tableName)) {
            goto CommonExit;
        }
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    input->numParameters = numParameters;
    for (ii = 0; ii < numParameters; ii++) {
        strlcpy(input->parameters[ii].parameterName,
                parameters[ii].parameterName,
                sizeof(input->parameters[ii].parameterName));
        strlcpy(input->parameters[ii].parameterValue,
                parameters[ii].parameterValue,
                sizeof(input->parameters[ii].parameterValue));
    }

    if (udfUserName) {
        strlcpy(input->udfUserName, udfUserName, sizeof(input->udfUserName));
    }

    if (udfSessionName) {
        strlcpy(input->udfSessionName,
                udfSessionName,
                sizeof(input->udfSessionName));
    }

    input->exportRetinaBufSize = 0;
    input->exportRetinaBuf = NULL;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiExecuteRetina, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListRetinasWorkItem(const char *namePattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiListRetinasInput *input = NULL;
    size_t inputSize = sizeof(*input);

    input = (XcalarApiListRetinasInput *) memCalloc(1, inputSize);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->namePattern, namePattern, sizeof(input->namePattern));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiListRetinas, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeMakeRetinaWorkItem(const char *retinaName,
                                uint64_t numTables,
                                RetinaDst *tableArray[],
                                uint64_t numSrcTables,
                                RetinaSrcTable *srcTables,
                                const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiMakeRetinaInput *input = NULL;
    bool error = true;
    size_t inputSize;
    uint64_t ii;
    Status status;
    size_t hdrSize = xcalarApiSizeOfRetinaInputHdr(numTables);

    if (retinaName == NULL || tableArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the dataflow name and destination name must be "
                "specified");
        goto CommonExit;
    }

    inputSize = hdrSize;
    for (ii = 0; ii < numTables; ii++) {
        inputSize += xcalarApiSizeOfRetinaDst(tableArray[ii]->numColumns);
    }
    inputSize += numSrcTables * sizeof(*srcTables);
    input = (XcalarApiMakeRetinaInput *) memCallocExt(1, inputSize, moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    status = xcalarApiSerializeRetinaInput(input,
                                           inputSize,
                                           retinaName,
                                           numTables,
                                           tableArray,
                                           numSrcTables,
                                           srcTables,
                                           0,
                                           NULL);
    if (status != StatusOk) {
        goto CommonExit;
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiMakeRetina, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListExportTargetsWorkItem(const char *typePattern,
                                       const char *namePattern)
{
    XcalarApiListExportTargetsInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (typePattern == NULL || namePattern == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the target type pattern and name pattern must be "
                "specified");
        goto CommonExit;
    }

    input = (XcalarApiListExportTargetsInput *) memCallocExt(1,
                                                             sizeof(*input),
                                                             moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->targetTypePattern,
            typePattern,
            sizeof(input->targetTypePattern));
    strlcpy(input->targetNamePattern,
            namePattern,
            sizeof(input->targetNamePattern));

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListExportTargets,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeExportWorkItem(const char *tableName,
                            const char *exportName,
                            int numColumns,
                            const ExColumnName *columns,
                            const char *driverName,
                            const char *driverParams,
                            const char *sessionName)
{
    XcalarApiExportInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    int ii = 0;
    bool error = true;
    size_t inputSize;

    if (tableName == NULL) {
        xSyslog(moduleName, XlogErr, "Export table name must be specified");
        goto CommonExit;
    }

    if (!(numColumns > 0 && numColumns <= TupleMaxNumValuesPerRecord)) {
        xSyslog(moduleName,
                XlogErr,
                "Number of columns must be between %i and %i. %i specified.",
                0,
                TupleMaxNumValuesPerRecord,
                numColumns);
        goto CommonExit;
    }

    inputSize = sizeof(*input) + numColumns * sizeof(input->meta.columns[0]);

    input = (XcalarApiExportInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    // Copy fields into the now-allocated input struct
    strlcpy(input->srcTable.tableName,
            tableName,
            sizeof(input->srcTable.tableName));

    if (exportName != NULL) {
        strlcpy(input->exportName, exportName, sizeof(input->exportName));
    } else {
        input->exportName[0] = '\0';
    }

    // Meta
    // Note that this will work only as long as specInput and target has no
    // variable sized structures like char[0], and copies all buffers
    input->meta.numColumns = numColumns;
    for (ii = 0; ii < numColumns; ii++) {
        strlcpy(input->meta.columns[ii].name,
                columns[ii].name,
                sizeof(input->meta.columns[ii].name));
        strlcpy(input->meta.columns[ii].headerAlias,
                columns[ii].headerAlias,
                sizeof(input->meta.columns[ii].headerAlias));
    }

    // Driver info to be handled by user driver
    strlcpy(input->meta.driverName, driverName, sizeof(input->meta.driverName));
    strlcpy(input->meta.driverParams,
            driverParams,
            sizeof(input->meta.driverParams));

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiExport, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeAggregateWorkItem(const char *srcTableName,
                               const char *dstTableName,
                               const char *evalStr,
                               const char *sessionName)
{
    XcalarApiAggregateInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (srcTableName == NULL || evalStr == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the table name and eval string must be specified");
        goto CommonExit;
    }

    input =
        (XcalarApiAggregateInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));
    strlcpy(input->evalStr, evalStr, sizeof(input->evalStr));

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiAggregate, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSynthesizeWorkItem(const char *sourceName,
                                const char *dstTableName,
                                bool sameSession,
                                unsigned numColumns,
                                XcalarApiRenameMap *columns,
                                const char *sessionName)
{
    XcalarApiSynthesizeInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = numColumns * (sizeof(*columns)) + sizeof(*input);

    input = (XcalarApiSynthesizeInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    input->source.xid = XidInvalid;
    strlcpy(input->source.name, sourceName, sizeof(input->source.name));

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    input->columnsCount = numColumns;
    memcpy(input->columns, columns, numColumns * sizeof(*columns));

    input->aggResult = NULL;
    input->sameSession = sameSession;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiSynthesize, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSelectWorkItem(const char *srcTableName,
                            const char *dstTableName,
                            int64_t minBatchId,
                            int64_t maxBatchId,
                            const char *filterString,
                            unsigned numColumns,
                            XcalarApiRenameMap *columns,
                            uint64_t limitRows,
                            const char *evalJsonStr,
                            const char *joinTableName,
                            bool createIndex,
                            const char *sessionName)
{
    XcalarApiSelectInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;
    size_t evalJsonLen = 0;

    if (evalJsonStr) {
        evalJsonLen = strlen(evalJsonStr) + 1;
    }

    inputSize = sizeof(*input) + evalJsonLen;

    input = (XcalarApiSelectInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    if (filterString != NULL) {
        strlcpy(input->filterString, filterString, sizeof(input->filterString));
    } else {
        input->filterString[0] = '\0';
    }

    if (joinTableName) {
        strlcpy(input->joinTable.tableName,
                joinTableName,
                sizeof(input->joinTable.tableName));
    } else {
        input->joinTable.tableName[0] = '\0';
    }

    input->minBatchId = minBatchId;
    input->maxBatchId = maxBatchId;

    input->evalInputCount = evalJsonLen;
    if (evalJsonStr) {
        memcpy(input->evalInput, evalJsonStr, evalJsonLen);
    }

    input->createIndex = createIndex;
    input->limitRows = limitRows;
    input->numColumns = numColumns;
    memcpy(input->columns, columns, numColumns * sizeof(*columns));

    // XXX TODO ENG-743 Added to track down invalid batch Ids
    xSyslog(moduleName,
            XlogInfo,
            "xcalarApiMakeSelectWorkItem: srcTableName: '%s', dstTableName: "
            "'%s', minBatchId: %ld, maxBatchId: %ld, numColumns: %u, "
            "sessionName: '%s'",
            input->srcTable.tableName,
            input->dstTable.tableName,
            input->minBatchId,
            input->maxBatchId,
            input->numColumns,
            sessionName);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSelect, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem
    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakePublishWorkItem(const char *srcTableName,
                             const char *dstTableName,
                             time_t time,
                             bool dropSrc,
                             const char *sessionName)
{
    XcalarApiPublishInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input);

    input = (XcalarApiPublishInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    strlcpy(input->dstTable.tableName,
            dstTableName,
            sizeof(input->dstTable.tableName));

    input->unixTS = time;
    input->dropSrc = dropSrc;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiPublish, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeRestoreTableWorkItem(const char *publishedTableName)
{
    XcalarApiRestoreTableInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input);

    input =
        (XcalarApiRestoreTableInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->name, publishedTableName, sizeof(input->name));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiRestoreTable, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUpdateWorkItem(unsigned numUpdates,
                            const char **srcTableNames,
                            const char **dstTableNames,
                            time_t *times,
                            bool dropSrc,
                            const char *sessionName)
{
    XcalarApiUpdateInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input) + numUpdates * sizeof(*input->updates);

    input = (XcalarApiUpdateInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    for (unsigned ii = 0; ii < numUpdates; ii++) {
        strlcpy(input->updates[ii].srcTable.tableName,
                srcTableNames[ii],
                sizeof(input->updates[ii].srcTable.tableName));

        strlcpy(input->updates[ii].dstTable.tableName,
                dstTableNames[ii],
                sizeof(input->updates[ii].dstTable.tableName));

        input->updates[ii].unixTS = times[ii];
        input->updates[ii].dropSrc = dropSrc;
    }
    input->numUpdates = numUpdates;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiUpdate, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUnpublishWorkItem(const char *srcTableName, bool inactivate)
{
    XcalarApiUnpublishInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input);

    input = (XcalarApiUnpublishInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));
    input->inactivateOnly = inactivate;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiUnpublish, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeCoalesceWorkItem(const char *srcTableName)
{
    XcalarApiCoalesceInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input);

    input = (XcalarApiCoalesceInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiCoalesce, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeMapWorkItem(const char *srcTableName,
                         const char *dstTableName,
                         bool icvMode,
                         unsigned numEvals,
                         const char *evalStr[],
                         const char *newFieldNames[],
                         const char *sessionName)
{
    XcalarApiMapInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;
    size_t stringsSize;

    if (srcTableName == NULL || evalStr == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the table name and eval string must be specified");
        goto CommonExit;
    }

    stringsSize = numEvals * (XcalarApiMaxEvalStringLen + 1 +
                              XcalarApiMaxFieldNameLen + 1);
    inputSize = stringsSize + sizeof(*input);

    input = (XcalarApiMapInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    input->icvMode = icvMode;
    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    input->numEvals = numEvals;
    input->stringsCount = stringsSize;

    char *cursor;
    cursor = (char *) input->strings;
    for (unsigned ii = 0; ii < numEvals; ii++) {
        input->evalStrs[ii] = cursor;
        strlcpy(input->evalStrs[ii],
                evalStr[ii],
                XcalarApiMaxEvalStringLen + 1);
        cursor += XcalarApiMaxEvalStringLen + 1;

        input->newFieldNames[ii] = cursor;
        strlcpy(input->newFieldNames[ii],
                newFieldNames[ii],
                XcalarApiMaxFieldNameLen + 1);

        cursor += XcalarApiMaxFieldNameLen + 1;
    }

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    xcalarApiSerializeMapInput(input);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiMap, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

void
xcalarApiSerializeMapInput(XcalarApiMapInput *mapInput)
{
    for (unsigned ii = 0; ii < mapInput->numEvals; ii++) {
        mapInput->evalStrs[ii] = (char *) XcalarApiMagic;
        mapInput->newFieldNames[ii] = (char *) XcalarApiMagic;
    }
}

size_t
xcalarApiDeserializeMapInput(XcalarApiMapInput *mapInput)
{
    char *cursor;
    cursor = (char *) mapInput->strings;

    for (unsigned ii = 0; ii < mapInput->numEvals; ii++) {
        mapInput->evalStrs[ii] = cursor;
        cursor += XcalarApiMaxEvalStringLen + 1;

        mapInput->newFieldNames[ii] = cursor;
        cursor += XcalarApiMaxFieldNameLen + 1;
    }

    return (size_t) cursor - (size_t) mapInput;
}

char *
xcalarApiDeserializeUnionInputL1(XcalarApiUnionInput *unionInput)
{
    char *cursor;
    unsigned numSrcTables = unionInput->numSrcTables;
    cursor = (char *) unionInput->buf;

    unionInput->srcTables = (XcalarApiTableInput *) cursor;
    cursor += sizeof(*unionInput->srcTables) * numSrcTables;

    unionInput->renameMapSizes = (unsigned *) cursor;
    cursor += sizeof(*unionInput->renameMapSizes) * numSrcTables;

    unionInput->renameMap = (XcalarApiRenameMap **) cursor;
    cursor += sizeof(*unionInput->renameMap) * numSrcTables;

    return cursor;
}

char *
xcalarApiDeserializeUnionInputL2(XcalarApiUnionInput *unionInput, char *cursor)
{
    unsigned numSrcTables = unionInput->numSrcTables;

    for (unsigned ii = 0; ii < numSrcTables; ii++) {
        unionInput->renameMap[ii] = (XcalarApiRenameMap *) cursor;
        cursor +=
            sizeof(*unionInput->renameMap[ii]) * unionInput->renameMapSizes[ii];
    }

    return cursor;
}

size_t
xcalarApiDeserializeUnionInput(XcalarApiUnionInput *unionInput)
{
    char *cursor = xcalarApiDeserializeUnionInputL1(unionInput);
    cursor = xcalarApiDeserializeUnionInputL2(unionInput, cursor);

    return (size_t) cursor - (size_t) unionInput;
}

void
xcalarApiSerializeGroupByInput(XcalarApiGroupByInput *groupByInput)
{
    for (unsigned ii = 0; ii < groupByInput->numEvals; ii++) {
        groupByInput->evalStrs[ii] = (char *) XcalarApiMagic;
        groupByInput->newFieldNames[ii] = (char *) XcalarApiMagic;
    }
}

size_t
xcalarApiDeserializeGroupByInput(XcalarApiGroupByInput *groupByInput)
{
    char *cursor;
    cursor = (char *) groupByInput->strings;

    for (unsigned ii = 0; ii < groupByInput->numEvals; ii++) {
        groupByInput->evalStrs[ii] = cursor;
        cursor += XcalarApiMaxEvalStringLen + 1;

        groupByInput->newFieldNames[ii] = cursor;
        cursor += XcalarApiMaxFieldNameLen + 1;
    }

    return (size_t) cursor - (size_t) groupByInput;
}

XcalarWorkItem *
xcalarApiMakeGetVersionWorkItem()
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetVersion, NULL, 0);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListDagNodesWorkItem(const char *namePattern,
                                  SourceType srcType,
                                  const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiDagNodeNamePatternInput *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    inputSize = sizeof(*input) + strlen(namePattern) + 1;

    input = (XcalarApiDagNodeNamePatternInput *) memCallocExt(1,
                                                              inputSize,
                                                              moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->srcType = srcType;
    strlcpy(input->namePattern, namePattern, inputSize);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDagNodeInfo,
                                            input,
                                            inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetRowNumWorkItem(const char *srcTableName,
                               const char *dstTableName,
                               const char *newFieldName,
                               const char *sessionName)
{
    XcalarApiGetRowNumInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    unsigned ret;

    if (srcTableName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of table must be specified");
        goto CommonExit;
    }

    input =
        (XcalarApiGetRowNumInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    ret = strlcpy(input->srcTable.tableName,
                  srcTableName,
                  sizeof(input->srcTable.tableName));
    if (ret > sizeof(input->srcTable.tableName)) {
        goto CommonExit;
    }

    ret =
        strlcpy(input->newFieldName, newFieldName, sizeof(input->newFieldName));
    if (ret > sizeof(input->newFieldName)) {
        goto CommonExit;
    }

    if (dstTableName != NULL) {
        ret = strlcpy(input->dstTable.tableName,
                      dstTableName,
                      sizeof(input->dstTable.tableName));
        if (ret > sizeof(input->dstTable.tableName)) {
            goto CommonExit;
        }
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiGetRowNum, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDeleteDagNodesWorkItem(const char *namePattern,
                                    SourceType srcType,
                                    const char *sessionName,
                                    bool deleteCompletely)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiDagNodeNamePatternInput *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    if (namePattern == NULL || namePattern[0] == '\0') {
        xSyslog(moduleName, XlogErr, "Name pattern must be specified");
        goto CommonExit;
    }

    inputSize = sizeof(*input) + strlen(namePattern) + 1;

    input = (XcalarApiDagNodeNamePatternInput *) memCallocExt(1,
                                                              inputSize,
                                                              moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->srcType = srcType;
    input->deleteCompletely = deleteCompletely;
    strlcpy(input->namePattern, namePattern, inputSize);

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiDeleteObjects, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeArchiveTablesWorkItem(bool archive,
                                   bool allTables,
                                   unsigned numTables,
                                   const char **tableNames,
                                   const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiArchiveTablesInput *input = NULL;
    size_t inputSize = 0;
    size_t bufCount = 0;
    bool error = true;

    // calculate the total string size
    for (unsigned ii = 0; ii < numTables; ii++) {
        // include null terminator
        bufCount += strlen(tableNames[ii]) + 1;
    }

    inputSize = sizeof(*input) + bufCount;

    input =
        (XcalarApiArchiveTablesInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, inputSize);

    input->archive = archive;
    input->allTables = allTables;
    input->numTables = numTables;

    input->tableNamesBufCount = bufCount;

    size_t bufLen;
    bufLen = 0;

    // copy names into buf delimited by null terminators
    // we've allocated exactly enough memory to hold the strings
    for (unsigned ii = 0; ii < numTables; ii++) {
        strcpy(&input->tableNamesBuf[bufLen], tableNames[ii]);

        bufLen += strlen(tableNames[ii]) + 1;
        assert(bufLen <= bufCount);
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiArchiveTables, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakePerNodeOpStatsWorkItem(const char *tableName,
                                    const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    char *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    inputSize = strlen(tableName) + 1;
    input = (char *) memCallocExt(1, inputSize, moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input, tableName, inputSize);
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetPerNodeOpStats,
                                            input,
                                            inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeOpStatsWorkItem(const char *tableName, const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    char *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    inputSize = strlen(tableName) + 1;
    input = (char *) memCallocExt(1, inputSize, moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input, tableName, inputSize);
    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiGetOpStats, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeCancelOpWorkItem(const char *tableName, const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    char *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    inputSize = strlen(tableName) + 1;
    input = (char *) memCallocExt(1, inputSize, moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input, tableName, inputSize);
    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiCancelOp, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDagWorkItem(const char *tableName, const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    char *input = NULL;
    size_t inputSize = 0;
    bool error = true;

    inputSize = strlen(tableName) + 1;
    input = (char *) memCallocExt(1, inputSize, moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input, tableName, inputSize);
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetDag, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeQueryStateWorkItem(const char *queryName, bool detailedStats)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiQueryNameInput *input = NULL;
    bool error = true;

    input =
        (XcalarApiQueryNameInput *) memCallocExt(1, sizeof(*input), moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->queryName, queryName, sizeof(input->queryName));
    input->detailedStats = detailedStats;
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiQueryState,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeQueryCancelWorkItem(const char *queryName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiQueryNameInput *input = NULL;
    bool error = true;

    input =
        (XcalarApiQueryNameInput *) memCallocExt(1, sizeof(*input), moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->queryName, queryName, sizeof(input->queryName));
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiQueryCancel,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeQueryDeleteWorkItem(const char *queryName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiQueryNameInput *input = NULL;
    bool error = true;

    input =
        (XcalarApiQueryNameInput *) memCallocExt(1, sizeof(*input), moduleName);

    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->queryName, queryName, sizeof(input->queryName));
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiQueryDelete,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeShutdownWorkItem(bool local, bool force)
{
    XcalarWorkItem *workItem = NULL;
    bool *input = NULL;
    bool error = true;

    input = (bool *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    *input = force;
    if (local) {
        workItem = xcalarApiMakeGenericWorkItem(XcalarApiShutdownLocal,
                                                input,
                                                sizeof(*input));
    } else {
        workItem = xcalarApiMakeGenericWorkItem(XcalarApiShutdown,
                                                input,
                                                sizeof(*input));
    }

    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeFreeResultSetWorkItem(uint64_t resultSetId)
{
    XcalarApiFreeResultSetInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    input = (XcalarApiFreeResultSetInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->resultSetId = resultSetId;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiFreeResultSet,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeResultSetAbsoluteWorkItem(uint64_t resultSetId, uint64_t position)
{
    XcalarApiResultSetAbsoluteInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    input = (XcalarApiResultSetAbsoluteInput *) memCallocExt(1,
                                                             sizeof(*input),
                                                             moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->resultSetId = resultSetId;
    input->position = position;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiResultSetAbsolute,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeRenameNodeWorkItem(const char *oldName,
                                const char *newName,
                                const char *sessionName)
{
    XcalarApiRenameNodeInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (oldName == NULL || newName == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the old name and new name must be specified");
        goto CommonExit;
    }

    input = (XcalarApiRenameNodeInput *) memCallocExt(1,
                                                      sizeof(*input),
                                                      moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->oldName, oldName, sizeof(input->oldName));
    strlcpy(input->newName, newName, sizeof(input->newName));

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiRenameNode,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeTagDagNodesWorkItem(unsigned numNodes,
                                 DagTypes::NamedInput *dagNodes,
                                 const char *tag,
                                 const char *sessionName)
{
    XcalarApiTagDagNodesInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize = sizeof(*input) + numNodes * sizeof(*dagNodes);

    input =
        (XcalarApiTagDagNodesInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeNamesCount = numNodes;
    strlcpy(input->tag, tag, sizeof(input->tag));
    memcpy(input->nodeNames, dagNodes, sizeof(*dagNodes) * numNodes);

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiTagDagNodes, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeCommentDagNodesWorkItem(
    unsigned numNodes,
    char dagNodeNames[0][XcalarApiMaxTableNameLen],
    const char *comment,
    const char *sessionName)
{
    XcalarApiCommentDagNodesInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize = sizeof(*input) + numNodes * XcalarApiMaxTableNameLen;

    input = (XcalarApiCommentDagNodesInput *) memCallocExt(1,
                                                           inputSize,
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeNamesCount = numNodes;
    strlcpy(input->comment, comment, sizeof(input->comment));

    for (unsigned ii = 0; ii < numNodes; ii++) {
        strlcpy(input->nodeNames[ii],
                dagNodeNames[ii],
                XcalarApiMaxTableNameLen);
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiCommentDagNodes,
                                            input,
                                            inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGroupByWorkItem(const char *tableName,
                             const char *groupByTableName,
                             unsigned numKeys,
                             XcalarApiKeyInput *keys,
                             bool includeSrcTableSample,
                             bool icvMode,
                             bool groupAll,
                             unsigned numEvals,
                             const char *evalStr[],
                             const char *newFieldNames[],
                             const char *sessionName)
{
    XcalarApiGroupByInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;
    size_t stringsSize;

    if (tableName == NULL || evalStr == NULL || newFieldNames == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Must specify name of table, eval string, and name of new "
                "field");
        goto CommonExit;
    }

    stringsSize = numEvals * (XcalarApiMaxEvalStringLen + 1 +
                              XcalarApiMaxFieldNameLen + 1);
    inputSize = stringsSize + sizeof(*input);

    input = (XcalarApiGroupByInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    memZero(input, inputSize);

    strlcpy(input->srcTable.tableName,
            tableName,
            sizeof(input->srcTable.tableName));

    input->numEvals = numEvals;
    input->stringsCount = stringsSize;

    char *cursor;
    cursor = (char *) input->strings;
    for (unsigned ii = 0; ii < numEvals; ii++) {
        input->evalStrs[ii] = cursor;
        strlcpy(input->evalStrs[ii],
                evalStr[ii],
                XcalarApiMaxEvalStringLen + 1);
        cursor += XcalarApiMaxEvalStringLen + 1;

        input->newFieldNames[ii] = cursor;
        strlcpy(input->newFieldNames[ii],
                newFieldNames[ii],
                XcalarApiMaxFieldNameLen + 1);

        cursor += XcalarApiMaxFieldNameLen + 1;
    }

    input->includeSrcTableSample = includeSrcTableSample;
    input->icvMode = icvMode;
    input->groupAll = groupAll;

    if (groupByTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                groupByTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    input->numKeys = numKeys;
    if (numKeys > 0) {
        memcpy(input->keys, keys, sizeof(*keys) * numKeys);
    }

    xcalarApiSerializeGroupByInput(input);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGroupBy, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeProjectWorkItem(int numColumns,
                             char (*columns)[XcalarApiMaxFieldNameLen + 1],
                             const char *srcTableName,
                             const char *dstTableName,
                             const char *sessionName)
{
    XcalarApiProjectInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    if (srcTableName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of table must be specified");
        goto CommonExit;
    }

    inputSize = sizeof(*input) + numColumns * sizeof(input->columnNames[0]);

    input = (XcalarApiProjectInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->numColumns = numColumns;
    for (int ii = 0; ii < numColumns; ii++) {
        strlcpy(input->columnNames[ii], columns[ii], sizeof(columns[ii]));
    }

    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiProject, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeFilterWorkItem(const char *filterStr,
                            const char *srcTableName,
                            const char *dstTableName,
                            const char *sessionName)
{
    XcalarApiFilterInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (srcTableName == NULL || filterStr == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the table name and filter string must be specified");
        goto CommonExit;
    }

    input =
        (XcalarApiFilterInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->filterStr, filterStr, sizeof(input->filterStr));
    strlcpy(input->srcTable.tableName,
            srcTableName,
            sizeof(input->srcTable.tableName));

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiFilter, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeJoinWorkItem(const char *leftTableName,
                          const char *rightTableName,
                          const char *joinTableName,
                          JoinOperator joinType,
                          bool collisionCheck,
                          bool keepAllColumns,
                          bool nullSafe,
                          unsigned numLeftColumns,
                          unsigned numRightColumns,
                          XcalarApiRenameMap *renameMap,
                          const char *filterString,
                          const char *sessionName)
{
    XcalarApiJoinInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    if (leftTableName == NULL || rightTableName == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the left and right table names must be specified");
        goto CommonExit;
    }

    inputSize = sizeof(*input) + (numLeftColumns + numRightColumns) *
                                     sizeof(input->renameMap[0]);

    input = (XcalarApiJoinInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->leftTable.tableName,
            leftTableName,
            sizeof(input->leftTable.tableName));
    strlcpy(input->rightTable.tableName,
            rightTableName,
            sizeof(input->rightTable.tableName));

    if (joinTableName != NULL) {
        strlcpy(input->joinTable.tableName,
                joinTableName,
                sizeof(input->joinTable.tableName));
    } else {
        input->joinTable.tableName[0] = '\0';
    }

    if (filterString != NULL) {
        strlcpy(input->filterString, filterString, sizeof(input->filterString));
    } else {
        input->filterString[0] = '\0';
    }

    input->joinType = joinType;
    input->numLeftColumns = numLeftColumns;
    input->numRightColumns = numRightColumns;
    input->collisionCheck = collisionCheck;
    input->keepAllColumns = keepAllColumns;
    input->nullSafe = nullSafe;

    if (renameMap != NULL) {
        for (unsigned ii = 0; ii < numLeftColumns + numRightColumns; ii++) {
            memcpy(&input->renameMap[ii],
                   &renameMap[ii],
                   sizeof(input->renameMap[ii]));
        }
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiJoin, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUnionWorkItem(unsigned numSrcTables,
                           const char **srcTables,
                           const char *dstTableName,
                           unsigned *renameMapSizes,
                           XcalarApiRenameMap **renameMap,
                           bool dedup,
                           UnionOperator unionType,
                           const char *sessionName)
{
    XcalarApiUnionInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t inputSize;

    inputSize = sizeof(*input);

    for (unsigned ii = 0; ii < numSrcTables; ii++) {
        inputSize += sizeof(*input->srcTables);
        inputSize += sizeof(*input->renameMapSizes);
        inputSize += sizeof(*input->renameMap);

        inputSize += sizeof(*input->renameMap[ii]) * renameMapSizes[ii];
    }

    input = (XcalarApiUnionInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->numSrcTables = numSrcTables;
    input->dedup = dedup;
    input->unionType = unionType;

    uint8_t *cursor;
    cursor = input->buf;
    input->srcTables = (XcalarApiTableInput *) cursor;
    cursor += sizeof(*input->srcTables) * numSrcTables;

    input->renameMapSizes = (unsigned *) cursor;
    cursor += sizeof(*input->renameMapSizes) * numSrcTables;

    input->renameMap = (XcalarApiRenameMap **) cursor;
    cursor += sizeof(*input->renameMap) * numSrcTables;

    for (unsigned ii = 0; ii < numSrcTables; ii++) {
        strlcpy(input->srcTables[ii].tableName,
                srcTables[ii],
                sizeof(input->srcTables[ii].tableName));
        input->renameMapSizes[ii] = renameMapSizes[ii];

        input->renameMap[ii] = (XcalarApiRenameMap *) cursor;
        cursor += sizeof(*input->renameMap[ii]) * renameMapSizes[ii];

        for (unsigned jj = 0; jj < renameMapSizes[ii]; jj++) {
            input->renameMap[ii][jj] = renameMap[ii][jj];
        }
    }

    input->bufCount = (uintptr_t) cursor - (uintptr_t) input->buf;

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiUnion, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeBulkLoadWorkItem(const char *name,
                              const DfLoadArgs *loadArgs,
                              const char *sessionName)
{
    XcalarApiBulkLoadInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (name != NULL && strlen(name) >= sizeof(input->datasetName)) {
        xSyslog(moduleName, XlogErr, "Dataset name, '%s', too long", name);
        goto CommonExit;
    }

    input =
        (XcalarApiBulkLoadInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, sizeof(*input));

    if (name != NULL) {
        verify(strlcpy(input->datasetName, name, sizeof(input->datasetName)) <
               sizeof(input->datasetName));
    }

    input->loadArgs = *loadArgs;

    memZero(&input->loadArgs.xdbLoadArgs, sizeof(input->loadArgs.xdbLoadArgs));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiBulkLoad, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDatasetCreateWorkItem(const char *datasetName,
                                   const DfLoadArgs *loadArgs,
                                   const char *sessionName)
{
    XcalarWorkItem *workItem;

    workItem =
        xcalarApiMakeBulkLoadWorkItem(datasetName, loadArgs, sessionName);

    if (workItem != NULL) {
        // Dataset Create leverages the Bulk Load work item
        workItem->api = XcalarApiDatasetCreate;
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeIndexWorkItem(const char *srcDatasetName,
                           const char *srcTableName,
                           unsigned numKeys,
                           const char **keyName,
                           const char **keyFieldName,
                           DfFieldType *keyType,
                           Ordering *keyOrdering,
                           const char *dstTableName,
                           const char *dhtName,
                           const char *fatptrPrefixName,
                           bool delaySort,
                           bool broadcast,
                           const char *sessionName)

{
    XcalarApiIndexInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    input = (XcalarApiIndexInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, sizeof(*input));
    assert(numKeys > 0);
    for (unsigned ii = 0; ii < numKeys; ii++) {
        strlcpy(input->keys[ii].keyName,
                keyName[ii],
                sizeof(input->keys[ii].keyName));

        if (keyFieldName) {
            strlcpy(input->keys[ii].keyFieldName,
                    keyFieldName[ii],
                    sizeof(input->keys[ii].keyFieldName));
        } else {
            input->keys[ii].keyFieldName[0] = '\0';
        }

        if (keyType) {
            input->keys[ii].type = keyType[ii];
        } else {
            input->keys[ii].type = DfUnknown;
        }

        if (keyOrdering) {
            assert(xcalar::internal::durable::dag::Ordering_IsValid(
                keyOrdering[ii]));
            input->keys[ii].ordering = keyOrdering[ii];
        } else {
            input->keys[ii].ordering = Unordered;
        }
    }

    if (srcTableName != NULL) {
        input->source.isTable = true;
        strlcpy(input->source.name, srcTableName, sizeof(input->source.name));
    } else {
        input->source.isTable = false;
        strlcpy(input->source.name, srcDatasetName, sizeof(input->source.name));
    }

    if (dstTableName != NULL) {
        strlcpy(input->dstTable.tableName,
                dstTableName,
                sizeof(input->dstTable.tableName));
    } else {
        input->dstTable.tableName[0] = '\0';
    }
    if (dhtName != NULL) {
        strlcpy(input->dhtName, dhtName, sizeof(input->dhtName));
    } else {
        input->dhtName[0] = '\0';
    }

    if (fatptrPrefixName != NULL) {
        strlcpy(input->fatptrPrefixName,
                fatptrPrefixName,
                sizeof(input->fatptrPrefixName));
    } else {
        assert(input->source.isTable &&
               "must supply prefix when indexing dataset");
        input->fatptrPrefixName[0] = '\0';
    }

    input->delaySort = delaySort;
    input->broadcast = broadcast;
    input->numKeys = numKeys;
    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiIndex, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  //  Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetTableMeta(const char *objectName,
                          bool isTable,
                          bool isPrecise,
                          const char *sessionName)
{
    XcalarApiGetTableMetaInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (objectName == NULL || objectName[0] == '\0') {
        goto CommonExit;
    }

    input = (XcalarApiGetTableMetaInput *) memCallocExt(1,
                                                        sizeof(*input),
                                                        moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->tableNameInput.isTable = isTable;
    input->isPrecise = isPrecise;
    if (strlcpy(input->tableNameInput.name,
                objectName,
                sizeof(input->tableNameInput.name)) >=
        sizeof(input->tableNameInput.name)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetTableMeta,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeResetStatWorkItem(int64_t nodeId)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiStatInput *input = NULL;
    bool error = true;

    input = (XcalarApiStatInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeId = nodeId;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiResetStat, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;
CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetStatWorkItem(int64_t nodeId)
{
    XcalarApiStatInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    input = (XcalarApiStatInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeId = nodeId;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiGetStat, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetStatGroupIdMapWorkItem(int64_t nodeId)
{
    XcalarApiStatInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    input = (XcalarApiStatInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeId = nodeId;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetStatGroupIdMap,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeResultSetNextWorkItem(uint64_t resultSetId,
                                   unsigned numRecords,
                                   const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiResultSetNextInput *input = NULL;
    bool error = true;

    input = (XcalarApiResultSetNextInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->resultSetId = resultSetId;
    input->numRecords = numRecords;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiResultSetNext,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeResultSetWorkItem(const char *dagNodeName,
                               bool errorDs,
                               const char *sessionName)
{
    XcalarApiMakeResultSetInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (dagNodeName == NULL || dagNodeName[0] == '\0') {
        xSyslog(moduleName, XlogErr, "Name of the dag node must be specified");
        goto CommonExit;
    }

    input = (XcalarApiMakeResultSetInput *) memCalloc(1, sizeof(*input));
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->dagNode.name, dagNodeName, sizeof(input->dagNode.name));
    input->errorDs = errorDs;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiMakeResultSet,
                                            input,
                                            sizeof(*input));

    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUdfAddUpdateWorkItem(XcalarApis api,
                                  UdfType type,
                                  const char *moduleName,
                                  const char *source,
                                  const char *sessionName)
{
    UdfModuleSrc *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    size_t sourceSize;
    size_t inputSize;

    if (moduleName == NULL || source == NULL || type != UdfTypePython ||
        (strlen(moduleName) + 1 > sizeof(input->moduleName))) {
        goto CommonExit;
    }

    sourceSize = strlen(source) + 1;
    inputSize = sizeof(*input) + sourceSize;
    input = (UdfModuleSrc *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->isBuiltin = false;  // Built-in modules must be loaded at start.
    input->modulePath[0] = '\0';
    input->type = type;
    input->sourceSize = sourceSize;
    strlcpy(input->moduleName, moduleName, sizeof(input->moduleName));
    strlcpy(input->source, source, sourceSize);

    workItem = xcalarApiMakeGenericWorkItem(api, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUdfGetWorkItem(const char *moduleName)
{
    XcalarApiUdfGetInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (moduleName == NULL ||
        (strlen(moduleName) + 1 > sizeof(input->moduleName))) {
        goto CommonExit;
    }

    input =
        (XcalarApiUdfGetInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->moduleName, moduleName, sizeof(input->moduleName));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiUdfGet, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeUdfDeleteWorkItem(const char *moduleName, const char *sessionName)
{
    XcalarApiUdfDeleteInput *input = NULL;
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    if (moduleName == NULL ||
        (strlen(moduleName) + 1 > sizeof(input->moduleName))) {
        goto CommonExit;
    }

    input =
        (XcalarApiUdfDeleteInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    strlcpy(input->moduleName, moduleName, sizeof(input->moduleName));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiUdfDelete, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    if (!allocSessionInfo(sessionName, workItem)) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListFilesWorkItem(const char *targetName,
                               const char *path,
                               const char *fileNamePattern,
                               bool recursive)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListFilesInput *input = NULL;
    bool error = true;

    input =
        (XcalarApiListFilesInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->sourceArgs.targetName,
                targetName,
                sizeof(input->sourceArgs.targetName)) >=
        sizeof(input->sourceArgs.targetName)) {
        goto CommonExit;
    }

    if (strlcpy(input->sourceArgs.path, path, sizeof(input->sourceArgs.path)) >=
        sizeof(input->sourceArgs.path)) {
        goto CommonExit;
    }

    if (strlcpy(input->sourceArgs.fileNamePattern,
                fileNamePattern,
                sizeof(input->sourceArgs.fileNamePattern)) >=
        sizeof(input->sourceArgs.fileNamePattern)) {
        goto CommonExit;
    }

    input->sourceArgs.recursive = recursive;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiListFiles, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeTargetWorkItem(const char *inputJson)
{
    // This is to just to prevent anything terrible from happening
    size_t maxInputLen = 100 * KB;
    XcalarWorkItem *workItem = NULL;
    XcalarApiTargetInput *input = NULL;
    size_t inputSize;
    bool error = true;

    size_t inputStrLen = strnlen(inputJson, maxInputLen);
    if (inputStrLen == maxInputLen) {
        xSyslog(moduleName,
                XlogErr,
                "Target input json is longer than max %zu",
                maxInputLen);
        goto CommonExit;
    }

    inputSize = sizeof(*input) + inputStrLen + 1;

    input = (XcalarApiTargetInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    assert(input != NULL);

    verify(strlcpy(input->inputJson, inputJson, inputStrLen + 1) ==
           inputStrLen);
    input->inputLen = inputStrLen + 1;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiTarget, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakePreviewWorkItem(const char *inputJson)
{
    // This is to just to prevent anything terrible from happening
    size_t maxInputLen = 100 * KB;
    XcalarWorkItem *workItem = NULL;
    XcalarApiPreviewInput *input = NULL;
    size_t inputSize;
    bool error = true;

    size_t inputStrLen = strnlen(inputJson, maxInputLen);
    if (inputStrLen == maxInputLen) {
        xSyslog(moduleName,
                XlogErr,
                "Preview input json is longer than max %zu",
                maxInputLen);
        goto CommonExit;
    }

    inputSize = sizeof(*input) + inputStrLen + 1;

    input = (XcalarApiPreviewInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    verify(strlcpy(input->inputJson, inputJson, inputStrLen + 1) ==
           inputStrLen);
    input->inputLen = inputStrLen + 1;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiPreview, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionListWorkItem(const char *sessionNamePattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionListInput *input = NULL;

    if (sessionNamePattern == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Pattern for session names must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionListInput *) memCallocExt(1,
                                                       sizeof(*input),
                                                       moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    memZero(input, sizeof(*input));
    strlcpy(input->pattern, sessionNamePattern, sizeof(input->pattern));

    input->patternLength = strlen(sessionNamePattern);
    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionList,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

static XcalarWorkItem *
makeSessionDeleteWorkItem(XcalarApis api, const char *sessionNamePattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionDeleteInput *input = NULL;

    if (sessionNamePattern == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Pattern for session names must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionDeleteInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    memZero(input, sizeof(*input));

    strlcpy(input->sessionName, sessionNamePattern, sizeof(input->sessionName));

    input->sessionNameLength = strlen(sessionNamePattern);

    workItem = xcalarApiMakeGenericWorkItem(api, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionDeleteWorkItem(const char *sessionNamePattern)
{
    return makeSessionDeleteWorkItem(XcalarApiSessionDelete,
                                     sessionNamePattern);
}

XcalarWorkItem *
xcalarApiMakeSessionInactWorkItem(const char *sessionNamePattern)
{
    return makeSessionDeleteWorkItem(XcalarApiSessionInact, sessionNamePattern);
}

XcalarWorkItem *
xcalarApiMakeSessionActivateWorkItem(const char *sessionName)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionActivateInput *input = NULL;

    if (sessionName == NULL) {
        xSyslog(moduleName, XlogErr, "The session name must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionActivateInput *) memCallocExt(1,
                                                           sizeof(*input),
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    memZero(input, sizeof(*input));

    strlcpy(input->sessionName, sessionName, sizeof(input->sessionName));

    input->sessionNameLength = strlen(sessionName);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionActivate,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionRenameWorkItem(const char *sessionName,
                                   const char *origSessionName)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionRenameInput *input = NULL;

    if (origSessionName == NULL || sessionName == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Both the old and new sessions names must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionRenameInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    memZero(input, sizeof(*input));

    strlcpy(input->sessionName, sessionName, sizeof(input->sessionName));
    strlcpy(input->origSessionName,
            origSessionName,
            sizeof(input->origSessionName));

    input->sessionNameLength = strlen(sessionName);
    input->origSessionNameLength = strlen(origSessionName);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionRename,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionPersistWorkItem(const char *sessionNamePattern)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionDeleteInput *input = NULL;

    if (sessionNamePattern == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Pattern for session names must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionDeleteInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, sizeof(*input));

    strlcpy(input->sessionName, sessionNamePattern, sizeof(input->sessionName));

    input->sessionNameLength = strlen(sessionNamePattern);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionPersist,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionNewWorkItem(const char *sessionName,
                                bool fork,
                                const char *forkedSessionName)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;
    XcalarApiSessionNewInput *input = NULL;

    if (sessionName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of session must be specified");
        goto CommonExit;
    }

    if (fork) {
        if (forkedSessionName == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Name of the forked session must be specified");
            goto CommonExit;
        }
    }

    input = (XcalarApiSessionNewInput *) memCallocExt(1,
                                                      sizeof(*input),
                                                      moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    memZero(input, sizeof(*input));

    strlcpy(input->sessionName, sessionName, sizeof(input->sessionName));
    input->sessionNameLength = strlen(sessionName);
    if (fork) {
        strlcpy(input->forkedSessionName,
                forkedSessionName,
                sizeof(input->forkedSessionName));
        input->forkedSessionNameLength = strlen(forkedSessionName);
    } else {
        input->forkedSessionNameLength = 0;
    }
    input->fork = fork;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionNew,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

// ================= Functions to make/free generic workItem ==================
// The user ID is not set here because of the different requirements of the
// UI and the CLI.  If neither sets the user ID, ApisSend will use whatever
// the current user ID and process ID happens to be (essentially reverting
// to a global session).  Currently, the session ID does not matter for a
// number of functions.
//
XcalarWorkItem *
xcalarApiMakeGenericWorkItem(XcalarApis api, void *input, size_t inputSize)
{
    XcalarWorkItem *workItem = NULL;

    workItem =
        (XcalarWorkItem *) memCallocExt(1, sizeof(*workItem), moduleName);
    if (workItem == NULL) {
        return NULL;
    }

    workItem->api = api;
    workItem->status = StatusOk.code();
    workItem->input = (XcalarApiInput *) input;
    workItem->inputSize = inputSize;
    workItem->output = NULL;
    workItem->outputSize = 0;
    workItem->userId = NULL;
    workItem->userIdSize = 0;
    workItem->sessionInfo = NULL;
    workItem->sessionInfoSize = 0;
    workItem->apiVersionSignature = versionGetApiSig();
    workItem->workItemLength = sizeof(*workItem);
    workItem->userId = NULL;
    workItem->legacyClient = false;

    return workItem;
}

void
xcalarApiFreeWorkItem(XcalarWorkItem *workItem)
{
    assert(workItem != NULL);
    if (workItem->input != NULL) {
        memFree(workItem->input);
        workItem->input = NULL;
        workItem->inputSize = 0;
    }

    if (workItem->output != NULL) {
        memFree(workItem->output);
        workItem->output = NULL;
        workItem->outputSize = 0;
    }

    if (workItem->userId != NULL) {
        memFree(workItem->userId);
        workItem->userId = NULL;
        workItem->userIdSize = 0;
    }

    if (workItem->sessionInfo != NULL) {
        memFree(workItem->sessionInfo);
        workItem->sessionInfo = NULL;
        workItem->sessionInfoSize = 0;
    }

    memFree(workItem);
    workItem = NULL;
}

XcalarWorkItem *
xcalarApiMakeKeyListWorkItem(XcalarApiKeyScope scope, const char *keyRegex)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiKeyListInput *input = NULL;
    bool error = true;
    size_t ret;

    input =
        (XcalarApiKeyListInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }
    assert(input != NULL);

    input->scope = scope;
    ret = strlcpy(input->keyRegex, keyRegex, sizeof(input->keyRegex));
    if (ret >= sizeof(input->keyRegex)) {
        xSyslog(moduleName,
                XlogErr,
                "Key regex is too long (%lu chars).  Max is %lu chars",
                strlen(keyRegex),
                sizeof(input->keyRegex) - 1);
        goto CommonExit;
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiKeyList, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    assert(workItem != NULL);
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeKeyAddOrReplaceWorkItem(XcalarApiKeyScope scope,
                                     const char *key,
                                     const char *value,
                                     bool persist)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiKeyAddOrReplaceInput *input = NULL;
    size_t inputSize = 0;
    bool error = true;
    size_t valueSize;

    if (scope == XcalarApiKeyInvalid) {
        xSyslog(moduleName, XlogErr, "Invalid key scope specified");
        goto CommonExit;
    }

    valueSize = strlen(value) + 1;
    inputSize = sizeof(XcalarApiKeyAddOrReplaceInput) + valueSize;
    input = (XcalarApiKeyAddOrReplaceInput *) memCallocExt(1,
                                                           inputSize,
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->scope = scope;
    strlcpy(input->kvPair.key, key, sizeof(input->kvPair.key));
    input->kvPair.valueSize = valueSize;
    strlcpy(input->kvPair.value, value, valueSize);
    input->persist = persist;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiKeyAddOrReplace,
                                            input,
                                            inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeKeyAppendWorkItem(XcalarApiKeyScope scope,
                               const char *key,
                               const char *suffix)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiKeyAppendInput *input = NULL;
    size_t inputSize = 0;
    bool error = true;
    size_t suffixSize;

    if (scope == XcalarApiKeyInvalid) {
        xSyslog(moduleName, XlogErr, "Invalid key scope specified");
        goto CommonExit;
    }

    suffixSize = strlen(suffix) + 1;
    inputSize = sizeof(XcalarApiKeyAppendInput) + suffixSize;
    input = (XcalarApiKeyAppendInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->scope = scope;
    strlcpy(input->key, key, sizeof(input->key));
    input->suffixSize = suffixSize;
    strlcpy(input->suffix, suffix, suffixSize);

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiKeyAppend, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
            inputSize = 0;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeKeySetIfEqualWorkItem(XcalarApiKeyScope scope,
                                   bool persist,
                                   uint32_t countSecondaryPairs,
                                   const char *keyCompare,
                                   const char *valueCompare,
                                   const char *valueReplace,
                                   const char *keySecondary,
                                   const char *valueSecondary)
{
    Status status = StatusStatusFieldNotInited;
    XcalarWorkItem *workItem = NULL;
    XcalarApiKeySetIfEqualInput *input = NULL;

    const size_t valueCompareSize = strlen(valueCompare) + 1;
    const size_t valueReplaceSize = strlen(valueReplace) + 1;
    const size_t valueSecondarySize = strlen(valueSecondary) + 1;
    const size_t inputSize = sizeof(XcalarApiKeySetIfEqualInput) +
                             valueCompareSize + valueReplaceSize +
                             valueSecondarySize;

    if (countSecondaryPairs == 0) {
        // Ignore passed secondary values.
        keySecondary = "";
        valueSecondary = "";
    }

    // Validation.
    if (scope == XcalarApiKeyInvalid) {
        xSyslog(moduleName, XlogErr, "Invalid key scope specified");
        goto CommonExit;
    }

    if (countSecondaryPairs > 1) {
        status = StatusInval;
        goto CommonExit;
    }
    if (strlen(keyCompare) + 1 > sizeof(input->keyCompare)) {
        xSyslog(moduleName,
                XlogErr,
                "Key '%s' too long. Max length is %lu.",
                keyCompare,
                sizeof(input->keyCompare) - 1);
        status = StatusInval;
        goto CommonExit;
    }
    if (strlen(keySecondary) + 1 > sizeof(input->keySecondary)) {
        xSyslog(moduleName,
                XlogErr,
                "Key '%s' too long. Max length is %lu.",
                keySecondary,
                sizeof(input->keySecondary) - 1);
        status = StatusInval;
        goto CommonExit;
    }

    input =
        (XcalarApiKeySetIfEqualInput *) memCallocExt(1, inputSize, moduleName);
    BailIfNull(input);

    input->scope = scope;
    input->valueCompareSize = valueCompareSize;
    input->valueReplaceSize = valueReplaceSize;
    input->valueSecondarySize = valueSecondarySize;
    input->persist = persist;
    input->countSecondaryPairs = countSecondaryPairs;

    size_t sizeNeeded;
    sizeNeeded =
        strlcpy(input->keyCompare, keyCompare, sizeof(input->keyCompare));
    if (sizeNeeded > sizeof(input->keyCompare)) {
        xSyslog(moduleName,
                XlogErr,
                "Name of key '%s' to compare is too long (max %lu bytes)",
                keyCompare,
                sizeof(input->keyCompare));
        goto CommonExit;
    }

    sizeNeeded =
        strlcpy(input->keySecondary, keySecondary, sizeof(input->keySecondary));
    if (sizeNeeded > sizeof(input->keySecondary)) {
        xSyslog(moduleName,
                XlogErr,
                "Name of secondary key '%s' is too long (max %lu bytes)",
                keySecondary,
                sizeof(input->keySecondary));
        goto CommonExit;
    }

    sizeNeeded = strlcpy(input->values, valueCompare, valueCompareSize);
    if (sizeNeeded > valueCompareSize) {
        xSyslog(moduleName,
                XlogErr,
                "Value to compare '%s' is too long (max %lu bytes)",
                valueCompare,
                valueCompareSize);
        goto CommonExit;
    }

    sizeNeeded = strlcpy(input->values + valueCompareSize,
                         valueReplace,
                         valueReplaceSize);
    if (sizeNeeded > valueReplaceSize) {
        xSyslog(moduleName,
                XlogErr,
                "Replace value '%s' is too long (max %lu bytes)",
                valueReplace,
                valueReplaceSize);
        goto CommonExit;
    }

    sizeNeeded = strlcpy(input->values + valueCompareSize + valueReplaceSize,
                         valueSecondary,
                         valueSecondarySize);
    if (sizeNeeded > valueSecondarySize) {
        xSyslog(moduleName,
                XlogErr,
                "Secondary replacement value '%s' is too long (max %lu bytes)",
                valueSecondary,
                valueSecondarySize);
        goto CommonExit;
    }

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiKeySetIfEqual, input, inputSize);
    BailIfNull(workItem);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeKeyDeleteWorkItem(XcalarApiKeyScope scope, const char *key)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiKeyDeleteInput *input = NULL;
    bool error = true;

    if (scope == XcalarApiKeyInvalid) {
        xSyslog(moduleName, XlogErr, "Invalid key scope specified");
        goto CommonExit;
    }

    input =
        (XcalarApiKeyDeleteInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->scope = scope;
    strlcpy(input->key, key, sizeof(input->key));

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiKeyDelete, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSupportGenerateWorkItem(bool generateMiniBundle,
                                     uint64_t supportCaseId)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiSupportGenerateInput *input = NULL;
    bool error = true;

    input = (XcalarApiSupportGenerateInput *) memCallocExt(1,
                                                           sizeof(*input),
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->generateMiniBundle = generateMiniBundle;
    input->supportCaseId = supportCaseId;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSupportGenerate,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetConfigParamsWorkItem()
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetConfigParams, NULL, 0);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeRuntimeSetParamWorkItem(const char **schedName,
                                     uint32_t *cpuReservedInPct,
                                     RuntimeType *rtType,
                                     uint32_t numSchedParams)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    XcalarApiRuntimeSetParamInput *input = (XcalarApiRuntimeSetParamInput *)
        memCalloc(1, sizeof(XcalarApiRuntimeSetParamInput));
    if (input == NULL) {
        goto CommonExit;
    }

    for (uint32_t ii = 0; ii < numSchedParams; ii++) {
        if (strlcpy(input->schedParams[ii].schedName,
                    schedName[ii],
                    sizeof(input->schedParams[ii].schedName)) >=
            sizeof(input->schedParams[ii].schedName)) {
            goto CommonExit;
        }
        input->schedParams[ii].cpusReservedInPct = cpuReservedInPct[ii];
        input->schedParams[ii].runtimeType = rtType[ii];
    }
    input->schedParamsCount = numSchedParams;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiRuntimeSetParam,
                                     input,
                                     sizeof(XcalarApiRuntimeSetParamInput));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeRuntimeGetParamWorkItem()
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiRuntimeGetParam, NULL, 0);
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeAppSetWorkItem(const char *name,
                            const char *hostType,
                            const char *duty,
                            const char *exec)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    // XXX Assumes exec is a string.
    size_t execSize = strlen(exec) + 1;
    size_t inputSize = sizeof(App::Packed) + execSize;

    App::Packed *input = (App::Packed *) memCalloc(1, inputSize);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->name, name, sizeof(input->name)) >=
        sizeof(input->name)) {
        goto CommonExit;
    }

    input->hostType = App::parseHostType(hostType);
    if (input->hostType == App::HostType::Invalid) {
        goto CommonExit;
    }

    input->flags = App::parseDuty(duty);
    if (input->flags == App::FlagInvalid) {
        goto CommonExit;
    }

    input->execSize = execSize;
    memcpy(input->exec, exec, input->execSize);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiAppSet, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeAppRunWorkItem(const char *name, bool isGlobal, const char *inStr)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    size_t inStrSize = strlen(inStr) + 1;
    size_t inputSize = sizeof(XcalarApiAppRunInput) + inStrSize;

    XcalarApiAppRunInput *input =
        (XcalarApiAppRunInput *) memCalloc(1, inputSize);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->name, name, sizeof(input->name)) >=
        sizeof(input->name)) {
        goto CommonExit;
    }

    input->scope =
        (isGlobal ? AppGroup::Scope::Global : AppGroup::Scope::Local);

    input->inStrSize = inStrSize;
    strlcpy(input->inStr, inStr, input->inStrSize);

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiAppRun, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeAppReapWorkItem(uint64_t appGroupId, bool cancel)
{
    XcalarWorkItem *workItem = NULL;
    bool error = true;

    size_t inputSize = sizeof(XcalarApiAppReapInput);
    XcalarApiAppReapInput *input =
        (XcalarApiAppReapInput *) memCalloc(1, inputSize);
    if (input == NULL) {
        goto CommonExit;
    }

    input->appGroupId = appGroupId;
    input->cancel = cancel;

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiAppReap, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
        }
    }
    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetIpAddrWorkItem(NodeId nodeId)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetIpAddrInput *input = NULL;
    bool error = true;

    input =
        (XcalarApiGetIpAddrInput *) memCallocExt(1, sizeof(*input), moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    input->nodeId = nodeId;

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiGetIpAddr, input, sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListDatasetUsersWorkItem(const char *datasetName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListDatasetUsersInput *input = NULL;
    bool error = true;

    input = (XcalarApiListDatasetUsersInput *) memCallocExt(1,
                                                            sizeof(*input),
                                                            moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->datasetName, datasetName, sizeof(input->datasetName)) >=
        sizeof(input->datasetName)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListDatasetUsers,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeGetDatasetsInfoWorkItem(const char *datasetsNamePattern)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiGetDatasetsInfoInput *input = NULL;
    bool error = true;

    input = (XcalarApiGetDatasetsInfoInput *) memCallocExt(1,
                                                           sizeof(*input),
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->datasetsNamePattern,
                datasetsNamePattern,
                sizeof(input->datasetsNamePattern)) >=
        sizeof(input->datasetsNamePattern)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiGetDatasetsInfo,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeListUserDatasetsWorkItem(const char *userIdName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiListUserDatasetsInput *input = NULL;
    bool error = true;

    input = (XcalarApiListUserDatasetsInput *) memCallocExt(1,
                                                            sizeof(*input),
                                                            moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->userIdName, userIdName, sizeof(input->userIdName)) >=
        sizeof(input->userIdName)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiListUserDatasets,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDatasetDeleteWorkItem(const char *datasetName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiDatasetDeleteInput *input = NULL;
    bool error = true;

    input = (XcalarApiDatasetDeleteInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->datasetName, datasetName, sizeof(input->datasetName)) >=
        sizeof(input->datasetName)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiDatasetDelete,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDatasetUnloadWorkItem(const char *datasetNamePattern)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiDatasetUnloadInput *input = NULL;
    bool error = true;

    input = (XcalarApiDatasetUnloadInput *) memCallocExt(1,
                                                         sizeof(*input),
                                                         moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->datasetNamePattern,
                datasetNamePattern,
                sizeof(input->datasetNamePattern)) >=
        sizeof(input->datasetNamePattern)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiDatasetUnload,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeDatasetGetMetaWorkItem(const char *datasetName)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiDatasetGetMetaInput *input = NULL;
    bool error = true;

    input = (XcalarApiDatasetGetMetaInput *) memCallocExt(1,
                                                          sizeof(*input),
                                                          moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    if (strlcpy(input->datasetName, datasetName, sizeof(input->datasetName)) >=
        sizeof(input->datasetName)) {
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiDatasetGetMeta,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    input = NULL;  // Passed to workItem

    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }

        if (workItem != NULL) {
            xcalarApiFreeWorkItem(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionDownloadWorkItem(const char *sessionName,
                                     const char *pathToAdditionalFiles)
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionDownloadInput *input = NULL;
    bool error = true;
    size_t ret;

    if (sessionName == NULL) {
        xSyslog(moduleName, XlogErr, "Name of session must be specified");
        goto CommonExit;
    }

    input = (XcalarApiSessionDownloadInput *) memCallocExt(1,
                                                           sizeof(*input),
                                                           moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    ret = strlcpy(input->sessionName, sessionName, sizeof(input->sessionName));
    if (ret >= sizeof(input->sessionName)) {
        xSyslog(moduleName,
                XlogErr,
                "Input session name too long (%lu bytes).  Max is %lu bytes",
                strlen(sessionName),
                sizeof(input->sessionName) - 1);
        goto CommonExit;
    }

    input->sessionNameLength = strlen(sessionName);

    ret = strlcpy(input->pathToAdditionalFiles,
                  pathToAdditionalFiles,
                  sizeof(input->pathToAdditionalFiles));
    if (ret >= sizeof(input->pathToAdditionalFiles)) {
        xSyslog(moduleName,
                XlogErr,
                "Input path to additional files is too long (%lu bytes).  Max "
                "is %lu bytes",
                strlen(pathToAdditionalFiles),
                sizeof(input->pathToAdditionalFiles) - 1);
        goto CommonExit;
    }

    workItem = xcalarApiMakeGenericWorkItem(XcalarApiSessionDownload,
                                            input,
                                            sizeof(*input));
    if (workItem == NULL) {
        goto CommonExit;
    }
    error = false;

CommonExit:
    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

XcalarWorkItem *
xcalarApiMakeSessionUploadWorkItem(const char *sessionName,
                                   const char *pathToAdditionalFiles,
                                   size_t sessionContentCount,
                                   uint8_t sessionContent[])
{
    XcalarWorkItem *workItem = NULL;
    XcalarApiSessionUploadInput *input = NULL;
    bool error = true;
    size_t inputSize;
    size_t ret;

    inputSize = sizeof(*input) + sessionContentCount;
    input =
        (XcalarApiSessionUploadInput *) memCallocExt(1, inputSize, moduleName);
    if (input == NULL) {
        goto CommonExit;
    }

    ret = strlcpy(input->sessionName, sessionName, sizeof(input->sessionName));
    if (ret >= sizeof(input->sessionName)) {
        xSyslog(moduleName,
                XlogErr,
                "Input session name too long (%lu bytes). Max is %lu bytes",
                strlen(sessionName),
                sizeof(input->sessionName) - 1);
        goto CommonExit;
    }

    input->sessionNameLength = strlen(sessionName);

    ret = strlcpy(input->pathToAdditionalFiles,
                  pathToAdditionalFiles,
                  sizeof(input->pathToAdditionalFiles));
    if (ret >= sizeof(input->pathToAdditionalFiles)) {
        xSyslog(moduleName,
                XlogErr,
                "Input path to additional files too long (%lu bytes).  Max is "
                "%lu bytes",
                strlen(pathToAdditionalFiles),
                sizeof(input->pathToAdditionalFiles) - 1);
        goto CommonExit;
    }

    input->sessionContentCount = sessionContentCount;
    memcpy(input->sessionContent, sessionContent, sessionContentCount);

    workItem =
        xcalarApiMakeGenericWorkItem(XcalarApiSessionUpload, input, inputSize);
    if (workItem == NULL) {
        goto CommonExit;
    }

    error = false;

CommonExit:

    if (error) {
        if (input != NULL) {
            memFree(input);
            input = NULL;
        }
        if (workItem != NULL) {
            memFree(workItem);
            workItem = NULL;
        }
    }

    return workItem;
}

// ========== Start of serialization functions =========
static Status
deserializeImportRetinaOutput(XcalarApiImportRetinaOutput *importRetinaOutput)
{
    uintptr_t bufCursor;
    size_t bytesSeen, ptrArraySize;
    XcalarApiUdfAddUpdateOutput *addUpdateOutput;
    size_t addUpdateOutputSize;
    uint64_t ii;

    if (importRetinaOutput->numUdfModules == 0) {
        if (importRetinaOutput->bufSize == 0) {
            return StatusOk;
        } else {
            return StatusInval;
        }
    }

    if (importRetinaOutput->udfModuleStatuses !=
        (XcalarApiUdfAddUpdateOutput **) XcalarApiMagic) {
        return StatusInval;
    }

    bytesSeen = 0;
    bufCursor = (uintptr_t) importRetinaOutput->buf;
    importRetinaOutput->udfModuleStatuses =
        (XcalarApiUdfAddUpdateOutput **) bufCursor;
    ptrArraySize = sizeof(importRetinaOutput->udfModuleStatuses[0]) *
                   importRetinaOutput->numUdfModules;
    bytesSeen += ptrArraySize;
    if (bytesSeen > importRetinaOutput->bufSize) {
        return StatusInval;
    }
    bufCursor += ptrArraySize;

    for (ii = 0; ii < importRetinaOutput->numUdfModules; ii++) {
        if (bytesSeen + sizeof(*addUpdateOutput) >
            importRetinaOutput->bufSize) {
            return StatusInval;
        }
        addUpdateOutput = (XcalarApiUdfAddUpdateOutput *) bufCursor;
        addUpdateOutputSize =
            UserDefinedFunction::sizeOfAddUpdateOutput(addUpdateOutput->error
                                                           .messageSize,
                                                       addUpdateOutput->error
                                                           .tracebackSize);
        // the output size takes in the output hdr into account,
        // but we are only copying the output result here
        addUpdateOutputSize -= offsetof(XcalarApiOutput, outputResult);

        bytesSeen += addUpdateOutputSize;
        if (bytesSeen > importRetinaOutput->bufSize) {
            return StatusInval;
        }
        importRetinaOutput->udfModuleStatuses[ii] = addUpdateOutput;
        addUpdateOutput = NULL;
        bufCursor += addUpdateOutputSize;
    }

    return StatusOk;
}

static size_t
getXcalarApiDagNodeSize(XcalarApiDagNode *dagNode)
{
    return sizeof(*dagNode) + dagNode->hdr.inputSize +
           dagNode->numChildren * sizeof(dagNode->children) +
           dagNode->numParents * sizeof(dagNode->parents);
}

static Status
deserializeDagOutput(XcalarApiDagOutput *dagOutput)
{
    Status status = StatusUnknown;
    void *bufCursor;
    size_t bytesSeen;
    uint64_t ii;

    bytesSeen = Dag::sizeOfDagOutputHdr(dagOutput->numNodes);

    bufCursor = (void *) ((uintptr_t) dagOutput + bytesSeen);

    for (ii = 0; ii < dagOutput->numNodes; ii++) {
        XcalarApiDagNode *dagNode;
        size_t dagNodeSize;

        dagNode = (XcalarApiDagNode *) bufCursor;
        if (dagOutput->node[ii] != (void *) XcalarApiMagic) {
            status = StatusInval;
            goto CommonExit;
        }

        dagOutput->node[ii] = dagNode;

        // populate parents array pointer, which is right after input
        XcalarApiDagNodeId *parentsPtr =
            (XcalarApiDagNodeId *) ((uintptr_t) dagNode + sizeof(*dagNode) +
                                    dagNode->hdr.inputSize);
        dagNode->parents = parentsPtr;

        // populate children array pointer, which is right after parents
        XcalarApiDagNodeId *childrenPtr =
            (XcalarApiDagNodeId *) ((uintptr_t) parentsPtr +
                                    dagNode->numParents *
                                        sizeof(*dagNode->parents));
        dagNode->children = childrenPtr;

        dagNodeSize = getXcalarApiDagNodeSize(dagNode);

        bytesSeen += dagNodeSize;
        if (bytesSeen > dagOutput->bufSize) {
            status = StatusInval;
            goto CommonExit;
        }

        bufCursor = (void *) ((uintptr_t) bufCursor + dagNodeSize);
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
xcalarApiDeserializeOutput(XcalarApis api, XcalarApiOutputResult *output)
{
    Status status = StatusUnknown;

    assert(output != NULL);

    switch (api) {
    case XcalarApiGetDag:
        status = deserializeDagOutput(&output->dagOutput);
        break;
    case XcalarApiQueryState:
        status = deserializeDagOutput(&output->queryStateOutput.queryGraph);
        break;
    case XcalarApiGetRetina:
        status =
            deserializeDagOutput(&output->getRetinaOutput.retina.retinaDag);
        break;
    case XcalarApiImportRetina:
        status = deserializeImportRetinaOutput(&output->importRetinaOutput);
        break;
    default:
        // No de-serialization required
        status = StatusOk;
        break;
    }
    return status;
}

Status
xcalarApiSerializeGetQueryInput(XcalarApiGetQueryInput *getQueryInput,
                                size_t inputSize)
{
    getQueryInput->input = (XcalarApiInput *) XcalarApiMagic;
    return StatusOk;
}

Status
xcalarApiDeserializeGetQueryInput(XcalarApiGetQueryInput *getQueryInput,
                                  size_t inputSize)
{
    Status status = StatusUnknown;
    size_t requiredSize = sizeof(*getQueryInput) + getQueryInput->inputSize;
    if (inputSize < requiredSize) {
        xSyslog(moduleName,
                XlogErr,
                "Error deserializing getQueryInput: inputSize too small. "
                "Required at least %lu bytes. Provided %lu bytes",
                requiredSize,
                inputSize);
        status = StatusNoBufs;
        goto CommonExit;
    }

    getQueryInput->input =
        (XcalarApiInput *) ((uintptr_t) getQueryInput + sizeof(*getQueryInput));
    status = StatusOk;
CommonExit:
    return status;
}
