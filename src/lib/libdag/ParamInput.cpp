// Copyright 2015 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// 1) Not every field can be parameterized.
// 2) We want an interface where the external agent can just specify precisely
//    what needs to be parameterized
// 3) We would like to make it easy for developers to add more APIs / operators
//    and to specify what fields in those operators/APIs are parameterizable,
//    and how they are parameterizable.
//
// As a result of 1 and 2, we have a separate data structure
// XcalarApiParamInput,that the external agent uses to specify the
// parameterization of fields.
//
// The temporary solution for 3 is to have one common place to deal with
// XcalarApiParamInput - XcalarApiInput mappings.
// Please note that XcalarApiParamInput is not a strict subset of
// XcalarApiInput. The author can freely choose to map ParamInputs to whatever
// combinations of fields in XcalarApiInput.
// For example, it can range from paramFilter.fiterStr to allow arbitrary
// parameterization of the evalStr field in XcalarApiFilter, to
// paramExport.useDefaults = DefaultExportToFile or DefaultExportToRDBMS, in
// which case the XcalarApiParamInput - XcalarApiInput mapping author need to
// figure out the appropriate fields and values to populate corresponding to
// each choice.

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "dag/DagLib.h"
#include "xdb/TableTypes.h"
#include "querymanager/QueryManager.h"
#include "msg/Message.h"
#include "queryparser/QueryParser.h"
#include "bc/BufferCache.h"
#include "dataset/Dataset.h"
#include "hash/Hash.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "optimizer/Optimizer.h"
#include "export/DataTarget.h"

Status
DagLib::updateXcalarApiInputFromParamInput(XcalarApiInput *apiInput,
                                           XcalarApiParamInput *paramInput)
{
    Status status = StatusUnknown;
    size_t ret;
    size_t bufSize;

    assert(apiInput != NULL);
    assert(paramInput != NULL);

    switch (paramInput->paramType) {
    case XcalarApiBulkLoad: {
        XcalarApiBulkLoadInput *loadInput;
        loadInput = &apiInput->loadInput;

        bufSize = sizeof(loadInput->loadArgs.sourceArgsList[0].path);
        ret = strlcpy(loadInput->loadArgs.sourceArgsList[0].path,
                      paramInput->paramInputArgs.paramLoad.datasetUrl,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        bufSize = sizeof(loadInput->loadArgs.sourceArgsList[0].fileNamePattern);
        ret = strlcpy(loadInput->loadArgs.sourceArgsList[0].fileNamePattern,
                      paramInput->paramInputArgs.paramLoad.namePattern,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }

        break;
    }
    case XcalarApiSynthesize: {
        XcalarApiSynthesizeInput *synthesizeInput;
        synthesizeInput = &apiInput->synthesizeInput;
        bufSize = sizeof(synthesizeInput->source.name);
        ret = strlcpy(synthesizeInput->source.name,
                      paramInput->paramInputArgs.paramSynthesize.source,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    }
    case XcalarApiFilter: {
        XcalarApiFilterInput *filterInput;
        filterInput = &apiInput->filterInput;
        bufSize = sizeof(filterInput->filterStr);
        ret = strlcpy(filterInput->filterStr,
                      paramInput->paramInputArgs.paramFilter.filterStr,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    }
    case XcalarApiExport: {
        break;
    }
    default:
        status = StatusXcalarApiNotParameterizable;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}

Status
DagLib::getParamStrFromXcalarApiInput(XcalarApiInput *input,
                                      XcalarApis api,
                                      char ***stringsOut,
                                      size_t **bufSizesOut,
                                      unsigned *numStringsOut)
{
    Status status = StatusUnknown;
    char **strings = NULL;
    size_t *bufSizes = NULL;
    unsigned numStrings = 0;

    assert(input != NULL);

    switch (api) {
    case XcalarApiBulkLoad: {
        DfLoadArgs *loadArgs = &input->loadInput.loadArgs;

        numStrings = 3 * loadArgs->sourceArgsListCount;

        strings = (char **) memAlloc(sizeof(*strings) * numStrings);
        BailIfNull(strings);

        bufSizes = (size_t *) memAlloc(sizeof(*bufSizes) * numStrings);
        BailIfNull(bufSizes);

        for (int ii = 0; ii < loadArgs->sourceArgsListCount; ii++) {
            int jj = ii * 3;

            strings[jj] = loadArgs->sourceArgsList[ii].path;
            bufSizes[jj] = sizeof(loadArgs->sourceArgsList[ii].path);

            jj++;

            strings[jj] = loadArgs->sourceArgsList[ii].fileNamePattern;
            bufSizes[jj] = sizeof(loadArgs->sourceArgsList[ii].fileNamePattern);

            jj++;

            strings[jj] = loadArgs->sourceArgsList[ii].targetName;
            bufSizes[jj] = sizeof(loadArgs->sourceArgsList[ii].targetName);
        }

        break;
    }
    case XcalarApiSynthesize:
        numStrings = 1;

        strings = (char **) memAlloc(sizeof(*strings) * numStrings);
        BailIfNull(strings);

        bufSizes = (size_t *) memAlloc(sizeof(*bufSizes) * numStrings);
        BailIfNull(bufSizes);

        strings[0] = input->synthesizeInput.source.name;
        bufSizes[0] = sizeof(input->synthesizeInput.source.name);
        break;
    case XcalarApiFilter:
        numStrings = 1;

        strings = (char **) memAlloc(sizeof(*strings) * numStrings);
        BailIfNull(strings);

        bufSizes = (size_t *) memAlloc(sizeof(*bufSizes) * numStrings);
        BailIfNull(bufSizes);

        strings[0] = input->filterInput.filterStr;
        bufSizes[0] = sizeof(input->filterInput.filterStr);
        break;
    default:
        status = StatusXcalarApiNotParameterizable;
        goto CommonExit;
        break;
    }

    status = StatusOk;

CommonExit:
    *numStringsOut = numStrings;
    *bufSizesOut = bufSizes;
    *stringsOut = strings;

    return status;
}

Status
DagLib::getStrFromParamInput(XcalarApiParamInput *paramInput,
                             char *stringsOut[],
                             unsigned *numStringsOut)
{
    Status status = StatusUnknown;
    unsigned index = 0;

    assert(paramInput != NULL);

    switch (paramInput->paramType) {
    case XcalarApiBulkLoad:
        if (strlen(paramInput->paramInputArgs.paramLoad.datasetUrl) > 0) {
            stringsOut[index] = paramInput->paramInputArgs.paramLoad.datasetUrl;
            index++;
        }

        if (strlen(paramInput->paramInputArgs.paramLoad.namePattern) > 0) {
            stringsOut[index] =
                paramInput->paramInputArgs.paramLoad.namePattern;
            index++;
        }
        break;
    case XcalarApiSynthesize:
        stringsOut[index] = paramInput->paramInputArgs.paramSynthesize.source;
        index++;
        break;
    case XcalarApiFilter:
        stringsOut[index] = paramInput->paramInputArgs.paramFilter.filterStr;
        index++;
        break;
    case XcalarApiExport:
        stringsOut[index] = paramInput->paramInputArgs.paramExport.fileName;
        index++;
        stringsOut[index] = paramInput->paramInputArgs.paramExport.targetName;
        index++;
        break;
    default:
        assert(0);
        break;
    }

    *numStringsOut = index;
    status = StatusOk;
    return status;
}

Status
DagLib::setParamInput(XcalarApiParamInput *paramInput,
                      XcalarApis paramType,
                      const char *inputStr)
{
    Status status = StatusUnknown;
    size_t ret;
    size_t bufSize;

    assert(paramInput != NULL);
    assert(inputStr != NULL);

    paramInput->paramType = paramType;
    switch (paramType) {
    case XcalarApiBulkLoad:
        bufSize = sizeof(paramInput->paramInputArgs.paramLoad.datasetUrl);
        ret = strlcpy(paramInput->paramInputArgs.paramLoad.datasetUrl,
                      inputStr,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    case XcalarApiSynthesize:
        bufSize = sizeof(paramInput->paramInputArgs.paramSynthesize.source);
        ret = strlcpy(paramInput->paramInputArgs.paramSynthesize.source,
                      inputStr,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    case XcalarApiFilter:
        bufSize = sizeof(paramInput->paramInputArgs.paramFilter.filterStr);
        ret = strlcpy(paramInput->paramInputArgs.paramFilter.filterStr,
                      inputStr,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    case XcalarApiExport:
        bufSize = sizeof(paramInput->paramInputArgs.paramExport.fileName);
        ret = strlcpy(paramInput->paramInputArgs.paramExport.fileName,
                      inputStr,
                      bufSize);
        if (ret >= bufSize) {
            status = StatusNoBufs;
            goto CommonExit;
        }
        break;
    default:
        status = StatusUnimpl;
        assert(0);
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
