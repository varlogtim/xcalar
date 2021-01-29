// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetQuery.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "queryparser/QueryParser.h"
#include "util/MemTrack.h"
#include "strings/String.h"

ApiHandlerGetQuery::ApiHandlerGetQuery(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerGetQuery::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetQuery::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiGetQueryOutput *getQueryOutput;
    QueryCmdParser *cmdParser = NULL;
    char *queryStr = NULL;
    json_t *query = json_array();
    BailIfNull(query);

    outputSize = XcalarApiSizeOfOutput(*getQueryOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }
    memZero(output, outputSize);

    getQueryOutput = &output->outputResult.getQueryOutput;
    assert((uintptr_t) getQueryOutput == (uintptr_t) &output->outputResult);

    cmdParser = QueryParser::get()->getCmdParser(input_->api);
    if (cmdParser == NULL) {
        status = StatusUnimpl;
        goto CommonExit;
    } else {
        if (input_->api == XcalarApiMap) {
            xcalarApiDeserializeMapInput(&input_->input->mapInput);
        } else if (input_->api == XcalarApiGroupBy) {
            xcalarApiDeserializeGroupByInput(&input_->input->groupByInput);
        } else if (input_->api == XcalarApiUnion) {
            xcalarApiDeserializeUnionInput(&input_->input->unionInput);
        } else if (input_->api == XcalarApiExecuteRetina) {
            input_->input->executeRetinaInput.userId = *userId_;
        }

        json_error_t err;
        status = cmdParser->reverseParse(input_->api,
                                         "",
                                         "",
                                         DgDagStateCreated,
                                         input_->input,
                                         NULL,
                                         &err,
                                         query);
        if (status != StatusOk) {
            goto CommonExit;
        }

        queryStr = json_dumps(query, JSON_INDENT(4) | JSON_ENSURE_ASCII);
        BailIfNull(queryStr);

        status = strStrlcpy(getQueryOutput->query,
                            queryStr,
                            sizeof(getQueryOutput->query));
        BailIfFailed(status);
    }

    status = StatusOk;
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    if (query) {
        json_decref(query);
        query = NULL;
    }

    if (queryStr) {
        memFree(queryStr);
        queryStr = NULL;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetQuery::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize = 0;

    assert((uintptr_t) input == (uintptr_t) &input->getQueryInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->getQueryInput;

    expectedInputSize = sizeof(*input_) + input_->inputSize;

    if (!isValidXcalarApis(input_->api)) {
        xSyslog(moduleName,
                XlogErr,
                "Api provided %u is not a valid XcalarApi",
                input_->api);
        status = StatusInval;
        goto CommonExit;
    }

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (api: %s, "
                "apiInputSize: %lu bytes)",
                inputSize,
                expectedInputSize,
                strGetFromXcalarApis(input_->api),
                input_->inputSize);
        status = StatusInval;
        goto CommonExit;
    }

    status = xcalarApiDeserializeGetQueryInput(input_, inputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error deserializing getQueryInput: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (QueryParser::get()->getCmdParser(input_->api) == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Command parser for %s does not exist",
                strGetFromXcalarApis(input_->api));
        status = StatusUnimpl;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
