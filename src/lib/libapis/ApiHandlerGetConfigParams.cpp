// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerGetConfigParams.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "libapis/LibApisCommon.h"

ApiHandlerGetConfigParams::ApiHandlerGetConfigParams(XcalarApis api)
    : ApiHandler(api)
{
}

ApiHandler::Flags
ApiHandlerGetConfigParams::getFlags()
{
    // UI command
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerGetConfigParams::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiGetConfigParamsOutput *getConfigParamsOutput = NULL;
    unsigned numParams = KeyValueMappings::MappingsCount;
    char valueBuffer[Config::MaxValueLength];
    char defaultValueBuffer[Config::MaxValueLength];
    Config *cfg = Config::get();
    bool visible;
    KeyValueMappings *mappings = XcalarConfig::get()->mappings_;

    outputSize =
        XcalarApiSizeOfOutput(output->outputResult.getConfigParamsOutput) +
        (sizeof(getConfigParamsOutput->parameter[0]) * numParams);
    output = (XcalarApiOutput *) memAlloc(outputSize);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes, numParams: %u)",
                outputSize,
                numParams);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }
    memZero(output, outputSize);

    getConfigParamsOutput = &output->outputResult.getConfigParamsOutput;
    getConfigParamsOutput->numParams = numParams;
    for (unsigned ii = 0; ii < numParams; ii++) {
        strlcpy(getConfigParamsOutput->parameter[ii].paramName,
                mappings[ii].keyName,
                sizeof(getConfigParamsOutput->parameter[0].paramName));

        switch (mappings[ii].type) {
        case KeyValueMappings::Boolean:
            snprintf(valueBuffer,
                     Config::MaxKeyLength,
                     "%s",
                     (*(bool *) mappings[ii].variable ? "true" : "false"));
            snprintf(defaultValueBuffer,
                     Config::MaxKeyLength,
                     "%s",
                     (mappings[ii].defaultBoolean ? "true" : "false"));
            break;
        case KeyValueMappings::Integer:
            snprintf(valueBuffer,
                     Config::MaxKeyLength,
                     "%d",
                     (*(uint32_t *) mappings[ii].variable));
            snprintf(defaultValueBuffer,
                     Config::MaxKeyLength,
                     "%d",
                     mappings[ii].defaultInteger);
            break;
        case KeyValueMappings::LongInteger:
            snprintf(valueBuffer,
                     Config::MaxKeyLength,
                     "%lu",
                     (*(size_t *) mappings[ii].variable));
            snprintf(defaultValueBuffer,
                     Config::MaxKeyLength,
                     "%lu",
                     mappings[ii].defaultLongInteger);
            break;
        case KeyValueMappings::String:
            // This isn't performance critial functionality
            snprintf(valueBuffer,
                     Config::MaxKeyLength,
                     "%s",
                     ((char *) mappings[ii].variable));
            snprintf(defaultValueBuffer,
                     Config::MaxKeyLength,
                     "%s",
                     ((char *) mappings[ii].defaultString));
            break;
        default:
            // Programming error
            assert(0);
            snprintf(valueBuffer, Config::MaxKeyLength, "%s", "*** error ***");
            snprintf(defaultValueBuffer,
                     Config::MaxKeyLength,
                     "%s",
                     "*** error ***");
            break;
        }
        strlcpy(getConfigParamsOutput->parameter[ii].paramValue,
                valueBuffer,
                sizeof(getConfigParamsOutput->parameter[0].paramValue));
        strlcpy(getConfigParamsOutput->parameter[ii].defaultValue,
                defaultValueBuffer,
                sizeof(getConfigParamsOutput->parameter[0].defaultValue));

        visible = false;
        if (mappings[ii].visible || cfg->changedFromDefault(&mappings[ii])) {
            // Parameter is spec'd out as visible or it is spec'd out
            // as being hidden but has a non-default value.  For the latter
            // case we tell XI that it should be displayed.
            visible = true;
        }
        getConfigParamsOutput->parameter[ii].visible = visible;
        getConfigParamsOutput->parameter[ii].changeable =
            mappings[ii].changeable;
        getConfigParamsOutput->parameter[ii].restartRequired =
            mappings[ii].restartRequired;
    }

    status = StatusOk;

CommonExit:

    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerGetConfigParams::setArg(XcalarApiInput *input, size_t inputSize)
{
    apiInput_ = input;
    inputSize_ = inputSize;
    return StatusOk;
}
