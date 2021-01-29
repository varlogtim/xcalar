// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListFuncTests.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "test/FuncTests/FuncTestTypes.h"
#include "test/FuncTests/FuncTestDriver.h"
#include "libapis/LibApisCommon.h"

ApiHandlerListFuncTests::ApiHandlerListFuncTests(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListFuncTests::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListFuncTests::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiListFuncTestOutput *listFuncTestOutput = NULL;
    char(*testNames)[FuncTestTypes::MaxTestNameLen + 1] = NULL;
    unsigned numTests = 0;

    FuncTestDriver *funcTestDriver;
    funcTestDriver = FuncTestDriver::get();
    if (funcTestDriver == NULL) {
        status = StatusFunctionalTestDisabled;
        goto CommonExit;
    }

    status =
        funcTestDriver->listTests(input_->namePattern, &testNames, &numTests);
    if (status != StatusOk) {
        goto CommonExit;
    }

    outputSize =
        XcalarApiSizeOfOutput(output->outputResult.listFuncTestOutput) +
        (sizeof(listFuncTestOutput->testName[0]) * numTests);
    output = (XcalarApiOutput *) memAlloc(outputSize);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes, numTests: %u)",
                outputSize,
                numTests);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    listFuncTestOutput = &output->outputResult.listFuncTestOutput;
    listFuncTestOutput->numTests = numTests;
    for (unsigned ii = 0; ii < numTests; ii++) {
        strlcpy(listFuncTestOutput->testName[ii],
                testNames[ii],
                sizeof(listFuncTestOutput->testName[ii]));
    }

    status = StatusOk;
CommonExit:
    if (testNames != NULL) {
        memFree(testNames);
        testNames = NULL;
    }

    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerListFuncTests::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    FuncTestDriver *funcTestDriver = NULL;

    assert((uintptr_t) input == (uintptr_t) &input->listFuncTestInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listFuncTestInput;

    if (inputSize > sizeof(*input_)) {
        xSyslog(moduleName,
                XlogErr,
                "inputSize (%lu bytes) exceeds sizeof(*input_) (%lu bytes)",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    funcTestDriver = FuncTestDriver::get();
    if (funcTestDriver == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Functional tests not available in production build. "
                "Please contact your local Xcalar sales representative");
        status = StatusFunctionalTestDisabled;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
