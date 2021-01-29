// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerStartFuncTests.h"
#include "msg/MessageTypes.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "test/FuncTests/FuncTestDriver.h"
#include "libapis/LibApisCommon.h"

ApiHandlerStartFuncTests::ApiHandlerStartFuncTests(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerStartFuncTests::getFlags()
{
    return (Flags)(NeedsAck);
}

Status
ApiHandlerStartFuncTests::run(XcalarApiOutput **outputOut,
                              size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    FuncTestDriver *funcTestDriver = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    funcTestDriver = FuncTestDriver::get();
    if (funcTestDriver == NULL) {
        status = StatusFunctionalTestDisabled;
        goto CommonExit;
    }

    status = funcTestDriver->runFuncTests(input_->parallel,
                                          input_->runAllTests,
                                          input_->runOnAllNodes,
                                          input_->numTestPatterns,
                                          input_->testNamePatterns,
                                          &output,
                                          &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerStartFuncTests::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize;
    FuncTestDriver *funcTestDriver = NULL;

    assert((uintptr_t) input == (uintptr_t) &input->startFuncTestInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->startFuncTestInput;
    expectedInputSize = sizeof(*input_) + (sizeof(input_->testNamePatterns[0]) *
                                           input_->numTestPatterns);

    funcTestDriver = FuncTestDriver::get();
    if (funcTestDriver == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Request to start functional test in production build "
                "denied. Please contact your local Xcalar sales "
                "representative.");
        status = StatusFunctionalTestDisabled;
        goto CommonExit;
    }

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (numTestPatterns: %u)",
                inputSize,
                expectedInputSize,
                input_->numTestPatterns);
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
