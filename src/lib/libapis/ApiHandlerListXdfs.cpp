// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerListXdfs.h"
#include "msg/MessageTypes.h"
#include "udf/UserDefinedFunction.h"
#include "operators/XcalarEval.h"
#include "sys/XLog.h"
#include "libapis/LibApisCommon.h"

ApiHandlerListXdfs::ApiHandlerListXdfs(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerListXdfs::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerListXdfs::run(XcalarApiOutput **output, size_t *outputSize)
{
    Status status;
    XcalarApiUdfContainer *udfContainer = NULL;

    // XXX: Add constraint that the pattern doesn't contain '/' - if it does,
    // then the workbook container choice would be wrong. Today, nobody supplies
    // patterns with absolute paths in it, but if this were to occur, it
    // wouldn't work.
    // XXX: Also, a NeedsSessionOrGraph flag is probably needed otherwise,
    // userId_, sessionInfo_ may not be correct below; check this out
    if (input_->fnNamePattern[0] != '*') {
        // Caller wants to constrain the search to within the current
        // workbook.
        udfContainer =
            (XcalarApiUdfContainer *) memAllocExt(sizeof(*udfContainer),
                                                  moduleName);
        BailIfNull(udfContainer);

        status = UserDefinedFunction::initUdfContainer(udfContainer,
                                                       userId_,
                                                       sessionInfo_,
                                                       NULL);
        BailIfFailed(status);
    }

    status = XcalarEval::get()->getFnList(input_->fnNamePattern,
                                          input_->categoryPattern,
                                          udfContainer,
                                          output,
                                          outputSize);
    BailIfFailed(status);

CommonExit:

    if (udfContainer != NULL) {
        memFree(udfContainer);
        udfContainer = NULL;
    }

    return status;
}

Status
ApiHandlerListXdfs::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;

    assert((uintptr_t) input == (uintptr_t) &input->listXdfsInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->listXdfsInput;

    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
