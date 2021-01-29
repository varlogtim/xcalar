// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerKeySetIfEqual.h"
#include "msg/MessageTypes.h"
#include "msg/Xid.h"
#include "sys/XLog.h"
#include "kvstore/KvStore.h"
#include "util/MemTrack.h"
#include "usr/Users.h"

ApiHandlerKeySetIfEqual::ApiHandlerKeySetIfEqual(XcalarApis api)
    : ApiHandler(api),
      input_(NULL),
      valueCompare_(NULL),
      valueReplace_(NULL),
      valueSecondary_(NULL)
{
}

ApiHandler::Flags
ApiHandlerKeySetIfEqual::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerKeySetIfEqual::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    KvStoreId kvStoreId;
    UserMgr *userMgr = UserMgr::get();

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
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

    status = StatusOk;
    switch (input_->scope) {
    case XcalarApiKeyScopeGlobal:
        kvStoreId = XidMgr::XidGlobalKvStore;
        break;
    case XcalarApiKeyScopeSession:
        status = userMgr->getKvStoreId(userId_, sessionInfo_, &kvStoreId);
        break;
    case XcalarApiKeyScopeUser:
        status = StatusUnimpl;
        break;
    default:
        status = StatusInval;
        break;
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = KvStoreLib::get()->setIfEqual(kvStoreId,
                                           input_->countSecondaryPairs,
                                           input_->keyCompare,
                                           valueCompare_,
                                           valueReplace_,
                                           input_->keySecondary,
                                           valueSecondary_,
                                           input_->persist,
                                           KvStoreOptSync);
CommonExit:
    if (output != NULL) {
        output->hdr.status = status.code();
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerKeySetIfEqual::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize = 0;
    KvStoreLib *kvs = KvStoreLib::get();

    assert((uintptr_t) input == (uintptr_t) &input->keySetIfEqualInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->keySetIfEqualInput;
    expectedInputSize = sizeof(*input_) + input_->valueCompareSize +
                        input_->valueReplaceSize + input_->valueSecondarySize;

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (valueCompareSize: %lu bytes, "
                "valueReplaceSize: %lu bytes, valueSecondarySize: %lu bytes)",
                inputSize,
                expectedInputSize,
                input_->valueCompareSize,
                input_->valueReplaceSize,
                input_->valueSecondarySize);
        status = StatusInval;
        goto CommonExit;
    }

    status = kvs->isValidKeyScope(input_->scope);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Invalid key scope %u provided: %s",
                input_->scope,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (kvs->requiresSession(input_->scope) && sessionInfo_ == NULL) {
        xSyslog(moduleName, XlogErr, "Session info cannot be NULL!");
        status = StatusInval;
        goto CommonExit;
    }

    status = kvs->parseSetIfEqualInput(input_,
                                       inputSize,
                                       &valueCompare_,
                                       &valueReplace_,
                                       &valueSecondary_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Error parsing keySetIfEqual input: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (kvs->requiresUserId(input_->scope) && userId_ == NULL) {
        xSyslog(moduleName, XlogErr, "userId cannot be NULL!");
        status = StatusInval;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    return status;
}
