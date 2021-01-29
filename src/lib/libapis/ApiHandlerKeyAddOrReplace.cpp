// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerKeyAddOrReplace.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "kvstore/KvStore.h"
#include "msg/Xid.h"
#include "usr/Users.h"

ApiHandlerKeyAddOrReplace::ApiHandlerKeyAddOrReplace(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerKeyAddOrReplace::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerKeyAddOrReplace::run(XcalarApiOutput **outputOut,
                               size_t *outputSizeOut)
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

    // consider exposing KvStoreOpts through to XcalarAPI
    status = KvStoreLib::get()->addOrReplace(kvStoreId,
                                             input_->kvPair.key,
                                             strlen(input_->kvPair.key) + 1,
                                             input_->kvPair.value,
                                             strlen(input_->kvPair.value) + 1,
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
ApiHandlerKeyAddOrReplace::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    size_t expectedInputSize = 0;
    KvStoreLib *kvs = KvStoreLib::get();

    assert((uintptr_t) input == (uintptr_t) &input->keyAddOrReplaceInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->keyAddOrReplaceInput;
    expectedInputSize = sizeof(*input_) + input_->kvPair.valueSize;

    if (expectedInputSize != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "expectedInputSize = %lu bytes (valueSize: %lu bytes)",
                inputSize,
                expectedInputSize,
                input_->kvPair.valueSize);
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

    if (kvs->requiresUserId(input_->scope) && userId_ == NULL) {
        xSyslog(moduleName, XlogErr, "userId cannot be NULL!");
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}
