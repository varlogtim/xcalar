// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerKeyList.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "kvstore/KvStore.h"
#include "usr/Users.h"
#include "msg/Xid.h"

ApiHandlerKeyList::ApiHandlerKeyList(XcalarApis api)
    : ApiHandler(api), input_(NULL)
{
}

ApiHandler::Flags
ApiHandlerKeyList::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerKeyList::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiKeyListOutput *keyListOutput = NULL;
    KvStoreId kvStoreId;
    UserMgr *userMgr = UserMgr::get();
    KvStoreLib::KeyList *keyList = NULL;

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

    status = KvStoreLib::get()->list(kvStoreId, input_->keyRegex, &keyList);
    if (status != StatusOk) {
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(XcalarApiKeyListOutput) +
                 keyList->numKeys * sizeof(keyListOutput->keys[0]);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output of %lu bytes",
                outputSize);
        status = StatusNoMem;
        outputSize = 0;
        goto CommonExit;
    }

    keyListOutput = &output->outputResult.keyListOutput;
    assert((uintptr_t) &output->outputResult == (uintptr_t) keyListOutput);
    keyListOutput->numKeys = keyList->numKeys;
    for (int ii = 0; ii < keyList->numKeys; ii++) {
        memcpy(keyListOutput->keys[ii],
               keyList->keys[ii],
               sizeof(keyList->keys[0]));
    }
    output->hdr.status = status.code();

CommonExit:
    if (keyList != NULL) {
        memFree(keyList);
        keyList = NULL;
    }
    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerKeyList::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusUnknown;
    KvStoreLib *kvs = KvStoreLib::get();

    assert((uintptr_t) input == (uintptr_t) &input->keyListInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->keyListInput;

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
