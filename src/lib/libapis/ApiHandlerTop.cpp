// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApiHandlerTop.h"
#include "msg/MessageTypes.h"
#include "sys/XLog.h"
#include "msg/MessageTypes.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "libapis/LibApisCommon.h"

ApiHandlerTop::ApiHandlerTop(XcalarApis api) : ApiHandler(api), input_(NULL) {}

ApiHandler::Flags
ApiHandlerTop::getFlags()
{
    return (Flags)(NeedsAck | NeedsToRunImmediately);
}

Status
ApiHandlerTop::run(XcalarApiOutput **outputOut, size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiTopOutput *topOutput = NULL;
    TwoPcHandle twoPcHandle;
    MsgEphemeral eph;
    MsgMgr *msgMgr = MsgMgr::get();
    Config *config = Config::get();
    Status status = StatusUnknown;

    uint64_t numNodes = config->getActiveNodes();

    outputSize = XcalarApiSizeOfOutput(*topOutput) +
                 (sizeof(topOutput->topOutputPerNode[0]) * numNodes);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output (numNodes: %lu)",
                numNodes);
        status = StatusNoMem;
        goto CommonExit;
    }

    topOutput = &output->outputResult.topOutput;
    assert((uintptr_t) &output->outputResult == (uintptr_t) topOutput);

    topOutput->numNodes = numNodes;
    topOutput->status = StatusOk.code();

    msgMgr->twoPcEphemeralInit(&eph,
                               input_,
                               inputSize_,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcXcalarApiTop1,
                               topOutput,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcXcalarApiTop,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcAllNodes,
                           TwoPcIgnoreNodeId,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }
    assert(!twoPcHandle.twoPcHandle);

    status.fromStatusCode(topOutput->status);

CommonExit:
    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
        outputSize = 0;
    }

    *outputOut = output;
    *outputSizeOut = outputSize;
    return status;
}

Status
ApiHandlerTop::setArg(XcalarApiInput *input, size_t inputSize)
{
    Status status = StatusOk;
    assert((uintptr_t) input == (uintptr_t) &input->topInput);
    apiInput_ = input;
    inputSize_ = inputSize;
    input_ = &input->topInput;

    if (sizeof(*input_) != inputSize) {
        xSyslog(moduleName,
                XlogErr,
                "Input size provided (%lu bytes) does not match "
                "sizeof(*input_) = %lu bytes)",
                inputSize,
                sizeof(*input_));
        status = StatusInval;
        goto CommonExit;
    }

CommonExit:
    return status;
}
