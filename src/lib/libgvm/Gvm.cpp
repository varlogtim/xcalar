// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "gvm/Gvm.h"
#include "config/Config.h"
#include "msg/Message.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "gvm";

Gvm *Gvm::instance = NULL;

Gvm::Gvm() : targets_{NULL} {}

Gvm::~Gvm() {}

Status  // static
Gvm::init()
{
    Status status = StatusOk;
    assert(instance == NULL);
    instance = new (std::nothrow) Gvm;
    if (instance == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (instance != NULL) {
            delete instance;
            instance = NULL;
        }
    }

    return status;
}

void
Gvm::destroy()
{
    instance = NULL;
    delete this;
}

Gvm *  // static
Gvm::get()
{
    return instance;
}

void
Gvm::registerTarget(GvmTarget *target)
{
    GvmTarget::Index index = target->getGvmIndex();
    assert(targets_[index] == NULL);
    targets_[index] = target;
}

//
// Perform global operations.
//
Status
Gvm::invoke(Gvm::Payload *input, Status *nodeStatus)
{
    Status status = broadcast(input, nodeStatus);
    return status;
}

Status
Gvm::invoke(Gvm::Payload *input)
{
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status nodeStatus[nodeCount];

    Status status = broadcast(input, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

    return status;
}

Status
Gvm::invokeWithOutput(Gvm::Payload *input,
                      size_t maxOutputSize,
                      void **outputPerNode,
                      size_t *sizePerNode,
                      Status *nodeStatus)
{
    CompletionBlock completion;
    completion.type = CompletionType::GatherOutput;
    completion.nodeStatus = nodeStatus;
    completion.outputPerNode = outputPerNode;
    completion.sizePerNode = sizePerNode;
    size_t ii;
    unsigned nodeCount = Config::get()->getActiveNodes();

    for (ii = 0; ii < nodeCount; ii++) {
        nodeStatus[ii] = StatusUnknown;
        outputPerNode[ii] = NULL;
        sizePerNode[ii] = 0;
    }

    assert(maxOutputSize > 0 && "Max output size is valid");

    input->maxOutPayloadSize = maxOutputSize + sizeof(Gvm::Payload);

    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      input,
                                      input->inPayloadSize,
                                      input->maxOutPayloadSize,
                                      TwoPcSlowPath,
                                      TwoPcCallId::CallIdGvmBroadcast,
                                      &completion,
                                      (TwoPcBufLife)(TwoPcMemCopyInput |
                                                     TwoPcMemCopyOutput));

    MsgSendRecvFlags sendRecvFlags =
        (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrPlusPayload);

    Status status = MsgMgr::get()->twoPc(&twoPcHandle,
                                         MsgTypeId::Msg2pcGvmBroadcast,
                                         TwoPcDoNotReturnHandle,
                                         &eph,
                                         (MsgSendRecvFlags)(sendRecvFlags),
                                         TwoPcSyncCmd,
                                         TwoPcAllNodes,
                                         TwoPcIgnoreNodeId,
                                         TwoPcClassNonNested);

    return status;
}

// Sends "do local work" to all nodes.
Status
Gvm::broadcast(Gvm::Payload *input, Status *nodeStatus)
{
    Status status;

    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgTypeId msgId = MsgTypeId::Msg2pcGvmBroadcast;
    size_t ii;
    unsigned nodeCount = Config::get()->getActiveNodes();

    for (ii = 0; ii < nodeCount; ii++) {
        nodeStatus[ii] = StatusUnknown;
    }

    CompletionBlock completion;
    completion.type = CompletionType::GatherStatus;
    completion.nodeStatus = nodeStatus;

    MsgMgr::get()->twoPcEphemeralInit(&eph,
                                      input,
                                      input->inPayloadSize,
                                      0,
                                      TwoPcSlowPath,
                                      TwoPcCallId::CallIdGvmBroadcast,
                                      &completion,
                                      TwoPcMemCopyInput);

    MsgSendRecvFlags sendRecvFlags =
        (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly);

    status = MsgMgr::get()->twoPc(&twoPcHandle,
                                  msgId,
                                  TwoPcDoNotReturnHandle,
                                  &eph,
                                  sendRecvFlags,
                                  TwoPcSyncCmd,
                                  TwoPcAllNodes,
                                  TwoPcIgnoreNodeId,
                                  TwoPcClassNonNested);

    return status;
}

//
// 2pc handlers for incoming broadcast requests.
//
void
TwoPcCallIdGvmBroadcast::schedLocalWork(MsgEphemeral *eph, void *payload)
{
    Status status = StatusOk;
    size_t outputSize = 0;
    Gvm *gvm = Gvm::get();
    Gvm::Payload *gPayload = (Gvm::Payload *) payload;
    GvmTarget *target;

    if (gPayload->index >= GvmTarget::Index::GvmIndexCount) {
        assert(false);
        status = StatusBadMsg;
        goto CommonExit;
    }

    target = gvm->targets_[gPayload->index];
    if (target == NULL) {
        assert(false);
        eph->status = StatusBadMsg;
        goto CommonExit;
    }

    status = target->localHandler(gPayload->action, gPayload->buf, &outputSize);
    BailIfFailed(status);

CommonExit:

    gPayload->outPayloadSize = outputSize + sizeof(Gvm::Payload);

    assert(gPayload->outPayloadSize - sizeof(Gvm::Payload) <=
               gPayload->maxOutPayloadSize &&
           "Output size is valid");

    eph->setAckInfo(status, gPayload->outPayloadSize);
}

// Completion for each node's local processing (completion handler run on DLM
// or originating node).
void
TwoPcCallIdGvmBroadcast::schedLocalCompletion(MsgEphemeral *eph, void *payload)
{
    Gvm::CompletionBlock *completion = (Gvm::CompletionBlock *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);

    if (completion->type == Gvm::CompletionType::GatherStatus) {
        completion->nodeStatus[dstNodeId] = eph->status;
    } else if (completion->type == Gvm::CompletionType::GatherOutput) {
        completion->nodeStatus[dstNodeId] = eph->status;
        if (eph->status != StatusOk) {
            completion->outputPerNode[dstNodeId] = NULL;
            completion->sizePerNode[dstNodeId] = 0;
        } else {
            Gvm::Payload *gPayload = (Gvm::Payload *) payload;

            assert(gPayload->outPayloadSize != 0 &&
                   gPayload->outPayloadSize > sizeof(Gvm::Payload) &&
                   "Output size is valid");

            assert(gPayload->outPayloadSize - sizeof(Gvm::Payload) <=
                       gPayload->maxOutPayloadSize &&
                   "Output size is valid");

            size_t outputSize = gPayload->outPayloadSize - sizeof(Gvm::Payload);
            void *output = memAlloc(outputSize);
            if (output != NULL) {
                memcpy(output, gPayload->buf, outputSize);
                completion->outputPerNode[dstNodeId] = output;
                completion->sizePerNode[dstNodeId] = outputSize;
            } else {
                completion->nodeStatus[dstNodeId] = StatusNoMem;
                completion->outputPerNode[dstNodeId] = NULL;
                completion->sizePerNode[dstNodeId] = 0;
            }
        }
    } else {
        assert(false);
    }
}

void
TwoPcCallIdGvmBroadcast::recvDataCompletion(MsgEphemeral *eph, void *payload)
{
    schedLocalCompletion(eph, payload);
}
