// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stddef.h>
#include <pthread.h>
#include "StrlFunc.h"
#include "UdfOperation.h"
#include "udf/UserDefinedFunction.h"
#include "UdfLocal.h"
#include "UdfPersist.h"
#include "libapis/LibApisCommon.h"
#include "msg/Message.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "gvm/Gvm.h"

static constexpr const char *moduleName = "libudf";

UdfOperation::UdfOperation(UdfLocal *udfLocal, UdfPersist *udfPersist)
    : udfLocal_(udfLocal), udfPersist_(udfPersist)
{
    Gvm::get()->registerTarget(this);
}

UdfOperation::~UdfOperation() {}

GvmTarget::Index
UdfOperation::getGvmIndex() const
{
    return GvmTarget::Index::GvmIndexUdf;
}

Status
UdfOperation::localHandler(uint32_t action,
                           void *payload,
                           size_t *outputSizeOut)
{
    UdfOperation::UdfOperationInput *input =
        (UdfOperation::UdfOperationInput *) payload;

    switch ((Action) action) {
    case Action::Add:
        return udfLocal_
            ->addUdf(&input->addUpdateInputVersion.addUpdateInput,
                     input->addUpdateInputVersion.modulePrefix,
                     input->addUpdateInputVersion.consistentVersion);
        break;

    case Action::Update:
        return udfLocal_
            ->updateUdf(&input->addUpdateInputVersion.addUpdateInput,
                        input->addUpdateInputVersion.modulePrefix,
                        input->addUpdateInputVersion.consistentVersion,
                        input->addUpdateInputVersion.nextVersion);
        break;

    case Action::Delete:
        return udfLocal_->deleteUdf(&input->deleteInputVersion.deleteInput,
                                    input->deleteInputVersion.modulePrefix,
                                    input->deleteInputVersion.consistentVersion,
                                    input->deleteInputVersion.nextVersion);
        break;

    case Action::FlushNwCache:
        return (udfLocal_->flushNwCacheUdf(&input->addUpdateFlush));
        break;

    default:
        assert(0 && "Invalid GVM action");
        break;
    }
    return StatusGvmInvalidAction;
}

Status
UdfOperation::dispatchUdfUpdateToDlmNode(NodeId dlmNode,
                                         void *payload,
                                         size_t payloadSize,
                                         void *ephemeral)
{
    Status status = StatusOk;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;
    MsgMgr *msgMgr = MsgMgr::get();
    MsgSendRecvFlags srFlags;
    TwoPcBufLife bLife;

    assert(payload != NULL && payloadSize != 0);

    srFlags = (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly);
    bLife = TwoPcMemCopyInput;

    msgMgr->twoPcEphemeralInit(&eph,
                               payload,
                               payloadSize,
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcAddOrUpdateUdfDlm1,
                               ephemeral,
                               bLife);

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcAddOrUpdateUdfDlm,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           srFlags,
                           TwoPcSyncCmd,
                           TwoPcSingleNode,
                           dlmNode,
                           TwoPcClassNonNested);
    if (status == StatusOk) {
        assert(!twoPcHandle.twoPcHandle);
    }

    return status;
}

// See UserDefinedFunction::updateUdf() for detailed comments describing
// this function. In brief, the function does a 2-pc to the udf's libNs DLM
// node (different for different UDFs) which updates the libNs and on-disk
// UDF state (either both fail or both succeed). All this must be done under
// the libNs r/w exclusive lock (so UDF getters which get the read lock, block
// until this is done)
Status
UdfOperation::udfUpdateCommit(UdfOperation::UdfModuleSrcVersion *udfInfo,
                              UdfOperation::Action action,
                              const char *udfFullyQualName,
                              XcalarApiUdfContainer *udfContainer,
                              LibNsTypes::NsHandle libNsHandle)
{
    Status status = StatusOk;
    size_t udfUpdateDlmMsgSize = 0;
    UdfUpdateDlmMsg *msg = NULL;
    LibNs *libNs = LibNs::get();
    NodeId dlmNodeId;
    size_t sourceSize;

    sourceSize = udfInfo->addUpdateInput.sourceSize;

    udfUpdateDlmMsgSize = sizeof(UdfUpdateDlmMsg) + sourceSize;

    msg = (UdfUpdateDlmMsg *) memAlloc(udfUpdateDlmMsgSize);
    BailIfNull(msg);

    msg->udfLibNsHandle = libNsHandle;
    memset(&msg->udfContainer, 0, sizeof(msg->udfContainer));
    if (udfContainer) {
        memcpy(&msg->udfContainer, udfContainer, sizeof(msg->udfContainer));
    }
    memcpy(&msg->udfInfo, udfInfo, sizeof(msg->udfInfo) + sourceSize);

    dlmNodeId = libNs->getNodeId(udfFullyQualName, &status);
    BailIfFailed(status);

    msg->action = action;

    status =
        dispatchUdfUpdateToDlmNode(dlmNodeId, msg, udfUpdateDlmMsgSize, msg);
    BailIfFailed(status);

    status = msg->status;
    BailIfFailed(status);

CommonExit:
    if (msg) {
        memFree(msg);
        msg = NULL;
    }
    return status;
}
