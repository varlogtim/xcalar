// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "primitives/Primitives.h"
#include "sys/Socket.h"
#include "sys/XLog.h"
#include "libapis/ApisRecvClient.h"
#include "common/Version.h"

static constexpr const char *moduleName = "libapis::ApisRecvClient";

ApisRecvClient::ApisRecvClient(SocketHandle clientFd)
    : clientFd_(clientFd), state_(State::NeedWorkItem)
{
    workItem_.input = NULL;
    workItem_.userId = NULL;
    workItem_.sessionInfo = NULL;
    workItem_.inputSize = 0;
    workItem_.userIdSize = 0;
    workItem_.sessionInfoSize = 0;
}

ApisRecvClient::~ApisRecvClient()
{
    if (clientFd_ != SocketHandleInvalid) {
        sockDestroy(&clientFd_);
    }

    if (workItem_.input != NULL) {
        memFree(workItem_.input);
        workItem_.input = NULL;
        workItem_.inputSize = 0;
    }

    if (workItem_.userId != NULL) {
        memFree(workItem_.userId);
        workItem_.userId = NULL;
        workItem_.userIdSize = 0;
    }

    if (workItem_.sessionInfo != NULL) {
        memFree(workItem_.sessionInfo);
        workItem_.sessionInfo = NULL;
        workItem_.sessionInfoSize = 0;
    }

    state_ = State::Destructed;
}

bool
ApisRecvClient::isReady()
{
    return state_ == State::Ready;
}

Status
ApisRecvClient::processIncoming()
{
    Status status = StatusUnknown;
    switch (state_) {
    case State::NeedWorkItem:
        status = sockRecvConverged(clientFd_, &workItem_, sizeof(workItem_));
        workItem_.input = NULL;
        workItem_.userId = NULL;
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error receiving workItem from client: %s",
                    strGetFromStatus(status));
            state_ = State::SocketError;
            return status;
        }

        // XXX Add more sanity checks here for workitem received from client.
        if (workItem_.workItemLength != sizeof(workItem_)) {
            state_ = State::ApiInputInvalid;
            status = StatusApisWorkInvalidLength;
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid workItemLength %u in workitem from client: %s",
                    workItem_.workItemLength,
                    strGetFromStatus(status));
            return status;
        }

        if (workItem_.apiVersionSignature != versionGetApiSig()) {
            state_ = State::ApiInputInvalid;
            status = StatusApisWorkInvalidSignature;
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid signature %u in workitem from client: %s",
                    workItem_.apiVersionSignature,
                    strGetFromStatus(status));
            return status;
        }

        if (workItem_.inputSize > 0) {
            state_ = State::NeedApiInput;
        } else if (workItem_.userIdSize > 0) {
            state_ = State::NeedUserId;
        } else if (workItem_.sessionInfoSize > 0) {
            state_ = State::NeedSessionInfo;
        } else {
            state_ = State::Ready;
        }
        status = StatusOk;
        break;

    case State::NeedApiInput:
        assert(workItem_.inputSize > 0);
        workItem_.input = (XcalarApiInput *) memAlloc(workItem_.inputSize);
        if (workItem_.input == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate apiInput (inputSize: "
                    "%lu bytes)",
                    workItem_.inputSize);
            return status;
        }

        status =
            sockRecvConverged(clientFd_, workItem_.input, workItem_.inputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error receiving apiInput from client: %s",
                    strGetFromStatus(status));
            state_ = State::SocketError;
            return status;
        }

        if (workItem_.userIdSize > 0) {
            state_ = State::NeedUserId;
        } else if (workItem_.sessionInfoSize > 0) {
            state_ = State::NeedSessionInfo;
        } else {
            state_ = State::Ready;
        }

        break;

    case State::NeedUserId:
        assert(workItem_.userIdSize > 0);
        workItem_.userId = (XcalarApiUserId *) memAlloc(workItem_.userIdSize);
        if (workItem_.userId == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate userId (userIdSize: "
                    "%lu bytes)",
                    workItem_.userIdSize);
            return status;
        }

        status = sockRecvConverged(clientFd_,
                                   workItem_.userId,
                                   workItem_.userIdSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error receiving userId from client: %s",
                    strGetFromStatus(status));
            state_ = State::SocketError;
            return status;
        }

        if (workItem_.sessionInfoSize > 0) {
            state_ = State::NeedSessionInfo;
        } else {
            state_ = State::Ready;
        }
        break;

    case State::NeedSessionInfo:
        assert(workItem_.sessionInfoSize > 0);
        workItem_.sessionInfo =
            (XcalarApiSessionInfoInput *) memAlloc(workItem_.sessionInfoSize);
        if (workItem_.sessionInfo == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate sessionInfo "
                    "(sessionInfoSize: %lu bytes)",
                    workItem_.sessionInfoSize);
            return status;
        }

        status = sockRecvConverged(clientFd_,
                                   workItem_.sessionInfo,
                                   workItem_.sessionInfoSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Error receiving sessionInfo from client: %s",
                    strGetFromStatus(status));
            state_ = State::SocketError;
            return status;
        }
        state_ = State::Ready;
        break;

    default:
        assert(0);
        xSyslog(moduleName, XlogErr, "Unknown state %u", state_);
        status = StatusUnimpl;
    }

    return status;
}

Status
ApisRecvClient::sendOutput(XcalarApiOutput *output, size_t outputSize)
{
    Status status = StatusUnknown;

    if (outputSize == 0) {
        return StatusOk;
    }

    status = sockSendConverged(clientFd_, &outputSize, sizeof(outputSize));
    if (status != StatusOk) {
        return status;
    }

    // Send output back to client
    assert(output != NULL);
    return sockSendConverged(clientFd_, output, outputSize);
}

// This function is invoked when a received work item is not going to be
// queued for processing, for example if a shutdown is in progress.
void
ApisRecvClient::sendRejection(Status rejectionStatus)
{
    XcalarApiOutput *output = NULL;
    Status status = StatusUnknown;
    size_t outputSize = 0;

    outputSize = XcalarApiSizeOfOutput(output->outputResult.noOutput);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate output "
                "(Required size: %lu bytes)",
                outputSize);
        goto CommonExit;
    }

    // Ensure that we do not return residual data
    memZero(output, outputSize);

    output->hdr.status = rejectionStatus.code();

    // XXX the two assert status == OK below should be removed for
    //     error testing
    status = sockSendConverged(clientFd_, &outputSize, sizeof(outputSize));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to send function failure response to requester. "
                "Socket send error: %s",
                strGetFromStatus(status));
    }

    // Send bad news back to client
    status = sockSendConverged(clientFd_, output, outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to send function failure response to requester. "
                "Socket send error: %s",
                strGetFromStatus(status));
    }
CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }
}

Status
ApisRecvClient::drainIncomingTcp()
{
    Status status = StatusUnknown;
    char drainBuffer[100];
    size_t bytesToDrain = 0;

    if (state_ == State::NeedWorkItem) {
        status = processIncoming();
        if (status != StatusOk) {
            return status;
        }
    }

    assert(state_ != State::NeedWorkItem);
    switch (state_) {
    case State::NeedApiInput:
        bytesToDrain = workItem_.inputSize + workItem_.userIdSize +
                       workItem_.sessionInfoSize;
        break;
    case State::NeedUserId:
        bytesToDrain = workItem_.userIdSize + workItem_.sessionInfoSize;
        break;
    case State::NeedSessionInfo:
        bytesToDrain = workItem_.sessionInfoSize;
        break;
    default:
        bytesToDrain = 0;
        break;
    }

    while (bytesToDrain > 0) {
        status = sockRecvConverged(clientFd_,
                                   drainBuffer,
                                   xcMin(sizeof(drainBuffer), bytesToDrain));
        if (status != StatusOk) {
            return status;
        }
        bytesToDrain -= xcMin(sizeof(drainBuffer), bytesToDrain);
    }

    return StatusOk;
}
