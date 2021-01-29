// Copyright 2014 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

// XXX: FIXME: Must rethink libapis interface and backward-compat issues

#include <string.h>
#include <cstdlib>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "libapis/LibApisCommon.h"
#include "libapis/LibApisSend.h"
#include "libapis/WorkItem.h"
#include "sys/Socket.h"
#include "config/Config.h"
#include "util/MemTrack.h"
#include "export/DataTargetTypes.h"
#include "usr/Users.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "libapisSend";

const std::string ServiceSocket::UnixDomainSocketDir = "/tmp/xcalar_sock";
const std::string ServiceSocket::UnixDomainSocketPath =
    ServiceSocket::UnixDomainSocketDir + "/usrnode";

// ================ helper function to make workIteam and send ================
Status
xcalarApiGetExportDir(char *exportDir,
                      size_t bufSize,
                      const char *exportTargetName,
                      const char *destIp,
                      int destPort,
                      const char *username,
                      const unsigned int userIdUnique)
{
    Status status = StatusUnknown;
    ExExportTarget *target;
    XcalarWorkItem *workItem = NULL;
    char *url;

    workItem = xcalarApiMakeListExportTargetsWorkItem("file", exportTargetName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status =
        xcalarApiQueueWork(workItem, destIp, destPort, username, userIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (workItem->output->outputResult.listTargetsOutput.numTargets < 1) {
        status = StatusInval;
        goto CommonExit;
    }

    target = &workItem->output->outputResult.listTargetsOutput.targets[0];
    url = target->specificInput.sfInput.url;
    strlcpy(exportDir, url, bufSize);

    assert(status == StatusOk);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
        target = NULL;
        url = NULL;
    }

    return status;
}

Status
xcalarApiDeleteTable(const char *tableName,
                     const char *destIp,
                     int destPort,
                     const char *username,
                     const unsigned int userIdUnique,
                     const char *sessionName)
{
    XcalarWorkItem *workItem;
    Status status;

    workItem =
        xcalarApiMakeDeleteDagNodesWorkItem(tableName, SrcTable, sessionName);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status =
        xcalarApiQueueWork(workItem, destIp, destPort, username, userIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status.fromStatusCode(workItem->output->hdr.status);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->outputSize ==
           XcalarApiSizeOfOutput(
               workItem->output->outputResult.deleteDagNodesOutput) +
               sizeof(workItem->output->outputResult.deleteDagNodesOutput
                          .statuses[0]));

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

Status
xcalarApiFreeResultSet(uint64_t resultSetId,
                       const char *destIp,
                       int destPort,
                       const char *username,
                       const unsigned int userIdUnique)
{
    XcalarWorkItem *workItem;
    Status status;

    workItem = xcalarApiMakeFreeResultSetWorkItem(resultSetId);
    assert(workItem != NULL);

    status =
        xcalarApiQueueWork(workItem, destIp, destPort, username, userIdUnique);
    xcalarApiFreeWorkItem(workItem);
    workItem = NULL;

    return status;
}

Status
xcalarApiResultSetAbsolute(uint64_t resultSetId,
                           uint64_t position,
                           const char *destIp,
                           int destPort,
                           const char *username,
                           const unsigned int userIdUnique)
{
    XcalarWorkItem *workItem = NULL;
    Status status;

    workItem = xcalarApiMakeResultSetAbsoluteWorkItem(resultSetId, position);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status =
        xcalarApiQueueWork(workItem, destIp, destPort, username, userIdUnique);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->output != NULL);
    assert(workItem->outputSize ==
           XcalarApiSizeOfOutput(workItem->output->outputResult.noOutput));

    status.fromStatusCode(workItem->output->hdr.status);

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    return status;
}

// ========================== Network interface to send workItem ==============
Status
xcalarApiQueueWork(XcalarWorkItem *workItem,
                   const char *destIp,
                   int destPort,
                   const char *username,
                   unsigned int userIdUnique)
{
    SocketAddr destAddr;
    SocketHandle destFd;
    Status status;
    bool destFdCreated = false;
    size_t ret;

    if (workItem->userId == NULL) {
        workItem->userId =
            (XcalarApiUserId *) memAllocExt(sizeof(XcalarApiUserId),
                                            moduleName);
        if (workItem->userId == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
        workItem->userIdSize = sizeof(XcalarApiUserId);
        ret = snprintf(workItem->userId->userIdName,
                       sizeof(workItem->userId->userIdName),
                       "%s",
                       username);
        if (ret >= sizeof(workItem->userId->userIdName)) {
            status = StatusOverflow;
            goto CommonExit;
        }
        workItem->userId->userIdUnique = userIdUnique;
    } else {
        assert(strcmp(workItem->userId->userIdName, username) == 0);
        assert(workItem->userId->userIdUnique == userIdUnique);
    }

    status = sockCreate(destIp,
                        destPort,
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &destFd,
                        &destAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }
    destFdCreated = true;

    // Because an API can take ages. E.g. running functional test
    status = sockSetOption(destFd, SocketOptionKeepAlive);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set keepalive flag on socket %d: %s",
                destFd,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = sockConnect(destFd, &destAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(workItem->workItemLength == sizeof(*workItem));
    status = sockSendConverged(destFd, workItem, workItem->workItemLength);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "sockSendConverged of workItem failed socket %d: %s",
                destFd,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Send input
    if (workItem->inputSize > 0) {
        status =
            sockSendConverged(destFd, workItem->input, workItem->inputSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sockSendConverged of workItem Input failed socket %d: %s",
                    destFd,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Send userId, if this work item has one
    if (workItem->userIdSize > 0) {
        status =
            sockSendConverged(destFd, workItem->userId, workItem->userIdSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sockSendConverged of userId failed socket %d: %s",
                    destFd,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // Send sessionInfo, if this work item has one
    if (workItem->sessionInfoSize > 0) {
        status = sockSendConverged(destFd,
                                   workItem->sessionInfo,
                                   workItem->sessionInfoSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sockSendConverged of sessionInfo failed socket %d: %s",
                    destFd,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // At this point the API will be processed. It might be a very long
    // time before we hear a response from the server. So we're going to rely
    // on sockSelect with timeouts to poll, instead of potentially blocking
    // forever
    status = sockSetOption(destFd, SocketOptionNonBlock);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set nonblocking flag on socket %d: %s",
                destFd,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (workItem->api != XcalarApiShutdownLocal) {
        struct pollfd pollFd;
        int descReady = 0;
        while (descReady == 0) {
            pollFd.fd = destFd;
            pollFd.events = POLLIN | POLLPRI | POLLRDHUP;
            pollFd.revents = 0;

            errno = 0;
            descReady = ppoll(&pollFd, 1, NULL, NULL);
            if (descReady < 0) {
                if (errno == EINTR) {
                    descReady = 0;
                    continue;
                }
                status = sysErrnoToStatus(errno);
                xSyslog(moduleName,
                        XlogErr,
                        "ppoll(%d) failed: %s",
                        destFd,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }

        // Get outputSize
        status = sockRecvConverged(destFd,
                                   &workItem->outputSize,
                                   sizeof(workItem->outputSize));
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sockRecvConverged(%d) failed to get outputSize: %s",
                    destFd,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } else {
        workItem->outputSize = 0;
    }

    if (workItem->outputSize > 0) {
        assert(workItem->output == NULL);
        workItem->output =
            (XcalarApiOutput *) memAllocExt(workItem->outputSize, moduleName);
        if (workItem->output == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate output. "
                    "(Required size: %lu bytes)",
                    workItem->outputSize);
            status = StatusNoMem;
            goto CommonExit;
        }
        workItem->output->hdr.status = StatusUnknown.code();
        status =
            sockRecvConverged(destFd, workItem->output, workItem->outputSize);

        if (status == StatusOk &&
            workItem->output->hdr.status == StatusOk.code()) {
            status =
                xcalarApiDeserializeOutput(workItem->api,
                                           &workItem->output->outputResult);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Error deserializing output for %s: %s",
                        strGetFromXcalarApis(workItem->api),
                        strGetFromStatus(status));
            }
        } else {
            xSyslog(moduleName,
                    XlogErr,
                    "sockRecvConverged(%d) failed to get output: %s",
                    destFd,
                    status != StatusOk
                        ? strGetFromStatus(status)
                        : strGetFromStatusCode(workItem->output->hdr.status));
        }
    }

CommonExit:
    if (destFdCreated) {
        sockDestroy(&destFd);
        destFdCreated = false;
    }
    workItem->status = status.code();

    return status;
}

ServiceSocket::~ServiceSocket()
{
    if (fd_ != -1) {
        assert(fd_ > 0);

        int ret;
        do {
            ret = ::close(fd_);
        } while (ret == -1 && errno == EINTR);
    }
}

Status
ServiceSocket::init()
{
    Status status;
    struct sockaddr_un sockAddr;
    sockAddr.sun_family = AF_UNIX;
    if ((size_t) snprintf(sockAddr.sun_path,
                          sizeof(sockAddr.sun_path),
                          "%s",
                          UnixDomainSocketPath.c_str()) >=
        sizeof(sockAddr.sun_path)) {
        xSyslog("xcmgmtd",
                XlogErr,
                "Unix domain socket path, '%s', longer than %lu",
                UnixDomainSocketPath.c_str(),
                sizeof(sockAddr.sun_path));
        return (StatusNoBufs);
    }

    fd_ = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd_ < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog("xcmgmtd",
                XlogErr,
                "Failed to create socket: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (::connect(fd_,
                  (struct sockaddr *) &sockAddr,
                  sizeof(struct sockaddr_un)) != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog("xcmgmtd",
                XlogErr,
                "Failed to connect to %s: %s",
                sockAddr.sun_path,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    return (status);
}

// Protobuf related service

Status
ServiceSocket::sendRequest(const ProtoRequestMsg *request,
                           ProtoResponseMsg *response)
{
    Status status;
    ProtoMsg sendMessage;
    ProtoMsg recvMessage;
    uint8_t *sendBuf = NULL;
    size_t sendBufSize;
    uint8_t *recvBuf = NULL;
    size_t recvBufSize;
    bool success;

    try {
        sendMessage.set_type(ProtoMsgTypeRequest);
        sendMessage.mutable_request()->CopyFrom(*request);
    } catch (std::exception) {
        status = StatusNoMem;
        goto CommonExit;
    }

    sendBufSize = sendMessage.ByteSizeLong();

    sendBuf = new (std::nothrow) uint8_t[sendBufSize];
    BailIfNull(sendBuf);

    success = sendMessage.SerializeToArray(sendBuf, sendBufSize);
    if (!success) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    // This is what "length prefixed" means. Always send 8 byte size first.
    status = sockSendConverged(fd_, (void *) &sendBufSize, sizeof(sendBufSize));
    if (status != StatusOk) {
        xSyslog("xcmgmtd",
                XlogErr,
                "Failed to send message: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Send serialized message.
    status = sockSendConverged(fd_, sendBuf, sendBufSize);
    if (status != StatusOk) {
        xSyslog("xcmgmtd",
                XlogErr,
                "Failed to send message: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = sockRecvConverged(fd_, &recvBufSize, sizeof(recvBufSize));
    BailIfFailed(status);

    recvBuf = new (std::nothrow) uint8_t[recvBufSize];
    BailIfNull(recvBuf);

    status = sockRecvConverged(fd_, recvBuf, recvBufSize);
    BailIfFailed(status);

    success = recvMessage.ParseFromArray(recvBuf, recvBufSize);
    if (!success) {
        status = StatusProtobufDecodeError;
        goto CommonExit;
    }

    response->CopyFrom(recvMessage.response());

CommonExit:
    if (sendBuf) {
        delete[] sendBuf;
    }
    if (recvBuf) {
        delete[] recvBuf;
    }
    return (status);
}
