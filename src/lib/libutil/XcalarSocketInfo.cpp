// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include "util/XcalarSocketInfo.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"

#include <sys/socket.h>
#include "StrlFunc.h"

static const char *moduleName = "XcalarSocketInfo";

XcalarSocketInfo::XcalarSocketInfo()
{
    socketDesc_ = SocketHandleInvalid;
}

XcalarSocketInfo::~XcalarSocketInfo()
{
    assert(socketDesc_ == SocketHandleInvalid);
}

void
XcalarSocketInfo::destroySocket()
{
    if (socketDesc_ != SocketHandleInvalid) {
        shutdown(socketDesc_, SHUT_RDWR);
        sockDestroy(&socketDesc_);
    }

    socketInfoInited_ = false;
}

XcalarListeningSocketInfo::XcalarListeningSocketInfo(int portNumber)
{
    portNumber_ = portNumber;
}

XcalarListeningSocketInfo::XcalarListeningSocketInfo(int portNumber,
                                                     int maxClientsWaiting)
{
    portNumber_ = portNumber;
    maxClientsWaiting_ = maxClientsWaiting;
}

XcalarListeningSocketInfo::~XcalarListeningSocketInfo()
{
    assert(socketDesc_ == SocketHandleInvalid);
}

Status
XcalarListeningSocketInfo::createSocket()
{
    Status status = StatusOk;
    SocketHandle newSocket = SocketHandleInvalid;
    SocketAddr socketAddr;

    status = sockCreate(SockIPAddrAny,
                        portNumber_,
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &newSocket,
                        &socketAddr);
    BailIfFailed(status);

    status = sockSetOption(newSocket, SocketOptionReuseAddr);
    BailIfFailed(status);

    status = sockSetOption(newSocket, SocketOptionCloseOnExec);
    BailIfFailed(status);

    status = sockBind(newSocket, &socketAddr);
    BailIfFailed(status);

    status = sockListen(newSocket, maxClientsWaiting_);
    BailIfFailed(status);

    socketDesc_ = newSocket;

CommonExit:

    if (status != StatusOk) {
        assert(socketDesc_ == SocketHandleInvalid);
        if (newSocket != SocketHandleInvalid) {
            sockDestroy(&newSocket);
        }
        xSyslog(moduleName,
                XlogErr,
                "%s() failure: %s",
                __func__,
                strGetFromStatus(status));
    }

    return status;
}

XcalarConnectedSocketInfo::XcalarConnectedSocketInfo()
{
    socketInfoInited_ = false;
    portNumber_ = InvalidPortNumber;
}

XcalarConnectedSocketInfo::~XcalarConnectedSocketInfo()
{
    assert(socketDesc_ == SocketHandleInvalid);
}

Status
XcalarConnectedSocketInfo::initSocketInfo(const char *ipAddress, int portNumber)
{
    Status status = StatusOk;
    int ipAddrLength = 0;

    assert(ipAddress != NULL);
    assert(socketInfoInited_ == false);

    ipAddrLength = strlen(ipAddress);
    if (ipAddrLength <= 0) {
        return StatusInval;
    }

    strlcpy(ipAddress_, ipAddress, ipAddrLength + 1);

    portNumber_ = portNumber;

    socketInfoInited_ = true;

    return status;
}

void
XcalarConnectedSocketInfo::tearDownSocketInfo()
{
    destroySocket();
}

Status
XcalarConnectedSocketInfo::createSocket()
{
    Status status = StatusOk;
    SocketHandle newSocket = SocketHandleInvalid;
    SocketAddr socketAddr;

    assert(socketInfoInited_ == true);

    assert(ipAddress_ != NULL);
    assert(portNumber_ != InvalidPortNumber);

    status = sockCreate(ipAddress_,
                        portNumber_,
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &newSocket,
                        &socketAddr);
    BailIfFailed(status);

    status = sockSetOption(newSocket, SocketOptionCloseOnExec);
    BailIfFailed(status);

    status = sockConnect(newSocket, &socketAddr);
    BailIfFailed(status);

    socketDesc_ = newSocket;

CommonExit:

    if (status != StatusOk) {
        assert(socketDesc_ == SocketHandleInvalid);
        if (newSocket != SocketHandleInvalid) {
            sockDestroy(&newSocket);
        }
    }

    return status;
}
