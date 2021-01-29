// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XCALAR_SOCKET_INFO_
#define _XCALAR_SOCKET_INFO_

#include "SocketTypes.h"
#include "sys/Socket.h"
#include "primitives/Macros.h"
#include <arpa/inet.h>

enum class SocketConstants : uint32_t {
    XcalarApiIpAddrSize = INET6_ADDRSTRLEN,

    XcalarMaxPortNumber = UINT16_MAX,

    XcalarInvalidPortNumber = (XcalarMaxPortNumber + 1),
};

class XcalarSocketInfo
{
  public:
    XcalarSocketInfo();
    virtual ~XcalarSocketInfo();

    MustCheck SocketHandle getSocketDesc() { return socketDesc_; }

    void destroySocket();

    SocketHandle socketDesc_ = SocketHandleInvalid;
    bool socketInfoInited_ = false;
};

class XcalarListeningSocketInfo : public XcalarSocketInfo
{
  public:
    XcalarListeningSocketInfo(int portNumber_, int maxClientsWaiting);
    XcalarListeningSocketInfo(int portNumber_);
    virtual ~XcalarListeningSocketInfo();

    MustCheck Status createSocket();

  private:
    static constexpr int InvalidPortNumber = 0xbaada555;
    static constexpr int maxClientsWaitingDefault = 32;
    int portNumber_ = InvalidPortNumber;
    int maxClientsWaiting_ = maxClientsWaitingDefault;
};

class XcalarConnectedSocketInfo : public XcalarSocketInfo
{
  public:
    XcalarConnectedSocketInfo();
    virtual ~XcalarConnectedSocketInfo();

    MustCheck Status initSocketInfo(const char *ipAddress, int portNumber);

    void tearDownSocketInfo();

    MustCheck Status createSocket();

  private:
    static constexpr int InvalidPortNumber = 0xbaada555;
    char ipAddress_[(uint32_t) SocketConstants::XcalarApiIpAddrSize];
    int portNumber_ = InvalidPortNumber;
};

#endif  // _XCALAR_SOCKET_INFO_
