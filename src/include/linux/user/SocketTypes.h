// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SOCKETTYPES_H_
#define _SOCKETTYPES_H_

#include "config.h"
#include "primitives/Primitives.h"

#include <sys/socket.h>
#include <netinet/in.h>

typedef int32_t SocketHandle;

enum : SocketHandle {
    SocketHandleInvalid = -1,
};

typedef struct SocketAddr {
    union {
        struct sockaddr ip;
        struct sockaddr_in ipv4;
        struct sockaddr_in6 ipv6;
    };
    int addrFamily;
    socklen_t addrLen;

    SocketAddr() { addrLen = sizeof(ipv6); }

} SocketAddr;

typedef enum {
    SocketDomainInet4 = AF_INET,
    SocketDomainInet6 = AF_INET6,
    SocketDomainUnspecified = AF_UNSPEC,
    // NOTE: For a new enum, add a string for the enum in sockGetDomainStr() in
    // lib/libsys/linux/user/Socket.cpp. This is for failure diagnosability.
} SocketDomain;

typedef enum {
    SocketTypeStream = SOCK_STREAM,
    SocketTypeDgram = SOCK_DGRAM,
    // NOTE: For a new enum, add a string for the enum in sockGetTypeStr() in
    // lib/libsys/linux/user/Socket.cpp. This is for failure diagnosability.
} SocketType;

typedef enum {
    SocketOptionCloseOnExec = 1,
    SocketOptionReuseAddr = 2,
    SocketOptionKeepAlive = 9,
    SocketOptionNonBlock = 04000,
} SocketOption;

#define SockIPAddrAny (NULL)

typedef struct {
    unsigned privMaxFd;
    unsigned padding;
    Opaque privFdSet[SIZEOF_FD_SET];
} Aligned(sizeof(uintptr_t)) SocketHandleSet;

#endif  // _SOCKETTYPES_H_
