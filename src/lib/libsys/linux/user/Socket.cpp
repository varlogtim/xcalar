// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stddef.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/select.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <ifaddrs.h>
#include <net/if.h>

#include "primitives/Primitives.h"
#include "sys/Socket.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"
#include "util/MemTrack.h"

static constexpr const char *moduleName = "libsys::sockets";

enum {
    SocketBindRetries = 10
};

static Status sendRecvCommon(SocketHandle sockHandle, bool isSend,
                             void *buf, size_t bufLen, size_t *bytesXferred);
static Status sendRecvVCommon(SocketHandle sockHandle, bool isSend,
                              SgArray *sgArray, size_t *bytesXferred,
                              int flags);
static Status
sockSend(SocketHandle sockHandle, void *buf, size_t bufLen, size_t *bytesSent);

static Status
sockRecv(SocketHandle sockHandle, void *buf, size_t bufLen, size_t *bytesRecv);

typedef struct {
    uint64_t codewords[2];
} HandshakePacket;

static HandshakePacket handshakePacket = {{ 0x7863616c61726e65ULL,
                                            0x7470726f746f636fULL }};

// NOTE: Keep enum strings in sync with SocketDomain enum defined in
// $XLRDIR/src/include/linux/user/SocketTypes.h
static constexpr const char *SocketDomainInet4Str = "SocketDomainInet4";
static constexpr const char *SocketDomainInet6Str = "SocketDomainInet6";
static constexpr const char *SocketDomainUnspecifiedStr =
                                                    "SocketDomainUnspecified";
static constexpr const char *SocketDomainUnknownStr = "SocketDomainUnknown";

static const char *
sockGetDomainStr(SocketDomain sockDomain)
{
    switch (sockDomain) {
    case SocketDomainInet4: return SocketDomainInet4Str;
    case SocketDomainInet6: return SocketDomainInet6Str;
    case SocketDomainUnspecified: return(SocketDomainUnspecifiedStr);
    default: return SocketDomainUnknownStr;
    }
}

// NOTE: Keep enum strings in sync with SocketType enum defined in
// $XLRDIR/src/include/linux/user/SocketTypes.h
static constexpr const char *SocketTypeStreamStr = "SocketTypeStream";
static constexpr const char *SocketTypeDgramStr = "SocketTypeDgram";
static constexpr const char *SocketTypeUnknownStr = "SocketTypeUnknown";

static const char *
sockGetTypeStr(SocketType sockType)
{
    switch (sockType) {
    case SocketTypeStream: return SocketTypeStreamStr;
    case SocketTypeDgram: return SocketTypeDgramStr;
    default: return SocketTypeUnknownStr;
    }
}


Status
sockCreate(const char *hostName, uint16_t port, SocketDomain domain,
           SocketType type, SocketHandle *sockHandle, SocketAddr *sockAddr)
{
    int fd;
    Status status = StatusUnknown;
    SocketDomain finalDomain = domain;

    assertStatic(sizeof(SocketHandle) == sizeof(int));
    assertStatic(SocketDomainInet4 == AF_INET);
    assertStatic(SocketDomainInet6 == AF_INET6);
    assertStatic(SocketDomainUnspecified == AF_UNSPEC);
    assertStatic(SocketTypeStream == (int)SOCK_STREAM);

    if (domain == SocketDomainUnspecified || sockAddr != NULL) {
        assert(sockAddr != NULL);
        status = sockGetAddr(hostName, port, domain, type, sockAddr);
        if (status != StatusOk) {
            *sockHandle = SocketHandleInvalid;
            xSyslog(moduleName, XlogErr, "sockGetAddr() failed in "
                    "sockCreate(hostName=%s, port=%d, domain=%s, type=%s): %s",
                    hostName, port, sockGetDomainStr(domain),
                    sockGetTypeStr(type), strGetFromStatus(status));
            goto CommonExit;
        }
    }

    if (domain == SocketDomainUnspecified) {
        assert(sockAddr != NULL);
        finalDomain = (SocketDomain) sockAddr->addrFamily;
    }

    if (finalDomain == SocketDomainUnspecified) {
        status = StatusAFNoSupport;
        xSyslog(moduleName, XlogErr, "sockCreate(hostName=%s, port=%d, "
                "domain=%s, type=%s) failed: sockGetAddr() returned unspecified"
                " addrFamily: %s",
                hostName, port, sockGetDomainStr(finalDomain),
                sockGetTypeStr(type), strGetFromStatus(status));
        goto CommonExit;
    }

    // @SymbolCheckIgnore
    if ((fd = socket(finalDomain, type, 0)) < 0) {
        *sockHandle = SocketHandleInvalid;
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName, XlogErr, "socket(%s, %s, 0) failed in sockCreate("
                "hostName=%s, port=%d, domain=%s, type=%s): %s",
                sockGetDomainStr(finalDomain), sockGetTypeStr(type),
                hostName, port, sockGetDomainStr(domain), sockGetTypeStr(type),
                strGetFromStatus(status));
        goto CommonExit;
    }

    *sockHandle = fd;

    status = StatusOk;

CommonExit:
    return status;
}

void
sockDestroy(SocketHandle *sockHandle)
{
    int fd = *sockHandle;

    close(fd);
    *sockHandle = SocketHandleInvalid;
}

Status
sockSetOption(SocketHandle sockHandle, SocketOption option)
{
    int ret;
    int setSockOptions = 1;
    int curFlags;
    Status sts;

    assertStatic(O_NONBLOCK == SocketOptionNonBlock);
    assertStatic(FD_CLOEXEC == SocketOptionCloseOnExec);
    assertStatic(SO_REUSEADDR == SocketOptionReuseAddr);
    assertStatic(SO_KEEPALIVE == SocketOptionKeepAlive);

    switch (option) {
    case SocketOptionReuseAddr:
        // @SymbolCheckIgnore
        ret = setsockopt(sockHandle, SOL_SOCKET, option,
                         (char *) &setSockOptions, sizeof(setSockOptions));
        break;
    case SocketOptionKeepAlive:
        // @SymbolCheckIgnore
        ret = setsockopt(sockHandle, SOL_SOCKET, option,
                         (char *) &setSockOptions, sizeof(setSockOptions));
        break;
    case SocketOptionNonBlock:
        ret = fcntl(sockHandle, F_GETFL, 0);
        if (ret < 0) {
            break;
        }
        curFlags = ret;
        ret = fcntl(sockHandle, F_SETFL, curFlags | option);
        break;
    case SocketOptionCloseOnExec:
        ret = fcntl(sockHandle, F_GETFD, 0);
        if (ret < 0) {
            break;
        }
        curFlags = ret;
        ret = fcntl(sockHandle, F_SETFD, curFlags | option);
        break;
    default:
        ret = -1;
        errno = StatusInval.code();
        assert(0);
    }

    sts = StatusOk;
    if (ret < 0) {
        sts = sysErrnoToStatus(errno);
    }

    return sts;
}

Status
sockSetRecvTimeout(SocketHandle sockHandle, uint64_t timeoutInUSeconds)
{
    Status status = StatusOk;
    int ret;
    struct timeval timeout;

    timeout.tv_sec = timeoutInUSeconds / USecsPerSec;
    timeout.tv_usec = timeoutInUSeconds % USecsPerSec;

    ret = setsockopt(sockHandle, SOL_SOCKET, SO_RCVTIMEO,
                (void *) &timeout, (socklen_t) sizeof(timeout));

    if (ret < 0) {
        status = sysErrnoToStatus(errno);
    }

    return status;
}

static inline bool
sockIsLinkLocal(SocketAddr *sockAddr)
{
    assert(sockAddr->addrFamily == SocketDomainInet6);
    assert(sockAddr->addrLen == sizeof(sockAddr->ipv6));
    return sockAddr->ipv6.sin6_addr.__in6_u.__u6_addr8[0] == 0xfe &&
            sockAddr->ipv6.sin6_addr.__in6_u.__u6_addr8[1] == 0x80;
}

static Status
sockGetScope(SocketAddr *sockAddr)
{
    struct ifaddrs *ifAddrs = NULL;
    int ret;
    Status status = StatusUnknown;

    errno = 0;
    ret = getifaddrs(&ifAddrs);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = StatusEAINoData;
    for (struct ifaddrs *ifAddr = ifAddrs; ifAddr != NULL;
         ifAddr = ifAddr->ifa_next) {
        struct sockaddr_in6 *ifaAddr = NULL;

        if (ifAddr->ifa_addr == NULL ||
            ifAddr->ifa_addr->sa_family != AF_INET6) {
            continue;
        }

        ifaAddr = (typeof(*ifaAddr) *) ifAddr->ifa_addr;

        if (memcmp(&ifaAddr->sin6_addr,
                   &sockAddr->ipv6.sin6_addr,
                   sizeof(sockAddr->ipv6.sin6_addr)) == 0) {
            errno = 0;
            ret = if_nametoindex(ifAddr->ifa_name);
            if (ret == 0) {
                status = sysErrnoToStatus(errno);
                goto CommonExit;
            }
            sockAddr->ipv6.sin6_scope_id = ret;
            status = StatusOk;
            break;
        }
    }

CommonExit:
    if (ifAddrs != NULL) {
        freeifaddrs(ifAddrs);
        ifAddrs = NULL;
    }

    return status;
}

Status
sockGetAddr(const char *hostName, uint16_t port, SocketDomain domain,
            SocketType type, SocketAddr *sockAddr)
{
    struct addrinfo hints;
    char portCStr[16];
    struct sockaddr_in *sockAddrIpv4 = NULL;
    struct sockaddr_in6 *sockAddrIpv6 = NULL;
    int ret = 0;
    struct addrinfo *result = NULL;
    struct addrinfo *rp;
    Status sts;

    memZero(&hints, sizeof(hints));
    hints.ai_family = domain;
    hints.ai_socktype = type;
    hints.ai_flags = (hostName == SockIPAddrAny) ? AI_PASSIVE : 0;
    // (AI_ADDRCONFIG | AI_V4MAPPED) are defaults if ai_flags is set
    // to NULL, so let's preserve that.
    // See http://man7.org/linux/man-pages/man3/getaddrinfo.3.html
    // for more information
    hints.ai_flags |= AI_NUMERICSERV | AI_ADDRCONFIG | AI_V4MAPPED;
    hints.ai_protocol = 0;
    snprintf(portCStr, sizeof(portCStr), "%hu", port);

    // Retry 3 times
    for (unsigned ii = 0; ii < 3; ii++) {
        // @SymbolCheckIgnore
        ret = getaddrinfo(hostName, portCStr, &hints, &result);
        if (ret != 0) {
            sts = sysEaiErrToStatus(ret);
            // See https://bugzilla.redhat.com/show_bug.cgi?id=1044628
            if (sts == StatusAgain ||
                (domain == SocketDomainUnspecified && sts == StatusEAINoName)) {
                sysSleep(1);
                continue;
            }
            return sts;
        }
        break;
    }

    if (ret != 0) {
        return sysEaiErrToStatus(ret);
    }

    // getaddrinfo(NULL) returns 0.0.0.0 and :: in this order. This is bad
    // because binding to :: actually listens on both ipV4 and ipV6, whereas
    // 0.0.0.0 only listens to ipv4 only. Thus, when hostName == NULL is when
    // we need to go against the order returned by getaddrinfo

    sts = StatusEAINoData;
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        if ((domain == SocketDomainInet4 ||
             domain == SocketDomainUnspecified) &&
            (rp->ai_addrlen == sizeof(*sockAddrIpv4)) &&
            XcalarConfig::get()->ipVersionOptions_ !=
                                     XcalarConfig::IpVersionOptions::IpV6Only) {
            assert(rp->ai_family == SocketDomainInet4);
            sockAddrIpv4 = ((typeof(*sockAddrIpv4) *) rp->ai_addr);

            sockAddr->ipv4 = *sockAddrIpv4;
            sockAddr->addrLen = rp->ai_addrlen;
            sockAddr->addrFamily = rp->ai_family;
            sts = StatusOk;

            if (XcalarConfig::get()->ipVersionOptions_ ==
                                    XcalarConfig::IpVersionOptions::FavorIpV4 ||
                XcalarConfig::get()->ipVersionOptions_ ==
                                     XcalarConfig::IpVersionOptions::IpV4Only ||
                (XcalarConfig::get()->ipVersionOptions_ ==
                              XcalarConfig::IpVersionOptions::UseLibcDefaults &&
                 hostName != SockIPAddrAny)) {
                break; // Found ipV4. We're good, stop searching
            }

        } else if ((domain == SocketDomainInet6 ||
                    domain == SocketDomainUnspecified) &&
                   (rp->ai_addrlen == sizeof(*sockAddrIpv6)) &&
                   XcalarConfig::get()->ipVersionOptions_ !=
                                     XcalarConfig::IpVersionOptions::IpV4Only) {
            assert(rp->ai_family == SocketDomainInet6);
            sockAddrIpv6 = ((typeof(*sockAddrIpv6) *) rp->ai_addr);

            sockAddr->ipv6 = *sockAddrIpv6;
            sockAddr->addrLen = rp->ai_addrlen;
            sockAddr->addrFamily = rp->ai_family;
            sts = StatusOk;

            if (XcalarConfig::get()->ipVersionOptions_ ==
                                    XcalarConfig::IpVersionOptions::FavorIpV6 ||
                XcalarConfig::get()->ipVersionOptions_ ==
                                     XcalarConfig::IpVersionOptions::IpV6Only ||
                XcalarConfig::get()->ipVersionOptions_ ==
                              XcalarConfig::IpVersionOptions::UseLibcDefaults) {
                break;
            }
        } else {
            continue;
        };
    }

    if (result != NULL) {
        freeaddrinfo(result);
        result = NULL;
    }

    if (sts == StatusOk && sockAddr->addrFamily == SocketDomainInet6 &&
        sockIsLinkLocal(sockAddr)) {
        sts = sockGetScope(sockAddr);
    }

    return sts;
}

Status
sockInitiateHandshake(SocketHandle sockHandle)
{
    Status sts;
    size_t bytesSent = 0;
    sts = sockSend(sockHandle, &handshakePacket, sizeof(handshakePacket),
                   &bytesSent);
    if (bytesSent != sizeof(handshakePacket)) {
        sts = StatusConnectionWrongHandshake;
    }
    return(sts);
}

Status
sockConnect(SocketHandle sockHandle, const SocketAddr *sockAddr)
{
    int ret;
    Status sts = StatusOk;

    assertStatic(sizeof(*sockAddr) >= sizeof(struct sockaddr));

    // @SymbolCheckIgnore
    ret = connect(sockHandle, &sockAddr->ip, sockAddr->addrLen);
    if (ret < 0) {
        sts = sysErrnoToStatus(errno);
        return sts;
    }

    sts = sockInitiateHandshake(sockHandle);
    if (sts != StatusOk) {
        sockDestroy(&sockHandle);
    }
    return sts;
}

Status
sockBind(SocketHandle sockHandle, const SocketAddr *sockAddr)
{
    int ret;
    Status sts = StatusOk;
    unsigned retryCount = 0;

    do {
        // @SymbolCheckIgnore
        ret = bind(sockHandle, &sockAddr->ip, sockAddr->addrLen);
        if (ret < 0) {
            sts = sysErrnoToStatus(errno);
            sysSleep(1);
        } else {
            sts = StatusOk;
        }
    } while (sts != StatusOk && retryCount++ < SocketBindRetries);

    return sts;
}

Status
sockListen(SocketHandle sockHandle, unsigned maxClientsWaiting)
{
    int ret;
    Status sts;

    // @SymbolCheckIgnore
    ret = listen(sockHandle, maxClientsWaiting);
    sts = StatusOk;
    if (ret < 0) {
        sts = sysErrnoToStatus(errno);
    }

    return sts;
}

Status
sockAccept(SocketHandle masterHandle, SocketAddr *newClientAddr,
           SocketHandle *newClientHandle)
{
    int ret;
    int fd = SocketHandleInvalid;
    Status sts;

    *newClientHandle = SocketHandleInvalid;

    if (newClientAddr != NULL) {
        assertStatic(sizeof(newClientAddr->ipv6) >= sizeof(newClientAddr->ip));
        newClientAddr->addrLen = sizeof(newClientAddr->ipv6);
        // @SymbolCheckIgnore
        ret = accept(masterHandle, &newClientAddr->ip, &newClientAddr->addrLen);
    } else {
        ret = accept(masterHandle, NULL, NULL);
    }
    sts = StatusOk;
    if (ret < 0) {
        if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
            sts = sysErrnoToStatus(EAGAIN);
        } else {
            sts = sysErrnoToStatus(errno);
        }
        return sts;
    }
    fd = ret;

    HandshakePacket incomingHandshake;
    size_t bytesRecv;
    sts = sockRecv(fd, &incomingHandshake, sizeof(incomingHandshake),
                   &bytesRecv);
    if (sts == StatusOk) {
        if (bytesRecv != sizeof(handshakePacket) ||
            memcmp(&incomingHandshake, &handshakePacket,
                   sizeof(handshakePacket)) != 0) {
            sts = StatusConnectionWrongHandshake;
        }
    }

    if (sts != StatusOk) {
        sockDestroy(&fd);
    }

    *((int *) newClientHandle) = fd;

    return sts;
}

static Status
sockSend(SocketHandle sockHandle, void *buf, size_t bufLen, size_t *bytesSent)
{
    return sendRecvCommon(sockHandle, true, buf, bufLen, bytesSent);
}

static Status
sockRecv(SocketHandle sockHandle, void *buf, size_t bufLen, size_t *bytesRecv)
{
    return sendRecvCommon(sockHandle, false, buf, bufLen, bytesRecv);
}

Status
sockSendConverged(SocketHandle sockHandle, void *buf, size_t bufLen)
{
    Status status = StatusUnknown;
    size_t totalBytesXfer;
    size_t curBytesXfer;

    assert(bufLen > 0);

    for (totalBytesXfer = 0; totalBytesXfer < bufLen; totalBytesXfer +=
         curBytesXfer) {
        status = sockSend(sockHandle, ((uint8_t *) buf) + totalBytesXfer,
                          bufLen - totalBytesXfer, &curBytesXfer);

        if (status == StatusAgain) {
            curBytesXfer = 0;
            continue;
        } else if (status != StatusOk) {
            break;
        } else if (curBytesXfer == 0) {
            status = StatusConnReset;
            break;
        }
    }

    return(status);
}

Status
sockRecvConverged(SocketHandle sockHandle, void *buf, size_t bufLen)
{
    Status status = StatusUnknown;
    size_t totalBytesXfer;
    size_t curBytesXfer;

    assert(bufLen > 0);

    for (totalBytesXfer = 0; totalBytesXfer < bufLen; totalBytesXfer +=
         curBytesXfer) {
        status = sockRecv(sockHandle, ((uint8_t *) buf) + totalBytesXfer,
                          bufLen - totalBytesXfer, &curBytesXfer);

        if (status == StatusAgain) {
            curBytesXfer = 0;
            continue;
        } else if (status != StatusOk) {
            break;
        } else if (curBytesXfer == 0) {
            status = StatusConnReset;
            break;
        }
    }

    return(status);
}

Status
sendRecvCommon(SocketHandle sockHandle, bool isSend, void *buf,
               size_t bufLen, size_t *bytesXferred)
{
    SgArray *sgArray = (SgArray *) memAlloc(sizeof(SgArray) + sizeof(SgElem));
    if (sgArray == NULL) {
        return StatusNoMem;
    }
    SgElem *elem = sgArray->elem;

    sgArray->numElems = 1;
    elem[0].baseAddr = buf;
    elem[0].len = bufLen;
    Status status = sendRecvVCommon(sockHandle, isSend, sgArray, bytesXferred, 0);
    if (sgArray != NULL) {
        memFree(sgArray);
        sgArray = NULL;
    }
    return status;
}

Status
sockSendV(SocketHandle sockHandle, SgArray *sgArray, size_t *bytesSent,
          int flags)
{
    return sendRecvVCommon(sockHandle, true, sgArray, bytesSent, flags);
}

Status
sockRecvV(SocketHandle sockHandle, SgArray *sgArray, size_t *bytesRecv)
{
    return sendRecvVCommon(sockHandle, false, sgArray, bytesRecv, 0);
}

Status
sendRecvVCommon(SocketHandle sockHandle, bool isSend, SgArray *sgArray,
                size_t *bytesXferred, int flags)
{
    ssize_t ret;
    size_t bytesRequested;
    unsigned i;
    Status sts;

    assertStatic(sizeof(struct iovec) == sizeof(SgElem));
    assertStatic(offsetof(struct iovec, iov_base) ==
                  offsetof(SgElem, baseAddr));
    assertStatic(offsetof(struct iovec, iov_len) ==
                  offsetof(SgElem, len));

#ifdef DEBUG
    bytesRequested = 0;
    for (i = 0; i < sgArray->numElems; i++) {
        bytesRequested += sgArray->elem[i].len;
    }
    assert(bytesRequested > 0);
#else
    bytesRequested = 0;
#endif

    struct msghdr msgHdr = {
        .msg_name = NULL,
        .msg_namelen = 0,
        .msg_iov = (struct iovec *)sgArray->elem,
        .msg_iovlen = sgArray->numElems,
        .msg_control = NULL,
        .msg_controllen = 0,
        .msg_flags = 0,
    };

    if (isSend) {
        ret = sendmsg(sockHandle, &msgHdr, MSG_NOSIGNAL | flags);
    } else {
        ret = recvmsg(sockHandle, &msgHdr, 0);
    }

    sts = StatusOk;
    if (ret < 0) {
        assert(ret != ENOTSOCK);
        sts = sysErrnoToStatus(errno);
        ret = 0;
    }
    *bytesXferred = ret;

    return sts;
}

void
sockHandleSetInit(SocketHandleSet *handleSet)
{
    assertStatic(sizeof(fd_set) == sizeof(handleSet->privFdSet));

    FD_ZERO((fd_set *) handleSet->privFdSet);
    handleSet->privMaxFd = 1;
    handleSet->padding = 0;
}

void
sockHandleSetAdd(SocketHandleSet *handleSet, SocketHandle sockHandle)
{
    int fd = sockHandle;

    assert(fd >= 0 && fd < FD_SETSIZE);

    FD_SET(fd, (fd_set *) handleSet->privFdSet);
    if ((unsigned) fd >= handleSet->privMaxFd) {
        handleSet->privMaxFd = fd + 1;
    }
}

void
sockHandleSetRemove(SocketHandleSet *handleSet, SocketHandle sockHandle)
{
    int fd = sockHandle;

    assert(fd >= 0 && fd < FD_SETSIZE);

    FD_CLR(fd, (fd_set *) handleSet->privFdSet);
}

bool
sockHandleSetIncludes(const SocketHandleSet *handleSet,
                      SocketHandle sockHandle)
{
    int fd = sockHandle;

    assert(fd >= 0 && fd < FD_SETSIZE);
    return FD_ISSET(fd, (fd_set *) handleSet->privFdSet);
}

Status
sockSelect(const SocketHandleSet *readHandles,
           const SocketHandleSet *writeHandles,
           const SocketHandleSet *exceptHandles, const struct timespec *timeout,
           unsigned *numReady)
{
    int ret;
    unsigned maxFd = 1;

    assertStatic(sizeof(*timeout) == sizeof(struct timespec));

    if (readHandles) {
        maxFd = xcMax(maxFd, readHandles->privMaxFd);
    }
    if (writeHandles) {
        maxFd = xcMax(maxFd, writeHandles->privMaxFd);
    }
    if (exceptHandles) {
        maxFd = xcMax(maxFd, exceptHandles->privMaxFd);
    }

    assert(maxFd <= FD_SETSIZE);

    ret = pselect(maxFd,
                  (readHandles) ? (fd_set *) readHandles->privFdSet : NULL,
                  (writeHandles) ? (fd_set *) writeHandles->privFdSet : NULL,
                  (exceptHandles) ? (fd_set *) exceptHandles->privFdSet : NULL,
                  (struct timespec *) timeout, NULL);
    if (ret < 0) {
        return sysErrnoToStatus(errno);
    }
    if (numReady) {
        *numReady = ret;
    }

    return StatusOk;
}

SocketHandle
sockHandleFromInt(int fd)
{
    SocketHandle ret;

    ret = fd;

    return ret;
}
