// Copyright 2015, 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// connection.cpp: Cluster monitor socket connection manager

#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <getopt.h>
#include <sys/epoll.h>

#include <unordered_map>

#include "monitor/MonConnection.h"
#include "LibMonInterfaceConstants.h"

using namespace monIface;

static constexpr const char *_moduleName = "libmoninterface-connection";
typedef std::unordered_map<SocketHandle, Ref<Connection> > CnxMap;
static CnxMap cnxMap;
static int epollFD;

uint32_t Connection::_nextCnxId = 1000;

// TCP keepalive settings:

int keepIdle = 3600;
int keepCnt = 15;
int keepIntvl = 120;

// keepIdle: time a connection can be idle before probes are sent
// keepCnt: no. of probes sent if TCP connection's idle for keepIdle
// keepIntvl: time interval between probes
// timeout = (keepCnt * keepIntvl)
//
// Note: keepCnt * keepIntvl must be sufficiently large to prevent premature
// shootdown; with a value of 15, and 120 respectively, this is 30 min. See
// comments below for explanation:
//
// These are SO_KEEPALIVE settings per-TCP-socket (for time, units are in
// seconds). See comments for SO_KEEPALIVE in monitor.cpp for background. See
// socket(7) for the SO_KEEPALIVE description.
//
// The main aspect to note about the use of TCP keepalive here is that the
// connections between a master and its slaves will be idle during steady state
// (master does not send heartbeats by default - see sendHeartBeats in
// monitor.cpp). So, in effect, the time for keepIdle is the time between the
// keepalive mechanism sending probes (effectively TCP heartbeats): so in steady
// state, every keepIdle seconds, keepalive probes are sent over the connections
// - typically only one probe will be sent (since it'll be ack'ed).
//
// However, if the peer isn't accessible, multiple (upto a max of keepCnt)
// probes are sent with an interval of keepIntvl seconds between probes. If none
// of them get an ack, the peer is considered dead, and eventually this results
// in the cluster being brought down. So, effectively, (keepCnt * keepIntvl) is
// the real timeout (keepIdle isn't included) - if a peer isn't accessible for
// (keepCnt * keepIntvl) seconds, it's considered dead. So this time should be
// sufficiently large to prevent a premature shutdown.
//
// How does a keepalive timeout manifest itself in the monitor? A EPOLLIN event
// is returned for the fd/socket in the fdset passed to epoll_wait(), and a
// subsequent read/write operation on the socket will return ETIMEDOUT. This
// will be noticed by the relevant monitor which is constantly polling the
// sockets for events, and the receipt of a ETIMEDOUT error will trigger the
// connection being closed, and the monitor dying after killing/reaping its
// usrnode. This will cascade to other monitors and the cluster will come down.

Connection *
Connection::CreateConnection(SocketHandle sockFd,
                             ConnectionType type,
                             uint32_t events,
                             SocketAddr *connectAddr,
                             Notifier *notifier,
                             MsgHandler *msgHandler)
{
    // XXX Do error handling for bad allocations. All this has to be moved
    // out of the constructor.
    Connection *cnx = new (std::nothrow)
        Connection(sockFd, type, connectAddr, notifier, msgHandler);
    cnxMap[sockFd] = cnx;

    if (epollFD != 0) {
        cnx->_epollEvent.events = events;
        if (epoll_ctl(epollFD, EPOLL_CTL_ADD, (int) sockFd, &cnx->_epollEvent) <
            0) {
            ERROR(_moduleName,
                  "CreatConnection epoll error for socket %d: %s",
                  epollFD,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            xcalarExit(1);
        }
    }

    return cnx;
}

void
createEPoll()
{
    if (epollFD == 0) {
        epollFD = epoll_create(1);
        if (epollFD < 0) {
            ERROR(_moduleName,
                  "CreatConnection epoll create error: %s",
                  strGetFromStatus(sysErrnoToStatus(errno)));
            xcalarExit(1);
        }
    }
}

void
Connection::CreateUDPConnection(const char *hostName,
                                int port,
                                Notifier *notifier,
                                MsgHandler *msgHandler,
                                Ref<Connection> &cnx)
{
    Status status = StatusUnknown;
    SocketHandle sockFd;
    SocketAddr sockAddr;

    createEPoll();

    status = sockCreate(hostName,
                        port,
                        SocketDomainUnspecified,
                        SocketTypeDgram,
                        &sockFd,
                        &sockAddr);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Could not create socket for host '%s' port %d: %s",
              hostName,
              port,
              strGetFromStatus(status));
        xcalarAbort();
    }

    // Descendants should not inherit this
    status = sockSetOption(sockFd, SocketOptionCloseOnExec);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Could not set CloseOnExec for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(status));
        xcalarAbort();
    }

    if (bind(sockFd, &sockAddr.ip, sockAddr.addrLen) < 0) {
        ERROR(_moduleName,
              "CreateUDPConnection bind error for host '%s' "
              "port %d socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        xcalarAbort();
    } else if (fcntl(sockFd, F_SETFL, O_NONBLOCK) < 0) {
        ERROR(_moduleName,
              "CreateUDPConnection fcntl error for host '%s' "
              "port %d socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        xcalarAbort();
    }
    cnx = CreateConnection(sockFd,
                           UDP_CONNECTION,
                           EPOLLIN,
                           NULL,
                           notifier,
                           msgHandler);
}

void
Connection::Listen(const char *hostName,
                   int port,
                   Notifier *notifier,
                   MsgHandler *msgHandler)
{
    struct protoent *pent;
    SocketHandle sockFd;
    Status status = StatusUnknown;
    SocketAddr sockAddr;

    createEPoll();

    status = sockCreate(hostName,
                        port,
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &sockFd,
                        &sockAddr);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Could not create socket for host '%s' port %d: %s",
              hostName,
              port,
              strGetFromStatus(status));
        xcalarAbort();
    }

    // Descendants should not inherit this
    status = sockSetOption(sockFd, SocketOptionCloseOnExec);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Could not set CloseOnExec for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(status));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    int reuon = 1;
    if (setsockopt(sockFd, SOL_SOCKET, SO_REUSEADDR, &reuon, sizeof(reuon)) <
        0) {
        ERROR(_moduleName,
              "Failed to set SO_REUSEADDR for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    int alon = 1;
    if (setsockopt(sockFd, SOL_SOCKET, SO_KEEPALIVE, &alon, sizeof(alon)) < 0) {
        ERROR(_moduleName,
              "Failed to set SO_KEEPALIVE for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    if ((pent = getprotobyname("TCP")) == NULL) {
        ERROR(_moduleName,
              "Failed to get TCP protocol info: %s",
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    if (setsockopt(sockFd,
                   pent->p_proto,
                   TCP_KEEPIDLE,
                   &keepIdle,
                   sizeof(keepIdle)) < 0) {
        ERROR(_moduleName,
              "Failed to set TCP_KEEPIDLE for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    if (setsockopt(sockFd,
                   pent->p_proto,
                   TCP_KEEPCNT,
                   &keepCnt,
                   sizeof(keepCnt)) < 0) {
        ERROR(_moduleName,
              "Failed to set TCP_KEEPCNT for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    if (setsockopt(sockFd,
                   pent->p_proto,
                   TCP_KEEPINTVL,
                   &keepIntvl,
                   sizeof(keepIntvl)) < 0) {
        ERROR(_moduleName,
              "Failed to set TCP_KEEPINTVL for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
        sockDestroy(&sockFd);
        xcalarAbort();
    }

    INFO(_moduleName, "Listening on port %d", port);

    if (bind(sockFd, &sockAddr.ip, sockAddr.addrLen) < 0) {
        ERROR(_moduleName,
              "Failed 'bind' for host '%s' port %d socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
    } else if (listen(sockFd, 5) < 0) {
        ERROR(_moduleName,
              "Failed 'listen' for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
    } else if (fcntl(sockFd, F_SETFL, O_NONBLOCK) < 0) {
        ERROR(_moduleName,
              "Failed to set O_NONBLOCK for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(sysErrnoToStatus(errno)));
    } else {
        CreateConnection(sockFd,
                         LISTENER_CONNECTION,
                         EPOLLIN,
                         NULL,
                         notifier,
                         msgHandler);
        return;
    }
    xcalarAbort();
}

void
Connection::Poll(uint64_t maxWaitTime)
{
    epoll_event events[cnxMap.size()];
    int count;

    // make epoll_wait restartable if interrupted by a signal or gdb
    // signal interruption for SIGTERM can't happen now since sigwait() is used.
    // xcmon shouldn't get any other signal, and if it does, why interrupt? It's
    // impacting debuggability of xcmon hence the change
    do {
        count = epoll_wait(epollFD, events, cnxMap.size(), maxWaitTime / 1000);
        if (count < 0 && errno == EINTR) {
            INFO(_moduleName, "Re-starting interrupted epoll_wait");
        }
    } while (count < 0 && errno == EINTR);

    if (count < 0) {
        ERROR(_moduleName, "epoll_wait failed");
        xcalarExit(1);
    }

    for (int i = 0; i < count; i++) {
        CnxMap::iterator iter = cnxMap.find(events[i].data.fd);
        if (iter == cnxMap.end()) {
            WARNING(_moduleName,
                    "Failed to find event %d in map",
                    events[i].data.fd);
            // Most likely due to SIGTERM handling having closed the
            // connection.
            xcalarExit(1);
        }
        //
        // Grab a reference since HandleEvent can destroy the connection
        // and we want to make sure that it doesn't get deleted until we
        // are done with it.
        //
        Ref<Connection> cnx = iter->second;
        cnx->HandleEvent(events[i].events);
    }
}

int
Connection::ReadMsg(bool wait)
{
    int flags = wait ? 0 : MSG_DONTWAIT;
    if (_hdrOffset < (int) sizeof(MsgHdr)) {
        ssize_t toRead = sizeof(MsgHdr) - _hdrOffset;
        ssize_t len =
            recv(_sockFd, (uint8_t *) &_msgHdr + _hdrOffset, toRead, flags);
        if (len < 0) {
            ERROR(_moduleName,
                  "ReadMsg recv error on socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            return errno;
        } else if (len == 0) {
            ERROR(_moduleName,
                  "ReadMsg recv of zero length on socket %d, "
                  "returning ECONNRESET",
                  _sockFd);
            return ECONNRESET;
        } else if (len < toRead) {
            INFO(_moduleName, "Read %ld < %ld bytes", len, toRead);
            _hdrOffset += len;
            return EWOULDBLOCK;
        }

        _hdrOffset = sizeof(MsgHdr);
        if (_msgHdr.messageLength > maxMessageSize) {
            WARNING(_moduleName,
                    "Message size %d on socket %d "
                    "exceeds maximum size %d for op %d",
                    _msgHdr.messageLength,
                    _sockFd,
                    maxMessageSize,
                    _msgHdr.op);
            return EFBIG;
        }

        if ((int) _msgHdr.messageLength > _msgBodyLength) {
            if (_msgBody != NULL) {
                delete[] _msgBody;
            }
            // XXX Do error handling for failed allocations
            _msgBody = new (std::nothrow) char[_msgHdr.messageLength];
            _msgBodyLength = _msgHdr.messageLength;
        }
        _bodyOffset = 0;
    }

    if (_msgHdr.messageLength > 0) {
        assert(_bodyOffset < (int) _msgHdr.messageLength);

        ssize_t toRead = _msgHdr.messageLength - _bodyOffset;
        ssize_t len = recv(_sockFd, _msgBody + _bodyOffset, toRead, flags);
        if (len < 0) {
            ERROR(_moduleName,
                  "ReadMsg recv error on socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            return errno;
        } else if (len == 0) {
            ERROR(_moduleName,
                  "ReadMsg recv of zero length on socket %d, "
                  "returning ECONNRESET",
                  _sockFd);
            return ECONNRESET;
        } else if (len < toRead) {
            INFO(_moduleName,
                 "Read %ld < %ld bytes on socket %d",
                 len,
                 toRead,
                 _sockFd);
            _bodyOffset += len;
            return EWOULDBLOCK;
        }
    }

    return 0;
}

int
Connection::SyncReadMsg(MsgOp &op, void *&msgBody, uint32_t &msgLength)
{
    int status = ReadMsg(true);
    if (status == 0) {
        op = (MsgOp) _msgHdr.op;
        msgBody = _msgBody;
        msgLength = _msgBodyLength;
        _hdrOffset = 0;
    }
    return status;
}

bool
Connection::AppendMsg(void *msgHdr,
                      size_t msgHdrLength,
                      void *msgBody,
                      uint32_t msgBodyLength)
{
    _msgQueueHead = _msgQueueTail =
        Message::Alloc(msgHdr, msgHdrLength, msgBody, msgBodyLength);
    _epollEvent.events = EPOLLIN | EPOLLOUT;
    if (epoll_ctl(epollFD, EPOLL_CTL_MOD, _sockFd, &_epollEvent) < 0) {
        ERROR(_moduleName,
              "Connection::AppendMsg failed epoll_ctl for socket %d: %s",
              epollFD,
              strGetFromStatus(sysErrnoToStatus(errno)));
        xcalarExit(1);
    }
    return true;
}

bool
Connection::SendMsg(MsgOp op, void *msg, uint32_t msgSize)
{
    MsgHdr hdr;
    memZero(&hdr, sizeof(hdr));
    hdr.op = op;
    hdr.flags = 0;
    hdr.messageLength = msgSize;

    if (_msgQueueHead != NULL) {
        Message *m = Message::Alloc(&hdr, sizeof(hdr), msg, msgSize);
        _msgQueueTail->SetNext(m);
        _msgQueueTail = m;
        INFO(_moduleName, "SendMsg: _msgQueueHead not NULL");
        return true;
    }

    if (msg == NULL) {
        ssize_t len = send(_sockFd, &hdr, sizeof(hdr), MSG_DONTWAIT);
        if (len < 0) {
            if (errno == EWOULDBLOCK) {
                INFO(_moduleName,
                     "SendMsg: non-blocking send for socket %d returned "
                     "EWOULDBLOCK",
                     _sockFd);
                return AppendMsg(&hdr, sizeof(hdr), msg, msgSize);
            } else {
                ERROR(_moduleName,
                      "SendMsg: send failed for socket %d: %s",
                      _sockFd,
                      strGetFromStatus(sysErrnoToStatus(errno)));
                Close();
                return false;
            }
        } else if (len < (int) sizeof(hdr)) {
            INFO(_moduleName,
                 "SendMsg: send len less than hdr for socket %d",
                 _sockFd);
            return AppendMsg(&hdr + len, sizeof(hdr) - len, msg, msgSize);
        } else {
            return true;
        }
    } else {
        iovec iov[2];
        iov[0].iov_base = &hdr;
        iov[0].iov_len = sizeof(hdr);
        iov[1].iov_base = msg;
        iov[1].iov_len = msgSize;
        msghdr msgHdr;
        memset(&msgHdr, 0, sizeof(msgHdr));
        msgHdr.msg_iov = iov;
        msgHdr.msg_iovlen = 2;
        int len = sendmsg(_sockFd, &msgHdr, MSG_DONTWAIT);
        if (len < 0) {
            if (errno == EWOULDBLOCK) {
                INFO(_moduleName,
                     "SendMsg: non-blocking send for socket %d returned "
                     "EWOULDBLOCK",
                     _sockFd);
                return AppendMsg(&hdr, sizeof(hdr), msg, msgSize);
            } else {
                ERROR(_moduleName,
                      "SendMsg: send failed for socket %d: %s",
                      _sockFd,
                      strGetFromStatus(sysErrnoToStatus(errno)));
                Close();
                return false;
            }
        } else if (len < (int) sizeof(hdr)) {
            return AppendMsg(&hdr, sizeof(hdr) - len, msg, msgSize);
        } else if (len < (int) sizeof(hdr) + (int) msgSize) {
            len -= sizeof(hdr);
            return AppendMsg(NULL, 0, (char *) msg + len, msgSize - len);
        }
    }

    return true;  // Fell through with no errors
}

void
Connection::CompleteMsgSend()
{
    INFO(_moduleName, "Connection::CompleteMsgSend");
    while (_msgQueueHead != NULL) {
        INFO(_moduleName, "Connection::CompleteMsgSend: msgQHd not NULL");
        Message *m = _msgQueueHead;
        ssize_t len = send(_sockFd, m->GetData(), m->GetLength(), MSG_DONTWAIT);
        if (len < 0) {
            if (errno != EWOULDBLOCK) {
                ERROR(_moduleName,
                      "Connection::CompleteMsgSend: failed for "
                      "socket %d: %s ",
                      _sockFd,
                      strGetFromStatus(sysErrnoToStatus(errno)));
                Close();
            } else {
                INFO(_moduleName,
                     "Connection::CompleteMsgSend for socked %d "
                     "returned EWOULDBLOCK",
                     _sockFd);
            }
            return;
        } else if (len == m->GetLength()) {
            _msgQueueHead = m->GetNext();
            m->Free();
        } else {
            m->IncOffset(len);
            return;
        }
    }
    _epollEvent.events = EPOLLIN;
    if (epoll_ctl(epollFD, EPOLL_CTL_MOD, _sockFd, &_epollEvent) < 0) {
        ERROR(_moduleName,
              "Connection::CompleteMsgSend: epoll_ctl failed "
              "for socket %d: %s",
              epollFD,
              strGetFromStatus(sysErrnoToStatus(errno)));
        xcalarExit(1);
    }
}

void
Connection::Close()
{
    INFO(_moduleName, "Connection::Close() %d", _cnxId);
    if (_sockFd != SocketHandleInvalid) {
        SocketHandle socketToClose = _sockFd;
        // Close any connection which sent us very large messages; this can't
        // be Xcalar communication so must be port scan/attack
        if (_notifier != NULL) {
            INFO(_moduleName,
                 "Calling notifier on %d, refCount %d",
                 _cnxId,
                 GetRefCount());
            _notifier->Notify(this, Notifier::SocketError, ECONNRESET);
        }
        sockDestroy(&_sockFd);
        CnxMap::iterator iter = cnxMap.find(socketToClose);
        assert(iter != cnxMap.end());
        cnxMap.erase(iter);
    }
}

Connection *
Connection::Connect(SocketAddr &sockAddr,
                    Notifier *notifier,
                    MsgHandler *msgHandler)
{
    struct protoent *pent;
    int sock = socket(sockAddr.addrFamily, SOCK_STREAM, 0);
    Status status = StatusUnknown;

    INFO(_moduleName, "Connection::Connect");
    if (sock < 0) {
        ERROR(_moduleName,
              "Connection::Connect socket failed: %s",
              strGetFromStatus(sysErrnoToStatus(errno)));
        return NULL;
    }

    int alon = 1;
    if (setsockopt(sock, SOL_SOCKET, SO_KEEPALIVE, &alon, sizeof(alon)) < 0) {
        ERROR(_moduleName,
              "Connection::Connect failed to set SO_KEEPALIVE, "
              "for socket %d: %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        xcalarAbort();
    }

    if ((pent = getprotobyname("TCP")) == NULL) {
        ERROR(_moduleName,
              "Connection::Connect failed to get TCP protocol "
              "info for socket %d: %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        xcalarAbort();
    }

    if (setsockopt(sock,
                   pent->p_proto,
                   TCP_KEEPIDLE,
                   &keepIdle,
                   sizeof(keepIdle)) < 0) {
        ERROR(_moduleName,
              "Connection::Connect failed to set TCP_KEEPIDLE "
              "for socket %d, %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        xcalarAbort();
    }

    if (setsockopt(sock,
                   pent->p_proto,
                   TCP_KEEPCNT,
                   &keepCnt,
                   sizeof(keepCnt)) < 0) {
        ERROR(_moduleName,
              "Connection::Connect failed to set TCP_KEEPCNT "
              "for socket %d, %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        xcalarAbort();
    }

    if (setsockopt(sock,
                   pent->p_proto,
                   TCP_KEEPINTVL,
                   &keepIntvl,
                   sizeof(keepIntvl)) < 0) {
        ERROR(_moduleName,
              "Connection::Connect failed to set TCP_KEEPINTVL "
              "for socket %d, %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        xcalarAbort();
    }

    // Descendants should not inherit this
    status = sockSetOption(sock, SocketOptionCloseOnExec);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Connection::Connect failed to set CloseOnExec "
              "for socked %d, %s",
              sock,
              strGetFromStatus(status));
        close(sock);
        xcalarAbort();
    }

    // We call connect here directly as a blocking connect call.
    // XXX: Simplify this by getting rid of the connecting state.
    //      See Xc-10311 and PR-0075.
    INFO(_moduleName, "Connection::Connect::connect");
    int err = connect(sock, &sockAddr.ip, sockAddr.addrLen);
    if (err < 0) {
        ERROR(_moduleName,
              "Connection::Connect failed to connect "
              "socket %d, %s",
              sock,
              strGetFromStatus(sysErrnoToStatus(errno)));
        close(sock);
        return NULL;
    }

    return CreateConnection(sock,
                            CONNECTING_CONNECTION,
                            EPOLLOUT,
                            &sockAddr,
                            notifier,
                            msgHandler);
}

Connection *
Connection::SyncConnect(const char *hostName, int port)
{
    SocketHandle sockFd = SocketHandleInvalid;
    SocketAddr sockAddr;
    Status status = StatusUnknown;

    status = sockCreate(hostName,
                        port,
                        SocketDomainUnspecified,
                        SocketTypeStream,
                        &sockFd,
                        &sockAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Descendants should not inherit this
    status = sockSetOption(sockFd, SocketOptionCloseOnExec);
    if (status != StatusOk) {
        ERROR(_moduleName,
              "Could not set CloseOnExec for host '%s' port %d "
              "socket %d: %s",
              hostName,
              port,
              sockFd,
              strGetFromStatus(status));
        goto CommonExit;
    }

    status = sockConnect(sockFd, &sockAddr);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (sockFd != SocketHandleInvalid) {
            sockDestroy(&sockFd);
        }
        return NULL;
    }

    return CreateConnection(sockFd,
                            CONNECTED_CONNECTION,
                            EPOLLIN,
                            NULL,
                            NULL,
                            NULL);
}

void
Connection::HandleEvent(uint32_t events)
{
    struct protoent *pent;

    WARNING(_moduleName, "Events 0x%x, Type %d", events, _type);

    if (events & EPOLLHUP) {
        WARNING(_moduleName, "Received EPOLLHUP on socket %d", _sockFd);
        Close();
        return;
    }

    if (events & EPOLLERR) {
        WARNING(_moduleName, "Received EPOLLERR on socket %d", _sockFd);
        // Try to get additional error information.
        int ret, soErr = 0;
        socklen_t soErrLen = sizeof(soErr);
        ret = getsockopt(_sockFd, SOL_SOCKET, SO_ERROR, &soErr, &soErrLen);
        if (ret != 0) {
            ERROR(_moduleName,
                  "Failed to get SO_ERROR for socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
        } else {
            ERROR(_moduleName,
                  "getsockopt after EPOLLERR for socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(soErr)));
        }
        Close();
        return;
    }

    if ((events & (EPOLLIN | EPOLLOUT)) == 0) {
        WARNING(_moduleName, "No in or out in events 0x%x", events);
        Close();
        return;
    }

    switch (_type) {
    case UDP_CONNECTION:
        _notifier->Notify(this, Notifier::UDPMessage, 0);
        break;
    case CONNECTION_UNDEFINED:
        WARNING(_moduleName, "Invalid connection event");
        xcalarAbort();
        break;
    case LISTENER_CONNECTION: {
        SocketHandle newSockFd;
        SocketAddr clientAddr;
        Status status = StatusUnknown;

        status = sockAccept(_sockFd, &clientAddr, &newSockFd);
        if (status != StatusOk) {
            // XXX: this needs to check for EAGAIN or EWOULDBLOCK? Since _sockFd
            // for a LISTENER_CONNECTION is non-blocking! See Connection::Listen
            ERROR(_moduleName,
                  "sockAccept error: %s",
                  strGetFromStatus(status));
            break;
        }

        assert(newSockFd > 0);

        INFO(_moduleName, "Listener good handshake: new socket %d", newSockFd);
        int on = 1;
        if (setsockopt(newSockFd, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
            ERROR(_moduleName,
                  "Failed to set TCP_NODELAY for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        int alon = 1;
        if (setsockopt(newSockFd,
                       SOL_SOCKET,
                       SO_KEEPALIVE,
                       &alon,
                       sizeof(alon)) < 0) {
            ERROR(_moduleName,
                  "Failed to set SO_KEEPALIVE for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        if ((pent = getprotobyname("TCP")) == NULL) {
            ERROR(_moduleName,
                  "Failed to TCP protocol info: %s",
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        if (setsockopt(newSockFd,
                       pent->p_proto,
                       TCP_KEEPIDLE,
                       &keepIdle,
                       sizeof(keepIdle)) < 0) {
            ERROR(_moduleName,
                  "Failed to set TCP_KEEPIDLE for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        if (setsockopt(newSockFd,
                       pent->p_proto,
                       TCP_KEEPCNT,
                       &keepCnt,
                       sizeof(keepCnt)) < 0) {
            ERROR(_moduleName,
                  "Failed to set TCP_KEEPCNT for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        if (setsockopt(newSockFd,
                       pent->p_proto,
                       TCP_KEEPINTVL,
                       &keepIntvl,
                       sizeof(keepIntvl)) < 0) {
            ERROR(_moduleName,
                  "Failed to set TCP_KEEPINTVL for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        if (fcntl(newSockFd, F_SETFL, O_NONBLOCK) < 0) {
            ERROR(_moduleName,
                  "Failed to set O_NONBLOCK for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        status = sockSetOption(newSockFd, SocketOptionCloseOnExec);
        if (status != StatusOk) {
            ERROR(_moduleName,
                  "Failed to set CloseOnExec for socket %d: %s",
                  newSockFd,
                  strGetFromStatus(status));
            sockDestroy(&newSockFd);
            xcalarAbort();
        }

        Connection *cnx = CreateConnection(newSockFd,
                                           CONNECTED_CONNECTION,
                                           EPOLLIN,
                                           NULL,
                                           _notifier,
                                           _msgHandler);
        INFO(_moduleName, "Accepted %d %p", cnx->GetId(), cnx);
        if (_notifier != NULL) {
            if (!_notifier->Notify(cnx, Notifier::AcceptedSocket, 0)) {
                WARNING(_moduleName, "Notify didn't want socket");
                _notifier = NULL;
                cnx->Close();
            }
        }
        break;
    }
    case CONNECTING_CONNECTION: {
        //
        // XXX: Get rid of this state and consolidate the handshake elsewhere.
        //      See Xc-10311 and PR-0075.
        //
        Status status = StatusUnknown;
        int ret, soErr = 1;
        socklen_t soErrLen = sizeof(soErr);

        // Need to make sure we wake up with a proper connection, and not
        // an error
        // XXX: We get here if the event has EPOLLIN or EPOLLOUT set.  How
        // would this not be a proper connection?
        ret = getsockopt(_sockFd, SOL_SOCKET, SO_ERROR, &soErr, &soErrLen);
        if (ret != 0) {
            ERROR(_moduleName,
                  "Failed to get SO_ERROR for socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(errno)));
            _notifier->Notify(this, Notifier::SocketError, EIO);
            _notifier = NULL;
            Close();
            break;
        }

        if (soErr != 0) {
            ERROR(_moduleName,
                  "getsockopt after connect for socket %d: %s",
                  _sockFd,
                  strGetFromStatus(sysErrnoToStatus(soErr)));
            _notifier->Notify(this, Notifier::SocketError, EIO);
            _notifier = NULL;
            Close();
            break;
        }

        status = sockInitiateHandshake(_sockFd);
        if (status != StatusOk) {
            ERROR(_moduleName,
                  "Handshake for socket %d: %s",
                  _sockFd,
                  strGetFromStatus(status));
            _notifier->Notify(this, Notifier::SocketError, EIO);
            _notifier = NULL;
            Close();
            break;
        }

        if (_notifier->Notify(this, Notifier::ConnectionComplete, 0)) {
            _type = CONNECTED_CONNECTION;
            _epollEvent.events = EPOLLIN;
            if (epoll_ctl(epollFD, EPOLL_CTL_MOD, _sockFd, &_epollEvent) < 0) {
                ERROR(_moduleName,
                      "Connection::HandleEvent: epoll_ctl after "
                      "successful connection for socket %d: %s",
                      epollFD,
                      strGetFromStatus(sysErrnoToStatus(errno)));
                Close();
            }
        } else {
            _notifier = NULL;
            ERROR(_moduleName,
                  "Connection::HandleEvent: Notify didn't want "
                  "socket");
            Close();
        }
        break;
    }
    case CONNECTED_CONNECTION: {
        if (events & EPOLLIN) {
            int status = ReadMsg();
            if (status == 0) {
                assert(_msgHandler != NULL);
                _msgHandler->Msg(this,
                                 (MsgOp) _msgHdr.op,
                                 _msgBody,
                                 _msgBodyLength);
                _hdrOffset = 0;
            } else if (status != EWOULDBLOCK) {
                WARNING(_moduleName,
                        "Connection::HandleEvent: Error %d on socket %d",
                        status,
                        _sockFd);
                Close();
                return;
            }
        }
        if (events & EPOLLOUT) {
            CompleteMsgSend();
        }
        break;
    }
    }
}
