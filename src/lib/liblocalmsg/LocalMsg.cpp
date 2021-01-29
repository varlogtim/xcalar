// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/eventfd.h>
#include "localmsg/LocalMsg.h"
#include "LocalMsgInt.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "config/Config.h"
#include "runtime/Runtime.h"
#include "util/MemTrack.h"
#include "sys/Socket.h"
#include "parent/Parent.h"
#include "service/ServiceMgr.h"
#include <google/protobuf/io/coded_stream.h>
#include "libapis/LibApisSend.h"

// XXX: these includes are for the localMsg handlers, move them to their modules
#include "bc/BufCacheMemMgr.h"
#include "xdb/Xdb.h"

#ifndef DISABLE_FUNC_TESTS
#include "test/FuncTests/LocalMsgTests.h"
#endif

#include "common/Version.h"  // XXX Remove once API handler implemented.

using namespace localmsg;

LocalMsg *LocalMsg::instance = NULL;
uint64_t LocalMsg::pokeVal = PokeValue;
uint64_t LocalMsg::timeoutUSecsConfig = TimeoutUSecsDefault;

LocalMsg::LocalMsg()
    : bcSmallMsg_(NULL),
      bcRequestHandler_(NULL),
      listenFd_(-1),
      pokeFd_(-1),
      shutdown_(false),
      connectionsCount_(0),
      pollFds_(NULL),
      pollFdsCount_(0)
{
    memZero(&sockAddr_, sizeof(sockAddr_));
    memZero(&outstanding_, sizeof(outstanding_));
}

LocalMsg::~LocalMsg()
{
    assert(pollFds_ == NULL);
    assert(connectionsCount_ == 0);
    assert(pokeFd_ == -1);
    assert(listenFd_ == -1);
    assert(bcRequestHandler_ == NULL);
    assert(bcSmallMsg_ == NULL);
    assert(LocalConnection::bcResponse == NULL);
}

Status  // static
LocalMsg::init(bool isParent)
{
    assert(instance == NULL);
    instance = new (std::nothrow) LocalMsg();
    if (instance == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Init %s failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(StatusNoMem));
        return StatusNoMem;
    }

    Status status = instance->initInternal(isParent);
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
        xSyslog(ModuleName,
                XlogErr,
                "Init %s failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(StatusNoMem));
        return status;
    }

    return StatusOk;
}

void
LocalMsg::stopListening()
{
    Status status;
    Config *config = Config::get();

    shutdown_ = true;
    connectionsLock_.lock();

    if (isParent_ && listenFd_ != -1) {
        // Like close, but plays nice with accept.
        verify(shutdown(listenFd_, SHUT_RDWR) == 0);
        listenFd_ = -1;

        if (unlink(sockAddr_.sun_path) != 0) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to unlink socket path %s: %s",
                    sockAddr_.sun_path,
                    strGetFromStatus(sysErrnoToStatus(errno)));
        }

        if (config->getLowestNodeIdOnThisHost() == config->getMyNodeId()) {
            char dstPath[ServiceSocket::UnixDomainSocketDir.length() + 128];

            if ((size_t) snprintf(dstPath,
                                  sizeof(dstPath),
                                  "%s/%s",
                                  ServiceSocket::UnixDomainSocketDir.c_str(),
                                  config->getBinaryName()) >=
                sizeof(dstPath) - 1) {
                status = StatusNoBufs;
                xSyslog(ModuleName,
                        XlogErr,
                        "LocalMsg destroy failed: %s",
                        strGetFromStatus(status));
            } else {
                int ret = unlink(dstPath);
                if (ret != 0) {
                    status = sysErrnoToStatus(errno);
                    xSyslog(ModuleName,
                            XlogErr,
                            "LocalMsg destroy failed: %s",
                            strGetFromStatus(status));
                }
            }
        }
    }

    verify(write(pokeFd_, &pokeVal, sizeof(pokeVal)) == 8);

    LocalConnection *connection;
    while ((connection = connections_.begin().get()) != NULL) {
        verify(connections_.remove(connection->getFd()) == connection);
        connectionsCount_--;
        connectionsLock_.unlock();

        // Invokes destructor and closes fd.
        connection->refPut();
        connectionsLock_.lock();
    }

    connectionsLock_.unlock();

    verify(sysThreadJoin(incomingThread_, NULL) == 0);

    if (pollFds_ != NULL) {
        memFree(pollFds_);
        pollFds_ = NULL;
    }

    closeSocket(pokeFd_);
    pokeFd_ = -1;
}

void
LocalMsg::destroy()
{
    assert(shutdown_);

    assert(LocalConnection::bcResponse != NULL);
    BcHandle::destroy(&LocalConnection::bcResponse);

    assert(bcRequestHandler_ != NULL);
    BcHandle::destroy(&bcRequestHandler_);

    assert(bcSmallMsg_ != NULL);
    BcHandle::destroy(&bcSmallMsg_);

    delete instance;
    instance = NULL;
}

Status
LocalMsg::initInternal(bool isParent)
{
    Config *config = Config::get();
    Status status;
    StatsLib *statsLib = StatsLib::get();

    isParent_ = isParent;
    pokeFd_ = eventfd(0, EFD_CLOEXEC);
    if (pokeFd_ == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Init %s failed to create poke fd: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(status));
        return status;
    }

    pollFds_ =
        (struct pollfd *) memAlloc(sizeof(pollFds_[0]) * PollFdsInitCount);
    if (pollFds_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init %s pollFds failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(status));
        goto CommonExit;
    }

    pollFdsCount_ = PollFdsInitCount;

    bcSmallMsg_ = BcHandle::create(BufferCacheObjects::LocalMsgSmallMsg);
    if (bcSmallMsg_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init %s Buffer Cache LocalMsgSmallMsg failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(status));
        goto CommonExit;
    }

    bcRequestHandler_ =
        BcHandle::create(BufferCacheObjects::LocalMsgRequestHandler);
    if (bcRequestHandler_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init %s Buffer Cache LocalMsgRequestHandler failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(status));
        goto CommonExit;
    }

    LocalConnection::bcResponse =
        BcHandle::create(BufferCacheObjects::LocalMsgResponse);
    if (LocalConnection::bcResponse == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Init %s Buffer Cache LocalConnection::bcResponse failed: %s",
                isParent == true ? "Listener" : "Non-listener",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (isParent) {
        //
        // Start listening for incoming connections.
        //
        const socklen_t sockAddrSize = sizeof(sockAddr_);
        int sockOpt = 1;

        int ret = mkdir(ServiceSocket::UnixDomainSocketDir.c_str(), S_IRWXU);
        if (ret != 0) {
            if (errno != EEXIST) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "Init listener failed to create domain socket path "
                        "'%s': %s",
                        ServiceSocket::UnixDomainSocketDir.c_str(),
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }

        sockAddr_.sun_family = AF_UNIX;
        if ((size_t) snprintf(sockAddr_.sun_path,
                              sizeof(sockAddr_.sun_path),
                              "%s/%s-%llx",
                              ServiceSocket::UnixDomainSocketDir.c_str(),
                              config->getBinaryName(),
                              (unsigned long long) config->getMyNodeId()) >=
            sizeof(sockAddr_.sun_path) - 1) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Init listener failed, Unix domain socket path '%s/%s-%llx'"
                    " longer than %lu",
                    ServiceSocket::UnixDomainSocketDir.c_str(),
                    config->getBinaryName(),
                    (unsigned long long) config->getMyNodeId(),
                    sizeof(sockAddr_.sun_path));
            status = StatusNoBufs;
            goto CommonExit;
        }

        // @SymbolCheckIgnore
        listenFd_ = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (listenFd_ < 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Init listener failed to create socket: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // @SymbolCheckIgnore
        if (setsockopt(listenFd_,
                       SOL_SOCKET,
                       SO_REUSEADDR,
                       (const void *) &sockOpt,
                       sizeof(sockOpt)) != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Init listener failed to set sock opt: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        unlink(sockAddr_.sun_path);

        // @SymbolCheckIgnore
        if (bind(listenFd_, (struct sockaddr *) &sockAddr_, sockAddrSize) !=
            0) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Init listener failed to bind to %s: %s",
                    sockAddr_.sun_path,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        // @SymbolCheckIgnore
        if (listen(listenFd_, ListenBacklog) != 0) {
            status = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Init listener failed to listen: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (config->getLowestNodeIdOnThisHost() == config->getMyNodeId()) {
            char dstPath[ServiceSocket::UnixDomainSocketDir.length() + 128];

            if ((size_t) snprintf(dstPath,
                                  sizeof(dstPath),
                                  "%s/%s",
                                  ServiceSocket::UnixDomainSocketDir.c_str(),
                                  config->getBinaryName()) >=
                sizeof(dstPath) - 1) {
                status = StatusNoBufs;
                xSyslog(ModuleName,
                        XlogErr,
                        "Init listener failed: %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            // Remove stale symlink if exists.
            unlink(dstPath);

            ret = symlink(sockAddr_.sun_path, dstPath);
            if (ret != 0) {
                status = sysErrnoToStatus(errno);
                xSyslog(ModuleName,
                        XlogErr,
                        "Init listener failed to create symlink: %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }
        }
    }

    status = statsLib->initNewStatGroup("localMsg", &statsGrpId_, StatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleIncomingPollError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.handleIncomingPollError",
                                         handleIncomingPollError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleIncomingTcpError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.handleIncomingTcpError",
                                         handleIncomingTcpError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleIncomingConnReset_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.handleIncomingConnReset",
                                         handleIncomingConnReset_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleIncomingRecvError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.handleIncomingRecvError",
                                         handleIncomingRecvError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&handleIncomingTermConn_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.handleIncomingTermConn",
                                         handleIncomingTermConn_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&receiveSocketError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.receiveSocketError",
                                         receiveSocketError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&receiveParseError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.receiveParseError",
                                         receiveParseError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&receiveUnknownConn_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.receiveUnknownConn",
                                         receiveUnknownConn_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&receiveResponseError_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.receiveResponseError",
                                         receiveResponseError_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localReqHandlerConnErr_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localReqHandlerConnErr",
                                         localReqHandlerConnErr_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localReqHandlerUnknownHandler_);
    BailIfFailed(status);
    status =
        statsLib->initAndMakeGlobal(statsGrpId_,
                                    "localMsg.localReqHandlerUnknownHandler",
                                    localReqHandlerUnknownHandler_,
                                    StatUint64,
                                    StatAbsoluteWithNoRefVal,
                                    StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localReqHandlerResponseErr_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localReqHandlerResponseErr",
                                         localReqHandlerResponseErr_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localMsgDisconnect_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localMsgDisconnect",
                                         localMsgDisconnect_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localMsgSendErr_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localMsgSendErr",
                                         localMsgSendErr_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localConnReset_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localConnReset",
                                         localConnReset_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&localConnFatalErr_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "localMsg.localConnFatalErr",
                                         localConnFatalErr_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = Runtime::get()->createBlockableThread(&incomingThread_,
                                                   this,
                                                   &LocalMsg::handleIncoming);

CommonExit:
    if (status != StatusOk) {
        if (listenFd_ != -1) {
            closeSocket(listenFd_);
            unlink(sockAddr_.sun_path);
            listenFd_ = -1;
        }
        if (LocalConnection::bcResponse != NULL) {
            BcHandle::destroy(&LocalConnection::bcResponse);
        }
        if (bcSmallMsg_ != NULL) {
            BcHandle::destroy(&bcSmallMsg_);
        }
        if (bcRequestHandler_ != NULL) {
            BcHandle::destroy(&bcRequestHandler_);
        }
        if (pollFds_ != NULL) {
            memFree(pollFds_);
            pollFds_ = NULL;
            pollFdsCount_ = 0;
        }
        if (pokeFd_ != -1) {
            closeSocket(pokeFd_);
            pokeFd_ = -1;
        }
    }
    return status;
}

void
LocalMsg::drainOutstanding()
{
    bool logged = false;
    while (atomicRead64(&outstanding_) > 0) {
        if (!logged) {
            logged = true;
            xSyslog(ModuleName,
                    XlogInfo,
                    "Waiting for outstanding messages to be processed");
        }
        sysUSleep(100);
    }
    if (logged) {
        xSyslog(ModuleName,
                XlogInfo,
                "All outstanding messages have completed");
    }
}

const char *
LocalMsg::getSockPath() const
{
    return sockAddr_.sun_path;
}

//
// Methods involved in waiting for incoming connections/messages and reading
// them in for dispatch.
//
void *
LocalMsg::handleIncoming()
{
    while (!shutdown_) {
        //
        // Welcome to the listener loop! First, let's make sure we have enough
        // memory to do our poll.
        //
        connectionsLock_.lock();

        int pollTimeout = TimeoutMSecsPoll;

        // + 2 for pokeFd_ and listenFd_
        if (pollFdsCount_ < connectionsCount_ + 2) {
            size_t pollFdsCountNew =
                xcMax(pollFdsCount_ * 2, connectionsCount_ + 2);
            struct pollfd *pollFdsNew = (struct pollfd *) memAlloc(
                sizeof(pollFdsNew[0]) * pollFdsCountNew);
            if (pollFdsNew == NULL) {
                pollTimeout = pollTimeout / 10;  // Try alloc again soon.
            } else {
                memFree(pollFds_);
                pollFds_ = pollFdsNew;
                pollFdsCount_ = pollFdsCountNew;
            }
        }

        //
        // Next, let's populate pollFds_ with all the fds we want to wait on.
        // connections_ is the authoritative source of fds.
        //
        memZero(pollFds_, sizeof(pollFds_[0]) * pollFdsCount_);

        // ii below will give number of items in pollFds_ populated.
        size_t ii = 0;

        pollFds_[ii].fd = pokeFd_;
        pollFds_[ii].events = PollFdEvents;
        ii++;

        if (listenFd_ != -1) {
            pollFds_[ii].fd = listenFd_;
            pollFds_[ii].events = PollFdEvents;
            ii++;
        }

        for (LocalConnection::HashTable::iterator it(connections_);
             it.get() != NULL && ii < pollFdsCount_;
             it.next()) {
            int fd = it.get()->getFd();
            if (fd != -1) {
                pollFds_[ii].fd = fd;
                pollFds_[ii].events = PollFdEvents;
                ii++;
            }
        }

        connectionsLock_.unlock();

        //
        // Now, go to sleep until one of those fds is readable (or we timeout).
        //
        assert(pollFds_[0].fd != 0);
        int ret = poll(pollFds_, ii, pollTimeout);

        if (shutdown_) {
            continue;
        }

        if (ret == -1) {
            if (errno == EINTR) {
                continue;
            }
            xSyslog(ModuleName,
                    XlogErr,
                    "Handle Incoming poll failed: %s",
                    strGetFromStatus(sysErrnoToStatus(errno)));

            // Poll errors will be logging and we will resume the loop. Just
            // cannot bail this loop!
            StatsLib::statNonAtomicIncr(handleIncomingPollError_);

            sysSleep(USecsPerSec);
            continue;
        } else if (ret == 0) {
            continue;
        }

        for (size_t jj = 0; jj < ii; jj++) {
            if (pollFds_[jj].revents == 0) {
                continue;
            }

            if (pollFds_[jj].fd == pokeFd_) {
                //
                // We were poked because some other thread added or removed
                // something from connections_ and wants us to wait on the
                // new set.
                //
                uint64_t val;
                verify(read(pokeFd_, &val, sizeof(val)) == sizeof(val));
                assert(val > 0);

            } else if (pollFds_[jj].fd == listenFd_) {
                //
                // listenFd_ is readable. This means we have a new incoming
                // connection. Create a LocalConnection so that it can be
                // tracked.
                //

                // @SymbolCheckIgnore
                int clientFd = accept(listenFd_, NULL, NULL);
                if (clientFd < 0) {
                    StatsLib::statNonAtomicIncr(handleIncomingTcpError_);
                    xSyslog(ModuleName,
                            XlogErr,
                            "Handle incoming TCP handshake failed: %s",
                            strGetFromStatus(sysErrnoToStatus(errno)));
                    continue;  // Inner loop.
                }

                LocalConnection *connection =
                    new (std::nothrow) LocalConnection(isParent_, clientFd);
                if (connection == NULL) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Handle incoming failed, not enough memory to"
                            " support new connection");
                    assert(isConnection(clientFd) == false);
                    closeSocket(clientFd);
                    continue;
                }

                connectionsLock_.lock();
                if (shutdown_) {
                    connection->refPut();
                } else {
                    connections_.insert(connection);  // Initial ref.
                    connectionsCount_++;
                }
                connectionsLock_.unlock();

            } else {
                if (pollFds_[jj].revents & POLLRDHUP ||
                    pollFds_[jj].revents & POLLHUP ||
                    pollFds_[jj].revents & POLLNVAL) {
                    // Connection closed (either by peer (HUP) or us (NVAL)).
                    // POLLERR handled below by attempting a read to get error
                    // value.
                    connectionsLock_.lock();
                    LocalConnection *connection =
                        connections_.remove(pollFds_[jj].fd);
                    if (connection != NULL) {
                        connectionsCount_--;
                    }
                    connectionsLock_.unlock();

                    if (connection != NULL) {
                        StatsLib::statNonAtomicIncr(handleIncomingConnReset_);
                        connection->onFatalError(StatusConnReset);
                        connection->refPut();
                    }
                } else {
                    // If connection is marked to be disconnected, stop tracking
                    // this fd.
                    connectionsLock_.lock();
                    LocalConnection *connection =
                        connections_.find(pollFds_[jj].fd);
                    if (connection->isMarkedToDisconnect()) {
                        LocalConnection *tmp =
                            connections_.remove(pollFds_[jj].fd);
                        assert(tmp == connection);
                    } else {
                        connection = NULL;
                    }
                    connectionsLock_.unlock();

                    if (connection != NULL) {
                        StatsLib::statNonAtomicIncr(handleIncomingConnReset_);
                        connection->onFatalError(StatusConnReset);
                        connection->refPut();
                        connection = NULL;
                        continue;
                    }

                    // Incoming message! Read in and dispatch onto Runtime.
                    atomicInc64(&outstanding_);
                    Status status = receive(pollFds_[jj].fd);
                    if (status != StatusOk) {
                        StatsLib::statNonAtomicIncr(handleIncomingRecvError_);
                        atomicDec64(&outstanding_);

                        // Receive failed. Tear down this connection.
                        connectionsLock_.lock();
                        connection = connections_.remove(pollFds_[jj].fd);
                        if (connection != NULL) {
                            connectionsCount_--;
                        }
                        connectionsLock_.unlock();

                        if (connection != NULL) {
                            StatsLib::statNonAtomicIncr(
                                handleIncomingTermConn_);
                            connection->onFatalError(status);
                            connection->refPut();
                        }
                    }
                }
            }
        }
    }

    return NULL;
}

// Reads in a request on the given fd in context of the listener thread and
// dispatch on runtime.
Status
LocalMsg::receive(int fd)
{
    //
    // All protobuf messages preceeded by size. Read in and allocate enough
    // space.
    //

    size_t bufSize;
    Status status = sockRecvConverged(fd, &bufSize, sizeof(uint64_t));
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(receiveSocketError_);
        xSyslog(ModuleName,
                XlogErr,
                "Failed local msg receive Fd %d on sockRecv: %s",
                fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
        return status;
    }

    void *buf = allocMsgBuf(bufSize);
    if (buf == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed local msg receive Fd %d: %s",
                fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
        return StatusNoMem;
    }

    //
    // Receive actual message.
    //
    ProtoMsg message;
    bool ret;

    status = sockRecvConverged(fd, buf, bufSize);
    if (status == StatusConnReset) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed local msg receive Fd %d, client aborts connection: %s",
                fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
        goto CommonExit;
    } else if (status != StatusOk) {
        StatsLib::statAtomicIncr64(receiveSocketError_);
        xSyslog(ModuleName,
                XlogErr,
                "Failed local msg receive Fd %d on sockRecv: %s",
                fd,
                strGetFromStatus(sysErrnoToStatus(errno)));
        goto CommonExit;
    }

    try {
        ret = parseBufIntoMsg(&message, (uint8_t *) buf, bufSize);
        if (!ret) {
            StatsLib::statAtomicIncr64(receiveParseError_);
            status = StatusProtobufDecodeError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed local msg receive Fd %d on parsing: %s",
                    fd,
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed local msg receive Fd %d on parsing: %s. %s",
                fd,
                strGetFromStatus(sysErrnoToStatus(errno)),
                e.what());
        goto CommonExit;
    }

    if (message.type() == ProtoMsgTypeResponse) {
        connectionsLock_.lock();
        LocalConnection *connection = connections_.find(fd);
        if (connection == NULL) {
            connectionsLock_.unlock();
            StatsLib::statAtomicIncr64(receiveUnknownConn_);
            status = StatusNoEnt;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed local msg receive Fd %d, unexpected message"
                    " request ID %llu: %s",
                    fd,
                    (unsigned long long) message.response().requestid(),
                    strGetFromStatus(sysErrnoToStatus(errno)));
            goto CommonExit;
        }

        connection->refGet();
        connectionsLock_.unlock();

        // buf now owned by connection.
        status = connection->onResponse(message.response().requestid(),
                                        buf,
                                        bufSize);
        if (status == StatusOk) {
            atomicDec64(&outstanding_);
        } else {
            StatsLib::statAtomicIncr64(receiveResponseError_);
        }

        buf = NULL;
        connection->refPut();

    } else if (message.type() == ProtoMsgTypeRequest) {
        Txn curTxn;
        void *ptr = bcRequestHandler_->allocBuf(XidInvalid, &status);
        if (ptr == NULL) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed local msg receive Fd %d: %s",
                    fd,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        //
        // Received new request message. Schedule handling of this new request
        // onto Runtime.
        //
        connectionsLock_.lock();
        LocalConnection *connection = connections_.find(fd);
        assert(connection != NULL);  // Just added.
        connection->refGet();        // Passed to reqHandler below.
        connectionsLock_.unlock();

        LocalMsgRequestHandler *reqHandler =
            new (ptr) LocalMsgRequestHandler(connection, buf, bufSize);
        buf = NULL;  // Owned by reqHandler now.

        curTxn = generateTxn(&message.request(), &status);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed local msg receive Fd %d: %s",
                    fd,
                    strGetFromStatus(status));
            reqHandler->done();
            goto CommonExit;
        }

        Txn::setTxn(curTxn);
        status = Runtime::get()->schedule(reqHandler);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed local msg receive Fd %d: %s",
                    fd,
                    strGetFromStatus(status));
            reqHandler->done();
            Txn::setTxn(Txn());
            goto CommonExit;
        }
        Txn::setTxn(Txn());
    } else {
        // Unknown message type. Drop connection.
        status = StatusUnimpl;
    }

CommonExit:
    if (buf != NULL) {
        freeMsgBuf(buf, bufSize);
    }
    return status;
}

//
// RequestHandler: dispatching a request message on Runtime.
//
LocalMsgRequestHandler::LocalMsgRequestHandler(LocalConnection *connection,
                                               void *buf,
                                               size_t bufSize)
    : SchedulableFsm("LocalMsgRequestHandler", NULL),
      processRequest_(this),
      connection_(connection),
      buf_(buf),
      bufSize_(bufSize)
{
    atomicWrite32(&ref_, 1);
    atomicWrite32(&sendResponse_, 0);
    setNextState(&processRequest_);
}

LocalMsgRequestHandler::~LocalMsgRequestHandler()
{
    assert(atomicRead32(&ref_) == 0);
    assert(buf_ == NULL);
    assert(connection_ == NULL);
}

// done frees all memory owned by RequestHandler.
void
LocalMsgRequestHandler::done()
{
    if (atomicDec32(&ref_) > 0) {
        return;
    }

    lock_.lock();

    if (atomicRead32(&sendResponse_) == 1) {
        // Send ACK out for the XPU request.
        msgResponse_.set_requestid(msgRequest_.request().requestid());

        reqStatus_ = connection_->sendResponse(&msgResponse_);
        if (reqStatus_ != StatusOk) {
            StatsLib::statAtomicIncr64(
                LocalMsg::get()->localReqHandlerResponseErr_);
        }
    }

    lock_.unlock();

    if (apiResponse_ != NULL) {
        delete apiResponse_;
        apiResponse_ = NULL;
    }

    if (reqStatus_ != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to reply: %s",
                strGetFromStatus(reqStatus_));
        LocalMsg::get()->disconnect(connection_);
        connection_->onFatalError(reqStatus_);
    }

    connection_->refPut();  // Dec ref gotted when creating RequestHandler.
    connection_ = NULL;

    LocalMsg::get()->freeMsgBuf(buf_, bufSize_);
    buf_ = NULL;
    this->~LocalMsgRequestHandler();
    LocalMsg::get()->bcRequestHandler_->freeBuf(this);
    atomicDec64(&LocalMsg::get()->outstanding_);
}

FsmState::TraverseState
LocalMsgProcessRequest::doWork()
{
    LocalMsgRequestHandler *fsm =
        dynamic_cast<LocalMsgRequestHandler *>(getSchedFsm());
    fsm->setNextState(NULL);
    bool ret;
    Status status = StatusUnknown;
    bool lockHeld = false;
    ServiceMgr *serviceMgr = ServiceMgr::get();

    try {
        ret = LocalMsg::parseBufIntoMsg(&fsm->msgRequest_,
                                        (uint8_t *) fsm->buf_,
                                        fsm->bufSize_);
        assert(ret);  // Already parsed once.
        status = StatusOk;
        assert(fsm->msgRequest_.type() == ProtoMsgTypeRequest);
    } catch (std::exception &e) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to set protobuf ParseFromArray: %s",
                e.what());
        status = StatusNoMem;
        goto CommonExit;
    }

    fsm->msgResponse_.set_status(StatusUnimpl.code());

    lockHeld = true;
    fsm->lock_.lock();

    if (!fsm->connection_->handleRequest(fsm,
                                         fsm->msgRequest_.mutable_request(),
                                         &fsm->msgResponse_)) {
        // No registered request handler. Dispatch to default handler for
        // target.
        switch (fsm->msgRequest_.request().target()) {
        case ProtoMsgTargetParent: {
            pid_t pid;
            status = fsm->connection_->getPeerPid(&pid);
            if (status != StatusOk) {
                StatsLib::statAtomicIncr64(
                    LocalMsg::get()->localReqHandlerConnErr_);
                try {
                    fsm->msgResponse_.set_error("Unable to query peer PID");
                } catch (std::exception &e) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed to set protobuf response message: %s",
                            e.what());
                }
                fsm->msgResponse_.set_status(status.code());
                break;
            }

            ILocalMsgHandler *handler =
                Parent::get()
                    ->lookupHandler(fsm->msgRequest_.request().childid(), pid);
            if (handler == NULL) {
                StatsLib::statAtomicIncr64(
                    LocalMsg::get()->localReqHandlerUnknownHandler_);
                try {
                    // XXX Come up with a way to return errors with format.
                    fsm->msgResponse_.set_error("Unknown XPU PID");
                } catch (std::exception &e) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed to set protobuf response message: %s",
                            e.what());
                }
                fsm->msgResponse_.set_status(StatusNoChild.code());
                break;
            }

            fsm->connection_->setHandler(handler);
            // setHandler grabs a ref to handler
            handler->refPut();
            bool done = fsm->connection_
                            ->handleRequest(fsm,
                                            fsm->msgRequest_.mutable_request(),
                                            &fsm->msgResponse_);
            if (!done) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Handler yanked out unexpectedly. fd %u Child Pid: %u "
                        "XPU ID: %ld",
                        fsm->connection_->getFd(),
                        pid,
                        fsm->msgRequest_.request().childid());
                status = StatusNoChild;
                goto CommonExit;
            }
            break;
        }

        case ProtoMsgTargetChild:
            assert(false);
            break;

        case ProtoMsgTargetApi: {
            const ProtoApiRequest &apiRequest =
                fsm->msgRequest_.request().api();
            fsm->apiResponse_ = new (std::nothrow) ProtoApiResponse();
            if (fsm->apiResponse_ == NULL) {
                fsm->msgResponse_.set_status(StatusNoMem.code());
                break;
            }

            status = StatusOk;
            switch (apiRequest.func()) {
            case ApiFuncGetVersion: {
                try {
                    fsm->apiResponse_->set_version(versionGetFullStr());
                    fsm->msgResponse_.set_allocated_api(fsm->apiResponse_);
                    fsm->apiResponse_ = NULL;
                } catch (std::exception &e) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed ApiFuncGetVersion: %s",
                            e.what());
                    status = StatusNoMem;
                    goto CommonExit;
                }
                break;
            }

            case ApiFuncGetBufCacheAddr: {
                try {
                    fsm->apiResponse_->set_addr(
                        (uint64_t) BufCacheMemMgr::get()->getStartAddr());
                    fsm->msgResponse_.set_allocated_api(fsm->apiResponse_);
                    fsm->apiResponse_ = NULL;
                } catch (std::exception &e) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "Failed ApiFuncGetBufCacheAddr: %s",
                            e.what());
                    status = StatusNoMem;
                    goto CommonExit;
                }
                break;
            }

            default:
                assert(false);
                break;
            }

            fsm->msgResponse_.set_status(status.code());
            status = StatusOk;
            break;
        }

        case ProtoMsgTargetService: {
            assert(serviceMgr);
            const ProtoRequestMsg *protoRequest = &fsm->msgRequest_.request();
            ServiceResponse *serResponse = new (std::nothrow) ServiceResponse();
            if (serResponse == NULL) {
                fsm->msgResponse_.set_status(StatusNoMem.code());
                break;
            }
            ProtoResponseMsg *protoResponse = &fsm->msgResponse_;
            try {
                protoResponse->set_allocated_servic(serResponse);
                status = serviceMgr->handleApi(protoRequest, protoResponse);
                fsm->msgResponse_.set_status(status.code());
            } catch (std::exception e) {
                status = StatusNoMem;
                xSyslog(ModuleName,
                        XlogErr,
                        "Failed ProtoMsgTargetService: %s",
                        e.what());
                goto CommonExit;
            }
            fsm->msgResponse_.set_status(status.code());
            break;
        }

        case ProtoMsgTargetTest: {
#ifndef DISABLE_FUNC_TESTS
            LocalMsgTests::currentInstance
                ->onRequestMsg(fsm,
                               fsm->connection_,
                               fsm->msgRequest_.mutable_request(),
                               &fsm->msgResponse_);
#else
            fsm->msgResponse_.set_status(StatusFunctionalTestDisabled);
#endif
            break;
        }

        default:
            assert(false);
            status = StatusUnimpl;
            break;
        }
    }

    // Send response/ACK.
    atomicWrite32(&fsm->sendResponse_, 1);

CommonExit:
    fsm->reqStatus_ = status;

    if (lockHeld) {
        fsm->lock_.unlock();
    }

    return TraverseState::TraverseNext;
}

//
// Methods allowing consumers to explicitly establish a connection with another
// Xcalar component. All LocalConnections must be created by LocalMsg.
//

// Only way to create a LocalConnection.
Status
LocalMsg::connect(const char *socketPath,
                  ILocalMsgHandler *handler,
                  LocalConnection **connectionOut)
{
#ifdef UNDEF
    // XXX ::connect blocks. Do not allow on Fibers.
    Runtime::get()->assertThreadBlockable();
#endif  // UNDEF

    Status status;
    struct sockaddr_un sockAddr;
    LocalConnection *connection = NULL;

    sockAddr.sun_family = AF_UNIX;
    if ((size_t) snprintf(sockAddr.sun_path,
                          sizeof(sockAddr.sun_path),
                          "%s",
                          socketPath) >= sizeof(sockAddr.sun_path)) {
        assert(false);
        xSyslog(ModuleName,
                XlogErr,
                "Unix domain socket path, '%s', longer than %lu",
                socketPath,
                sizeof(sockAddr.sun_path));
        return StatusNoBufs;
    }

    int fd = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to create socket: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (::connect(fd,
                  (struct sockaddr *) &sockAddr,
                  sizeof(struct sockaddr_un)) != 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to connect to %s: %s",
                sockAddr.sun_path,
                strGetFromStatus(status));
        goto CommonExit;
    }

    connection = new (std::nothrow)
        LocalConnection(isParent_, fd, handler);  // Ref is 1.
    if (connection == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to connect to %s: %s",
                sockAddr.sun_path,
                strGetFromStatus(status));
        goto CommonExit;
    }

    connectionsLock_.lock();
    verifyOk(connections_.insert(connection));  // Owns initial ref.
    connectionsCount_++;
    connectionsLock_.unlock();

    // Poke listener thread to notice new connection.
    verify(write(pokeFd_, &pokeVal, sizeof(pokeVal)) == 8);

    connection->refGet();  // For caller.
    *connectionOut = connection;
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (fd != -1) {
            assert(isConnection(fd) == false);
            closeSocket(fd);
        }
    }
    return status;
}

// Publicly exposed interface to mark a LocalConnection for removal from the
// system.
void
LocalMsg::disconnect(LocalConnection *connection)
{
    LocalConnection *tmp;

    connectionsLock_.lock();
    tmp = connections_.find(connection->getFd());
    if (tmp != NULL) {
        tmp->markToDisconnect();
    }
    connectionsLock_.unlock();

    if (tmp == NULL) {
        return;
    }

    // Poke listener thread to notice the removal.
    StatsLib::statAtomicIncr64(localMsgDisconnect_);
    verify(write(pokeFd_, &pokeVal, sizeof(pokeVal)) == 8);
}

void *
LocalMsg::allocMsgBuf(size_t bufSize)
{
    if (bufSize <= BcSmallMsgSize) {
        return bcSmallMsg_->allocBuf(XidInvalid);
    } else {
        return memAlloc(bufSize);
    }
}

void
LocalMsg::freeMsgBuf(void *buf, size_t bufSize)
{
    if (bufSize <= BcSmallMsgSize) {
        bcSmallMsg_->freeBuf(buf);
    } else {
        memFree(buf);
    }
}

// Simply transmits a message on the wire. Not concerned with the bigger
// picture.
Status
LocalMsg::send(int fd, ProtoMsg *message)
{
    // Serialize message into buffer.
    size_t bufSize = 0;
    void *buf = NULL;
    Status status = StatusOk;

    try {
        bufSize = message->ByteSizeLong();
        buf = allocMsgBuf(bufSize);
        if (buf == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to send to fd %d: %s",
                    fd,
                    strGetFromStatus(status));
            return status;
        }

        bool ret = message->SerializeToArray(buf, bufSize);
        if (!ret) {
            status = StatusProtobufDecodeError;
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to send to fd %d: %s",
                    fd,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Failed to send to fd %d: %s, %s",
                fd,
                strGetFromStatus(status),
                e.what());
        goto CommonExit;
    }

    // This is what "length prefixed" means. Always send 8 byte size first.
    uint64_t size;
    size = bufSize;
    status = sockSendConverged(fd, (void *) &size, sizeof(size));
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to send to fd %d: %s",
                fd,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Send serialized message.
    status = sockSendConverged(fd, buf, bufSize);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to send to fd %d: %s",
                fd,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    freeMsgBuf(buf, bufSize);
    if (status != StatusOk) {
        StatsLib::statAtomicIncr64(localMsgSendErr_);
    }
    return status;
}

bool
LocalMsg::isConnection(int fd)
{
    bool validConn = false;
    connectionsLock_.lock();
    if (connections_.find(fd)) {
        validConn = true;
    }
    connectionsLock_.unlock();
    return validConn;
}

bool
LocalMsg::parseBufIntoMsg(ProtoMsg *message, uint8_t *buf, size_t bufSize)
{
    bool ret;

    google::protobuf::io::CodedInputStream coded_fs((uint8_t *) buf, bufSize);
    coded_fs.SetTotalBytesLimit(INT_MAX);

    ret = message->ParseFromCodedStream(&coded_fs);

    return ret;
}

Txn
LocalMsg::generateTxn(const ::ProtoRequestMsg *reqMsg, Status *retStatus)
{
    Status status = StatusOk;
    Txn genNewTxn = Txn();

    switch (reqMsg->target()) {
    case ProtoMsgTargetParent: {
        switch (reqMsg->parent().func()) {
        case ParentFuncConnect:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppDone:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppGetGroupId:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppReportNumFiles:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppReportFileError:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppGetOutputBuffer:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncAppLoadBuffer:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncXpuSendListToDsts:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncXdbGetLocalRows:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        case ParentFuncXdbGetMeta:
        case ParentFuncGetRuntimeHistograms:
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;

        default:
            DCHECK(false) << "Unknown ProtoMsgTargetParent func: "
                          << static_cast<int>(reqMsg->parent().func());
            status = StatusUnimpl;
            break;
        }
        break;
    }
    case ProtoMsgTargetApi: {
        switch (reqMsg->api().func()) {
        case ApiFuncGetVersion: {
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;
        }
        case ApiFuncGetBufCacheAddr: {
            genNewTxn =
                Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
            break;
        }
        default: {
            assert(0 && "Unknown ProtoMsgTargetApi func");
            break;
        }
        }
        break;
    }
    case ProtoMsgTargetChild: {
        genNewTxn = Txn();  // Executed in XPU
        goto CommonExit;
        break;  // Never reached
    }
    case ProtoMsgTargetService: {
        genNewTxn = ServiceMgr::get()->generateTxn(&reqMsg->servic(), &status);
        break;
    }
    case ProtoMsgTargetTest: {
#ifndef DISABLE_FUNC_TESTS
        genNewTxn = Txn::newTxn(Txn::Mode::NonLRQ, Runtime::SchedId::Immediate);
#else   // DISABLE_FUNC_TESTS
        status = StatusFunctionalTestDisabled;
#endif  // DISABLE_FUNC_TESTS
        break;
    }
    default:
        assert(0 && "Unknown ProtoMsgTarget");
        status = StatusUnimpl;
        break;
    }

#ifdef DEBUG
    if (status == StatusOk) {
        assert(genNewTxn.valid());
    }
#endif
CommonExit:
    return genNewTxn;
}
