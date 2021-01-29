// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LOCALMSG_H
#define LOCALMSG_H

#include <sys/un.h>
#include <pthread.h>
#include <poll.h>
#include "primitives/Primitives.h"
#include "localmsg/LocalConnection.h"
#include "localmsg/ILocalMsgHandler.h"
#include "bc/BufferCache.h"
#include "runtime/Schedulable.h"
#include "SocketTypes.h"
#include "util/SchedulableFsm.h"
#include "util/Atomics.h"

//
// Type used to dispatch incoming requests on the Runtime.
//
class LocalMsgProcessRequest : public FsmState
{
  public:
    LocalMsgProcessRequest(SchedulableFsm *schedFsm)
        : FsmState("LocalMsgProcessRequest", schedFsm)
    {
    }

    TraverseState doWork() override;
};

class LocalMsgRequestHandler : public SchedulableFsm
{
    friend class LocalMsgProcessRequest;

  public:
    LocalMsgRequestHandler(LocalConnection *connection,
                           void *buf,
                           size_t bufSize);

    ~LocalMsgRequestHandler() override;

    void incRef()
    {
        assert(atomicRead32(&ref_) > 0);
        atomicInc32(&ref_);
    }

    void setResponseStatus(Status resStatus)
    {
        lock_.lock();
        reqStatus_ = resStatus;
        lock_.unlock();
    }

    void dontSendResponse() { atomicWrite32(&sendResponse_, 0); }

    void done() override;

  private:
    LocalMsgProcessRequest processRequest_;
    ProtoMsg msgRequest_;
    ProtoResponseMsg msgResponse_;
    ProtoApiResponse *apiResponse_ = NULL;
    LocalConnection *connection_ = NULL;
    void *buf_ = NULL;
    size_t bufSize_ = 0;
    Atomic32 sendResponse_;
    Status reqStatus_ = StatusOk;

    Atomic32 ref_;
    Spinlock lock_;

    LocalMsgRequestHandler(const LocalMsgRequestHandler &) = delete;
    LocalMsgRequestHandler &operator=(const LocalMsgRequestHandler &) = delete;
};

//
// This module deals with communication between different Xcalar components on
// the same host. Deals with sending outgoing connections and dispatching
// incoming connections. Provides request/response abstraction (but doesn't
// mandate its use.
//
// Unix domain sockets are used as transport. Length prefixed protobufs are
// used as data format.
//

class LocalMsg final
{
    friend class LocalConnection;
    friend class LocalMsgRequestHandler;
    friend class LocalMsgProcessRequest;
    friend class LocalMsgDispatchAck;

  public:
    MustCheck static Status init(bool isParent);
    void stopListening();
    void destroy();
    MustCheck static LocalMsg *get() { return instance; }

    MustCheck Status connect(const char *socketPath,
                             ILocalMsgHandler *handler,
                             LocalConnection **connectionOut);
    void disconnect(LocalConnection *connection);
    MustCheck Status send(int fd, ProtoMsg *message);
    MustCheck void *allocMsgBuf(size_t bufSize);
    void freeMsgBuf(void *buf, size_t bufSize);
    void drainOutstanding();
    MustCheck const char *getSockPath() const;
    MustCheck bool isConnection(int fd);
    MustCheck static bool parseBufIntoMsg(ProtoMsg *message,
                                          uint8_t *buf,
                                          size_t bufSize);

    // Changed by config.
    static uint64_t timeoutUSecsConfig;

  private:
    // See 'man listen'. Can be many API consumers but there is a dedicated
    // listener thread, so this shouldn't get too long.
    static constexpr unsigned ListenBacklog = 64;

    // No need for timing out listener thread, closing sockets will wake it up.
    static constexpr uint64_t ListenTimeoutSecs = SecsPerMinute;
    static constexpr size_t PollFdsInitCount = 128;
    static constexpr int TimeoutMSecsPoll = MSecsPerSec * 10;
    static constexpr short PollFdEvents = POLLIN | POLLPRI | POLLRDHUP;
    static constexpr uint64_t PokeValue = 1;
    static uint64_t pokeVal;  // Used to free listener thread from select.
    static LocalMsg *instance;

    MustCheck Status initInternal(bool isListener);
    MustCheck void *handleIncoming();
    MustCheck Status receive(SocketHandle sockHandle);
    MustCheck Txn generateTxn(const ::ProtoRequestMsg *reqMsg,
                              Status *retStatus);

    BcHandle *bcSmallMsg_ = NULL;
    BcHandle *bcRequestHandler_ = NULL;

    bool isParent_ = false;
    int listenFd_ = -1;            // Accepts new connections.
    int pokeFd_ = -1;              // Used to wake a select to add a new fd.
    struct sockaddr_un sockAddr_;  // Unix domain socket on which to listen.
    pthread_t incomingThread_;     // Waits for incoming connections/reads.
    bool shutdown_ = false;        // Used to teardown listener thread.

    // Collection of all active LocalConnections. Because each is 2-way, the
    // listener thread is employed to dispatch incoming messages.
    LocalConnection::HashTable connections_;
    Mutex connectionsLock_;
    size_t connectionsCount_ = 0;

    struct pollfd *pollFds_ = NULL;
    size_t pollFdsCount_ = 0;
    size_t pollErrors_ = 0;

    Atomic64 outstanding_;

    static constexpr size_t StatCount = 16;
    struct {
        StatGroupId statsGrpId_;
        StatHandle handleIncomingPollError_;
        StatHandle handleIncomingTcpError_;
        StatHandle handleIncomingConnReset_;
        StatHandle handleIncomingRecvError_;
        StatHandle handleIncomingTermConn_;
        StatHandle receiveSocketError_;
        StatHandle receiveParseError_;
        StatHandle receiveUnknownConn_;
        StatHandle receiveResponseError_;
        StatHandle localReqHandlerConnErr_;
        StatHandle localReqHandlerUnknownHandler_;
        StatHandle localReqHandlerResponseErr_;
        StatHandle localMsgDisconnect_;
        StatHandle localMsgSendErr_;
        StatHandle localConnReset_;
        StatHandle localConnFatalErr_;
    };

    LocalMsg();
    ~LocalMsg();

    // Disallow.
    LocalMsg(const LocalMsg &) = delete;
    LocalMsg &operator=(const LocalMsg &) = delete;

  public:
    // Needed by config.
    static constexpr uint64_t TimeoutUSecsDefault = 5 * 60 * USecsPerSec;
    static constexpr uint64_t TimeoutUSecsMin = 1 * 60 * USecsPerSec;
    static constexpr uint64_t TimeoutUSecsMax = 60 * 60 * USecsPerSec;

    // Buffer cache constants. Use BC for reasonably sized messages and
    // request handlers.
    static constexpr size_t BcSmallMsgCount = 64;
    static constexpr size_t BcSmallMsgSize = 128;

    static constexpr size_t BcRequestHandlerCount = 32;
    static constexpr size_t BcRequestHandlerSize =
        sizeof(LocalMsgRequestHandler);
};

#endif  // LOCALMSG_H
