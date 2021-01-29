// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef LOCALCONNECTION_H
#define LOCALCONNECTION_H

#include "primitives/Primitives.h"
#include "util/IntHashTable.h"
#include "hash/Hash.h"
#include "bc/BufferCache.h"
#include "runtime/Mutex.h"
#include "runtime/Semaphore.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "util/AtomicTypes.h"
#include "runtime/Schedulable.h"

class ILocalMsgHandler;
class LocalMsgRequestHandler;

//
// Represents a two-way connection with another Xcalar component. Provides
// request/response abstraction. LocalMsg will dispatch both request and
// response messages coming in from this connection.
//
// LocalConnection::Response exists because sendRequest is asynchronous. It
// allows waiting for and accessing response message.
//

class LocalConnection final
{
  public:
    //
    // Response type.
    //
    // Produced when sending request. Provides a way to wait for response to
    // arrive and get at it when it does. Ref counted to ensure responseMsg_
    // is managed correctly.
    //

    class Response final
    {
      public:
        Response(uint64_t requestId);

        // A Response is created on success of sendRequest. A ref is held until
        // a response message arrives or an error is reported to
        // LocalConnection. The only other ref should be held by the caller of
        // sendRequest.
        void refGet();
        void refPut();

        MustCheck Status wait(uint64_t timeoutUSecs = 0);
        MustCheck const ProtoResponseMsg *get();
        void onResponse(void *buf, size_t bufSize);
        void abortConnection(Status status);

        MustCheck uint64_t getRequestId() const { return requestId_; }

      private:
        // Support large number of pending connections on one fd.
        static constexpr size_t PendingResponsesNumSlots = 97;

        static constexpr const char *ModuleName =
            "liblocalmsg::localConnection";

        // Disallow.
        Response(const Response &) = delete;
        Response &operator=(const Response &) = delete;

        ~Response();

        uint64_t requestId_;
        Semaphore sem_;
        Status semStatus_;  // Adopt this status once sem_ posted.
        IntHashTableHook hook_;

        // Buffer containing serialized response. Populated only once response
        // is received and sem_ is signalled. Must be freed.
        void *buf_;
        size_t bufSize_;
        ProtoMsg responseMsg_;
        Atomic32 ref_;

      public:
        typedef IntHashTable<uint64_t,
                             Response,
                             &Response::hook_,
                             &Response::getRequestId,
                             PendingResponsesNumSlots,
                             hashIdentity>
            HashTable;
    };  // End class Response

    LocalConnection(bool isParent, int fd, ILocalMsgHandler *handler = NULL);

    // LocalConnections should only be created by LocalMsg. The initial ref is
    // given to LocalMsg::connections_. Subsequent refs should be acquired every
    // time a pointer to a LocalConnection is stored. LocalMsg will ensure a ref
    // is held over anything it synchronously invokes.
    void refGet();
    void refPut();

    void onFatalError(Status status);

    MustCheck Status sendRequest(ProtoRequestMsg *request,
                                 Response **responseOut);
    MustCheck Status sendResponse(ProtoResponseMsg *response);
    MustCheck Status onResponse(uint64_t requestId, void *buf, size_t bufSize);
    void setHandler(ILocalMsgHandler *handler);
    MustCheck bool handleRequest(LocalMsgRequestHandler *reqHandler,
                                 const ProtoRequestMsg *request,
                                 ProtoResponseMsg *response);
    MustCheck Status getPeerPid(pid_t *pidOut);

    MustCheck int getFd() const { return fd_; }

    MustCheck bool isMarkedToDisconnect() { return markToDisconnect_; }

    void markToDisconnect() { markToDisconnect_ = true; }

  private:
    // Type used to report connection errors on the Runtime
    class OnFatalHandler : public Schedulable
    {
      public:
        OnFatalHandler(LocalConnection *connection,
                       ILocalMsgHandler *handler,
                       Status errorStatus);
        ~OnFatalHandler() override;

        void run() override;
        void done() override;

      private:
        ILocalMsgHandler *handler_;
        LocalConnection *connection_;
        Status errorStatus_;
    };

    // Support a fairly large number of ongoing connections.
    static constexpr size_t ConnectionsNumSlots = 1307;

    // Disallow.
    LocalConnection(const LocalConnection &) = delete;
    LocalConnection &operator=(const LocalConnection &) = delete;

    ~LocalConnection();

    ILocalMsgHandler *getHandler();

    Atomic32 ref_;
    bool isParent_ = false;
    int fd_;
    IntHashTableHook hook_;
    uint64_t nextRequestId_;

    // Registered handler of incoming requests and errors. LocalConnection holds
    // a ref on this object. Can be reset at any time, so get another ref under
    // handlerLock_ if you plan to use it.
    ILocalMsgHandler *handler_;
    Spinlock handlerLock_;

    // This lock is used to serialize outgoing messages on fd_. This includes
    // both requests and responses.
    mutable Spinlock lock_;

    // Cached.
    pid_t peerPid_;

    Response::HashTable pendingResponses_;
    bool markToDisconnect_ = false;

  public:
    typedef IntHashTable<int,
                         LocalConnection,
                         &LocalConnection::hook_,
                         &LocalConnection::getFd,
                         ConnectionsNumSlots,
                         hashCastUint64<int>>
        HashTable;

    static constexpr size_t BcResponseCount = 128;

    static BcHandle *bcResponse;
};

#endif  // LOCALCONNECTION_H
