// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <sys/socket.h>
#include "localmsg/LocalConnection.h"
#include "localmsg/LocalMsg.h"
#include "LocalMsgInt.h"
#include "util/MemTrack.h"
#include "runtime/Runtime.h"
#include "stat/Statistics.h"

using namespace localmsg;

BcHandle *LocalConnection::bcResponse = NULL;

LocalConnection::LocalConnection(bool isParent,
                                 int fd,
                                 ILocalMsgHandler *handler)
    : isParent_(isParent),
      fd_(fd),
      nextRequestId_(0),
      handler_(handler),
      peerPid_(-1)
{
    atomicWrite32(&ref_, 1);
    if (handler_ != NULL) {
        handler_->refGet();
    }
}

LocalConnection::~LocalConnection()
{
    setHandler(NULL);

    assert(atomicRead32(&ref_) == 0);
    if (fd_ != -1) {
        closeSocket(fd_);
        fd_ = -1;
    }
}

// Unix domain sockets allow us to lookup the PID of the attached (local)
// process via SO_PEERCRED.
Status
LocalConnection::getPeerPid(pid_t *pidOut)
{
    lock_.lock();

    if (fd_ == -1) {
        lock_.unlock();
        StatsLib::statAtomicIncr64(LocalMsg::get()->localConnReset_);
        return StatusConnReset;
    }

    if (peerPid_ == -1) {
        struct ucred cred;
        socklen_t credSize = sizeof(cred);

        // @SymbolCheckIgnore
        int ret =
            getsockopt(fd_, SOL_SOCKET, SO_PEERCRED, (void *) &cred, &credSize);
        if (ret != 0) {
            lock_.unlock();
            return sysErrnoToStatus(ret);
        }

        peerPid_ = cred.pid;
    }

    *pidOut = peerPid_;
    lock_.unlock();
    return StatusOk;
}

void
LocalConnection::onFatalError(Status status)
{
    StatsLib::statAtomicIncr64(LocalMsg::get()->localConnFatalErr_);

    if (!isParent_) {
        // If child, don't bother cleaning up. Just force exit.
        xcalarExit(status.code());
        return;
    }

    //
    // Abort any outstanding responses. They're waiting for a reply that will
    // never come.
    //
    lock_.lock();
    Response *response;
    while ((response = pendingResponses_.begin().get()) != NULL) {
        verify(pendingResponses_.remove(response->getRequestId()) == response);
        lock_.unlock();

        response->abortConnection(status);
        response->refPut();

        lock_.lock();
    }
    lock_.unlock();

    //
    // Tell handler (e.g. ParentChild) the bad news.
    //
    ILocalMsgHandler *handler = NULL;
    handlerLock_.lock();
    handler = handler_;
    if (handler != NULL) {
        handler->refGet();
    }
    handler_ = NULL;
    handlerLock_.unlock();

    if (handler != NULL) {
        // XXX - The fatal handler must be run asyncronously, and must succeed.
        // In general, onFatal should be handled like the success path (meaning
        // it should be asyncronous), since client code may do arbitrarily
        // complex things.
        // Specifically, there is the following deadlock:
        // 1. Child 0: AppGroup::spawn grabs a lock on the AppGroup
        // 2. Child 0: ParentChild::waitUntilReady on child 2
        // 3. Child 1: LocalConnection::onFatalError
        // 4. Child 1: AppGroup::onInstanceDone tries to grab a lock
        //             on the AppGroup
        // 5. Child 2: Dies, but never reaches LocalConnection::onFatalError,
        //             because Child 1 is holding the thread hostage
        // Child 0 is waiting on a message (onFatal) from child 2
        // Child 2 is waiting on the receive thread, held by child 1
        // Child 1 is waiting on the AppGroup lock, held by child 0
        static constexpr const size_t TxnAbortMaxRetryCount = 1000;
        static constexpr uint64_t TxnAbortSleepUSec = 1000;
        size_t ii = 0;
        Runtime *rt = Runtime::get();
        OnFatalHandler *fatalHandler = NULL;
        Txn savedTxn = Txn::currentTxn();

        // Round robin on all the operator runtime schedulers, i.e. Sched0,
        // Sched1 and Sched2.
        Runtime::SchedId rtSchedId = static_cast<Runtime::SchedId>(
            StatsLib::statReadUint64(LocalMsg::get()->localConnFatalErr_) %
            static_cast<int>(Runtime::SchedId::Immediate));

        Txn curTxn = savedTxn;
        curTxn.rtSchedId_ = rtSchedId;
        curTxn.rtType_ = RuntimeTypeThroughput;
        Txn::setTxn(curTxn);

        do {
            if (fatalHandler == NULL) {
                // This steals the ref for handler gotten above
                fatalHandler =
                    new (std::nothrow) OnFatalHandler(this, handler, status);
            }
            if (fatalHandler != NULL) {
                Status status = Runtime::get()->schedule(fatalHandler);
                if (status == StatusOk) {
                    handler->refPut();
                    Txn::setTxn(savedTxn);
                    return;
                }
            }
            // Kick Txn abort on Schedulables in the hope that some resources
            // will be released.
            rt->kickSchedsTxnAbort();
            ii++;
            sysUSleep(TxnAbortSleepUSec);
        } while (ii < TxnAbortMaxRetryCount);
        buggyPanic("Failed to schedule OnFatalHandler");
    }

    //
    // Notice how concurrent onFatalError calls, while annoying, aren't
    // incorrect.
    //
}

// Safely lookup and get reference to handler, if there is one. Caller must put
// ref.
ILocalMsgHandler *
LocalConnection::getHandler()
{
    handlerLock_.lock();
    ILocalMsgHandler *handler = handler_;
    if (handler != NULL) {
        handler->refGet();
    }
    handlerLock_.unlock();
    return handler;
}

void
LocalConnection::refGet()
{
    assert(atomicRead32(&ref_) > 0);
    atomicInc32(&ref_);
}

void
LocalConnection::refPut()
{
    assert(atomicRead32(&ref_) > 0);
    if (atomicDec32(&ref_) == 0) {
        delete this;
    }
}

void
LocalConnection::setHandler(ILocalMsgHandler *handler)
{
    if (handler != NULL) {
        handler->refGet();
    }

    handlerLock_.lock();
    ILocalMsgHandler *prevHandler = handler_;
    handler_ = handler;  // Passes ref to handler_.
    handlerLock_.unlock();

    if (prevHandler != NULL) {
        prevHandler->refPut();
    }
}

// Send request message. responseOut allows asynchronously waiting for and
// accessing response. Ref on responseOut must be decremented on success. Caller
// must have ref on LocalConnection.
Status
LocalConnection::sendRequest(ProtoRequestMsg *request,
                             LocalConnection::Response **responseOut)
{
    Status status = StatusOk;
    assert(bcResponse != NULL);
    void *ptr = bcResponse->allocBuf(XidInvalid, &status);
    if (ptr == NULL) {
        return status;
    }

    lock_.lock();  // Serializes outgoing requests.

    if (fd_ == -1) {
        lock_.unlock();
        bcResponse->freeBuf(ptr);
        StatsLib::statAtomicIncr64(LocalMsg::get()->localConnReset_);
        return StatusConnReset;
    }

    // fd_ cannot be set to -1 without holding lock_. If fd_ becomes invalid,
    // the below send will simply fail (as opposed to a send(-1) whose behavior
    // is undefined.

    uint64_t requestId = nextRequestId_++;

    request->set_requestid(requestId);

    ProtoMsg message;
    message.set_type(ProtoMsgTypeRequest);
    message.set_allocated_request(request);

    Response *response = new (ptr) Response(requestId);

    status = LocalMsg::get()->send(fd_, &message);
    BailIfFailed(status);

    pendingResponses_.insert(response);  // Initial ref.
    response->refGet();                  // Ref for caller.
    *responseOut = response;
    response = NULL;

CommonExit:

    lock_.unlock();

    message.release_request();  // Memory owned by caller.

    if (response != NULL) {
        response->refPut();
    }

    return status;
}

Status
LocalConnection::sendResponse(ProtoResponseMsg *response)
{
    ProtoMsg msgResponse;
    msgResponse.set_type(ProtoMsgTypeResponse);
    msgResponse.set_allocated_response(response);

    lock_.lock();  // Serialize outgoing messages.

    if (fd_ == -1) {
        lock_.unlock();
        msgResponse.release_response();
        StatsLib::statAtomicIncr64(LocalMsg::get()->localConnReset_);
        return StatusConnReset;
    }

    Status status = LocalMsg::get()->send(fd_, &msgResponse);
    msgResponse.release_response();

    lock_.unlock();
    return status;
}

// Handle this request if there is a registered handler. Returns true if request
// handled, false otherwise.
bool
LocalConnection::handleRequest(LocalMsgRequestHandler *reqHandler,
                               const ProtoRequestMsg *request,
                               ProtoResponseMsg *response)
{
    ILocalMsgHandler *handler = getHandler();
    if (handler == NULL) {
        return false;
    }

    handler->onRequestMsg(reqHandler, this, request, response);
    handler->refPut();
    return true;
}

//
// Handling, waiting for, and interacting with responses.
//

// LocalMsg notifying us of incoming response. Caller must have ref on
// LocalConnection.
Status
LocalConnection::onResponse(uint64_t requestId, void *buf, size_t bufSize)
{
    lock_.lock();
    // Lookup response by requestId. Remove. It is no longer pending. Steals
    // ref from pendingResponses_.
    Response *response = pendingResponses_.remove(requestId);
    lock_.unlock();

    if (response == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Received response without associated request. request ID %llu",
                (unsigned long long) requestId);
        LocalMsg::get()->freeMsgBuf(buf, bufSize);
        return StatusNoEnt;
    }

    response->onResponse(buf, bufSize);

    // Dec initial ref. LocalConnection no longer cares about this Response.
    // This will free response is sender has abandoned it.
    response->refPut();

    return StatusOk;
}

LocalConnection::Response::Response(uint64_t requestId)
    : requestId_(requestId),
      sem_(0),
      semStatus_(StatusUnknown),
      buf_(NULL),
      bufSize_(0)
{
    atomicWrite32(&ref_, 1);
}

LocalConnection::Response::~Response()
{
    assert(atomicRead32(&ref_) == 0);
    if (buf_ != NULL) {
        LocalMsg::get()->freeMsgBuf(buf_, bufSize_);
        buf_ = NULL;
    }
}

void
LocalConnection::Response::refGet()
{
    assert(atomicRead32(&ref_) > 0);
    atomicInc32(&ref_);
}

void
LocalConnection::Response::refPut()
{
    assert(atomicRead32(&ref_) > 0);
    if (atomicDec32(&ref_) == 0) {
        this->~Response();
        LocalConnection::bcResponse->freeBuf(this);
    }
}

// Blocks until response arrives or error occurs.
Status
LocalConnection::Response::wait(uint64_t timeoutUSecs)
{
    if (timeoutUSecs == 0) {
        sem_.semWait();
        return semStatus_;
    }
    Status status = sem_.timedWait(timeoutUSecs);
    if (status == StatusOk) {
        assert(semStatus_ != StatusUnknown);
        status = semStatus_;
    }
    return status;
}

// Return actual response message.
const ProtoResponseMsg *
LocalConnection::Response::get()
{
    if (buf_ == NULL) {
        return NULL;
    }
    return responseMsg_.mutable_response();
}

// Called by LocalMsg once a response with this requestId is received.
void
LocalConnection::Response::onResponse(void *buf, size_t bufSize)
{
    Status status = StatusUnknown;
    bool ret = false;
    assert(buf_ == NULL);
    buf_ = buf;
    bufSize_ = bufSize;
    try {
        ret = responseMsg_.ParseFromArray(buf_, bufSize_);
        assert(ret);
        if (!ret) {
            xSyslog(ModuleName, XlogErr, "Failed to parse response from array");
            status = StatusInval;
        } else {
            status = StatusOk;
        }
    } catch (std::exception &e) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to parse response from array: %s",
                e.what());
        status = StatusNoMem;
    }
    semStatus_ = status;
    sem_.post();
}

// Something bad happened. Notify whoever's waiting on this that a response is
// not on the way.
void
LocalConnection::Response::abortConnection(Status status)
{
    assert(status != StatusOk);
    semStatus_ = status;
    sem_.post();
}

// OnFatalHandler: dispatching a request message on Runtime.
//
LocalConnection::OnFatalHandler::OnFatalHandler(LocalConnection *connection,
                                                ILocalMsgHandler *handler,
                                                Status errorStatus)
    : Schedulable("OnFatalHandler"),
      handler_(handler),
      connection_(connection),
      errorStatus_(errorStatus)
{
    connection_->refGet();
}

LocalConnection::OnFatalHandler::~OnFatalHandler()
{
    connection_->refPut();
}

void
LocalConnection::OnFatalHandler::run()
{
    handler_->onConnectionClose(connection_, errorStatus_);
    handler_->refPut();
    handler_ = NULL;
}

void
LocalConnection::OnFatalHandler::done()
{
    delete this;
}
