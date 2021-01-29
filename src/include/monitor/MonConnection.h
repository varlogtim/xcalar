// Copyright 2015 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonConnection.h: Classes used for connection management by the
//                  cluster monitor

#ifndef _MONCONNECTION_H
#define _MONCONNECTION_H

#include <sys/types.h>
#include <assert.h>
#include <string>
#include "util/System.h"
#include <string.h>

#include "monitor/MonRefCounted.h"
#include "monitor/MonLogMsg.h"
#include "monitor/MonMsg.h"
#include "sys/Socket.h"
#include "SocketTypes.h"

class Connection : public RefCounted
{
  public:
    enum ConnectionType {
        CONNECTION_UNDEFINED,
        LISTENER_CONNECTION,
        CONNECTING_CONNECTION,
        CONNECTED_CONNECTION,
        UDP_CONNECTION
    };

    class Notifier
    {
      public:
        enum NotifierType {
            AcceptedSocket,
            ConnectionComplete,
            UDPMessage,
            SocketError
        };
        virtual bool Notify(Connection *cnx, NotifierType type, int status) = 0;
    };

    class MsgHandler
    {
      public:
        virtual void Msg(Connection *cnx,
                         MsgOp op,
                         void *msgBody,
                         uint32_t msgLength) = 0;
    };

    Connection(SocketHandle sockFd,
               ConnectionType type,
               SocketAddr *connectAddr,
               Notifier *notifier = NULL,
               MsgHandler *msgHandler = NULL)
        : _sockFd(sockFd),
          _type(type),
          _hdrOffset(0),
          _msgBody(NULL),
          _msgBodyLength(0),
          _bodyOffset(0),
          _notifier(notifier),
          _msgHandler(msgHandler),
          _msgQueueHead(NULL),
          _msgQueueTail(NULL),
          _gotPeerAddr(false)
    {
        memZero(&_msgHdr, sizeof(MsgHdr));
        memZero(&_peerAddr, sizeof(_peerAddr));
        if (connectAddr != NULL) {
            _connectAddr = *connectAddr;
        } else {
            memZero(&_connectAddr, sizeof(_connectAddr));
        }
        memZero(&_epollEvent, sizeof(_epollEvent));
        _epollEvent.data.fd = (int) _sockFd;
        _epollEvent.events = 0;
        _cnxId = _nextCnxId++;
        INFO(_moduleName,
             "Connection constructor: _cnxId: %d, _type %d\n",
             _cnxId,
             _type);
    }
    ~Connection()
    {
        INFO(_moduleName,
             "~Connection::Connection: _cnxId: %d, _type %d\n",
             _cnxId,
             _type);
        if (_sockFd != SocketHandleInvalid) {
            sockDestroy(&_sockFd);
        }
        if (_msgBody != NULL) {
            delete[] _msgBody;
        }
        while (_msgQueueHead != NULL) {
            Message *m = _msgQueueHead;
            _msgQueueHead = _msgQueueHead->GetNext();
            m->Free();
        }
    }
    epoll_event &GetEpollEvent() { return _epollEvent; }
    void Close();

    static Connection *Connect(const char *hostName,
                               int port,
                               Notifier *notifier,
                               MsgHandler *msgHandler);
    static Connection *Connect(SocketAddr &sockAddr,
                               Notifier *notifier,
                               MsgHandler *msgHandler);
    static Connection *SyncConnect(const char *hostName, int port);
    static void Listen(const char *hostName,
                       int port,
                       Notifier *newConnectionNotifier,
                       MsgHandler *msgHandler);
    static void CreateUDPConnection(const char *hostName,
                                    int port,
                                    Notifier *notifier,
                                    MsgHandler *msgHandler,
                                    Ref<Connection> &cnx);
    static void Poll(uint64_t maxWaitTime);

    bool SendMsg(MsgOp op, void *msg, uint32_t msgSize);
    int SyncReadMsg(MsgOp &op, void *&msgBody, uint32_t &msgLength);
    uint32_t GetId() { return _cnxId; }
    SocketHandle GetSocket() { return _sockFd; }

  private:
    class Message
    {
      public:
        static Message *Alloc(void *msgHdr,
                              size_t msgHdrLength,
                              void *msgBody,
                              size_t msgBodyLength)
        {
            size_t msgLength = msgHdrLength + msgBodyLength;
            // @SymbolCheckIgnore
            Message *msg = (Message *) malloc(sizeof(Message) + msgLength);
            msg->_offset = 0;
            msg->_length = msgLength;
            msg->_next = NULL;
            if (msgHdrLength != 0) {
                assert(msgHdr != NULL);
                memcpy(msg->_data, msgHdr, msgHdrLength);
            }
            if (msgBodyLength != 0) {
                assert(msgBody != NULL);
                memcpy(msg->_data + msgHdrLength, msgBody, msgBodyLength);
            }
            return msg;
        }
        void Free()
        {
            // @SymbolCheckIgnore
            free(this);
        }
        void *GetData()
        {
            assert(_offset < _length);
            return &_data[_offset];
        }
        uint32_t GetLength() { return _length - _offset; }
        void IncOffset(uint32_t value)
        {
            _offset += value;
            assert(_offset <= _length);
        }
        Message *GetNext() { return _next; }
        void SetNext(Message *message) { _next = message; }

      private:
        uint32_t _offset;
        uint32_t _length;
        Message *_next;
        char _data[0];
    };

    static Connection *CreateConnection(int sock,
                                        ConnectionType type,
                                        uint32_t events,
                                        SocketAddr *connectAddr,
                                        Notifier *notifier,
                                        MsgHandler *msgHandler);

    bool AppendMsg(void *msgHdr,
                   size_t msgHdrLength,
                   void *msgBody,
                   uint32_t msgBodyLength);
    void CompleteMsgSend();
    void HandleEvent(uint32_t events);

    int ReadMsg(bool wait = false);

    static uint32_t _nextCnxId;

    SocketHandle _sockFd;
    ConnectionType _type;
    MsgHdr _msgHdr;
    int _hdrOffset;
    char *_msgBody;
    int _msgBodyLength;
    int _bodyOffset;
    Notifier *_notifier;
    MsgHandler *_msgHandler;
    uint32_t _cnxId;
    epoll_event _epollEvent;
    Message *_msgQueueHead;
    Message *_msgQueueTail;
    SocketAddr _peerAddr;
    bool _gotPeerAddr;
    SocketAddr _connectAddr;

    static constexpr const char *_moduleName = "MonConnection";
};

#endif
