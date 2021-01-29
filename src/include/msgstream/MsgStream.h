// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_H_
#define _MSGSTREAM_H_

#include "primitives/Primitives.h"
#include "util/IntHashTable.h"
#include "stat/Statistics.h"
#include "msg/TwoPcFuncDefsClient.h"

class StreamObjectActions;

// Singleton class to manage MsgStream. This keeps state or context of every
// stream.
class MsgStreamMgr final
{
  public:
    enum class StreamState : uint8_t {
        StreamInit,
        // Initiate stream set up.
        StreamSetup,
        // Stream is now established with a destination or all the destinations.
        StreamEstablished,
        // Stream in progress.
        StreamInProgress,
        // Stream work is done, needs to be torn down.
        StreamIsDone,
        StreamTeardown,
    };

    enum class StreamObject : uint8_t {
        StreamObjectMin = 0,
        LibNsPathInfoPerNode,
        MsgStreamFuncTest,
        XpuCommunication,
        // must be last
        StreamObjectMax,
    };

    // Pack this to 8 Byte alignment, since this is sent across the wire.
    struct ProtocolDataUnit {
        Xid streamId_;         // MsgStream cookie
        NodeId srcNodeId_;     // Source node
        NodeId dstNodeId_;     // Destination node
        uint32_t bufferSize_;  // Size of the buffer that follows.
        TwoPcNodeProperty twoPcNodeProperty_;
        StreamState streamState_;
        StreamObject streamObject_;
        uint8_t padding_[6];
        uint8_t buffer_[0];
    };

    static MustCheck MsgStreamMgr *get();

    static MustCheck Status init();

    void destroy();

    // Start a new Msg Stream. Return the Stream ID as a handle for the
    // Msg stream established.
    MustCheck Status startStream(TwoPcNodeProperty twoPcNodeProperty,
                                 NodeId dstNodeId,
                                 StreamObject streamObject,
                                 void *objectContext,
                                 Xid *retStreamId);

    // End a Msg Stream. Identify a Msg Stream uniquely with Stream ID.
    void endStream(Xid streamId);

    // Send a payload on an existing Msg Stream asynchronously. Identify a
    // Msg Stream uniquely with Stream ID.
    MustCheck Status sendStream(Xid streamId,
                                ProtocolDataUnit *pdu,
                                StreamState streamState,
                                void *sendContext);

    // Initialize payload to be sent.
    MustCheck Status initPayloadToSend(Xid streamId,
                                       void *payload,
                                       uint32_t payloadLen);

  private:
    static MsgStreamMgr *instance;

    MsgStreamMgr() {}
    ~MsgStreamMgr() {}
    MsgStreamMgr(const MsgStreamMgr &) = delete;
    MsgStreamMgr &operator=(const MsgStreamMgr &) = delete;
};

// Abstract class that represents each MsgStream object.
class StreamObjectActions
{
  public:
    StreamObjectActions() {}
    virtual ~StreamObjectActions() {}

    // Return the StreamObject that is being specializing here.
    virtual MustCheck MsgStreamMgr::StreamObject getStreamObject() const = 0;

    // Invoked on the destination node to handle Stream start request.
    virtual MustCheck Status
    startStreamLocalHandler(Xid streamId, void **retObjectContext) = 0;

    // Invoked on the destination node to handle Stream end request.
    virtual MustCheck Status endStreamLocalHandler(Xid streamId,
                                                   void *objectContext) = 0;

    // Invoked on the destination node to handle Streams received.
    virtual MustCheck Status
    streamReceiveLocalHandler(Xid streamId,
                              void *objectContext,
                              MsgStreamMgr::ProtocolDataUnit *payload) = 0;

    // Invoked on the source node.
    virtual void streamSendCompletionHandler(Xid streamId,
                                             void *objectContext,
                                             void *sendContext,
                                             Status reason) = 0;
};

// MsgStream two PC action implementation.
class Msg2pcStreamActionImpl : public TwoPcAction
{
  public:
    Msg2pcStreamActionImpl() {}
    virtual ~Msg2pcStreamActionImpl() {}
    virtual void schedLocalWork(MsgEphemeral *ephemeral, void *payload);
    virtual void schedLocalCompletion(MsgEphemeral *ephemeral, void *payload);
    virtual void recvDataCompletion(MsgEphemeral *ephemeral, void *payload);

  private:
    Msg2pcStreamActionImpl(const Msg2pcStreamActionImpl &) = delete;
    Msg2pcStreamActionImpl(const Msg2pcStreamActionImpl &&) = delete;
    Msg2pcStreamActionImpl &operator=(const Msg2pcStreamActionImpl &) = delete;
};

#endif  // _MSGSTREAM_H_
