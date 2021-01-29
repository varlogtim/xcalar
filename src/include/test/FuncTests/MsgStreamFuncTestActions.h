// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_FUNC_TESTS_H
#define _MSGSTREAM_FUNC_TESTS_H

#include "msgstream/MsgStream.h"
#include "util/Atomics.h"

class MsgStreamFuncTestObjectAction : public StreamObjectActions
{
  public:
    static constexpr uint64_t ReceiveSideDummyCookie = 0xc001cafe;
    static constexpr uint64_t PayloadMagic = 0xfeedcafe;

    class SendSideCookie
    {
      public:
        SendSideCookie(uint64_t threadId, uint64_t payloadSent_)
            : threadId_(threadId),
              semDone_(0),
              payloadSent_(payloadSent_),
              streamDone_(false)
        {
            atomicWrite64(&payloadReceived_, 0);
            atomicWrite32(&streamStatus_, StatusOk.code());
        }

        ~SendSideCookie() {}

        uint64_t threadId_;
        Semaphore semDone_;
        uint64_t payloadSent_;
        bool streamDone_;
        Atomic64 payloadReceived_;
        Atomic32 streamStatus_;
    };

    MsgStreamFuncTestObjectAction();
    virtual ~MsgStreamFuncTestObjectAction();

    // Return the StreamObject that is being specializing here.
    virtual MustCheck MsgStreamMgr::StreamObject getStreamObject() const;

    // Invoked on the destination node to handle Stream start request.
    virtual MustCheck Status startStreamLocalHandler(Xid streamId,
                                                     void **retObjectContext);

    // Invoked on the destination node to handle Stream end request.
    virtual MustCheck Status endStreamLocalHandler(Xid streamId,
                                                   void *objectContext);

    // Invoked on the destination node to handle Streams received.
    virtual MustCheck Status
    streamReceiveLocalHandler(Xid streamId,
                              void *objectContext,
                              MsgStreamMgr::ProtocolDataUnit *payload);

    // Invoked on the source node.
    virtual void streamSendCompletionHandler(Xid streamId,
                                             void *objectContext,
                                             void *sendContext,
                                             Status reason);
};

#endif  // _MSGSTREAM_FUNC_TESTS_H
