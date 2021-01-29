// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LIB_NS_STREAM_ACTIONS_H_
#define _LIB_NS_STREAM_ACTIONS_H_

#include "msgstream/MsgStream.h"
#include "util/Atomics.h"

class LibNsStreamObjectAction : public StreamObjectActions
{
  public:
    class SendSideCookie
    {
      public:
        SendSideCookie(uint64_t sendCount)
            : semDone_(0), sendCount_(sendCount), streamDone_(false)
        {
            atomicWrite64(&receiveCount_, 0);
            atomicWrite32(&streamStatus_, StatusOk.code());
        }

        ~SendSideCookie() {}

        Semaphore semDone_;
        uint64_t sendCount_;
        bool streamDone_;
        Atomic64 receiveCount_;
        Atomic32 streamStatus_;
    };

    LibNsStreamObjectAction();
    virtual ~LibNsStreamObjectAction();

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

#endif  // _LIB_NS_STREAM_ACTIONS_H_
