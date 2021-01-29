// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_INFO_H_
#define _MSGSTREAM_INFO_H_

#include "msgstream/MsgStream.h"
#include "runtime/Spinlock.h"

class MsgStreamStartFsm;
class MsgStreamEndFsm;

struct MsgStreamInfo {
    IntHashTableHook hook_;

    Xid streamId_;  // MsgStream cookie

    // State Machine describing the Stream.
    MsgStreamMgr::StreamState streamState_;
    Spinlock stateLock_;

    // XXX This is currently only TwoPcSingleNode.
    TwoPcNodeProperty twoPcNodeProperty_;

    // Source node of MsgStream.
    NodeId srcNodeId_;

    // Destination node of MsgStream.
    NodeId dstNodeId_;

    MsgStreamMgr::StreamObject streamObject_;

    // Opaque object context that will be passed along with every callback.
    void *objectContext_ = NULL;

    // Track the start of stream on the source node only.
    MsgStreamStartFsm *streamStartFsm_ = NULL;

    // Track the end of stream on the source node only.
    MsgStreamEndFsm *streamEndFsm_ = NULL;

    // Public Methods.
    MsgStreamInfo(Xid streamId,
                  MsgStreamMgr::StreamState streamState,
                  TwoPcNodeProperty twoPcNodeProperty,
                  NodeId srcNodeId,
                  NodeId dstNodeId,
                  MsgStreamMgr::StreamObject streamObject,
                  void *objectContext);

    ~MsgStreamInfo() = default;

    MustCheck Xid getStreamId() const;
};

#endif  // _MSGSTREAM_INFO_H_
