// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "msgstream/MsgStream.h"
#include "MsgStreamInfo.h"

static constexpr const char *moduleName = "LibMsgStream";

//
// MsgStreamInfo implementation
//
MsgStreamInfo::MsgStreamInfo(Xid streamId,
                             MsgStreamMgr::StreamState streamState,
                             TwoPcNodeProperty twoPcNodeProperty,
                             NodeId srcNodeId,
                             NodeId dstNodeId,
                             MsgStreamMgr::StreamObject streamObject,
                             void *objectContext)
{
    streamId_ = streamId;
    streamState_ = streamState;
    twoPcNodeProperty_ = twoPcNodeProperty;
    srcNodeId_ = srcNodeId;
    dstNodeId_ = dstNodeId;

#ifdef DEBUG
    // XXX Need to implement Stream to all nodes in the cluster. This would
    // be a fairly straightforward extension to TwoPcSingleNode stream. Note
    // that twoPcAsync needs to be enhanced for TwoPcAllNodes case.
    assert(twoPcNodeProperty == TwoPcSingleNode);
    assert(dstNodeId_ < Config::get()->getActiveNodes());
    if (twoPcNodeProperty == TwoPcSingleNode) {
        assert(dstNodeId_ < Config::get()->getActiveNodes());
    } else {
        assert(twoPcNodeProperty == TwoPcAllNodes);
        assert(dstNodeId_ == TwoPcIgnoreNodeId);
    }
#endif  // DEBUG

    assert(streamObject > MsgStreamMgr::StreamObject::StreamObjectMin &&
           streamObject < MsgStreamMgr::StreamObject::StreamObjectMax);
    streamObject_ = streamObject;
    objectContext_ = objectContext;
}

Xid
MsgStreamInfo::getStreamId() const
{
    return streamId_;
}
