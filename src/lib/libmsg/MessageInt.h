// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MESSAGEINT_H_
#define _MESSAGEINT_H_

#include <semaphore.h>

#include "primitives/Primitives.h"
#include "msg/MessageTypes.h"
#include "operators/GenericTypes.h"
#include "msg/Message.h"

// Not all messages use all fields. For now this is not worth optimizing
// by creating different MsgHdr types.
// Strictly speaking, msgFormat is not required as typeId can be overloaded.
// But having this seems cleaner at least for debug.
//
// In the case of a completion message, the srcNodeId and dstNodeId will not
// be changed.
// For example: if Node 0 send a load msg to Node 1, the msgHdr->srNodeId = 0,
// the msgHdr->dstNodeId = 1. When the load completed, and Node 1 send a
// completeion msg back to Node 0, the msgHdr->srNodeId = 0,
// the msgHdr->dstNodeId = 1 even though msg is sent from Node 1 to Node 0
// XXX Add some reserved space when we start supporting rolling upgrades
struct MsgHdr {
    uint64_t key;
    uint64_t xid;
    void *wakeupSem;  // Used internally by twoPc to wakeup on completion
    TwoPcHandle twoPcHandle;
    // This is used when the caller manages their own bufs for twoPc() and
    // twoPcAlt(). These fields do not change on destination nodes and can be
    // relied upon to do user managed bug cleanup. These fields are only valid
    // when payloadBufCacheType == CallerBufForPayload.
    void *payloadImmutable;
    NodeId srcNodeId;               // Node where message originated
    NodeId dstNodeId;               // Node where message must be sent
    Atomic32 *outstandingMessages;  // keep track of messages sent to nodes
    MsgEphemeral eph;
    MsgBufType msgBufType;
    // This is an immutable flag to denote: msg hdr only or hdr + payload
    MsgSendRecvFlags msgSendRecvFlags;
    MsgCreator creator;         // msg created in which function?
    TwoPcCmdType twoPcCmdType;  // TwoPcSyncCmd/TwoPcAsyncCmd/TwoPcAltCmd
    PayloadBufCacheType payloadBufCacheType;
    PayloadBufCacheType payloadBufCacheTypeImmutable;
    TwoPcFastOrSlowPath twoPcFastOrSlowPath;
    MsgTypeId typeId;
};

// Network message - Message are sent using the below type and always
// received by a bufCache buffer.
struct MsgMessage {
    MsgHdr msgHdr;
    void *payload;
    size_t ukInternalPayloadLen;
};

#endif  // _MESSAGEINT_H_
