// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef ILOCALMSGHANDLER_H
#define ILOCALMSGHANDLER_H

#include "primitives/Primitives.h"
#include "util/IRefCounted.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"

class LocalMsgRequestHandler;
class LocalConnection;

//
// Implement this interface in classes that are handling/dispatching local
// messages.
//

class ILocalMsgHandler : public IRefCounted
{
  public:
    virtual ~ILocalMsgHandler() {}

    virtual void onRequestMsg(
        LocalMsgRequestHandler *reqHandler,
        // connection gives details on connection with sender and allows changes
        // to registration, sending follow up messages, etc.
        LocalConnection *connection,
        // Request input.
        const ProtoRequestMsg *request,
        // Response send to all request messages (use different ProtoMsgType to
        // avoid this). Must at least be populated with Status.
        ProtoResponseMsg *response) = 0;

    // An error occurred that caused the connection to be torn down.
    // connection is a borrowed reference and thus should NOT be ref-put
    virtual void onConnectionClose(LocalConnection *connection,
                                   Status status) = 0;
};

#endif  // ILOCALMSGHANDLER_H
