// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APISRECVCLIENT_H_
#define _APISRECVCLIENT_H_

#include "libapis/WorkItem.h"
#include "libapis/LibApisCommon.h"

class ApisRecvClient final
{
  public:
    enum class State : uint32_t {
        NeedWorkItem,
        NeedApiInput,
        NeedUserId,
        NeedSessionInfo,
        Ready,
        SocketError,
        ApiInputInvalid,
        Destructed,
    };

    ApisRecvClient(SocketHandle clientFd);
    ~ApisRecvClient();
    uint64_t getClientFd() const { return clientFd_; }

    bool isReady();
    Status processIncoming();
    Status sendOutput(XcalarApiOutput *output, size_t outputSize);
    void sendRejection(Status rejectionStatus);
    Status drainIncomingTcp();

    XcalarWorkItem workItem_;
    IntHashTableHook hook;

  private:
    SocketHandle clientFd_;
    State state_;
};

#endif  // _APISRECVCLIENT_H_
