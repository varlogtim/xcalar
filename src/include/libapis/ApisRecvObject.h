// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APISRECVOBJECT_H
#define _APISRECVOBJECT_H

#include "primitives/Primitives.h"
#include "runtime/Schedulable.h"
#include "libapis/LibApisEnums.h"
#include "libapis/ApiHandler.h"
#include "libapis/ApisRecvClient.h"
#include "bc/BufferCache.h"
#include "dag/DagTypes.h"

class ApisRecvObject final : public Schedulable
{
  public:
    ApisRecvObject(ApiHandler *apiHandler,
                   ApisRecvClient *apisRecvClient,
                   BcHandle *apisRecvObjectBc,
                   Semaphore *sem);
    virtual ~ApisRecvObject(){};
    void sendOutputToClient();
    void setApiStatus(Status apiStatus);
    MustCheck Status setTxnAndTxnLog();
    void run() override;
    void done() override;

  private:
    Semaphore *sem_;
    Status apiStatus_;
    XcalarApis api_;
    XcalarApiOutput *output_;
    size_t outputSize_;
    Dag *sessionGraph_;
    DagTypes::NodeId workspaceGraphNodeId_;
    ApiHandler *apiHandler_;
    BcHandle *apisRecvObjectBc_;
    ApisRecvClient *apisRecvClient_;
};

#endif  // _APISRECVOBJECT_H
