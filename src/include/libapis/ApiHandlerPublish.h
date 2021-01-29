// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_PUBLISH_H
#define _APIHANDLER_PUBLISH_H

#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"
#include "table/TableNs.h"

class ApiHandlerPublish : public ApiHandler
{
  public:
    ApiHandlerPublish(XcalarApis api);
    virtual ~ApiHandlerPublish();

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    Xid hashTreeId_;
    XcalarApiPublishInput *input_;
    bool parentRefAcquired_ = false;
    DagTypes::NodeId parentId_;
    TableNsMgr::TableHandleTrack handleTrack_;

    static constexpr const char *moduleName = "libapis::apiHandler::Publish";
    Flags getFlags() override;
};

#endif  // _APIHANDLER_PUBLISH_H
