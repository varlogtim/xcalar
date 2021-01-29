// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_PER_NODE_TOP_
#define _APIHANDLER_PER_NODE_TOP_

#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"

class ApiHandlerPerNodeTop : public ApiHandler
{
  public:
    ApiHandlerPerNodeTop(XcalarApis api);
    virtual ~ApiHandlerPerNodeTop(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName = "libapis::apiHandler::perNodeTop";
    XcalarApiTopInput *input_;
    Flags getFlags() override;
};

#endif  // _APIHANDLER_PER_NODE_TOP_
