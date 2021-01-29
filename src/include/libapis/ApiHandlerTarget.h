// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_TARGET_H
#define _APIHANDLER_TARGET_H

#include "libapis/ApiHandler.h"

struct XcalarApiTargetInput;

class ApiHandlerTarget : public ApiHandler
{
  public:
    ApiHandlerTarget(XcalarApis api);
    virtual ~ApiHandlerTarget(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName = "libapis::apiHandler::target";
    XcalarApiTargetInput *input_;
    Flags getFlags() override;
};

#endif  // _APIHANDLER_TARGET_H
