// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APIHANDLER_APPRUN_H
#define APIHANDLER_APPRUN_H

#include "libapis/ApiHandler.h"

struct XcalarApiAppRunInput;

class ApiHandlerAppRun : public ApiHandler
{
  public:
    ApiHandlerAppRun(XcalarApis api);
    ~ApiHandlerAppRun() override{};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *ModuleName = "libapis::apiHandler::appRun";
    XcalarApiAppRunInput *input_;
    Flags getFlags() override;
};

#endif  // APIHANDLER_APPRUN_H
