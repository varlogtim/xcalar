// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef APIHANDLER_APPSET_H
#define APIHANDLER_APPSET_H

#include "libapis/ApiHandler.h"
#include "app/App.h"

class ApiHandlerAppSet : public ApiHandler
{
  public:
    ApiHandlerAppSet(XcalarApis api);
    ~ApiHandlerAppSet() override{};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *ModuleName = "libapis::apiHandler::appSet";
    App::Packed *input_;
    Flags getFlags() override;
};

#endif  // APIHANDLER_APPSET_H
