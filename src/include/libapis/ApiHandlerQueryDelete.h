// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_QUERYDELETE_H
#define _APIHANDLER_QUERYDELETE_H

#include "libapis/ApiHandler.h"

struct XcalarApiQueryNameInput;

class ApiHandlerQueryDelete : public ApiHandler
{
  public:
    ApiHandlerQueryDelete(XcalarApis api);
    virtual ~ApiHandlerQueryDelete(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName =
        "libapis::apiHandler::queryDelete";
    XcalarApiQueryNameInput *input_;
    Flags getFlags() override;
};

#endif  // _APIHANDLER_QUERYDELETE_H
