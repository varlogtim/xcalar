// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_LISTRETINAS_H
#define _APIHANDLER_LISTRETINAS_H

#include "libapis/ApiHandler.h"

class ApiHandlerListRetinas : public ApiHandler
{
  public:
    ApiHandlerListRetinas(XcalarApis api);
    virtual ~ApiHandlerListRetinas(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName =
        "libapis::apiHandler::listRetinas";
    Flags getFlags() override;
};

#endif  // _APIHANDLER_LISTRETINAS_H
