// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_UPDATERETINA_H
#define _APIHANDLER_UPDATERETINA_H

#include "libapis/ApiHandler.h"

struct XcalarApiUpdateRetinaInput;

class ApiHandlerUpdateRetina : public ApiHandler
{
  public:
    ApiHandlerUpdateRetina(XcalarApis api);
    virtual ~ApiHandlerUpdateRetina(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName =
        "libapis::apiHandler::updateRetina";
    XcalarApiUpdateRetinaInput *input_;
    Flags getFlags() override;
    bool isValidParamType(XcalarApis paramType);
};

#endif  // _APIHANDLER_UPDATERETINA_H
