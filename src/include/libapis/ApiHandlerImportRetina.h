// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_IMPORTRETINA_H
#define _APIHANDLER_IMPORTRETINA_H

#include "libapis/ApiHandler.h"

struct XcalarApiImportRetinaInput;

class ApiHandlerImportRetina : public ApiHandler
{
  public:
    ApiHandlerImportRetina(XcalarApis api);
    virtual ~ApiHandlerImportRetina(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;
    Status serializeOutput(XcalarApiOutput *buf, size_t bufSize) override;

  private:
    static constexpr const char *moduleName =
        "libapis::apiHandler::importRetina";
    XcalarApiImportRetinaInput *input_;
    Status isValidName(char *name);
    bool isValidNameChar(char c);
    Flags getFlags() override;
};

#endif  // _APIHANDLER_IMPORTRETINA_H
