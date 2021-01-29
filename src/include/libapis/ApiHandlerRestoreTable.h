// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_RESTORETABLE_H
#define _APIHANDLER_RESTORETABLE_H

#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"

class ApiHandlerRestoreTable : public ApiHandler
{
  public:
    ApiHandlerRestoreTable(XcalarApis api);
    virtual ~ApiHandlerRestoreTable(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    XcalarApiRestoreTableInput *input_;
    static constexpr const char *moduleName =
        "libapis::apiHandler::RestoreTable";
    Flags getFlags() override;
};

#endif  // _APIHANDLER_RESTORETABLE_H
