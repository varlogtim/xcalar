// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_COALESCE_H
#define _APIHANDLER_COALESCE_H

#include "libapis/ApiHandler.h"
#include "libapis/LibApisCommon.h"
#include "xdb/HashTree.h"

class ApiHandlerCoalesce : public ApiHandler
{
  public:
    ApiHandlerCoalesce(XcalarApis api);
    virtual ~ApiHandlerCoalesce();

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    XcalarApiCoalesceInput *input_;
    HashTreeRefHandle refHandle_;
    bool handleInit_ = false;

    static constexpr const char *moduleName = "libapis::apiHandler::Coalesce";
    Flags getFlags() override;
};

#endif  // _APIHANDLER_COALESCE_H
