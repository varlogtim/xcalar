// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _APIHANDLER_DATASETGETMETA_H
#define _APIHANDLER_DATASETGETMETA_H

#include "libapis/ApiHandler.h"

class ApiHandlerDatasetGetMeta : public ApiHandler
{
  public:
    ApiHandlerDatasetGetMeta(XcalarApis api);
    virtual ~ApiHandlerDatasetGetMeta(){};

    Status run(XcalarApiOutput **output, size_t *outputSize) override;
    Status setArg(XcalarApiInput *input, size_t inputSize) override;

  private:
    static constexpr const char *moduleName =
        "libapis::apiHandler::datasetGetMeta";
    XcalarApiDatasetGetMetaInput *input_;
    Flags getFlags() override;
};

#endif  // _APIHANDLER_DATASETGETMETA_H
