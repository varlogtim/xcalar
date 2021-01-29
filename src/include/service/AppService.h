// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DFHISTSERVICE_H_
#define _DFHISTSERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/App.xcrpc.h"
#include "xcalar/compute/localtypes/App.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace App
{
class AppService : public IAppService
{
  public:
    AppService();
    virtual ~AppService() = default;
    ServiceAttributes getAttr(const char *methodName) override;
    Status appStatus(const xcalar::compute::localtypes::App::AppStatusRequest
                         *appStatusRequest,
                     xcalar::compute::localtypes::App::AppStatusResponse
                         *appStatusResponse) override;
    Status driver(const DriverRequest *empty, DriverResponse *resp) override;

  private:
    static constexpr const char *ModuleName = "AppService";
};

}  // namespace App
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _DFHISTSERVICE_H_
