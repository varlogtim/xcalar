// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CGROUP_SERVICE_H_
#define _CGROUP_SERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Cgroup.xcrpc.h"
#include "xcalar/compute/localtypes/Cgroup.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Cgroup
{
class CgroupService : public ICgroupService
{
  public:
    CgroupService() = default;
    virtual ~CgroupService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status process(const CgRequest *cgRequest, CgResponse *cgResponse) override;

  private:
    static constexpr const char *ModuleName = "CgroupService";
};
}  // namespace Cgroup
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _CGROUP_SERVICE_H_
