// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MEMORYSERVICE_H_
#define _MEMORYSERVICE_H_

#include "xcalar/compute/localtypes/memory.xcrpc.h"
#include "xcalar/compute/localtypes/memory.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace memory
{
class MemoryService : public IMemoryService
{
  public:
    MemoryService();

    virtual ~MemoryService() = default;

    virtual ServiceAttributes getAttr(const char *methodName) override;

    Status getUsage(const GetUsageRequest *empty,
                    GetUsageResponse *resp) override;

  private:
    static constexpr const char *moduleName = "MemoryService";
};

}  // namespace memory
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _MEMORYSERVICE_H_
