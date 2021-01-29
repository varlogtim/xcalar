// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERY_SERVICE_H_
#define _QUERY_SERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Query.xcrpc.h"
#include "xcalar/compute/localtypes/Query.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Query
{
class QueryService : public IQueryService
{
  public:
    QueryService() = default;
    virtual ~QueryService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status list(const ListRequest *listRequest,
                ListResponse *listResponse) override;

  private:
    static constexpr const char *ModuleName = "QueryService";
};
}  // namespace Query
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _QUERY_SERVICE_H_
