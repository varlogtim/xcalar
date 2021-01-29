// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _STATSSERVICE_H_
#define _STATSSERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Stats.xcrpc.h"
#include "xcalar/compute/localtypes/Service.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Stats
{
class StatsService : public IStatsService
{
  public:
    static constexpr const char *moduleName = "StatsService";
    ServiceAttributes getAttr(const char *methodName) override;
    Status getStats(const GetStatsRequest *request,
                    GetStatsResponse *response) override;
    Status getLibstats(const GetLibstatsRequest *request,
                       GetLibstatsResponse *response) override;
    Status resetStats(const ResetStatsRequest *request,
                      ResetStatsResponse *response) override;
};
}  // namespace Stats
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _ECHOSERVICE_H_
