// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOGSERVICE_H_
#define _LOGSERVICE_H_

#include "xcalar/compute/localtypes/log.xcrpc.h"
#include "xcalar/compute/localtypes/log.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace log
{
class LogService : public ILogService
{
  public:
    LogService();

    virtual ~LogService() = default;

    virtual ServiceAttributes getAttr(const char *methodName) override;

    Status getLevel(const google::protobuf::Empty *empty,
                    GetLevelResponse *resp) override;

    Status setLevel(const SetLevelRequest *req,
                    google::protobuf::Empty *empty) override;

  private:
    static constexpr const char *moduleName = "LogService";
};

}  // namespace log
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _LOGSERVICE_H_
