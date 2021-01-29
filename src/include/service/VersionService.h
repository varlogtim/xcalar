// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _VERSIONSERVICE_H_
#define _VERSIONSERVICE_H_

#include "xcalar/compute/localtypes/Version.xcrpc.h"
#include "xcalar/compute/localtypes/Service.pb.h"
#include "libapis/LibApisCommon.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Version
{
class VersionService : public IVersionService
{
  public:
    VersionService();

    virtual ~VersionService() = default;

    virtual ServiceAttributes getAttr(const char *methodName) override;

    Status getVersion(const google::protobuf::Empty *empty,
                      GetVersionResponse *response) override;

  private:
    static constexpr const char *moduleName = "VersionService";
};

}  // namespace Version
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _VERSIONSERVICE_H_
