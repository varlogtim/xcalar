// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _CONNECTORSSERVICE_H_
#define _CONNECTORSSERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Connectors.xcrpc.h"
#include "xcalar/compute/localtypes/Connectors.pb.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

class UserMgr;

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Connectors
{
class ConnectorsService : public IConnectorsService
{
  public:
    ConnectorsService();
    virtual ~ConnectorsService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status removeFile(const RemoveFileRequest *request,
                      google::protobuf::Empty *empty) override;
    Status listFiles(const ListFilesRequest *request,
                     ListFilesResponse *response) override;

  private:
    static constexpr const char *ModuleName = "ConnectorsService";
    UserMgr *userMgr_ = NULL;
};
}  // namespace File
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _CONNECTORSSERVICE_H_
