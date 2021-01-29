// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DAGNODESERVICE_H_
#define _DAGNODESERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/DagNode.xcrpc.h"
#include "xcalar/compute/localtypes/DagNode.pb.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

class UserMgr;

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace DagNode
{
class DagNodeService : public IDagNodeService
{
  public:
    DagNodeService();
    virtual ~DagNodeService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status pin(const DagNodeInputMsg *request,
               google::protobuf::Empty *empty) override;
    Status unpin(const DagNodeInputMsg *request,
                 google::protobuf::Empty *empty) override;

  private:
    static constexpr const char *ModuleName = "DagNodeService";
};

}  // namespace DagNode
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _DAGNODESERVICE_H_
