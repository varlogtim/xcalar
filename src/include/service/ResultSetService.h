// Copyright 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _RESULT_SET_PROTO_SERVICE_H_
#define _RESULT_SET_PROTO_SERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/ResultSet.xcrpc.h"
#include "xcalar/compute/localtypes/ResultSet.pb.h"
#include "dag/DagTypes.h"
#include "xcalar/compute/localtypes/Workbook.xcrpc.h"
#include "xcalar/compute/localtypes/Workbook.pb.h"

class UserMgr;

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace ResultSet
{
class ResultSetService : public IResultSetService
{
  public:
    ResultSetService();
    virtual ~ResultSetService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status make(const ResultSetMakeRequest *rsMakeRequest,
                ResultSetMakeResponse *rsMakeResponse) override;
    Status release(const ResultSetReleaseRequest *rsReleaseRequest,
                   google::protobuf::Empty *empty) override;
    Status next(const ResultSetNextRequest *rsNextRequest,
                ResultSetNextResponse *rsNextResponse) override;
    Status seek(const ResultSetSeekRequest *rsSeekRequest,
                google::protobuf::Empty *empty) override;

  private:
    static constexpr const char *ModuleName = "ResultSetService";

    Status setupSessionScope(
        const xcalar::compute::localtypes::Workbook::WorkbookScope *scope,
        Dag **retDag,
        bool *retTrackOpsToSession);

    Status teardownSessionScope(
        const xcalar::compute::localtypes::Workbook::WorkbookScope *scope);

    UserMgr *userMgr_ = NULL;
};
}  // namespace ResultSetProto
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _RESULT_SET_PROTO_SERVICE_H_
