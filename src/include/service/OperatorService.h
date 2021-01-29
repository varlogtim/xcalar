// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATOR_SERVICE_H_
#define _OPERATOR_SERVICE_H_

#include "primitives/Primitives.h"
#include "xcalar/compute/localtypes/Operator.xcrpc.h"
#include "xcalar/compute/localtypes/Operator.pb.h"
#include "table/TableNs.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Operator
{
class OperatorService : public IOperatorService
{
  public:
    OperatorService() { atomicWrite64(&mergeOpRoundRobin_, 0); }
    virtual ~OperatorService() = default;

    ServiceAttributes getAttr(const char *methodName) override;

    Status opMerge(
        const xcalar::compute::localtypes::Operator::MergeRequest *request,
        google::protobuf::Empty *empty) override;

  private:
    static constexpr const char *moduleName = "OperatorService";
    Atomic64 mergeOpRoundRobin_;

    MustCheck Status
    mergeSetupHelper(Dag *curDag,
                     const char *tableName,
                     DagTypes::NodeId *retDagNodeId,
                     XdbId *retXdbId,
                     bool *retDagRefAcquired,
                     TableNsMgr::TableHandleTrack *retHandleTrack,
                     LibNsTypes::NsOpenFlags nsOpenFlag);

    void mergeTeardownHelper(bool *dagRefAcquired,
                             Dag *curDag,
                             DagTypes::NodeId dagNodeId,
                             const char *tableName,
                             TableNsMgr::TableHandleTrack *handleTrack);
};
}  // namespace Operator
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _OPERATOR_SERVICE_H_
