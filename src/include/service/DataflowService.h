// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _Dataflow_PROTO_SERVICE_H_
#define _Dataflow_PROTO_SERVICE_H_

#include "xcalar/compute/localtypes/Dataflow.xcrpc.h"
#include "xcalar/compute/localtypes/Dataflow.pb.h"

namespace xcalar
{
namespace compute
{
namespace localtypes
{
namespace Dataflow
{
class DataflowService : public IDataflowService
{
  public:
    DataflowService();
    virtual ~DataflowService() = default;

    ServiceAttributes getAttr(const char *methodName) override;
    Status execute(const ExecuteRequest *executeRequest,
                   ExecuteResponse *executeResponse) override;

    // actual implementation of the following methods are in expserver layer
    Status filter(const xcalar::compute::localtypes::Dataflow::FilterRequest
                      *xcalarcomputelocaltypesDataflowFilterRequest,
                  xcalar::compute::localtypes::Dataflow::FilterResponse
                      *xcalarcomputelocaltypesDataflowFilterResponse) override
    {
        return StatusUnimpl;
    }

    Status aggregate(
        const xcalar::compute::localtypes::Dataflow::AggregateRequest
            *xcalarcomputelocaltypesDataflowAggregateRequest,
        xcalar::compute::localtypes::Dataflow::AggregateResponse
            *xcalarcomputelocaltypesDataflowAggregateResponse) override
    {
        return StatusUnimpl;
    }

    Status aggregateWithEvalStr(
        const xcalar::compute::localtypes::Dataflow::AggregateEvalStrRequest
            *xcalarcomputelocaltypesDataflowAggregateEvalStrRequest,
        xcalar::compute::localtypes::Dataflow::AggregateResponse
            *xcalarcomputelocaltypesDataflowAggregateResponse) override
    {
        return StatusUnimpl;
    }

    Status map(const xcalar::compute::localtypes::Dataflow::MapRequest
                   *xcalarcomputelocaltypesDataflowMapRequest,
               xcalar::compute::localtypes::Dataflow::MapResponse
                   *xcalarcomputelocaltypesDataflowMapResponse) override
    {
        return StatusUnimpl;
    }

    Status genRowNum(
        const xcalar::compute::localtypes::Dataflow::GenRowNumRequest
            *xcalarcomputelocaltypesDataflowGenRowNumRequest,
        xcalar::compute::localtypes::Dataflow::GenRowNumResponse
            *xcalarcomputelocaltypesDataflowGenRowNumResponse) override
    {
        return StatusUnimpl;
    }

    Status project(const xcalar::compute::localtypes::Dataflow::ProjectRequest
                       *xcalarcomputelocaltypesDataflowProjectRequest,
                   xcalar::compute::localtypes::Dataflow::ProjectResponse
                       *xcalarcomputelocaltypesDataflowProjectResponse) override
    {
        return StatusUnimpl;
    }

    Status join(const xcalar::compute::localtypes::Dataflow::JoinRequest
                    *xcalarcomputelocaltypesDataflowJoinRequest,
                xcalar::compute::localtypes::Dataflow::JoinResponse
                    *xcalarcomputelocaltypesDataflowJoinResponse) override
    {
        return StatusUnimpl;
    }

    Status unionOp(const xcalar::compute::localtypes::Dataflow::UnionRequest
                       *xcalarcomputelocaltypesDataflowUnionRequest,
                   xcalar::compute::localtypes::Dataflow::UnionResponse
                       *xcalarcomputelocaltypesDataflowUnionResponse) override
    {
        return StatusUnimpl;
    }

    Status groupBy(const xcalar::compute::localtypes::Dataflow::GroupByRequest
                       *xcalarcomputelocaltypesDataflowGroupByRequest,
                   xcalar::compute::localtypes::Dataflow::GroupByResponse
                       *xcalarcomputelocaltypesDataflowGroupByResponse) override
    {
        return StatusUnimpl;
    }

    Status indexFromDataset(
        const xcalar::compute::localtypes::Dataflow::IndexFromDatasetRequest
            *xcalarcomputelocaltypesDataflowIndexFromDatasetRequest,
        xcalar::compute::localtypes::Dataflow::IndexFromDatasetResponse
            *xcalarcomputelocaltypesDataflowIndexFromDatasetResponse) override
    {
        return StatusUnimpl;
    }

    Status index(const xcalar::compute::localtypes::Dataflow::IndexRequest
                     *xcalarcomputelocaltypesDataflowIndexRequest,
                 xcalar::compute::localtypes::Dataflow::IndexResponse
                     *xcalarcomputelocaltypesDataflowIndexResponse) override
    {
        return StatusUnimpl;
    }

    Status sort(const xcalar::compute::localtypes::Dataflow::SortRequest
                    *xcalarcomputelocaltypesDataflowSortRequest,
                xcalar::compute::localtypes::Dataflow::SortResponse
                    *xcalarcomputelocaltypesDataflowSortResponse) override
    {
        return StatusUnimpl;
    }

    Status synthesize(
        const xcalar::compute::localtypes::Dataflow::SynthesizeRequest
            *xcalarcomputelocaltypesDataflowSynthesizeRequest,
        xcalar::compute::localtypes::Dataflow::SynthesizeResponse
            *xcalarcomputelocaltypesDataflowSynthesizeResponse) override
    {
        return StatusUnimpl;
    }

  private:
    static constexpr const char *moduleName = "DataflowService";
};
}  // namespace DataflowProto
}  // namespace localtypes
}  // namespace compute
}  // namespace xcalar

#endif  // _Dataflow_PROTO_SERVICE_H_
