// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORHANDLER_BULKLOAD_H
#define _OPERATORHANDLER_BULKLOAD_H

#include "libapis/ApiHandler.h"
#include "libapis/OperatorHandler.h"

class OperatorHandlerBulkLoad final : public OperatorHandler
{
  public:
    OperatorHandlerBulkLoad(XcalarApis api);
    virtual ~OperatorHandlerBulkLoad(){};

    Status setArg(XcalarApiInput *input,
                  size_t inputSize,
                  bool parentNodeIdsToBeProvided) override;
    Status createDagNode(DagTypes::NodeId *dstNodeIdOut,
                         DagTypes::GraphType srcGraphType,
                         const char *nodeName,
                         uint64_t numParents,
                         Dag **parentGraphs,
                         DagTypes::NodeId *parentNodeIds) override;
    const char *getDstNodeName() override;
    const char *getDstNodeName(XcalarApiOutput *output) override;
    Status createXdb(void *optimizerContext) override;
    // Only to be used by queryEvaluate and OperatorHandlerIndex to allow us
    // to re-use already loaded datasets
    Status setPreserveDatasetUuidMode();
    Status unsetPreserveDatasetUuidMode();

  private:
    static constexpr const char *moduleName =
        "libapis::operatorHandler::bulkLoad";
    XcalarApiBulkLoadInput *input_;
    bool preserveDatasetUuid_;

    Flags getFlags() override;

    Status runHandler(
        XcalarApiOutput **outputOut,
        size_t *outputSizeOut,
        void *optimizerContext,
        DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals]) override;

    Status getParentNodes(uint64_t *numParentsOut,
                          XcalarApiUdfContainer **sessionContainersOut,
                          DagTypes::NodeId **parentNodeIdsOut,
                          DagTypes::DagId **parentGraphIdsOut) override;
    Status prefixDatasetName(const char *nameIn,
                             char *nameBuf,
                             size_t nameBufSize);
};

#endif  // _OPERATORHANDLER_BULKLOAD_H
