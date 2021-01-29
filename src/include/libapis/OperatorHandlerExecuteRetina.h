// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORHANDLER_EXECUTERETINA_H
#define _OPERATORHANDLER_EXECUTERETINA_H

#include "libapis/OperatorHandler.h"
#include "dag/DagLib.h"
#include "table/TableNs.h"

struct XcalarApiExecuteRetinaInput;

class OperatorHandlerExecuteRetina : public OperatorHandler
{
  public:
    OperatorHandlerExecuteRetina(XcalarApis api);
    virtual ~OperatorHandlerExecuteRetina();

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

    // ExecuteRetina is special. All other operatorHandlers should not have
    // to override run
    Status run(XcalarApiOutput **output,
               size_t *outputSize,
               void *optimizerContext) override;

  private:
    static constexpr const char *moduleName =
        "libapis::OperatorHandler::executeRetina";
    char nodeName_[DagTypes::MaxNameLen + 1];
    XcalarApiExecuteRetinaInput *input_;
    LibNsTypes::NsHandle nsHandle_;
    bool pathOpened_ = false;
    bool retinaImported_ = false;
    DagLib::DgRetina *retina_ = NULL;

    // XXX Todo All lot of this metadata looks similar to OperatorHandler
    // and this code duplication is hard to maintain. Needs to be cleaned up
    // and consolidated.
    uint64_t srcSessionOpsIdx_ = 0;
    bool *srcRefsAcquired_ = NULL;
    unsigned numSrcNodes_ = 0;
    DagTypes::DagId dstGraphId_ = DagTypes::DagIdInvalid;
    DagTypes::DagId *srcGraphIds_ = NULL;
    XcalarApiUdfContainer *srcUdfContainers_ = NULL;
    Dag **srcGraphs_ = NULL;
    Xid *srcNodeIds_ = NULL;
    TableNsMgr::TableHandleTrack *srcHandleTrack_ = NULL;

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
};

#endif  // _OPERATORHANDLER_EXECUTERETINA_H
