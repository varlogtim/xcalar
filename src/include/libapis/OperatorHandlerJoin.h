// Copyright 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORHANDLER_JOIN_H
#define _OPERATORHANDLER_JOIN_H

#include "libapis/ApiHandler.h"
#include "libapis/OperatorHandler.h"
#include "table/TableNs.h"

struct XcalarApiJoinInput;

class OperatorHandlerJoin final : public OperatorHandler
{
  public:
    OperatorHandlerJoin(XcalarApis api);
    virtual ~OperatorHandlerJoin();

    Status setArg(XcalarApiInput *input,
                  size_t inputSize,
                  bool parentNodeIdsToBeProvided) override;
    Status createDagNode(DagTypes::NodeId *dstNodeIdOut,
                         DagTypes::GraphType srcGraphType,
                         const char *nodeName,
                         uint64_t numParents,
                         Dag **parentGraphs,
                         DagTypes::NodeId *parentNodeIds) override;

    Status createXdb(void *optimizerContext) override;
    const char *getDstNodeName() override;
    const char *getDstNodeName(XcalarApiOutput *output) override;

    static Status validateKeys(XdbMeta *left, XdbMeta *right);

  private:
    static constexpr const char *moduleName = "libapis::operatorHandler::join";
    XcalarApiJoinInput *input_;
    unsigned numAggVariables_ = 0;
    const char **aggVariableNames_ = NULL;
    // aggVariableNames refers the strings in ast_.
    XcalarEvalAstCommon ast_;
    bool astCreated_ = false;

    TableNsMgr::TableHandleTrack broadcastHandleTrack_;
    bool broadcastTableCreated_ = false;
    bool broadcastRef_ = false;
    DagTypes::NodeId broadcastTableId_ = XidInvalid;
    char broadcastTable_[XcalarApiMaxTableNameLen + 1];
    XcalarApiInput *newApiInput_ = NULL;

    const char *tableToBroadcast_ = NULL;

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

    Status generateFullRenameMap();
    Status addKeyToRenameMap();
};

#endif  // _OPERATORHANDLER_JOIN_H
