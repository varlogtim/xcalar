// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _OPERATORHANDLER_H
#define _OPERATORHANDLER_H

#include "libapis/ApiHandler.h"
#include "libapis/LibApisEnums.h"
#include "operators/OperatorsTypes.h"
#include "operators/Operators.h"
#include "table/TableNs.h"

class OperatorHandler : public ApiHandler
{
  public:
    OperatorHandler(XcalarApis api);
    virtual ~OperatorHandler();

    DagTypes::NodeId dstNodeId_;
    XdbId dstXdbId_ = XidInvalid;
    TableNsMgr::TableId dstTableId_ = TableNsMgr::InvalidTableId;

    Status run(XcalarApiOutput **outputOut, size_t *outputSizeOut) override;

    virtual Status run(XcalarApiOutput **outputOut,
                       size_t *outputSizeOut,
                       void *optimizerContext);

    Status setArg(XcalarApiInput *input, size_t inputSize) override;
    virtual Status setArgQueryGraph(XcalarApiInput *input, size_t inputSize);

    Status createDagNode(DagTypes::NodeId *dstNodeIdOut,
                         DagTypes::GraphType srcGraphType);
    virtual Status createDagNode(DagTypes::NodeId *dstNodeIdOut,
                                 DagTypes::GraphType srcGraphType,
                                 const char *nodeName,
                                 uint64_t numParents,
                                 Dag **parentGraphs,
                                 DagTypes::NodeId *parentNodeIds) = 0;
    void setParents(uint64_t numParents,
                    DagTypes::NodeId *parentNodeIds,
                    XcalarApiUdfContainer *parentSessionContainers,
                    DagTypes::DagId *parentGraphIds,
                    Dag **parentGraphs);

    virtual Status createXdb(void *optimizerContext)
    {
        assert(0);
        return StatusUnimpl;
    }

    // We can get dstNodeName either from input (user-provided), or
    // from output (the final name we have decided on)
    virtual const char *getDstNodeName() = 0;
    virtual const char *getDstNodeName(XcalarApiOutput *output) = 0;
    static Status copySrcXdbMeta(void *optimizerContext,
                                 XdbMeta *srcMeta,
                                 NewTupleMeta *tupMeta,
                                 unsigned &numDatasets,
                                 DsDatasetId *&datasetIds,
                                 unsigned &numImmediates,
                                 const char **&immediateNames,
                                 unsigned &numFatptrs,
                                 const char **&fatptrPrefixNames);

    static Status createSrcXdbCopy(
        XdbId srcXdbId,
        XdbId dstXdbId,
        DfFieldType *immediateTypesIn,
        char *immediateNamesIn[XcalarApiMaxFieldNameLen + 1],
        unsigned numImmediatesIn,
        void *optimizerContext);

    static MustCheck Status
    getSourceDagNode(const char *srcTableName,
                     Dag *dstGraph,
                     XcalarApiUdfContainer *retSessionContainer,
                     DagTypes::DagId *retSrcGraphId,
                     DagTypes::NodeId *retSrcNodeId);

  protected:
    bool xdbCreated_ = false;

    TableNsMgr::TableHandleTrack dstTableTrack_;

    // XXX TODO Parent DagNode tracking could use a lot of code restructure.
    uint64_t numParents_ = 0;
    XcalarApiUdfContainer *parentSessionContainers_ = NULL;
    DagTypes::DagId *parentGraphIds_ = NULL;
    Dag **parentGraphs_ = NULL;
    DagTypes::NodeId *parentNodeIds_ = NULL;
    bool *parentRefAcquired_ = NULL;
    char *parentNodeIdBuf_ = NULL;
    TableNsMgr::TableHandleTrack *parentTableTrack_ = NULL;

    XdbMgr *xdbMgr_;
    Operators *op_;
    char query_[XcalarApiMaxSingleQuerySize];

    // run doesn't rely on names, and relies solely on nodeIds
    // in the sessionGraph. This means we need to perform a lookup
    // of each name to get each node's nodeId. This is usually done
    // in a call to createDagNode in the case of a operatorHandler
    // This is becuase we need the nodeIds for the src as well as the dst,
    // and dst can only be determined after a call to createDagNode.
    // Alternatively, the caller can provide the parentNodeIds to us
    // directly, in which we case we ignore the src's names altogether.
    bool parentNodeIdsToBeProvided_;

  private:
    // Some handlers destroy the opStatus on our behalf.
    // All operatorHandlers require opStatus to have been created
    //
    // failureTableIdOut specifies an output table - if the handler generates
    // it, it must have a specific schema to report details about failures the
    // operator encounters on any row, while processing the input table. The
    // schema must be is a two-column schema: {numRowsFailed, failureDescString}
    //
    // NOTE: 'failureTableIdOut' is so named since it really represents a table
    // and so it should be a table-id (of the failure table potentially
    // generated by the runHandler()). However, tableIDs don't exist currently
    // but since there's a 1-1 correspondence between a dag-node and a table,
    // the dag-node-id serves as a table-id - so the type is DagTypes::NodeId
    // for now...
    // ... we should change the type once we reach a point where the 1-1
    // correspondence is broken and a table namespace/TableId type is added.
    //
    // NOTE: The max number of evals for which failure is handled is specified
    // via XcalarApiMaxFailureEvals - so that's the max no of failure tabs which
    // can be generated

    virtual Status runHandler(
        XcalarApiOutput **outputOut,
        size_t *outputSizeOut,
        void *optimizerContext,
        DagTypes::NodeId *failureTableIdOut[XcalarApiMaxFailureEvals]) = 0;

    virtual Status setArg(XcalarApiInput *input,
                          size_t inputSize,
                          bool parentNodeIdsToBeProvided) = 0;

    Status setQuery(XcalarApiInput *input);

    virtual Status getParentNodes(uint64_t *numParentsOut,
                                  XcalarApiUdfContainer **sessionContainersOut,
                                  DagTypes::NodeId **parentNodeIdsOut,
                                  DagTypes::DagId **parentGraphIdsOut) = 0;
};

#endif  // _OPERATORHANDLER_H
