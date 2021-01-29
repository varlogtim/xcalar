// Copyright 2014 - 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#ifndef _DAGNODETYPES_H_
#define _DAGNODETYPES_H_

#include "libapis/LibApisCommon.h"
#include "operators/OperatorsTypes.h"
#include "dag/DagTypes.h"
#include "util/Stopwatch.h"
#include "table/TableNs.h"

// XXX: This is a temporary fix to make the DAG node struct public, due to
// Optimizer.cpp needs internal access to the DAG node struct. The DAG node
// struct should be made private to DAG module after rewrite the query graph
//
// Needs PageSize alignment for Sparse copy.
class __attribute__((aligned(PageSize))) DagNodeTypes
{
  public:
    struct DagNodeOrder {
        DagTypes::NodeId prev;
        DagTypes::NodeId next;
    };

    struct DagNodeHdr {
        XcalarApiDagNodeHdr apiDagNodeHdr;
        XcalarApiInput *apiInput;
        struct timespec timeStamp;
        OpDetails opDetails;
        DagNodeOrder dagNodeOrder;  // used to track chronological order
    };

    struct NodeIdListElt {
        DagTypes::NodeId nodeId;
        struct NodeIdListElt *next;
    };

    struct Node {
        DagNodeHdr dagNodeHdr;

        uint64_t numChild;
        NodeIdListElt *childrenList;

        uint64_t numParent;
        NodeIdListElt *parentsList;

        RefCount refCount;

        IntHashTableHook globalIdHook;
        IntHashTableHook idHook;
        IntHashTableHook xdbIdHook;
        IntHashTableHook tableIdHook;
        StringHashTableHook nameHook;

        // These are really just a generic context.
        // Depending on how we specialize dagNodes, we can get
        // 1 of them. We don't need all of them
        unsigned numNodes;
        Stopwatch stopwatch;

        Scalar *scalarResult = NULL;  // Used by aggregate
        void *context = NULL;         // Used by dataset.

        void *annotations = NULL;  // Used by the query optimizer

        OpStatus opStatus;  // Used by operation

        // the xdb backing this table
        Xid xdbId = XidInvalid;
        DagTypes::DagId dagId = DagTypes::DagIdInvalid;
        TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
        bool dropTable = false;

        char log[MaxTxnLogSize];
        StatusCode status;

        DagTypes::NodeId getId() const
        {
            return dagNodeHdr.apiDagNodeHdr.dagNodeId;
        }

        Xid getXdbId() const { return xdbId; }

        const char *getName() const { return dagNodeHdr.apiDagNodeHdr.name; }

        TableNsMgr::TableId getTableId() const { return tableId; }

        void setTableDropped() { dropTable = true; }

        bool tableDropped() { return dropTable; }
    };
};

#endif  // _DAGNODETYPES_H_
