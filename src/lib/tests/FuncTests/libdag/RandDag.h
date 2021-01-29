// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include <pthread.h>
#include "util/Random.h"
#include "dag/DagTypes.h"
#include "util/Atomics.h"

class RandDag
{
  public:
    RandDag(uint64_t numNodes);
    ~RandDag();

    void getParentNodeIds(uint64_t randNodeIdx,
                          DagTypes::NodeId *parentNodeIdArray,
                          uint64_t *numParentOut);

    void saveNodeId(uint64_t randNodeIdx, DagTypes::NodeId nodeId);

    uint64_t getNumNode() { return numNodes_; }
    DagTypes::NodeId getNodeId(uint64_t randNodeIdx);
    void deleteNode(uint64_t idx);

    bool nodeDeleted(uint64_t idx) { return matrix_[idx][idx]; };

    void getRandDag(uint64_t idx, uint64_t numNodes, uint64_t *nodeArray);

    void printRandDag();

    void verifyRootNodeIds(DagTypes::NodeId *rootNodeArray,
                           uint64_t numRootNode);

    void verifyChildNodeIds(uint64_t randDagNodeId,
                            DagTypes::NodeId *childNodeArray,
                            uint64_t numChildNode,
                            uint64_t *numChildLeft);

    void verifyParentNodeIds(uint64_t randDagNodeId,
                             DagTypes::NodeId *parentNodeArray,
                             uint64_t numParentNode);
    bool reserveNode(uint64_t idx);
    void releaseNode(uint64_t idx);
    uint64_t getNodeIdx(DagTypes::NodeId nodeId);

    void outputRandDag(char *buf, size_t size);

  private:
    enum {
        MaxNumParent = 2,
        NodeAvailable = 0,
        NodeBusy = 1,
    };
    unsigned **matrix_;
    uint64_t numNodes_;
    DagTypes::NodeId *nodeIdArray_;
    Atomic64 *nodeStateArray_;
    RandHandle rndHandle_;
};
