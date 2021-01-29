// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#include "RandDag.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"

#include "test/QA.h"

static constexpr const char *moduleName = "libDagTest";

RandDag::RandDag(uint64_t numNodes) : numNodes_(numNodes)
{
    rndInitHandle(&rndHandle_, sysGetTid());
    matrix_ =
        (unsigned **) memAllocExt(numNodes * sizeof(*matrix_), moduleName);
    assert(matrix_ != NULL);

    matrix_[0] = (unsigned *) memCallocExt(numNodes * numNodes,
                                           sizeof(*matrix_[0]),
                                           moduleName);
    assert(matrix_[0] != NULL);

    for (uint64_t ii = 1; ii < numNodes; ++ii) {
        matrix_[ii] = matrix_[0] + ii * numNodes;
    }

    nodeIdArray_ = (DagTypes::NodeId *) memCallocExt(numNodes,
                                                     sizeof(*nodeIdArray_),
                                                     moduleName);

    nodeStateArray_ = (Atomic64 *) memCallocExt(numNodes,
                                                sizeof(*nodeStateArray_),
                                                moduleName);

    uint32_t randNum = rndGenerate32(&rndHandle_);
    unsigned randNumIdx = 32;
    unsigned mask = 1;

    // initialize matrix
    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        uint64_t parentCount = 0;
        for (uint64_t jj = 0; jj < ii; ++jj) {
            if (randNumIdx == 0) {
                randNum = rndGenerate32(&rndHandle_);
                randNumIdx = 32;
            }

            matrix_[ii][jj] = randNum & mask;
            randNum = randNum >> 1;
            randNumIdx--;

            if (matrix_[ii][jj]) {
                if (++parentCount == MaxNumParent) {
                    break;
                }
            }
        }
    }
}

RandDag::~RandDag()
{
    memFree(matrix_[0]);
    memFree(matrix_);
    memFree(nodeIdArray_);
    memFree(nodeStateArray_);
}

void
RandDag::getParentNodeIds(uint64_t randNodeIdx,
                          DagTypes::NodeId *parentNodeIdArray,
                          uint64_t *numParentOut)
{
    uint64_t numParent = 0;

    // numNodes - 1 because the graph does not allow edge connect to same node
    for (uint64_t ii = 0; ii < randNodeIdx; ++ii) {
        if (matrix_[randNodeIdx][ii] != 0) {
            if (parentNodeIdArray != NULL) {
                parentNodeIdArray[numParent] = nodeIdArray_[ii];
            }
            numParent++;
        }
    }

    *numParentOut = numParent;
}

void
RandDag::saveNodeId(uint64_t randNodeIdx, DagTypes::NodeId nodeId)
{
    assert(nodeIdArray_[randNodeIdx] == 0);
    assert(nodeId != DagTypes::InvalidDagNodeId);
    nodeIdArray_[randNodeIdx] = nodeId;
}

DagTypes::NodeId
RandDag::getNodeId(uint64_t randNodeIdx)
{
    return nodeIdArray_[randNodeIdx];
}

void
RandDag::deleteNode(uint64_t idx)
{
    // mark the node is deleted
    matrix_[idx][idx] = 1;

    // remove from childnode's parent list
    for (uint64_t ii = idx + 1; ii < numNodes_; ++ii) {
        matrix_[ii][idx] = 0;
    }
}

void
RandDag::getRandDag(uint64_t idx, uint64_t numNodes, uint64_t *nodeArray)
{
    assert(numNodes > 0);

    uint64_t nodeCount = 0;
    uint64_t loopIndex = 0;

    nodeArray[nodeCount++] = idx;

    while (loopIndex < nodeCount) {
        uint64_t curNodeId = nodeArray[loopIndex];

        for (uint64_t ii = 0; nodeCount < numNodes && ii < curNodeId; ++ii) {
            if (matrix_[curNodeId][ii]) {
                assert(nodeCount < numNodes);
                nodeArray[nodeCount++] = ii;
            }
        }
        loopIndex++;
    }

    // assert(nodeCount == numNodes);
}

void
RandDag::outputRandDag(char *buf, size_t size)
{
    int ret;
    ret = sprintf(buf, "     ");
    buf += ret;
    for (uint64_t jj = 0; jj < numNodes_; ++jj) {
        ret = sprintf(buf, "%lu   ", jj);
        buf += ret;
    }
    ret = sprintf(buf, "\n");
    buf += ret;

    ret = sprintf(buf, "     ");
    for (uint64_t jj = 0; jj < numNodes_; ++jj) {
        ret = sprintf(buf, "~   ");
        buf += ret;
    }
    ret = sprintf(buf, "\n");
    buf += ret;

    for (uint64_t ii = 0; ii < numNodes_; ++ii) {
        for (uint64_t jj = 0; jj < numNodes_; ++jj) {
            if (jj == 0) {
                ret = sprintf(buf, "%lu:   ", ii);
                buf += ret;
            }

            ret = sprintf(buf, "%d   ", matrix_[ii][jj]);
            buf += ret;
        }
        ret = sprintf(buf, "\n");
        buf += ret;
    }
}

void
RandDag::printRandDag()
{
    printf("     ");
    for (uint64_t jj = 0; jj < numNodes_; ++jj) {
        printf("%lu   ", jj);
    }
    printf("\n");

    printf("     ");
    for (uint64_t jj = 0; jj < numNodes_; ++jj) {
        printf("~   ");
    }
    printf("\n");

    for (uint64_t ii = 0; ii < numNodes_; ++ii) {
        for (uint64_t jj = 0; jj < numNodes_; ++jj) {
            if (jj == 0) printf("%lu:   ", ii);
            printf("%d   ", matrix_[ii][jj]);
        }
        printf("\n");
    }
}

void
RandDag::verifyRootNodeIds(DagTypes::NodeId *rootNodeArray,
                           uint64_t numRootNode)
{
    int numNodeLeft = (int) numRootNode;

    for (uint64_t ii = 0; ii < numNodes_; ++ii) {
        if (matrix_[ii][ii]) {
            // this node is deleted;
            continue;
        }

        if (nodeIdArray_[ii] == DagTypes::InvalidDagNodeId) {
            // this node never get created
            continue;
        }

        bool isRoot = true;
        for (uint64_t jj = 0; jj < ii; ++jj) {
            if (matrix_[ii][jj]) {
                isRoot = false;
            }
        }

        if (isRoot) {
            DagTypes::NodeId nodeId = nodeIdArray_[ii];
            bool found = false;
            for (uint64_t kk = 0; kk < numRootNode; ++kk) {
                if (rootNodeArray[kk] == nodeId) {
                    found = true;
                    rootNodeArray[kk] = DagTypes::InvalidDagNodeId;
                    numNodeLeft--;
                }
            }
            if (found == false) {
                this->printRandDag();
            }

            assert(found);
        }
    }

    // The Xcalar DAG could have nodes that generated by other randDag
    assert(numNodeLeft >= 0);
}

void
RandDag::verifyChildNodeIds(uint64_t randDagNodeId,
                            DagTypes::NodeId *childNodeArray,
                            uint64_t numChildNode,
                            uint64_t *numChildLeft)
{
    int numNodeLeft = (int) (numChildNode);

    for (uint64_t ii = randDagNodeId + 1; ii < numNodes_; ++ii) {
        if (matrix_[ii][ii]) {
            // this node is deleted;
            continue;
        }

        if (matrix_[ii][randDagNodeId]) {
            DagTypes::NodeId nodeId = nodeIdArray_[ii];
            if (nodeId == DagTypes::InvalidDagNodeId) {
                continue;
            }

            if (this->nodeDeleted(ii)) {
                continue;
            }

            bool found = false;
            for (uint64_t jj = 0; jj < numChildNode; ++jj) {
                if (childNodeArray[jj] == nodeId) {
                    found = true;
                    childNodeArray[jj] = DagTypes::InvalidDagNodeId;
                    numNodeLeft--;
                }
            }
            if (found == false) {
                this->printRandDag();
            }
            assert(found);
        }
    }

    if (numChildLeft != NULL) {
        *numChildLeft = numNodeLeft;
    }
}

uint64_t
RandDag::getNodeIdx(DagTypes::NodeId nodeId)
{
    for (uint64_t ii = 0; ii < numNodes_; ++ii) {
        if (nodeIdArray_[ii] == nodeId) {
            return ii;
        }
    }

    return numNodes_;
}

void
RandDag::verifyParentNodeIds(uint64_t randDagNodeId,
                             DagTypes::NodeId *parentNodeArray,
                             uint64_t numParentNode)
{
    int numNodeLeft = (int) numParentNode;

    for (uint64_t ii = 0; ii < randDagNodeId; ++ii) {
        assert(!matrix_[randDagNodeId][randDagNodeId]);

        if (matrix_[randDagNodeId][ii]) {
            DagTypes::NodeId nodeId = nodeIdArray_[ii];
            if (nodeId == DagTypes::InvalidDagNodeId) {
                continue;
            }

            if (this->nodeDeleted(ii)) {
                continue;
            }

            bool found = false;
            for (uint64_t jj = 0; jj < numParentNode; ++jj) {
                if (parentNodeArray[jj] == nodeId) {
                    found = true;
                    parentNodeArray[jj] = DagTypes::InvalidDagNodeId;
                    numNodeLeft--;
                }
            }
            if (found == false) {
                this->printRandDag();
            }
            assert(found);
        }
    }

    // The Xcalar DAG could have nodes that generated by other randDag
    assert(numNodeLeft == 0);
}

bool
RandDag::reserveNode(uint64_t idx)
{
    int64_t ret = atomicCmpXchg64(&nodeStateArray_[idx],
                                  (int64_t) NodeAvailable,
                                  (int64_t) NodeBusy);

    return ret == (int64_t) NodeAvailable;
}

void
RandDag::releaseNode(uint64_t idx)
{
    int64_t ret = atomicCmpXchg64(&nodeStateArray_[idx],
                                  (int64_t) NodeBusy,
                                  (int64_t) NodeAvailable);

    assert(ret == (int64_t) NodeBusy);
}
