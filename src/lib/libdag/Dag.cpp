// Copyright 2015 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/mman.h>

#include "util/Math.h"
#include "dag/Dag.h"
#include "dag/DagLib.h"
#include "util/MemTrack.h"
#include "operators/Operators.h"
#include "msg/Xid.h"
#include "operators/OperatorsApiWrappers.h"
#include "dataset/Dataset.h"
#include "xdb/Xdb.h"
#include "strings/String.h"
#include "msg/TwoPcFuncDefs.h"
#include "DagNodeGvm.h"
#include "gvm/Gvm.h"
#include "WorkItem.h"

#include "DurableVersions.h"
#include "subsys/DurableSession.pb.h"
#include "durable/Durable.h"
#include "table/ResultSet.h"
#include "operators/XcalarEval.h"
#include "usr/Users.h"
#include "util/System.h"

static constexpr const char *moduleName = "libdag";
using namespace xcalar::internal;

static constexpr char zeroPage[PageSize] = {0};

Status
Dag::init(void *constructorArgs)
{
    uint64_t numSlot;
    DagTypes::GraphType graphType;
    CreateNewDagParams *createNewDagParams;
    int primeNumSlot;

    createNewDagParams = (CreateNewDagParams *) constructorArgs;
    numSlot = createNewDagParams->numSlot;
    assert(numSlot > 0);
    graphType = createNewDagParams->graphType;
    primeNumSlot = numSlot >= 2 ? (int) mathGetPrime(numSlot) : numSlot;

    hdr_.graphType = graphType;
    hdr_.numSlot = primeNumSlot;
    hdr_.firstNode = hdr_.lastNode = DagTypes::InvalidDagNodeId;

    dagId_ = createNewDagParams->dagId;
    hdr_.numNodes = 0;
    ownerNode_ =
        (Config::get()->getMyNodeId() == createNewDagParams->initNodeId);

    memset(&udfContainer_, 0, sizeof(udfContainer_));
    memset(&sessionContainer_, 0, sizeof(sessionContainer_));
    return StatusOk;
}

Dag::~Dag()
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;
    DagTypes::NodeId dagNodeId;

    lock_.lock();
    while ((dagNodeId = hdr_.lastNode) != DagTypes::InvalidDagNodeId) {
        status = this->lookupNodeById(dagNodeId, &dagNode);
        if (status != StatusOk) {
            break;
        }
        removeDagNodeLocal(dagNode);
    }
    lock_.unlock();

    assert(hdr_.numNodes == 0);
}

Status
Dag::destroyDagNodes(DagTypes::DestroyOpts destroyOpts)
{
    // It is important to delete the dag starting from the last descendant
    // and work your way up, otherwise there'll be a leak.
    Status status = StatusOk;
    DagTypes::NodeId dagNodeId;
    DagNodeTypes::Node *dagNode;
    char dagNodeName[DagTypes::MaxNameLen + 1];

    assertStatic(sizeof(dagNodeName) ==
                 sizeof(dagNode->dagNodeHdr.apiDagNodeHdr.name));

    dagNodeId = hdr_.lastNode;
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        // XXX We should attempt to go through the rest of the DAG to free as
        // many nodes as possible and not bail on the first lookup problem
        lock_.lock();

        status = this->lookupNodeById(dagNodeId, &dagNode);

        lock_.unlock();

        if (status != StatusOk) {
            break;
        }

        // We need to keep a temporary copy of dagNode->hdr.name or we'll
        // have a use-after-free
        verifyOk(strStrlcpy(dagNodeName,
                            dagNode->dagNodeHdr.apiDagNodeHdr.name,
                            sizeof(dagNodeName)));

        if (destroyOpts == DagTypes::DestroyDeleteAndCleanNodes &&
            dagNode->dagNodeHdr.apiDagNodeHdr.state != DgDagStateDropped) {
            status =
                this->cleanAndDeleteDagNode(dagNodeName,
                                            (dagNode->dagNodeHdr.apiDagNodeHdr
                                                 .api == XcalarApiBulkLoad)
                                                ? SrcDataset
                                                : SrcTable,
                                            DagTypes::DeleteNodeCompletely,
                                            NULL,
                                            NULL);
        } else {
            assert(destroyOpts == DagTypes::DestroyDeleteNodes ||
                   dagNode->dagNodeHdr.apiDagNodeHdr.state ==
                       DgDagStateDropped);
            status = this->deleteDagNodeById(dagNodeId,
                                             DagTypes::DeleteNodeCompletely);
        }

        if (status != StatusOk) {
            // XXX FIXME we've half-deleted.. how is it safe to proceed here?
            break;
        }
        dagNodeId = hdr_.lastNode;
    }

    return status;
}

XdbId
Dag::getXdbIdFromNodeId(DagTypes::NodeId id)
{
    Status status;
    DagNodeTypes::Node *node;
    Xid xdbId = XdbIdInvalid;
    lock_.lock();
    status = lookupNodeById(id, &node);
    if (status == StatusOk) {
        xdbId = node->getXdbId();
    }
    lock_.unlock();
    return xdbId;
}

Status
Dag::getTableIdFromNodeId(DagTypes::NodeId id, TableNsMgr::TableId *tableIdOut)
{
    Status status;
    DagNodeTypes::Node *node;
    *tableIdOut = TableNsMgr::InvalidTableId;
    lock_.lock();
    status = lookupNodeById(id, &node);
    if (status == StatusOk) {
        *tableIdOut = node->getTableId();
    }
    lock_.unlock();
    return status;
}

Status
Dag::getNodeIdFromTableId(TableNsMgr::TableId tableId,
                          DagTypes::NodeId *dagNodeIdOut)
{
    Status status;
    DagNodeTypes::Node *node;
    *dagNodeIdOut = DagTypes::InvalidDagNodeId;
    lock_.lock();
    status = lookupNodeByTableId(tableId, &node);
    if (status == StatusOk) {
        *dagNodeIdOut = node->getId();
    }
    lock_.unlock();
    return status;
}

// caller should hold the lock of the dag before calling this function
Status
Dag::lookupNodeById(DagTypes::NodeId id, DagNodeTypes::Node **nodeOut)
{
    Status status = StatusOk;
    DagNodeTypes::Node *node = NULL;

    assert(id != DagTypes::InvalidDagNodeId);

    node = idHashTable_.find(id);
    if (node != NULL) {
        if (nodeOut != NULL) {
            *nodeOut = node;
        }
    } else {
        status = StatusDagNodeNotFound;
    }

    return status;
}

// caller should hold the lock of the dag before calling this function
Status
Dag::lookupNodeByTableId(TableNsMgr::TableId id, DagNodeTypes::Node **nodeOut)
{
    Status status = StatusOk;
    DagNodeTypes::Node *node = NULL;

    assert(id != TableNsMgr::InvalidTableId);

    node = tableIdHashTable_.find(id);
    if (node != NULL) {
        if (nodeOut != NULL) {
            *nodeOut = node;
        }
    } else {
        status = StatusDagNodeNotFound;
    }
    return status;
}

// caller should hold the lock of the dag before calling this function
Status
Dag::lookupNodesByXdbId(XdbId xdbId,
                        DagNodeTypes::Node **&nodesOut,
                        unsigned &numNodes)
{
    Status status = StatusOk;
    DagNodeTypes::Node *node = NULL;
    bool found = false;
    unsigned index = 0;
    numNodes = 0;
    assert(xdbId != XdbIdInvalid);

    for (unsigned ii = 0; ii < 2; ii++) {
        if (ii == 1) {
            nodesOut =
                (DagNodeTypes::Node **) memAlloc(sizeof(*nodesOut) * numNodes);
            BailIfNull(nodesOut);
        }

        node = xdbIdHashTable_.find(xdbId);
        if (node != NULL) {
            if (ii == 1) {
                nodesOut[index] = node;
                index++;
            } else {
                found = true;
                numNodes++;
            }
        }

        if (!found) {
            numNodes = 0;
            nodesOut = NULL;
            status = StatusDagNodeNotFound;
            goto CommonExit;
        }
    }

CommonExit:
    return status;
}

Status
Dag::lookupClonedNode(ClonedNodeElt **clonedNodeHashBase,
                      DagTypes::NodeId nodeIdIn,
                      DagTypes::NodeId *nodeIdOut)
{
    Status status;
    ClonedNodeElt *clonedNodeElt = NULL;
    bool found = false;
    uint64_t slotNum = nodeIdIn % ClonedNodeSlotNum;

    clonedNodeElt = clonedNodeHashBase[slotNum];
    while (clonedNodeElt != NULL) {
        if (clonedNodeElt->originalNodeId == nodeIdIn) {
            found = true;
            if (nodeIdOut != NULL) {
                *nodeIdOut = clonedNodeElt->clonedNodeId;
            }
            status = StatusOk;
            break;
        }
        clonedNodeElt = clonedNodeElt->next;
    }

    if (!found) {
        status = StatusDagNodeNotFound;
    }

    return status;
}

Status
Dag::insertClonedNode(ClonedNodeElt **clonedNodeHashBase,
                      DagTypes::NodeId originalNodeId,
                      DagTypes::NodeId clonedNodeId)
{
    ClonedNodeElt *clonedNodeElt = NULL;
    uint64_t slotNum = originalNodeId % ClonedNodeSlotNum;

    clonedNodeElt = new (std::nothrow) ClonedNodeElt;
    if (clonedNodeElt == NULL) {
        return StatusNoMem;
    }

    clonedNodeElt->originalNodeId = originalNodeId;
    clonedNodeElt->clonedNodeId = clonedNodeId;

    clonedNodeElt->next = clonedNodeHashBase[slotNum];
    clonedNodeHashBase[slotNum] = clonedNodeElt;

    return StatusOk;
}

void
Dag::deleteClonedNodeHashTable(ClonedNodeElt **clonedNodeHashBase)
{
    ClonedNodeElt *clonedNodeElt = NULL;

    for (uint64_t ii = 0; ii < ClonedNodeSlotNum; ++ii) {
        clonedNodeElt = clonedNodeHashBase[ii];
        while (clonedNodeElt != NULL) {
            clonedNodeHashBase[ii] = clonedNodeElt->next;
            delete clonedNodeElt;
            clonedNodeElt = clonedNodeHashBase[ii];
        }
    }

    memFree(clonedNodeHashBase);
}

Dag::ClonedNodeElt **
Dag::allocateCloneNodeHashBase()
{
    ClonedNodeElt **clonedNodeHashBase = NULL;
    size_t hashBaseSize;

    hashBaseSize = sizeof(*clonedNodeHashBase) * ClonedNodeSlotNum;
    clonedNodeHashBase =
        (ClonedNodeElt **) memAllocExt(hashBaseSize, moduleName);
    if (clonedNodeHashBase != NULL) {
        memset(clonedNodeHashBase, 0, hashBaseSize);
    }

    return clonedNodeHashBase;
}

Status
Dag::isValidNodeName(const char *name, size_t *nameLenOut)
{
    assert(name != NULL);
    size_t nameLen = 0;

    *nameLenOut = 0;

    while (*name != '\0') {
        if (*name == '*' || *name == ';') {
            return StatusDgInvalidDagName;
        }

        nameLen++;
        if (nameLen >= DagTypes::MaxNameLen) {
            return StatusDgDagNameTooLong;
        }

        name++;
    }

    assert(*name == '\0');
    *nameLenOut = nameLen;

    return StatusOk;
}

// caller should hold the lock of the dag before calling this function
Status
Dag::lookupNodeByName(const DagTypes::NodeName name,
                      DagNodeTypes::Node **nodeOut,
                      TableScope tScope,
                      bool needsLock)
{
    Status status = StatusOk;
    DagNodeTypes::Node *node = NULL;
    TableNsMgr *tnsMgr = TableNsMgr::get();
    char *tableName = NULL;

    if (needsLock) {
        lock_.lock();
    }

    if (tScope == TableScope::FullyQualOnly) {
        if (!tnsMgr->isValidFullyQualTableName(name)) {
            status = StatusInvalFullyQualTabName;
            goto CommonExit;
        }
        status = tnsMgr->getNameFromFqTableName(name, tableName);
        BailIfFailed(status);
        node = nameHashTable_.find(tableName);
    } else if (tScope == TableScope::LocalOnly) {
        node = nameHashTable_.find(name);
    } else {
        assert(tScope == TableScope::FullyQualOrLocal);
        if (tnsMgr->isValidFullyQualTableName(name)) {
            status = tnsMgr->getNameFromFqTableName(name, tableName);
            BailIfFailed(status);
            node = nameHashTable_.find(tableName);
        } else {
            node = nameHashTable_.find(name);
        }
    }
    if (node != NULL) {
        if (nodeOut != NULL) {
            *nodeOut = node;
        }
    } else {
        status = StatusDagNodeNotFound;
    }
CommonExit:
    if (tableName) {
        delete[] tableName;
        tableName = NULL;
    }
    if (needsLock) {
        lock_.unlock();
    }
    return status;
}

Status
Dag::createNewDagNode(XcalarApis api,
                      XcalarApiInput *apiInput,
                      size_t apiInputSize,
                      XdbId xdbId,
                      TableNsMgr::TableId tableId,
                      char *name,
                      uint64_t numParent,
                      Dag **parentGraphs,
                      DagTypes::NodeId *parentNodeIds,
                      DagTypes::NodeId *dagNodeIdInOut)
{
    bool locked = false;
    CreateDagNodeInput *createDagNodeInput = NULL;
    Status status = StatusOk;
    DagTypes::NodeId dagNodeId;
    size_t length;
    int ret;
    size_t nameLenOut;
    bool globalLockAcquired = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    DagTypes::DagId *parentGraphIds = NULL;
    DagLib *dlib = DagLib::get();

    assert(name != NULL);

    status = this->isValidNodeName(name, &nameLenOut);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "%s is not a valid dagNodeName: %s",
                name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (numParent) {
        parentGraphIds = new (std::nothrow) DagTypes::DagId[numParent];
        BailIfNull(parentGraphIds);
    }

    globalLock_.lock();
    globalLockAcquired = true;

    lock_.lock();
    locked = true;
    if (nameLenOut > 0) {
        status = this->lookupNodeByName(name, NULL, TableScope::LocalOnly);
        if (status == StatusOk) {
            xSyslog(moduleName, XlogErr, "%s already exists in dag", name);
            status = StatusDgDagAlreadyExists;
            goto CommonExit;
        }
        status = StatusOk;
    }

    if (*dagNodeIdInOut == DagTypes::InvalidDagNodeId) {
        dagNodeId = XidMgr::get()->xidGetNext();
    } else {
        dagNodeId = *dagNodeIdInOut;
    }

    for (uint64_t ii = 0; ii < numParent; ++ii) {
        if (parentNodeIds[ii] == DagTypes::InvalidDagNodeId) {
            status = StatusDgParentNodeNotExist;
            goto CommonExit;
        }

        parentGraphIds[ii] = parentGraphs[ii]->getId();
        assert(dagNodeId > parentNodeIds[ii]);

        if (parentGraphIds[ii] == this->getId()) {
            // Lookup Parent DagNodeId in local Dag.
            status = this->lookupNodeById(parentNodeIds[ii], NULL);
        } else {
            // Lookup Parent DagNodeId across all Dags.
            status = dlib->lookupNodeById(parentNodeIds[ii]);
        }
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    if (nameLenOut == 0) {
        ret = snprintf(name,
                       DagTypes::MaxNameLen + 1,
                       XcalarTempDagNodePrefix "%lu",
                       dagNodeId);
        if (ret >= DagTypes::MaxNameLen + 1) {
            status = StatusOverflow;
            goto CommonExit;
        }
    }

    lock_.unlock();
    locked = false;

    length = this->sizeOfCreateDagNodeInput(numParent, apiInputSize);
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + length);
    BailIfNull(gPayload);

    createDagNodeInput = (CreateDagNodeInput *) gPayload->buf;
    createDagNodeInput->dagNodeId = dagNodeId;
    createDagNodeInput->dagId = this->getId();
    createDagNodeInput->api = api;
    createDagNodeInput->xdbId = xdbId;
    createDagNodeInput->tableId = tableId;

    verifyOk(strStrlcpy(createDagNodeInput->name,
                        name,
                        sizeof(createDagNodeInput->name)));

    this->serializeCreateDagNodeInput(createDagNodeInput,
                                      length,
                                      numParent,
                                      parentNodeIds,
                                      parentGraphIds,
                                      apiInput,
                                      apiInputSize);

    if (hdr_.graphType == DagTypes::QueryGraph) {
        status = DagNodeGvm::get()->localHandler(DgUpdateCreateNode,
                                                 createDagNodeInput,
                                                 NULL);
    } else {
        gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                       DgUpdateCreateNode,
                       length);

        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
    }
    if (status == StatusOk) {
        *dagNodeIdInOut = createDagNodeInput->dagNodeId;
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (globalLockAcquired) {
        globalLock_.unlock();
        globalLockAcquired = false;
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (parentGraphIds != NULL) {
        delete[] parentGraphIds;
        parentGraphIds = NULL;
    }

    return status;
}

Status
Dag::dropAndChangeState(DagTypes::NodeId dagNodeId, DgDagState state)
{
    Status status = StatusOk;
    status = deleteDagNodeById(dagNodeId, DagTypes::RemoveNodeFromNamespace);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed dropAndChangeState dagNodeId %lu state %u: %s",
                    dagNodeId,
                    state,
                    strGetFromStatus(status));

    status = changeDagNodeState(dagNodeId, state);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed dropAndChangeState dagNodeId %lu state %u: %s",
                    dagNodeId,
                    state,
                    strGetFromStatus(status));
CommonExit:
    return status;
}

// This function should only be called from persistbale DAG
Status
Dag::changeDagNodeState(DagTypes::NodeId dagNodeId, DgDagState state)
{
    Status status = StatusUnknown;

    assert(this->ownerNode_);

    lock_.lock();

    status = this->changeDagStateLocalNoLock(dagNodeId, state);

    lock_.unlock();

    return status;
}

Status
Dag::sparseMemCpy(void *dstIn, const void *srcIn, size_t size, size_t blkSize)
{
    DagLib *dagLib = DagLib::get();
    StatsLib::statAtomicAdd64(dagLib->dagNodeBytesVirt_, size);
    if (XcalarConfig::get()->enableSparseDag_) {
        size_t bytesPhys = 0;
        Status status = ::sparseMemCpy(dstIn, srcIn, size, blkSize, &bytesPhys);
        if (status != StatusOk) {
            return status;
        }
        StatsLib::statAtomicAdd64(dagLib->dagNodeBytesPhys_, bytesPhys);
    } else {
        memcpy(dstIn, srcIn, size);
    }

    return StatusOk;
}

XcalarApiInput *
Dag::dagApiAlloc(const size_t size)
{
    void *ptr = NULL;
    if (XcalarConfig::get()->enableSparseDag_) {
        if (size == 0) {
            // Emulate malloc(0) behavior
            return (XcalarApiInput *) ptr;
        }

        ptr = mmap(NULL,
                   size,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS,
                   -1,
                   0);
        if (ptr == MAP_FAILED) {
            ptr = NULL;
        }
    } else {
        ptr = (XcalarApiInput *) memAlloc(size);
    }

    if (ptr) {
        StatsLib::statAtomicIncr64(DagLib::get()->numDagNodesAlloced_);
    }

    return (XcalarApiInput *) ptr;
}

void
Dag::dagApiFree(DagNodeTypes::Node *dagNode)
{
    if (XcalarConfig::get()->enableSparseDag_) {
        if (dagNode->dagNodeHdr.apiDagNodeHdr.inputSize == 0 ||
            dagNode->dagNodeHdr.apiInput == MAP_FAILED ||
            dagNode->dagNodeHdr.apiInput == NULL) {
            // Emulate free(NULL) behavior
            return;
        }

        int ret = munmap(dagNode->dagNodeHdr.apiInput,
                         dagNode->dagNodeHdr.apiDagNodeHdr.inputSize);
        if (ret) {
            // In theory munmap can fail, almost always due to an invalid
            // address.  In malloc/free land this would be heap corruption, so
            // we treat this just like free and keep going (and log an error
            // since we can).
            xSyslog(moduleName,
                    XlogErr,
                    "munmap failed with %s",
                    strGetFromStatus(sysErrnoToStatus(errno)));
            assert(false && "Failed to unmap dag node memory");
        }
    } else {
        memFree(dagNode->dagNodeHdr.apiInput);
    }

    dagNode->dagNodeHdr.apiInput = NULL;
    StatsLib::statAtomicIncr64(DagLib::get()->numDagNodesFreed_);
}

DagNodeTypes::Node *
Dag::allocDagNode()
{
    DagNodeTypes::Node *dagNode = NULL;
    void *ptr = mmap(NULL,
                     sizeof(DagNodeTypes::Node),
                     PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS,
                     -1,
                     0);
    if (ptr != MAP_FAILED) {
        dagNode = new (ptr) DagNodeTypes::Node;
    }
    return dagNode;
}

void
Dag::freeDagNode(DagNodeTypes::Node *dagNode)
{
    dagNode->DagNodeTypes::Node::~Node();
    int ret = munmap(dagNode, sizeof(DagNodeTypes::Node));
    if (ret) {
        // In theory munmap can fail, almost always due to an invalid
        // address.  In malloc/free land this would be heap corruption, so
        // we treat this just like free and keep going (and log an error
        // since we can).
        xSyslog(moduleName,
                XlogErr,
                "munmap failed with %s",
                strGetFromStatus(sysErrnoToStatus(errno)));
        assert(false && "Failed to unmap dag node memory");
    }
}

// No parents info attached on the new node
// No dagNode ID or timestamp assigned
Status
Dag::createSingleNode(XcalarApis api,
                      const XcalarApiInput *apiInput,
                      size_t apiInputSize,
                      XdbId xdbId,
                      TableNsMgr::TableId tableId,
                      const char *name,
                      DagTypes::NodeId dagNodeId,
                      DagNodeTypes::Node **dagNodeOut)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode = allocDagNode();
    if (dagNode == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    if (apiInputSize > 0) {
        dagNode->dagNodeHdr.apiDagNodeHdr.inputSize = apiInputSize;
        dagNode->dagNodeHdr.apiInput = dagApiAlloc(apiInputSize);
        BailIfNull(dagNode->dagNodeHdr.apiInput);
        status =
            sparseMemCpy(dagNode->dagNodeHdr.apiInput, apiInput, apiInputSize);
        BailIfFailed(status);
        switch (api) {
        case XcalarApiExecuteRetina: {
            // Adjust the pointer to the actual retina.
            XcalarApiExecuteRetinaInput *executeRetinaInput =
                &dagNode->dagNodeHdr.apiInput->executeRetinaInput;

            executeRetinaInput->exportRetinaBuf =
                (char *) ((uintptr_t) executeRetinaInput->parameters +
                          (executeRetinaInput->numParameters *
                           sizeof(*executeRetinaInput->parameters)));
            break;
        }
        case XcalarApiMap:
            xcalarApiDeserializeMapInput(
                &dagNode->dagNodeHdr.apiInput->mapInput);
            break;
        case XcalarApiGroupBy:
            xcalarApiDeserializeGroupByInput(
                &dagNode->dagNodeHdr.apiInput->groupByInput);
            break;
        case XcalarApiUnion:
            xcalarApiDeserializeUnionInput(
                &dagNode->dagNodeHdr.apiInput->unionInput);
            break;
        default:
            break;
        }
    } else {
        dagNode->dagNodeHdr.apiInput = NULL;
    }

    dagNode->dagId = this->getId();
    dagNode->xdbId = xdbId;
    dagNode->tableId = tableId;
    dagNode->annotations = NULL;
    dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId = dagNodeId;
    dagNode->numParent = 0;
    dagNode->parentsList = NULL;
    dagNode->numChild = 0;
    dagNode->childrenList = NULL;
    dagNode->scalarResult = NULL;
    dagNode->context = NULL;
    dagNode->dagNodeHdr.dagNodeOrder.prev =
        dagNode->dagNodeHdr.dagNodeOrder.next = DagTypes::InvalidDagNodeId;
    dagNode->dagNodeHdr.apiDagNodeHdr.api = api;
    refInit(&dagNode->refCount, destroyDagNodeFromRef);
    dagNode->dagNodeHdr.apiDagNodeHdr.tag[0] = '\0';
    dagNode->dagNodeHdr.apiDagNodeHdr.comment[0] = '\0';
    dagNode->dagNodeHdr.apiDagNodeHdr.pinned = false;
    dagNode->log[0] = '\0';
    dagNode->status = StatusCodeDgDagNodeNotReady;

    assert(name[0] != '\0');
    status = strStrlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        name,
                        sizeof(dagNode->dagNodeHdr.apiDagNodeHdr.name));
    BailIfFailed(status);

    *dagNodeOut = dagNode;
CommonExit:
    if (status != StatusOk) {
        if (dagNode != NULL) {
            if (dagNode->dagNodeHdr.apiInput != NULL) {
                dagApiFree(dagNode);
                dagNode->dagNodeHdr.apiInput = NULL;
            }
            freeDagNode(dagNode);
            dagNode = NULL;
        }
    }

    return status;
}

// caller should hold the lock of the dag before calling this function
Status
Dag::addChildNode(DagNodeTypes::Node *parentNode, DagNodeTypes::Node *childNode)
{
    DagNodeTypes::NodeIdListElt *childElt;

    childElt = new (std::nothrow) DagNodeTypes::NodeIdListElt;
    if (childElt == NULL) {
        return StatusNoMem;
    }

    childElt->nodeId = childNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;
    childElt->next = parentNode->childrenList;
    parentNode->childrenList = childElt;

    parentNode->numChild++;
    return StatusOk;
}

void
Dag::removeNodeFromIdList(DagNodeTypes::NodeIdListElt **listEltHead,
                          DagNodeTypes::Node *toRemove)
{
    DagNodeTypes::NodeIdListElt *eltToFree = NULL;

    if ((*listEltHead)->nodeId ==
        toRemove->dagNodeHdr.apiDagNodeHdr.dagNodeId) {
        eltToFree = *listEltHead;
        *listEltHead = (*listEltHead)->next;
    } else {
        DagNodeTypes::NodeIdListElt *prev = *listEltHead;
        DagNodeTypes::NodeIdListElt *cur = prev->next;
        bool found = false;

        do {
            if (cur->nodeId == toRemove->dagNodeHdr.apiDagNodeHdr.dagNodeId) {
                found = true;
                break;
            }
            prev = cur;
            cur = cur->next;
        } while (cur != NULL);

        assert(found);
        eltToFree = cur;
        prev->next = cur->next;
    }

    delete eltToFree;
}

void
Dag::removeNodeFromTableIdTable(DagNodeTypes::Node *node)
{
    tableIdHashTable_.remove(node->getTableId());
}

void
Dag::removeNodeFromIdTable(DagNodeTypes::Node *node)
{
    verify(idHashTable_.remove(node->getId()) != NULL);
}

void
Dag::removeNodeFromGlobalIdTable(DagNodeTypes::Node *node)
{
    verify(DagLib::get()->removeNodeFromGlobalIdTable(node) != NULL);
}

void
Dag::removeNodeFromXdbIdTable(DagNodeTypes::Node *node)
{
    // Xdb IDs need not be unique
    xdbIdHashTable_.remove(node->getXdbId());
}

// the Dag lock must be held before calling the function
void
Dag::removeDagNode(DagNodeTypes::Node *dagNode)
{
    Status status;
    DagNodeTypes::Node *parentDagNode = NULL;

    assert(lock_.tryLock() == false);

    // Remove the deleting node from it's parents' children list
    DagNodeTypes::NodeIdListElt *parentElt;
    parentElt = dagNode->parentsList;
    while (parentElt != NULL) {
        status = this->lookupNodeById(parentElt->nodeId, &parentDagNode);

        assert(status == StatusOk);
        this->removeAndCleanChildNode(parentDagNode, dagNode);
        parentElt = parentElt->next;
    }

    // Remove the deleting node from it's children's parent list

    DagNodeTypes::Node *childDagNode = NULL;
    DagNodeTypes::NodeIdListElt *childElt;
    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        status = this->lookupNodeById(childElt->nodeId, &childDagNode);

        assert(status == StatusOk);

        // removeParentNode
        this->removeNodeFromIdList(&childDagNode->parentsList, dagNode);
        childDagNode->numParent--;

        childElt = childElt->next;
    }

    // Fix dag choronological list
    DagNodeTypes::Node *node = NULL;
    if (hdr_.firstNode == dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId) {
        hdr_.firstNode = dagNode->dagNodeHdr.dagNodeOrder.next;
        if (dagNode->dagNodeHdr.dagNodeOrder.next !=
            DagTypes::InvalidDagNodeId) {
            status = this->lookupNodeById(dagNode->dagNodeHdr.dagNodeOrder.next,
                                          &node);
            assert(status == StatusOk);
            node->dagNodeHdr.dagNodeOrder.prev = DagTypes::InvalidDagNodeId;
        } else {
            // There is only one node
            assert(hdr_.lastNode ==
                   dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId);
            hdr_.lastNode = dagNode->dagNodeHdr.dagNodeOrder.prev;
        }
    } else if (hdr_.lastNode == dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId) {
        hdr_.lastNode = dagNode->dagNodeHdr.dagNodeOrder.prev;
        assert(dagNode->dagNodeHdr.dagNodeOrder.prev !=
               DagTypes::InvalidDagNodeId);
        status =
            this->lookupNodeById(dagNode->dagNodeHdr.dagNodeOrder.prev, &node);
        assert(status == StatusOk);
        node->dagNodeHdr.dagNodeOrder.next = DagTypes::InvalidDagNodeId;
    } else {
        status =
            this->lookupNodeById(dagNode->dagNodeHdr.dagNodeOrder.prev, &node);
        assert(status == StatusOk);

        node->dagNodeHdr.dagNodeOrder.next =
            dagNode->dagNodeHdr.dagNodeOrder.next;

        status =
            this->lookupNodeById(dagNode->dagNodeHdr.dagNodeOrder.next, &node);
        assert(status == StatusOk);

        node->dagNodeHdr.dagNodeOrder.prev =
            dagNode->dagNodeHdr.dagNodeOrder.prev;
    }

    this->removeNodeFromGlobalIdTable(dagNode);
    this->removeNodeFromIdTable(dagNode);
    this->removeNodeFromXdbIdTable(dagNode);
    this->removeNodeFromTableIdTable(dagNode);
    hdr_.numNodes--;
}

void
Dag::removeAndDeleteDagNode(DagNodeTypes::Node *dagNode)
{
    assert(lock_.tryLock() == false);
    this->removeDagNode(dagNode);
    if (this->ownerNode_) {
        assert(refRead(&dagNode->refCount) == 1);
        refPut(&dagNode->refCount);
    } else {
        DagLib::get()->destroyDagNode(dagNode);
    }
}

// caller should hold the lock of the dag before calling this function
void
Dag::removeAndCleanChildNode(DagNodeTypes::Node *parentNode,
                             DagNodeTypes::Node *childNode)
{
    // removeChildNode
    removeNodeFromIdList(&parentNode->childrenList, childNode);
    parentNode->numChild--;

    // Delete Dropped Parent Node if there is no child node existing after
    // deletion
    if ((parentNode->numChild == 0) &&
        (parentNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped)) {
        assert(parentNode->childrenList == NULL);

        if (hdr_.graphType == QueryGraph) {
            this->removeNodeFromNameTable(parentNode);
        }
        assert(lookupNodeByName(parentNode->dagNodeHdr.apiDagNodeHdr.name,
                                NULL,
                                TableScope::LocalOnly) ==
               StatusDagNodeNotFound);
        this->removeAndDeleteDagNode(parentNode);
    }
}

void
Dag::insertNodeToXdbIdTable(DagNodeTypes::Node *node)
{
    // XdbIds need not be unique
    xdbIdHashTable_.insert(node);
}

void
Dag::insertNodeToTableIdTable(DagNodeTypes::Node *node)
{
    verifyOk(tableIdHashTable_.insert(node));
}

void
Dag::insertNodeToGlobalIdTable(DagNodeTypes::Node *node)
{
    verifyOk(DagLib::get()->insertNodeToGlobalIdTable(node));
}

void
Dag::insertNodeToIdTable(DagNodeTypes::Node *node)
{
    verifyOk(idHashTable_.insert(node));
}

void
Dag::insertNodeToNameTable(DagNodeTypes::Node *node)
{
    verifyOk(nameHashTable_.insert(node));
}

// caller should hold the lock of the dag before calling this function
//
// The node will be added to nameHashTable only if the node are not dropped
void
Dag::addNodeToDag(DagNodeTypes::Node *node)
{
    this->insertNodeToGlobalIdTable(node);
    this->insertNodeToIdTable(node);
    if (node->dagNodeHdr.apiDagNodeHdr.state != DgDagStateDropped) {
        this->insertNodeToNameTable(node);
    }
    if (node->tableId != TableIdInvalid) {
        this->insertNodeToTableIdTable(node);
    }

    if (node->xdbId != XdbIdInvalid) {
        this->insertNodeToXdbIdTable(node);
    }

    if (hdr_.firstNode == DagTypes::InvalidDagNodeId) {
        assert(hdr_.numNodes == 0);
        assert(hdr_.lastNode == DagTypes::InvalidDagNodeId);

        hdr_.firstNode = node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        hdr_.lastNode = node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        node->dagNodeHdr.dagNodeOrder.prev =
            node->dagNodeHdr.dagNodeOrder.next = DagTypes::InvalidDagNodeId;
    } else {
        DagNodeTypes::Node *dagNode = NULL;
        Status status;

        status = this->lookupNodeById(hdr_.lastNode, &dagNode);
        assert(status == StatusOk);

        assert(dagNode->dagNodeHdr.dagNodeOrder.next ==
               DagTypes::InvalidDagNodeId);
        dagNode->dagNodeHdr.dagNodeOrder.next =
            node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        node->dagNodeHdr.dagNodeOrder.prev = hdr_.lastNode;
        node->dagNodeHdr.dagNodeOrder.next = DagTypes::InvalidDagNodeId;
        hdr_.lastNode = node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
    }

    hdr_.numNodes++;
}

Status
Dag::setContext(DagTypes::NodeId dagNodeId, void *context)
{
    DagNodeTypes::Node *dagNode = NULL;
    bool lockAcquired = false;
    Status status = StatusUnknown;

    lock_.lock();
    lockAcquired = true;

    status = lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dagNode->context = context;

    status = StatusOk;
CommonExit:
    if (lockAcquired) {
        lock_.unlock();
        lockAcquired = false;
    }

    return status;
}

Status
Dag::getContext(DagTypes::NodeId dagNodeId, void **contextOut)
{
    DagNodeTypes::Node *dagNode = NULL;
    bool lockAcquired = false;
    Status status = StatusUnknown;
    void *context = NULL;

    lock_.lock();
    lockAcquired = true;

    status = lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    context = dagNode->context;
    status = StatusOk;
CommonExit:
    if (lockAcquired) {
        lock_.unlock();
        lockAcquired = false;
    }

    *contextOut = context;
    return status;
}

Status
Dag::createNewDagNodeCommon(DagTypes::NodeId dagNodeId,
                            const char *name,
                            Xid xdbId,
                            TableNsMgr::TableId tableId,
                            XcalarApis api,
                            size_t apiInputSize,
                            XcalarApiInput *apiInput,
                            uint64_t numParent,
                            DagTypes::NodeId *parentNodeIds,
                            DagTypes::DagId *parentGraphIds,
                            DgDagState state,
                            const OpDetails *opDetails,
                            DagNodeTypes::Node **newNodeOut)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *newNode = NULL;
    bool locked = false;
    bool nodeAddedToDag = false;
    uint64_t ii;
    DagNodeTypes::NodeIdListElt *parentNodeElt = NULL;
    DagNodeTypes::Node **parentNodes = NULL;
    DagLib *dagLib = DagLib::get();
    DagTypes::NodeId *dgLocalParentNodeIds = NULL;
    uint64_t dgLocalNumParent = 0;

    assert(newNodeOut != NULL);
    assert(name[0] != '\0');

    *newNodeOut = NULL;

    lock_.lock();
    locked = true;

    status = this->lookupNodeByName(name, NULL, TableScope::LocalOnly);

    if (status == StatusOk) {
        xSyslog(moduleName, XlogErr, "%s already exists in dag", name);

        status = StatusDgDagAlreadyExists;
        goto CommonExit;
    }
    assert(status == StatusDagNodeNotFound);

    status = this->createSingleNode(api,
                                    apiInput,
                                    apiInputSize,
                                    xdbId,
                                    tableId,
                                    name,
                                    dagNodeId,
                                    &newNode);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "createSingleNode failed with status: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (opDetails != NULL) {
        verifyOk(::sparseMemCpy(&newNode->dagNodeHdr.opDetails,
                                opDetails,
                                sizeof(OpDetails),
                                PageSize,
                                NULL));
    }

    // opDetails have per-node counts in count arrays, and numNodes is
    // needed to iterate through these arrays. Moved from updateOpDetailsLocal
    newNode->numNodes = Config::get()->getActiveNodes();

    if (hdr_.graphType == DagTypes::WorkspaceGraph) {
        status = Operators::get()->operatorsInitOpStatus(&newNode->opStatus,
                                                         api,
                                                         apiInput);
        // we don't care whether or not opStatus was initiated properly
        (void) status;
    }

    newNode->dagNodeHdr.apiDagNodeHdr.state = state;

    // Add parent nodes onto the newly created node, provided parent nodes
    // belong to the local DAG.
    if (numParent > 0) {
        dgLocalParentNodeIds = new (std::nothrow) DagTypes::NodeId[numParent];
        BailIfNull(dgLocalParentNodeIds);

        uint64_t jj = 0;
        for (ii = 0; ii < numParent; ++ii) {
            if (this->getId() == parentGraphIds[ii]) {
                dgLocalParentNodeIds[jj++] = parentNodeIds[ii];
            }
        }
        dgLocalNumParent = jj;
    }

    if (dgLocalNumParent) {
        parentNodes = new (std::nothrow) DagNodeTypes::Node *[dgLocalNumParent];
        if (parentNodes == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate parentNodes "
                    "(numParent: %lu)",
                    dgLocalNumParent);
            status = StatusNoMem;
            goto CommonExit;
        }

        // We need to add to tail of parentsList in order to
        // maintain FIFO ordering. FIFO ordering is important
        // because callers might assume the first table inserted
        // is the leftTable in a join, and the second table, the rightTable
        // and so we need to return parents in the same order
        // they were inserted
        DagNodeTypes::NodeIdListElt *parentNodeTail = NULL,
                                    *parentNodeCurr = NULL;
        parentNodeCurr = newNode->parentsList;
        while (parentNodeCurr != NULL) {
            parentNodeTail = parentNodeCurr;
            parentNodeCurr = parentNodeCurr->next;
        }

        for (ii = 0; ii < dgLocalNumParent; ++ii) {
            assert(dgLocalParentNodeIds[ii] != DagTypes::InvalidDagNodeId);
            status = this->lookupNodeById(dgLocalParentNodeIds[ii],
                                          &parentNodes[ii]);

            if (status != StatusOk) {
                status = StatusDgParentNodeNotExist;
                break;
            }

            assert(parentNodeElt == NULL);
            parentNodeElt = new (std::nothrow) DagNodeTypes::NodeIdListElt;
            if (parentNodeElt == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Could not allocate parentNodeElt");
                status = StatusNoMem;
                break;
            }
            parentNodeElt->nodeId =
                parentNodes[ii]->dagNodeHdr.apiDagNodeHdr.dagNodeId;
            parentNodeElt->next = NULL;

            if (parentNodeTail == NULL) {
                newNode->parentsList = parentNodeElt;
            } else {
                parentNodeTail->next = parentNodeElt;
            }

            parentNodeTail = parentNodeElt;
            parentNodeElt = NULL;
            newNode->numParent++;

            status = addChildNode(parentNodes[ii], newNode);

            if (status != StatusOk) {
                break;
            }
        }

        if (status != StatusOk) {
            // remove child node from parents in case of failure
            for (uint64_t jj = 0; jj < ii; ++jj) {
                this->removeAndCleanChildNode(parentNodes[jj], newNode);
            }
            goto CommonExit;
        }

        assert(newNode->numParent == dgLocalNumParent);
    }

    this->addNodeToDag(newNode);
    nodeAddedToDag = true;

    DagNodeTypes::Node *assertNode;
    assert(this->lookupNodeById(dagNodeId, &assertNode) == StatusOk);

    assert(newNodeOut != NULL);
    *newNodeOut = newNode;

    status = StatusOk;

CommonExit:
    if (parentNodes != NULL) {
        delete[] parentNodes;
        parentNodes = NULL;
    }

    if (status != StatusOk) {
        assert(!nodeAddedToDag);

        if (newNode != NULL) {
            dagLib->destroyDagNode(newNode);
        }
    }

    if (parentNodeElt != NULL) {
        assert(status != StatusOk);
        delete parentNodeElt;
        parentNodeElt = NULL;
    }

    if (dgLocalParentNodeIds != NULL) {
        delete[] dgLocalParentNodeIds;
        dgLocalParentNodeIds = NULL;
    }

    if (locked) {
        lock_.unlock();
        locked = false;
    }
    return status;
}

Status
Dag::copyDagNodeToNewDagNode(const DagNodeTypes::DagNodeHdr *dagNodeHdr,
                             DagTypes::NodeId dagNodeId,
                             uint64_t numParent,
                             DagTypes::NodeId *parentNodeIds,
                             Xid xdbId,
                             TableNsMgr::TableId tableId,
                             DgDagState state)
{
    Status status;
    DagNodeTypes::Node *newNode = NULL;
    DagTypes::DagId *parentGraphIds = NULL;

    if (numParent) {
        parentGraphIds = new (std::nothrow) DagTypes::DagId[numParent];
        BailIfNull(parentGraphIds);
        for (uint64_t ii = 0; ii < numParent; ii++) {
            parentGraphIds[ii] = this->getId();
        }
    }

    status = createNewDagNodeCommon(dagNodeId,
                                    dagNodeHdr->apiDagNodeHdr.name,
                                    xdbId,
                                    tableId,
                                    dagNodeHdr->apiDagNodeHdr.api,
                                    dagNodeHdr->apiDagNodeHdr.inputSize,
                                    dagNodeHdr->apiInput,
                                    numParent,
                                    parentNodeIds,
                                    parentGraphIds,
                                    state,
                                    &dagNodeHdr->opDetails,
                                    &newNode);
    if (status == StatusOk) {
        assert(newNode != NULL);
        verifyOk(strStrlcpy(newNode->dagNodeHdr.apiDagNodeHdr.tag,
                            dagNodeHdr->apiDagNodeHdr.tag,
                            XcalarApiMaxDagNodeTagLen));

        verifyOk(strStrlcpy(newNode->dagNodeHdr.apiDagNodeHdr.comment,
                            dagNodeHdr->apiDagNodeHdr.comment,
                            XcalarApiMaxDagNodeCommentLen));
        newNode->dagNodeHdr.apiDagNodeHdr.pinned =
            dagNodeHdr->apiDagNodeHdr.pinned;
    } else {
        xSyslog(moduleName,
                XlogErr,
                "Dag::copyDagNodeToNewDagNode failed! %s",
                strGetFromStatus(status));
    }

CommonExit:
    if (parentGraphIds) {
        delete[] parentGraphIds;
        parentGraphIds = NULL;
    }

    return status;
}

Status  // static
Dag::createNewDagNodeLocal(CreateDagNodeInput *createDagNodeInput)
{
    Status status;
    DagNodeTypes::Node *newNode = NULL;

    status = createNewDagNodeCommon(createDagNodeInput->dagNodeId,
                                    createDagNodeInput->name,
                                    createDagNodeInput->xdbId,
                                    createDagNodeInput->tableId,
                                    createDagNodeInput->api,
                                    createDagNodeInput->apiInputSize,
                                    createDagNodeInput->input,
                                    createDagNodeInput->numParent,
                                    createDagNodeInput->parentNodeIds,
                                    createDagNodeInput->parentGraphIds,
                                    DgDagStateCreated,
                                    NULL,  // opDetails
                                    &newNode);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Dag::createNewDagNodeLocal failed! %s",
                strGetFromStatus(status));
    }
    return status;
}

void
Dag::serializeCreateDagNodeInput(CreateDagNodeInput *createDagNodeInput,
                                 size_t bufSize,
                                 uint64_t numParents,
                                 DagTypes::NodeId *parentNodeIds,
                                 DagTypes::DagId *parentGraphIds,
                                 XcalarApiInput *apiInput,
                                 size_t apiInputSize)
{
    uintptr_t bufCursor;
    uint64_t ii;

    size_t bytesUsed, parentNodeIdsSize, parentGraphIdsSize;

    assert(createDagNodeInput != NULL);

    bytesUsed = sizeof(*createDagNodeInput);
    assert(bufSize >= bytesUsed);

    bufCursor = (uintptr_t) createDagNodeInput->buf;
    assert(bufCursor - (uintptr_t) createDagNodeInput ==
           sizeof(*createDagNodeInput));

    createDagNodeInput->numParent = numParents;

    parentNodeIdsSize = numParents * sizeof(parentNodeIds[0]);
    bytesUsed += parentNodeIdsSize;
    assert(bytesUsed <= bufSize);
    createDagNodeInput->parentNodeIds = (DagTypes::NodeId *) bufCursor;
    for (ii = 0; ii < numParents; ++ii) {
        createDagNodeInput->parentNodeIds[ii] = parentNodeIds[ii];
    }
    createDagNodeInput->parentNodeIds = NULL;  // To be fixed up
    bufCursor += parentNodeIdsSize;

    parentGraphIdsSize = numParents * sizeof(parentGraphIds[0]);
    bytesUsed += parentGraphIdsSize;
    assert(bytesUsed <= bufSize);
    createDagNodeInput->parentGraphIds = (DagTypes::DagId *) bufCursor;
    for (ii = 0; ii < numParents; ++ii) {
        createDagNodeInput->parentGraphIds[ii] = parentGraphIds[ii];
    }
    createDagNodeInput->parentGraphIds = NULL;  // To be fixed up
    bufCursor += parentGraphIdsSize;

    // And now we copy the apiInput
    if (apiInputSize > 0) {
        bytesUsed += apiInputSize;
        assert(bytesUsed <= bufSize);
        memcpy((void *) bufCursor, apiInput, apiInputSize);
        bufCursor += apiInputSize;
    }

    createDagNodeInput->input = NULL;  // To be fixed up
    createDagNodeInput->apiInputSize = apiInputSize;
    createDagNodeInput->bufSize = bufSize;
}

void
Dag::deserializeCreateDagNodeInput(CreateDagNodeInput *createDagNodeInput)
{
    size_t bytesSeen, parentNodeIdsSize, parentGraphIdsSize;
    uintptr_t bufCursor;

    bytesSeen = sizeof(*createDagNodeInput);
    assert(bytesSeen <= createDagNodeInput->bufSize);

    bufCursor = (uintptr_t) createDagNodeInput->buf;
    assert(bufCursor - (uintptr_t) createDagNodeInput ==
           sizeof(*createDagNodeInput));

    if (createDagNodeInput->numParent > 0) {
        parentNodeIdsSize = sizeof(createDagNodeInput->parentNodeIds[0]) *
                            createDagNodeInput->numParent;
        bytesSeen += parentNodeIdsSize;
        assert(bytesSeen <= createDagNodeInput->bufSize);
        createDagNodeInput->parentNodeIds = (DagTypes::NodeId *) bufCursor;
        bufCursor += parentNodeIdsSize;

        parentGraphIdsSize = sizeof(createDagNodeInput->parentGraphIds[0]) *
                             createDagNodeInput->numParent;
        bytesSeen += parentGraphIdsSize;
        assert(bytesSeen <= createDagNodeInput->bufSize);
        createDagNodeInput->parentGraphIds = (DagTypes::DagId *) bufCursor;
        bufCursor += parentGraphIdsSize;
    }

    if (createDagNodeInput->apiInputSize > 0) {
        bytesSeen += createDagNodeInput->apiInputSize;
        assert(bytesSeen <= createDagNodeInput->bufSize);
        assert(createDagNodeInput->input == NULL);
        createDagNodeInput->input = (XcalarApiInput *) bufCursor;
        bufCursor += createDagNodeInput->apiInputSize;
    }
}

// * Iterate the parentDag with BFS and for the white listed Dag Nodes in
// dagNodesIn, find out it's ancestry. Also exclude the ancestry of Dag
// Node Ids which match the prunedNodeIds list.
// * Returned list of Dag Nodes ancestry in resultListTailOut,
// resultListHeadOut amd numNodesOut will be in sorted order, sorted on
// the Dag Node Id.
Status
Dag::getDagAncestorTree(DagNodeTypes::Node **dagNodesIn,
                        unsigned numNodesIn,
                        unsigned numPrunedNodes,
                        DagTypes::NodeId *prunedNodeIds,
                        Dag *parentDag,
                        DagNodeListElt **resultListHeadOut,
                        DagNodeListElt **resultListTailOut,
                        uint64_t *numNodesOut)
{
    DagNodeListElt resultListHead;
    DagNodeListElt resultListTail;
    Status status = StatusOk;
    DagNodeListElt bfsListHead;
    DagNodeListElt bfsListTail;
    uint64_t numNodes = 0;

    IntHashTable<NodeId,
                 DagNodeListElt,
                 &DagNodeListElt::hook,
                 &DagNodeListElt::getId,
                 DagNodeIdBfsSlotNum,
                 hashIdentity>
        seen;

    resultListHead.next = &resultListTail;
    resultListHead.prev = NULL;
    resultListTail.prev = &resultListHead;
    resultListTail.next = NULL;

    bfsListHead.next = &bfsListTail;
    bfsListTail.prev = &bfsListHead;

    DagNodeListElt *listElt = new (std::nothrow) DagNodeListElt;
    BailIfNull(listElt);

    listElt->node = dagNodesIn[0];
    bfsListHead.next = listElt;
    listElt->next = &bfsListTail;
    bfsListTail.prev = listElt;
    listElt->prev = &bfsListHead;

    // Do BFS on dagNodesIn.
    for (unsigned ii = 1; ii < numNodesIn; ii++) {
        listElt = new (std::nothrow) DagNodeListElt;
        BailIfNull(listElt);

        listElt->node = dagNodesIn[ii];
        listElt->prev = bfsListTail.prev;
        listElt->next = &bfsListTail;
        bfsListTail.prev->next = listElt;
        bfsListTail.prev = listElt;
    }

    while (bfsListHead.next != &bfsListTail) {
        listElt = bfsListHead.next;
        bfsListHead.next = listElt->next;
        bfsListHead.next->prev = &bfsListHead;

        DagTypes::NodeId dagNodeId =
            listElt->node->dagNodeHdr.apiDagNodeHdr.dagNodeId;

        // Pruning a Dag Node means that we don't add it's parents.
        bool pruned = false;
        for (unsigned ii = 0; ii < numPrunedNodes; ii++) {
            if (dagNodeId == prunedNodeIds[ii]) {
                pruned = true;
                break;
            }
        }

        if (!pruned) {
            DagNodeTypes::NodeIdListElt *parentElt = listElt->node->parentsList;
            while (parentElt != NULL) {
                if (!seen.find(parentElt->nodeId)) {
                    DagNodeListElt *parentListElt =
                        new (std::nothrow) DagNodeListElt;
                    BailIfNull(parentListElt);

                    status = this->lookupNodeById(parentElt->nodeId,
                                                  &parentListElt->node);
                    if (status != StatusOk) {
                        // Parent must have come from another Dag.
                        if (parentDag) {
                            status =
                                parentDag->lookupNodeById(parentElt->nodeId,
                                                          &parentListElt->node);
                        }
                        BailIfFailed(status);
                    } else {
                        parentListElt->prev = bfsListTail.prev;
                        parentListElt->next = &bfsListTail;
                        bfsListTail.prev->next = parentListElt;
                        bfsListTail.prev = parentListElt;

                        seen.insert(parentListElt);
                    }
                }
                parentElt = parentElt->next;
            }
        }

        // Put node into result list in ascending sorted order on Dag Node Id.
        DagNodeListElt *tmpDagNode;
        bool found = false;
        tmpDagNode = resultListTail.prev;
        if (resultListTail.prev == &resultListHead) {
            assert(numNodes == 0);
            listElt->prev = resultListTail.prev;
            listElt->next = &resultListTail;
            resultListTail.prev->next = listElt;
            resultListTail.prev = listElt;
        } else {
            while (tmpDagNode != &resultListHead) {
                DagTypes::NodeId tmpDagNodeId =
                    tmpDagNode->node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
                if (tmpDagNodeId == dagNodeId) {
                    found = true;
                    break;
                }
                if (tmpDagNodeId > dagNodeId) {
                    tmpDagNode->next->prev = listElt;
                    listElt->next = tmpDagNode->next;
                    listElt->prev = tmpDagNode;
                    tmpDagNode->next = listElt;
                    break;
                }
                tmpDagNode = tmpDagNode->prev;
            }
            if (!found && tmpDagNode == &resultListHead) {
                listElt->prev = &resultListHead;
                listElt->next = resultListHead.next;
                resultListHead.next->prev = listElt;
                resultListHead.next = listElt;
            }
        }
        if (!found) {
            seen.insert(listElt);
            numNodes++;
        }
    }

CommonExit:
    if (status != StatusOk) {
        // Free elements on BFS list.
        while (bfsListHead.next != &bfsListTail) {
            listElt = bfsListHead.next;
            bfsListHead.next = listElt->next;
            delete listElt;
        }

        // Free items that are on the result list.
        while (resultListHead.next != &resultListTail) {
            listElt = resultListHead.next;
            resultListHead.next = listElt->next;
            delete listElt;
        }
        numNodes = 0;
    } else {
        assert(bfsListHead.next == &bfsListTail &&
               "BFS list should be empty, since everything got transferred"
               "  to resultList");
    }

    *numNodesOut = numNodes;

    if (numNodes > 0) {
        *resultListHeadOut = resultListHead.next;
        assert((*resultListHeadOut)->prev == &resultListHead);
        (*resultListHeadOut)->prev = NULL;

        *resultListTailOut = resultListTail.prev;
        assert((*resultListTailOut)->next == &resultListTail);
        (*resultListTailOut)->next = NULL;
    } else {
        *resultListHeadOut = *resultListTailOut = NULL;
    }

    return status;
}

// Caller need to clean up the whole Dag if any node copying fails.
// This function create the chronological order of the DAG based on the order
// of calling this function.
Status
Dag::copyDagToNewDag(ClonedNodeElt **clonedNodeHashBase,
                     DagNodeTypes::Node **dagNodesIn,
                     unsigned numNodesIn,
                     Dag *dagOut,
                     unsigned numPrunedNodes,
                     DagTypes::NodeId *prunedNodeIds,
                     CloneFlags flags,
                     uint64_t *numNodesCreated)
{
    Status status = StatusOk;
    uint64_t numNodes = 0;
    DagNodeListElt *listEltToFree = NULL;
    DagNodeListElt *listHead = NULL;
    DagNodeListElt *listTail = NULL;
    DagNodeListElt *listElt = NULL;
    DagTypes::NodeId *parentNodeIds = NULL;

    status = this->getDagAncestorTree(dagNodesIn,
                                      numNodesIn,
                                      numPrunedNodes,
                                      prunedNodeIds,
                                      dagOut,
                                      &listHead,
                                      &listTail,
                                      &numNodes);
    BailIfFailed(status);

    listElt = listTail;

    parentNodeIds =
        (DagTypes::NodeId *) memAllocExt(hdr_.numNodes * sizeof(*parentNodeIds),
                                         moduleName);
    BailIfNullWith(parentNodeIds, StatusNoMem);

    while (listElt != NULL) {
        DagTypes::NodeId newDagNodeId = XidMgr::get()->xidGetNext();
        DagTypes::NodeId oldDagNodeId =
            listElt->node->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        DgDagState state = listElt->node->dagNodeHdr.apiDagNodeHdr.state;

#ifdef DEBUG
        // Since the list of Dag Nodes returned by getDagAncestorTree is
        // unique, we should not find the Dag Node in clonedNodeHashBase.
        status = this->lookupClonedNode(clonedNodeHashBase, oldDagNodeId, NULL);
        assert(status == StatusDagNodeNotFound);
        status = StatusOk;
#endif  // DEBUG

        // Find all of its parent nodes' name.
        int64_t numParents = 0;
        DagNodeTypes::NodeIdListElt *parentElt;
        parentElt = listElt->node->parentsList;

        bool pruned = false;
        for (unsigned ii = 0; ii < numPrunedNodes; ii++) {
            if (listElt->getId() == prunedNodeIds[ii]) {
                pruned = true;
                break;
            }
        }

        if (!pruned) {
            while (parentElt != NULL) {
                assert(numParents < hdr_.numNodes);
                verifyOk(this->lookupClonedNode(clonedNodeHashBase,
                                                parentElt->nodeId,
                                                &parentNodeIds[numParents]));
                numParents++;
                parentElt = parentElt->next;
            }
        }

        if (flags & ConvertNamesToImmediate) {
            status =
                DagLib::get()->convertNamesToImmediate(this,
                                                       listElt->node->dagNodeHdr
                                                           .apiDagNodeHdr.api,
                                                       listElt->node->dagNodeHdr
                                                           .apiInput);
            BailIfFailed(status);
        }

        if (flags & ConvertNamesFromImmediate) {
            status = DagLib::get()
                         ->convertNamesFromImmediate(listElt->node->dagNodeHdr
                                                         .apiDagNodeHdr.api,
                                                     listElt->node->dagNodeHdr
                                                         .apiInput);
            BailIfFailed(status);
        }

        if (flags & ConvertUDFNamesToRelative) {
            status = DagLib::get()
                         ->convertUDFNamesToRelative(listElt->node->dagNodeHdr
                                                         .apiDagNodeHdr.api,
                                                     &udfContainer_,
                                                     listElt->node->dagNodeHdr
                                                         .apiInput);
            BailIfFailed(status);
        }

        if (flags & CopyUDFs) {
            // If current Dag and dagOut (to which the current Dag is being
            // copied), have different UDF containers, this means the UDF
            // references in dagOut may not resolve correctly unless the
            // UDFs are copied also. During makeRetina() (for the case in
            // which a retina is being created from a workbook dag, as
            // opposed to during importRetina()), the UDF containers will
            // typically be different - the udfContainer_ will belong to the
            // workbook, and the dagOut's container will be the new retina's
            // container. So the caller would've turned this flag on. See
            // assert and comment below.
            if (!UserDefinedFunction::
                    containersMatch(&udfContainer_,
                                    dagOut->getUdfContainer())) {
                // XXX: CopyUDFs can do 2pc, drop the lock. Note that in
                // general, holding the srcDag's lock across a clone
                // operation may not be needed at all- especially for a
                // clone operation triggered by makeRetina(). See Xc-12147.
                lock_.unlock();
                status =
                    DagLib::get()
                        ->copyUDFs(listElt->node->dagNodeHdr.apiDagNodeHdr.api,
                                   listElt->node->dagNodeHdr.apiInput,
                                   &udfContainer_,
                                   dagOut->getUdfContainer());
                lock_.lock();
                BailIfFailed(status);
            }
        }

        if (flags & ResetState) {
            state = DgDagStateCreated;
        } else {
            // Only change Ready states to Created
            if (dagOut->hdr_.graphType == QueryGraph &&
                state == DgDagStateReady) {
                state = DgDagStateCreated;
            }
        }

        // Don't expand pruned retinas
        if (!pruned && (flags & ExpandRetina) &&
            listElt->node->dagNodeHdr.apiDagNodeHdr.api ==
                XcalarApiExecuteRetina) {
            XcalarApiExecuteRetinaInput *executeRetinaInput =
                &listElt->node->dagNodeHdr.apiInput->executeRetinaInput;
            lock_.unlock();
            // This can do 2pc, drop the lock
            status =
                DagLib::get()->copyRetinaToNewDag(executeRetinaInput, dagOut);
            lock_.lock();
            BailIfFailedMsg(moduleName,
                            status,
                            "copyBatchDataflowToNewDag failed for batch "
                            "dataflow %s: %s",
                            executeRetinaInput->retinaName,
                            strGetFromStatus(status));

            // retinaNode is the last node
            newDagNodeId = dagOut->hdr_.lastNode;
        } else {
            status = dagOut->copyDagNodeToNewDagNode(&listElt->node->dagNodeHdr,
                                                     newDagNodeId,
                                                     numParents,
                                                     parentNodeIds,
                                                     listElt->node->xdbId,
                                                     listElt->node->tableId,
                                                     state);
            BailIfFailed(status);
        }

        status = this->insertClonedNode(clonedNodeHashBase,
                                        oldDagNodeId,
                                        newDagNodeId);
        BailIfFailed(status);

        (*numNodesCreated)++;

        listElt = listElt->prev;
    }

CommonExit:
    while (listHead != NULL) {
        listEltToFree = listHead;
        listHead = listHead->next;
        delete listEltToFree;
        listEltToFree = NULL;
    }

    if (parentNodeIds != NULL) {
        memFree(parentNodeIds);
        parentNodeIds = NULL;
    }

    return status;
}

Status
Dag::dgGetDag(DagNodeTypes::Node *dagNode,
              XcalarApiOutput **outputOut,
              size_t *outputSizeOut)
{
    Status status;

    uint64_t numNodes = 0;
    XcalarApiDagOutput *dagOutput = NULL;
    XcalarApiOutput *output = NULL;
    size_t outputSize, totalApiInputSize = 0, totalParentSize = 0,
                       totalChildrenSize = 0;

    DagNodeListElt *listEltToFree;
    bool locked = false;
    uint64_t ii = 0;
    size_t hdrSize;
    DagNodeListElt *listHead = NULL;
    DagNodeListElt *listTail = NULL;
    DagNodeListElt *listElt = NULL;
    void **dagNodesSelected = NULL;

    lock_.lock();
    locked = true;

    // put the first node onto bfs queue
    status = getDagAncestorTree(&dagNode,
                                1,
                                0,
                                NULL,
                                NULL,
                                &listHead,
                                &listTail,
                                &numNodes);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dagNodesSelected =
        (void **) memAllocExt(sizeof(*dagNodesSelected) * numNodes, moduleName);
    if (dagNodesSelected == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    assertStatic(XcalarApiSizeOfOutput(*dagOutput) >= sizeof(*dagOutput));
    hdrSize = XcalarApiSizeOfOutput(*dagOutput) - sizeof(*dagOutput);
    listElt = listHead;
    ii = 0;
    while (listElt != NULL) {
        assert(ii < numNodes);
        dagNodesSelected[ii] = listElt->node;
        ii++;
        totalApiInputSize += listElt->node->dagNodeHdr.apiDagNodeHdr.inputSize;
        totalParentSize +=
            listElt->node->numParent * sizeof(XcalarApiDagNodeId);

        totalChildrenSize +=
            listElt->node->numChild * sizeof(XcalarApiDagNodeId);

        listElt = listElt->next;
    }

    outputSize = hdrSize + this->sizeOfDagOutput(numNodes,
                                                 totalApiInputSize,
                                                 totalParentSize,
                                                 totalChildrenSize);
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    dagOutput = &output->outputResult.dagOutput;

    status = DagLib::get()->copyDagNodesToDagOutput(dagOutput,
                                                    outputSize - hdrSize,
                                                    dagNodesSelected,
                                                    numNodes,
                                                    &udfContainer_,
                                                    DagNodeTypeDagNode);
    assert(status == StatusOk);

    assert(ii == numNodes);
    assert(dagOutput->numNodes == numNodes);
    assert(status == StatusOk);

    *outputOut = output;
    *outputSizeOut = outputSize;

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (dagNodesSelected != NULL) {
        memFree(dagNodesSelected);
        dagNodesSelected = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
        }
    }

    while (listHead != NULL) {
        listEltToFree = listHead;
        listHead = listHead->next;
        delete listEltToFree;
        listEltToFree = NULL;
    }

    return status;
}

Status
Dag::checkDescendantDatasetRef(DagNodeTypes::Node *dagNode,
                               DsDatasetId datasetId,
                               bool *hasRef)
{
    Status status = StatusOk;
    assert(lock_.tryLock() == false);

    *hasRef = false;

    DagNodeTypes::NodeIdListElt *childElt = dagNode->childrenList;
    DagNodeTypes::Node *childNode;

    while (childElt) {
        refGet(&dagNode->refCount);

        status = this->lookupNodeById(childElt->nodeId, &childNode);
        assert(status == StatusOk);

        if (childNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateCreated ||
            childNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateProcessing) {
            // xdb might not have been created yet
            // must assume that it has a ref
            *hasRef = true;
        }

        if ((childNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateReady ||
             childNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateArchiving ||
             childNode->dagNodeHdr.apiDagNodeHdr.state ==
                 DgDagStateArchiveError) &&
            childNode->xdbId != XidInvalid) {
            // check if the xdb has a reference to the dataset
            XdbMeta *xdbMeta;
            status = XdbMgr::get()->xdbGet(childNode->xdbId, NULL, &xdbMeta);

            if (status == StatusOk) {
                for (unsigned ii = 0; ii < xdbMeta->numDatasets; ii++) {
                    if (datasetId == xdbMeta->datasets[ii]->datasetId_) {
                        // found a reference
                        *hasRef = true;
                        break;
                    }
                }
            }
        }

        refPut(&dagNode->refCount);

        if (*hasRef == true) {
            // we've found one ref, no need to search our descendents
            return StatusOk;
        }

        if (childNode->childrenList) {
            status = checkDescendantDatasetRef(childNode, datasetId, hasRef);
            if (status != StatusOk || *hasRef == true) {
                return status;
            }
        }
        childElt = childElt->next;
    }

    return StatusOk;
}

Status
Dag::checkBeforeDrop(DagNodeTypes::Node *dagNode)
{
    Status status;

    // XXX: need to make the rule for deleting the node
    if (dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateProcessing) {
        return StatusDeleteDagNodeFailed;
    }

    if (refRead(&dagNode->refCount) > 1) {
        return StatusDgNodeInUse;
    }

    if (dagNode->dagNodeHdr.apiDagNodeHdr.pinned) {
        if (!isMarkedForDeletion()) {
            // DagNode has pinned flag. Also this is not a case of entire
            // Dag getting deleted.
            return StatusTablePinned;
        } else {
            // DagNode has pinned flag, but is overridden because the
            // entire Dag is getting deleted anyway and we want to cleanout
            // all the underlying resources including Tables & Datasets.
            xSyslog(moduleName,
                    XlogInfo,
                    "Dag Node %lu Pin overridden by markForDelete",
                    dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId);
        }
    }

    // loop through descendants, checking if any of them are using the
    // dataset
    if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiBulkLoad &&
        strncmp(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                XcalarApiLrqPrefix,
                XcalarApiLrqPrefixLen) != 0 &&
        dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateReady) {
        const char *datasetName =
            dagNode->dagNodeHdr.apiInput->loadInput.datasetName;

        DsDatasetId datasetId =
            Dataset::get()->getDatasetIdFromName(datasetName, &status);

        // only check when we have a valid dataset
        if (status == StatusOk) {
            bool hasRef = false;
            status = checkDescendantDatasetRef(dagNode, datasetId, &hasRef);
            if (status != StatusOk) {
                return status;
            }

            if (hasRef) {
                return StatusDsDatasetInUse;
            }
        }
    }

    // this has to be the last check
    if (dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped) {
        // may be in the name hash table; will be removed later if so
        return StatusDagNodeDropped;
    }

    return StatusOk;
}

Status
Dag::checkBeforeDelete(DagNodeTypes::Node *dagNode)
{
    Status status;

    status = this->checkBeforeDrop(dagNode);
    if (status != StatusOk && status != StatusDagNodeDropped) {
        return status;
    }

    DagNodeTypes::NodeIdListElt *childElt = dagNode->childrenList;
    DagNodeTypes::Node *childNode;

    if (dagNode->dagNodeHdr.apiDagNodeHdr.api != XcalarApiBulkLoad) {
        while (childElt != NULL) {
            status = this->lookupNodeById(childElt->nodeId, &childNode);
            if (status != StatusOk) {
                return status;
            }

            if (childNode->dagNodeHdr.apiDagNodeHdr.state ==
                DgDagStateProcessing) {
                return StatusDgNodeInUse;
            }
            childElt = childElt->next;
        }
    }

    return StatusOk;
}

// this removes an operation from a query, fixing up the parents and children
Status
Dag::deleteQueryOperationNode(DagNodeTypes::Node *dagNode)
{
    Status status = StatusOk;
    char *parentName = NULL;
    DagTypes::NodeId parentId;
    assert(hdr_.graphType == DagTypes::QueryGraph);

    DagNodeTypes::NodeIdListElt *childElt = NULL, *elt = NULL;
    DagNodeTypes::Node *parent = NULL, *child = NULL;
    DagTypes::NodeId dagNodeId = dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;

    // this only works if you have exactly one parent
    assert(dagNode->numParent == 1);

    // fail in non-debug, instead of crashing usrnode
    if (dagNode->numParent != 1) {
        status = StatusDagNodeNumParentInvalid;
        xSyslog(moduleName,
                XlogErr,
                "Dag node (id %lu, api %s) parent count (%lu) not 1: %s ",
                dagNodeId,
                strGetFromXcalarApis(dagNode->dagNodeHdr.apiDagNodeHdr.api),
                dagNode->numParent,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = lookupNodeById(dagNode->parentsList->nodeId, &parent);
    assert(status == StatusOk);

    parentName = parent->dagNodeHdr.apiDagNodeHdr.name;
    parentId = parent->dagNodeHdr.apiDagNodeHdr.dagNodeId;

    // fix parent child relations and childs' apiInputs
    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        status = this->lookupNodeById(childElt->nodeId, &child);
        assert(status == StatusOk);

        // add new child to parent
        status = addChildNode(parent, child);
        BailIfFailed(status);

        // fixup child apiInputs
        DagLib::renameSrcApiInput(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                  parentName,
                                  child);

        // add new parent in child
        DagNodeTypes::NodeIdListElt *parentElt;
        parentElt = new (std::nothrow) DagNodeTypes::NodeIdListElt;
        BailIfNull(parentElt);

        parentElt->nodeId = parentId;

        // need to add it immediately after the current entry to maintain order
        elt = child->parentsList;
        do {
            if (elt->nodeId == dagNodeId) {
                parentElt->next = elt->next;
                elt->next = parentElt;
                break;
            }

            elt = elt->next;
        } while (elt != NULL);
        child->numParent++;

        childElt = childElt->next;
    }

    removeNodeFromNameTable(dagNode);
    removeAndDeleteDagNode(dagNode);

CommonExit:
    return status;
}

Status
Dag::deleteDagNodeLocal(DagTypes::NodeId dagNodeId, bool dropOnly)
{
    Status status;
    bool locked = false;
    DagNodeTypes::Node *dagNode;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(!this->isOnOwnerNode() ||
           (this->checkBeforeDrop(dagNode) == StatusDagNodeDropped));

    assert(!this->isOnOwnerNode() || dropOnly ||
           (this->checkBeforeDelete(dagNode) == StatusOk));

    // persistable DAG should have already removed the node from name table
    if (!this->isOnOwnerNode()) {
        this->removeNodeFromNameTable(dagNode);
        assert(lookupNodeByName(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                NULL,
                                TableScope::LocalOnly) ==
               StatusDagNodeNotFound);
    }
    if (dropOnly) {
        goto CommonExit;  // We're done here regardless of status
    }

    DagNodeTypes::Node *dagNodeTmp;
    assert(this->isOnOwnerNode() ||
           this->lookupNodeByName(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                  &dagNodeTmp,
                                  TableScope::LocalOnly) != StatusOk);
    this->removeAndDeleteDagNode(dagNode);
    dagNode = NULL;

    assert(this->lookupNodeById(dagNodeId, &dagNode) != StatusOk);

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

// This assumes you've already acquired the lock
Status
Dag::changeDagStateLocalNoLock(DagTypes::NodeId dagNodeId, DgDagState state)
{
    DagNodeTypes::Node *dagNode = NULL;
    Status status = StatusUnknown;

    assert(lock_.tryLock() == false);

    status = this->lookupNodeById(dagNodeId, &dagNode);

    if (status != StatusOk) {
        goto CommonExit;
    }

    if (this->hdr_.graphType == DagTypes::WorkspaceGraph) {
        if (dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped &&
            state != DgDagStateError) {
            assert(lookupNodeByName(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                    NULL,
                                    TableScope::LocalOnly) ==
                   StatusDagNodeNotFound);
            status = StatusDagNodeDropped;
            goto CommonExit;
        }
    }

    if (dagNode->dagNodeHdr.apiDagNodeHdr.state != state) {
        dagNode->dagNodeHdr.apiDagNodeHdr.state = state;
    }

CommonExit:
    return status;
}

void
Dag::copyCommentsAndTags(Dag *dstDag)
{
    Status status;
    DagTypes::NodeId dagNodeId;
    DagNodeTypes::Node *dagNodeIn = NULL;
    DagNodeTypes::Node *dagNodeOut = NULL;

    lock_.lock();
    dagNodeId = hdr_.firstNode;
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = lookupNodeById(dagNodeId, &dagNodeIn);
        assert(status == StatusOk);

        status =
            dstDag->lookupNodeByName(dagNodeIn->dagNodeHdr.apiDagNodeHdr.name,
                                     &dagNodeOut,
                                     TableScope::LocalOnly);
        if (status != StatusOk) {
            dagNodeId = dagNodeIn->dagNodeHdr.dagNodeOrder.next;
            continue;
        }

        verifyOk(strStrlcpy(dagNodeOut->dagNodeHdr.apiDagNodeHdr.tag,
                            dagNodeIn->dagNodeHdr.apiDagNodeHdr.tag,
                            sizeof(dagNodeOut->dagNodeHdr.apiDagNodeHdr.tag)));

        verifyOk(
            strStrlcpy(dagNodeOut->dagNodeHdr.apiDagNodeHdr.comment,
                       dagNodeIn->dagNodeHdr.apiDagNodeHdr.comment,
                       sizeof(dagNodeOut->dagNodeHdr.apiDagNodeHdr.comment)));

        dagNodeId = dagNodeIn->dagNodeHdr.dagNodeOrder.next;
    }
    lock_.unlock();
}

Status
Dag::commentDagNodes(char *comment,
                     unsigned int numNodes,
                     char (*nodeNames)[255])
{
    Status status = StatusOk;
    bool locked = false;

    lock_.lock();
    locked = true;

    for (unsigned ii = 0; ii < numNodes; ii++) {
        status = this->commentDagNodeLocalEx(nodeNames[ii], comment);
        (void) status;
    }

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::pinUnPinDagNode(const char *nodeName, PinApiType reqType)
{
    Status status = StatusOk;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = this->pinUnPinDagNodeLocalEx(nodeName, reqType);

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::pinUnPinDagNodeLocalEx(const char *nodeName, PinApiType reqType)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode = NULL;

    status = this->lookupNodeByName(nodeName, &dagNode, TableScope::LocalOnly);
    BailIfFailed(status);

    switch (reqType) {
    case Pin:
        if (!dagNode->dagNodeHdr.apiDagNodeHdr.pinned) {
            dagNode->dagNodeHdr.apiDagNodeHdr.pinned = true;
        } else {
            status = StatusTableAlreadyPinned;
        }
        break;
    case Unpin:
        if (dagNode->dagNodeHdr.apiDagNodeHdr.pinned) {
            dagNode->dagNodeHdr.apiDagNodeHdr.pinned = false;
        } else {
            status = StatusTableNotPinned;
        }
        break;
    default:
        assert(false);
        status = StatusInval;
        break;
    }

CommonExit:
    return status;
}

Status
Dag::isDagNodePinned(DagTypes::NodeId id, bool *isPinnedRet)
{
    Status status;
    DagNodeTypes::Node *node;
    *isPinnedRet = false;
    lock_.lock();
    status = lookupNodeById(id, &node);
    if (status.ok()) {
        *isPinnedRet = node->dagNodeHdr.apiDagNodeHdr.pinned;
    }
    lock_.unlock();
    return status;
}

Status
Dag::commentDagNodeLocalEx(const char *dagNodeName,
                           const char *comment,
                           bool needsLock)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *dagNode = NULL;

    status = this->lookupNodeByName(dagNodeName,
                                    &dagNode,
                                    TableScope::LocalOnly,
                                    needsLock);
    if (status != StatusOk) {
        // check the dropped nodes
        status = lookupDroppedNodeByName(dagNodeName, &dagNode);
        BailIfFailed(status);
    }

    verifyOk(strStrlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.comment,
                        comment,
                        XcalarApiMaxDagNodeCommentLen));

CommonExit:
    return status;
}

Status
Dag::tagDagNodeLocalEx(const char *dagNodeName, const char *tag, bool needsLock)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *dagNode = NULL;

    status = this->lookupNodeByName(dagNodeName,
                                    &dagNode,
                                    TableScope::LocalOnly,
                                    needsLock);
    if (status != StatusOk) {
        // check the dropped nodes
        status = lookupDroppedNodeByName(dagNodeName, &dagNode);
        BailIfFailed(status);
    }

    verifyOk(strStrlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.tag,
                        tag,
                        XcalarApiMaxDagNodeCommentLen));

CommonExit:
    return status;
}

Status
Dag::tagDagNodes(char *tag, unsigned int numNodes, DagTypes::NamedInput *nodes)
{
    Status status = StatusOk;
    bool locked = false;

    lock_.lock();
    locked = true;

    for (unsigned ii = 0; ii < numNodes; ii++) {
        status = this->tagDagNodeLocalEx(nodes[ii].nodeId, tag);
        (void) status;
    }

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::tagDagNodeLocalEx(DagTypes::NodeId nodeId, const char *tag)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *dagNode = NULL;
    size_t ret;

    status = lookupNodeById(nodeId, &dagNode);
    BailIfFailed(status);

    ret = strlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.tag,
                  tag,
                  XcalarApiMaxDagNodeTagLen);
    assert(ret < XcalarApiMaxDagNodeTagLen);

CommonExit:
    return status;
}

Status
Dag::renameDagNodeLocal(char *oldName, const char *newName)
{
    Status status;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = this->renameDagNodeLocalEx(oldName, newName);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::renameDagNodeLocalEx(char *oldName, const char *newName)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *dagNode;

    status = this->lookupNodeByName(oldName, &dagNode, TableScope::LocalOnly);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = this->lookupNodeByName(newName, NULL, TableScope::LocalOnly);
    if (status == StatusOk) {
        status = StatusDgDagAlreadyExists;
        goto CommonExit;
    }

    if (status != StatusDagNodeNotFound) {
        goto CommonExit;
    }

    status = StatusOk;

    // update the apiInput of my children
    DagNodeTypes::NodeIdListElt *childElt;
    DagNodeTypes::Node *childNode;

    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        status = this->lookupNodeById(childElt->nodeId, &childNode);
        if (status != StatusOk) {
            goto CommonExit;
        }

        DagLib::get()->renameSrcApiInput(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                         newName,
                                         childNode);
        childElt = childElt->next;
    }

    this->removeNodeFromNameTable(dagNode);

    // update my apiInput
    DagLib::get()->renameDstApiInput(dagNode,
                                     dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                     newName);

    status = strStrlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        newName,
                        DagTypes::MaxNameLen);
    BailIfFailed(status);

    this->insertNodeToNameTable(dagNode);

CommonExit:
    return status;
}

// This function should only be called from persistbale DAG
Status
Dag::getPutRef(bool isGet, NodeId dagNodeId)
{
    Status status = StatusUnknown;
    bool dagLocked = false;
    DagNodeTypes::Node *dagNode;

    assert(this->ownerNode_);

    this->lock();
    dagLocked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to lookup node %lu's reference count: %s",
                dagNodeId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dagNode->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped) {
        assert(lookupNodeByName(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                                NULL,
                                TableScope::LocalOnly) ==
               StatusDagNodeNotFound);
        status = StatusDagNodeDropped;
        goto CommonExit;
    }

    if (isGet) {
        refGet(&dagNode->refCount);
    } else {
#ifdef DEBUG
        if (refRead(&dagNode->refCount) == 1) {
            assert(dagNode->numChild == 0);
            assert(dagNode->numParent == 0);
            assert(this->hdr_.lastNode !=
                   dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId);
            assert(this->hdr_.firstNode !=
                   dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId);
        }
#endif
        refPut(&dagNode->refCount);
    }

CommonExit:
    if (dagLocked) {
        this->unlock();
        dagLocked = false;
    }

    return status;
}

Status
Dag::updateOpDetailsLocal(void *payload)
{
    Status status;
    UpdateOpDetailsInput *input = (UpdateOpDetailsInput *) payload;
    Dag *dag = NULL;
    DagNodeTypes::Node *dagNode;

    dag = DagLib::get()->lookupDag(input->dagId);
    if (dag == NULL) {
        assert(0);
        return StatusDgDagNotFound;
    }

    XdbMgr *xdbMgr = XdbMgr::get();

    Xdb *xdb;
    uint64_t xdbAllocated = 0, xdbConsumed = 0, transPageSent = 0,
             transPageReceived = 0;

    dag->lock();

    status = dag->lookupNodeById(input->dagNodeId, &dagNode);
    if (status != StatusOk) {
        assert(0);
        dag->unlock();
        return status;
    }

    status = xdbMgr->xdbGet(dagNode->xdbId, &xdb, NULL);
    if (status == StatusOk) {
        // get the precise xdb density as the number will be store in the DAG
        // node as the final number
        status =
            xdbMgr->xdbGetPageDensity(xdb, &xdbAllocated, &xdbConsumed, true);
        if (status != StatusOk) {
            return status;
        }

        // XXX TODO TP stats is not populated anywhere?
        status = xdbMgr->xdbGetNumTransPages(xdb,
                                             &transPageSent,
                                             &transPageReceived);
        if (status != StatusOk) {
            return status;
        }
    } else {
        // there isn't an xdb associated with this dagNode
        status = StatusOk;
    }

    verifyOk(::sparseMemCpy(&dagNode->dagNodeHdr.opDetails,
                            &input->opDetails,
                            sizeof(OpDetails),
                            PageSize,
                            NULL));

    dagNode->dagNodeHdr.opDetails.xdbBytesConsumed = xdbConsumed;
    dagNode->dagNodeHdr.opDetails.xdbBytesRequired = xdbAllocated;

    dagNode->opStatus.atomicOpDetails.numRowsTotal =
        input->opDetails.numRowsTotal;
    dagNode->opStatus.atomicOpDetails.sizeTotal = input->opDetails.sizeTotal;

    unsigned numNodes = Config::get()->getActiveNodes();
    for (unsigned ii = 0; ii < numNodes; ii++) {
        dagNode->opStatus.atomicOpDetails.perNode[ii].numRows =
            input->opDetails.perNode[ii].numRows;
        dagNode->opStatus.atomicOpDetails.perNode[ii].sizeBytes =
            input->opDetails.perNode[ii].sizeBytes;
    }

    dagNode->stopwatch.stop();

    dag->unlock();

    return status;
}

Status
Dag::dagSetScalarResultLocal(void *payload)
{
    Status status;
    bool locked = false;
    DagNodeTypes::Node *dagNode = NULL;
    Dag *dag = NULL;
    SetScalarResultInput *input = (SetScalarResultInput *) payload;

    dag = DagLib::get()->lookupDag(input->dagId);
    if (dag == NULL) {
        return StatusDgDagNotFound;
    }

    dag->lock();  // XXX ENG-9967 potential race if dag get deleted
    locked = true;

    status = dag->lookupNodeById(input->dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNode->scalarResult == NULL) {
        dagNode->scalarResult = Scalar::allocScalar(DfMaxFieldValueSize);
        if (dagNode->scalarResult == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }
    }
    status = dagNode->scalarResult->copyFrom(input->scalarIn);

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    if (locked) {
        dag->unlock();
        locked = false;
    }

    if (status != StatusOk) {
        if (dagNode != NULL) {
            if (dagNode->scalarResult != NULL) {
                Scalar::freeScalar(dagNode->scalarResult);
                dagNode->scalarResult = NULL;
            }
        }
    }

    return status;
}

Status
Dag::dropLastNodeAndReturnInfo(DagTypes::NodeId *dagNodeIdOut,
                               XdbId *xdbIdOut,
                               OpDetails *opDetailsOut)
{
    Status status;
    DagTypes::NodeId lastNode = hdr_.lastNode;
    DagNodeTypes::Node *dagNode = NULL;
    lock_.lock();
    status = this->lookupNodeById(lastNode, &dagNode);
    assert(status == StatusOk);
    lock_.unlock();

    if (xdbIdOut != NULL) {
        *xdbIdOut = dagNode->xdbId;
    }

    if (opDetailsOut != NULL) {
        memcpy(opDetailsOut, &dagNode->dagNodeHdr.opDetails, sizeof(OpDetails));
    }

    status = deleteDagNodeById(lastNode, DagTypes::RemoveNodeFromNamespace);
    BailIfFailed(status);

CommonExit:
    *dagNodeIdOut = lastNode;

    return status;
}

Status
Dag::deleteDagNodeById(DagTypes::NodeId dagNodeId, bool dropOnly)
{
    DeleteDagNodeInput *deleteDagNodeInput = NULL;
    Status status;
    DagNodeTypes::Node *dagNode;
    bool locked = false;
    bool globalLockAcquired = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
    char nodeName[MaxNameLen + 1];
    TableNsMgr *tnsMgr = TableNsMgr::get();
    bool tableDropped = false;

    Gvm::Payload *gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(DeleteDagNodeInput));
    BailIfNull(gPayload);

    deleteDagNodeInput = (DeleteDagNodeInput *) gPayload->buf;

    globalLock_.lock();
    globalLockAcquired = true;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    tableId = dagNode->getTableId();
    status = strStrlcpy(nodeName,
                        dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        sizeof(nodeName));
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dropOnly) {
        status = this->checkBeforeDrop(dagNode);
        if (status != StatusOk) {
            goto CommonExit;
        }
    } else {
        status = this->checkBeforeDelete(dagNode);
        if (status != StatusOk) {
            goto CommonExit;
        }
    }

    // See Xc-13097: a query replay or upload may create dag nodes in dropped
    // state which must be entered in the name hash table on creation. So the
    // dropped state isn't a reliable indicator of whether a node is in the name
    // table or not. So unconditionally remove the node from the name table - if
    // it's not there, the remove function below will just return; if it is
    // there, it'll be removed.
    //
    this->removeNodeFromNameTable(dagNode);
    assert(lookupNodeByName(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                            NULL,
                            TableScope::LocalOnly) == StatusDagNodeNotFound);
    if (dagNode->dagNodeHdr.apiDagNodeHdr.state != DgDagStateDropped) {
        status = this->changeDagStateLocalNoLock(dagNodeId, DgDagStateDropped);
        assert(status == StatusOk);
    }

    tableDropped = dagNode->tableDropped();
    dagNode->setTableDropped();

    lock_.unlock();
    locked = false;

    deleteDagNodeInput->dagId = this->getId();
    deleteDagNodeInput->dagNodeId = dagNodeId;
    deleteDagNodeInput->dropOnly = dropOnly;

    if (hdr_.graphType == DagTypes::QueryGraph) {
        status = DagNodeGvm::get()->localHandler(DgUpdateDeleteNode,
                                                 deleteDagNodeInput,
                                                 NULL);
    } else {
        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                       DgUpdateDeleteNode,
                       sizeof(*deleteDagNodeInput));

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (globalLockAcquired) {
        globalLock_.unlock();
        globalLockAcquired = false;
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (!tableDropped && hdr_.graphType == DagTypes::WorkspaceGraph &&
        tnsMgr->isTableIdValid(tableId)) {
        XcalarApiUdfContainer *sessionContainer = getSessionContainer();
        xSyslog(moduleName,
                XlogInfo,
                "Dag::deleteDagNodeById userName '%s', session '%s', Id "
                "%lX, retina '%s', remove table Id %ld",
                sessionContainer->userId.userIdName,
                sessionContainer->sessionInfo.sessionName,
                sessionContainer->sessionInfo.sessionId,
                sessionContainer->retinaName,
                tableId);
        tnsMgr->removeFromNs(sessionContainer, tableId, nodeName);
    }

    return status;
}

Status
Dag::getDagNodeIds(const DagTypes::NodeName name,
                   TableScope tScope,
                   DagTypes::NodeId *dagNodeIdOut,
                   XdbId *xdbIdOut,
                   TableNsMgr::TableId *tableIdOut)
{
    DagNodeTypes::Node *dagNode;
    Status status;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = this->lookupNodeByName(name, &dagNode, tScope);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNodeIdOut != NULL) {
        *dagNodeIdOut = dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;
    }

    if (xdbIdOut != NULL) {
        *xdbIdOut = dagNode->xdbId;
    }

    if (tableIdOut != NULL) {
        *tableIdOut = dagNode->tableId;
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }
    return status;
}

Status
Dag::getDagNodeId(const DagTypes::NodeName name,
                  TableScope tScope,
                  DagTypes::NodeId *dagNodeIdOut)
{
    return getDagNodeIds(name, tScope, dagNodeIdOut, NULL, NULL);
}

Status
Dag::getXdbId(const DagTypes::NodeName name, TableScope tScope, XdbId *xdbIdOut)
{
    return getDagNodeIds(name, tScope, NULL, xdbIdOut, NULL);
}

// This function should only be called from persistbale DAG
Status
Dag::getDagNodeRefById(DagTypes::NodeId dagNodeId)
{
    return this->getPutRef(true, dagNodeId);
}

// This function should only be called from persistbale DAG
void
Dag::putDagNodeRefById(DagTypes::NodeId dagNodeId)
{
    if (this->getPutRef(false, dagNodeId) != StatusOk) {
        xSyslogTxnBuf(moduleName,
                      XlogErr,
                      "Cannot drop dag node reference on node %lu",
                      dagNodeId);
    }
}

Status
Dag::renameDagNode(char *oldName, char *newName)
{
    RenameDagNodeInput *renameDagNodeInput = NULL;
    Status status = StatusOk;
    DagTypes::NodeId dagNodeId;
    bool decRef = false;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;
    XdbId xdbId = XidInvalid;
    TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    // oldName does not exist
    status = this->getDagNodeIds(oldName,
                                 TableScope::LocalOnly,
                                 &dagNodeId,
                                 &xdbId,
                                 &tableId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = this->getDagNodeRefById(dagNodeId);
    if (status != StatusOk) {
        assert(0);
        goto CommonExit;
    }

    decRef = true;

    // newName already exist
    status = this->getDagNodeId(newName, TableScope::LocalOnly, NULL);
    if (status == StatusOk) {
        status = StatusDgDagAlreadyExists;
        goto CommonExit;
    }

    if (status != StatusDagNodeNotFound) {
        goto CommonExit;
    }

    status = StatusOk;

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(RenameDagNodeInput));
    BailIfNull(gPayload);

    renameDagNodeInput = (RenameDagNodeInput *) gPayload->buf;
    renameDagNodeInput->dagId = this->getId();
    status = strStrlcpy(renameDagNodeInput->oldName,
                        oldName,
                        sizeof(renameDagNodeInput->oldName));
    BailIfFailed(status);

    status = strStrlcpy(renameDagNodeInput->newName,
                        newName,
                        sizeof(renameDagNodeInput->newName));
    BailIfFailed(status);

    if (hdr_.graphType == DagTypes::QueryGraph) {
        status = DagNodeGvm::get()->localHandler(DgUpdateRenameNode,
                                                 renameDagNodeInput,
                                                 NULL);
    } else {
        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);

        gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                       DgUpdateRenameNode,
                       sizeof(RenameDagNodeInput));

        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
    }

    if (status != StatusOk) {
        status = StatusRenameDagNodeFailed;
        goto CommonExit;
    }

    if (hdr_.graphType == DagTypes::WorkspaceGraph &&
        tnsMgr->isTableIdValid(tableId)) {
        status =
            tnsMgr->renameNs(getSessionContainer(), tableId, oldName, newName);
        BailIfFailedMsg(moduleName,
                        status,
                        "Error renaming node \"%s\" to \"%s\": %s",
                        oldName,
                        newName,
                        strGetFromStatus(status));
    }

CommonExit:
    if (decRef) {
        this->putDagNodeRefById(dagNodeId);
    }

    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

Status
Dag::copyDagNodeOpStatus(Dag *srcDag,
                         DagTypes::NodeId srcNodeId,
                         DagTypes::NodeId dstNodeId)
{
    Status status = StatusOk;
    bool locked = false;
    bool srcLocked = false;
    DagNodeTypes::Node *dagNodeIn = NULL;
    DagNodeTypes::Node *dagNodeOut = NULL;

    lock_.lock();
    locked = true;

    srcDag->lock();
    srcLocked = true;

    status = srcDag->lookupNodeById(srcNodeId, &dagNodeIn);
    BailIfFailed(status);

    status = lookupNodeById(dstNodeId, &dagNodeOut);
    BailIfFailed(status);

    verifyOk(::sparseMemCpy(&dagNodeOut->opStatus,
                            &dagNodeIn->opStatus,
                            sizeof(OpStatus),
                            PageSize,
                            NULL));

    verifyOk(::sparseMemCpy(&dagNodeOut->dagNodeHdr.opDetails,
                            &dagNodeIn->dagNodeHdr.opDetails,
                            sizeof(OpDetails),
                            PageSize,
                            NULL));

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (srcLocked) {
        srcDag->unlock();
        srcLocked = false;
    }

    return status;
}

Status
Dag::copyOpDetails(Dag *dstDag)
{
    Status status;
    DagTypes::NodeId dagNodeId;
    DagNodeTypes::Node *dagNodeIn = NULL;
    DagNodeTypes::Node *dagNodeOut = NULL;

    lock_.lock();
    dagNodeId = hdr_.firstNode;
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = lookupNodeById(dagNodeId, &dagNodeIn);
        assert(status == StatusOk);

        status =
            dstDag->lookupNodeByName(dagNodeIn->dagNodeHdr.apiDagNodeHdr.name,
                                     &dagNodeOut,
                                     TableScope::LocalOnly);
        if (status != StatusOk) {
            dagNodeId = dagNodeIn->dagNodeHdr.dagNodeOrder.next;
            continue;
        }

        verifyOk(::sparseMemCpy(&dagNodeOut->dagNodeHdr.opDetails,
                                &dagNodeIn->dagNodeHdr.opDetails,
                                sizeof(OpDetails),
                                PageSize,
                                NULL));

        dagNodeId = dagNodeIn->dagNodeHdr.dagNodeOrder.next;
    }
    lock_.unlock();

    return StatusOk;
}

Status
Dag::convertNamesToImmediate()
{
    Status status = StatusOk;
    DagTypes::NodeId dagNodeId;
    DagNodeTypes::Node *dagNode = NULL;

    dagNodeId = hdr_.firstNode;
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = lookupNodeById(dagNodeId, &dagNode);
        assert(status == StatusOk);

        status =
            DagLib::get()
                ->convertNamesToImmediate(this,
                                          dagNode->dagNodeHdr.apiDagNodeHdr.api,
                                          dagNode->dagNodeHdr.apiInput);
        BailIfFailed(status);

        dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
    }

CommonExit:
    return status;
}

// Caller must make sure there is no ongoing operation on the source dag
Status
Dag::cloneDag(Dag **dagOut,
              DagTypes::GraphType dstType,
              XcalarApiUdfContainer *udfContainer,
              uint64_t numTargetNodes,
              DagTypes::NodeName *targetNodesNameArray,
              unsigned numPrunedNodes,
              NodeName *prunedNodeNames,
              CloneFlags flags)
{
    lock_.lock();

    Status status = cloneDagInt(dagOut,
                                dstType,
                                udfContainer,
                                numTargetNodes,
                                targetNodesNameArray,
                                numPrunedNodes,
                                prunedNodeNames,
                                flags);

    lock_.unlock();

    return status;
}

Status
Dag::getAndCloneDag(Dag **dagOut,
                    DagTypes::GraphType srcGraphType,
                    DagTypes::GraphType dstType,
                    CloneFlags flags,
                    DagTypes::SearchMode ignoreProcessing)
{
    Status status;
    bool dagLocked = false;
    uint64_t numTargetNodes;
    DagTypes::NodeName *activeNodes = NULL;

    assert(dstType == DagTypes::QueryGraph);
    lock_.lock();
    dagLocked = true;

    status = getActiveDagNodesInt(srcGraphType,
                                  &activeNodes,
                                  NULL,
                                  &numTargetNodes,
                                  ignoreProcessing,
                                  false);
    BailIfFailed(status);

    status = cloneDagInt(dagOut,
                         dstType,
                         &this->udfContainer_,
                         numTargetNodes,
                         activeNodes,
                         0,
                         NULL,
                         flags);
    BailIfFailed(status);

CommonExit:
    if (dagLocked) {
        lock_.unlock();
        dagLocked = false;
    }

    if (activeNodes != NULL) {
        memFree(activeNodes);
        activeNodes = NULL;
        numTargetNodes = 0;
    }

    return status;
}

// Caller must make sure there is no ongoing operation on the source dag.
Status
Dag::cloneDagInt(Dag **dagOut,
                 DagTypes::GraphType dstType,
                 XcalarApiUdfContainer *udfContainer,
                 uint64_t numTargetNodes,
                 DagTypes::NodeName *targetNodesNameArray,
                 uint64_t numPrunedNodes,
                 DagTypes::NodeName *prunedNodeNames,
                 CloneFlags flags)
{
    Status status = StatusOk;
    bool cloneWholeDag = (numTargetNodes == DagTypes::CloneWholeDag);
    DagNodeTypes::Node **targetNodeArray = NULL;
    DagTypes::NodeId *prunedNodeIdArray = NULL;
    bool dagLocked = true;
    // Caller is managing the lock and must already hold it during this call.
    assert(!lock_.tryLock());
    ClonedNodeElt **clonedNodeHashBase = NULL;

    assert(dstType == DagTypes::QueryGraph &&
           "Dest graph type has to be DagTypes::QueryGraph, i.e. node local."
           " It does not make sense to cloned dst graph to be cluster wide!");
    if (dstType != DagTypes::QueryGraph) {
        status = StatusInval;
        goto CommonExit;
    }

    status = DagLib::get()->createNewDag(hdr_.numSlot,
                                         dstType,
                                         udfContainer,
                                         dagOut);
    if (status != StatusOk) {
        goto CommonExit;
    }

    uint64_t numDstNodes;
    numDstNodes = hdr_.numNodes;
    if (numDstNodes == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    if (cloneWholeDag) {
        assert(numTargetNodes == DagTypes::CloneWholeDag);
        DagTypes::NodeId dagNodeId = hdr_.firstNode;
        numTargetNodes = 0;
        while (dagNodeId != DagTypes::InvalidDagNodeId) {
            DagNodeTypes::Node *dagNode = NULL;
            numTargetNodes++;
            status = this->lookupNodeById(dagNodeId, &dagNode);
            assert(status == StatusOk);
            dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
        }
    }

    if (numTargetNodes == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    targetNodeArray = (DagNodeTypes::Node **) memAlloc(
        numTargetNodes * sizeof(targetNodeArray[0]));
    BailIfNull(targetNodeArray);

    prunedNodeIdArray = (DagTypes::NodeId *) memAlloc(
        numPrunedNodes * sizeof(prunedNodeIdArray[0]));
    BailIfNull(prunedNodeIdArray);

    if (cloneWholeDag) {
        DagTypes::NodeId dagNodeId = hdr_.firstNode;
        for (uint64_t ii = 0; dagNodeId != DagTypes::InvalidDagNodeId; ii++) {
            DagNodeTypes::Node *dagNode;
            status = this->lookupNodeById(dagNodeId, &dagNode);
            assert(status == StatusOk);
            targetNodeArray[ii] = dagNode;
            dagNodeId = dagNode->dagNodeHdr.dagNodeOrder.next;
        }
    } else {
        for (uint64_t ii = 0; ii < numTargetNodes; ii++) {
            DagNodeTypes::Node *dagNode;
            status = this->lookupNodeByName(targetNodesNameArray[ii],
                                            &dagNode,
                                            TableScope::LocalOnly);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to lookupNode %s: %s",
                        targetNodesNameArray[ii],
                        strGetFromStatus(status));
                goto CommonExit;
            }
            targetNodeArray[ii] = dagNode;
        }
    }

    for (uint64_t ii = 0; ii < numPrunedNodes; ii++) {
        DagNodeTypes::Node *dagNode;
        status = this->lookupNodeByName(prunedNodeNames[ii],
                                        &dagNode,
                                        TableScope::LocalOnly);
        if (status != StatusOk) {
            prunedNodeIdArray[ii] = DagTypes::InvalidDagNodeId;
        } else {
            prunedNodeIdArray[ii] = dagNode->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        }
    }

    clonedNodeHashBase = allocateCloneNodeHashBase();
    BailIfNull(clonedNodeHashBase);

    uint64_t numNodesCreated;
    numNodesCreated = 0;
    status = copyDagToNewDag(clonedNodeHashBase,
                             targetNodeArray,
                             numTargetNodes,
                             *dagOut,
                             numPrunedNodes,
                             prunedNodeIdArray,
                             flags,
                             &numNodesCreated);
    BailIfFailed(status);

CommonExit:
    if (!dagLocked) {
        // must grab the lock again before returning
        lock_.lock();
    }

    if (targetNodeArray != NULL) {
        memFree(targetNodeArray);
        targetNodeArray = NULL;
    }

    if (prunedNodeIdArray != NULL) {
        memFree(prunedNodeIdArray);
        prunedNodeIdArray = NULL;
    }

    if (clonedNodeHashBase != NULL) {
        deleteClonedNodeHashTable(clonedNodeHashBase);
        clonedNodeHashBase = NULL;
    }

    if (status != StatusOk) {
        if (*dagOut != NULL) {
            verify(DagLib::get()->destroyDag(*dagOut,
                                             DagTypes::DestroyDeleteNodes) ==
                   StatusOk);
            *dagOut = NULL;
        }
    }

    return status;
}

Status
Dag::gatherOpStatus(DagTypes::NodeId dagNodeId,
                    OpDetailWrapper *opDetailsWrapperArray,
                    uint64_t numOpDetails)
{
    Status status = StatusOk;
    OperatorsStatusInput input;
    input.dagNodeId = dagNodeId;
    input.dagId = this->getId();
    MsgMgr *msgMgr = MsgMgr::get();
    unsigned numActiveNodes = Config::get()->getActiveNodes();
    size_t returnPayloadMaxSize = 0;
    MsgEphemeral eph;
    TwoPcHandle twoPcHandle;

    assert(opDetailsWrapperArray != NULL);
    assert(numOpDetails == Config::get()->getActiveNodes());

    // estimate the size of payload on return path

    returnPayloadMaxSize = sizeof(OpDetails);

    // payload length needs to cover returnPayloadMaxSize size, because the
    // payload will be reused to send back OpDetails struct - so
    // pass in returnPayloadMaxSize explicitly to the EphemeralInit()
    // XXX: file bug to review all callers of twoPcEphemeralInit()
    // which pass in 0 for 4th arg - if the payload is re-purposed
    // for a different struct on return, there shouldn't be any harm
    // in supplying the size of this struct in 4th arg - and it'll
    // prevent over-writes on return
    msgMgr->twoPcEphemeralInit(&eph,
                               &input,
                               sizeof(input),
                               returnPayloadMaxSize,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcXcalarApiGetOpStatus1,
                               opDetailsWrapperArray,
                               (TwoPcBufLife)(TwoPcMemCopyInput |
                                              TwoPcMemCopyOutput));

    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcXcalarApiGetOpStatus,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrPlusPayload),
                           TwoPcSyncCmd,
                           TwoPcAllNodes,
                           TwoPcIgnoreNodeId,
                           TwoPcClassNonNested);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(!twoPcHandle.twoPcHandle);
    for (unsigned jj = 0; jj < numActiveNodes; ++jj) {
        if (opDetailsWrapperArray[jj].status != StatusOk) {
            status = opDetailsWrapperArray[jj].status;
            break;
        }
    }

CommonExit:
    return status;
}

Status
Dag::getDagNodeApi(DagTypes::NodeId dagNodeId, XcalarApis *api)
{
    DagNodeTypes::Node *dagNode;
    Status status;
    bool locked = false;

    assert(dagNodeId != DagTypes::InvalidDagNodeId);
    assert(api != NULL);

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    BailIfFailed(status);

    *api = dagNode->dagNodeHdr.apiDagNodeHdr.api;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

void
Dag::processOpErrors(OpErrorStats *outAcc, OpErrorStats *in, XcalarApis api)
{
    switch (api) {
    case XcalarApiFilter:
    case XcalarApiMap: {
        // XDF errors
        outAcc->evalErrorStats.evalXdfErrorStats.numTotal +=
            in->evalErrorStats.evalXdfErrorStats.numTotal;

        outAcc->evalErrorStats.evalXdfErrorStats.numUnsubstituted +=
            in->evalErrorStats.evalXdfErrorStats.numUnsubstituted;

        outAcc->evalErrorStats.evalXdfErrorStats.numUnspportedTypes +=
            in->evalErrorStats.evalXdfErrorStats.numUnspportedTypes;

        outAcc->evalErrorStats.evalXdfErrorStats.numMixedTypeNotSupported +=
            in->evalErrorStats.evalXdfErrorStats.numMixedTypeNotSupported;

        outAcc->evalErrorStats.evalXdfErrorStats.numEvalCastError +=
            in->evalErrorStats.evalXdfErrorStats.numEvalCastError;
        ;

        outAcc->evalErrorStats.evalXdfErrorStats.numDivByZero +=
            in->evalErrorStats.evalXdfErrorStats.numDivByZero;
        ;

        outAcc->evalErrorStats.evalXdfErrorStats.numMiscError +=
            in->evalErrorStats.evalXdfErrorStats.numMiscError;
        ;

        // UDF errors
        outAcc->evalErrorStats.evalUdfErrorStats.numEvalUdfError +=
            in->evalErrorStats.evalUdfErrorStats.numEvalUdfError;

    } break;
    case XcalarApiBulkLoad: {
        outAcc->loadErrorStats.numFileOpenFailure +=
            in->loadErrorStats.numFileOpenFailure;
        outAcc->loadErrorStats.numFileOpenFailure +=
            in->loadErrorStats.numFileOpenFailure;
    } break;
    case XcalarApiIndex: {
        outAcc->indexErrorStats.numParseError +=
            in->indexErrorStats.numParseError;
        outAcc->indexErrorStats.numFieldNoExist +=
            in->indexErrorStats.numFieldNoExist;
        outAcc->indexErrorStats.numTypeMismatch +=
            in->indexErrorStats.numTypeMismatch;
        outAcc->indexErrorStats.numOtherError +=
            in->indexErrorStats.numOtherError;
    } break;
    default:
        // XXX: errors for no other APIs supported currently
        ;
    }
}

// The caller should call the function only with valid opStatusArray
void
Dag::processOpStatuses(OpDetailWrapper *opDetailsWrapperArray,
                       unsigned numOpDetails,
                       OpDetails *out,
                       XcalarApis api)
{
    out->numWorkCompleted = 0;
    out->numWorkTotal = 0;
    out->numRowsTotal = 0;
    out->cancelled = false;
    // Anything that's potentially accumulated should be zero'ed here. Several
    // error stats may be (currently it's numRowsFailedTotal in evalErrorStats,
    // but in the future there'd be other error stats - so just memZero all)
    memZero(&out->errorStats, sizeof(OpErrorStats));

    for (unsigned ii = 0; ii < numOpDetails; ii++) {
        assert(opDetailsWrapperArray[ii].status == StatusOk);
        out->numWorkCompleted +=
            opDetailsWrapperArray[ii].opDetails.numWorkCompleted;
        out->numWorkTotal += opDetailsWrapperArray[ii].opDetails.numWorkTotal;
        out->perNode[ii].numRows =
            opDetailsWrapperArray[ii].opDetails.perNode[ii].numRows;
        out->numRowsTotal += out->perNode[ii].numRows;
        out->perNode[ii].sizeBytes =
            opDetailsWrapperArray[ii].opDetails.perNode[ii].sizeBytes;
        // XXX: should out->sizeTotal be zeroed out above along with the others?
        out->sizeTotal += out->perNode[ii].sizeBytes;
        out->perNode[ii].numTransPagesReceived = opDetailsWrapperArray[ii]
                                                     .opDetails.perNode[ii]
                                                     .numTransPagesReceived;

        out->perNode[ii].hashSlotSkew =
            opDetailsWrapperArray[ii].opDetails.perNode[ii].hashSlotSkew;

        if (!out->cancelled && opDetailsWrapperArray[ii].opDetails.cancelled) {
            out->cancelled = true;
        }

        // Note: following could be replaced by the summary stats from the
        // summarized errors table but it may be good to have two ways to
        // do this and then cross-check that they're the same...

        processOpErrors(&out->errorStats,
                        &opDetailsWrapperArray[ii].opDetails.errorStats,
                        api);
    }
}

Status
Dag::setOpDetails(DagTypes::NodeId dagNodeId, OpDetails *opDetails)
{
    UpdateOpDetailsInput *input = NULL;
    Status status = StatusUnknown;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) +
                                         sizeof(UpdateOpDetailsInput));
    BailIfNull(gPayload);

    gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                   DgUpdateOpDetails,
                   sizeof(UpdateOpDetailsInput));
    input = (UpdateOpDetailsInput *) gPayload->buf;
    input->dagNodeId = dagNodeId;
    input->dagId = getId();
    input->opDetails = *opDetails;

    assert(hdr_.graphType == DagTypes::WorkspaceGraph);

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }
CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

//
// This routine logs and copies summarized failure information from the
// supplied summarized failure table in failureTabId.
//
// The entire summary will be logged, but only upto XcalarApiMaxFailures rows
// will be copied to the opDetails struct in 'input' since the latter has this
// limit.
//
// This is done by extracting the information from the failure table using
// getTableMeta (to get the table schema) first, and then using the schema to
// extract the fields from the failure table using a result set. The extracted
// fields are added into input->opDetails before broadcasting it to all nodes
// via GVM.

Status
Dag::updateOpDetailsOneFailSummTab(XcalarApis api,
                                   DagTypes::NodeId dagNodeId,
                                   UpdateOpDetailsInput *input,
                                   DagTypes::NodeId failureTabId,
                                   uint32_t tabNum,
                                   XcalarApiUserId *userId,
                                   XcalarApiSessionInfoInput *sessInfo)
{
    Status status = StatusOk;
    ResultSet *rs = NULL;
    uint64_t resultSetId = XidInvalid;
    XcalarApiMakeResultSetOutput *makeResultSetOutput = NULL;
    char failureTabName[XcalarApiMaxTableNameLen + 1];

    // NOTE: We should use 'api' to do per-api customized processing of
    // failureTabId - e.g. for map API, evalErrorStats must be updated,
    // for load, the loadErrorStats, etc.; for now, assume this is supported
    // only for the map and filter operations.
    assert(api == XcalarApiMap || api == XcalarApiFilter);

    switch (api) {
    case XcalarApiFilter:
    case XcalarApiMap: {
        char dagNodeName[XcalarApiMaxTableNameLen + 1];
        char failTabCountFieldName[XcalarApiMaxFieldNameLen + 1];
        char failTabDescFieldName[XcalarApiMaxFieldNameLen + 1];
        XcalarApiGetTableMetaInput getTableMetaInput;
        XcalarApiGetTableMetaOutput *failTabMeta;
        XcalarApiOutput *apiOut;
        size_t outputSize;
        uint64_t numRowsFailedTotal = 0;
        json_t *entries;
        uint64_t numEntries;
        json_t *jsonRecord = NULL;
        bool has_count = false;
        bool has_desc = false;
        OpEvalUdfErrorStats *opUdfError = NULL;
        json_t *jsonFailInfo = NULL;
        json_t *jsonFailCnt;

        opUdfError =
            &input->opDetails.errorStats.evalErrorStats.evalUdfErrorStats;

        // get failure table's schema which is expected to be a 2 column
        // schema (count, string). The column names are copied into:
        // failTabCountFieldName and failTabDescFieldName, respectively.
        getTableMetaInput.tableNameInput.isTable = true;
        getTableMetaInput.tableNameInput.nodeId = failureTabId;
        getTableMetaInput.isPrecise = false;  // field's probably dead (SDK-823)

        status = Operators::get()->getTableMeta(&getTableMetaInput,
                                                &apiOut,
                                                &outputSize,
                                                dagId_,
                                                NULL  // table doesn't need user
        );
        BailIfFailed(status);

        failTabMeta = &apiOut->outputResult.getTableMetaOutput;

        assert(failTabMeta->numValues == 2);  // failure tab must have 2 cols

        if (failTabMeta->numValues != 2) {
            status = StatusMapFailureSummarySchema;
            xSyslog(moduleName,
                    XlogErr,
                    "updateOpDetailsFailureSummary failed for API \"%s\", "
                    "failure table has too many cols",
                    strGetFromXcalarApis(api));
            goto CommonExit;
        }

        for (unsigned ii = 0; ii < failTabMeta->numValues; ii++) {
            if (failTabMeta->valueAttrs[ii].type == DfString) {
                if (strlcpy(failTabDescFieldName,
                            failTabMeta->valueAttrs[ii].name,
                            sizeof(failTabDescFieldName)) <
                    sizeof(failTabDescFieldName)) {
                    has_desc = true;
                }
            } else if (failTabMeta->valueAttrs[ii].type == DfInt64) {
                if (strlcpy(failTabCountFieldName,
                            failTabMeta->valueAttrs[ii].name,
                            sizeof(failTabCountFieldName)) <
                    sizeof(failTabCountFieldName)) {
                    has_count = true;
                }
            }
        }
        assert(has_desc && has_count);

        if (!has_desc || !has_count) {
            status = StatusMapFailureSummarySchema;
            xSyslog(moduleName,
                    XlogErr,
                    "updateOpDetailsFailureSummary failed for API \"%s\", "
                    "failure table has invalid column name or type",
                    strGetFromXcalarApis(api));
            goto CommonExit;
        }

        status = getDagNodeName(failureTabId,
                                failureTabName,
                                sizeof(failureTabName));

        BailIfFailed(status);

        makeResultSetOutput = (XcalarApiMakeResultSetOutput *) memAlloc(
            sizeof(*makeResultSetOutput) +
            sizeof(XcalarApiTableMeta) * Config::get()->getActiveNodes());

        BailIfNull(makeResultSetOutput);

        // create a result set so failure table can be cursored
        status = ResultSetMgr::get()->makeResultSet(this,
                                                    failureTabName,
                                                    false,
                                                    makeResultSetOutput,
                                                    userId);
        BailIfFailed(status);

        resultSetId = makeResultSetOutput->resultSetId;
        numEntries = makeResultSetOutput->numEntries;

        rs = ResultSetMgr::get()->getResultSet(resultSetId);
        BailIfNullMsg(rs,
                      StatusInvalidResultSetId,
                      moduleName,
                      "Failed result set next for %ld: %s",
                      resultSetId,
                      strGetFromStatus(status));

        // Seek to the start
        status = rs->seek(0);
        BailIfFailed(status);

        // Get all 'numEntries' entries into a json object 'entries'
        status = rs->getNext(numEntries, &entries);
        BailIfFailed(status);

        dagNodeName[0] = '\0';
        status = getDagNodeName(dagNodeId, dagNodeName, sizeof(dagNodeName));
        // use empty table name instead of bailing on failure

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "updateOpDetailsFailureSummary for API \"%s\", "
                    "couldn't get failure table name due to: %s",
                    strGetFromXcalarApis(api),
                    strGetFromStatus(status));
            status = StatusOk;  // non-fatal
        }

        xSyslog(moduleName,
                XlogErr,
                "API \"%s\" on table \"%s\", column \"%s\", has failures!"
                " Breakdown below:",
                strGetFromXcalarApis(api),
                dagNodeName,
                failTabDescFieldName);

        //
        // Extract each of the 'numEntries' entries from 'entries' using the
        // failTabCountFieldName and failTabDescFieldName column names, into
        // numRowsFailed and failureDesc, respectively.
        //
        // Log each row from the failure table, and copy the row into the
        // opDetails struct (using opUdfError) upto the XcalarApiMaxFailures
        // limit)
        //
        for (unsigned ii = 0; ii < numEntries; ii++) {
            uint64_t numRowsFailed;

            // json_array_get does a borrowed reference for jsonRecord so no
            // need to decref it, and we don't hold on to it so no incref needed
            jsonRecord = json_array_get(entries, ii);

            if (jsonRecord == NULL) {
                xSyslog(moduleName,
                        XlogWarn,
                        "Failure table ends prematurely at %d instead of at "
                        "%lu",
                        ii,
                        numEntries - 1);
                break;
            }
            // json_object_get does a borrowed reference so no decref needed...
            jsonFailCnt = json_object_get(jsonRecord, failTabCountFieldName);
            numRowsFailed = json_integer_value(jsonFailCnt);
            numRowsFailedTotal += numRowsFailed;

            assert(numRowsFailed != 0);

            // since we now don't produce empty tables, and there shouldn't be
            // any tables with rows with a count of 0, numRowsFailed can't be 0.
            // If it is, log the event, and break out of the loop...otherwise
            // jsonFailInfo below could be NULL and crash usrnode

            if (numRowsFailed == 0) {
                break;
            }
            jsonFailInfo = json_object_get(jsonRecord, failTabDescFieldName);

            // Log all failures but copy to opUdfError only upto
            // XcalarApiMaxFailures; rest could be retrieved via the API on a
            // post-fix re-run
            xSyslog(moduleName,
                    XlogErr,
                    "\n\n\t%lu rows failed with: \"%s\"\n",
                    numRowsFailed,
                    json_string_value(jsonFailInfo));

            if (ii < XcalarApiMaxFailures) {
                struct FailureDesc *fDesc;
                struct FailureSummary *fSumm;
                size_t descSize;

                fSumm = &opUdfError->opFailureSummary[tabNum];
                fDesc = &fSumm->failureSummInfo[ii];
                fDesc->numRowsFailed = numRowsFailed;

                // The field name of eval string is used to name the failure
                // summary, to easily relate the failure to the eval string
                // (whose destination column name will be in the
                // failTabDescFieldName).  This will eventually be replaced
                // with a better interface!

                status = strStrlcpy(fSumm->failureSummName,
                                    failTabDescFieldName,
                                    sizeof(fSumm->failureSummName));

                if (status == StatusOverflow) {
                    xSyslog(moduleName,
                            XlogErr,
                            "failure %d summary name truncated in dag node",
                            ii);
                    status = StatusOk;  // non-fatal
                }
                // Now, insert the actual failure description into the
                // failure table
                descSize = strlcat(fDesc->failureDesc,
                                   json_string_value(jsonFailInfo),
                                   sizeof(fDesc->failureDesc));

                if (descSize >= sizeof(fDesc->failureDesc)) {
                    xSyslog(moduleName,
                            XlogErr,
                            "failure %d description truncated in dag node",
                            ii);
                    status = StatusOk;  // non-fatal
                }
            }
        }
        // Done with entries so decref it
        json_decref(entries);

        xSyslog(moduleName,
                XlogErr,
                "API %s on table %s has a total of %lu failures! Breakdown "
                "above",
                strGetFromXcalarApis(api),
                dagNodeName,
                numRowsFailedTotal);
    } break;
    default:
        assert(0);
    }

CommonExit:
    if (rs) {
        ResultSetMgr::get()->putResultSet(rs);
        rs = NULL;
    }
    if (resultSetId != XidInvalid) {
        ResultSetMgr::get()->freeResultSet(resultSetId);
        resultSetId = XidInvalid;
    }
    if (makeResultSetOutput != NULL) {
        memFree(makeResultSetOutput);
        makeResultSetOutput = NULL;
    }
    // Destroy the failure table now given that info from it has been extracted
    if (failureTabId != DagTypes::InvalidDagNodeId) {
        Status status2;
        status2 = getDagNodeName(failureTabId,
                                 failureTabName,
                                 sizeof(failureTabName));
        if (status2 != StatusOk) {
            // XXX: Should deleteDagNodeById(failureTabId, false) be used here?
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to drop node with ID %lu (can't get name)",
                    failureTabId);
            status2 = StatusOk;  // non-fatal
        } else {
            status2 = dropNode(failureTabName, SrcTable, NULL, NULL, true);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to drop node %s during map failure processing",
                        failureTabName);
                status2 = StatusOk;  // non-fatal
            }
        }
    }
    return status;
}

// called on operation completion to consolidate opDetails from each node. This
// call may be supplied a table (with ID failureTabId) which has summarized
// failure information for an operation that just completed. The summarized
// information must be extracted from the table, and copied (upto a limit) into
// the opDetails for the dag node (dagNodeId) and in addition, logged (all
// information is logged - no limit on this).

Status
Dag::updateOpDetails(DagTypes::NodeId dagNodeId,
                     DagTypes::NodeId *failureTabId[XcalarApiMaxFailureEvals],
                     XcalarApiUserId *userId,
                     XcalarApiSessionInfoInput *sessInfo)
{
    unsigned numActiveNodes = Config::get()->getActiveNodes();
    OpDetailWrapper *opDetailsWrapperArray = NULL;
    UpdateOpDetailsInput *input = NULL;
    Status status = StatusOk;
    XcalarApis api = XcalarApiUnknown;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    bool ftabPresent = false;

    Gvm::Payload *gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(UpdateOpDetailsInput));
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    input = (UpdateOpDetailsInput *) gPayload->buf;
    memZero(input, sizeof(UpdateOpDetailsInput));

    opDetailsWrapperArray =
        (OpDetailWrapper *) memAllocExt(sizeof(*opDetailsWrapperArray) *
                                            numActiveNodes,
                                        moduleName);
    if (opDetailsWrapperArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate opDetailsWrapperArray for "
                "%u nodes. Size required: %lu bytes",
                numActiveNodes,
                sizeof(*opDetailsWrapperArray) * numActiveNodes);
        status = StatusNoMem;
        goto CommonExit;
    }

    // NOTE: it may seem that opDetailsWrapperArray should be memZero'ed but
    // this isn't needed, since every element in the per-node opDetails struct
    // is initalized from the dag-node header's opDetails - see
    // DagLib::getXcalarApiOpStatusLocal, and
    // DagLib::getXcalarApiOpStatusComplete

    // XXX: why not just call getOpStats() here? The call to gatherOpStatus,
    // getDagNodeApi and processOpStatuses is what this routine does!
    status =
        this->gatherOpStatus(dagNodeId, opDetailsWrapperArray, numActiveNodes);
    BailIfFailed(status);

    status = this->getDagNodeApi(dagNodeId, &api);
    // always succeed by desgin
    assert(status == StatusOk);

    this->processOpStatuses(opDetailsWrapperArray,
                            numActiveNodes,
                            &input->opDetails,
                            api);

    // At this point, input->opDetails has the accumulated error stats but a
    // breakdown of this total error count (into counts per-unique-failure) is
    // now added into it, if there's a failure table available which has this
    // break-down - computed by the handler). This is done by the routine
    // updateOpDetailsFailureSummary() called below.

    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        if (failureTabId != NULL && failureTabId[ii] != NULL &&
            *failureTabId[ii] != DagTypes::InvalidDagNodeId) {
            ftabPresent = true;
            break;
        }
    }
    if (ftabPresent) {
        status = updateOpDetailsFailureSummary(api,
                                               dagNodeId,
                                               input,
                                               failureTabId,
                                               userId,
                                               sessInfo);
        if (status != StatusOk) {
            Status status2;
            char tableName[XcalarApiMaxTableNameLen + 1];
            tableName[0] = '\0';
            status2 =
                this->getDagNodeName(dagNodeId, tableName, sizeof(tableName));
            // if above fails, ignore error - empty table name is indicator
            xSyslog(moduleName,
                    XlogErr,
                    "updateOpDetails for API \"%s\", table \"%s\" "
                    "failed to extract failure summary due to \"%s\"!",
                    strGetFromXcalarApis(api),
                    tableName,
                    strGetFromStatus(status));
            status = StatusOk;  // non-fatal
        }
    }

    input->dagId = this->getId();
    input->dagNodeId = dagNodeId;

    assert(hdr_.graphType == DagTypes::WorkspaceGraph);

    gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                   DgUpdateOpDetails,
                   sizeof(UpdateOpDetailsInput));
    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:
    if (opDetailsWrapperArray != NULL) {
        memFree(opDetailsWrapperArray);
        opDetailsWrapperArray = NULL;
    }
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
Dag::updateOpDetailsFailureSummary(
    XcalarApis api,
    DagTypes::NodeId dagNodeId,
    UpdateOpDetailsInput *input,
    DagTypes::NodeId *failureTabId[XcalarApiMaxFailureEvals],
    XcalarApiUserId *userId,
    XcalarApiSessionInfoInput *sessInfo)
{
    Status status = StatusOk;
    Status firstFailedStatus = StatusOk;

    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        if (*failureTabId[ii] != DagTypes::InvalidDagNodeId) {
            status = updateOpDetailsOneFailSummTab(api,
                                                   dagNodeId,
                                                   input,
                                                   *failureTabId[ii],
                                                   ii,
                                                   userId,
                                                   sessInfo);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "updateOpDetailsOneFailSummTab failed for "
                        "eval num %d: %s",
                        ii,
                        strGetFromStatus(status));
                // non-fatal; continue to next eval
                if (firstFailedStatus == StatusOk) {
                    firstFailedStatus = status;
                }
                status = StatusOk;
            }
        } else {
            break;
        }
    }
    if (firstFailedStatus != StatusOk) {
        status = firstFailedStatus;
    }
    return status;
}

Status
Dag::setScalarResult(DagTypes::NodeId dagNodeId, Scalar *scalarIn)
{
    Status status;
    SetScalarResultInput *input = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = NULL;

    size_t inputSize = sizeof(*input) + scalarIn->getUsedSize();
    gPayload = (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + inputSize);
    BailIfNull(gPayload);

    nodeStatus = new (std::nothrow) Status[nodeCount];
    BailIfNull(nodeStatus);

    gPayload->init(DagNodeGvm::get()->getGvmIndex(),
                   DgSetScalarResult,
                   inputSize);
    input = (SetScalarResultInput *) gPayload->buf;
    input->dagId = this->getId();
    input->dagNodeId = dagNodeId;
    memcpy(input->scalarIn, scalarIn, scalarIn->getUsedSize());

    assert(hdr_.graphType == DagTypes::WorkspaceGraph);

    status = Gvm::get()->invoke(gPayload, nodeStatus);
    if (status == StatusOk) {
        for (unsigned ii = 0; ii < nodeCount; ii++) {
            if (nodeStatus[ii] != StatusOk) {
                status = nodeStatus[ii];
                break;
            }
        }
    }

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    return status;
}

Status
Dag::getPerNodeOpStats(DagTypes::NodeId dagNodeId,
                       XcalarApiOutput **outputOut,
                       size_t *outputSizeOut)
{
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;
    XcalarApiPerNodeOpStats *perNodeopStats;
    uint64_t numNodes = Config::get()->getActiveNodes();
    Status status = StatusOk;
    OpDetailWrapper *opDetailArrayWrapper = NULL;
    XcalarApis api = XcalarApiUnknown;

    opDetailArrayWrapper = (OpDetailWrapper *)
        memAllocExt(sizeof(*opDetailArrayWrapper) * numNodes, moduleName);
    if (opDetailArrayWrapper == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate opDetailArrayWrapper for "
                "%lu nodes. Size required: %lu bytes",
                numNodes,
                sizeof(*opDetailArrayWrapper) * numNodes);
        status = StatusNoMem;
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(*perNodeopStats) +
                 (numNodes * sizeof(perNodeopStats->nodeOpStats[0]));
    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);

    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = this->gatherOpStatus(dagNodeId, opDetailArrayWrapper, numNodes);
    BailIfFailed(status);

    status = this->getDagNodeApi(dagNodeId, &api);
    assert(status == StatusOk);

    perNodeopStats = &output->outputResult.perNodeOpStatsOutput;
    perNodeopStats->numNodes = numNodes;
    perNodeopStats->api = api;

    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        perNodeopStats->nodeOpStats[ii].nodeId = ii;
        perNodeopStats->nodeOpStats[ii].status =
            opDetailArrayWrapper[ii].status.code();

        if (perNodeopStats->nodeOpStats[ii].status != StatusOk.code()) {
            continue;
        }

        memcpy(&perNodeopStats->nodeOpStats[ii].opDetails,
               &opDetailArrayWrapper[ii].opDetails,
               sizeof(perNodeopStats->nodeOpStats[ii].opDetails));
    }

CommonExit:
    if (opDetailArrayWrapper != NULL) {
        memFree(opDetailArrayWrapper);
        opDetailArrayWrapper = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            outputSize = 0;
        }
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
Dag::getOpProgress(DagTypes::NodeId dagNodeId,
                   uint64_t &numWorkCompleted,
                   uint64_t &numWorkTotal)
{
    Status status;
    XcalarApiOutput *output = NULL;
    size_t outputSize;
    DgDagState state;

    status = getDagNodeState(dagNodeId, &state);
    BailIfFailed(status);

    status = getOpStats(dagNodeId, state, &output, &outputSize);
    if (status != StatusOk) {
        goto CommonExit;
    }

    numWorkCompleted =
        output->outputResult.opStatsOutput.opDetails.numWorkCompleted;
    numWorkTotal = output->outputResult.opStatsOutput.opDetails.numWorkTotal;

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    return status;
}

Status
Dag::getOpStats(DagTypes::NodeId dagNodeId,
                DgDagState state,
                XcalarApiOutput **outputOut,
                size_t *outputSizeOut)
{
    Status status = StatusOk;
    XcalarApis api = XcalarApiUnknown;
    XcalarApiOutput *output = NULL;
    XcalarApiOpStatsOut *opStatsOutput;
    size_t outputSize = 0;
    OpDetailWrapper *opDetailsWrapperArray = NULL;
    outputSize = XcalarApiSizeOfOutput(*opStatsOutput);
    const unsigned numActiveNodes = Config::get()->getActiveNodes();

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    opDetailsWrapperArray =
        (OpDetailWrapper *) memAllocExt(sizeof(*opDetailsWrapperArray) *
                                            numActiveNodes,
                                        moduleName);
    if (opDetailsWrapperArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate opDetailsWrapperArray for "
                "%u nodes. Size required: %lu bytes",
                numActiveNodes,
                sizeof(*opDetailsWrapperArray) * numActiveNodes);
        status = StatusNoMem;
        goto CommonExit;
    }

    // NOTE: it may seem that opDetailsWrapperArray should be memZero'ed but
    // this isn't needed, since every element in the per-node opDetails struct
    // is initalized from the dag-node header's opDetails - see
    // DagLib::getXcalarApiOpStatusLocal, and
    // DagLib::getXcalarApiOpStatusComplete (invoked from gatherOpStatus())

    opStatsOutput = &output->outputResult.opStatsOutput;

    if (state == DgDagStateReady || state == DgDagStateDropped ||
        state == DgDagStateCleaned) {
        // The stats should come from dag node header
        DagNodeTypes::Node *dagNode = NULL;

        lock_.lock();

        status = this->lookupNodeById(dagNodeId, &dagNode);
        if (status != StatusOk) {
            lock_.unlock();
            goto CommonExit;
        }

        opStatsOutput->api = dagNode->dagNodeHdr.apiDagNodeHdr.api;
        memcpy(&opStatsOutput->opDetails,
               &dagNode->dagNodeHdr.opDetails,
               sizeof(OpDetails));

        lock_.unlock();
    } else {
        assert(state != DgDagStateUnknown);

        // The stats come from each node
        status = this->gatherOpStatus(dagNodeId,
                                      opDetailsWrapperArray,
                                      numActiveNodes);
        if (status != StatusOk) {
            goto CommonExit;
        }

        status = this->getDagNodeApi(dagNodeId, &api);
        BailIfFailed(status);

        processOpStatuses(opDetailsWrapperArray,
                          numActiveNodes,
                          &opStatsOutput->opDetails,
                          api);

        opStatsOutput->api = api;
    }

CommonExit:
    if (opDetailsWrapperArray != NULL) {
        memFree(opDetailsWrapperArray);
        opDetailsWrapperArray = NULL;
    }

    if (status != StatusOk) {
        if (output != NULL) {
            memFree(output);
            output = NULL;
            outputSize = 0;
        }
    }

    *outputOut = output;
    *outputSizeOut = outputSize;

    return status;
}

Status
Dag::getDagNodeState(DagTypes::NodeId dagNodeId, DgDagState *state)
{
    Status status;
    DagNodeTypes::Node *dagNode;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *state = dagNode->dagNodeHdr.apiDagNodeHdr.state;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::getDagNodeSourceSize(DagTypes::NodeId dagNodeId, size_t *sizeOut)
{
    Status status;
    DagNodeTypes::Node *dagNode;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *sizeOut = dagNode->dagNodeHdr.opDetails.numRowsTotal;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::getDagNodeStateAndRef(DagTypes::NodeId dagNodeId, DgDagState *state)
{
    Status status = this->getDagNodeRefById(dagNodeId);

    if (status != StatusOk) {
        *state = DgDagStateUnknown;
        return status;
    }

    // guaranteed to succeed since we have a ref on the dag node
    verifyOk(this->getDagNodeState(dagNodeId, state));

    return StatusOk;
}

Status
Dag::cancelOp(char *name)
{
    DagTypes::NodeId dagNodeId;
    Status status;
    DgDagState state = DgDagStateUnknown;
    bool decRef = false;
    DagNodeOpInfo cancelOpInfo;
    unsigned numNodes = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    unsigned numFinished = 0;
    unsigned ii;
    MsgMgr *msgMgr = MsgMgr::get();
    MsgEphemeral eph;
    TableNsMgr::TableHandleTrack handleTrack;
    TableNsMgr *tnsMgr = TableNsMgr::get();

    nodeStatus = new (std::nothrow) Status[numNodes];
    BailIfNull(nodeStatus);

    status = this->getDagNodeId(name, TableScope::LocalOnly, &dagNodeId);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = this->getDagNodeStateAndRef(dagNodeId, &state);
    if (status != StatusOk) {
        goto CommonExit;
    }

    decRef = true;

    if (state != DgDagStateProcessing && state != DgDagStateQueued &&
        state != DgDagStateCreated) {
        assert(state != DgDagStateUnknown);
        status = StatusOperationHasFinished;

        goto CommonExit;
    }

    status = getTableIdFromNodeId(dagNodeId, &handleTrack.tableId);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed getTableIdFromNodeId for dagNode %lu: %s",
                    dagNodeId,
                    strGetFromStatus(status));

    if (hdr_.graphType == DagTypes::WorkspaceGraph &&
        tnsMgr->isTableIdValid(handleTrack.tableId)) {
        // Cancel is able to grab the lock b/c the Operator grabs
        // ReadSharedWriteExclWriterr lock
        status = tnsMgr->openHandleToNs(&sessionContainer_,
                                        handleTrack.tableId,
                                        LibNsTypes::ReadSharedWriteExclReader,
                                        &handleTrack.tableHandle,
                                        TableNsMgr::OpenSleepInUsecs);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open handle to table %ld: %s",
                        handleTrack.tableId,
                        strGetFromStatus(status));
        handleTrack.tableHandleValid = true;
    }

    cancelOpInfo.dagId = this->getId();
    cancelOpInfo.dagNodeId = dagNodeId;

    msgMgr->twoPcEphemeralInit(&eph,
                               &cancelOpInfo,
                               sizeof(cancelOpInfo),
                               0,
                               TwoPcSlowPath,
                               TwoPcCallId::Msg2pcXcalarApiCancel1,
                               nodeStatus,
                               TwoPcMemCopyInput);

    TwoPcHandle twoPcHandle;
    status = msgMgr->twoPc(&twoPcHandle,
                           MsgTypeId::Msg2pcXcalarApiCancel,
                           TwoPcDoNotReturnHandle,
                           &eph,
                           (MsgSendRecvFlags)(MsgSendHdrPlusPayload |
                                              MsgRecvHdrOnly),
                           TwoPcSyncCmd,
                           TwoPcAllNodes,
                           TwoPcIgnoreNodeId,
                           TwoPcClassNonNested);
    BailIfFailed(status);
    assert(!twoPcHandle.twoPcHandle);

    for (ii = 0; ii < numNodes; ++ii) {
        if (nodeStatus[ii] == StatusOk) {
            assert(status == StatusOk);
            continue;
        }

        if (nodeStatus[ii] == StatusOperationHasFinished) {
            numFinished++;
            continue;
        }

        if (nodeStatus[ii] == StatusDagNodeNotFound) {
            // Not a big deal, because we could just be racing
            // with createDagNode.
            continue;
        }

        status = nodeStatus[ii];
        break;
    }

    if (numFinished == numNodes) {
        assert(status == StatusOk);
        status = StatusOperationHasFinished;
    }
CommonExit:
    if (decRef) {
        this->putDagNodeRefById(dagNodeId);
    }

    if (handleTrack.tableHandleValid) {
        tnsMgr->closeHandleToNs(&handleTrack.tableHandle);
        handleTrack.tableHandleValid = false;
    }

    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    return status;
}

// XXX: SrcTable and SrcConstant are treated as the same and there is no
// seperated delete SrcConstatn function
Status
Dag::cleanAndDeleteDagNode(const char *targetName,
                           SourceType srcType,
                           bool dropOnly,
                           uint64_t *numRefsOut,
                           DagTypes::DagRef **refsOut)
{
    Status status;
    DagTypes::NodeId dagNodeId = 0ULL;
    DagNodeTypes::Node *dagNode = NULL;
    bool locked = false;
    bool dagNodeDropped = false;
    DgDagState dagNodeState;
    XcalarApis dagNodeApi = XcalarApiUnknown;
    Dataset *ds = Dataset::get();
    XdbId xdbId;
    bool dropXdb = true;

    status = this->getDagNodeId(targetName, TableScope::LocalOnly, &dagNodeId);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = this->getDagNodeApi(dagNodeId, &dagNodeApi);
    assert(status == StatusOk);

    // != serves as XOR
    if ((srcType == SrcDataset) != (dagNodeApi == XcalarApiBulkLoad)) {
        status = StatusDagNodeNotFound;
        goto CommonExit;
    }

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    BailIfFailed(status);

    xdbId = dagNode->xdbId;

    if (dagNodeApi == XcalarApiSynthesize) {
        // synthesize sometimes reuses xdbs
        // check to see if srcXdbId is equal to dst
        dropXdb = !(dagNode->dagNodeHdr.apiInput->synthesizeInput.source.xid ==
                    xdbId);
    }

    dagNodeState = dagNode->dagNodeHdr.apiDagNodeHdr.state;

    if (dagNodeState == DgDagStateCreated || dagNodeState == DgDagStateReady ||
        dagNodeState == DgDagStateError || dagNodeState == DgDagStateCleaned ||
        dagNodeState == DgDagStateArchived ||
        dagNodeState == DgDagStateArchiveError ||
        dagNodeState == DgDagStateUnknown) {
        // XXX When we created this dagNode, if this was a load of a dataset,
        // we open a handle to that dataset. So if we don't need that handle
        // anymore, (i.e. if we're deleting this dagNode for good and this
        // dagNode has no descendants), then we need to close that handle. We
        // really should be incrementing refCount on ALL dagNodes that reference
        // this dataset (e.g. via fatpointers), rather than just have this one.
        if (dagNodeState == DgDagStateReady ||
            dagNodeState == DgDagStateArchived ||
            dagNodeState == DgDagStateArchiveError) {
            void *dagNodeContext = NULL;

            status = this->checkBeforeDrop(dagNode);
            BailIfFailed(status);

            dagNodeContext = dagNode->context;
            // Do not NULLify dagNode->context. This is because the thread with
            // dagNodeContext might not be the thread to have successfully
            // dropped the node, that's why we need all threads in the race
            // to have access to dagNodeContext

            lock_.unlock();
            locked = false;
            // As soon as we release the lock, dagNode could be stale
            // and we can't look at it again, because of racing destroy dagNodes
            dagNode = NULL;

            // We need to deleteDagNodeById before we do the datasetDrop or
            // xdbDrop because deleteDagNodeById changes the dagNodeState to
            // dropped, to ensure that no one else will look at the underlying
            // dataset or XDB.
            switch (srcType) {
            case SrcDataset: {
                status = this->deleteDagNodeById(dagNodeId, dropOnly);
                BailIfFailed(status);

                // The winner of the race comes here. Everyone else in the race
                // to drop the same node would have bailed above
                DatasetRefHandle *dsRefHandle =
                    (DatasetRefHandle *) dagNodeContext;
                assert(dsRefHandle != NULL);

                // Drop ref owned by our dagNode. For the dataset to be
                // completely destroyed, user must call drop dataset. Deleting
                // a dagNode can happen when a graph is being  destroyed (e.g.
                // during a session teardown), that's not an explicit call by
                // the user to drop the dataset.
                Status status2 = ds->closeHandleToDataset(dsRefHandle);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to close handle to dataset %ld: %s",
                            dsRefHandle->nsHandle.nsId,
                            strGetFromStatus(status2));
                }

                memFree(dsRefHandle);
                dsRefHandle = NULL;
                dagNodeDropped = true;

                if (strncmp(targetName,
                            XcalarApiLrqPrefix,
                            XcalarApiLrqPrefixLen) == 0) {
                    XdbMgr::get()->xdbDrop(xdbId);
                }
                break;
            }
            case SrcExport:
                // fall through
            case SrcConstant:
                // No Op
                break;
            case SrcTable:
                status = this->deleteDagNodeById(dagNodeId, dropOnly);
                BailIfFailed(status);
                dagNodeDropped = true;

                if (dropXdb) {
                    XdbMgr::get()->xdbDrop(xdbId);
                }
                break;
            default:
                xSyslog(moduleName,
                        XlogErr,
                        "Unknown srcType (%u) provided",
                        srcType);
                assert(0);
            }
        }

        if (!dagNodeDropped) {
            if (locked) {
                lock_.unlock();
                locked = false;
            }

            status = this->deleteDagNodeById(dagNodeId, dropOnly);
            BailIfFailed(status);
        }
    } else {
        xSyslog(moduleName,
                XlogDebug,
                "can not delete node %lu at state: %s",
                dagNodeId,
                strGetFromDgDagState(dagNodeState));
        status = StatusDgDeleteOperationNotPermitted;
        goto CommonExit;
    }

CommonExit:
    if (locked) {
        assert(status != StatusOk ||
               this->lookupNodeByName(targetName,
                                      &dagNode,
                                      TableScope::LocalOnly) != StatusOk);
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::dropNode(const char *targetName,
              SourceType srcType,
              uint64_t *numRefsOut,
              DagTypes::DagRef **refsOut,
              bool deleteCompletely)
{
    Status status;
    XcalarApiUdfContainer *sessionContainer = getSessionContainer();
    bool delMode = deleteCompletely ? DagTypes::DeleteNodeCompletely
                                    : DagTypes::RemoveNodeFromNamespace;
    TableNsMgr::TableId tableId = TableNsMgr::InvalidTableId;

    if (numRefsOut) {
        *numRefsOut = 0;
    }
    if (refsOut) {
        *refsOut = NULL;
    }

    status =
        getDagNodeIds(targetName, TableScope::LocalOnly, NULL, NULL, &tableId);
    BailIfFailed(status);

    if (TableNsMgr::get()->isTableIdValid(tableId)) {
        UserMgr::get()->untrackResultsets(tableId,
                                          &sessionContainer->userId,
                                          &sessionContainer->sessionInfo);
    }

    status = this->cleanAndDeleteDagNode(targetName,
                                         srcType,
                                         delMode,
                                         numRefsOut,
                                         refsOut);

CommonExit:
    return status;
}

Status
Dag::archiveNode(const char *nodeName)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode = NULL;
    DgDagState dagNodeState;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = lookupNodeByName(nodeName, &dagNode, TableScope::LocalOnly);
    BailIfFailed(status);

    if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiExport ||
        dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiAggregate ||
        dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiBulkLoad) {
        xSyslog(moduleName,
                XlogErr,
                "can archive node %s with api: %s",
                nodeName,
                strGetFromXcalarApis(dagNode->dagNodeHdr.apiDagNodeHdr.api));
        status = StatusOpNotSupp;
        goto CommonExit;
    }

    dagNodeState = dagNode->dagNodeHdr.apiDagNodeHdr.state;

    // can only archive previously attempted archives and ready nodes
    if (dagNodeState != DgDagStateArchiveError &&
        dagNodeState != DgDagStateReady) {
        xSyslog(moduleName,
                XlogErr,
                "can archive node %s at state: %s",
                nodeName,
                strGetFromDgDagState(dagNodeState));
        status = StatusOpNotSupp;
        goto CommonExit;
    }

    status = this->checkBeforeDrop(dagNode);
    BailIfFailed(status);

    dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateArchiving;
    lock_.unlock();
    locked = false;

    // should not hold dag lock while serializing
    status = XdbMgr::get()->xdbSerDes(dagNode->xdbId, true);

    lock_.lock();
    locked = true;

    if (status == StatusOk) {
        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateArchived;
    } else {
        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateArchiveError;
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::unarchiveNode(const char *nodeName)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode = NULL;
    DgDagState dagNodeState;
    bool locked = false;

    lock_.lock();
    locked = true;

    status = lookupNodeByName(nodeName, &dagNode, TableScope::LocalOnly);
    BailIfFailed(status);

    dagNodeState = dagNode->dagNodeHdr.apiDagNodeHdr.state;

    // can only archive previously attempted archives and ready nodes
    if (dagNodeState != DgDagStateArchiveError &&
        dagNodeState != DgDagStateArchived) {
        status = StatusOpNotSupp;
        goto CommonExit;
    }

    status = this->checkBeforeDrop(dagNode);
    BailIfFailed(status);

    dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateArchiving;
    lock_.unlock();
    locked = false;

    // should not hold dag lock while deserializing
    status = XdbMgr::get()->xdbSerDes(dagNode->xdbId, false);

    lock_.lock();
    locked = true;

    if (status == StatusOk) {
        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateReady;
    } else {
        dagNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateArchiveError;
    }

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

// List all the nodes if (apiArray[0] == XcalarApiUnknown)
Status
Dag::listAvailableNodes(const char *dagNamePattern,
                        XcalarApiDagOutput **listDagsOut,
                        size_t *outputSizeOut,
                        unsigned numApis,
                        XcalarApis *apiArray)
{
    Status status;
    DagNodeTypes::Node *dagNode;
    XcalarApiDagOutput *listDag = NULL;
    void **dagNodesSelected = NULL;
    size_t outputSize;
    bool locked = false;
    size_t apiInputSizeRequired = 0, parentSize = 0, childrenSize = 0;
    unsigned numNodes = 0;
    size_t nameLenOut;

    *listDagsOut = NULL;
    *outputSizeOut = 0;

    lock_.lock();
    locked = true;

    dagNodesSelected =
        (void **) memAllocExt(sizeof(*dagNodesSelected) * hdr_.numNodes,
                              moduleName);
    if (dagNodesSelected == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    apiInputSizeRequired = 0;
    // XXX: All these to go away when we start using libns to manage names
    // Don't show xcalarBroadcast tables in list output

    // check if the dagNamePattern contains any regex
    status = this->isValidNodeName(dagNamePattern, &nameLenOut);
    if (status == StatusOk) {
        // nodeName without regex
        status = this->lookupNodeByName(dagNamePattern,
                                        &dagNode,
                                        TableScope::LocalOnly);
        if (status == StatusOk &&
            (apiArray[0] == XcalarApiUnknown ||
             foundApi(dagNode->dagNodeHdr.apiDagNodeHdr.api,
                      numApis,
                      apiArray))) {
            apiInputSizeRequired += dagNode->dagNodeHdr.apiDagNodeHdr.inputSize;
            parentSize += dagNode->numParent * sizeof(XcalarApiDagNodeId);
            childrenSize += dagNode->numChild * sizeof(XcalarApiDagNodeId);
            dagNodesSelected[numNodes] = dagNode;
            numNodes++;
        }
    } else {
        for (auto iter = nameHashTable_.begin(); (dagNode = iter.get()) != NULL;
             iter.next()) {
            dagNode = iter.get();
            if (strMatch(dagNamePattern,
                         dagNode->dagNodeHdr.apiDagNodeHdr.name) &&
                !strstr(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        XcalarApiBroadcastTablePrefix)) {
                if (apiArray[0] == XcalarApiUnknown ||
                    foundApi(dagNode->dagNodeHdr.apiDagNodeHdr.api,
                             numApis,
                             apiArray)) {
                    apiInputSizeRequired +=
                        dagNode->dagNodeHdr.apiDagNodeHdr.inputSize;
                    parentSize +=
                        dagNode->numParent * sizeof(XcalarApiDagNodeId);
                    childrenSize +=
                        dagNode->numChild * sizeof(XcalarApiDagNodeId);

                    dagNodesSelected[numNodes] = dagNode;
                    numNodes++;
                }
            }
        }
    }
    // if not able to find nodes with the given pattern,
    // status can be still OK
    status = StatusOk;

    outputSize = this->sizeOfDagOutput(numNodes,
                                       apiInputSizeRequired,
                                       parentSize,
                                       childrenSize);
    listDag = (XcalarApiDagOutput *) memAllocExt(outputSize, moduleName);
    if (listDag == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = DagLib::get()->copyDagNodesToDagOutput(listDag,
                                                    outputSize,
                                                    dagNodesSelected,
                                                    numNodes,
                                                    &udfContainer_,
                                                    DagNodeTypeDagNode);
    BailIfFailed(status);
    assert(listDag->numNodes == numNodes);

    *listDagsOut = listDag;
    *outputSizeOut = outputSize;

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (dagNodesSelected != NULL) {
        memFree(dagNodesSelected);
        dagNodesSelected = NULL;
    }

    if (status != StatusOk) {
        if (listDag != NULL) {
            memFree(listDag);
        }
    }

    return status;
}

Status
Dag::getDagNode(DagTypes::NodeId dagNodeId, XcalarApiDagNode **apiDagNodeOut)
{
    DagNodeTypes::Node *dagNode;
    Status status;
    bool locked = false;
    XcalarApiDagNode *apiDagNode = NULL;
    size_t bufSize;
    size_t bytesCopied;

    assert(apiDagNodeOut != NULL);
    assert(dagNodeId != DagTypes::InvalidDagNodeId);

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    bufSize =
        DagLib::get()->sizeOfXcalarApiDagNode(dagNode->dagNodeHdr.apiDagNodeHdr
                                                  .inputSize,
                                              dagNode->numParent,
                                              dagNode->numChild);

    apiDagNode = (XcalarApiDagNode *) memAllocExt(bufSize, moduleName);
    if (apiDagNode == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = DagLib::get()->copyDagNodeToXcalarApiDagNode(apiDagNode,
                                                          bufSize,
                                                          dagNode,
                                                          &udfContainer_,
                                                          &bytesCopied);
    assert(status == StatusOk);
    *apiDagNodeOut = apiDagNode;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (status != StatusOk) {
        if (apiDagNode != NULL) {
            memFree(apiDagNode);
            apiDagNode = NULL;
        }
    }

    return status;
}

Status
Dag::getDatasetIdFromDagNodeId(DagTypes::NodeId dagNodeId,
                               DsDatasetId *datasetIdOut)
{
    assert(datasetIdOut != NULL);

    char *datasetName;
    XcalarApiDagNode *dagNodeOut = NULL;
    Status status = this->getDagNode(dagNodeId, &dagNodeOut);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNodeOut->hdr.api == XcalarApiBulkLoad) {
        datasetName = dagNodeOut->input->loadInput.datasetName;
    } else if (dagNodeOut->hdr.api == XcalarApiSynthesize) {
        datasetName = dagNodeOut->input->synthesizeInput.source.name;
    } else {
        assert(0);
    }

    *datasetIdOut = Dataset::get()->getDatasetIdFromName(datasetName, &status);

CommonExit:
    if (dagNodeOut != NULL) {
        memFree(dagNodeOut);
        dagNodeOut = NULL;
    }

    return status;
}

Status
Dag::listDagNodeInfo(const char *namePattern,
                     XcalarApiOutput **outputOut,
                     size_t *outputSizeOut,
                     SourceType srcType,
                     XcalarApiUserId *user)
{
    unsigned ii;
    Status status = StatusOk;
    XcalarApiOutput *output = NULL;
    XcalarApiListDagNodesOutput *listTables = NULL;
    size_t outputSize = 0;
    XcalarApiDagOutput *listDagsOut = NULL;
    size_t listDagOutputSize;

    // XXX:change the srcType to bitmap
    switch (srcType) {
    case SrcUnknown: {
        XcalarApis api = XcalarApiUnknown;
        status = this->listAvailableNodes(namePattern,
                                          &listDagsOut,
                                          &listDagOutputSize,
                                          1,
                                          &api);
        break;
    }

    case SrcDataset: {
        XcalarApis api = XcalarApiBulkLoad;
        status = this->listAvailableNodes(namePattern,
                                          &listDagsOut,
                                          &listDagOutputSize,
                                          1,
                                          &api);
        break;
    }

    case SrcConstant: {
        XcalarApis api = XcalarApiAggregate;
        status = this->listAvailableNodes(namePattern,
                                          &listDagsOut,
                                          &listDagOutputSize,
                                          1,
                                          &api);
        break;
    }

    case SrcTable: {
        XcalarApis apiArray[] = {XcalarApiIndex,
                                 XcalarApiMap,
                                 XcalarApiFilter,
                                 XcalarApiGroupBy,
                                 XcalarApiJoin,
                                 XcalarApiProject,
                                 XcalarApiGetRowNum,
                                 XcalarApiExecuteRetina,
                                 XcalarApiSynthesize,
                                 XcalarApiUnion,
                                 XcalarApiSelect};

        status = this->listAvailableNodes(namePattern,
                                          &listDagsOut,
                                          &listDagOutputSize,
                                          (unsigned) ArrayLen(apiArray),
                                          apiArray);
        break;
    }

    case SrcExport: {
        XcalarApis api = XcalarApiExport;
        status = this->listAvailableNodes(namePattern,
                                          &listDagsOut,
                                          &listDagOutputSize,
                                          1,
                                          &api);
        break;
    }

    default:
        assert(0);
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    outputSize = XcalarApiSizeOfOutput(*listTables) +
                 (listDagsOut->numNodes * sizeof(listTables->nodeInfo[0]));

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    listTables = &output->outputResult.listNodesOutput;

    for (ii = 0; ii < listDagsOut->numNodes; ++ii) {
        listTables->nodeInfo[ii].dagNodeId =
            listDagsOut->node[ii]->hdr.dagNodeId;
        listTables->nodeInfo[ii].state = listDagsOut->node[ii]->hdr.state;
        assertStatic(sizeof(listTables->nodeInfo[ii].name) ==
                     sizeof(listDagsOut->node[ii]->hdr.name));
        size_t ret = strlcpy(listTables->nodeInfo[ii].name,
                             listDagsOut->node[ii]->hdr.name,
                             sizeof(listTables->nodeInfo[ii].name));
        assert(ret <= sizeof(listTables->nodeInfo[ii].name));

        listTables->nodeInfo[ii].api = listDagsOut->node[ii]->hdr.api;

        if (listDagsOut->node[ii]->hdr.state == DgDagStateReady ||
            listDagsOut->node[ii]->hdr.state == DgDagStateCreated) {
            listTables->nodeInfo[ii].size =
                listDagsOut->node[ii]->localData.sizeTotal;
        } else {
            listTables->nodeInfo[ii].size = 0;
        }
        listTables->nodeInfo[ii].pinned = listDagsOut->node[ii]->hdr.pinned;
    }

    listTables->numNodes = listDagsOut->numNodes;
    status = StatusOk;

CommonExit:
    if (listDagsOut != NULL) {
        memFree(listDagsOut);
    }

    if (status == StatusOk) {
        assert(output != NULL);
        *outputOut = output;
        *outputSizeOut = outputSize;
    } else {
        if (output != NULL) {
            memFree(output);
            output = NULL;
        }
    }

    return status;
}

Status
Dag::bulkDropNodes(const char *namePattern,
                   XcalarApiOutput **outputOut,
                   size_t *outputSizeOut,
                   SourceType srcType,
                   XcalarApiUserId *user,
                   bool deleteCompletely)
{
    struct StatusAndRefs {
        Status status;
        uint64_t numRefs;
        DagTypes::DagRef *refList;
    };

    XcalarApiListDagNodesOutput *listNodesOutput = NULL;
    XcalarApiOutput *output = NULL;
    XcalarApiOutput *outputForListTable = NULL;
    size_t outputSize = 0;
    XcalarApiDeleteDagNodesOutput *deleteNodesStatus = NULL;
    size_t listTablesOutputSize;
    unsigned ii;
    Status status;
    Status savedStatus = StatusOk;
    StatusAndRefs *refs = NULL;
    size_t refSize = 0;

    xSyslog(moduleName,
            XlogDebug,
            "Dropping DAG node(s) with pattern %s",
            namePattern);

    if (strstr(namePattern, "*") != NULL) {
        status = this->listDagNodeInfo(namePattern,
                                       &outputForListTable,
                                       &listTablesOutputSize,
                                       srcType,
                                       user);
        if (status != StatusOk) {
            goto CommonExit;
        }
        assert(listTablesOutputSize > 0);
        assert(outputForListTable != NULL);
        listNodesOutput = &outputForListTable->outputResult.listNodesOutput;
    } else {
        // Without the wild card, let's not even bother looking up
        // the nodes, which could take a while, and instead just straight up
        // attempt to delete it
        listTablesOutputSize = XcalarApiSizeOfOutput(*listNodesOutput) +
                               sizeof(listNodesOutput->nodeInfo[0]);
        outputForListTable =
            (XcalarApiOutput *) memAllocExt(listTablesOutputSize, moduleName);
        if (outputForListTable == NULL) {
            status = StatusNoMem;
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate outputForListTable "
                    "(Required size: %lu bytes)",
                    listTablesOutputSize);
            goto CommonExit;
        }
        listNodesOutput = &outputForListTable->outputResult.listNodesOutput;
        listNodesOutput->numNodes = 1;
        strlcpy(listNodesOutput->nodeInfo[0].name,
                namePattern,
                sizeof(listNodesOutput->nodeInfo[0].name));
        // initialising to false and it will not effect the deletetion
        listNodesOutput->nodeInfo[0].pinned = false;
    }

    refs =
        (StatusAndRefs *) memAllocExt(listNodesOutput->numNodes * sizeof(*refs),
                                      moduleName);
    BailIfNull(refs);
    memZero(refs, listNodesOutput->numNodes * sizeof(*refs));

    for (ii = 0; ii < listNodesOutput->numNodes; ii++) {
        refs[ii].status = dropNode(listNodesOutput->nodeInfo[ii].name,
                                   srcType,
                                   &refs[ii].numRefs,
                                   &refs[ii].refList,
                                   deleteCompletely);
        if (refs[ii].status != StatusOk) {
            refSize += refs[ii].numRefs * sizeof(*refs[ii].refList);
            // don't mask other error status codes with StatusTablePinned
            if (savedStatus == StatusOk || savedStatus == StatusTablePinned) {
                savedStatus = refs[ii].status;
            }
        }
    }

    outputSize =
        XcalarApiSizeOfOutput(*deleteNodesStatus) + refSize +
        (listNodesOutput->numNodes * sizeof(deleteNodesStatus->statuses[0]));

    output = (XcalarApiOutput *) memAllocExt(outputSize, moduleName);
    if (output == NULL) {
        status = StatusNoMem;
        memFree(refs);
        refs = NULL;
        goto CommonExit;
    }

    deleteNodesStatus = &output->outputResult.deleteDagNodesOutput;
    deleteNodesStatus->numNodes = listNodesOutput->numNodes;

    // since we have a variable length of variable size structs, we need
    // a cursor to keep track of the correct memory offset
    uint8_t *cursor;
    cursor = (uint8_t *) &deleteNodesStatus->statuses[0];
    for (ii = 0; ii < deleteNodesStatus->numNodes; ii++) {
        XcalarApiDeleteDagNodeStatus *nodeStatus =
            (XcalarApiDeleteDagNodeStatus *) cursor;
        size_t nodeStatusSize = sizeof(*nodeStatus);

        verifyOk(strStrlcpy(nodeStatus->nodeInfo.name,
                            listNodesOutput->nodeInfo[ii].name,
                            sizeof(nodeStatus->nodeInfo.name)));

        nodeStatus->nodeInfo.pinned = listNodesOutput->nodeInfo[ii].pinned;

        nodeStatus->status = refs[ii].status.code();
        if (refs[ii].status == StatusOk) {
            nodeStatus->numRefs = 0;
            if (refs[ii].refList != NULL) {
                memFree(refs[ii].refList);
            }
        } else {
            if (refs[ii].refList != NULL) {
                size_t refListSize =
                    refs[ii].numRefs * sizeof(*refs[ii].refList);

                nodeStatus->numRefs = refs[ii].numRefs;
                memcpy(nodeStatus->refs, refs[ii].refList, refListSize);
                cursor += refListSize;

                memFree(refs[ii].refList);
            } else {
                nodeStatus->numRefs = 0;
            }
        }

        cursor += nodeStatusSize;
    }

    status = StatusOk;

CommonExit:
    *outputSizeOut = outputSize;
    *outputOut = output;

    if (outputForListTable != NULL) {
        memFree(outputForListTable);
        outputForListTable = NULL;
        listNodesOutput = NULL;
        listTablesOutputSize = 0;
    }

    if (refs != NULL) {
        memFree(refs);
        refs = NULL;
    }

    if (status == StatusOk) {
        // return the status of any drop failures
        status = savedStatus;
    }

    return status;
}

Status
Dag::getDagByName(char *dagName,
                  TableScope tScope,
                  XcalarApiOutput **outputOut,
                  size_t *outputSizeOut)
{
    Status status;
    DagNodeTypes::Node *dagNode;

    status = this->lookupNodeByName(dagName, &dagNode, tScope, true);
    if (status != StatusOk) {
        return status;
    }

    status = this->dgGetDag(dagNode, outputOut, outputSizeOut);

    return status;
}

Status
Dag::getDagById(DagTypes::NodeId dagNodeId,
                XcalarApiOutput **outputOut,
                size_t *outputSizeOut)
{
    Status status;
    DagNodeTypes::Node *dagNode;

    lock_.lock();

    status = this->lookupNodeById(dagNodeId, &dagNode);

    lock_.unlock();

    if (status != StatusOk) {
        return status;
    }

    status = this->dgGetDag(dagNode, outputOut, outputSizeOut);

    return status;
}

Status
Dag::readDagNodeRefById(DagTypes::NodeId dagNodeId, uint64_t *refCount)
{
    Status status = StatusOk;
    DagNodeTypes::Node *dagNode;

    assert(refCount != NULL);

    lock_.lock();
    status = this->lookupNodeById(dagNodeId, &dagNode);

    if (status != StatusOk) {
        goto CommonExit;
    }

    *refCount = refRead(&dagNode->refCount);

CommonExit:

    lock_.unlock();
    return status;
}

Status
Dag::getDagNodeName(DagTypes::NodeId dagNodeId, char *nameOut, size_t size)
{
    DagNodeTypes::Node *dagNode;
    Status status;
    bool locked = false;

    assert(dagNodeId != DagTypes::InvalidDagNodeId);

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);

    if (status != StatusOk) {
        goto CommonExit;
    }

    if (nameOut != NULL) {
        status =
            strStrlcpy(nameOut, dagNode->dagNodeHdr.apiDagNodeHdr.name, size);
        BailIfFailed(status);
    }

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

// The caller must hold refCount on the dagNodeId
Status
Dag::getOpStatus(DagTypes::NodeId dagNodeId, OpStatus **statusOut)
{
    DagNodeTypes::Node *dagNode = NULL;
    Status status;
    assert(dagNodeId != DagTypes::InvalidDagNodeId);

    lock_.lock();
    status = this->lookupNodeById(dagNodeId, &dagNode);

    // this should always succeed by design
    assert(status == StatusOk);

    *statusOut = &dagNode->opStatus;

    lock_.unlock();

    return status;
}

// The caller must hold refCount on the dagNodeId
Status
Dag::getOpStatusFromXdbId(XdbId xdbId, OpStatus **statusOut)
{
    DagNodeTypes::Node **dagNodes = NULL;
    unsigned numNodes = 0;
    Status status;

    lock_.lock();
    status = this->lookupNodesByXdbId(xdbId, dagNodes, numNodes);
    BailIfFailed(status);

    assert(numNodes == 1);

    *statusOut = &dagNodes[0]->opStatus;

CommonExit:
    lock_.unlock();

    if (dagNodes != NULL) {
        memFree(dagNodes);
    }

    return status;
}

Status
Dag::getChildDagNode(char *dagName,
                     XcalarApiDagOutput **output,
                     size_t *outputSizeOut)
{
    Status status;
    XcalarApiDagOutput *dagOutput = NULL;
    bool locked = false;
    DagNodeTypes::Node *dagNode;
    size_t outputSize;
    DagNodeTypes::NodeIdListElt *childElt;
    DagNodeTypes::Node *childDagNode = NULL;
    unsigned numChild = 0;
    void **dagNodesSelected = NULL;
    size_t totalApiInputSize = 0, parentSize = 0, childrenSize = 0;

    lock_.lock();
    locked = true;

    status = this->lookupNodeByName(dagName, &dagNode, TableScope::LocalOnly);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dagNodesSelected =
        (void **) memAllocExt(sizeof(*dagNodesSelected) * dagNode->numChild,
                              moduleName);
    if (dagNodesSelected == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    totalApiInputSize = 0;
    numChild = 0;
    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        status = this->lookupNodeById(childElt->nodeId, &childDagNode);
        assert(status == StatusOk);
        totalApiInputSize += childDagNode->dagNodeHdr.apiDagNodeHdr.inputSize;
        parentSize += childDagNode->numParent * sizeof(XcalarApiDagNodeId);
        childrenSize += childDagNode->numChild * sizeof(XcalarApiDagNodeId);

        dagNodesSelected[numChild] = childDagNode;
        numChild++;
        childElt = childElt->next;
    }
    assert(numChild == dagNode->numChild);

    outputSize = this->sizeOfDagOutput(dagNode->numChild,
                                       totalApiInputSize,
                                       parentSize,
                                       childrenSize);
    dagOutput = (XcalarApiDagOutput *) memAllocExt(outputSize, moduleName);
    if (dagOutput == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = DagLib::get()->copyDagNodesToDagOutput(dagOutput,
                                                    outputSize,
                                                    dagNodesSelected,
                                                    numChild,
                                                    &udfContainer_,
                                                    DagNodeTypeDagNode);
    assert(status == StatusOk);
    assert(dagOutput->numNodes == numChild);

    status = StatusOk;

    *output = dagOutput;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (dagNodesSelected != NULL) {
        memFree(dagNodesSelected);
        dagNodesSelected = NULL;
    }

    if (status != StatusOk) {
        if (dagOutput != NULL) {
            memFree(dagOutput);
            dagOutput = NULL;
        }
    }

    return status;
}

Status
Dag::getChildDagNodeId(DagTypes::NodeId dagNodeId,
                       DagTypes::NodeId **childNodesArrayOut,
                       uint64_t *numChildOut)
{
    Status status;
    DagTypes::NodeId *childNodesArray = NULL;
    bool locked = false;
    DagNodeTypes::Node *dagNode;
    uint64_t numChild = 0, ii;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNode->numChild > 0) {
        childNodesArray =
            (DagTypes::NodeId *) memAllocExt(sizeof(DagTypes::NodeId) *
                                                 dagNode->numChild,
                                             moduleName);
        if (childNodesArray == NULL) {
            status = StatusNoMem;
            goto CommonExit;
        }

        DagNodeTypes::NodeIdListElt *childElt;

        childElt = dagNode->childrenList;
        while (childElt != NULL) {
            // There is a chance that children might be double counted
            // (e.g. during a self-join). We allow parents to be
            // double counted, because we want to know a self-join
            // happened. But double counting children is very
            // non-intuitive. So we have to do some de-duping
            bool isDuplicate = false;
            for (ii = 0; ii < numChild; ii++) {
                if (childNodesArray[ii] == childElt->nodeId) {
                    // Duplicated
                    isDuplicate = true;
                    break;
                }
            }

            if (!isDuplicate) {
                childNodesArray[numChild] = childElt->nodeId;
                numChild++;
            }

            childElt = childElt->next;
        }
        assert(numChild <= dagNode->numChild);
    }

    status = StatusOk;

CommonExit:

    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (status != StatusOk) {
        if (childNodesArray != NULL) {
            memFree(childNodesArray);
            childNodesArray = NULL;
        }
    }

    *numChildOut = numChild;
    *childNodesArrayOut = childNodesArray;

    return status;
}

Status
Dag::getParentDagNode(DagTypes::NodeId dagNodeId,
                      XcalarApiDagOutput **output,
                      size_t *outputSizeOut)
{
    Status status;
    XcalarApiDagOutput *dagOutput = NULL;
    bool locked = false;
    DagNodeTypes::Node *dagNode;
    void **dagNodesSelected = NULL;
    size_t outputSize, totalApiInputSize = 0, parentSize = 0, childrenSize = 0;
    uint64_t ii;
    DagNodeTypes::Node *parentDagNode = NULL;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    dagNodesSelected =
        (void **) memAllocExt(sizeof(*dagNodesSelected) * dagNode->numParent,
                              moduleName);
    if (dagNodesSelected == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    ii = 0;
    DagNodeTypes::NodeIdListElt *parentElt;
    parentElt = dagNode->parentsList;
    while (parentElt != NULL) {
        status = this->lookupNodeById(parentElt->nodeId, &parentDagNode);
        assert(status == StatusOk);

        totalApiInputSize += parentDagNode->dagNodeHdr.apiDagNodeHdr.inputSize;
        parentSize += parentDagNode->numParent * sizeof(XcalarApiDagNodeId);
        childrenSize += parentDagNode->numChild * sizeof(XcalarApiDagNodeId);

        assert(ii < dagNode->numParent);
        dagNodesSelected[ii] = parentDagNode;

        parentElt = parentElt->next;
        ii++;
    }

    outputSize = this->sizeOfDagOutput(dagNode->numParent,
                                       totalApiInputSize,
                                       parentSize,
                                       childrenSize);
    dagOutput = (XcalarApiDagOutput *) memAllocExt(outputSize, moduleName);
    if (dagOutput == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    assert(ii == dagNode->numParent);
    status = DagLib::get()->copyDagNodesToDagOutput(dagOutput,
                                                    outputSize,
                                                    dagNodesSelected,
                                                    dagNode->numParent,
                                                    &udfContainer_,
                                                    DagNodeTypeDagNode);
    assert(status == StatusOk);

    status = StatusOk;

    assert(dagOutput->numNodes == dagNode->numParent);
    *output = dagOutput;
    *outputSizeOut = outputSize;

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (dagNodesSelected != NULL) {
        memFree(dagNodesSelected);
        dagNodesSelected = NULL;
    }

    if (status != StatusOk) {
        if (dagOutput != NULL) {
            memFree(dagOutput);
            dagOutput = NULL;
        }
    }

    return status;
}

Status
Dag::getParentDagNodeId(DagTypes::NodeId dagNodeId,
                        DagTypes::NodeId **parentNodesArrayOut,
                        uint64_t *numParentOut)
{
    Status status;
    bool locked = false;
    DagNodeTypes::Node *dagNode;
    uint64_t ii;
    DagTypes::NodeId *parentNodesArray = NULL;
    uint64_t numParent = 0;

    assert(parentNodesArrayOut != NULL);
    assert(numParentOut != NULL);

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    numParent = dagNode->numParent;
    parentNodesArray = new (std::nothrow) DagTypes::NodeId[numParent];
    if (parentNodesArray == NULL) {
        xSyslog(moduleName,
                XlogErr,
                "Insufficient memory to allocate parentNodesArray. "
                "(numParent: %lu)",
                numParent);
        status = StatusNoMem;
        goto CommonExit;
    }

    ii = 0;
    DagNodeTypes::NodeIdListElt *parentElt;
    parentElt = dagNode->parentsList;
    while (parentElt != NULL) {
        parentNodesArray[ii++] = parentElt->nodeId;
        parentElt = parentElt->next;
    }
    assert(ii == numParent);

    status = StatusOk;

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (status != StatusOk) {
        if (parentNodesArray != NULL) {
            assert(numParent > 0);
            delete[] parentNodesArray;
            parentNodesArray = NULL;
            numParent = 0;
        }
    }

    *parentNodesArrayOut = parentNodesArray;
    *numParentOut = numParent;

    return status;
}

Status
Dag::getFirstDagInOrder(DagTypes::NodeId *dagNodeIdOut)
{
    Status status;

    lock_.lock();

    if (hdr_.numNodes > 0) {
        assert(hdr_.firstNode != DagTypes::InvalidDagNodeId);
        *dagNodeIdOut = hdr_.firstNode;
    } else {
        *dagNodeIdOut = DagTypes::InvalidDagNodeId;
    }

    lock_.unlock();

    status = StatusOk;
    return status;
}

Status
Dag::getNextDagInOrder(DagTypes::NodeId dagNodeIdIn,
                       DagTypes::NodeId *dagNodeIdOut)
{
    Status status;
    bool locked = false;
    DagNodeTypes::Node *dagNode = NULL;

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeIdIn, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    *dagNodeIdOut = dagNode->dagNodeHdr.dagNodeOrder.next;
    status = StatusOk;
CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::getScalarResult(DagTypes::NodeId dagNodeId, Scalar **scalarOut)
{
    Status status;
    bool locked = false;
    DagNodeTypes::Node *dagNode;
    Scalar *newScalar = NULL;

    assert(scalarOut != NULL);

    lock_.lock();
    locked = true;

    status = this->lookupNodeById(dagNodeId, &dagNode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNode->scalarResult == NULL) {
        status = StatusAggregateResultNotFound;
        goto CommonExit;
    }

    newScalar = Scalar::allocScalar(DfMaxFieldValueSize);
    BailIfNullWith(newScalar, StatusNoMem);

    status = newScalar->copyFrom(dagNode->scalarResult);
    // We allocated this to be large enough, so it must succeed
    assert(status == StatusOk);

    if (status != StatusOk) {
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    if (status != StatusOk) {
        if (newScalar) {
            Scalar::freeScalar(newScalar);
            newScalar = NULL;
        }
    }

    *scalarOut = newScalar;

    return status;
}

bool
Dag::isNodeActive(DagNodeTypes::Node *node,
                  DagTypes::GraphType srcGraphType,
                  DagTypes::SearchMode ignoreProcessing)
{
    if (srcGraphType == DagTypes::QueryGraph) {
        return node->dagNodeHdr.apiDagNodeHdr.state != DgDagStateError &&
               node->dagNodeHdr.apiDagNodeHdr.state != DgDagStateDropped;
    } else if (node->dagNodeHdr.apiDagNodeHdr.state == DgDagStateReady) {
        // Otherwise, if node is ready -> ready
        return true;
    } else if (node->dagNodeHdr.apiDagNodeHdr.state == DgDagStateProcessing ||
               node->dagNodeHdr.apiDagNodeHdr.state == DgDagStateCreated) {
        // If node is in the middle of something (i.e. Created or Processing),
        // then it's up to the user to decide
        return !ignoreProcessing;
    }
    return false;
}

Status
Dag::getActiveDagNodes(DagTypes::GraphType srcGraphType,
                       DagTypes::NodeName *activeNodeNamesOut[],
                       DagTypes::NodeId *activeNodeIdsOut[],
                       uint64_t *numActiveNodesOut,
                       DagTypes::SearchMode ignoreProcessing)
{
    return getActiveDagNodesInt(srcGraphType,
                                activeNodeNamesOut,
                                activeNodeIdsOut,
                                numActiveNodesOut,
                                ignoreProcessing,
                                true);
}

Status
Dag::getActiveDagNodesInt(DagTypes::GraphType srcGraphType,
                          DagTypes::NodeName *activeNodeNamesOut[],
                          DagTypes::NodeId *activeNodeIdsOut[],
                          uint64_t *numActiveNodesOut,
                          DagTypes::SearchMode ignoreProcessing,
                          bool takeLock)
{
    Status status = StatusUnknown;
    DagTypes::NodeName *activeNodeNames = NULL;
    DagTypes::NodeId *activeNodeIds = NULL;
    uint64_t numActiveNodes = 0, activeNodeIdx = 0;
    int ii;
    bool lockAcquired = false;
    SourceType srcType;

    assert(numActiveNodesOut != NULL);

    if (takeLock) {
        lock_.lock();
        lockAcquired = true;
    } else {
        assert(!lock_.tryLock());
    }

    for (ii = 0; ii < 2; ii++) {
        DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;
        DagNodeTypes::Node *node = NULL;

        if (ii == 0) {
            numActiveNodes = 0;
        } else {
            activeNodeIdx = 0;
            if (numActiveNodes == 0) {
                break;
            }
            activeNodeNames = (DagTypes::NodeName *) memAlloc(
                sizeof(*activeNodeNames) * numActiveNodes);
            if (activeNodeNames == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Insufficient memory to allocate activeNodeNames "
                        "(numActiveNodes: %lu)",
                        numActiveNodes);
                status = StatusNoMem;
                goto CommonExit;
            }

            activeNodeIds = (DagTypes::NodeId *) memAlloc(
                sizeof(*activeNodeIds) * numActiveNodes);
            if (activeNodeIds == NULL) {
                xSyslog(moduleName,
                        XlogErr,
                        "Insufficient memory to allocate activeNodeIds "
                        "(numActiveNodes: %lu)",
                        numActiveNodes);
                status = StatusNoMem;
                goto CommonExit;
            }
        }

        nodeId = hdr_.firstNode;
        while (nodeId != DagTypes::InvalidDagNodeId) {
            status = this->lookupNodeById(nodeId, &node);
            if (status != StatusOk) {
                assert(0);
                goto CommonExit;
            }

            srcType = DagLib::get()->getSourceTypeFromApi(
                node->dagNodeHdr.apiDagNodeHdr.api);

            if ((srcType == SrcDataset || srcType == SrcTable ||
                 srcType == SrcConstant) &&
                isNodeActive(node, srcGraphType, ignoreProcessing)) {
                // This is an active load node

                if (ii == 0) {
                    numActiveNodes++;
                } else {
                    verifyOk(
                        strStrlcpy(activeNodeNames[activeNodeIdx],
                                   node->dagNodeHdr.apiDagNodeHdr.name,
                                   sizeof(activeNodeNames[activeNodeIdx])));
                    activeNodeIds[activeNodeIdx] = nodeId;
                    activeNodeIdx++;
                }
            }
            nodeId = node->dagNodeHdr.dagNodeOrder.next;
        }
    }

    // See Xc-8325: report if second pass found less active nodes than first
    // pass.  Based on logic above, this means the dataset for
    // numActiveNodes-activeNodeIdx nodes was dropped between the first and
    // second passes (isDatasetListable() returned true in first pass but false
    // in second pass, for these nodes). This also means activeNodeNames[] array
    // is larger than needed (but this is safe as long as we return
    // activeNodeIdx as the number of active Dag nodes - which we now do as the
    // fix for Xc-8325).

    if (numActiveNodes != activeNodeIdx) {
        xSyslog(moduleName,
                XlogInfo,
                "numActiveNodes %lu != activeNodeIdx %lu",
                numActiveNodes,
                activeNodeIdx);
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (activeNodeNames != NULL) {
            memFree(activeNodeNames);
            activeNodeNames = NULL;
        }

        if (activeNodeIds != NULL) {
            memFree(activeNodeIds);
            activeNodeIds = NULL;
        }

        numActiveNodes = 0;
    }

    if (activeNodeNamesOut != NULL) {
        *activeNodeNamesOut = activeNodeNames;
    } else if (activeNodeNames != NULL) {
        // Nobody wants this stuff :-(
        // Maybe we shouldn't have allocated it in the first place...
        memFree(activeNodeNames);
        activeNodeNames = NULL;
    }

    if (activeNodeIdsOut != NULL) {
        *activeNodeIdsOut = activeNodeIds;
    } else if (activeNodeIds != NULL) {
        memFree(activeNodeIds);
        activeNodeIds = NULL;
    }

    if (lockAcquired) {
        lock_.unlock();
        lockAcquired = false;
    }

    //
    // Return the number of active nodes whose names were filled into the
    // activeNodeNames[] array - this is activeNodeIdx. This may be less than
    // numActiveNodes.
    //
    // numActiveNodes was computed in the first pass, memory allocated for it,
    // and in second pass, some DAG nodes may have become inactive due to their
    // datasets being dropped in the meantime (isDatasetListable for the DAG
    // node's dataset returning false only in the second pass), and so
    // activeNodeIdx could be less than numActiveNodes...the array will be sized
    // to numActiveNodes, which is larger than needed, but the first
    // activeNodeIdx slots in it would be the only slots accessed, since that's
    // the number being returned here.
    //

    *numActiveNodesOut = activeNodeIdx;

    return status;
}

Status
Dag::getRootNodeIds(DagTypes::NodeId *srcNodeArrayOut[],
                    uint64_t *numSrcNodesOut)
{
    Status status = StatusUnknown;
    uint64_t numSrcNodes = 0;
    bool lockAcquired = false;
    DagTypes::NodeId nodeId = DagTypes::InvalidDagNodeId;
    DagNodeTypes::Node *node = NULL;

    DagTypes::NodeId *srcNodeArray =
        (DagTypes::NodeId *) memAlloc(hdr_.numNodes * sizeof(*srcNodeArray));
    BailIfNull(srcNodeArray);

    lock_.lock();
    lockAcquired = true;

    if (hdr_.numNodes == 0) {
        status = StatusOk;
        goto CommonExit;
    }

    nodeId = hdr_.firstNode;
    while (nodeId != DagTypes::InvalidDagNodeId) {
        status = this->lookupNodeById(nodeId, &node);
        if (status != StatusOk) {
            assert(0);
            goto CommonExit;
        }
        if (node->numParent == 0) {
            srcNodeArray[numSrcNodes++] = nodeId;
        }
        nodeId = node->dagNodeHdr.dagNodeOrder.next;
    }

CommonExit:
    if (status != StatusOk) {
        if (srcNodeArray != NULL) {
            memFree(srcNodeArray);
            srcNodeArray = NULL;
            numSrcNodes = 0;
        }
    }

    if (lockAcquired) {
        lock_.unlock();
        lockAcquired = false;
    }

    *srcNodeArrayOut = srcNodeArray;
    *numSrcNodesOut = numSrcNodes;

    return status;
}

Status
Dag::updateDagNodeParam(DagTypes::NodeId dagNodeId,
                        XcalarApiParamInput *paramInput)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *dagNode = NULL;
    bool locked = false;

    assert(paramInput != NULL);

    this->lock();
    locked = true;
    status = this->lookupNodeById(dagNodeId, &dagNode);

    if (status != StatusOk) {
        goto CommonExit;
    }

    if (dagNode->dagNodeHdr.apiDagNodeHdr.api != paramInput->paramType) {
        status = StatusDagParamInputTypeMismatch;
        goto CommonExit;
    }

    XcalarApiInput *apiInput;
    apiInput = dagNode->dagNodeHdr.apiInput;
    assert(dagNode->dagNodeHdr.apiDagNodeHdr.api == paramInput->paramType);
    status =
        DagLib::get()->updateXcalarApiInputFromParamInput(apiInput, paramInput);
CommonExit:
    if (locked) {
        this->unlock();
        locked = false;
    }

    return status;
}

void
Dag::removeNodeFromNameTable(DagNodeTypes::Node *node)
{
    nameHashTable_.remove(node->getName());
}

// XXX: change DAG node state consistency
void
Dag::removeDagNodeLocal(DagNodeTypes::Node *dagNode)
{
    this->removeNodeFromNameTable(dagNode);
    this->removeAndDeleteDagNode(dagNode);
}

Status
Dag::changeAllNodesState(DgDagState state)
{
    Status status = StatusOk;
    bool locked = false;

    lock_.lock();
    locked = true;

    DagNodeTypes::Node *node = NULL;
    DagTypes::NodeId nodeId = hdr_.firstNode;
    ssize_t ii = 0;
    while (nodeId != DagTypes::InvalidDagNodeId) {
        status = this->lookupNodeById(nodeId, &node);
        if (status != StatusOk) {
            assert(0);
            goto CommonExit;
        }
        node->dagNodeHdr.apiDagNodeHdr.state = state;

        ii++;
        nodeId = node->dagNodeHdr.dagNodeOrder.next;
    }
    assert(ii == hdr_.numNodes);

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

Status
Dag::lookupDroppedNodeByName(const char *name, DagNodeTypes::Node **nodeOut)
{
    Status status = StatusDagNodeNotFound;
    bool locked = false;

    lock_.lock();
    locked = true;

    DagNodeTypes::Node *node = NULL;
    DagTypes::NodeId nodeId = hdr_.firstNode;
    ssize_t ii = 0;
    while (nodeId != DagTypes::InvalidDagNodeId) {
        status = this->lookupNodeById(nodeId, &node);
        assert(status == StatusOk);

        if (node->dagNodeHdr.apiDagNodeHdr.state == DgDagStateDropped &&
            strcmp(node->dagNodeHdr.apiDagNodeHdr.name, name) == 0) {
            *nodeOut = node;
            status = StatusOk;
            goto CommonExit;
        }

        ii++;
        nodeId = node->dagNodeHdr.dagNodeOrder.next;
    }
    assert(ii == hdr_.numNodes);

CommonExit:
    if (locked) {
        lock_.unlock();
        locked = false;
    }

    return status;
}

void
Dag::destroyDagNodeFromRef(RefCount *refCountIn)
{
    DagLib *dagLib = DagLib::get();
    assert(refCountIn != NULL);
    DagNodeTypes::Node *dagNode =
        RefEntry(refCountIn, DagNodeTypes::Node, refCount);
    dagLib->destroyDagNode(dagNode);
}

bool
Dag::foundApi(XcalarApis api, unsigned numApis, XcalarApis *apiArray)
{
    for (unsigned ii = 0; ii < numApis; ++ii) {
        if (api == apiArray[ii]) {
            return true;
        }
    }

    return false;
}

struct UpdateLocalDataInput {
    DagTypes::DagId dagId;
    DagTypes::NodeId nodeId;
};

Status  // static
Dag::updateLocalHandler(uint32_t userAction, const void *payload)
{
    Status status = StatusUnknown;
    DagLib *dagLib = DagLib::get();

    switch (userAction) {
    case DgUpdateCreateNode: {
        CreateDagNodeInput *createDagNodeInput;
        createDagNodeInput = (CreateDagNodeInput *) payload;
        Dag::deserializeCreateDagNodeInput(createDagNodeInput);

        Dag *dag = dagLib->getDagLocal(createDagNodeInput->dagId);
        assert(dag != NULL);  // guaranteed by contract
        status = dag->createNewDagNodeLocal(createDagNodeInput);
        break;
    }

    case DgUpdateDeleteNode: {
        DeleteDagNodeInput *deleteDagNodeInput;
        deleteDagNodeInput = (DeleteDagNodeInput *) payload;

        Dag *dag = dagLib->getDagLocal(deleteDagNodeInput->dagId);
        assert(dag != NULL);  // guaranteed by contract

        status = dag->deleteDagNodeLocal(deleteDagNodeInput->dagNodeId,
                                         deleteDagNodeInput->dropOnly);
        break;
    }

    case DgUpdateRenameNode: {
        RenameDagNodeInput *renameDagNodeInput;
        renameDagNodeInput = (RenameDagNodeInput *) payload;

        Dag *dag = dagLib->getDagLocal(renameDagNodeInput->dagId);
        assert(dag != NULL);  // guaranteed by contract

        status = dag->renameDagNodeLocal(renameDagNodeInput->oldName,
                                         renameDagNodeInput->newName);
        break;
    }

        // XXX Below APIs should accept const void *.
    case DgUpdateOpDetails: {
        status = Dag::updateOpDetailsLocal((void *) payload);
        break;
    }

    case DgSetScalarResult: {
        status = Dag::dagSetScalarResultLocal((void *) payload);
        break;
    }

    case DgRemoveNodeFromNs: {
        DagNodeTypes::Node *dagNode = NULL;
        RemoveNodeFromNsInput *rmNodeFromNsInput;
        rmNodeFromNsInput = (RemoveNodeFromNsInput *) payload;

        Dag *dag = dagLib->getDagLocal(rmNodeFromNsInput->dagId);
        assert(dag != NULL);  // guaranteed by contract

        dag->lock();

        status = dag->lookupNodeById(rmNodeFromNsInput->dagNodeId, &dagNode);

        assert(status == StatusOk);

        dag->removeNodeFromNameTable(dagNode);

        dag->unlock();

        break;
    }

    default:
        assert(0);
        break;
    }

    return status;
}

Status
Dag::appendExportDagNode(DagTypes::NodeId newNodeId,
                         char *exportTableName,
                         DagTypes::NodeId parentNodeId,
                         size_t apiInputSize,
                         XcalarApiInput *apiInput)
{
    assert(hdr_.graphType == DagTypes::QueryGraph &&
           "Export Dag Nodes are appended only for DagTypes::QueryGraph");
    Status status = StatusOk;
    DagTypes::DagId parentGraphId = this->getId();

    CreateDagNodeInput createDagNodeInput;
    createDagNodeInput.dagNodeId = XidMgr::get()->xidGetNext();
    createDagNodeInput.dagId = getId();
    createDagNodeInput.api = XcalarApiExport;
    createDagNodeInput.input = apiInput;
    createDagNodeInput.apiInputSize = apiInputSize;
    createDagNodeInput.xdbId = XdbIdInvalid;
    createDagNodeInput.tableId = TableNsMgr::InvalidTableId;
    verifyOk(strStrlcpy(createDagNodeInput.name,
                        exportTableName,
                        sizeof(createDagNodeInput.name)));
    createDagNodeInput.numParent = 1;
    createDagNodeInput.parentNodeIds = &parentNodeId;
    createDagNodeInput.parentGraphIds = &parentGraphId;

    status = createNewDagNodeLocal(&createDagNodeInput);
    BailIfFailed(status);

CommonExit:
    return status;
}

void
Dag::removeAndDestroyTransitDagNodes()
{
    Status status = StatusOk;
    DagTypes::NodeId dagNodeId;
    DagNodeTypes::Node *dagNode = NULL;

    assert(lock_.tryLock() == false);
    dagNodeId = hdr_.lastNode;

    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = this->lookupNodeById(dagNodeId, &dagNode);
        assert(status == StatusOk);
        removeDagNode(dagNode);
        DagLib::get()->destroyDagNode(dagNode);

        dagNodeId = hdr_.lastNode;
    }
}

bool
Dag::isOnOwnerNode()
{
    return ownerNode_;
}

void
Dag::lock()
{
    lock_.lock();
}

void
Dag::unlock()
{
    lock_.unlock();
}

uint64_t
Dag::getNumNode()
{
    return hdr_.numNodes;
}

DagTypes::DagId
Dag::getId() const
{
    return dagId_;
}

DagTypes::NodeId
Dag::getFirstNodeIdInOrder()
{
    return hdr_.firstNode;
}

DagNodeTypes::Node *
Dag::getFirstNode()
{
    Status status;
    DagNodeTypes::Node *node = NULL;

    status = lookupNodeById(hdr_.firstNode, &node);

    return node;
}

bool
Dag::inSession()
{
    if (sessionContainer_.sessionInfo.sessionNameLength != 0) {
        return true;
    } else {
        return false;
    }
}

void
Dag::initSessionContainer(XcalarApiUdfContainer *sessionContainer)
{
    UserDefinedFunction::copyContainers(&sessionContainer_, sessionContainer);
}

void
Dag::initContainer(XcalarApiUdfContainer *container)
{
    initUdfContainer(container);
    initSessionContainer(container);
}

void
Dag::initUdfContainer(XcalarApiUdfContainer *udfContainer)
{
    UserDefinedFunction::copyContainers(&udfContainer_, udfContainer);
}

XcalarApiUdfContainer *
Dag::getUdfContainer()
{
    return &udfContainer_;
}

XcalarApiUdfContainer *
Dag::getSessionContainer()
{
    return &sessionContainer_;
}

size_t
Dag::sizeOfDagOutputHdr(uint64_t numNodes)
{
    XcalarApiDagOutput *dagOutput;
    return sizeof(*dagOutput) + (numNodes * sizeof(dagOutput->node[0]));
}

size_t
Dag::sizeOfDagOutput(uint64_t numNodes,
                     size_t totalApiInputSize,
                     size_t totalParentSize,
                     size_t totalChildrenSize)
{
    XcalarApiDagOutput *dagOutput;
    return sizeOfDagOutputHdr(numNodes) +
           (numNodes * sizeof(*(dagOutput->node[0]))) + totalApiInputSize +
           totalParentSize + totalChildrenSize;
}

size_t
Dag::sizeOfDagOutput(uint64_t numNodes, size_t totalVariableBufSize)
{
    XcalarApiDagOutput *dagOutput;
    return sizeOfDagOutputHdr(numNodes) +
           (numNodes * sizeof(*(dagOutput->node[0]))) + totalVariableBufSize;
}

size_t
Dag::sizeOfSerializedNodeHdr(SerializedDagNode *node, uint64_t numParent)
{
    return sizeof(*node) + (sizeof(node->parentsList[0]) * numParent);
}

size_t
Dag::sizeOfSerializedNode(SerializedDagNode *node, uint64_t numParent)
{
    return sizeOfSerializedNodeHdr(node, numParent) +
           node->dagNodeHdr.apiDagNodeHdr.inputSize;
}

size_t
Dag::sizeOfCreateDagNodeInput(uint64_t numParent, size_t totalApiInputSize)
{
    CreateDagNodeInput *createDagNodeInput;
    return sizeof(*createDagNodeInput) +
           (sizeof(createDagNodeInput->parentNodeIds[0]) * numParent) +
           (sizeof(createDagNodeInput->parentGraphIds[0]) * numParent) +
           totalApiInputSize;
}

DagNodeTypes::Node *
Dag::getLastNode()
{
    DagNodeTypes::Node *node = NULL;
    lock_.lock();
    Status status = lookupNodeById(hdr_.lastNode, &node);
    lock_.unlock();

    if (status != StatusOk) {
        return NULL;
    } else {
        return node;
    }
}

DagTypes::GraphType
Dag::getDagType()
{
    return hdr_.graphType;
}

Status
Dag::getRetinas(unsigned numTargets,
                DagTypes::NodeName *targetNodeNames,
                unsigned *numRetinasOut,
                RetinaInfo ***retinaInfoOut)
{
    bool srcDagLocked = false;
    Status status;
    DagNodeListElt *listEltToFree;
    DagNodeListElt *listHead = NULL;
    DagNodeListElt *listTail = NULL;
    DagNodeListElt *listElt = NULL;
    uint64_t numNodes;
    unsigned numRetinas = 0;
    RetinaInfo **retinaInfos = NULL;
    unsigned ii = 0;

    DagNodeTypes::Node **dagNodes =
        (DagNodeTypes::Node **) memAlloc(numTargets * sizeof(*dagNodes));
    BailIfNull(dagNodes);

    lock_.lock();
    srcDagLocked = true;

    for (ii = 0; ii < numTargets; ++ii) {
        status = this->lookupNodeByName(targetNodeNames[ii],
                                        &dagNodes[ii],
                                        TableScope::LocalOnly);
        BailIfFailed(status);
    }

    status = getDagAncestorTree(dagNodes,
                                numTargets,
                                0,
                                NULL,
                                NULL,
                                &listHead,
                                &listTail,
                                &numNodes);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // count the number of retina nodes
    listElt = listHead;

    while (listElt) {
        if (listElt->node->dagNodeHdr.apiDagNodeHdr.api ==
            XcalarApiExecuteRetina) {
            numRetinas++;
        }

        listElt = listElt->next;
    }

    if (numRetinas == 0) {
        goto CommonExit;
    }

    retinaInfos = (RetinaInfo **) memAlloc(sizeof(*retinaInfos) * numRetinas);
    BailIfNull(retinaInfos);
    memZero(retinaInfos, sizeof(*retinaInfos) * numRetinas);

    listElt = listHead;
    ii = 0;
    while (listElt) {
        if (listElt->node->dagNodeHdr.apiDagNodeHdr.api ==
            XcalarApiExecuteRetina) {
            XcalarApiExecuteRetinaInput *executeRetinaInput =
                &listElt->node->dagNodeHdr.apiInput->executeRetinaInput;

            status =
                DagLib::get()
                    ->parseRetinaFile(executeRetinaInput->exportRetinaBuf,
                                      executeRetinaInput->exportRetinaBufSize,
                                      executeRetinaInput->retinaName,
                                      &retinaInfos[ii]);
            BailIfFailed(status);
            ii++;
        }

        listElt = listElt->next;
    }

CommonExit:
    if (srcDagLocked) {
        lock_.unlock();
        srcDagLocked = false;
    }

    if (dagNodes) {
        memFree(dagNodes);
        dagNodes = NULL;
    }

    while (listHead != NULL) {
        listEltToFree = listHead;
        listHead = listHead->next;
        delete listEltToFree;
        listEltToFree = NULL;
    }

    if (status != StatusOk) {
        if (retinaInfos) {
            for (ii = 0; ii < numRetinas; ii++) {
                if (retinaInfos[ii]) {
                    DagLib::get()->destroyRetinaInfo(retinaInfos[ii]);
                    retinaInfos[ii] = NULL;
                }
            }

            memFree(retinaInfos);
        }
    } else {
        *retinaInfoOut = retinaInfos;
        *numRetinasOut = numRetinas;
    }

    return status;
}

Status
Dag::convertLastExportNode()
{
    Status status;
    DagTypes::NodeId exportNodeId = hdr_.lastNode;
    DagNodeTypes::Node *exportNode;
    XcalarApiExportInput *exportInput;
    XcalarWorkItem *workItem = NULL;

    status = lookupNodeById(exportNodeId, &exportNode);
    assert(status == StatusOk);

    exportInput = &exportNode->dagNodeHdr.apiInput->exportInput;
    XcalarApiRenameMap *renameMap = NULL;

    renameMap =
        new (std::nothrow) XcalarApiRenameMap[exportInput->meta.numColumns];
    BailIfNull(renameMap);

    assert(exportInput->meta.numColumns <= TupleMaxNumValuesPerRecord);
    for (int ii = 0; ii < exportInput->meta.numColumns; ii++) {
        verifyOk(strStrlcpy(renameMap[ii].oldName,
                            exportInput->meta.columns[ii].name,
                            sizeof(renameMap[ii].oldName)));

        verifyOk(strStrlcpy(renameMap[ii].newName,
                            exportInput->meta.columns[ii].headerAlias,
                            sizeof(renameMap[ii].newName)));

        DataFormat::replaceFatptrPrefixDelims(renameMap[ii].newName);

        renameMap[ii].type = DfUnknown;
        renameMap[ii].isKey = false;
    }

    workItem = xcalarApiMakeSynthesizeWorkItem(exportInput->srcTable.tableName,
                                               exportInput->exportName,
                                               true,
                                               exportInput->meta.numColumns,
                                               renameMap);
    BailIfNull(workItem);

    workItem->input->synthesizeInput.source.nodeId =
        exportNode->parentsList->nodeId;

    workItem->input->synthesizeInput.source.xid = exportInput->srcTable.xdbId;

    exportNode->dagNodeHdr.apiDagNodeHdr.api = XcalarApiSynthesize;

    dagApiFree(exportNode);
    exportNode->dagNodeHdr.apiInput = dagApiAlloc(workItem->inputSize);
    BailIfNull(exportNode->dagNodeHdr.apiInput);
    exportNode->dagNodeHdr.apiDagNodeHdr.inputSize = workItem->inputSize;
    status = Dag::sparseMemCpy(exportNode->dagNodeHdr.apiInput,
                               workItem->input,
                               workItem->inputSize);
    BailIfFailed(status);

CommonExit:
    if (workItem) {
        if (workItem->input) {
            memFree(workItem->input);
            workItem->input = NULL;
        }

        memFree(workItem);
    }
    if (renameMap) {
        delete[] renameMap;
        renameMap = NULL;
    }

    return status;
}

Status
Dag::convertUdfsToAbsolute(XcalarApis api, XcalarApiInput *input)
{
    Status status = StatusOk;

    switch (api) {
    case XcalarApiMap: {
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            status = XcalarEval::get()
                         ->convertEvalString(input->mapInput.evalStrs[ii],
                                             getUdfContainer(),
                                             XcalarApiMaxEvalStringLen + 1,
                                             XcalarFnTypeEval,
                                             0,
                                             NULL,
                                             XcalarEval::NoFlags);
            BailIfFailed(status);
        }
        break;
    }
    case XcalarApiBulkLoad: {
        status =
            Operators::convertLoadUdfToAbsolutePath(&input->loadInput.loadArgs,
                                                    getUdfContainer());
        BailIfFailed(status);
    }
    default:
        return StatusOk;
    }

CommonExit:
    return status;
}
