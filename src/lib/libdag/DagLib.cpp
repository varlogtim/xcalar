// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <math.h>
#include <new>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "dag/DagLib.h"
#include "config/Config.h"
#include "msg/MessageTypes.h"
#include "msg/Message.h"
#include "queryparser/QueryParser.h"
#include "util/CmdParser.h"
#include "util/MemTrack.h"
#include "hash/Hash.h"
#include "dag/DagTypes.h"
#include "DagStateEnums.h"
#include "xdb/Xdb.h"
#include "dataset/Dataset.h"
#include "operators/XcalarEval.h"
#include "sys/XLog.h"
#include "strings/String.h"
#include "optimizer/Optimizer.h"
#include "operators/OperatorsTypes.h"
#include "operators/Operators.h"
#include "operators/OperatorsApiWrappers.h"
#include "LibDagConstants.h"
#include "msg/Xid.h"
#include "dag/DagNodeTypes.h"
#include "dag/Dag.h"
#include "gvm/Gvm.h"
#include "DagNodeGvm.h"
#include "DagGvm.h"
#include "ns/LibNs.h"
#include "ns/LibNsTypes.h"
#include "subsys/DurableDag.pb.h"
#include "querymanager/QueryManager.h"
#include "udf/UserDefinedFunction.h"

using namespace dagLib;
static constexpr const char *moduleName = "libdag";

bool DagLib::dagInited = false;
DagLib *DagLib::dagLib = NULL;

size_t
DagLib::sizeOfXcalarApiDagNode(size_t apiInputSize,
                               unsigned numParents,
                               unsigned numChildren)
{
    XcalarApiDagNode *apiDagNode;
    return sizeof(*apiDagNode) + apiInputSize +
           numParents * sizeof(*apiDagNode->parents) +
           numChildren * sizeof(*apiDagNode->children);
}

size_t
DagLib::sizeOfImportRetinaOutputBuf(uint64_t numUdfModules,
                                    size_t totalUdfModulesStatusesSize)
{
    XcalarApiImportRetinaOutput *importRetinaOutput;
    return sizeof(importRetinaOutput->udfModuleStatuses[0]) * numUdfModules +
           totalUdfModulesStatusesSize;
}

Status
DagLib::init()
{
    Status status;
    void *ptr = NULL;

    ptr = memAllocExt(sizeof(DagLib), __PRETTY_FUNCTION__);
    if (ptr == NULL) {
        return StatusNoMem;
    }

    DagLib::dagLib = new (ptr) DagLib();

    status = DagLib::dagLib->createInternal();

    if (status != StatusOk) {
        if (ptr != NULL) {
            DagLib::dagLib->~DagLib();
            memFree(ptr);
            DagLib::dagLib = NULL;
        }
    }

    return status;
}

DagLib *
DagLib::get()
{
    assert(dagLib);
    return dagLib;
}

Dag *
DagLib::lookupDag(DagTypes::DagId dagId)
{
    Dag *dag = NULL;
    dagTableIdLock_.lock();
    dag = dagHTbyId_.find(dagId);
    dagTableIdLock_.unlock();
    // This "contract" may not be followed by all code paths.  Take
    // geteOpStats as an example.
    assert(dag != NULL);  // guaranteed by contract
    return dag;
}

Status
DagLib::createInternal()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = DagGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = DagNodeGvm::init();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initNewStatGroup("Dag", &statsGrpId_, 4);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDagNodesAlloced_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numDagNodesAlloced",
                                         numDagNodesAlloced_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&numDagNodesFreed_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "numDagNodesFreed",
                                         numDagNodesFreed_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&dagNodeBytesPhys_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "dagNodeBytesPhys",
                                         dagNodeBytesPhys_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&dagNodeBytesVirt_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(statsGrpId_,
                                         "dagNodeBytesVirt",
                                         dagNodeBytesVirt_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    dagInited = true;
    status = StatusOk;

CommonExit:

    return status;
}

void
DagLib::destroy()
{
    if (DagNodeGvm::get()) {
        DagNodeGvm::get()->destroy();
    }

    if (DagGvm::get()) {
        DagGvm::get()->destroy();
    }

    if (!dagInited) {
        return;
    }

    DagLib *dagLib = DagLib::get();

    dagTableIdLock_.lock();
    while (true) {
        DagHashTableById::iterator it = dagHTbyId_.begin();
        Dag *dag = it.get();
        if (dag == NULL) {
            break;
        }
        dagHTbyId_.remove(dag->getId());
        delete dag;
        dag = NULL;
    }
    dagTableIdLock_.unlock();

    dagNodeTableIdLock_.lock();
    dagNodeTableById_.removeAll(NULL);
    dagNodeTableIdLock_.unlock();

    dagLib->dagInited = false;
    dagLib->~DagLib();
    memFree(dagLib);
    dagLib = NULL;
}

Status
DagLib::createNewDagLocal(void *args)
{
    Status status = StatusOk;
    Dag *dag = NULL;

    dag = new (std::nothrow) Dag;
    if (dag == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = dag->init(args);
    if (status != StatusOk) {
        delete dag;
        goto CommonExit;
    }

    dagTableIdLock_.lock();
    status = dagHTbyId_.insert(dag);
    dagTableIdLock_.unlock();

    if (status != StatusOk) {
        delete dag;
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
DagLib::prepareDeleteDagLocal(void *args)
{
    DagId dagId = *(DagId *) args;
    Dag *dag = NULL;
    dagTableIdLock_.lock();
    dag = dagHTbyId_.find(dagId);
    dagTableIdLock_.unlock();
    if (dag == NULL) {
        return StatusDgDagNotFound;
    }
    dag->markForDelete();
    return StatusOk;
}

Status
DagLib::destroyDagLocal(void *args)
{
    DagId dagId = *(DagId *) args;

    Dag *dag = NULL;
    dagTableIdLock_.lock();
    dag = dagHTbyId_.remove(dagId);
    dagTableIdLock_.unlock();

    if (dag == NULL) {
        return StatusDgDagNotFound;
    }

    if (!dag->isOnOwnerNode()) {
        dag->lock();
        dag->removeAndDestroyTransitDagNodes();
        dag->unlock();
    }

    assert(dag->getNumNode() == 0);
    delete dag;
    dag = NULL;

    return StatusOk;
}

Status
DagLib::createNewDag(uint64_t numSlot,
                     DagTypes::GraphType graphType,
                     XcalarApiUdfContainer *udfContainer,
                     Dag **dagOut)
{
    Dag *dag = NULL;
    Status status;
    CreateNewDagParams *createNewDagParams = NULL;
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload = (Gvm::Payload *) memAlloc(
        sizeof(Gvm::Payload) + sizeof(CreateNewDagParams));
    BailIfNull(gPayload);

    createNewDagParams = (CreateNewDagParams *) gPayload->buf;
    createNewDagParams->dagId = XidMgr::get()->xidGetNext();
    createNewDagParams->numSlot = numSlot;
    createNewDagParams->graphType = graphType;
    createNewDagParams->initNodeId = Config::get()->getMyNodeId();

    assert(graphType != DagTypes::InvalidGraph);

    if (graphType == DagTypes::QueryGraph) {
        status = DagGvm::get()->localHandler((uint32_t) DagGvm::Action::Create,
                                             createNewDagParams,
                                             NULL);
    } else {
        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);
        gPayload->init(DagGvm::get()->getGvmIndex(),
                       (uint32_t) DagGvm::Action::Create,
                       sizeof(CreateNewDagParams));
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
        xSyslog(moduleName,
                XlogErr,
                "Failed create Dag numSlots %ld graphType %d: %s",
                numSlot,
                graphType,
                strGetFromStatus(status));
        goto CommonExit;
    }

    dagTableIdLock_.lock();
    dag = dagHTbyId_.find(createNewDagParams->dagId);
    dagTableIdLock_.unlock();
    assert(dag != NULL);
    if (udfContainer != NULL) {
        UserDefinedFunction::copyContainers(&dag->udfContainer_, udfContainer);
    } else {
        memset(&dag->udfContainer_, 0, sizeof(*udfContainer));
    }

    // XXX TODO
    // * Need cleaner abstraction to manage UDF scope and session scope which
    // need not be the same. For instance in optimized exection, UDF scope
    // may be belong to active session and session needs to be different
    // from UDF scope to avoid namespace collision.
    // * For now, make gross assumption that UDF scope and session scope are
    // identical until explicitly overridden.
    UserDefinedFunction::copyContainers(&dag->sessionContainer_,
                                        &dag->udfContainer_);

CommonExit:
    if (gPayload != NULL) {
        memFree(gPayload);
        gPayload = NULL;
    }
    if (nodeStatus != NULL) {
        delete[] nodeStatus;
        nodeStatus = NULL;
    }
    *dagOut = dag;
    return status;
}

Status
DagLib::destroyDag(Dag *dag, DagTypes::DestroyOpts destroyOpts)
{
    Status status = StatusOk;
    DagId dagId = dag->getId();
    unsigned nodeCount = Config::get()->getActiveNodes();
    Status *nodeStatus = NULL;
    Gvm::Payload *gPayload =
        (Gvm::Payload *) memAlloc(sizeof(Gvm::Payload) + sizeof(DagId));
    BailIfNull(gPayload);

    *(DagId *) gPayload->buf = dagId;

    if (dag->hdr_.graphType == DagTypes::QueryGraph) {
        status =
            DagGvm::get()->localHandler((uint32_t) DagGvm::Action::PreDelete,
                                        &dagId,
                                        NULL);
    } else {
        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);
        gPayload->init(DagGvm::get()->getGvmIndex(),
                       (uint32_t) DagGvm::Action::PreDelete,
                       sizeof(DagId));
        status = Gvm::get()->invoke(gPayload, nodeStatus);
        if (status == StatusOk) {
            for (unsigned ii = 0; ii < nodeCount; ii++) {
                if (nodeStatus[ii] != StatusOk) {
                    status = nodeStatus[ii];
                    break;
                }
            }
        }
        delete[] nodeStatus;
        nodeStatus = NULL;
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to destroy Dag %ld: %s",
                dagId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (destroyOpts == DagTypes::DestroyDeleteAndCleanNodes ||
        destroyOpts == DagTypes::DestroyDeleteNodes) {
        status = dag->destroyDagNodes(destroyOpts);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed destroy Dag %ld on destroyDagNodes: %s",
                dagId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    dag->lock();
    if (dag->getNumNode() != 0) {
        status = StatusDgDagNotEmpty;
    }
    dag->unlock();
    if (status != StatusOk) {
        // XXX FIXME we've half-deleted.. how is it safe to proceed here?
        xSyslog(moduleName,
                XlogErr,
                "Failed destroy Dag %ld on destroyDagNodes left back %ld"
                " nodes: %s",
                dagId,
                dag->getNumNode(),
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (dag->hdr_.graphType == DagTypes::QueryGraph) {
        status = DagGvm::get()->localHandler((uint32_t) DagGvm::Action::Delete,
                                             &dagId,
                                             NULL);
    } else {
        assert(nodeStatus == NULL);
        assert(*(DagId *) gPayload->buf == dagId);

        nodeStatus = new (std::nothrow) Status[nodeCount];
        BailIfNull(nodeStatus);
        gPayload->init(DagGvm::get()->getGvmIndex(),
                       (uint32_t) DagGvm::Action::Delete,
                       sizeof(DagId));
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
        xSyslog(moduleName,
                XlogErr,
                "Failed destroy Dag %ld: %s",
                dagId,
                strGetFromStatus(status));
        goto CommonExit;
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

bool
DagLib::isValidHeader(PersistedDagHeader *header)
{
    return header->magic == PersistedDagHeaderMagic &&
           header->majVersion == QueryGraphMajorVersion2 &&
           header->headerSize == sizeof(*header) &&
           header->footerSize == sizeof(PersistedDagFooter);
}

bool
DagLib::isValidFooter(PersistedDagFooter *footer, PersistedDagHeader *header)
{
    uint32_t checksum;

    if (footer->magic != PersistedDagFooterMagic) {
        return false;
    }

    checksum = hashCrc32c(0,
                          header,
                          header->headerSize + header->dataSize +
                              header->footerSize - sizeof(footer->checksum));

    if (checksum != footer->checksum) {
        return false;
    }

    return true;
}

Dag *
DagLib::getDagLocal(DagTypes::DagId dagId)
{
    Dag *dag = NULL;

    dag = lookupDag(dagId);
    return dag;
}

// Forcely destroy dag node, this function does not remove the dag node from its
// parent nor remove the dag node from the dag
void
DagLib::destroyDagNode(DagNodeTypes::Node *dagNode)
{
    // free childListElt
    uint64_t numChild = 0;
    DagNodeTypes::NodeIdListElt *childElt;
    childElt = dagNode->childrenList;
    while (childElt != NULL) {
        dagNode->childrenList = childElt->next;
        delete childElt;
        numChild++;
        childElt = dagNode->childrenList;
    }
    assert(numChild == dagNode->numChild);

    // free parentsList
    uint64_t numParent = 0;
    DagNodeTypes::NodeIdListElt *parentElt;
    parentElt = dagNode->parentsList;
    while (parentElt != NULL) {
        dagNode->parentsList = parentElt->next;
        delete parentElt;
        numParent++;
        parentElt = dagNode->parentsList;
    }
    assert(numParent == dagNode->numParent);

    if (dagNode->scalarResult != NULL) {
        Scalar::freeScalar(dagNode->scalarResult);
        dagNode->scalarResult = NULL;
    }

    if (dagNode->dagNodeHdr.apiInput != NULL) {
        Dag::dagApiFree(dagNode);
        dagNode->dagNodeHdr.apiInput = NULL;
    }

    if (dagNode->annotations != NULL) {
        Optimizer::get()->removeAnnotations(
            (Optimizer::DagNodeAnnotations *) dagNode->annotations);
        dagNode->annotations = NULL;
    }

    verifyOk(strStrlcpy(dagNode->dagNodeHdr.apiDagNodeHdr.name,
                        "FREE",
                        sizeof(dagNode->dagNodeHdr.apiDagNodeHdr.name)));
    Dag::freeDagNode(dagNode);
    dagNode = NULL;
}

// Abusing void * to mean either XcalarApiDagNode *dagNodesSelected[]
// or DagNodeTypes::Node *dagNodesSelected[]
Status
DagLib::copyDagNodesToDagOutput(XcalarApiDagOutput *dagOutput,
                                size_t bufSize,
                                void *dagNodesSelected[],
                                uint64_t numNodes,
                                XcalarApiUdfContainer *udfContainer,
                                DagNodeType dagNodeType)
{
    Status status = StatusUnknown;
    uintptr_t bufCursor;
    size_t bytesLeft = 0UL, bytesCopied = 0UL;
    unsigned ii;

    assert(dagOutput != NULL);

    if (bufSize < sizeof(*dagOutput)) {
        status = StatusOverflow;
        goto CommonExit;
    }

    bytesLeft = bufSize - Dag::sizeOfDagOutputHdr(numNodes);
    bufCursor = (uintptr_t) dagOutput + Dag::sizeOfDagOutputHdr(numNodes);

    status = StatusOk;
    for (ii = 0; ii < numNodes; ii++) {
        switch (dagNodeType) {
        case DagNodeTypeDagNode:
            status =
                copyDagNodeToXcalarApiDagNode((XcalarApiDagNode *) bufCursor,
                                              bytesLeft,
                                              (DagNodeTypes::Node *)
                                                  dagNodesSelected[ii],
                                              udfContainer,
                                              &bytesCopied);
            break;
        case DagNodeTypeXcalarApiDagNode:
            status =
                copyXcalarApiDagNodeToXcalarApiDagNode((XcalarApiDagNode *)
                                                           bufCursor,
                                                       bytesLeft,
                                                       (XcalarApiDagNode *)
                                                           dagNodesSelected[ii],
                                                       &bytesCopied);
            break;
        default:
            assert(0);
        }

        if (status != StatusOk) {
            goto CommonExit;
        }

        dagOutput->node[ii] = (XcalarApiDagNode *) bufCursor;
        bytesLeft -= bytesCopied;
        bufCursor += bytesCopied;
    }

    dagOutput->bufSize = bufSize;
    dagOutput->numNodes = numNodes;
CommonExit:
    return status;
}

void
DagLib::copyOpDetailsToXcalarApiLocalData(XcalarApiDagNodeLocalData *apiDataDst,
                                          OpDetails *opDetailsSrc)
{
    apiDataDst->numTransPageSent = 0;  // obsolete
    apiDataDst->numTransPageRecv = 0;  // obsolete

    apiDataDst->xdbBytesRequired = opDetailsSrc->xdbBytesRequired;
    apiDataDst->xdbBytesConsumed = opDetailsSrc->xdbBytesConsumed;
    apiDataDst->numWorkCompleted = opDetailsSrc->numWorkCompleted;
    apiDataDst->numWorkTotal = opDetailsSrc->numWorkTotal;
    apiDataDst->numRowsTotal = opDetailsSrc->numRowsTotal;
    apiDataDst->sizeTotal = opDetailsSrc->sizeTotal;

    memZero(apiDataDst->numTransPagesReceivedPerNode,
            sizeof(apiDataDst->numTransPagesReceivedPerNode));
    memZero(apiDataDst->numRowsPerNode, sizeof(apiDataDst->numRowsPerNode));
    memZero(apiDataDst->sizePerNode, sizeof(apiDataDst->sizePerNode));
    memZero(apiDataDst->hashSlotSkewPerNode,
            sizeof(apiDataDst->hashSlotSkewPerNode));

    unsigned numNodes = Config::get()->getActiveNodes();
    for (unsigned ii = 0; ii < numNodes; ii++) {
        apiDataDst->numTransPagesReceivedPerNode[ii] =
            opDetailsSrc->perNode[ii].numTransPagesReceived;
        apiDataDst->numRowsPerNode[ii] = opDetailsSrc->perNode[ii].numRows;
        apiDataDst->sizePerNode[ii] = opDetailsSrc->perNode[ii].sizeBytes;
        apiDataDst->hashSlotSkewPerNode[ii] =
            opDetailsSrc->perNode[ii].hashSlotSkew;
    }

    // XXX: make the following cognizant of the operator! For now, it only
    // reports eval errors in API (both XDFs and UDFs related errs added up).
    // But one could add support for load (loadErrorStats) and index
    // (indexErrorStats) at the very least

    apiDataDst->opFailureInfo.numRowsFailedTotal =
        // UDF failures
        opDetailsSrc->errorStats.evalErrorStats.evalUdfErrorStats
            .numEvalUdfError +
        opDetailsSrc->errorStats.evalErrorStats.evalXdfErrorStats.numTotal;

    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        for (int jj = 0; jj < XcalarApiMaxFailures; jj++) {
            memset(&apiDataDst->opFailureInfo.opFailureSummary[ii]
                        .failureSummInfo[jj],
                   0,
                   sizeof(FailureDesc));
        }
    }

    for (int ii = 0; ii < XcalarApiMaxFailureEvals; ii++) {
        FailureSummary *fSummIn;
        FailureSummary *fSummOut;

        fSummIn = &opDetailsSrc->errorStats.evalErrorStats.evalUdfErrorStats
                       .opFailureSummary[ii];

        fSummOut = &apiDataDst->opFailureInfo.opFailureSummary[ii];

        if (strStrlcpy(fSummOut->failureSummName,
                       fSummIn->failureSummName,
                       sizeof(fSummOut->failureSummName)) != StatusOk) {
            // truncation is non-fatal; report as much as possible but log
            // it
            xSyslog(moduleName,
                    XlogErr,
                    "copyOpDetailsToXcalarApiLocalData: truncated summary "
                    "name for failed eval %d, \"%s\"",
                    ii,
                    fSummIn->failureSummName);
        }

        for (int jj = 0; jj < XcalarApiMaxFailures; jj++) {
            FailureDesc *failDescIn;
            FailureDesc *failDescOut;

            failDescIn = &fSummIn->failureSummInfo[jj];

            if (failDescIn->numRowsFailed == 0) {
                break;
            }

            failDescOut = &fSummOut->failureSummInfo[jj];

            failDescOut->numRowsFailed = failDescIn->numRowsFailed;

            if (strStrlcpy(failDescOut->failureDesc,
                           failDescIn->failureDesc,
                           sizeof(failDescOut->failureDesc)) != StatusOk) {
                // truncation is non-fatal; report as much as possible but log
                // it
                xSyslog(moduleName,
                        XlogErr,
                        "copyOpDetailsToXcalarApiLocalData: truncated failure "
                        "description %d, %d, \"%s\"",
                        ii,
                        jj,
                        failDescIn->failureDesc);
            }
        }
    }
}

Status
DagLib::copyDagNodeToXcalarApiDagNode(XcalarApiDagNode *apiDagNodeOut,
                                      size_t bufSize,
                                      DagNodeTypes::Node *dagNodeIn,
                                      XcalarApiUdfContainer *udfContainer,
                                      size_t *bytesCopiedOut)
{
    Status status = StatusUnknown;
    unsigned ii = 0;
    assert(dagNodeIn != NULL);
    assert(apiDagNodeOut != NULL);
    size_t bytesCopied = 0;
    DagNodeTypes::NodeIdListElt *listElt;

    assert(apiDagNodeOut != NULL);
    assert(bytesCopiedOut != NULL);
    assert(dagNodeIn != NULL);

    bytesCopied =
        sizeOfXcalarApiDagNode(dagNodeIn->dagNodeHdr.apiDagNodeHdr.inputSize,
                               dagNodeIn->numParent,
                               dagNodeIn->numChild);

    if (bufSize < bytesCopied) {
        status = StatusOverflow;
        goto CommonExit;
    }

    apiDagNodeOut->hdr = dagNodeIn->dagNodeHdr.apiDagNodeHdr;

    // numNodes needed to iterate through the per-node count arrays in
    // opDetails
    apiDagNodeOut->localData.numNodes = dagNodeIn->numNodes;
    copyOpDetailsToXcalarApiLocalData(&apiDagNodeOut->localData,
                                      &dagNodeIn->dagNodeHdr.opDetails);

    if (apiDagNodeOut->hdr.state == DgDagStateCreated) {
        // node has just been created, no elapsed time
        apiDagNodeOut->elapsed.milliseconds = 0;
    } else {
        apiDagNodeOut->elapsed.milliseconds =
            dagNodeIn->stopwatch.getCurElapsedMSecs();

        apiDagNodeOut->operatorStartTime =
            dagNodeIn->stopwatch.getStartRealTime();
        apiDagNodeOut->operatorEndTime = dagNodeIn->stopwatch.getCurTimeStamp();
    }

    if (apiDagNodeOut->localData.numWorkCompleted == 0) {
        // work is may still be progress, check opDetails. Note that this
        // will not be completely accurate as we are only checking the local
        // node
        apiDagNodeOut->localData.numWorkCompleted = atomicRead64(
            &dagNodeIn->opStatus.atomicOpDetails.numWorkCompletedAtomic);
        apiDagNodeOut->localData.numWorkTotal =
            dagNodeIn->opStatus.atomicOpDetails.numWorkTotal;
        memZero(apiDagNodeOut->localData.numRowsPerNode,
                sizeof(apiDagNodeOut->localData.numRowsPerNode));
        memZero(apiDagNodeOut->localData.numTransPagesReceivedPerNode,
                sizeof(apiDagNodeOut->localData.numTransPagesReceivedPerNode));
    }

    memcpy(apiDagNodeOut->input,
           dagNodeIn->dagNodeHdr.apiInput,
           apiDagNodeOut->hdr.inputSize);

    // Make sure that UDF names, if any, in the input payload for the API
    // are relative, not absolute so as to not expose absolute UDF path
    // names to the users. The dagNodeIn being copied out, may have been
    // modified (due to various reasons, its input payload may have been
    // modified) to convert the original UDF names into absolute - so we
    // need to convert them back to relative names, if needed.
    status = convertUDFNamesToRelative(apiDagNodeOut->hdr.api,
                                       udfContainer,
                                       apiDagNodeOut->input);
    BailIfFailed(status);

    assertStatic(sizeof(apiDagNodeOut->hdr.name) ==
                 sizeof(dagNodeIn->dagNodeHdr.apiDagNodeHdr.name));
    verifyOk(strStrlcpy(apiDagNodeOut->hdr.name,
                        dagNodeIn->dagNodeHdr.apiDagNodeHdr.name,
                        sizeof(apiDagNodeOut->hdr.name)));

    verifyOk(strStrlcpy(apiDagNodeOut->hdr.tag,
                        dagNodeIn->dagNodeHdr.apiDagNodeHdr.tag,
                        sizeof(apiDagNodeOut->hdr.tag)));

    verifyOk(strStrlcpy(apiDagNodeOut->hdr.comment,
                        dagNodeIn->dagNodeHdr.apiDagNodeHdr.comment,
                        sizeof(apiDagNodeOut->hdr.comment)));

    apiDagNodeOut->hdr.pinned = dagNodeIn->dagNodeHdr.apiDagNodeHdr.pinned;

    // copy over parents
    apiDagNodeOut->numParents = dagNodeIn->numParent;
    apiDagNodeOut->parents =
        (XcalarApiDagNodeId *) ((uintptr_t) apiDagNodeOut->input +
                                apiDagNodeOut->hdr.inputSize);

    listElt = dagNodeIn->parentsList;
    ii = 0;
    while (listElt != NULL) {
        apiDagNodeOut->parents[ii++] = listElt->nodeId;
        listElt = listElt->next;
    }

    // copy over children
    apiDagNodeOut->numChildren = dagNodeIn->numChild;
    apiDagNodeOut->children =
        (XcalarApiDagNodeId *) ((uintptr_t) apiDagNodeOut->parents +
                                apiDagNodeOut->numParents *
                                    sizeof(*apiDagNodeOut->parents));

    listElt = dagNodeIn->childrenList;
    ii = 0;
    while (listElt != NULL) {
        apiDagNodeOut->children[ii++] = listElt->nodeId;
        listElt = listElt->next;
    }

    verifyOk(strStrlcpy(apiDagNodeOut->log,
                        dagNodeIn->log,
                        sizeof(apiDagNodeOut->log)));
    apiDagNodeOut->status = dagNodeIn->status;

    *bytesCopiedOut = bytesCopied;
    status = StatusOk;
CommonExit:
    return status;
}

Status
DagLib::convertNamesToImmediate(Dag *dag, XcalarApis api, XcalarApiInput *input)
{
    DataFormat *df = DataFormat::get();
    Status status = StatusOk;

    switch (api) {
    case XcalarApiIndex:
        for (unsigned ii = 0; ii < input->indexInput.numKeys; ii++) {
            if (!input->indexInput.source.isTable ||
                strstr(input->indexInput.keys[ii].keyName,
                       DfFatptrPrefixDelimiter)) {
                df->replaceFatptrPrefixDelims(
                    input->indexInput.keys[ii].keyName);
                status =
                    DataFormat::escapeNestedDelim(input->indexInput.keys[ii]
                                                      .keyName,
                                                  sizeof(
                                                      input->indexInput.keys[ii]
                                                          .keyName),
                                                  NULL);
                BailIfFailed(status);
            }
        }

        break;

    case XcalarApiUnion:
    case XcalarApiJoin:
        break;
    case XcalarApiProject:
        for (unsigned ii = 0; ii < input->projectInput.numColumns; ii++) {
            size_t bufSize = sizeof(input->projectInput.columnNames[ii]);
            if (strstr(input->projectInput.columnNames[ii],
                       DfFatptrPrefixDelimiter)) {
                df->replaceFatptrPrefixDelims(
                    input->projectInput.columnNames[ii]);

                status = DataFormat::escapeNestedDelim(input->projectInput
                                                           .columnNames[ii],
                                                       bufSize,
                                                       NULL);
                BailIfFailed(status);
            }
        }

        break;

    case XcalarApiFilter:
        status = XcalarEval::get()
                     ->convertEvalString(input->filterInput.filterStr,
                                         dag->getUdfContainer(),
                                         sizeof(input->filterInput.filterStr),
                                         XcalarFnTypeEval,
                                         0,
                                         NULL,
                                         XcalarEval::ToImmediate);
        BailIfFailed(status);
        break;

    case XcalarApiGroupBy:
        for (unsigned ii = 0; ii < input->groupByInput.numEvals; ii++) {
            status = XcalarEval::get()
                         ->convertEvalString(input->groupByInput.evalStrs[ii],
                                             dag->getUdfContainer(),
                                             XcalarApiMaxEvalStringLen + 1,
                                             XcalarFnTypeAgg,
                                             0,
                                             NULL,
                                             XcalarEval::ToImmediate);
            BailIfFailed(status);
        }
        break;

    case XcalarApiMap:
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            status = XcalarEval::get()
                         ->convertEvalString(input->mapInput.evalStrs[ii],
                                             dag->getUdfContainer(),
                                             XcalarApiMaxEvalStringLen + 1,
                                             XcalarFnTypeEval,
                                             0,
                                             NULL,
                                             XcalarEval::ToImmediate);
            BailIfFailed(status);
        }
        break;

    case XcalarApiAggregate:
        status = XcalarEval::get()
                     ->convertEvalString(input->aggregateInput.evalStr,
                                         dag->getUdfContainer(),
                                         sizeof(input->aggregateInput.evalStr),
                                         XcalarFnTypeAgg,
                                         0,
                                         NULL,
                                         XcalarEval::ToImmediate);
        BailIfFailed(status);
        break;

    case XcalarApiExport:
        for (int ii = 0; ii < input->exportInput.meta.numColumns; ii++) {
            size_t bufSize = sizeof(input->exportInput.meta.columns[ii].name);
            if (strstr(input->exportInput.meta.columns[ii].name,
                       DfFatptrPrefixDelimiter)) {
                df->replaceFatptrPrefixDelims(
                    input->exportInput.meta.columns[ii].name);
                status = DataFormat::escapeNestedDelim(input->exportInput.meta
                                                           .columns[ii]
                                                           .name,
                                                       bufSize,
                                                       NULL);
                BailIfFailed(status);
            }
        }
        break;

    case XcalarApiSynthesize:
        if (!input->synthesizeInput.sameSession) {
            // we'll be pulling these columns from a modeling session, don't
            // convert names to immediate
            break;
        }

        for (unsigned ii = 0; ii < input->synthesizeInput.columnsCount; ii++) {
            size_t bufSize = sizeof(input->synthesizeInput.columns[ii].oldName);
            if (strstr(input->synthesizeInput.columns[ii].oldName,
                       DfFatptrPrefixDelimiter)) {
                df->replaceFatptrPrefixDelims(
                    input->synthesizeInput.columns[ii].oldName);
                status = DataFormat::escapeNestedDelim(input->synthesizeInput
                                                           .columns[ii]
                                                           .oldName,
                                                       bufSize,
                                                       NULL);
                BailIfFailed(status);
            }
        }
        break;

    default:
        break;
    }

CommonExit:
    return status;
}

// XXX: Why does this routine not need a udfContainer even though
// convertNamesToImmediate does? See calls to convertEvalString() below,
// where a NULL udfContainer is passed. This needs testing
Status
DagLib::convertNamesFromImmediate(XcalarApis api, XcalarApiInput *input)
{
    DataFormat *df = DataFormat::get();
    Status status = StatusOk;

    switch (api) {
    case XcalarApiIndex:
        for (unsigned ii = 0; ii < input->indexInput.numKeys; ii++) {
            if (input->indexInput.source.isTable &&
                strstr(input->indexInput.keys[ii].keyName,
                       DfFatptrPrefixDelimiterReplaced)) {
                df->revertFatptrPrefixDelims(
                    input->indexInput.keys[ii].keyName);
                DataFormat::unescapeNestedDelim(
                    input->indexInput.keys[ii].keyName);
            }
        }

        break;

    case XcalarApiUnion:
    case XcalarApiJoin:
        break;
    case XcalarApiProject:
        for (unsigned ii = 0; ii < input->projectInput.numColumns; ii++) {
            if (strstr(input->projectInput.columnNames[ii],
                       DfFatptrPrefixDelimiterReplaced)) {
                df->revertFatptrPrefixDelims(
                    input->projectInput.columnNames[ii]);

                DataFormat::unescapeNestedDelim(
                    input->projectInput.columnNames[ii]);
            }
        }

        break;

    case XcalarApiFilter:
        status = XcalarEval::get()
                     ->convertEvalString(input->filterInput.filterStr,
                                         NULL,  // XXX: test / check this
                                                // scenario
                                         sizeof(input->filterInput.filterStr),
                                         XcalarFnTypeEval,
                                         0,
                                         NULL,
                                         XcalarEval::FromImmediate);
        break;

    case XcalarApiGroupBy:
        for (unsigned ii = 0; ii < input->groupByInput.numEvals; ii++) {
            status = XcalarEval::get()
                         ->convertEvalString(input->groupByInput.evalStrs[ii],
                                             NULL,
                                             XcalarApiMaxEvalStringLen + 1,
                                             XcalarFnTypeAgg,
                                             0,
                                             NULL,
                                             XcalarEval::FromImmediate);
        }
        break;

    case XcalarApiMap:
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            status = XcalarEval::get()
                         ->convertEvalString(input->mapInput.evalStrs[ii],
                                             NULL,
                                             XcalarApiMaxEvalStringLen + 1,
                                             XcalarFnTypeEval,
                                             0,
                                             NULL,
                                             XcalarEval::FromImmediate);
        }
        break;

    case XcalarApiAggregate:
        status = XcalarEval::get()
                     ->convertEvalString(input->aggregateInput.evalStr,
                                         NULL,
                                         sizeof(input->aggregateInput.evalStr),
                                         XcalarFnTypeAgg,
                                         0,
                                         NULL,
                                         XcalarEval::FromImmediate);
        break;

    case XcalarApiExport:
        for (int ii = 0; ii < input->exportInput.meta.numColumns; ii++) {
            if (strstr(input->exportInput.meta.columns[ii].name,
                       DfFatptrPrefixDelimiterReplaced)) {
                df->revertFatptrPrefixDelims(
                    input->exportInput.meta.columns[ii].name);
                DataFormat::unescapeNestedDelim(
                    input->exportInput.meta.columns[ii].name);
            }
        }
        break;

    case XcalarApiSynthesize:
        for (unsigned ii = 0; ii < input->synthesizeInput.columnsCount; ii++) {
            if (strstr(input->synthesizeInput.columns[ii].oldName,
                       DfFatptrPrefixDelimiterReplaced)) {
                df->revertFatptrPrefixDelims(
                    input->synthesizeInput.columns[ii].oldName);
                DataFormat::unescapeNestedDelim(
                    input->synthesizeInput.columns[ii].oldName);
            }
        }
        break;

    default:
        break;
    }

    return status;
}

// Copy all UDFs referenced in the API 'input' param from 'fromUdfContainer'
// to 'toUdfContainer'. This is invoked during a DAG clone operation, in
// response to the CopyUDFs clone flag being set, when the containers are
// different (only known case is makeRetina()'s clone operation, which
// clones a workbook's DAG to the new retina's DAG - the two DAGs'
// containers would of course be different).
Status
DagLib::copyUDFs(XcalarApis api,
                 XcalarApiInput *input,
                 XcalarApiUdfContainer *fromUdfContainer,
                 XcalarApiUdfContainer *toUdfContainer)
{
    Status status = StatusOk;
    char *sudfNameDup = NULL;
    XcalarApiOutput *getUdfOutput = NULL;
    size_t getUdfOutputSize = 0;
    XcalarApiOutput *addUdfOutput = NULL;
    size_t addUdfOutputSize = 0;
    XcalarEvalAstCommon ast;
    bool astCreated = false;
    EvalUdfModuleSet udfModulesLocal;
    EvalUdfModuleSet *udfModules = &udfModulesLocal;
    EvalUdfModule *module;
    char *moduleNameDup = NULL;
    XcalarApiUdfGetInput getInput;
    UdfModuleSrc *newModule = NULL;
    const char *evalString;
    uint64_t numUdfModules = 0;
    uint64_t numUdfModulesLocal = 0;
    XcalarApiUdfContainer sharedUDFcontainer;

    assert(fromUdfContainer != NULL);
    assert(toUdfContainer != NULL);

    status = UserDefinedFunction::initUdfContainer(&sharedUDFcontainer,
                                                   NULL,
                                                   NULL,
                                                   NULL);
    BailIfFailed(status);

    switch (api) {
    case XcalarApiBulkLoad: {
        char fullUdfName[UdfVersionedFQFname];
        XcalarApiUdfGetInput getInput;
        char *sudfName = input->loadInput.loadArgs.parseArgs.parserFnName;

        if (sudfName[0] != '\0') {
            const char *udfModuleName;
            const char *udfModuleVersion;
            const char *udfFunctionName;
            verifyOk(strStrlcpy(fullUdfName, sudfName, sizeof(fullUdfName)));
            status = UserDefinedFunction::parseFunctionName(fullUdfName,
                                                            &udfModuleName,
                                                            &udfModuleVersion,
                                                            &udfFunctionName);
            BailIfFailed(status);
            moduleNameDup = strAllocAndCopy(udfModuleName);
            BailIfNull(moduleNameDup);

            // skip default module
            if (strncmp(basename(moduleNameDup),
                        UserDefinedFunction::DefaultModuleName,
                        strlen(UserDefinedFunction::DefaultModuleName)) != 0) {
                // get UDF from the fromUdfContainer (assume workbook in
                // log)
                verifyOk(strStrlcpy(getInput.moduleName,
                                    basename(moduleNameDup),
                                    sizeof(getInput.moduleName)));
                status = UserDefinedFunction::get()->getUdf(&getInput,
                                                            fromUdfContainer,
                                                            &getUdfOutput,
                                                            &getUdfOutputSize);
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed to get UDF '%s' from workbook '%s': %s",
                                getInput.moduleName,
                                fromUdfContainer->sessionInfo
                                            .sessionNameLength == 0
                                    ? ""
                                    : fromUdfContainer->sessionInfo.sessionName,
                                strGetFromStatus(status));

                newModule = &getUdfOutput->outputResult.udfGetOutput;
                assert(isValidUdfType(newModule->type));

                // add UDF to the toUdfContainer (assume retina in log).
                // Ignore failures due to DUPs (use addUdfIgnoreDup()).
                status = UserDefinedFunction::get()
                             ->addUdfIgnoreDup(newModule,
                                               toUdfContainer,
                                               &addUdfOutput,
                                               &addUdfOutputSize);
                if (status == StatusUdfModuleInUse) {
                    // In case there is failure to compare bc the UDF is in use
                    // with exclusive access, let the caller retry later.
                    status = StatusBusy;
                }
                BailIfFailedMsg(moduleName,
                                status,
                                "Failed to add UDF '%s' to retina '%s': %s",
                                newModule->moduleName,
                                toUdfContainer->retinaName,
                                strGetFromStatus(status));
            }
        }
    } break;
    case XcalarApiMap: {
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            numUdfModules = 0;

            status =
                XcalarEval::get()->parseEvalStr(input->mapInput.evalStrs[ii],
                                                XcalarFnTypeEval,
                                                fromUdfContainer,
                                                &ast,
                                                NULL,
                                                NULL,
                                                NULL,
                                                NULL);
            BailIfFailed(status);
            astCreated = true;
            status = XcalarEval::get()->getUdfModules(ast.rootNode,
                                                      udfModules,
                                                      &numUdfModules);
            BailIfFailed(status);

            numUdfModulesLocal += numUdfModules;

            assert(astCreated);
            XcalarEval::get()->freeCommonAst(&ast);
            astCreated = false;
        }
        assert(status == StatusOk);
    } break;

    default: {
        evalString = xcalarApiGetEvalStringFromInput(api, input);
        if (evalString == NULL) {
            break;
        }
        // XXX: Check how/why XcalarFnTypeAgg matters here. This is what
        // searchQueryGraph() does, so stick to this but understand the
        // logic
        status = XcalarEval::get()->parseEvalStr(evalString,
                                                 XcalarFnTypeAgg,
                                                 fromUdfContainer,
                                                 &ast,
                                                 NULL,
                                                 NULL,
                                                 NULL,
                                                 NULL);
        BailIfFailed(status);
        astCreated = true;
        status = XcalarEval::get()->getUdfModules(ast.rootNode,
                                                  udfModules,
                                                  &numUdfModules);
        BailIfFailed(status);
        numUdfModulesLocal = numUdfModules;
        assert(astCreated);
        XcalarEval::get()->freeCommonAst(&ast);
        astCreated = false;
    }  // default
    break;

    }  // switch end

    if (numUdfModulesLocal != 0) {
        for (EvalUdfModuleSet::iterator it = udfModules->begin();
             (module = it.get()) != NULL;
             it.next()) {
            numUdfModulesLocal--;
            if (moduleNameDup != NULL) {
                memFree(moduleNameDup);
                moduleNameDup = NULL;
            }
            moduleNameDup = strAllocAndCopy(module->getName());
            BailIfNull(moduleNameDup);

            if (strncmp(basename(moduleNameDup),
                        UserDefinedFunction::DefaultModuleName,
                        strlen(UserDefinedFunction::DefaultModuleName)) != 0) {
                verifyOk(strStrlcpy(getInput.moduleName,
                                    basename(moduleNameDup),
                                    sizeof(getInput.moduleName)));

                // get the UDF from the 'fromUdfContainer'
                if (getUdfOutput != NULL) {
                    memFree(getUdfOutput);
                    getUdfOutput = NULL;
                }
                status = UserDefinedFunction::get()->getUdf(&getInput,
                                                            fromUdfContainer,
                                                            &getUdfOutput,
                                                            &getUdfOutputSize);
                if (status == StatusUdfModuleNotFound) {
                    status =
                        UserDefinedFunction::get()->getUdf(&getInput,
                                                           &sharedUDFcontainer,
                                                           &getUdfOutput,
                                                           &getUdfOutputSize);
                }

                BailIfFailed(status);

                newModule = &getUdfOutput->outputResult.udfGetOutput;
                assert(isValidUdfType(newModule->type));

                // add the UDF to the 'toUdfContainer'
                if (addUdfOutput != NULL) {
                    memFree(addUdfOutput);
                    addUdfOutput = NULL;
                }
                // When adding the module, ignore any real DUPs (i.e. module
                // with the same name exists, and has the same source code).
                status = UserDefinedFunction::get()
                             ->addUdfIgnoreDup(newModule,
                                               toUdfContainer,
                                               &addUdfOutput,
                                               &addUdfOutputSize);
                BailIfFailed(status);
            }  // process a UDF module in list
        }      // for loop
        assert(numUdfModulesLocal == 0);
    }
CommonExit:
    udfModules->removeAll(&EvalUdfModule::del);

    if (sudfNameDup != NULL) {
        memFree(sudfNameDup);
        sudfNameDup = NULL;
    }
    if (getUdfOutput != NULL) {
        memFree(getUdfOutput);
        getUdfOutput = NULL;
    }
    if (addUdfOutput != NULL) {
        memFree(addUdfOutput);
        addUdfOutput = NULL;
    }
    if (moduleNameDup != NULL) {
        memFree(moduleNameDup);
        moduleNameDup = NULL;
    }
    if (astCreated) {
        assert(status != StatusOk);
        XcalarEval::get()->freeCommonAst(&ast);
        astCreated = false;
    }
    return status;
}

// When copying or cloning dags, esp. those which will be persisted, or
// downloaded, any UDF references in it must be converted to have relative
// names, not the absolute path names to the UDFs
Status
DagLib::convertUDFNamesToRelative(XcalarApis api,
                                  XcalarApiUdfContainer *udfContainer,
                                  XcalarApiInput *input)
{
    Status status = StatusOk;
    char *udfModuleNameDup = NULL;
    const char *udfModuleName;
    const char *udfModuleVersion;
    const char *udfFunctionName;
    char udfModuleFunctionName[XcalarApiMaxUdfModuleNameLen + 1];
    char parserFnNameCopy[UdfVersionedFQFname];

    switch (api) {
    case XcalarApiMap:
        // mapInput has to be deserialized to ensure evalStrs[] pointers in
        // mapInput are pointing to the strings[] block in mapInput - so
        // that any upcoming mods to the eval strings occur to the right
        // memory locations.
        xcalarApiDeserializeMapInput(&input->mapInput);
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            if (UserDefinedFunction::get()->absUDFinEvalStr(
                    input->mapInput.evalStrs[ii])) {
                status = XcalarEval::get()->makeUDFRelativeInEvalString(
                    input->mapInput.evalStrs[ii],
                    XcalarApiMaxEvalStringLen + 1,
                    XcalarFnTypeEval,
                    udfContainer,
                    XcalarEval::UDFToRelative);
                BailIfFailed(status);
            }
        }
        break;

    case XcalarApiBulkLoad: {
        char *sudfName = input->loadInput.loadArgs.parseArgs.parserFnName;

        if (sudfName[0] != '\0' && strchr(sudfName, '/') != NULL) {
            // Copy parser name as parseFunctionName modifies what is
            // passed to it.
            verifyOk(strStrlcpy(parserFnNameCopy,
                                sudfName,
                                sizeof(parserFnNameCopy)));

            status = UserDefinedFunction::parseFunctionName(parserFnNameCopy,
                                                            &udfModuleName,
                                                            &udfModuleVersion,
                                                            &udfFunctionName);
            BailIfFailed(status);

            udfModuleNameDup = strAllocAndCopy(udfModuleName);
            BailIfNull(udfModuleNameDup);

            snprintf(udfModuleFunctionName,
                     sizeof(udfModuleFunctionName),
                     "%s:%s",
                     basename(udfModuleNameDup),
                     udfFunctionName);

            verifyOk(
                strStrlcpy(input->loadInput.loadArgs.parseArgs.parserFnName,
                           udfModuleFunctionName,
                           sizeof(input->loadInput.loadArgs.parseArgs
                                      .parserFnName)));
        }
    } break;

    case XcalarApiFilter:  // XXX: add support for this - blocked by SDK-365
        break;

    default:
        break;
    }

CommonExit:
    if (udfModuleNameDup != NULL) {
        memFree(udfModuleNameDup);
        udfModuleNameDup = NULL;
    }
    return status;
}

void
DagLib::renameDstApiInput(DagNodeTypes::Node *dagNode,
                          const char *oldName,
                          const char *newName)
{
    XcalarApis api = dagNode->dagNodeHdr.apiDagNodeHdr.api;
    XcalarApiInput *input = dagNode->dagNodeHdr.apiInput;

    switch (api) {
    case XcalarApiIndex:
        assert(strcmp(oldName, input->indexInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->indexInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiJoin:
        assert(strcmp(oldName, input->joinInput.joinTable.tableName) == 0);
        verifyOk(strStrlcpy(input->joinInput.joinTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiUnion:
        assert(strcmp(oldName, input->unionInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->unionInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiProject:
        assert(strcmp(oldName, input->projectInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->projectInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiFilter:
        assert(strcmp(oldName, input->filterInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->filterInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiGroupBy:
        assert(strcmp(oldName, input->groupByInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->groupByInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));

        break;

    case XcalarApiMap:
        assert(strcmp(oldName, input->mapInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->mapInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiSynthesize:
        assert(strcmp(oldName, input->synthesizeInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->synthesizeInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiAggregate:
        assert(strcmp(oldName, input->aggregateInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->aggregateInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiGetRowNum:
        assert(strcmp(oldName, input->getRowNumInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->getRowNumInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiExecuteRetina:
        assert(strcmp(oldName, input->executeRetinaInput.dstTable.tableName) ==
               0);
        verifyOk(strStrlcpy(input->executeRetinaInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiBulkLoad:
        assert(strcmp(oldName, input->loadInput.datasetName) == 0);
        verifyOk(strStrlcpy(input->loadInput.datasetName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiSelect:
        assert(strcmp(oldName, input->selectInput.dstTable.tableName) == 0);
        verifyOk(strStrlcpy(input->selectInput.dstTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiExport:
    case XcalarApiDeleteObjects:
        break;

    default:
        assert(0);
        break;
    }
}

void
DagLib::renameSrcApiInput(const char *oldName,
                          const char *newName,
                          DagNodeTypes::Node *dagNode)
{
    XcalarApis api = dagNode->dagNodeHdr.apiDagNodeHdr.api;
    XcalarApiInput *input = dagNode->dagNodeHdr.apiInput;

    switch (api) {
    case XcalarApiIndex:
        verifyOk(strStrlcpy(input->indexInput.source.name,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiUnion:
        for (unsigned ii = 0; ii < input->unionInput.numSrcTables; ii++) {
            if (strcmp(input->unionInput.srcTables[ii].tableName, oldName) ==
                0) {
                verifyOk(strStrlcpy(input->unionInput.srcTables[ii].tableName,
                                    newName,
                                    DagTypes::MaxNameLen));
            }
        }
        break;

    case XcalarApiJoin:
        if (oldName == NULL ||
            strcmp(oldName, input->joinInput.leftTable.tableName) == 0) {
            verifyOk(strStrlcpy(input->joinInput.leftTable.tableName,
                                newName,
                                DagTypes::MaxNameLen));
        }

        if (oldName == NULL ||
            strcmp(oldName, input->joinInput.rightTable.tableName) == 0) {
            verifyOk(strStrlcpy(input->joinInput.rightTable.tableName,
                                newName,
                                DagTypes::MaxNameLen));
        }
        break;

    case XcalarApiProject:
        verifyOk(strStrlcpy(input->projectInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiFilter:
        verifyOk(strStrlcpy(input->filterInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));

        XcalarEval::replaceAggregateVariableName(oldName,
                                                 newName,
                                                 input->filterInput.filterStr);
        break;

    case XcalarApiGroupBy:
        verifyOk(strStrlcpy(input->groupByInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));

        for (unsigned ii = 0; ii < input->groupByInput.numEvals; ii++) {
            XcalarEval::replaceAggregateVariableName(oldName,
                                                     newName,
                                                     input->groupByInput
                                                         .evalStrs[ii]);
        }

        break;

    case XcalarApiMap:
        verifyOk(strStrlcpy(input->mapInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        for (unsigned ii = 0; ii < input->mapInput.numEvals; ii++) {
            XcalarEval::replaceAggregateVariableName(oldName,
                                                     newName,
                                                     input->mapInput
                                                         .evalStrs[ii]);
        }
        break;

    case XcalarApiSynthesize:
        verifyOk(strStrlcpy(input->synthesizeInput.source.name,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiAggregate:
        verifyOk(strStrlcpy(input->aggregateInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiGetRowNum:
        verifyOk(strStrlcpy(input->getRowNumInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiExport:
        verifyOk(strStrlcpy(input->exportInput.srcTable.tableName,
                            newName,
                            DagTypes::MaxNameLen));
        break;

    case XcalarApiSelect:
    case XcalarApiExecuteRetina:
    case XcalarApiBulkLoad:
        break;

    default:
        assert(0);
        break;
    }
}

// XXX Another potential landmine, where we
// depend on api to know if the underlying data
// structure is an XDB or not
// Should instead, given a dagNode, retrieve its context,
// and get the contextType
SourceType
DagLib::getSourceTypeFromApi(XcalarApis api)
{
    switch (api) {
    case XcalarApiBulkLoad:
        return SrcDataset;
    case XcalarApiIndex:
    case XcalarApiMap:
    case XcalarApiSynthesize:
    case XcalarApiFilter:
    case XcalarApiGroupBy:
    case XcalarApiJoin:
    case XcalarApiUnion:
    case XcalarApiProject:
    case XcalarApiGetRowNum:
    case XcalarApiExecuteRetina:
    case XcalarApiSelect:
        return SrcTable;
    case XcalarApiExport:
        return SrcExport;
    case XcalarApiAggregate:
        return SrcConstant;
    default:
        return SrcUnknown;
    }
}

Status
DagLib::copyXcalarApiDagNodeToXcalarApiDagNode(XcalarApiDagNode *apiDagNodeOut,
                                               size_t bufSize,
                                               XcalarApiDagNode *apiDagNodeIn,
                                               size_t *bytesCopiedOut)
{
    Status status = StatusUnknown;
    size_t bytesCopied;

    bytesCopied = sizeOfXcalarApiDagNode(apiDagNodeIn->hdr.inputSize,
                                         apiDagNodeIn->numParents,
                                         apiDagNodeIn->numChildren);
    if (bufSize < bytesCopied) {
        status = StatusOverflow;
        goto CommonExit;
    }

    memcpy(apiDagNodeOut, apiDagNodeIn, bytesCopied);
    *bytesCopiedOut = bytesCopied;
    status = StatusOk;
CommonExit:
    return status;
}

void
DagLib::getXcalarApiOpStatusComplete(MsgEphemeral *eph, void *payload)
{
    OpDetails *opDetails = (OpDetails *) payload;
    Dag::OpDetailWrapper *opDetailWrapperArray =
        (Dag::OpDetailWrapper *) eph->ephemeral;
    NodeId dstNodeId = MsgMgr::get()->getMsgDstNodeId(eph);

    opDetailWrapperArray[dstNodeId].status = eph->status;
    if (eph->status == StatusOk) {
        memcpy(&opDetailWrapperArray[dstNodeId].opDetails,
               opDetails,
               sizeof(*opDetails));
    }
}

void
DagLib::cancelOpLocal(MsgEphemeral *eph, void *payload)
{
    DagNodeTypes::Node *node = NULL;
    Status status = StatusOk;
    DgDagState state;
    DagNodeOpInfo *cancelOpInfo = (DagNodeOpInfo *) payload;
    DagLib *dagLib = DagLib::get();
    bool dagLocked = false;

    DagTypes::NodeId dagNodeId = cancelOpInfo->dagNodeId;
    DagTypes::DagId dagId = cancelOpInfo->dagId;

    Dag *dag = dagLib->getDagLocal(dagId);
    // always succeed by design
    assert(dag != NULL);

    dag->lock();
    dagLocked = true;

    status = dag->lookupNodeById(dagNodeId, &node);
    if (status != StatusOk) {
        // This can happen during a createDagNode race. The
        // createDagNodeLocal just hasn't been processed on this node yet
        assert(status == StatusDagNodeNotFound);
        goto CommonExit;
    }

    state = node->dagNodeHdr.apiDagNodeHdr.state;

    if (state == DgDagStateProcessing || state == DgDagStateQueued ||
        state == DgDagStateCreated) {
        if (node->dagNodeHdr.apiDagNodeHdr.api == XcalarApiExecuteRetina &&
            Config::get()->getMyNodeId() == 0) {
            status = QueryManager::get()->requestQueryCancel(
                node->dagNodeHdr.apiInput->executeRetinaInput.queryName);
        }
        node->opStatus.atomicOpDetails.cancelled = true;
    } else {
        assert(state != DgDagStateUnknown);
        status = StatusOperationHasFinished;
    }

CommonExit:
    if (dagLocked) {
        dag->unlock();
        dagLocked = false;
    }

    eph->setAckInfo(status, 0);
}

void
DagLib::getXcalarApiOpStatusLocal(MsgEphemeral *eph, void *payload)
{
    Status status;
    Status statusGetSlotInfo;
    OperatorsStatusInput *input = (OperatorsStatusInput *) payload;
    XcalarApis api = XcalarApiUnknown;
    Dag *dag;
    DagTypes::NodeId dagNodeId;
    OpStatus *opStatus;
    OpDetails *outOpDetail;
    DgDagState state = DgDagStateUnknown;
    DagLib *dagLib = DagLib::get();
    size_t payloadLength = 0;
    XcalarConfig *xc = XcalarConfig::get();
    unsigned myNode = Config::get()->getMyNodeId();

    dag = dagLib->getDagLocal(input->dagId);
    if (dag == NULL) {
        status = StatusDgDagNotFound;
        goto CommonExit;
    }

    dagNodeId = input->dagNodeId;

    input = NULL;

    // Repurpose payload for our output
    outOpDetail = (OpDetails *) payload;

    status = dag->getDagNodeState(dagNodeId, &state);

    if (status != StatusOk) {
        goto CommonExit;
    }

    if (state == DgDagStateError) {
        status = StatusDgDagNodeError;
        goto CommonExit;
    }

    status = dag->getOpStatus(dagNodeId, &opStatus);
    if (status != StatusOk) {
        goto CommonExit;
    }

    outOpDetail->numWorkCompleted =
        atomicRead64(&opStatus->atomicOpDetails.numWorkCompletedAtomic);
    outOpDetail->numWorkTotal = opStatus->atomicOpDetails.numWorkTotal;

    XdbMgr::get()
        ->xdbGetNumLocalRowsFromXdbId(dag->getXdbIdFromNodeId(dagNodeId),
                                      &outOpDetail->perNode[myNode].numRows);
    XdbMgr::get()
        ->xdbGetSizeInBytesFromXdbId(dag->getXdbIdFromNodeId(dagNodeId),
                                     &outOpDetail->perNode[myNode].sizeBytes);

    XdbMgr::get()->xdbGetNumTransPagesReceived(dag->getXdbIdFromNodeId(
                                                   dagNodeId),
                                               &outOpDetail->perNode[myNode]
                                                    .numTransPagesReceived);

    if (xc->collectStats_ && xc->collectDataflowStats_ &&
        xc->includeTableMetaStats_) {
        XdbMgr::get()
            ->xdbGetHashSlotSkew(dag->getXdbIdFromNodeId(dagNodeId),
                                 &outOpDetail->perNode[myNode].hashSlotSkew);

    } else {
        outOpDetail->perNode[myNode].hashSlotSkew = -1;
    }

    outOpDetail->cancelled = opStatus->atomicOpDetails.cancelled;

    status = dag->getDagNodeApi(dagNodeId, &api);
    if (status != StatusOk) {
        goto CommonExit;
    }

    outOpDetail->errorStats = opStatus->atomicOpDetails.errorStats;

    payloadLength = sizeof(OpDetails);

CommonExit:

    eph->setAckInfo(status, payloadLength);
}

Status
DagLib::insertNodeToGlobalIdTable(DagNodeTypes::Node *node)
{
    Status status;
    dagNodeTableIdLock_.lock();
    status = dagNodeTableById_.insert(node);
    dagNodeTableIdLock_.unlock();
    return status;
}

DagNodeTypes::Node *
DagLib::removeNodeFromGlobalIdTable(DagNodeTypes::Node *node)
{
    DagNodeTypes::Node *retNode;
    dagNodeTableIdLock_.lock();
    retNode = dagNodeTableById_.remove(node->getId());
    dagNodeTableIdLock_.unlock();
    return retNode;
}

Status
DagLib::lookupNodeById(DagTypes::NodeId nodeId)
{
    assert(nodeId != DagTypes::InvalidDagNodeId);
    Status status;
    dagNodeTableIdLock_.lock();
    if (!dagNodeTableById_.find(nodeId)) {
        status = StatusDagNodeNotFound;
    }
    dagNodeTableIdLock_.unlock();
    return status;
}
