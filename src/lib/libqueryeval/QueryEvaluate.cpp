// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>

#include "primitives/Primitives.h"

#include "optimizer/Optimizer.h"
#include "xdb/Xdb.h"
#include "msg/Message.h"
// With libgraph, the query library will define its own query nodes,
// rather than have to rely on a common DagNodeTypes::Node
#include "bc/BufferCache.h"
#include "dag/DagTypes.h"
#include "dag/DagLib.h"
#include "queryeval/QueryEvaluate.h"
#include "sys/XLog.h"
#include "util/MemTrack.h"
#include "libapis/LibApisRecv.h"
#include "libapis/WorkItem.h"
#include "operators/Operators.h"
#include "xdb/Xdb.h"
#include "util/IntHashTable.h"
#include "dataset/Dataset.h"
#include "primitives/Macros.h"
#include "usr/Users.h"

QueryEvaluate *QueryEvaluate::instance = NULL;
static constexpr const char *moduleName = "libqueryeval";

QueryEvaluate *
QueryEvaluate::get()
{
    return QueryEvaluate::instance;
}

Status
QueryEvaluate::init()
{
    Status status;
    assert(instance == NULL);
    instance = new (std::nothrow) QueryEvaluate;
    if (instance == NULL) {
        return StatusNoMem;
    }

    status = instance->initInternal();
    if (status != StatusOk) {
        delete instance;
        instance = NULL;
    }
    return status;
}

Status
QueryEvaluate::initInternal()
{
    Status status = StatusUnknown;

    runningWorkItemNodeBcHandle_ =
        BcHandle::create(BufferCacheObjects::QueryEvalRunningElems);
    if (runningWorkItemNodeBcHandle_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    runnableWorkItemNodeBcHandle_ =
        BcHandle::create(BufferCacheObjects::QueryEvalRunnableElems);
    if (runnableWorkItemNodeBcHandle_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    completedWorkItemNodeBcHandle_ =
        BcHandle::create(BufferCacheObjects::QueryEvalCompletedElems);
    if (completedWorkItemNodeBcHandle_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        destroyInternal();
    }
    return status;
}

void
QueryEvaluate::destroy()
{
    if (instance == NULL) {
        return;
    }

    instance->destroyInternal();
    delete instance;
    instance = NULL;
}

void
QueryEvaluate::destroyInternal()
{
    if (runnableWorkItemNodeBcHandle_ != NULL) {
        BcHandle::destroy(&runnableWorkItemNodeBcHandle_);
        runnableWorkItemNodeBcHandle_ = NULL;
    }

    if (runningWorkItemNodeBcHandle_ != NULL) {
        BcHandle::destroy(&runningWorkItemNodeBcHandle_);
        runningWorkItemNodeBcHandle_ = NULL;
    }

    if (completedWorkItemNodeBcHandle_ != NULL) {
        BcHandle::destroy(&completedWorkItemNodeBcHandle_);
        completedWorkItemNodeBcHandle_ = NULL;
    }
}

bool
QueryEvaluate::parentsInWorkspace(Dag *workspaceGraph, DagNodeTypes::Node *node)
{
    if (node->numParent == 0) {
        return false;
    }

    DagNodeTypes::NodeIdListElt *parentElt = node->parentsList;
    while (parentElt) {
        DagNodeTypes::Node *parent;
        Status status =
            workspaceGraph->lookupNodeById(parentElt->nodeId, &parent);
        assert(status == StatusOk);

        // check if synthesize is re-using the xdb from another workspace
        if (parent->dagNodeHdr.apiDagNodeHdr.api == XcalarApiSynthesize &&
            parent->dagNodeHdr.apiInput->synthesizeInput.sameSession == false &&
            parent->dagNodeHdr.apiInput->synthesizeInput.source.xid ==
                parent->xdbId) {
            return false;
        }

        parentElt = parentElt->next;
    }

    return true;
}

Runtime::SchedId
QueryEvaluate::pickSchedToRun()
{
    // Round robin operator on all the operator runtime schedulers, i.e.
    // Sched0, Sched1 and Sched2.
    return static_cast<Runtime::SchedId>(atomicInc64(&opSchedRoundRobin_) %
                                         Runtime::TotalFastPathScheds);
}

Status
QueryEvaluate::executeQueryGraphNode(XcalarWorkItem **workItemOut,
                                     OperatorHandler *operatorHandler,
                                     void *optimizerAnnotations)
{
    XcalarWorkItem *workItem = NULL;
    Status status = StatusUnknown;
    XcalarApiInput *input = NULL;
    size_t inputSize = 0;

    xSyslog(moduleName,
            XlogDebug,
            "execute name %s api:%s",
            operatorHandler->getDstNodeName(),
            strGetFromXcalarApis(operatorHandler->getApi()));

    inputSize = operatorHandler->getInputSize();
    input = (XcalarApiInput *) memAlloc(inputSize);
    if (input == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memcpy(input, operatorHandler->getInput(), inputSize);
    workItem = xcalarApiMakeGenericWorkItem(operatorHandler->getApi(),
                                            input,
                                            inputSize);
    if (workItem == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    input = NULL;  // Handed off to workItem

    status = operatorHandler->run(&workItem->output,
                                  &workItem->outputSize,
                                  optimizerAnnotations);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = StatusOk;
CommonExit:
    if (status != StatusOk) {
        if (input != NULL) {
            assert(workItem == NULL);
            memFree(input);
            input = NULL;
        }
    }

    if (workItem != NULL) {
        if (workItem->output != NULL) {
            workItem->output->hdr.status = status.code();
        }
    }
    *workItemOut = workItem;
    workItem = NULL;

    assert(input == NULL);

    return status;
}

void
QueryEvaluate::CompletedWorkItemNode::del()
{
    QueryEvaluate::get()->freeCompletedDictEle(this);
}

void
QueryEvaluate::freeCompletedDictEle(CompletedWorkItemNode *node)
{
    completedWorkItemNodeBcHandle_->freeBuf(node);
}

void
QueryEvaluate::freeCompletedDict(CompletedDictHashTable *completedDict)
{
    assert(completedDict != NULL);
    completedDict->removeAll(&CompletedWorkItemNode::del);
}

void
QueryEvaluate::freeRunningWorkItemNode(RunningWorkItemNode *runningWorkItemNode)
{
    assert(runningWorkItemNode != NULL);

    if (runningWorkItemNode->workItem != NULL) {
        xcalarApiFreeWorkItem(runningWorkItemNode->workItem);
        runningWorkItemNode->workItem = NULL;
    }

    if (runningWorkItemNode->operatorHandler != NULL) {
        delete runningWorkItemNode->operatorHandler;
        runningWorkItemNode->operatorHandler = NULL;
    }

    assert(runningWorkItemNode->workItem == NULL);
    assert(runningWorkItemNode->operatorHandler == NULL);

    runningWorkItemNodeBcHandle_->freeBuf(runningWorkItemNode);
    runningWorkItemNode = NULL;
}

// Prep the data structures before making a call to executeQueryGraphNode
// XXX TODO ENG-9062
// This API needs to be fixed for Shared Table abstraction
Status
QueryEvaluate::executeQueryGraphNodePre(
    XcalarApiUserId *userId,
    Dag *workspaceGraph,
    Dag *queryGraph,
    DagTypes::NodeId queryGraphNodeId,
    bool dropSrcSlots,
    QueryManager::QueryJob *queryJob,
    RunningWorkItemNode **runningWorkItemNodeOut,
    CompletedDictHashTable *completedDict,
    Runtime::SchedId schedId)
{
    Status status = StatusUnknown;
    DagNodeTypes::Node *queryGraphNode = NULL, *workspaceGraphNode = NULL;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    RunningWorkItemNode *runningWorkItemNode = NULL;
    uint64_t numParent;
    DagTypes::NodeId *parentNodeIds = NULL;
    DagTypes::NodeId *parentNodeIdsWorkspace = NULL;
    XcalarApiUdfContainer *parentUdfContainers = NULL;
    DagTypes::DagId *parentGraphIds = NULL;
    unsigned ii;
    CompletedWorkItemNode *completedWorkItem = NULL;
    OperatorHandler *operatorHandler = NULL;
    Optimizer::DagNodeAnnotations *annotations;
    Txn savedTxn;
    Txn::Mode mode = Txn::Mode::Invalid;
    bool txnIdSwapped = false;
    Dag **parentGraphs = NULL;

    status = queryGraph->lookupNodeById(queryGraphNodeId, &queryGraphNode);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to dag->lookupNodeById: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    queryGraphNode->stopwatch.restart();

    runningWorkItemNode =
        (RunningWorkItemNode *) runningWorkItemNodeBcHandle_->allocBuf(
            XidInvalid, &status);
    if (runningWorkItemNode == NULL) {
        goto CommonExit;
    }

    runningWorkItemNode->workItemFailed = false;
    runningWorkItemNode->workItem = NULL;
    runningWorkItemNode->queryGraphNode = queryGraphNode;
    runningWorkItemNode->queryGraphNodeId = queryGraphNodeId;
    runningWorkItemNode->workspaceGraphNode = NULL;
    runningWorkItemNode->workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    runningWorkItemNode->operatorHandler = NULL;

    // Preserve uuid is set to true becuase we should just trust the queryGraph
    assert(operatorHandler == NULL);
    status = xcApiGetOperatorHandlerUninited(&operatorHandler,
                                             queryGraphNode->dagNodeHdr
                                                 .apiDagNodeHdr.api,
                                             true);
    if (status != StatusOk) {
        goto CommonExit;
    }

    assert(operatorHandler->needsSessionOrGraph());

    status = operatorHandler->init(userId, NULL, workspaceGraph);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to initialize operatorHandler %s: %s",
                strGetFromXcalarApis(
                    queryGraphNode->dagNodeHdr.apiDagNodeHdr.api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    annotations = (Optimizer::DagNodeAnnotations *) queryGraphNode->annotations;

    if (annotations != NULL) {
        mode = Txn::Mode::LRQ;
    } else {
        mode = Txn::Mode::NonLRQ;
    }

    savedTxn = Txn::currentTxn();

    if (schedId == Runtime::SchedId::MaxSched) {
        schedId = pickSchedToRun();
    }
    assert(static_cast<uint8_t>(schedId) < Runtime::TotalFastPathScheds);

    status = operatorHandler->setTxnAndTxnLog(mode, schedId);
    BailIfFailed(status);
    txnIdSwapped = true;

    xSyslog(moduleName,
            XlogInfo,
            "Query graph state %s parent: %lu,%hhu,%hhu, graph-node %s, api %s",
            queryJob->queryName,
            savedTxn.id_,
            (unsigned char) savedTxn.rtType_,
            static_cast<uint8_t>(savedTxn.rtSchedId_),
            queryGraphNode->dagNodeHdr.apiDagNodeHdr.name,
            strGetFromXcalarApis(queryGraphNode->dagNodeHdr.apiDagNodeHdr.api));

    status =
        operatorHandler->setArgQueryGraph(queryGraphNode->dagNodeHdr.apiInput,
                                          queryGraphNode->dagNodeHdr
                                              .apiDagNodeHdr.inputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set argument for operatorHandler %s: %s",
                strGetFromXcalarApis(
                    queryGraphNode->dagNodeHdr.apiDagNodeHdr.api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    // XXX TODO
    // For now only fix the synthesize case of shared tables.
    if (queryGraphNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiSynthesize &&
        !queryGraphNode->dagNodeHdr.apiInput->synthesizeInput.sameSession &&
        queryGraphNode->dagNodeHdr.apiInput->synthesizeInput.source.isTable) {
        numParent = 1;

        parentGraphs = new (std::nothrow) Dag *[numParent];
        BailIfNull(parentGraphs);

        parentUdfContainers =
            new (std::nothrow) XcalarApiUdfContainer[numParent];
        BailIfNull(parentUdfContainers);

        parentGraphIds = new (std::nothrow) DagTypes::DagId[numParent];
        BailIfNull(parentGraphIds);

        parentNodeIdsWorkspace = new (std::nothrow) DagTypes::NodeId[numParent];
        BailIfNull(parentNodeIdsWorkspace);

        status =
            OperatorHandler::getSourceDagNode(queryGraphNode->dagNodeHdr
                                                  .apiInput->synthesizeInput
                                                  .source.name,
                                              workspaceGraph,
                                              &parentUdfContainers[0],
                                              &parentGraphIds[0],
                                              &parentNodeIdsWorkspace[0]);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to get source %s: %s",
                        queryGraphNode->dagNodeHdr.apiInput->synthesizeInput
                            .source.name,
                        strGetFromStatus(status));
        parentGraphs[0] = DagLib::get()->getDagLocal(parentGraphIds[0]);

        if (workspaceGraph->getId() != parentGraphIds[0]) {
            UserMgr *usrMgr = UserMgr::get();
            status = usrMgr->trackOutstandOps(&parentUdfContainers[0],
                                              UserMgr::OutstandOps::Inc);
            BailIfFailedMsg(moduleName,
                            status,
                            "Failed trackOutstandOps userName '%s', session "
                            "'%s', Id %ld: %s",
                            parentUdfContainers[0].userId.userIdName,
                            parentUdfContainers[0].sessionInfo.sessionName,
                            parentUdfContainers[0].sessionInfo.sessionId,
                            strGetFromStatus(status));
        }
    } else {
        status = queryGraph->getParentDagNodeId(queryGraphNodeId,
                                                &parentNodeIds,
                                                &numParent);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to dag->getParentDagNodeId: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        if (numParent) {
            parentGraphs = new (std::nothrow) Dag *[numParent];
            BailIfNull(parentGraphs);

            parentUdfContainers =
                new (std::nothrow) XcalarApiUdfContainer[numParent];
            BailIfNull(parentUdfContainers);

            parentGraphIds = new (std::nothrow) DagTypes::DagId[numParent];
            BailIfNull(parentGraphIds);

            parentNodeIdsWorkspace =
                new (std::nothrow) DagTypes::NodeId[numParent];
            BailIfNull(parentNodeIdsWorkspace);

            for (ii = 0; ii < numParent; ++ii) {
                completedWorkItem = completedDict->find(parentNodeIds[ii]);
                assert(completedWorkItem != NULL);

                parentNodeIdsWorkspace[ii] =
                    completedWorkItem->workspaceGraphNodeId;
                parentGraphs[ii] = workspaceGraph;
                UserDefinedFunction::copyContainers(&parentUdfContainers[ii],
                                                    workspaceGraph
                                                        ->getUdfContainer());
                parentGraphIds[ii] = workspaceGraph->getId();
            }
        }
    }

    operatorHandler->setParents(numParent,
                                parentNodeIdsWorkspace,
                                parentUdfContainers,
                                parentGraphIds,
                                parentGraphs);

    status = operatorHandler
                 ->createDagNode(&workspaceGraphNodeId,
                                 DagTypes::QueryGraph,
                                 queryGraphNode->dagNodeHdr.apiDagNodeHdr.name,
                                 numParent,
                                 parentGraphs,
                                 parentNodeIdsWorkspace);
    parentNodeIdsWorkspace = NULL;
    parentUdfContainers = NULL;
    parentGraphIds = NULL;
    parentGraphs = NULL;

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to createDagNode for operatorHandler %s: %s",
                strGetFromXcalarApis(
                    queryGraphNode->dagNodeHdr.apiDagNodeHdr.api),
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (workspaceGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = workspaceGraph->lookupNodeById(workspaceGraphNodeId,
                                                &workspaceGraphNode);
        assert(status == StatusOk);  // We just created it

        queryJob->currentRunningNodeName =
            workspaceGraphNode->dagNodeHdr.apiDagNodeHdr.name;
    } else {
        workspaceGraphNode = NULL;
        queryJob->currentRunningNodeName = NULL;
    }

    runningWorkItemNode->workspaceGraphNode = workspaceGraphNode;
    runningWorkItemNode->workspaceGraphNodeId = workspaceGraphNodeId;
    queryJob->currentRunningNodeId = workspaceGraphNodeId;

    if (mode == Txn::Mode::LRQ) {
        // only touch parents if they are in the same workspace as us
        if (parentsInWorkspace(workspaceGraph, workspaceGraphNode)) {
            if (dropSrcSlots) {
                annotations->flags = (OperatorFlag)(annotations->flags |
                                                    OperatorFlagDropSrcSlots);
            }

            annotations->flags =
                (OperatorFlag)(annotations->flags | OperatorFlagSerializeSlots);
        }
    }

    status = executeQueryGraphNode(&runningWorkItemNode->workItem,
                                   operatorHandler,
                                   annotations);
    if (status == StatusOk) {
        if (workspaceGraphNodeId == DagTypes::InvalidDagNodeId &&
            operatorHandler->dstNodeId_ != DagTypes::InvalidDagNodeId) {
            // dag node was created during execute
            workspaceGraphNodeId = operatorHandler->dstNodeId_;

            status = workspaceGraph->lookupNodeById(workspaceGraphNodeId,
                                                    &workspaceGraphNode);
            assert(status == StatusOk);  // We just created it

            runningWorkItemNode->workspaceGraphNode = workspaceGraphNode;
            runningWorkItemNode->workspaceGraphNodeId = workspaceGraphNodeId;
        }
    }

CommonExit:
    // Not async, so op should have completed.
    queryGraphNode->stopwatch.stop();

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "queryExecuteQueryGraphNode failed with status: %s",
                strGetFromStatus(status));

        if (runningWorkItemNode->workItem &&
            runningWorkItemNode->workItem->api == XcalarApiBulkLoad &&
            runningWorkItemNode->workItem->output) {
            // need to write the load output's error string into the
            // current txn log which will be transfered later
            XcalarApiBulkLoadOutput *loadOutput =
                &runningWorkItemNode->workItem->output->outputResult.loadOutput;
            MsgMgr *msgMgr = MsgMgr::get();

            msgMgr->logTxnBuf(Txn::currentTxn(),
                              loadOutput->errorFile,
                              loadOutput->errorString);
        }

        runningWorkItemNode->workItemFailed = true;
        runningWorkItemNode->workItemFailureStatus = status;

        queryGraphNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateError;

        // copy over any log messages to the node itself
        MsgMgr::TxnLog *log = MsgMgr::get()->getTxnLog(Txn::currentTxn());
        if (log) {
            strlcpy(queryGraphNode->log,
                    log->messages[0],
                    sizeof(queryGraphNode->log));
        }
        queryGraphNode->status = status.code();
        status = StatusOk;
    } else {
        queryGraphNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateReady;
    }

    if (txnIdSwapped == true) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(savedTxn);
    }

    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (parentNodeIdsWorkspace != NULL) {
        delete[] parentNodeIdsWorkspace;
        parentNodeIdsWorkspace = NULL;
    }

    if (parentNodeIds != NULL) {
        delete[] parentNodeIds;
        parentNodeIds = NULL;
    }

    if (parentGraphs != NULL) {
        delete[] parentGraphs;
        parentGraphs = NULL;
    }

    if (parentUdfContainers != NULL) {
        delete[] parentUdfContainers;
        parentUdfContainers = NULL;
    }

    if (parentGraphIds != NULL) {
        delete[] parentGraphIds;
        parentGraphIds = NULL;
    }

    if (status != StatusOk) {
        if (runningWorkItemNode != NULL) {
            freeRunningWorkItemNode(runningWorkItemNode);
            runningWorkItemNode = NULL;
        }
    }

    if (runningWorkItemNodeOut != NULL) {
        *runningWorkItemNodeOut = runningWorkItemNode;
    } else if (runningWorkItemNode != NULL) {
        // Not passing to caller, so we must free it here
        freeRunningWorkItemNode(runningWorkItemNode);
    }

    return status;
}

bool
QueryEvaluate::isTarget(DagTypes::NodeId dagNodeId,
                        uint64_t numTarget,
                        DagTypes::NodeId *targetNodeArray)
{
    bool found = false;

    for (uint64_t ii = 0; ii < numTarget; ++ii) {
        if (targetNodeArray[ii] == dagNodeId) {
            found = true;
            break;
        }
    }

    return found;
}

Status
QueryEvaluate::stepThroughNode(Dag *queryGraph,
                               DagNodeTypes::Node *queryGraphNode,
                               Dag *workspaceGraph,
                               Runtime::SchedId schedId,
                               bool isLrq,
                               bool pinNode,
                               QueryManager::QueryJob *queryJob,
                               DagTypes::NodeId *workspaceGraphNodeIdOut)
{
    Status status = StatusOk;
    XcalarWorkItem *workItem = NULL;
    OperatorHandler *operatorHandler = NULL;
    DagTypes::NodeId workspaceGraphNodeId = XidInvalid;
    Txn savedTxn;
    bool txnIdSwapped = false;
    XcalarApiDagNodeHdr *hdr = &queryGraphNode->dagNodeHdr.apiDagNodeHdr;
    Txn::Mode mode = Txn::Mode::Invalid;

    if (queryJob->markedForCancelling) {
        status = StatusCanceled;
        goto CommonExit;
    }

    if (hdr->state == DgDagStateReady || hdr->state == DgDagStateError) {
        // node has already been processed
        goto CommonExit;
    }

    queryGraphNode->stopwatch.restart();

    // check if the query has been supplied a different udf container than the
    // workspace it's running in. If so, we'll need to resolve the paths before
    // initiating the op instead of going through the default workspace path
    if (queryGraph->getUdfContainer()->sessionInfo.sessionId !=
        workspaceGraph->getUdfContainer()->sessionInfo.sessionId) {
        status = queryGraph->convertUdfsToAbsolute(hdr->api,
                                                   queryGraphNode->dagNodeHdr
                                                       .apiInput);
        BailIfFailed(status);
    }

    hdr->state = DgDagStateProcessing;

    assert(operatorHandler == NULL);
    status = xcApiGetOperatorHandlerUninited(&operatorHandler, hdr->api, true);
    BailIfFailed(status);

    status = operatorHandler->init(&queryJob->userId, NULL, workspaceGraph);
    BailIfFailed(status);

    savedTxn = Txn::currentTxn();

    mode = isLrq ? Txn::Mode::LRQ : Txn::Mode::NonLRQ;
    if (schedId == Runtime::SchedId::MaxSched) {
        schedId = pickSchedToRun();
    }
    assert(static_cast<uint8_t>(schedId) < Runtime::TotalFastPathScheds);
    status = operatorHandler->setTxnAndTxnLog(mode, schedId);
    BailIfFailed(status);
    txnIdSwapped = true;

    xSyslog(moduleName,
            XlogDebug,
            "New txn started from query %s, parent txn: %lu,%hhu,%hhu",
            queryJob->queryName,
            savedTxn.id_,
            (unsigned char) savedTxn.rtType_,
            static_cast<uint8_t>(savedTxn.rtSchedId_));

    status = operatorHandler->setArg(queryGraphNode->dagNodeHdr.apiInput,
                                     hdr->inputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set argument for %s: %s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = operatorHandler->createDagNode(&workspaceGraphNodeId,
                                            DagTypes::QueryGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create dag node for %s: %s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (strlen(hdr->tag) > 0) {
        status = workspaceGraph->tagDagNodeLocalEx(hdr->name, hdr->tag, true);
        BailIfFailed(status);
    }

    if (strlen(hdr->comment) > 0) {
        status = workspaceGraph->commentDagNodeLocalEx(hdr->name,
                                                       hdr->comment,
                                                       true);
        BailIfFailed(status);
    }

    if (pinNode) {
        status = workspaceGraph->pinUnPinDagNode(hdr->name, Dag::Pin);
        if (status == StatusTableAlreadyPinned) {
            status = StatusOk;
        }
        BailIfFailed(status);
    }

    queryJob->currentRunningNodeId = workspaceGraphNodeId;
    queryJob->currentRunningNodeName = hdr->name;

    status = executeQueryGraphNode(&workItem,
                                   operatorHandler,
                                   queryGraphNode->annotations);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to execute %s:%s",
                hdr->name,
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (workspaceGraphNodeId != XidInvalid) {
        status = queryGraph->copyDagNodeOpStatus(workspaceGraph,
                                                 workspaceGraphNodeId,
                                                 hdr->dagNodeId);
        if (status == StatusDagNodeNotFound &&
            hdr->api == XcalarApiDeleteObjects) {
            hdr->state = DgDagStateDropped;
            status = StatusOk;
        }
    }
    BailIfFailed(status);

CommonExit:
    queryGraphNode->stopwatch.stop();
    queryGraphNode->status = status.code();

    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }

    // The following code handles the transferring of txn logs
    if (status != StatusOk) {
        hdr->state = DgDagStateError;

        if (workItem && workItem->api == XcalarApiBulkLoad &&
            workItem->output) {
            // need to write the load output's error string into the
            // current txn log which will be transfered later
            XcalarApiBulkLoadOutput *loadOutput =
                &workItem->output->outputResult.loadOutput;
            MsgMgr *msgMgr = MsgMgr::get();

            msgMgr->logTxnBuf(Txn::currentTxn(),
                              loadOutput->errorFile,
                              loadOutput->errorString);
        }

        // copy over any log messages to the node itself
        MsgMgr::TxnLog *log = MsgMgr::get()->getTxnLog(Txn::currentTxn());
        if (log) {
            strlcpy(queryGraphNode->log,
                    log->messages[0],
                    sizeof(queryGraphNode->log));
        }
    } else {
        hdr->state = DgDagStateReady;
    }

    if (txnIdSwapped == true) {
        MsgMgr::get()->restoreTxnAndTransferTxnLog(savedTxn);
    }

    if (operatorHandler != NULL) {
        delete operatorHandler;
        operatorHandler = NULL;
    }

    if (workspaceGraphNodeIdOut) {
        *workspaceGraphNodeIdOut = workspaceGraphNodeId;
    }

    return status;
}

struct DagNodeHashElt {
    DagNodeHashElt(DagNodeTypes::Node *nodeIn) { node = nodeIn; }

    IntHashTableHook hook;
    DagNodeTypes::Node *node;

    Xid getNodeId() const { return node->dagNodeHdr.apiDagNodeHdr.dagNodeId; }

    const char *getName() const { return node->dagNodeHdr.apiDagNodeHdr.name; }

    void destroy() { delete this; }
};

typedef IntHashTable<Xid,
                     DagNodeHashElt,
                     &DagNodeHashElt::hook,
                     &DagNodeHashElt::getNodeId,
                     121,
                     hashIdentity>
    NodesTable;

// Difference between queryStepThrough and queryEvaluate is
// 1) stepThrough doesn't automatically drop tables
// 2) stepThrough executes every command in the query graph
// as opposed to queryEvaluate which only executes just the commands
// required to produce the desired output
// 3) The output of executing each command in stepThrough is immediately
// added to the workspaceGraph. On the other hand. evalute only adds
// the output of evaluating the entire queryGraph to the workspaceGraph
Status
QueryEvaluate::stepThrough(Dag *workspaceGraph,
                           Dag **queryGraphIn,
                           uint64_t numDagNodes,
                           bool bailOnError,
                           Runtime::SchedId schedId,
                           bool pinResults,
                           QueryManager::QueryJob *queryJob)
{
    Status status = StatusOk;
    DagTypes::NodeId queryGraphNodeId;
    DagTypes::NodeId workspaceGraphNodeId;
    DagNodeTypes::Node *queryGraphNode;

    Dag *queryGraph;

    NodesTable *nodesToBeDropped = NULL;
    NodesTable *completedNodes = NULL;

    nodesToBeDropped = new (std::nothrow) NodesTable();
    BailIfNull(nodesToBeDropped);

    completedNodes = new (std::nothrow) NodesTable();
    BailIfNull(completedNodes);

    assert(workspaceGraph != NULL);
    assert(*queryGraphIn != NULL);
    assert(queryJob != NULL);

    queryJob->workspaceGraph = workspaceGraph;
    queryJob->queryGraph = *queryGraphIn;
    *queryGraphIn = NULL;
    queryGraph = queryJob->queryGraph;

    if (queryJob->markedForCancelling) {
        status = StatusCanceled;
        goto CommonExit;
    }

    atomicWrite32(&queryJob->queryState, qrProcessing);

    status = updateSelectNodes(queryGraph);
    BailIfFailed(status);

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    // 1 pass to track all Dropped nodes
    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->lookupNodeById(queryGraphNodeId, &queryGraphNode);
        assert(status == StatusOk);

        if (queryGraphNode->dagNodeHdr.apiDagNodeHdr.state ==
            DgDagStateDropped) {
            // keep track of nodes that need to be dropped
            DagNodeHashElt *elt =
                new (std::nothrow) DagNodeHashElt(queryGraphNode);
            BailIfNull(elt);

            nodesToBeDropped->insert(elt);

            queryGraphNode->dagNodeHdr.apiDagNodeHdr.state = DgDagStateCreated;
        }

        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    // run the query
    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->lookupNodeById(queryGraphNodeId, &queryGraphNode);
        assert(status == StatusOk);

        status =
            stepThroughNode(queryGraph,
                            queryGraphNode,
                            workspaceGraph,
                            schedId,
                            false,
                            pinResults &&
                                queryGraphNode->numChild ==
                                    0,  // pin, if this node is the leaf node
                            queryJob,
                            &workspaceGraphNodeId);
        if (status != StatusOk) {
            if (status == StatusCanceled || bailOnError) {
                goto CommonExit;
            }
        }

        DagNodeHashElt *elt = new (std::nothrow) DagNodeHashElt(queryGraphNode);
        BailIfNull(elt);

        completedNodes->insert(elt);

        // see if we can drop any of our parents
        DagNodeTypes::NodeIdListElt *parentElt = queryGraphNode->parentsList;
        while (parentElt) {
            DagNodeHashElt *droppedElt =
                nodesToBeDropped->find(parentElt->nodeId);

            if (droppedElt) {
                // check if all of this parent's children have been processed
                DagNodeTypes::NodeIdListElt *childElt =
                    droppedElt->node->childrenList;

                bool canBeDropped = true;
                while (childElt) {
                    if (!completedNodes->find(childElt->nodeId)) {
                        canBeDropped = false;
                        break;
                    }

                    childElt = childElt->next;
                }

                if (canBeDropped) {
                    XcalarApis api =
                        droppedElt->node->dagNodeHdr.apiDagNodeHdr.api;
                    SourceType sourceType;
                    if (api == XcalarApiBulkLoad) {
                        sourceType = SrcDataset;
                    } else if (api == XcalarApiAggregate) {
                        sourceType = SrcConstant;
                    } else {
                        sourceType = SrcTable;
                    }

                    // drop node in workspaceGraph
                    status = workspaceGraph->dropNode(droppedElt->getName(),
                                                      sourceType,
                                                      NULL,
                                                      NULL);
                    if (status != StatusOk) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Failed to drop node %s in query %s",
                                droppedElt->getName(),
                                queryJob->queryName);

                        // non-fatal
                        status = StatusOk;
                    } else {
                        // change state in the query graph
                        droppedElt->node->dagNodeHdr.apiDagNodeHdr.state =
                            DgDagStateDropped;

                        nodesToBeDropped->remove(parentElt->nodeId);
                        delete droppedElt;
                    }
                }
            }

            parentElt = parentElt->next;
        }

        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

CommonExit:
    if (completedNodes) {
        completedNodes->removeAll(&DagNodeHashElt::destroy);
        delete completedNodes;
    }

    if (nodesToBeDropped) {
        nodesToBeDropped->removeAll(&DagNodeHashElt::destroy);
        delete nodesToBeDropped;
    }

    queryJob->queryLock_.lock();
    queryJob->workspaceGraph = NULL;
    queryJob->queryLock_.unlock();

    return status;
}

Status
QueryEvaluate::updateSelectNodes(Dag *queryGraph)
{
    Status status = StatusOk;
    unsigned numSelectNodes = 0;
    unsigned numPublishedTables = 0;
    DagTypes::NodeId queryGraphNodeId;
    DagNodeTypes::Node *dagNode;
    HashTreeRefHandle *refHandles = NULL;
    bool handlesInit = false;
    const char **publishedTableNames = NULL;
    unsigned *pubTableIndex = NULL;

    // XXX TODO Better to use a hash table here instead, since DAG can get
    // really deep. find select nodes in the query graph
    DagNodeTypes::Node **selectNodes = (DagNodeTypes::Node **) memAlloc(
        sizeof(*selectNodes) * queryGraph->getNumNode());
    BailIfNull(selectNodes);

    status = queryGraph->getFirstDagInOrder(&queryGraphNodeId);
    BailIfFailed(status);

    while (queryGraphNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->lookupNodeById(queryGraphNodeId, &dagNode);
        BailIfFailed(status);

        if (dagNode->dagNodeHdr.apiDagNodeHdr.api == XcalarApiSelect) {
            selectNodes[numSelectNodes++] = dagNode;
        }

        status =
            queryGraph->getNextDagInOrder(queryGraphNodeId, &queryGraphNodeId);
        BailIfFailed(status);
    }

    refHandles =
        (HashTreeRefHandle *) memAlloc(numSelectNodes * sizeof(*refHandles));
    BailIfNull(refHandles);

    publishedTableNames =
        (const char **) memAlloc(numSelectNodes * sizeof(*publishedTableNames));
    BailIfNull(publishedTableNames);

    pubTableIndex =
        (unsigned *) memAlloc(numSelectNodes * sizeof(*pubTableIndex));
    BailIfNull(pubTableIndex);

    // find set of published tables to grab handles for
    for (unsigned ii = 0; ii < numSelectNodes; ii++) {
        const char *publishedTableName =
            selectNodes[ii]
                ->dagNodeHdr.apiInput->selectInput.srcTable.tableName;

        bool seen = false;
        for (unsigned jj = 0; jj < ii; jj++) {
            if (strcmp(publishedTableName,
                       selectNodes[jj]
                           ->dagNodeHdr.apiInput->selectInput.srcTable
                           .tableName) == 0) {
                seen = true;
                pubTableIndex[ii] = jj;
                break;
            }
        }

        if (seen) {
            // no depulicates
            continue;
        }

        publishedTableNames[numPublishedTables] = publishedTableName;
        pubTableIndex[ii] = numPublishedTables;
        numPublishedTables++;
    }

    assert(numPublishedTables <= numSelectNodes);

    status =
        HashTreeMgr::get()->openHandles(publishedTableNames,
                                        numPublishedTables,
                                        LibNsTypes::ReadSharedWriteExclReader,
                                        refHandles);
    BailIfFailed(status);
    handlesInit = true;

    for (unsigned ii = 0; ii < numSelectNodes; ii++) {
        queryGraphNodeId = selectNodes[ii]->dagNodeHdr.apiDagNodeHdr.dagNodeId;
        XcalarApiInput *apiInput = selectNodes[ii]->dagNodeHdr.apiInput;
        XcalarApiSelectInput *selectIp = &apiInput->selectInput;

        if (selectIp->maxBatchId == HashTree::InvalidBatchId) {
            selectIp->maxBatchId = refHandles[pubTableIndex[ii]].currentBatchId;
            selectIp->minBatchId = HashTree::InvalidBatchId;
        }

        // XXX TODO ENG-743 Added to track down invalid batch Ids
        xSyslog(moduleName,
                XlogInfo,
                "updateSelectNodes: source '%s', dest '%s', minBatchId %ld, "
                "maxBatchId %ld, numColumns %u",
                selectIp->srcTable.tableName,
                selectIp->dstTable.tableName,
                selectIp->minBatchId,
                selectIp->maxBatchId,
                selectIp->numColumns);
    }

CommonExit:
    if (handlesInit) {
        HashTreeMgr::get()->closeHandles(publishedTableNames,
                                         numPublishedTables,
                                         refHandles);
    }
    if (selectNodes) {
        memFree(selectNodes);
        selectNodes = NULL;
    }
    if (publishedTableNames) {
        memFree(publishedTableNames);
        publishedTableNames = NULL;
    }
    if (refHandles) {
        memFree(refHandles);
        refHandles = NULL;
    }
    if (pubTableIndex) {
        memFree(pubTableIndex);
        pubTableIndex = NULL;
    }
    return status;
}

bool
QueryEvaluate::checkParentsCanBeDroppedDuringEval(
    Dag *workspaceGraph,
    Dag *queryGraph,
    DagTypes::NodeId queryGraphNodeId,
    uint64_t numTargets,
    DagTypes::NodeId targetNodeArray[],
    CompletedDictHashTable *completedDict)
{
    Status status = StatusUnknown;
    uint64_t ii, jj;
    DagTypes::NodeId *parentNodesArray = NULL;
    DagTypes::NodeId *parentNodesProcessed = NULL;
    uint64_t numParent, numParentProcessed;
    DagTypes::NodeId *childNodesArray = NULL;
    uint64_t numChild = 0;
    XcalarApis api;
    CompletedWorkItemNode *completedWorkItem = NULL;
    bool parentsCanBeDropped = false;

    // look up the parent node which could be freed up
    status = queryGraph->getParentDagNodeId(queryGraphNodeId,
                                            &parentNodesArray,
                                            &numParent);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (numParent > 0) {
        parentNodesProcessed = new (std::nothrow) DagTypes::NodeId[numParent];
        if (parentNodesProcessed == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate parentNodesProcessed "
                    "(numParent: %lu)",
                    numParent);
            status = StatusNoMem;
            goto CommonExit;
        }
    }

    numParentProcessed = 0;
    for (ii = 0; ii < numParent; ++ii) {
        // Becuase parents can be duplicated (e.g. via a self join),
        // we have to dedupe parents
        for (jj = 0; jj < numParentProcessed; jj++) {
            if (parentNodesProcessed[jj] == parentNodesArray[ii]) {
                // Duplicate!
                goto CommonExit;
            }
        }

        parentNodesProcessed[numParentProcessed++] = parentNodesArray[ii];

        // parent is needed. Do not drop
        if (isTarget(parentNodesArray[ii], numTargets, targetNodeArray)) {
            goto CommonExit;
        }

        status = queryGraph->getDagNodeApi(parentNodesArray[ii], &api);
        assert(status == StatusOk);

        // Check if this parent is done spawning all its children
        // Once a parent is past its reproductive age, it becomes
        // a candidate for removal
        assert(childNodesArray == NULL);
        status = queryGraph->getChildDagNodeId(parentNodesArray[ii],
                                               &childNodesArray,
                                               &numChild);
        assert(status == StatusOk);

        for (jj = 0; jj < numChild; ++jj) {
            if (childNodesArray[jj] == queryGraphNodeId) {
                // don't count myself, I will be spawned during drop
                continue;
            }

            completedWorkItem = completedDict->find(childNodesArray[jj]);
            if (completedWorkItem == NULL) {
                // 1 or more children not spawned yet. Let's give this
                // parent some time and privacy to spawn the child
                goto CommonExit;
            }
        }

        if (numChild > 0) {
            assert(childNodesArray != NULL);
            memFree(childNodesArray);
            childNodesArray = NULL;
        }
    }

    parentsCanBeDropped = true;
CommonExit:
    if (parentNodesArray != NULL) {
        delete[] parentNodesArray;
        parentNodesArray = NULL;
    }

    if (parentNodesProcessed != NULL) {
        delete[] parentNodesProcessed;
        parentNodesProcessed = NULL;
    }

    if (childNodesArray != NULL) {
        memFree(childNodesArray);
        childNodesArray = NULL;
    }

    return parentsCanBeDropped;
}

Status
QueryEvaluate::dropUnneededParents(Dag *workspaceGraph,
                                   Dag *queryGraph,
                                   DagTypes::NodeId queryGraphNodeId,
                                   uint64_t numTargets,
                                   DagTypes::NodeId targetNodeArray[],
                                   CompletedDictHashTable *completedDict)
{
    Status status = StatusUnknown;
    uint64_t ii, jj;
    DagTypes::NodeId *parentNodesArray = NULL;
    DagTypes::NodeId *parentNodesProcessed = NULL;
    uint64_t numParent, numParentProcessed;
    DagTypes::NodeId *childNodesArray = NULL;
    uint64_t numChild = 0;
    XcalarApis api;
    CompletedWorkItemNode *completedWorkItem = NULL;

    // look up the parent node which could be freed up
    status = queryGraph->getParentDagNodeId(queryGraphNodeId,
                                            &parentNodesArray,
                                            &numParent);
    if (status != StatusOk) {
        goto CommonExit;
    }

    if (numParent > 0) {
        parentNodesProcessed = new (std::nothrow) DagTypes::NodeId[numParent];
        if (parentNodesProcessed == NULL) {
            xSyslog(moduleName,
                    XlogErr,
                    "Insufficient memory to allocate parentNodesProcessed "
                    "(numParent: %lu)",
                    numParent);
            status = StatusNoMem;
            goto CommonExit;
        }
    }

    numParentProcessed = 0;
    for (ii = 0; ii < numParent; ++ii) {
        bool parentCanBeDropped = true;
        bool duplicateParent = false;

        // Becuase parents can be duplicated (e.g. via a self join),
        // we have to dedupe parents
        for (jj = 0; jj < numParentProcessed; jj++) {
            if (parentNodesProcessed[jj] == parentNodesArray[ii]) {
                // Duplicate!
                parentCanBeDropped = false;
                duplicateParent = true;
                break;
            }
        }

        if (duplicateParent) {
            // We've already processed this parent before. Keep going
            continue;
        }
        parentNodesProcessed[numParentProcessed++] = parentNodesArray[ii];

        // parent is needed. Do not drop
        if (isTarget(parentNodesArray[ii], numTargets, targetNodeArray)) {
            parentCanBeDropped = false;
            continue;
        }

        status = queryGraph->getDagNodeApi(parentNodesArray[ii], &api);
        assert(status == StatusOk);

        // Skip the datasets here. We will handle them at the very end
        // of the session replay. This is because the dataset could be needed
        // for the subsequent tables.
        if (api == XcalarApiBulkLoad) {
            continue;
        }

        // Check if this parent is done spawning all its children
        // Once a parent is past its reproductive age, it becomes
        // a candidate for removal
        assert(childNodesArray == NULL);
        status = queryGraph->getChildDagNodeId(parentNodesArray[ii],
                                               &childNodesArray,
                                               &numChild);
        assert(status == StatusOk);

        for (jj = 0; jj < numChild; ++jj) {
            completedWorkItem = completedDict->find(childNodesArray[jj]);
            if (completedWorkItem == NULL) {
                // 1 or more children not spawned yet. Let's give this
                // parent some time and privacy to spawn the child
                parentCanBeDropped = false;
                break;
            }
        }

        if (parentCanBeDropped) {
            DagNodeTypes::Node *parentNode;

            xSyslog(moduleName,
                    XlogDebug,
                    "Clean Node:%lu",
                    parentNodesArray[ii]);
            // Find its corresponding workspaceGraphNodeId
            completedWorkItem = completedDict->find(parentNodesArray[ii]);
            assert(completedWorkItem != NULL);

            status = workspaceGraph->lookupNodeById(completedWorkItem
                                                        ->workspaceGraphNodeId,
                                                    &parentNode);
            assert(status == StatusOk);
            assert(parentNode != NULL);
            if (status != StatusOk) {
                goto CommonExit;
            }

            status = workspaceGraph
                         ->dropNode(parentNode->dagNodeHdr.apiDagNodeHdr.name,
                                    (api == XcalarApiBulkLoad) ? SrcDataset
                                                               : SrcTable,
                                    NULL,
                                    NULL);

            if (status != StatusOk) {
                goto CommonExit;
            }
        }

        if (numChild > 0) {
            assert(childNodesArray != NULL);
            memFree(childNodesArray);
            childNodesArray = NULL;
        }
    }

    status = StatusOk;
CommonExit:
    if (parentNodesArray != NULL) {
        delete[] parentNodesArray;
        parentNodesArray = NULL;
    }

    if (parentNodesProcessed != NULL) {
        delete[] parentNodesProcessed;
        parentNodesProcessed = NULL;
    }

    if (childNodesArray != NULL) {
        assert(status != StatusOk);
        memFree(childNodesArray);
        childNodesArray = NULL;
    }

    return status;
}

// drop the datasets that are not part of the target nodes to keep
Status
QueryEvaluate::dropUnneededDatasets(Dag *queryGraph,
                                    uint64_t numTarget,
                                    DagTypes::NodeId targetNodeArray[],
                                    CompletedDictHashTable *completedDict,
                                    Dag *workspaceGraph)
{
    DagTypes::NodeId dagNodeId;
    XcalarApis dagNodeApi;
    DagNodeTypes::Node *datasetNode;
    Status status;
    CompletedWorkItemNode *completedWorkItem = NULL;

    status = queryGraph->getFirstDagInOrder(&dagNodeId);
    assert(status == StatusOk);
    while (dagNodeId != DagTypes::InvalidDagNodeId) {
        status = queryGraph->getDagNodeApi(dagNodeId, &dagNodeApi);
        assert(status == StatusOk);
        if ((!isTarget(dagNodeId, numTarget, targetNodeArray)) &&
            (dagNodeApi == XcalarApiBulkLoad)) {
            xSyslog(moduleName, XlogDebug, "Clean Node:%lu", dagNodeId);
            // Find its corresponding workspaceGraphNodeId
            completedWorkItem = completedDict->find(dagNodeId);
            assert(completedWorkItem != NULL);

            status = workspaceGraph->lookupNodeById(completedWorkItem
                                                        ->workspaceGraphNodeId,
                                                    &datasetNode);
            assert(status == StatusOk);
            assert(datasetNode != NULL);
            if (status != StatusOk) {
                goto CommonExit;
            }

            status = workspaceGraph
                         ->dropNode(datasetNode->dagNodeHdr.apiDagNodeHdr.name,
                                    SrcDataset,
                                    NULL,
                                    NULL);

            if (status != StatusOk) {
                goto CommonExit;
            }

            // Lets try to unload the dataset. If someone else is using the
            // dataset, this call will fail and that is ok and just drive on.
            // The reason we are doing an unload here is because we do not know
            // if there was an explicit unload done by the user on this dataset.
            // Since we do not know that and the dataset reference is
            // dropped(which will happen when the user unlocks the dataset),
            // we are assuming that the user has dropped the dataset.
            Status status2;
            status2 = Dataset::get()->unloadDatasetByName(
                datasetNode->dagNodeHdr.apiDagNodeHdr.name);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogInfo,
                        "Failed to delete dataset \"%s\": %s",
                        datasetNode->dagNodeHdr.apiDagNodeHdr.name,
                        strGetFromStatus(status2));
            }
        }
        status = queryGraph->getNextDagInOrder(dagNodeId, &dagNodeId);
        assert(status == StatusOk);
    }

CommonExit:

    return status;
}

Status
QueryEvaluate::addChildrenToRunnableQueue(RunnableWorkItemNode *runnableQueue,
                                          uint64_t *numQueuedTask,
                                          Dag *queryGraph,
                                          DagTypes::NodeId queryGraphNodeId,
                                          CompletedDictHashTable *completedDict,
                                          ExecutionMode lrqMode)
{
    Status status = StatusUnknown;
    // A parent could have potentially unlimited number of children
    DagTypes::NodeId *childNodesArray = NULL;
    uint64_t numChild = 0, ii, jj;
    // A child could have potentially unlimited number of parents
    DagTypes::NodeId *parentNodesArray = NULL;
    uint64_t numParent;
    bool allParentsReady;
    RunnableWorkItemNode *runnableWorkItemNode = NULL;
    CompletedWorkItemNode *node;

    status = queryGraph->getChildDagNodeId(queryGraphNodeId,
                                           &childNodesArray,
                                           &numChild);
    if (status != StatusOk) {
        goto CommonExit;
    }

    for (ii = 0; ii < numChild; ++ii) {
        // The reason we have to do this is because a child could have
        // multiple parents. We need all the parents to be ready
        // before we can execute the child
        allParentsReady = true;
        assert(parentNodesArray == NULL);
        status = queryGraph->getParentDagNodeId(childNodesArray[ii],
                                                &parentNodesArray,
                                                &numParent);
        if (status != StatusOk) {
            goto CommonExit;
        }

        assert(numParent > 0);  // I'm 1 of the parent. So at least 1 parent

        for (jj = 0; jj < numParent; ++jj) {
            node = completedDict->find(parentNodesArray[jj]);
            if (node == NULL) {
                // At least 1 of the parents is not ready
                allParentsReady = false;
                break;
            }
        }

        // If all parents ready, we can now schedule the child into
        // the runnableQueue
        if (allParentsReady) {
            runnableWorkItemNode =
                (RunnableWorkItemNode *)
                    runnableWorkItemNodeBcHandle_->allocBuf(XidInvalid, &status);
            if (runnableWorkItemNode == NULL) {
                goto CommonExit;
            }

            runnableWorkItemNode->queryGraphNodeId = childNodesArray[ii];

            if (lrqMode == BFS) {
                // append to tail
                runnableWorkItemNode->prev = runnableQueue->prev;
                runnableWorkItemNode->next = runnableQueue;
                runnableQueue->prev->next = runnableWorkItemNode;
                runnableQueue->prev = runnableWorkItemNode;
            } else {
                // Insert to head
                runnableWorkItemNode->next = runnableQueue->next;
                runnableWorkItemNode->prev = runnableQueue;
                runnableQueue->next->prev = runnableWorkItemNode;
                runnableQueue->next = runnableWorkItemNode;
            }

            runnableWorkItemNode = NULL;  // Ref given to runnableQueue
            (*numQueuedTask)++;
        }

        assert(parentNodesArray != NULL);
        delete[] parentNodesArray;
        parentNodesArray = NULL;
    }

CommonExit:
    if (parentNodesArray != NULL) {
        delete[] parentNodesArray;
        parentNodesArray = NULL;
    }

    if (childNodesArray != NULL) {
        assert(numChild != 0);
        memFree(childNodesArray);
        childNodesArray = NULL;
        numChild = 0;
    }

    if (runnableWorkItemNode != NULL) {
        assert(status != StatusOk);
        runnableWorkItemNodeBcHandle_->freeBuf(runnableWorkItemNode);
        runnableWorkItemNode = NULL;
    }

    return status;
}

Status
QueryEvaluate::deleteLrqDataset(XcalarApiBulkLoadOutput *loadOutput)
{
    Status status = StatusUnknown;
    XcalarApiOutput *output = NULL;
    size_t outputSize = 0;

    // Check if dataset is executed as a result of LRQ
    if (strncmp(XcalarApiLrqPrefix,
                loadOutput->dataset.name,
                XcalarApiLrqPrefixLen) != 0) {
        // Dataset is not created as part of LRQ
        status = StatusOk;
        goto CommonExit;
    }

    status = Dataset::get()->unloadDatasets(loadOutput->dataset.name,
                                            &output,
                                            &outputSize);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to delete dataset \"%s\": %s",
                loadOutput->dataset.name,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (output != NULL) {
        memFree(output);
        output = NULL;
    }

    return status;
}

Status
QueryEvaluate::processCompletedRunningWorkItemNode(
    RunningWorkItemNode *runningWorkItemNode,
    RunnableWorkItemNode *runnableQueue,
    uint64_t *numQueuedTask,
    Dag *workspaceGraph,
    Dag *queryGraph,
    uint64_t numTargets,
    DagTypes::NodeId targetNodeArray[],
    CompletedDictHashTable *completedDict,
    ExecutionMode lrqMode)
{
    Status status = StatusUnknown;
    DagTypes::NodeId queryGraphNodeId = DagTypes::InvalidDagNodeId;
    DagTypes::NodeId workspaceGraphNodeId = DagTypes::InvalidDagNodeId;
    CompletedWorkItemNode *completedWorkItemNode = NULL;

    status = StatusOk;
    if (runningWorkItemNode->workItemFailed) {
        status = runningWorkItemNode->workItemFailureStatus;
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    // When we reach here, the operation has run to completion and succeeded.
    // If the operation was a load, we want to make the dataset invisible.
    // There's a brief period where it's visible, but it's not really a
    // correcntess problem. We're more concerned that the datasets created
    // by LRQ are not cleaned up, which we are doing now.
    if (runningWorkItemNode->workItem != NULL &&
        runningWorkItemNode->workItem->api == XcalarApiBulkLoad) {
        XcalarApiOutput *apiOutput;
        apiOutput = runningWorkItemNode->workItem->output;
        if (apiOutput->hdr.status == StatusOk.code()) {
            status = deleteLrqDataset(&apiOutput->outputResult.loadOutput);
            if (status != StatusOk) {
                goto CommonExit;
            }
        }
    }

    queryGraphNodeId = runningWorkItemNode->queryGraphNodeId;
    workspaceGraphNodeId = runningWorkItemNode->workspaceGraphNodeId;

    status = queryGraph->copyDagNodeOpStatus(workspaceGraph,
                                             workspaceGraphNodeId,
                                             queryGraphNodeId);
    BailIfFailed(status);

    completedWorkItemNode =
        (CompletedWorkItemNode *) completedWorkItemNodeBcHandle_->allocBuf(
            XidInvalid, &status);
    if (completedWorkItemNode == NULL) {
        goto CommonExit;
    }

    completedWorkItemNode->queryGraphNodeId = queryGraphNodeId;
    completedWorkItemNode->workspaceGraphNodeId = workspaceGraphNodeId;

    verifyOk(completedDict->insert(completedWorkItemNode));

    completedWorkItemNode = NULL;  // Ref handed off to completedDict

    // If all parents are ready, we can spawn new children
    status = addChildrenToRunnableQueue(runnableQueue,
                                        numQueuedTask,
                                        queryGraph,
                                        queryGraphNodeId,
                                        completedDict,
                                        lrqMode);
    if (status != StatusOk) {
        goto CommonExit;
    }

    // Once the children have been spawned, we can get rid of some of the
    // older parents if they're no longer required
    status = dropUnneededParents(workspaceGraph,
                                 queryGraph,
                                 queryGraphNodeId,
                                 numTargets,
                                 targetNodeArray,
                                 completedDict);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (completedWorkItemNode != NULL) {
        assert(status != StatusOk);
        completedWorkItemNodeBcHandle_->freeBuf(completedWorkItemNode);
        completedWorkItemNode = NULL;
    }

    return status;
}

void
QueryEvaluate::ExecuteQueryGraphNodePre::run()
{
    Status status;
    QueryEvaluate *qe = QueryEvaluate::get();
    RunningWorkItemNode *runningWorkItemNode = NULL;

    start_ = true;
    status = qe->executeQueryGraphNodePre(userId_,
                                          workspaceGraph_,
                                          queryGraph_,
                                          queryGraphNodeId_,
                                          dropSrcSlots_,
                                          queryJob_,
                                          &runningWorkItemNode,
                                          completedDict_,
                                          schedId_);
    if (status == StatusOk) {
        lock_->lock();
        runningWorkItemNode->prev = runningQueueAnchor_->prev;
        runningWorkItemNode->next = runningQueueAnchor_;
        runningQueueAnchor_->prev->next = runningWorkItemNode;
        runningQueueAnchor_->prev = runningWorkItemNode;
        lock_->unlock();
    } else {
        XcalarApiOutputHeader outputHdr;
        XcalarApiOutput *apiOutput;
        Status status2;

        apiOutput = (XcalarApiOutput *) &outputHdr;
        assert((uintptr_t) apiOutput == (uintptr_t) &apiOutput->hdr);
        assert(XcalarApiSizeOfOutput(apiOutput->outputResult.noOutput) ==
               sizeof(outputHdr));
        outputHdr.status = status.code();

        status2 =
            queryGraph_->changeDagNodeState(queryGraphNodeId_, DgDagStateError);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to change dag node state (%lu): %s",
                    queryGraphNodeId_,
                    strGetFromStatus(status2));
        }
        xSyslog(moduleName,
                XlogErr,
                "Failed to execute queryGraph node: %s",
                strGetFromStatus(status));
    }
}

void
QueryEvaluate::ExecuteQueryGraphNodePre::done()
{
    atomicInc32(execSchedsDone_);
    lock_->lock();
    cv_->signal();
    lock_->unlock();
}

// XXX:there are unnecessary memory copy when getting parent/child node
// This function assume the query graph contains the all the necessary and
// sufficient nodes that lead to the target nodes
Status
QueryEvaluate::evaluate(Dag *queryGraph,
                        uint64_t numTarget,
                        DagTypes::NodeId targetNodeArray[],
                        QueryManager::QueryJob *queryJob,
                        Dag **workspaceGraphOut,
                        Runtime::SchedId schedId,
                        ExecutionMode lrqMode,
                        XcalarApiUdfContainer *sessionContainer)
{
    Status status = StatusOk;

    // Runnable Queue
    RunnableWorkItemNode runnableQueueAnchor;
    runnableQueueAnchor.next = runnableQueueAnchor.prev = &runnableQueueAnchor;
    RunnableWorkItemNode *runnableWorkItemNode = NULL;
    uint64_t numRunnableWorkItem = 0;

    // Running Queue
    RunningWorkItemNode runningQueueAnchor;
    runningQueueAnchor.next = runningQueueAnchor.prev = &runningQueueAnchor;
    RunningWorkItemNode *runningWorkItemNode = NULL;

    CompletedDictHashTable completedDict;
    Dag *workspaceGraph = NULL;
    DagLib *dagLib = DagLib::get();
    uint64_t numSrc = 0;
    DagTypes::NodeId *srcNodeArray = NULL;

    // Tracks outstanding Schedulables
    Mutex lock;
    CondVar cv;
    Atomic32 execSchedsDone;
    ExecuteQueryGraphNodePre **execScheds = NULL;
    int execSchedsIdx = -1;
    uint32_t execSchedsIssued = 0;
    uint32_t execSchedsTotal = 0;
    uint32_t execSchedsCount = 0;

    xSyslog(moduleName,
            XlogDebug,
            "query:%lu started to run in mode %s",
            queryGraph->getId(),
            strGetFromExecutionMode(lrqMode));

    if (lrqMode == DFS) {
        execSchedsCount = 1;
    } else {
        execSchedsCount =
            (uint32_t) XcalarConfig::get()->optimizedExecParallelism_;
    }
    execScheds = new (std::nothrow) ExecuteQueryGraphNodePre *[execSchedsCount];
    BailIfNull(execScheds);

    memZero(execScheds, sizeof(ExecuteQueryGraphNodePre *) * execSchedsCount);

    if (queryJob->markedForCancelling) {
        status = StatusCanceled;
        xSyslog(moduleName,
                XlogInfo,
                "query:%lu started to run in mode %s marked for cancelling",
                queryGraph->getId(),
                strGetFromExecutionMode(lrqMode));
        goto CommonExit;
    }

    atomicWrite32(&queryJob->queryState, qrProcessing);

    // XXX If we wanted table outputs, we can just use the current sessions'
    // sessionGraph
    status = dagLib->createNewDag(128,
                                  DagTypes::WorkspaceGraph,
                                  queryGraph->getUdfContainer(),
                                  &workspaceGraph);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to create dag handle: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Override the default session container here.
    if (!UserDefinedFunction::containersMatch(sessionContainer,
                                              queryGraph->getUdfContainer())) {
        workspaceGraph->initSessionContainer(sessionContainer);
    }

    status = updateSelectNodes(queryGraph);
    BailIfFailed(status);

    status = queryGraph->getRootNodeIds(&srcNodeArray, &numSrc);
    BailIfFailed(status);

    queryJob->workspaceGraph = workspaceGraph;

    for (uint64_t ii = 0; ii < numSrc; ++ii) {
        runnableWorkItemNode =
            (RunnableWorkItemNode *) runnableWorkItemNodeBcHandle_->allocBuf(
                XidInvalid, &status);
        BailIfFailed(status);

        runnableWorkItemNode->queryGraphNodeId = srcNodeArray[ii];
        // append
        runnableWorkItemNode->prev = runnableQueueAnchor.prev;
        runnableWorkItemNode->next = &runnableQueueAnchor;
        runnableQueueAnchor.prev->next = runnableWorkItemNode;
        runnableQueueAnchor.prev = runnableWorkItemNode;

        runnableWorkItemNode = NULL;  // Ref given to runnableQueue
    }
    numRunnableWorkItem = numSrc;

    while ((runnableQueueAnchor.next != &runnableQueueAnchor) ||
           (runningQueueAnchor.next != &runningQueueAnchor)) {
        assert(execSchedsIdx + 1 == (int) execSchedsIssued);
        for (uint32_t ii = 0; ii < execSchedsCount; ii++) {
            delete execScheds[ii];
        }

        memZero(execScheds,
                sizeof(ExecuteQueryGraphNodePre *) * execSchedsCount);
        execSchedsIdx = -1;
        atomicWrite32(&execSchedsDone, 0);
        execSchedsTotal += execSchedsIssued;
        execSchedsIssued = 0;

        // Run everything in the runnable queue
        while ((runnableQueueAnchor.next != &runnableQueueAnchor)) {
            runnableWorkItemNode = runnableQueueAnchor.next;

            // remove head
            runnableWorkItemNode->next->prev = &runnableQueueAnchor;
            runnableQueueAnchor.next = runnableWorkItemNode->next;

            DagTypes::NodeId queryGraphNodeId =
                runnableWorkItemNode->queryGraphNodeId;
            runnableWorkItemNodeBcHandle_->freeBuf(runnableWorkItemNode);
            runnableWorkItemNode = NULL;

            if (queryJob->markedForCancelling) {
                status = StatusCanceled;
                goto CommonExit;
            }

            status = queryGraph->changeDagNodeState(queryGraphNodeId,
                                                    DgDagStateProcessing);
            BailIfFailed(status);

            bool dropSrcSlots =
                checkParentsCanBeDroppedDuringEval(workspaceGraph,
                                                   queryGraph,
                                                   queryGraphNodeId,
                                                   numTarget,
                                                   targetNodeArray,
                                                   &completedDict);

            execSchedsIdx++;
            execScheds[execSchedsIdx] =
                new (std::nothrow) ExecuteQueryGraphNodePre(&queryJob->userId,
                                                            queryGraphNodeId,
                                                            dropSrcSlots,
                                                            queryJob,
                                                            &runningQueueAnchor,
                                                            &completedDict,
                                                            schedId,
                                                            &lock,
                                                            &cv,
                                                            &execSchedsDone);
            BailIfNull(execScheds[execSchedsIdx]);

            execSchedsIssued++;
            status = Runtime::get()->schedule(execScheds[execSchedsIdx]);
            BailIfFailed(status);

            if (lrqMode == DFS) {
                // All right that's it. Time to see what children this job
                // spawned and execute its children. Its peers can wait;
                // we're in DFS mode
                break;
            } else if (execSchedsCount == execSchedsIssued) {
                break;
            }
        }

        assert(execSchedsIdx + 1 == (int) execSchedsIssued);
        assert(execSchedsIssued <= execSchedsCount);

        lock.lock();
        while ((uint32_t) atomicRead32(&execSchedsDone) < execSchedsIssued) {
            cv.wait(&lock);
        }
        lock.unlock();

        assert(runningQueueAnchor.next != &runningQueueAnchor);

        for (runningWorkItemNode = runningQueueAnchor.next;
             runningWorkItemNode != &runningQueueAnchor;
             runningWorkItemNode = runningWorkItemNode->next) {
            RunningWorkItemNode *prevNode = runningWorkItemNode->prev;

            // remove from list
            runningWorkItemNode->prev->next = runningWorkItemNode->next;
            runningWorkItemNode->next->prev = runningWorkItemNode->prev;
            DagTypes::NodeId queryGraphNodeId =
                runningWorkItemNode->queryGraphNodeId;

            status = processCompletedRunningWorkItemNode(runningWorkItemNode,
                                                         &runnableQueueAnchor,
                                                         &numRunnableWorkItem,
                                                         workspaceGraph,
                                                         queryGraph,
                                                         numTarget,
                                                         targetNodeArray,
                                                         &completedDict,
                                                         lrqMode);

            // must free the runningWorkItemNode since it is removed from list
            freeRunningWorkItemNode(runningWorkItemNode);
            runningWorkItemNode = NULL;

            if (status != StatusOk) {
                Status status2;
                status2 = queryGraph->changeDagNodeState(queryGraphNodeId,
                                                         DgDagStateError);
                if (status2 != StatusOk) {
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to change dag node state (%lu): %s",
                            queryGraphNodeId,
                            strGetFromStatus(status2));
                }

                xSyslog(moduleName,
                        XlogErr,
                        "Failed to process completedWorkItemNode: %s",
                        strGetFromStatus(status));
                goto CommonExit;
            }

            status = queryGraph->changeDagNodeState(queryGraphNodeId,
                                                    DgDagStateReady);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to change dag node state (%lu): %s",
                        queryGraphNodeId,
                        strGetFromStatus(status));
                goto CommonExit;
            }

            numRunnableWorkItem--;
            runningWorkItemNode = prevNode;
        }
    }
    assert(runningWorkItemNode == NULL ||
           runningWorkItemNode == &runningQueueAnchor);
    runningWorkItemNode = NULL;

    // drop the datasets that are not part of the target nodes to keep
    status = dropUnneededDatasets(queryGraph,
                                  numTarget,
                                  targetNodeArray,
                                  &completedDict,
                                  workspaceGraph);
    if (status != StatusOk) {
        goto CommonExit;
    }

    queryGraph->copyCommentsAndTags(workspaceGraph);

    xSyslog(moduleName,
            XlogDebug,
            "query:%lu is finished",
            queryGraph->getId());

CommonExit:
    // Ensure all outstanding schedulables are complete
    lock.lock();
    while ((uint32_t) atomicRead32(&execSchedsDone) < execSchedsIssued) {
        cv.wait(&lock);
    }
    lock.unlock();

    if (execScheds) {
        for (uint32_t ii = 0; ii < (uint32_t)(execSchedsIdx + 1); ii++) {
            delete execScheds[ii];
        }
        delete[] execScheds;
        execScheds = NULL;
    }

    if (srcNodeArray) {
        memFree(srcNodeArray);
        srcNodeArray = NULL;
    }

    queryJob->queryLock_.lock();
    queryJob->workspaceGraph = NULL;
    queryJob->currentRunningNodeName = NULL;
    queryJob->currentRunningNodeId = XidInvalid;
    queryJob->queryLock_.unlock();

    // This must be the first thing we do, because we might have
    // to drain the runningQueue
    if (runningQueueAnchor.next != &runningQueueAnchor) {
        DagNodeTypes::Node *workspaceGraphNode;

        // Must have been an error
        assert(status != StatusOk);

        assert(runningWorkItemNode == NULL);
        for (runningWorkItemNode = runningQueueAnchor.next;
             runningWorkItemNode != &runningQueueAnchor;
             runningWorkItemNode = runningWorkItemNode->next) {
            Status status2;

            workspaceGraphNode = runningWorkItemNode->workspaceGraphNode;
            status2 = workspaceGraph->cancelOp(
                workspaceGraphNode->dagNodeHdr.apiDagNodeHdr.name);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to cancel operation for '%s': %s",
                        workspaceGraphNode->dagNodeHdr.apiDagNodeHdr.name,
                        strGetFromStatus(status2));
            }
        }

        runningWorkItemNode = runningQueueAnchor.next;
        while (runningWorkItemNode != &runningQueueAnchor) {
            // remove head
            runningWorkItemNode->prev->next = runningWorkItemNode->next;
            runningWorkItemNode->next->prev = runningWorkItemNode->prev;

            Status status2 =
                processCompletedRunningWorkItemNode(runningWorkItemNode,
                                                    &runnableQueueAnchor,
                                                    &numRunnableWorkItem,
                                                    workspaceGraph,
                                                    queryGraph,
                                                    numTarget,
                                                    targetNodeArray,
                                                    &completedDict,
                                                    lrqMode);
            if (status2 != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "Failed process completed work item: %s",
                        strGetFromStatus(status2));
            }

            RunningWorkItemNode *nextRunningNode = runningWorkItemNode->next;
            freeRunningWorkItemNode(runningWorkItemNode);
            runningWorkItemNode = nextRunningNode;
        }
    }

    freeCompletedDict(&completedDict);

    // Make sure this is NULL before reusing it
    assert(runnableWorkItemNode == NULL);

    runnableWorkItemNode = runnableQueueAnchor.next;
    while (runnableWorkItemNode != &runnableQueueAnchor) {
        // remove head
        runnableWorkItemNode->prev->next = runnableWorkItemNode->next;
        runnableWorkItemNode->next->prev = runnableWorkItemNode->prev;

        RunnableWorkItemNode *nextRunnableNode = runnableWorkItemNode->next;
        runnableWorkItemNodeBcHandle_->freeBuf(runnableWorkItemNode);
        runnableWorkItemNode = nextRunnableNode;
    }

    assert(runnableQueueAnchor.next == &runnableQueueAnchor);
    assert(runnableQueueAnchor.prev == &runnableQueueAnchor);

    if (workspaceGraphOut != NULL && status == StatusOk) {
        *workspaceGraphOut = workspaceGraph;
    } else if (workspaceGraph != NULL) {
        queryJob->queryLock_.lock();

        Status status2 =
            dagLib->destroyDag(workspaceGraph,
                               DagTypes::DestroyDeleteAndCleanNodes);
        if (status2 != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to destroy Dag '%lu': %s",
                    workspaceGraph->getId(),
                    strGetFromStatus(status2));
        }
        workspaceGraph = NULL;
        queryJob->queryLock_.unlock();
    }

    return status;
}
