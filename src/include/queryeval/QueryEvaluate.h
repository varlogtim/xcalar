// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERY_EVALUATE_H
#define _QUERY_EVALUATE_H

#include "querymanager/QueryManager.h"
#include "dag/DagNodeTypes.h"
#include "libapis/ApiHandler.h"
#include "xdb/HashTree.h"
#include "runtime/Runtime.h"
#include "util/Atomics.h"

class OperatorHandler;

class QueryEvaluate final
{
  private:
    QueryEvaluate() { atomicWrite64(&opSchedRoundRobin_, 0); };
    ~QueryEvaluate(){};

    static QueryEvaluate *instance;
    MustCheck Status initInternal();
    void destroyInternal();

    BcHandle *runningWorkItemNodeBcHandle_ = NULL;
    BcHandle *runnableWorkItemNodeBcHandle_ = NULL;
    BcHandle *completedWorkItemNodeBcHandle_ = NULL;

    Atomic64 opSchedRoundRobin_;
    MustCheck Runtime::SchedId pickSchedToRun();

    // All the information to logically encapsulate a running
    // xcalarApiWorkItem
    struct RunningWorkItemNode {
        DagNodeTypes::Node *workspaceGraphNode;
        DagTypes::NodeId workspaceGraphNodeId;
        DagNodeTypes::Node *queryGraphNode;
        DagTypes::NodeId queryGraphNodeId;
        XcalarWorkItem *workItem;
        bool workItemFailed;
        Status workItemFailureStatus;
        OperatorHandler *operatorHandler;
        RunningWorkItemNode *prev;
        RunningWorkItemNode *next;
    };

    struct RunnableWorkItemNode {
        DagTypes::NodeId queryGraphNodeId;
        struct RunnableWorkItemNode *prev;
        struct RunnableWorkItemNode *next;
    };

    // Only used to store WorkItemNodes that has run to completion successfully
    struct CompletedWorkItemNode {
        IntHashTableHook hook;
        DagTypes::NodeId getQueryGraphNodeId() const
        {
            return queryGraphNodeId;
        };

        DagTypes::NodeId queryGraphNodeId;
        DagTypes::NodeId workspaceGraphNodeId;
        void del();
    };

    typedef IntHashTable<DagTypes::NodeId,
                         CompletedWorkItemNode,
                         &CompletedWorkItemNode::hook,
                         &CompletedWorkItemNode::getQueryGraphNodeId,
                         128,
                         hashIdentity>
        CompletedDictHashTable;

    static bool parentsInWorkspace(Dag *workspaceGraph,
                                   DagNodeTypes::Node *node);

    MustCheck Status executeQueryGraphNode(XcalarWorkItem **workItemOut,
                                           OperatorHandler *operatorHandler,
                                           void *optimizerAnnotations);

    MustCheck Status
    executeQueryGraphNodePre(XcalarApiUserId *userId,
                             Dag *workspaceGraph,
                             Dag *queryGraph,
                             DagTypes::NodeId queryGraphNodeId,
                             bool dropSrcSlots,
                             QueryManager::QueryJob *queryJob,
                             RunningWorkItemNode **runningWorkItemNodeOut,
                             CompletedDictHashTable *completedDict,
                             Runtime::SchedId schedId);

    MustCheck bool isTarget(DagTypes::NodeId dagNodeId,
                            uint64_t numTarget,
                            DagTypes::NodeId *targetNodeArray);

    MustCheck Status deleteLrqDataset(XcalarApiBulkLoadOutput *loadOutput);
    MustCheck bool checkParentsCanBeDroppedDuringEval(
        Dag *workspaceGraph,
        Dag *queryGraph,
        DagTypes::NodeId queryGraphNodeId,
        uint64_t numTargets,
        DagTypes::NodeId targetNodeArray[],
        CompletedDictHashTable *completedDict);

    MustCheck Status dropUnneededParents(Dag *workspaceGraph,
                                         Dag *queryGraph,
                                         DagTypes::NodeId queryGraphNodeId,
                                         uint64_t numTargets,
                                         DagTypes::NodeId targetNodeArray[],
                                         CompletedDictHashTable *completedDict);

    MustCheck Status dropUnneededDatasets(Dag *queryGraph,
                                          uint64_t numTarget,
                                          DagTypes::NodeId targetNodeArray[],
                                          CompletedDictHashTable *completedDict,
                                          Dag *workspaceGraph);

    MustCheck Status
    addChildrenToRunnableQueue(RunnableWorkItemNode *runnableQueue,
                               uint64_t *numQueuedTask,
                               Dag *queryGraph,
                               DagTypes::NodeId queryGraphNodeId,
                               CompletedDictHashTable *completedDict,
                               ExecutionMode lrqMode);

    MustCheck Status processCompletedRunningWorkItemNode(
        RunningWorkItemNode *runningWorkItemNode,
        RunnableWorkItemNode *runnableQueue,
        uint64_t *numQueuedTask,
        Dag *workspaceGraph,
        Dag *queryGraph,
        uint64_t numTargets,
        DagTypes::NodeId targetNodeArray[],
        CompletedDictHashTable *completedDict,
        ExecutionMode lrqMode);

    void freeCompletedDictEle(CompletedWorkItemNode *node);

    MustCheck Status stepThroughNode(Dag *queryGraph,
                                     DagNodeTypes::Node *queryGraphNode,
                                     Dag *workspaceGraph,
                                     Runtime::SchedId schedId,
                                     bool isLrq,
                                     bool pinNode,
                                     QueryManager::QueryJob *queryJob,
                                     DagTypes::NodeId *workspaceGraphNodeIdOut);

    void freeCompletedDict(CompletedDictHashTable *completedDict);
    void freeRunningWorkItemNode(RunningWorkItemNode *runningWorkItemNode);

    // Disallow
    QueryEvaluate(const QueryEvaluate &) = delete;
    QueryEvaluate &operator=(const QueryEvaluate &) = delete;

    class ExecuteQueryGraphNodePre : public Schedulable
    {
      public:
        ExecuteQueryGraphNodePre(XcalarApiUserId *userId,
                                 DagTypes::NodeId queryGraphNodeId,
                                 bool dropSrcSlots,
                                 QueryManager::QueryJob *queryJob,
                                 RunningWorkItemNode *runningQueueAnchor,
                                 CompletedDictHashTable *completedDict,
                                 Runtime::SchedId schedId,
                                 Mutex *lock,
                                 CondVar *cv,
                                 Atomic32 *execSchedsDone)
            : Schedulable("ExecuteQueryGraphNodePre"),
              userId_(userId),
              queryGraphNodeId_(queryGraphNodeId),
              dropSrcSlots_(dropSrcSlots),
              queryJob_(queryJob),
              runningQueueAnchor_(runningQueueAnchor),
              completedDict_(completedDict),
              schedId_(schedId),
              lock_(lock),
              cv_(cv),
              execSchedsDone_(execSchedsDone)
        {
            workspaceGraph_ = queryJob_->workspaceGraph;
            queryGraph_ = queryJob_->queryGraph;
        }
        ~ExecuteQueryGraphNodePre() {}

        virtual void run();
        virtual void done();

        // Stash Inputs
        XcalarApiUserId *userId_;
        DagTypes::NodeId queryGraphNodeId_;
        bool dropSrcSlots_;
        QueryManager::QueryJob *queryJob_;
        RunningWorkItemNode *runningQueueAnchor_;
        CompletedDictHashTable *completedDict_;
        Runtime::SchedId schedId_;
        Mutex *lock_;
        CondVar *cv_;
        Atomic32 *execSchedsDone_;
        Dag *workspaceGraph_;
        Dag *queryGraph_;

        // Outputs
        bool start_ = false;
        Status retStatus_;
    };

  public:
    static constexpr size_t RunnableBcBufSize = sizeof(RunnableWorkItemNode);
    static constexpr size_t RunningBcBufSize = sizeof(RunningWorkItemNode);
    static constexpr size_t CompletedBcBufSize = sizeof(CompletedWorkItemNode);

    static MustCheck QueryEvaluate *get();
    static MustCheck Status init();
    void destroy();

    MustCheck Status stepThrough(Dag *workspaceGraph,
                                 Dag **queryGraphIn,
                                 uint64_t numDagNodes,
                                 bool bailOnError,
                                 Runtime::SchedId schedId,
                                 bool pinResults,
                                 QueryManager::QueryJob *queryJob);

    MustCheck Status evaluate(Dag *dag,
                              uint64_t numTarget,
                              DagTypes::NodeId *targetNodeArray,
                              QueryManager::QueryJob *queryJob,
                              Dag **workspaceGraphOut,
                              Runtime::SchedId schedId,
                              ExecutionMode lrqMode,
                              XcalarApiUdfContainer *sessionContainer);

    MustCheck Status updateSelectNodes(Dag *queryGraph);

    enum : uint64_t {
        RunningBcNumElems = 128,
        RunnableBcNumElems = 128,
        CompletedBcNumElems = 128,
    };
};
#endif  // _QUERY_EVALUATE_H
