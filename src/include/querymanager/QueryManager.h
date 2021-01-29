// Copyright 2016 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _QUERY_MANAGER_H_
#define _QUERY_MANAGER_H_

#include "msg/MessageTypes.h"
#include "dag/DagTypes.h"
#include "util/IntHashTable.h"
#include "bc/BufferCache.h"
#include "runtime/Schedulable.h"
#include "callout/Callout.h"
#include "libapis/LibApisCommon.h"
#include "ns/LibNs.h"
#include "util/Stopwatch.h"
#include "runtime/Runtime.h"
#include "dag/DagLib.h"
#include "xcalar/compute/localtypes/Dataflow.pb.h"
#include "DataflowEnums.h"

class QueryManager final
{
  public:
    static constexpr const char *NsPrefix = "/query/";
    static constexpr uint64_t QueryMaxNameLen = 255;
    static constexpr uint64_t QueryJobBcNumElems = 32;
    static constexpr uint64_t QueryJobTaskBcNumElems = 32;

    StatGroupId jobStatGroupId_;
    static constexpr size_t StatCount = 2;
    struct {
        StatHandle countJobsInQueue;  // submitted but not picked up yet
        StatHandle countJobsRunning;  // number of jobs now running
    } stats;

    struct QueryStateMsg {
        LibNsTypes::NsId queryId;
        bool detailedStats;
    };

    struct QueryJobTask {
        XcalarWorkItem *workItem;
        struct QueryJobTask *prev;
        struct QueryJobTask *next;
    };

    // XXX:need a finer error state to find out the failed reason/failed query
    struct QueryJob {
        QueryJob();
        ~QueryJob();

        XcalarApiUserId userId;
        char *query = NULL;
        char queryName[QueryMaxNameLen + 1];
        char sessionName[QueryMaxNameLen + 1];
        char queryStatsHistoryDir[XcalarApiMaxPathLen + 1];
        AppGroup::Id systemsAppGroupId;
        bool systemStatsAppLaunched;
        LibNsTypes::NsId queryId;
        IntHashTableHook hook;
        LibNsTypes::NsId getQueryId() const { return queryId; };
        Atomic32 queryState;
        Status status;
        Dag *workspaceGraph = NULL;
        Dag *queryGraph = NULL;
        char *currentRunningNodeName = NULL;
        DagTypes::NodeId currentRunningNodeId = XidInvalid;
        CalloutQueue::Handle calloutHandle;
        bool calloutIssued = false;
        CalloutQueue::Handle *deleteQueryStateCalloutHandle = NULL;
        bool deleteCalloutIssued = false;
        bool markedForCancelling = false;
        Stopwatch stopwatch;
        Mutex queryLock_;
        Atomic64 ref;
        bool statsCollectionInProgress;
        bool markForDeletion;
        void incRef();
        void decRef();
    };

    struct QueryJobPendingStatsCollection {
        QueryJob *queryJob;
        QueryJobPendingStatsCollection *nextJob;
    };

    class QueryRecord final : public NsObject
    {
      public:
        QueryRecord(NodeId nodeId)
            : NsObject(sizeof(QueryRecord)), nodeId_(nodeId)
        {
        }
        ~QueryRecord() {}
        NodeId nodeId_;

      private:
        QueryRecord(const QueryRecord &) = delete;
        QueryRecord &operator=(const QueryRecord &) = delete;
    };

    static constexpr uint64_t QueryJobBcBufSize = sizeof(QueryJob);
    static constexpr uint64_t QueryJobTaskBcBufSize = sizeof(QueryJobTask);

    MustCheck static QueryManager *get();
    MustCheck static Status init();
    void destroy();

    MustCheck Status processQueryGraph(const XcalarApiUserId *userId,
                                       Dag **queryGraph,
                                       uint64_t numTarget,
                                       DagTypes::NodeId targetNodeArray[],
                                       Dag **workspaceGraphOut,
                                       const char *queryName,
                                       const char *sessionName,
                                       Runtime::SchedId schedId);

    MustCheck Status
    processDataflow(const xcalar::compute::localtypes::Dataflow::ExecuteRequest
                        *executeRetinaInput,
                    xcalar::compute::localtypes::Dataflow::ExecuteResponse
                        *executeRetinaOutput);

    MustCheck Status processQueryString(const char *query,
                                        size_t querySize,
                                        const char *queryName,
                                        const XcalarApiUserId *userId,
                                        char *sessionName,
                                        XcalarApiUdfContainer *udfContainer,
                                        bool bailOnError,
                                        Runtime::SchedId schedId,
                                        bool async,
                                        bool pinResults,
                                        bool collectStats);

    MustCheck Status requestQueryState(XcalarApiOutput **outputOut,
                                       size_t *outputSizeOut,
                                       const char *queryName,
                                       bool detailedStats);
    MustCheck Status getQueryState(QueryStateMsg *queryStateMsg,
                                   void **outputOut,
                                   size_t *outputSizeOut);

    void requestQueryStateLocal(MsgEphemeral *eph, void *payload);
    void requestQueryStateCompletion(MsgEphemeral *eph, void *payload);

    MustCheck Status requestQueryCancel(const char *queryName);
    void requestQueryCancelLocal(MsgEphemeral *eph, void *payload);

    MustCheck Status initQueryJob(QueryJob *queryJob,
                                  LibNsTypes::NsId queryId,
                                  const char *queryName,
                                  const char *sessionName,
                                  const XcalarApiUserId *userId);

    MustCheck Status requestQueryDelete(const char *queryName);
    void requestQueryDeleteLocal(MsgEphemeral *eph, void *payload);

  private:
    static constexpr uint64_t NumQueryJobHashSlots = 257;
    static constexpr uint32_t QpUniquifier = 0x7fffff00;

    QueryManager() {}
    ~QueryManager() {}

    static QueryManager *instance;
    Mutex queryJobHTLock_;
    Atomic32 qpUnique_;

    IntHashTable<LibNsTypes::NsId,
                 QueryJob,
                 &QueryJob::hook,
                 &QueryJob::getQueryId,
                 NumQueryJobHashSlots,
                 hashIdentity>
        queryJobHashTable_;

    class QueryWork : public Schedulable
    {
      public:
        QueryWork() : Schedulable("QueryWork") {}
        ~QueryWork()
        {
            if (queryInput_ != NULL) {
                memFree(queryInput_);
            }
            if (udfContainer_ != NULL) {
                memFree(udfContainer_);
            }
            if (retinaInput_ != NULL) {
                memFree(retinaInput_);
            }
        }
        void run()
        {
            if (isOptimized_) {
                runOptimized();
            } else {
                runNonOptimized();
            }
        }
        virtual void done();
        Status initWork(
            const xcalar::compute::localtypes::Dataflow::ExecuteRequest
                *executeReq,
            QueryJob *queryJob,
            XcalarApiUserId *user);

      private:
        Status queryStatus_ = StatusUnknown;
        QueryJob *queryJob_ = NULL;
        char sessionName_[XcalarApiSessionNameLen + 1];
        XcalarApiUdfContainer *udfContainer_ = NULL;
        XcalarApiUserId userId_;
        bool isOptimized_ = false;
        bool pinResults_ = false;
        bool collectStats_ = false;
        bool exportToActiveSession_ = false;
        // if set to true and job is success, delete job state
        bool cleanUpState_ = false;
        Runtime::SchedId schedId_;
        // This will be decprecated once we support more than
        // one table
        char tableName_[XcalarApiMaxTableNameLen + 1];
        char retinaName_[XcalarApiMaxTableNameLen + 1];
        char *queryInput_ = NULL;
        size_t querySize_ = 0;
        XcalarApiInput *retinaInput_ = NULL;
        size_t retinaInputSize_;
        ExecutionMode dataflowExecMode_;  // BFS or DFS

        Status getRetinaFromTemplate(DagLib::DgRetina **retinaOut);
        Status optimizeRetina(DagLib::DgRetina *retina);
        Status grabSourceRefs(DagLib::DgRetina *retina,
                              Xid **srcNodeIdsOut,
                              Dag ***srcGraphsOut,
                              DagTypes::DagId **srcGraphIdsOut,
                              XcalarApiUdfContainer **srcUdfContainersOut,
                              TableNsMgr::TableHandleTrack **srcHandleTrack,
                              unsigned *numSrcNodesOut);
        void runOptimized();
        void runNonOptimized();
    };

    QueryJobPendingStatsCollection *dataflowStatsQueueFront_ = NULL;
    QueryJobPendingStatsCollection *dataflowStatsQueueRear_ = NULL;
    Mutex dataflowStatsQueueLock_;
    class DataflowStatsAppMgr : public Schedulable
    {
      public:
        DataflowStatsAppMgr() : Schedulable("DataflowStatsAppMgr")
        {
            atomicWrite32(&activeShippers_, 0);
            atomicWrite32(&shippersDone_, 0);
            doneSem_.init(0);
        }
        virtual ~DataflowStatsAppMgr() {}
        virtual void run();
        virtual void done();

        void waitUntilDone() { doneSem_.semWait(); }

        void incActiveShippers() { atomicInc32(&activeShippers_); }

        MustCheck unsigned getActiveShippers()
        {
            return atomicRead32(&activeShippers_);
        }

        Semaphore doneSem_;
        Atomic32 activeShippers_;
        Atomic32 shippersDone_;
    };

    DataflowStatsAppMgr *dataflowStatsAppMgr_ = nullptr;
    void runDataflowStatsApp(QueryJob *queryJob);
    void scheduleDataflowStatsApp(QueryJob *queryJob);
    Status createStatsHistDirForQuery(QueryJob *queryJob);

    Status initQueryNS(const char *queryName,
                       LibNsTypes::NsHandle *nsHandle,
                       LibNsTypes::NsId *queryId,
                       bool deleteIfExists);
    static void convertProtoInputToApiInput(
        const xcalar::compute::localtypes::Dataflow::ExecuteRequest
            *executeDFProtoInput,
        XcalarApiInput *executeRetinaApiInput);
    static Status validateExecuteRequest(
        const xcalar::compute::localtypes::Dataflow::ExecuteRequest
            *executeReq);
    MustCheck Status initInternal(void);
    void destroyInternal(void);

    static void queryJobTimeout(void *arg);
    MustCheck uint32_t getUniqueUserId();
    MustCheck Status requestQueryStateInt(QueryStateMsg *queryStateMsg,
                                          NodeId dstNodeId,
                                          XcalarApiOutput **outputOut,
                                          size_t *outputSizeOut);

    struct QueryStateResponse {
        Status status = StatusOk;
        XcalarApiOutput *output = NULL;
        size_t outputSize = 0;
    };

    // Disallow
    QueryManager(const QueryManager &) = delete;
    QueryManager &operator=(const QueryManager &) = delete;
};

#endif  // _QUERY_MANAGER_H_
