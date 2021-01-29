// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "app/AppGroup.h"
#include "AppStats.h"
#include "app/App.h"
#include "app/AppMgr.h"
#include "AppGroupMgr.h"
#include "gvm/Gvm.h"
#include "strings/String.h"
#include "util/MemTrack.h"
#include "config/Config.h"
#include "util/Atomics.h"
#include "sys/XLog.h"
#include "parent/Parent.h"
#include "runtime/Runtime.h"
#include "XpuCommStream.h"
#include "xdb/Xdb.h"
#include "runtime/Async.h"

AppGroup::AppGroup()
    : txn_(),
      appInstancesCount_(0),
      nodes_(NULL),
      nodesCount_(0),
      reapable_(0),
      myGroupAllNodesDone_(0),
      overallStatus_(StatusOk),
      outJsonArray_(NULL),
      errorJsonArray_(NULL)
{
    atomicWrite32(&ref_, 1);
}

AppGroup::~AppGroup()
{
    assert(atomicRead32(&ref_) == 0);
    assert(appInstancesCount_ == 0);

    xpuCommStreamHtLock_.lock();
    xpuCommStreamHt_.removeAll(&AppGroup::XpuCommStream::doDelete);
    xpuCommStreamHtLock_.unlock();

    if (outJsonArray_ != NULL) {
        json_decref(outJsonArray_);
        outJsonArray_ = NULL;
    }

    if (errorJsonArray_ != NULL) {
        json_decref(errorJsonArray_);
        errorJsonArray_ = NULL;
    }

    if (nodes_ != NULL) {
        delete[] nodes_;
        nodes_ = NULL;
    }
}

Status
AppGroup::init(AppGroup::Id id,
               App *app,
               AppGroup::Scope scope,
               const char *userIdName,
               uint64_t sessionId,
               const char *inBlob,
               uint64_t cookie,
               Txn txn,
               LibNsTypes::NsHandle appHandle,
               unsigned *xpusPerNode,
               unsigned xpusPerNodeCount)
{
    unsigned numLocalInstances = 0;
    Config *config = Config::get();
    unsigned activeNodes = config->getActiveNodes();
    NodeId myNodeId = config->getMyNodeId();
    unsigned minXpuId = (unsigned) -1;
    bool locked = false;
    Status status = StatusOk;

    id_ = id;
    scope_ = scope;
    txn_ = txn;
    app_ = app;
    appHandle_ = appHandle;

    if (scope == Scope::Global) {
        nodesCount_ = activeNodes;

        nodes_ = new (std::nothrow) NodeInfo[nodesCount_];
        BailIfNull(nodes_);

        for (size_t ii = 0; ii < nodesCount_; ii++) {
            nodes_[ii].state = State::Inited;
            nodes_[ii].nodeId = ii;
        }

        if (app->getFlags() & App::FlagInstancePerNode) {
            minXpuId = myNodeId;
            xpuClusterSize_ = activeNodes;
            for (unsigned ii = 0; ii < activeNodes; ii++) {
                nodes_[ii].numXpus = 1;
            }
            numLocalInstances = 1;
        } else {
            xpuClusterSize_ = 0;
            assert(xpusPerNodeCount == activeNodes);
            for (unsigned ii = 0; ii < activeNodes; ii++) {
                nodes_[ii].numXpus = xpusPerNode[ii];
                if (ii == (unsigned) myNodeId) {
                    minXpuId = xpuClusterSize_;
                    numLocalInstances = nodes_[ii].numXpus;
                }
                xpuClusterSize_ += nodes_[ii].numXpus;
            }
        }
    } else {
        assert(scope_ == Scope::Local);
        nodesCount_ = 1;

        nodes_ = new (std::nothrow) NodeInfo[nodesCount_];
        BailIfNull(nodes_);

        numLocalInstances = 1;
        nodes_[0].state = State::Inited;
        nodes_[0].nodeId = Config::get()->getMyNodeId();
        nodes_[0].numXpus = numLocalInstances;
        minXpuId = 0;
        xpuClusterSize_ = numLocalInstances;
    }

    assert(numLocalInstances > 0);

    AppInstance **initialInstances;
    initialInstances =
        (AppInstance **) memAlloc(sizeof(AppInstance *) * numLocalInstances);
    BailIfNullMsg(initialInstances,
                  StatusNoMem,
                  ModuleName,
                  "App %s group %lu init failed:%s",
                  app->getName(),
                  id,
                  strGetFromStatus(status));
    memZero(initialInstances, sizeof(AppInstance *) * numLocalInstances);

    outJsonArray_ = json_array();
    if (outJsonArray_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu init failed:%s",
                app->getName(),
                id,
                strGetFromStatus(status));
        goto CommonExit;
    }

    errorJsonArray_ = json_array();
    if (errorJsonArray_ == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu init failed:%s",
                app->getName(),
                id,
                strGetFromStatus(status));
        goto CommonExit;
    }

    locked = true;
    lock_.lock();
    for (unsigned ii = 0; ii < numLocalInstances; ii++) {
        // Initialize (don't start) initial AppInstances.
        initialInstances[ii] =
            new (std::nothrow) AppInstance;  // Ref count is 1
        BailIfNullMsg(initialInstances[ii],
                      StatusNoMem,
                      ModuleName,
                      "App %s group %lu init failed:%s",
                      app->getName(),
                      id,
                      strGetFromStatus(status));

        status = initialInstances[ii]->init(app,
                                            this,
                                            inBlob,
                                            userIdName,
                                            sessionId,
                                            cookie,
                                            txn_,
                                            ii + minXpuId,
                                            xpuClusterSize_);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu init failed on AppInstance init:%s",
                    app->getName(),
                    id,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    // This node can now accept State updates about this AppGroup from other
    // nodes.
    for (unsigned ii = 0; ii < numLocalInstances; ii++) {
        // appInstances_ owns initial ref.
        initialInstances[ii]->refGet();
        appInstances_.insert(initialInstances[ii]);
        appInstancesCount_++;
        AppMgr::get()->getStats()->incrCountAppInstances();
    }
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (initialInstances != NULL) {
            for (unsigned ii = 0; ii < numLocalInstances; ii++) {
                AppInstance *instance = initialInstances[ii];
                if (instance != NULL) {
                    instance->refPut();  // drop appInstance ref
                }
            }
        }
    }

    if (locked) {
        lock_.unlock();
    }

    if (initialInstances != NULL) {
        memFree(initialInstances);
        initialInstances = NULL;
    }

    return status;
}

Status
AppGroup::startInstances(LocalConnection::Response **responseOut,
                         AppInstance **appInstances,
                         int numChildren)
{
    Status status = StatusOk;
    ParentChild **pchildren = NULL;
    Future<Status> startFutures[numChildren];
    ParentChild::Level level = ParentChild::Level::Max;
    Parent *parent = Parent::get();

    if (app_->getFlags() & App::FlagSysLevel) {
        level = ParentChild::Level::Sys;
    } else {
        level = ParentChild::Level::User;
    }

    memZero(responseOut, sizeof(LocalConnection::Response *) * numChildren);

    pchildren = (ParentChild **) memAlloc(sizeof(ParentChild *) * numChildren);
    if (pchildren == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "AppGroup %lu startInstances failed: %s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    memZero(pchildren, sizeof(ParentChild *) * numChildren);

    status = parent->getChildren(pchildren,
                                 numChildren,
                                 level,
                                 ParentChild::getXpuSchedId(
                                     Txn::currentTxn().rtSchedId_));
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "AppGroup %lu startInstances failed on getChildren: %s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    for (int ii = 0; ii < numChildren; ii++) {
        status = asyncObj(&startFutures[ii],
                          appInstances[ii],
                          &AppInstance::startMyInstance,
                          &responseOut[ii],
                          pchildren[ii]);
        BailIfFailed(status);
        pchildren[ii] = NULL;  // AppInstance owns it now
    }

    for (int ii = 0; ii < numChildren; ii++) {
        startFutures[ii].wait();
        status = startFutures[ii].get();
        BailIfFailed(status);
    }

CommonExit:
    for (int ii = 0; ii < numChildren; ii++) {
        if (startFutures[ii].valid()) {
            // wait for any stray futures that didn't get grabbed; this should
            // only happen in the error case
            assert(status != StatusOk);
            startFutures[ii].wait();
            startFutures[ii].get();  // ignore the output; keep original status
        }
    }
    if (pchildren != NULL) {
        for (int ii = 0; ii < numChildren; ii++) {
            ParentChild *newChild = pchildren[ii];
            if (newChild != NULL) {
                assert(status != StatusOk);
                newChild->abortConnection(status);
            }
        }
        memFree(pchildren);
        pchildren = NULL;
    }

    return status;
}

Status
AppGroup::localMyGroupStart()
{
    Status status = StatusOk;

    lock_.lock();
    if (appInstancesCount_ == 0) {
        lock_.unlock();
        return status;
    }
    assert(appInstancesCount_ > 0);

    AppInstance *inst;
    AppInstance *instances[appInstancesCount_];
    LocalConnection::Response *responseOut[appInstancesCount_];
    size_t instanceCount = appInstancesCount_;

    // Lock around the appInstances_ table and refGet all instances
    auto instanceIter = appInstances_.begin();
    size_t ii = 0;
    while ((inst = instanceIter.get()) != NULL) {
        inst->refGet();
        instances[ii] = inst;
        instanceIter.next();
        ++ii;
    }
    lock_.unlock();

    status = startInstances(responseOut, instances, instanceCount);

    for (size_t instNum = 0; instNum < instanceCount; instNum++) {
        inst = instances[instNum];
        LocalConnection::Response *response = responseOut[instNum];
        if (status != StatusOk) {
            if (response != NULL) {
                xSyslog(ModuleName,
                        XlogErr,
                        "App Group %lu localMyGroupStart failed on"
                        " startInstances %lu: %s",
                        id_,
                        instNum,
                        strGetFromStatus(status));

                lock_.lock();
                const ProtoResponseMsg *responseMsg = response->get();
                assert(responseMsg != NULL);
                assert(!responseMsg->error().empty());
                Status appendStatus =
                    appendOutput(NULL, responseMsg->error().c_str());
                if (appendStatus != StatusOk) {
                    xSyslog(ModuleName,
                            XlogErr,
                            "App Group %lu localMyGroupStart failed on"
                            " appendOutput %lu: %s",
                            id_,
                            instNum,
                            strGetFromStatus(appendStatus));
                }
                lock_.unlock();
                response->refPut();
                response = NULL;
            }
        }
        inst->refPut();
    }

    if (status != StatusOk) {
        goto CommonExit;
    }

    lock_.lock();

    for (size_t jj = 0; jj < nodesCount_; jj++) {
        if (nodes_[jj].state == State::Inited) {
            nodes_[jj].state = State::Running;
        }
    }
    lock_.unlock();

CommonExit:
    return status;
}

void
AppGroup::waitForPendingAbort()
{
TryAgain:
    lock_.lock();
    if (abortStillPending_) {
        abortCompletion_.wait(&lock_);
        lock_.unlock();
        goto TryAgain;
    }
    lock_.unlock();
}

void
AppGroup::waitForMyGroupAllNodesDone()
{
    myGroupAllNodesDone_.semWait();
}

// Wait for, then collect, group output.
Status
AppGroup::waitForMyGroupResult(uint64_t usecsTimeout,
                               char **outBlobOut,
                               char **errorBlobOut,
                               bool *retAppInternalError)
{
    Status status;

    *outBlobOut = NULL;
    *errorBlobOut = NULL;

    // Wait for AppGroup to terminate on all nodes.
    if (usecsTimeout != 0) {
        status = reapable_.timedWait(usecsTimeout);
        if (status == StatusTimedOut) {
            AppMgr::get()->getStats()->incCountAppWaitForResultTimeout();
            return status;
        }
    } else {
        reapable_.semWait();
    }

    unsigned nodeCount = Config::get()->getActiveNodes();
    void *outputPerNode[nodeCount];
    size_t sizePerNode[nodeCount];

    // Needed for local case.
    memZero(outputPerNode, sizeof(outputPerNode));
    memZero(sizePerNode, sizeof(sizePerNode));

    AppGroupMgr *appGroupMgr = AppMgr::get()->appGroupMgr_;
    AppGroup::Id id = getMyId();
    status = appGroupMgr->reapThisGroup(getMyId(),
                                        getMyScope(),
                                        outputPerNode,
                                        sizePerNode,
                                        retAppInternalError);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu waitForMyGroupResult failed on reapThisGroup:%s",
                id,
                strGetFromStatus(status));
    }

    Status statusUnpack =
        appGroupMgr->unpackOutput(id,
                                  (AppGroupMgr::OutputPacked **) outputPerNode,
                                  sizePerNode,
                                  nodeCount,
                                  outBlobOut,
                                  errorBlobOut);
    if (statusUnpack != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu waitForMyGroupResult failed on unpackOutput:%s",
                id,
                strGetFromStatus(statusUnpack));
    }

    if (statusUnpack != StatusOk && status == StatusOk) {
        status = statusUnpack;
    }

    for (size_t ii = 0; ii < nodeCount; ii++) {
        if (outputPerNode[ii] != NULL) {
            memFree(outputPerNode[ii]);
        }
    }
    return status;
}

//
// Handlers for certain events coming in from other nodes or running
// AppInstances.
//
void
AppGroup::localMyGroupAbort(Status status)
{
    assert(status != StatusOk);
    assert(txn_.valid());
    AppInstance *instance = NULL;

    xSyslog(ModuleName,
            XlogErr,
            "App Group %lu localMyGroupAbort:%s",
            id_,
            strGetFromStatus(status));

    lock_.lock();

    assert(abortInvoked_ == true);
    assert(abortStillPending_ == false);

    NodeInfo *myNodeInfo = getNodeInfo(Config::get()->getMyNodeId());
    assert(myNodeInfo);

    if (overallStatus_ == StatusOk || overallStatus_ == StatusFailed) {
        overallStatus_ = status;
    }

    if (myNodeInfo->state == State::Done) {
        lock_.unlock();
        return;
    }

    AppMgr::get()->getStats()->appGroupAbort();
    abortStillPending_ = true;

TryAgain:
    for (AppInstance::AppGroupHashTable::iterator it = appInstances_.begin();
         (instance = it.get()) != NULL;
         it.next()) {
        // For the case when AppInstance Abort is racing with the AppInstance
        // start, we need to grab AppInstance reference here.
        instance->refGet();
        lock_.unlock();
        instance->abortMyInstance(status);
        instance->refPut();
        lock_.lock();
        goto TryAgain;
    }

    abortStillPending_ = false;
    abortCompletion_.broadcast();
    lock_.unlock();
}

// Caller must be holding lock_.
Status
AppGroup::appendOutput(const char *outStr, const char *errStr)
{
    Status status = StatusOk;
    int ret;
    const char *thisOut;
    const char *thisErr;
    json_t *outJsonStr = NULL;
    json_t *errJsonStr = NULL;

    thisOut = (outStr != NULL) ? outStr : "";
    thisErr = (errStr != NULL) ? errStr : "";

    //
    // Handle outStr
    //
    outJsonStr = json_string(thisOut);
    if (outJsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu appendOutput failed:%s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // The 'new' part of append_new means that the array steals the
    // reference from the element being appended, which is useful here.
    ret = json_array_append_new(outJsonArray_, outJsonStr);
    if (ret == -1) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu appendOutput failed to append json output '%s'",
                id_,
                outStr);
        status = StatusFailed;
        goto CommonExit;
    }
    outJsonStr = NULL;

    //
    // Handle errStr
    //
    errJsonStr = json_string(thisErr);
    if (errJsonStr == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu appendOutput failed:%s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // The 'new' part of append_new means that the array steals the
    // reference from the element being appended, which is useful here.
    ret = json_array_append_new(errorJsonArray_, errJsonStr);
    if (ret == -1) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu appendOutput failed to append json error '%s'",
                id_,
                errStr);
        status = StatusFailed;
        goto CommonExit;
    }
    errJsonStr = NULL;

CommonExit:
    if (outJsonStr != NULL) {
        json_decref(outJsonStr);
        outJsonStr = NULL;
    }
    if (errJsonStr != NULL) {
        json_decref(errJsonStr);
        errJsonStr = NULL;
    }
    return status;
}

// This function releases the lock!
void
AppGroup::localRemoveAppInstanceFromGroup(AppInstance *appInstance)
{
    // We need to be holding the lock
    assert(!lock_.tryLock());

    if (appInstances_.remove(appInstance->getMyXpuId()) == NULL) {
        // If this is already removed, the notification is being handled
        // elsewhere.
        lock_.unlock();
        return;
    }

    assert(appInstancesCount_ > 0);
    appInstancesCount_--;
    AppMgr::get()->getStats()->decrCountAppInstances();
    appInstance->refPut();  // appInstances_ ref.

    bool sendGlobalDone = false;
    bool postReapable = false;
    if (appInstancesCount_ == 0) {
        if (scope_ == Scope::Global) {
            sendGlobalDone = true;
        } else {
            assert(scope_ == Scope::Local);
            assert(nodesCount_ == 1);
            // Local Scope means that there is only one node (node 0). We can
            // set this node to Done.
            nodes_[0].state = State::Done;
            postReapable = true;
        }
    }

    lock_.unlock();

    if (postReapable) {
        assert(!sendGlobalDone);
        reapable_.post();
        myGroupAllNodesDone_.post();
    } else if (sendGlobalDone) {
        static constexpr const size_t TxnAbortMaxRetryCount = 1000;
        static constexpr uint64_t TxnAbortSleepUSec = 1000;
        size_t ii = 0;
        Runtime *rt = Runtime::get();
        SchedulableNodeDone *schedNodeDone = NULL;
        do {
            if (schedNodeDone == NULL) {
                schedNodeDone = new (std::nothrow) SchedulableNodeDone(id_);
            }
            if (schedNodeDone != NULL) {
                Status status = Runtime::get()->schedule(schedNodeDone);
                if (status == StatusOk) {
                    return;
                }
            }
            // Kick Txn abort on Schedulables in the hope that some resources
            // will be released.
            rt->kickSchedsTxnAbort();
            ii++;
            sysUSleep(TxnAbortSleepUSec);
        } while (ii < TxnAbortMaxRetryCount);
        buggyPanic("Failed to schedule SchedulableNodeDone");
    }
}

// AppInstance has finished. Concatenate its output with output from other
// instances.
void
AppGroup::localMyGroupInstanceDone(AppInstance *appInstance,
                                   Status doneStatus,
                                   const char *outStr,
                                   const char *errStr)
{
    lock_.lock();

    if (doneStatus != StatusOk &&
        (overallStatus_ == StatusOk || doneStatus != StatusConnReset)) {
        // StatusConnReset may just be a symptom. Try to
        // capture the first order error status.
        if (doneStatus == StatusConnReset) {
            doneStatus = StatusXpuConnAborted;
        }
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupInstanceDone %lu failed"
                " outStr '%s' errStr '%s': %s",
                id_,
                appInstance->getMyXpuId(),
                outStr,
                errStr,
                strGetFromStatus(doneStatus));
        overallStatus_ = doneStatus;
    }

    Status appendStatus = appendOutput(outStr, errStr);
    if (appendStatus != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupInstanceDone failed on"
                " appendOutput: %s",
                id_,
                strGetFromStatus(appendStatus));
    }

    if (appendStatus != StatusOk && overallStatus_ == StatusOk) {
        overallStatus_ = appendStatus;
    }

    // This function releases the lock
    localRemoveAppInstanceFromGroup(appInstance);
    appInstance = NULL;
}

// A certain node has told us its done with this group.
void
AppGroup::localMyGroupNodeDone(NodeId nodeId)
{
    bool doneGlobal = true;
    lock_.lock();

    assert(scope_ == Scope::Global);
    assert(nodeId < nodesCount_);
    NodeInfo *nodeInfo = getNodeInfo(nodeId);
    if (nodeInfo->state != State::Done) {
        nodeInfo->state = State::Done;
    }
    for (size_t ii = 0; ii < nodesCount_; ii++) {
        if (nodes_[ii].state != State::Done) {
            doneGlobal = false;
        }
    }

    lock_.unlock();

    if (doneGlobal) {
        reapable_.post();
        myGroupAllNodesDone_.post();
    }
}

// Gather output/errors and return overall status. outputSize gives the max
// amount of space available in payload. This doesn't concern itself with
// AppGroup state. On abort, it can be called before the AppGroup is fully
// done and is just concerned with what we collected thus far.
Status
AppGroup::localMyGroupReap(void *output, size_t *outputSizeOut)
{
    Status status = StatusOk;
    OutputPacked *pack = NULL;
    char *outStr = NULL;
    char *errStr = NULL;
    size_t outBlobLen;
    size_t errorBlobLen;
    size_t packedSize = 0;

    memZero(output, AppInstance::InputOutputLimit);

    lock_.lock();

    outStr = json_dumps(outJsonArray_, 0);
    if (outStr == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupReap failed on json_dumps:%s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    errStr = json_dumps(errorJsonArray_, 0);
    if (errStr == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupReap failed on json_dumps:%s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    outBlobLen = strlen(outStr);
    errorBlobLen = strlen(errStr);

    if (AppInstance::InputOutputLimit < sizeof(OutputPacked)) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupReap failed on OutputPacked"
                " allocation:%s",
                id_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // 1 for each '\0'
    packedSize =
        sizeof(OutputPacked) + errorBlobLen + outBlobLen + 2 + sizeof(Status);
    if (AppInstance::InputOutputLimit < packedSize) {
        status = StatusNoBufs;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu localMyGroupReap Error packing app output; "
                "required size %zu greater than preallocated size %zu",
                id_,
                packedSize,
                AppInstance::InputOutputLimit);
        goto CommonExit;
    }

    pack = (OutputPacked *) output;
    if (errorBlobLen > 0) {
        pack->errorSize = errorBlobLen + 1;
        verify(strlcpy(pack->pack, errStr, errorBlobLen + 1) <
               errorBlobLen + 1);
    } else {
        pack->errorSize = 0;
    }

    pack->outputSize = 0;
    if (outBlobLen > 0) {
        pack->outputSize = outBlobLen + 1;
        verify(strlcpy(pack->pack + pack->errorSize, outStr, outBlobLen + 1) <
               outBlobLen + 1);
    }

    assert(txn_.valid());
    assert(appInstancesCount_ == 0);

    if (overallStatus_ != StatusOk) {
        // This is needed to free any outstanding memory that was allocated to
        // XPUs
        status = XdbMgr::get()->bcScanCleanout(txn_.id_);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu localMyGroupReap failed to do buffer cache"
                    " clean out for txnId %lu: %s",
                    id_,
                    txn_.id_,
                    strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;
    reapDone_ = true;

CommonExit:
    if (status != StatusOk) {
        if (overallStatus_ == StatusOk) {
            // Failure to gather output is failure of operation.
            overallStatus_ = status;
        }
        *outputSizeOut = 0;
    } else {
        *outputSizeOut = packedSize;
        *(Status *) ((uintptr_t) output + packedSize - sizeof(Status)) =
            overallStatus_;
    }

    lock_.unlock();

    if (outStr) {
        memFree(outStr);
        outStr = NULL;
    }

    if (errStr) {
        memFree(errStr);
        errStr = NULL;
    }

    return status;
}

bool
AppGroup::kickLocalAbort()
{
    bool kickAbort = false;
    lock_.lock();
    if (abortInvoked_ == false) {
        abortInvoked_ = true;
        kickAbort = true;
    }
    lock_.unlock();
    return kickAbort;
}

AppGroup::SchedulableNodeDone::SchedulableNodeDone(AppGroup::Id id)
    : Schedulable("SchedulableNodeDone"), id_(id)
{
}

void
AppGroup::SchedulableNodeDone::run()
{
    Status status = AppMgr::get()->appGroupMgr_->nodeDoneThisGroup(id_);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu failed on nodeDoneThisGroup: %s",
                id_,
                strGetFromStatus(status));
        status = AppMgr::get()->appGroupMgr_->abortThisGroup(id_, status, NULL);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App Group %lu localRemoveAppInstanceFromGroup failed on"
                    " nodeDoneThisGroup: %s",
                    id_,
                    strGetFromStatus(status));
        }
    }
}

void
AppGroup::SchedulableNodeDone::done()
{
    delete this;
}

AppGroup::XpuCommStreamBuf::~XpuCommStreamBuf()
{
    // payload is the sendContext and may out live an AppGroup in the event
    // of an AppGroup transaction abort.
    payload = NULL;
}

void
AppGroup::XpuCommStreamBuf::doDelete()
{
    delete this;
}

size_t
AppGroup::XpuCommStreamBuf::getMySeq() const
{
    MsgStreamMgr::ProtocolDataUnit *pdu =
        (MsgStreamMgr::ProtocolDataUnit *) payload;
    XpuCommObjectAction::XpuCommBuffer *cbuf =
        (XpuCommObjectAction::XpuCommBuffer *) pdu->buffer_;
    return cbuf->seqNum;
}

void
AppGroup::XpuCommStream::doDelete()
{
    delete this;
}

AppGroup::SrcXpuCommStream::~SrcXpuCommStream()
{
    lock_.lock();
    srcXpuCommStreamBufHt_.removeAll(&AppGroup::XpuCommStreamBuf::doDelete);
    lock_.unlock();

    if (reqHandler_ != NULL) {
        // This is an error case where the stream encountered an error. So don't
        // bother sending response to the source XPU. It should have been
        // cleaned out already, so there is no one to receive your response
        // anyway :).
        reqHandler_->dontSendResponse();
        reqHandler_->done();
    }
}

AppGroup::DstXpuCommStream::~DstXpuCommStream()
{
    if (recvBuf_ != NULL) {
        assert(numRecvBufs_ > 0);
        for (size_t ii = 0; ii < numRecvBufs_; ii++) {
            if (recvBuf_[ii].buf) {
                XdbMgr::get()->bcFree(recvBuf_[ii].buf);
                recvBuf_[ii].buf = NULL;
            }
        }
        memFree(recvBuf_);
        recvBuf_ = NULL;
    }
}

Status
AppGroup::dispatchToDstXpu(AppInstance::BufDesc *recvBufs,
                           size_t numRecvBufs,
                           unsigned srcXpuId,
                           unsigned dstXpu)
{
    Status status = StatusOk;
    AppInstance *appInstance = NULL;

    lock_.lock();
    appInstance = appInstances_.find((size_t) dstXpu);
    if (appInstance == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu numRecvBufs %lu srcXpuId %u dstXpuId %u"
                " dispatchToDstXpu failed: %s",
                id_,
                numRecvBufs,
                srcXpuId,
                dstXpu,
                strGetFromStatus(status));
        lock_.unlock();
        goto CommonExit;
    }

    appInstance->refGet();
    lock_.unlock();

    status = appInstance->dispatchToDstXpu(recvBufs, numRecvBufs, srcXpuId);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App Group %lu numRecvBufs %lu srcXpuId %u dstXpuId %u"
                " dispatchToDstXpu failed: %s",
                id_,
                numRecvBufs,
                srcXpuId,
                dstXpu,
                strGetFromStatus(status));
        appInstance->refPut();
        goto CommonExit;
    }
    appInstance->refPut();

CommonExit:
    return status;
}

bool
AppGroup::isXpuIdValid(unsigned xpuId)
{
    if (xpuId < xpuClusterSize_) {
        return true;
    } else {
        return false;
    }
}

unsigned
AppGroup::getNumXpusPerXceNode(NodeId nodeId)
{
    assert((unsigned) nodeId < Config::get()->getActiveNodes());
    unsigned numXpus = 0;
    if (scope_ == Scope::Global) {
        numXpus = nodes_[nodeId].numXpus;
    } else {
        assert(scope_ == Scope::Local);
        assert(nodesCount_ == 1);
        numXpus = nodes_[0].numXpus;
    }
    assert(numXpus > 0);
    return numXpus;
}

NodeId
AppGroup::getXceNodeIdFromXpuId(unsigned xpuId)
{
    Config *config = Config::get();
    assert(isXpuIdValid(xpuId));

    if (getMyScope() == Scope::Global) {
        if (app_->getFlags() & App::FlagInstancePerNode) {
            return xpuId;
        } else {
            unsigned activeNodes = config->getActiveNodes();
            unsigned curXpuId = 0;
            unsigned xceNode = 0;
            for (xceNode = 0; xceNode < activeNodes; xceNode++) {
                unsigned numXpus = getNumXpusPerXceNode((NodeId) xceNode);
                if (xpuId >= curXpuId && (xpuId < curXpuId + numXpus)) {
                    break;
                } else {
                    curXpuId += numXpus;
                }
            }
            assert(xceNode < activeNodes);
            return xceNode;
        }
    } else {
        return config->getMyNodeId();
    }
}

AppGroup::NodeInfo *
AppGroup::getNodeInfo(NodeId nodeId) const
{
    for (size_t ii = 0; ii < nodesCount_; ii++) {
        if (nodes_[ii].nodeId == nodeId) {
            return &nodes_[ii];
        }
    }
    return NULL;
}

AppGroup::Id
AppGroup::getMyId() const
{
    return id_;
}

AppGroup::Scope
AppGroup::getMyScope() const
{
    return scope_;
}

void
AppGroup::incRef()
{
    assert(atomicRead32(&ref_) > 0);
    atomicInc32(&ref_);
}

void
AppGroup::decRef()
{
    assert(atomicRead32(&ref_) > 0);
    if (atomicDec32(&ref_) == 0) {
        delete this;
    }
}
