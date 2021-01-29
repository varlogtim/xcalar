// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/select.h>

#include "StrlFunc.h"
#include "app/AppInstance.h"
#include "app/App.h"
#include "AppGroupMgr.h"
#include "app/AppGroup.h"
#include "app/AppMgr.h"
#include "AppStats.h"
#include "parent/Parent.h"
#include "parent/ParentChild.h"
#include "msg/Xid.h"
#include "runtime/Runtime.h"
#include "dataset/AppLoader.h"
#include "util/MemTrack.h"
#include "util/Atomics.h"
#include "util/FileUtils.h"
#include "util/Stopwatch.h"
#include "util/Standard.h"
#include "xdb/Xdb.h"
#include "df/DataFormat.h"
#include "XpuCommStream.h"
#include "bc/BufCacheMemMgr.h"
#include "sys/XLog.h"
#include "table/ResultSet.h"
#include "table/Table.h"
#include "dataformat/DataFormatCsv.h"

//
// Starting/stopping an App in an XPU.
//

AppInstance::AppInstance()
    : app_(NULL),
      group_(NULL),
      state_(State::Invalid),
      child_(NULL),
      txn_(),
      pid_(-1),
      sideBufsLoadedSem_(0)
{
    atomicWrite32(&ref_, 1);
    atomicWrite64(&outstandingBuffers_, 1);
    atomicWrite64(&outstandingOps_, 0);

    memZero(loadResult_.errorFileBuf, sizeof(loadResult_.errorFileBuf));
    memZero(loadResult_.errorStringBuf, sizeof(loadResult_.errorStringBuf));
    loadResult_.numBytes = 0;
    loadResult_.numBuffers = 0;
    loadResult_.numFiles = 0;
    loadResult_.status = StatusOk;
}

AppInstance::~AppInstance()
{
    assert(atomicRead32(&ref_) == 0);
    assert(child_ == NULL);
    group_->decRef();
    group_ = NULL;
}

void
AppInstance::refGet()
{
    assert(atomicRead32(&ref_) > 0);
    atomicInc32(&ref_);
}

void
AppInstance::refPut()
{
    assert(atomicRead32(&ref_) > 0);
    if (atomicDec32(&ref_) == 0) {
        destroy();
    }
}

void
AppInstance::destroy()
{
    if (startArgs_) {
        delete startArgs_;
        startArgs_ = NULL;
    }
    delete this;
}

// Allocate and prepare all resources except the child.
Status
AppInstance::init(App *app,
                  AppGroup *group,
                  const char *inBlob,
                  const char *userIdName,
                  uint64_t sessionId,
                  uint64_t cookie,
                  Txn txn,
                  unsigned xpuId,
                  unsigned xpuClusterSize)
{
    const uint8_t *exec;
    size_t execSize;
    Status status = StatusOk;
    Config *config = Config::get();
    unsigned activeNodes = config->getActiveNodes();
    unsigned curXpuId = 0;

    lock_.lock();

    cookie_ = cookie;
    txn_ = txn;
    app_ = app;
    group_ = group;
    group_->incRef();
    xpuId_ = xpuId;

    app_->getExec(&exec, &execSize);

    // Allocate any memory needed for startArgs_. This will be freed by
    // ~AppInstance.
    try {
        startArgs_ = new ChildAppStartRequest();  // can throw

        startArgs_->set_exec(exec, execSize);  // can throw
        startArgs_->set_instr(inBlob);         // can throw
        startArgs_->set_hosttype((uint32_t) app_->getHostType());
        startArgs_->set_appname(app_->getName());  // can throw
        startArgs_->set_username(userIdName);      // can throw
        startArgs_->set_sessionid(sessionId);
        startArgs_->set_xpuid(xpuId);
        startArgs_->set_xpuclustersize(xpuClusterSize);
        for (unsigned ii = 0; ii < activeNodes; ii++) {
            ChildAppStartRequest_XpuIdRange *tmp = startArgs_->add_xpuidrange();
            unsigned numInst = group_->getNumXpusPerXceNode(ii);
            tmp->set_xpuidstart(curXpuId);
            tmp->set_xpuidend(curXpuId + numInst - 1);
            curXpuId += numInst;
        }
    } catch (std::exception &e) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u init failed: %s, %s",
                app->getName(),
                group->getMyId(),
                xpuId,
                strGetFromStatus(status),
                e.what());
        goto CommonExit;
    }

    state_ = State::Inited;

CommonExit:
    lock_.unlock();
    return status;
}

// Grab a child. Asynchronously run App.
Status
AppInstance::startMyInstance(LocalConnection::Response **responseOut,
                             ParentChild *newChild)
{
    Status status;
    // Begin tracking how long start takes.
    Stopwatch stopwatch;
    ProtoChildRequest childStartRequest;
    LocalConnection::Response *response = NULL;

    lock_.lock();
    State prevState = state_;

    assert(child_ == NULL && "child_ should be acquired once in start");

    //
    // Acquire a child and prepare it to execute AppInstance.
    //
    child_ = newChild;
    pid_ = child_->getPid();
    child_->registerParent(this);  // Get notifications about child events.
    AppMgr::get()->registerAppInstance(this);  // Route AppInstance calls here.

    if (state_ == State::Destructing) {
        doneCondVar_.wait(&lock_);
        status = StatusCanceled;
        goto CommonExit;
    } else if (state_ == State::Done) {
        status = StatusCanceled;
        goto CommonExit;
    }
    assert(state_ == State::Inited && "Shouldn't be running before start");

    //
    // Send "start this AppInstance" request to our child. Deal with response.
    //
    childStartRequest.set_func(ChildFuncAppStart);
    childStartRequest.set_allocated_appstart(startArgs_);  // Setup in init.

    status = child_->sendAsync(&childStartRequest, &response);
    childStartRequest.release_appstart();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u startMyInstance failed to send"
                " start to child: %s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    state_ = State::Running;
    startCondVar_.broadcast();
    lock_.unlock();

    // Don't hold lock_ over wait. This can cause deadlock with child death
    // notifications.
    status = response->wait(TimeoutUSecsStart);
    lock_.lock();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u startMyInstance failed to wait"
                " for response from child: %s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    {
        const ProtoResponseMsg *responseMsg = response->get();

        status.fromStatusCode((StatusCode) responseMsg->status());
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u startMyInstance failed to"
                    " wait for response from child: %s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    strGetFromStatus(status));
            // Fall through
        }

        if (status != StatusOk && !responseMsg->error().empty()) {
            // Pass response to caller to process if it contains error.
            *responseOut = response;
            response = NULL;
        }
    }

CommonExit:

    if (status != StatusOk) {
        if (state_ == State::Running) {
            lock_.unlock();
            abortMyInstance(status);
        } else {
            if (child_ != NULL) {
                AppMgr::get()->deregisterAppInstance(getMyChildId());
                child_->deregisterParent(this);
                child_->abortConnection(status);
                Parent::get()->putChild(child_);
                child_ = NULL;
            }
            lock_.unlock();
        }
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u startMyInstance state %u"
                " failed: %s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                prevState,
                strGetFromStatus(status));
    } else {
        lock_.unlock();
    }

    stopwatch.stop();
    if (status == StatusOk) {
        AppMgr::get()->getStats()->appStartSuccess(stopwatch.getElapsedNSecs() /
                                                   NSecsPerMSec);
    } else {
        AppMgr::get()->getStats()->appStartFail(stopwatch.getElapsedNSecs() /
                                                NSecsPerMSec);
    }

    if (response != NULL) {
        response->refPut();
    }

    return status;
}

Status
AppInstance::dispatchToDstXpu(BufDesc *recvBuf,
                              unsigned recvBufCount,
                              unsigned srcXpuId)
{
    Status status = StatusOk;
    bool lockHeld = false;
    uint64_t bufAsOffset;

    lockHeld = true;
    lock_.lock();

    if (state_ == State::Inited) {
        // Wait until AppInstance state is not State::Inited
        startCondVar_.wait(&lock_);
        assert(state_ != State::Inited);
    }

    if (state_ == State::Running) {
        ProtoChildRequest childRequest;
        XpuReceiveBufferFromSrc recvBufFromSrc;
        try {
            childRequest.set_func(ChildFuncRecvBufferFromSrc);
            recvBufFromSrc.set_srcxpuid(srcXpuId);
            recvBufFromSrc.set_dstxpuid(xpuId_);

            for (unsigned ii = 0; ii < recvBufCount; ii++) {
                XpuReceiveBufferFromSrc_Buffer *recvBuffer =
                    recvBufFromSrc.add_buffers();  // can throw
#ifdef BUFCACHESHADOW
                bufAsOffset = BufferCacheMgr::get()->getOffsetFromAddr(
                    XdbMgr::get()->xdbGetBackingBcAddr(recvBuf[ii].buf));
#else
                bufAsOffset =
                    BufferCacheMgr::get()->getOffsetFromAddr(recvBuf[ii].buf);
#endif
                recvBuffer->set_offset(bufAsOffset);
                recvBuffer->set_length(recvBuf[ii].bufLen);
            }

            childRequest.set_allocated_recvbufferfromsrc(&recvBufFromSrc);
        } catch (std::exception &e) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u dispatch buffer from src xpu %u"
                    " failed:%s, %s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    srcXpuId,
                    strGetFromStatus(status),
                    e.what());
            childRequest.release_recvbufferfromsrc();
            goto CommonExit;
        }

        status =
            child_->sendSync(&childRequest, LocalMsg::timeoutUSecsConfig, NULL);
        childRequest.release_recvbufferfromsrc();
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u dispatch buffer from src xpu %u"
                    " failed:%s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    srcXpuId,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        goto CommonExit;
    } else {
        // This status doesn't matter; the app instance is gone
        status = StatusChildTerminated;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u dispatch buffer from src xpu %u"
                " failed:%s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                srcXpuId,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:

    if (lockHeld == true) {
        lock_.unlock();
    }

    return status;
}

void
AppInstance::abortMyInstance(Status status)
{
    ParentChild *child = NULL;
    AppMgr::get()->getStats()->appInstanceAbort();
    bool postStartCondVar = false;
    State prevState;

    lock_.lock();
    prevState = state_;

    switch (state_) {
    case State::Inited:
        postStartCondVar = true;
        break;

    case State::Destructing:
        // Racing death/done/abort.
        doneCondVar_.wait(&lock_);
        lock_.unlock();
        return;

    case State::Done:
        lock_.unlock();
        return;

    case State::Running:
        if (child_ != NULL) {
            AppMgr::get()->deregisterAppInstance(getMyChildId());
            child_->deregisterParent(this);
            child_->abortConnection(status);
            child = child_;
            child_ = NULL;
        }
        break;

    default:
        assert(false);
        break;
    }

    state_ = State::Destructing;
    if (postStartCondVar == true) {
        startCondVar_.broadcast();
    }
    lock_.unlock();

    xSyslog(ModuleName,
            XlogErr,
            "App %s group %lu XPU Id %u abortMyInstance state %u: %s",
            app_->getName(),
            group_->getMyId(),
            xpuId_,
            prevState,
            strGetFromStatus(status));

    doneMyInstance(status, NULL, NULL);

    if (child != NULL) {
        Parent::get()->putChild(child);
    }
}

//
// Handlers for commands coming in from XPU.
//
void
AppInstance::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                          LocalConnection *connection,
                          const ProtoRequestMsg *request,
                          ProtoResponseMsg *response)
{
    Txn::setTxn(group_->txn_);
    Status status = StatusOk;

    switch (request->parent().func()) {
    case ParentFuncXdbGetLocalRows:  // pass through
    case ParentFuncXdbGetMeta: {
        atomicInc64(&outstandingOps_);
        status = TableMgr::get()->onRequestMsg(reqHandler,
                                               connection,
                                               request,
                                               response);
        if (!atomicDec64(&outstandingOps_)) {
            outstandingOpsCv_.broadcast();
        }
        break;
    }

    case ParentFuncAppGetGroupId: {
        ProtoParentChildResponse *parentResponse =
            new (std::nothrow) ProtoParentChildResponse();
        if (parentResponse == NULL) {
            status = StatusNoMem;
            break;
        }

        char uniqueId[XcalarApiMaxPathLen + 1];
        if ((size_t) snprintf(uniqueId,
                              sizeof(uniqueId),
                              "%lx",
                              group_->getMyId()) >= sizeof(uniqueId)) {
            delete parentResponse;
            status = StatusTrunc;
            break;
        }

        try {
            parentResponse->set_groupid(uniqueId);  // can throw
            response->set_allocated_parentchild(parentResponse);
            response->set_status(StatusOk.code());
        } catch (std::exception &e) {
            delete parentResponse;
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u onRequestMsg %u failed,"
                    " abort child:%s, %s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    (unsigned) request->parent().func(),
                    strGetFromStatus(status),
                    e.what());
        }
        break;
    }

    case ParentFuncAppGetOutputBuffer: {
        ProtoParentChildResponse *parentResponse =
            new (std::nothrow) ProtoParentChildResponse();
        if (parentResponse == NULL) {
            status = StatusNoMem;
            break;
        }

        int64_t bufsRequested =
            request->parent().app().getbuffers().numbuffers();
        uint64_t totalMemory = 0;
        uint64_t freeMemory = 0;
        uint64_t totalSwap = 0;
        uint64_t freeSwap = 0;
        bool lowDsMemory = false;
        status = XcSysHelper::get()->getProcFsHelper()->getMemInfo(&totalMemory,
                                                                   &freeMemory,
                                                                   &totalSwap,
                                                                   &freeSwap);
        if (status != StatusOk) break;

        if ((XcalarConfig::get()->datasetPercentOfMem_ < 100) &&
            (totalMemory -
             (totalMemory * XcalarConfig::get()->datasetPercentOfMem_)) <
                freeMemory) {
            lowDsMemory = true;
        }

        lock_.lock();
        if (state_ == State::Running) {
            void *bufs[bufsRequested];
            // If an error occurs after this allocation the transaction
            // will be aborted and out-of-band bcScanCleanup will free
            // buffer cache tagged with the failed transaction Id.
            //
            // Note that for Dataset loads in interactive mode, we use try
            // to use OsPageable Xdb page slab.
            int64_t actualNumBuffers =
                XdbMgr::get()->bcBatchAlloc(bufs,
                                            bufsRequested,
                                            0,
                                            Txn::currentTxn().id_,
                                            (Txn::currentTxn().mode_ ==
                                             Txn::Mode::NonLRQ)
                                                ? XdbMgr::SlabHint::OsPageable
                                                : XdbMgr::SlabHint::Default,
                                            BcHandle::BcScanCleanoutToFree);
#ifdef BUFCACHESHADOW
            for (int64_t ii = 0; ii < actualNumBuffers; ii++) {
                bufs[ii] = XdbMgr::get()->xdbGetBackingBcAddr(bufs[ii]);
                assert(bufs[ii]);
            }
#endif
            if (actualNumBuffers == 0) {
                lock_.unlock();
                delete parentResponse;
                status = StatusNoXdbPageBcMem;
                break;
            } else if (lowDsMemory) {
                lock_.unlock();
                delete parentResponse;
                status = StatusNoDatasetMemory;
                break;
            } else {
                try {
                    uint64_t shmBaseAddr =
                        (uint64_t) BufCacheMemMgr::get()->getStartAddr();
                    for (int ii = 0; ii < actualNumBuffers; ii++) {
                        // can throw.
                        ParentGetOutputBufferResponse_Buffer *newBuffer =
                            parentResponse->mutable_outputbuffers()->add_bufs();
                        if (newBuffer == NULL) {
                            lock_.unlock();
                            delete parentResponse;
                            status = StatusNoMem;
                            break;
                        }

#ifdef XLR_VALGRIND
                        // XXX: Alternatively pass --trace-children=yes to
                        // valgrind but that gives lots of spew from the Python
                        // interpreter.
                        memset(bufs[ii], 0, XdbMgr::bcSize());
#endif

                        assert((uintptr_t) shmBaseAddr < (uintptr_t) bufs[ii]);
                        newBuffer->set_offset((uint64_t) bufs[ii] -
                                              shmBaseAddr);
                    }
                    response->set_allocated_parentchild(parentResponse);
                    parentResponse = NULL;
                } catch (std::exception &e) {
                    lock_.unlock();
                    delete parentResponse;
                    status = StatusNoMem;
                    xSyslog(ModuleName,
                            XlogErr,
                            "App %s group %lu XPU Id %u onRequestMsg %u"
                            " failed: %s, %s",
                            app_->getName(),
                            group_->getMyId(),
                            xpuId_,
                            (unsigned) request->parent().func(),
                            strGetFromStatus(status),
                            e.what());
                    break;
                }
            }
        } else {
            lock_.unlock();
            delete parentResponse;
            status = StatusChildTerminated;
            break;
        }
        lock_.unlock();
        assert(parentResponse == NULL);
        if (parentResponse != NULL) {
            delete parentResponse;
            parentResponse = NULL;
        }
        break;
    }

    case ParentFuncAppLoadBuffer: {
        const ParentAppLoadBufferRequest &loadRequest =
            request->parent().app().loadbuffers();
        status = loadBuffers(&loadRequest);
        break;
    }

    case ParentFuncXpuSendListToDsts: {
        const XpuSendListToDsts &sendListToDsts =
            request->parent().app().sendlisttodsts();
        status = sendListToDstXpus(reqHandler, &sendListToDsts);
        break;
    }

    case ParentFuncAppReportFileError: {
        const ParentReportFileErrorRequest &fileErrorRequest =
            request->parent().app().fileerror();

        lock_.lock();
        // Take the first error we encounter
        if (loadResult_.errorStringBuf[0] == '\0') {
            strlcpy(loadResult_.errorStringBuf,
                    fileErrorRequest.fileerror().c_str(),
                    sizeof(loadResult_.errorStringBuf));
            strlcpy(loadResult_.errorFileBuf,
                    fileErrorRequest.filename().c_str(),
                    sizeof(loadResult_.errorFileBuf));
        }
        lock_.unlock();
        status = StatusOk;
        break;
    }

    case ParentFuncAppDone: {
        lock_.lock();
        if (state_ == State::Destructing) {
            doneCondVar_.wait(&lock_);
            lock_.unlock();
            status = StatusAlready;
            break;
        } else if (state_ == State::Done) {
            lock_.unlock();
            status = StatusAlready;
            break;
        }

        state_ = State::Destructing;
        lock_.unlock();

        const ParentAppDoneRequest &doneRequest =
            request->parent().app().done();

        doneMyInstance(Status((StatusCode) doneRequest.status()),
                       doneRequest.outstr().c_str(),
                       doneRequest.errstr().c_str());

        Status doneStatus;
        doneStatus.fromStatusCode((StatusCode) doneRequest.status());
        if (doneStatus != StatusOk) {
            Status status =
                AppMgr::get()->appGroupMgr_->abortThisGroup(group_->getMyId(),
                                                            doneStatus,
                                                            NULL);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "App %s group %lu XPU Id %u onDeath failed on"
                        " abortThisGroup: %s",
                        app_->getName(),
                        group_->getMyId(),
                        xpuId_,
                        strGetFromStatus(status));
            }
        }

        status = StatusOk;
        break;
    }

    case ParentFuncAppReportNumFiles:
        lock_.lock();
        if (state_ == State::Running) {
            setTotalWork(request->parent()
                             .app()
                             .reportnumfiles()
                             .totalfilebytes(),
                         request->parent()
                             .app()
                             .reportnumfiles()
                             .downsampled());
        }
        lock_.unlock();
        status = StatusOk;
        break;

    case ParentFuncGetRuntimeHistograms:
        status = exportRuntimeHistograms(response);
        break;

    default:
        DCHECK(false) << "Unimplemented function id="
                      << request->parent().func();
        status = StatusUnimpl;
        break;
    }

    if (!status.ok()) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u onRequestMsg %u failed: %s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                (unsigned) request->parent().func(),
                strGetFromStatus(status));
    }
    response->set_status(status.code());
}

Status
AppInstance::exportRuntimeHistograms(ProtoResponseMsg *response)
{
    DCHECK(response != nullptr);

    try {
        auto presp = std::make_unique<ProtoParentChildResponse>();
        DCHECK(presp != nullptr);

        auto status = StatsCollector::instance().exportFinished(
            presp->mutable_histograms());
        if (!status.ok()) return status;

        response->set_allocated_parentchild(presp.release());
        return StatusOk;
    } catch (std::exception &ex) {
        return StatusNoMem;
    }
}

Status
AppInstance::loadBuffers(const ParentAppLoadBufferRequest *loadRequest)
{
    Status status = StatusOk;
    DsDatasetId datasetId = (DsDatasetId) cookie_;
    int64_t numFiles = loadRequest->numfiles();
    int64_t numFileBytes = loadRequest->numfilebytes();
    int64_t numErrors = loadRequest->numerrors();
    uint8_t *shmBaseAddr;
    XdbMgr *xdbMgr = XdbMgr::get();
    int numValidBufs = 0;
    int scheduledLoaders = 0;
    bool markedCleanoutNotToFree = false;

    shmBaseAddr = (uint8_t *) BufCacheMemMgr::get()->getStartAddr();

    // We are racing with the app instance dying, which could cause the load
    // to cancel and the dataset to be cleaned up. As long as the state is
    // not done, the dataset is valid.
    // We must atomically:
    // 1. Check that the state is valid
    // 2. Increment the outstanding buffers to keep the app instance alive
    // After this, the appInstance/dataset will be valid until all
    // PageLoaders have been scheduled onto the runtime, at which point
    // the appInstance/dataset may become invalid at any point.
    lock_.lock();

    if (state_ != State::Running) {
        lock_.unlock();
        // This status doesn't matter; the app instance is gone
        status = StatusChildTerminated;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u loadBuffers failed:%s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Free all of the unused buffers
    for (int ii = 0; ii < loadRequest->unusedbuffers_size(); ii++) {
        uint8_t *pageData =
            shmBaseAddr + loadRequest->unusedbuffers(ii).offset();
        XdbMgr::get()->bcFree(pageData);
    }

    // Count our valid buffers
    // XXX we should have a check here that `pageData` falls into the xdb page
    // memory range and is otherwise valid
    for (int ii = 0; ii < loadRequest->databuffers_size(); ii++) {
        uint8_t *pageData =
            shmBaseAddr + loadRequest->databuffers(ii).buffer().offset();

        // This xdbPage is no longer transient as we're going to hang it on a
        // dataset / Xdb that's going to outlive this transaction. As a result,
        // we no longer need bcScanCleanout to free this as part of txn cleanout
        xdbMgr->bcMarkCleanoutNotToFree(pageData);

        numValidBufs++;
    }
    markedCleanoutNotToFree = true;

    status = updateProgress(numFileBytes);
    if (status != StatusOk) {
        lock_.unlock();
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u loadBuffers failed:%s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));
        goto CommonExit;
    }
    atomicAdd64(&outstandingBuffers_, numValidBufs);

    lock_.unlock();

    // We now know that we have numValidBufs valid buffers to be loaded;
    // we need to go through these bufs and schedule them to be loaded on
    // the runtime. If this scheduling fails, we need to make sure we set
    // outstandingBuffers_ correctly to avoid getting deadlocked

    for (int ii = 0; ii < loadRequest->databuffers_size(); ii++) {
        auto dataBuffer = loadRequest->databuffers(ii);
        uint8_t *pageData = shmBaseAddr + dataBuffer.buffer().offset();
        assert(pageData != NULL && "we check for valid pages above");

        // Only report the number of files for the first buffer, since
        // we don't actually know which file each buffer came from
        int64_t thisNumFiles = ii == 0 ? numFiles : 0;
        int64_t thisNumErrors = ii == 0 ? numErrors : 0;

        status = AppLoader::addDataPage(this,
                                        datasetId,
                                        thisNumFiles,
                                        thisNumErrors,
                                        dataBuffer.haserrors(),
                                        pageData,
                                        loadRequest->fixedschema());
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u loadBuffers failed:%s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    strGetFromStatus(status));
            goto CommonExit;
        }

        ++scheduledLoaders;
    }

CommonExit:
    assert(numValidBufs >= scheduledLoaders && "cant load more than we have");
    if (scheduledLoaders < numValidBufs) {
        assert(status != StatusOk && "we must have failed to alloc/schedule");
        xSyslog(ModuleName,
                XlogDebug,
                "scheduledLoaders: %d, numValidBufs: %d",
                scheduledLoaders,
                numValidBufs);
        assert(numValidBufs == loadRequest->databuffers_size());
        if (markedCleanoutNotToFree) {
            for (int ii = scheduledLoaders; ii < numValidBufs; ii++) {
                auto dataBuffer = loadRequest->databuffers(ii);
                uint8_t *pageData = shmBaseAddr + dataBuffer.buffer().offset();
                XdbMgr::get()->bcFree(pageData);
            }
        }
        int unloadedBufs = numValidBufs - scheduledLoaders;
        if (atomicSub64(&outstandingBuffers_, unloadedBufs) == 0) {
            sideBufsLoadedSem_.post();
        }
    }

    return status;
}

Status
AppInstance::sendListToDstXpus(LocalMsgRequestHandler *reqHandler,
                               const XpuSendListToDsts *sendListToDsts)
{
    Status status = StatusOk;
    uint32_t srcXpu = sendListToDsts->srcxpuid();
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    Config *config = Config::get();
    NodeId srcXce = config->getMyNodeId();
    bool lockHeld = false;
    BufDesc *sendBuf = NULL;
    uint8_t *shmBaseAddr;
    bool freeBufCache = true;
    XdbMgr *xdbMgr = XdbMgr::get();

    shmBaseAddr = (uint8_t *) BufCacheMemMgr::get()->getStartAddr();

    // Source XPU ID should match our AppInstance ID
    assert(srcXpu == xpuId_);

    lockHeld = true;
    lock_.lock();
    if (state_ != State::Running) {
        // This status doesn't matter; the app instance is gone
        status = StatusChildTerminated;
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u send buffer to dst xpu"
                " failed:%s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));

        // Buf$ cannot be freed anymore, since Buf$ scan cleanout may have
        // already freed it.
        freeBufCache = false;
        goto CommonExit;
    }

    for (int jj = 0; jj < sendListToDsts->unusedlist_size(); jj++) {
        const XpuSendListToDsts_Buffer &unusedBuf =
            sendListToDsts->unusedlist(jj);
        uint8_t *dataBuf = shmBaseAddr + unusedBuf.offset();
        assert(dataBuf != NULL && "unused buffers are not NULL");
        xdbMgr->bcFree(dataBuf);
    }

    for (int jj = 0; jj < sendListToDsts->sendlist_size(); jj++) {
        const XpuSendListToDsts_XpuSendListBufferToDst &sendBufferToDst =
            sendListToDsts->sendlist(jj);
        for (int ii = 0; ii < sendBufferToDst.buffers_size(); ii++) {
            const XpuSendListToDsts_XpuSendListBufferToDst_Buffer &buffer =
                sendBufferToDst.buffers(ii);
            uint8_t *dataBuf = shmBaseAddr + buffer.offset();
            assert(dataBuf != NULL &&
                   "send buffer should never send NULL bufs");
            xdbMgr->bcMarkCleanoutNotToFree(dataBuf);
        }
    }

    lock_.unlock();
    lockHeld = false;

    for (int jj = 0; jj < sendListToDsts->sendlist_size(); jj++) {
        uint32_t dstXpu;

        const XpuSendListToDsts_XpuSendListBufferToDst &sendBufferToDst =
            sendListToDsts->sendlist(jj);
        dstXpu = sendBufferToDst.dstxpuid();
        assert(srcXpu != dstXpu);
        NodeId dstXce = group_->getXceNodeIdFromXpuId(dstXpu);

        sendBuf = (BufDesc *) memAlloc(sizeof(BufDesc) *
                                       sendBufferToDst.buffers_size());
        if (sendBuf == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u send buffer to dst xpu %u"
                    " failed:%s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    dstXpu,
                    strGetFromStatus(status));
            goto CommonExit;
        }
        memZero(sendBuf, sizeof(BufDesc) * sendBufferToDst.buffers_size());

#if 0
        xSyslog(ModuleName, XlogInfo,
                "App %s group %lu XPU Id %u send buffer to dst xpu %u"
                " srcXce %u dstXce %u buffers_size %u",
                app_->getName(), group_->getMyId(), xpuId_, dstXpu,
                srcXce, dstXce, sendBufferToDst.buffers_size());
#endif

        for (int ii = 0; ii < sendBufferToDst.buffers_size(); ii++) {
            const XpuSendListToDsts_XpuSendListBufferToDst_Buffer &buffer =
                sendBufferToDst.buffers(ii);
            uint8_t *dataBuf = shmBaseAddr + buffer.offset();
            assert(dataBuf != NULL &&
                   "send buffer should never send NULL bufs");
            sendBuf[ii].buf = dataBuf;
            sendBuf[ii].bufLen = buffer.length();
        }

        if (srcXce == dstXce) {
            // Destination XCE happens to be the same as source XCE node, so
            // dispatch the request to the source XPU/appInstance's group. This
            // group will compute the dstXpu's appInstanceId and dispatch a
            // local protobuf request to that appInstance's child_.

            status =
                group_->dispatchToDstXpu(sendBuf,
                                         (size_t)
                                             sendBufferToDst.buffers_size(),
                                         srcXpu,
                                         dstXpu);
            assert(sendBuf != NULL);
            // free ASAP given other dest XPUs may be pending in the loop
            memFree(sendBuf);
            sendBuf = NULL;
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "App %s group %lu XPU Id %u send buffer to dst xpu %u"
                        " failed:%s",
                        app_->getName(),
                        group_->getMyId(),
                        xpuId_,
                        dstXpu,
                        strGetFromStatus(status));
                goto CommonExit;
            }
        } else {
            // Grab an extra reference here. This ensures that
            // LocalMsgRequestHandler::done() does not delete the
            // SchedulableFsm. This allows the XPU communication code to handle
            // the ACK to the source node or transaction abort.
            reqHandler->incRef();

            status =
                XpuCommObjectAction::sendBufferToDstXpu(group_->getMyId(),
                                                        srcXpu,
                                                        dstXpu,
                                                        sendBuf,
                                                        (size_t) sendBufferToDst
                                                            .buffers_size(),
                                                        reqHandler);
            assert(sendBuf != NULL);
            // free sendBuf ASAP given other dest XPUs may be pending in the
            // loop
            memFree(sendBuf);
            sendBuf = NULL;
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "App %s group %lu XPU Id %u send buffer to"
                        " dst xpu %u failed:%s",
                        app_->getName(),
                        group_->getMyId(),
                        xpuId_,
                        dstXpu,
                        strGetFromStatus(status));
                reqHandler->run();
                reqHandler->done();
                goto CommonExit;
            }
        }
    }

CommonExit:

    if (lockHeld) {
        lock_.unlock();
    }

    if (freeBufCache == true) {
        for (int jj = 0; jj < sendListToDsts->sendlist_size(); jj++) {
            const XpuSendListToDsts_XpuSendListBufferToDst &sendBufferToDst =
                sendListToDsts->sendlist(jj);
            for (int ii = 0; ii < sendBufferToDst.buffers_size(); ii++) {
                uint8_t *invalidatedBuf =
                    shmBaseAddr + sendBufferToDst.buffers(ii).offset();
                xdbMgr->bcFree(invalidatedBuf);
            }
        }
    }

    if (sendBuf != NULL) {
        memFree(sendBuf);
        sendBuf = NULL;
    }

    if (status != StatusOk) {
        // Clean out the transaction since the XPU send buffer to a destination
        // XPU failed.
        Status status2 =
            appGroupMgr->abortThisGroup(group_->getMyId(), status, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u sendBufferToDstXpu failed on"
                    " abortThisGroup:%s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    strGetFromStatus(status));
        }
    }

    return status;
}

void
AppInstance::doneMyInstance(Status doneStatus,
                            const char *outStr,
                            const char *errStr)
{
    ParentChild *child = NULL;
    char *loadOut = NULL;
    char *loadErr = NULL;
    lock_.lock();
    assert(state_ == State::Destructing);

    // Free child. No longer needed.
    if (child_ != NULL) {
        AppMgr::get()->deregisterAppInstance(getMyChildId());
        child_->deregisterParent(this);
        child = child_;
        child_ = NULL;
    }

    lock_.unlock();

    // Either there were no buffers for this instance, or the buffers
    // are already done. In either case, we don't want to wait if outstanding
    // is 0
    if (atomicDec64(&outstandingBuffers_) != 0) {
        sideBufsLoadedSem_.semWait();
    }

    // Make sure to quiece any outstanding XPU SDK API in XCE before declaring
    // that this App instance has been completely cleaned out.
    while (atomicRead64(&outstandingOps_)) {
        outstandingOpsCv_.wait();
    }

    lock_.lock();
    // If we have been successful so far, we override the app out/err
    // with any info from buffer processing
    // XXX - We shouldn't be looking at cookie_ for this, but we need to
    // because this app is permanently marked a 'load app', but the app
    // is also servicing requests for 'list files' and 'preview'. Those
    // requests don't have a cookie set so we can determine whether or not
    // to override the app output
    if (app_ != NULL && app_->getFlags() & App::FlagImport && cookie_ != 0) {
        Status status;
        if (doneStatus == StatusOk) {
            status = getLoadOutput(&loadOut);
            if (status != StatusOk) {
                xSyslog(ModuleName,
                        XlogErr,
                        "App %s group %lu XPU Id %u doneMyInstance failed"
                        " on getLoadOutput:%s",
                        app_->getName(),
                        group_->getMyId(),
                        xpuId_,
                        strGetFromStatus(status));
                doneStatus = status;
            } else {
                outStr = loadOut;
            }
        }
        bool foundError;
        status = getLoadErr(&loadErr, &foundError);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "App %s group %lu XPU Id %u doneMyInstance failed"
                    " on getLoadErr:%s",
                    app_->getName(),
                    group_->getMyId(),
                    xpuId_,
                    strGetFromStatus(status));
            doneStatus = status;
        } else if (foundError) {
            errStr = loadErr;
        }
        if (doneStatus == StatusOk && loadResult_.status != StatusOk) {
            doneStatus = loadResult_.status;
        }
    }

    lock_.unlock();

    group_->localMyGroupInstanceDone(this, doneStatus, outStr, errStr);

    // doneMyInstance is now complete from the perspective of any viewer,
    // includingthe AppGroup or the ParentChild. These are all free to continue
    // executing.
    lock_.lock();
    state_ = State::Done;
    doneCondVar_.broadcast();
    lock_.unlock();

    // Free child last so that it isn't reused while we're still doing stuff.
    if (child != NULL) {
        if (doneStatus != StatusOk) {
            // Once a Txn fails, it's XPUs should be nuked.
            child->abortConnection(doneStatus);
        }
        Parent::get()->putChild(child);
    }
    if (loadOut) {
        memFree(loadOut);
        loadOut = NULL;
    }
    if (loadErr) {
        memFree(loadErr);
        loadErr = NULL;
    }

    refPut();  // drop original ref
}

void
AppInstance::onDeath()
{
    Txn::setTxn(group_->txn_);

    refGet();

    lock_.lock();

    // Racing death/done.
    if (state_ == State::Destructing) {
        doneCondVar_.wait(&lock_);
        lock_.unlock();
        refPut();
        return;
    } else if (state_ == State::Done) {
        lock_.unlock();
        refPut();
        return;
    }

    lock_.unlock();

    Status status =
        AppMgr::get()->appGroupMgr_->abortThisGroup(group_->getMyId(),
                                                    StatusChildTerminated,
                                                    NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s group %lu XPU Id %u onDeath failed on"
                " abortThisGroup:%s",
                app_->getName(),
                group_->getMyId(),
                xpuId_,
                strGetFromStatus(status));
    }

    refPut();
}

void
AppInstance::bufferLoaded(const BufferResult *bufResult)
{
    lock_.lock();

    if (bufResult->status == StatusOk) {
        loadResult_.numBytes += bufResult->numBytes;
        loadResult_.numFiles += bufResult->numFiles;
        loadResult_.numBuffers++;
    } else {
        // Abandon the load if fatal error
        if (bufResult->status != StatusOk) {
            loadResult_.status = bufResult->status;
            if (loadResult_.errorStringBuf[0] == '\0') {
                strlcpy(loadResult_.errorStringBuf,
                        strGetFromStatus(bufResult->status),
                        sizeof(loadResult_.errorStringBuf));
                loadResult_.errorFileBuf[0] = '\0';
            }
        }
    }
    if (atomicDec64(&outstandingBuffers_) == 0) {
        sideBufsLoadedSem_.post();
    }
    lock_.unlock();
}

Status
AppInstance::updateProgress(int64_t numFileBytes)
{
    Status status = StatusOk;
    if (!(app_->getFlags() & App::FlagImport)) {
        status = StatusInval;
        goto CommonExit;
    }
    assert(state_ == State::Running && "caller holds the lock");
    if (numFileBytes > 0) {
        DsDatasetId datasetId = (DsDatasetId) cookie_;
        assert(datasetId != 0);
        status = AppLoader::updateProgress(datasetId, numFileBytes);
    }
CommonExit:
    return status;
}

void
AppInstance::setTotalWork(int64_t numFileBytes, bool downSampled)
{
    if (!(app_->getFlags() & App::FlagImport)) {
        return;
    }
    DsDatasetId datasetId = (DsDatasetId) cookie_;
    AppLoader::setTotalProgress(datasetId, numFileBytes, downSampled);
}

// Caller owns outStr and errStr
Status
AppInstance::getLoadOutput(char **outStr)
{
    assert(app_->getFlags() && App::FlagImport);
    Status status = StatusOk;
    json_t *outJson = NULL;
    *outStr = NULL;

    outJson = json_pack(
        "{"
        "s:I"  // numBuffers
        "s:I"  // numBytes
        "s:I"  // numFiles
        "}",
        "numBuffers",
        loadResult_.numBuffers,
        "numBytes",
        loadResult_.numBytes,
        "numFiles",
        loadResult_.numFiles);
    BailIfNull(outJson);

    *outStr = json_dumps(outJson, 0);
    BailIfNull(*outStr);

CommonExit:
    if (outJson) {
        json_decref(outJson);
        outJson = NULL;
    }
    if (status != StatusOk) {
        if (*outStr != NULL) {
            memFree(*outStr);
            *outStr = NULL;
        }
    }
    return status;
}

Status
AppInstance::getLoadErr(char **errStr, bool *foundError)
{
    assert(app_->getFlags() && App::FlagImport);
    Status status = StatusOk;
    json_t *errJson = NULL;
    *errStr = NULL;
    *foundError = false;

    if (loadResult_.errorStringBuf[0] == '\0') {
        goto CommonExit;
    }
    errJson = json_pack(
        "{"
        "s:s"  // errorFile
        "s:s"  // errorString
        "}",
        "errorFile",
        loadResult_.errorFileBuf,
        "errorString",
        loadResult_.errorStringBuf);
    BailIfNull(errJson);
    *foundError = true;

    *errStr = json_dumps(errJson, 0);
    BailIfNull(*errStr);

CommonExit:
    if (errJson) {
        json_decref(errJson);
        errJson = NULL;
    }
    if (status != StatusOk) {
        if (*errStr != NULL) {
            memFree(*errStr);
            *errStr = NULL;
        }
        *foundError = false;
    }
    return status;
}

ParentChild::Id
AppInstance::getMyChildId() const
{
    return child_->getId();
}

size_t
AppInstance::getMyXpuId() const
{
    return xpuId_;
}
