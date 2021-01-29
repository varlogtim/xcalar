// Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include <sys/stat.h>
#include <fcntl.h>

#include "Child.h"
#include "sys/XLog.h"
#include "config/Config.h"
#include "xcalar/compute/localtypes/ProtoMsg.pb.h"
#include "operators/Operators.h"
#include "util/Atomics.h"
#include "app/AppHost.h"
#include "runtime/Runtime.h"
#include "strings/String.h"
#include "shmsg/SharedMemory.h"
#include <linux/oom.h>
#include "bc/BufCacheMemMgr.h"

// XXX Pulled from libudfpy to allow dispatching of Python requests. Make
//     once supporting multiple languages.
extern void udfPyChildDispatch(LocalConnection *connection,
                               const ProtoRequestMsg *request,
                               ProtoResponseMsg *response);

Child *Child::instance = NULL;

Status  // static
Child::init(unsigned parentId, uint64_t childId, const char *parentUds)
{
    Status status = StatusOk;

    assert(instance == NULL);
    char *parentUdsCopy = strAllocAndCopy(parentUds);
    if (parentUdsCopy == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on parentUds strAllocAndCopy: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    instance = new (std::nothrow) Child(parentId, childId, parentUdsCopy);
    if (instance == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on Child allocation: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    parentUdsCopy = NULL;

    status = instance->initInternal();

CommonExit:
    if (parentUdsCopy != NULL) {
        memFree(parentUdsCopy);
        parentUdsCopy = NULL;
    }
    return status;
}

Child::Child(unsigned parentIdParam, uint64_t childIdParam, char *parentUds)
    : parentId_(parentIdParam),
      childId_(childIdParam),
      parentUds_(parentUds),
      parentConn_(NULL)
{
    assert(parentUds_ != NULL);
    assert(strlen(parentUds_) > 0);
    atomicWrite32(&ref_, 1);
}

Status
Child::setupShm()
{
    Status status = StatusOk;
    char shmName[SharedMemory::NameLength];
    SharedMemory shm;
    SharedMemory::Type type = BufCacheMemMgr::shmType();

    verify(snprintf(shmName,
                    sizeof(shmName),
                    BufferCacheMgr::FileNamePattern,
                    SharedMemory::XcalarShmPrefix,
                    (unsigned) Config::get()->getMyNodeId()) <
           (int) sizeof(shmName));

    status = shm.attach(shmName, type);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Failed to attach to buffer cache: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    status = StatusOk;
    bufCacheBase_ = shm.getAddr();

CommonExit:
    return status;
}

void
Child::childnodeSetupMemPolicy()
{
    Status oomstatus;
    int oomFd;
    size_t oomWret;
    uint64_t oomAdj;
    char oomAdjStr[10];

    //
    // Now, adjust the XPU child proc's OOM score to bias OOM killer to pick it
    // over XCE nodes (which don't have any adjustment). Higher positive values
    // lead to greater likelihood of being picked.  The range of valid values
    // is documented in the proc(5) man page for /proc/[pid]/oom_score_adj.
    // Negative values reduce likelihood of being picked, but need special
    // privilege (CAP_SYS_RESOURCE). If we want usrnodes to be negatively
    // biased instead of childnodes being positively biased, the usrnode
    // executable will need to be assigned CAP_SYS_RESOURCE capability during
    // Xcalar installation, and appropriate code added to mainUsrnode during
    // boot-up. For now, we're hard-coding XPU OOM policy bias to
    // maximum possible positive bias (OOM_SCORE_ADJ_MAX), and not making
    // it configurable. If this becomes an issue, a new config knob
    // can be added, or better, the hard-coded value changed.
    //

    oomFd = open("/proc/self/oom_score_adj", O_WRONLY);
    if (oomFd < 0) {
        oomstatus = sysErrnoToStatus(errno);
        xSyslog(ModuleName,
                XlogErr,
                "Failed to set OOM bias for XPU! Couldn't open "
                "/proc/self/oom_score_adj (%s)",
                strGetFromStatus(oomstatus));
    } else {
        oomAdj = OOM_SCORE_ADJ_MAX;

        snprintf(oomAdjStr, sizeof(oomAdjStr), "%lu\n", oomAdj);
        oomWret = write(oomFd, oomAdjStr, strlen(oomAdjStr));
        if ((int) oomWret < 0 || oomWret != strlen(oomAdjStr)) {
            oomstatus = sysErrnoToStatus(errno);
            xSyslog(ModuleName,
                    XlogErr,
                    "Failed to set OOM bias for XPU! Couldn't write "
                    "%s to  /proc/self/oom_score_adj (%s)",
                    oomAdjStr,
                    strGetFromStatus(oomstatus));
        } else {
            oomstatus = StatusOk;
        }
        close(oomFd);
    }

    if (oomstatus != StatusOk) {
        xSyslog(ModuleName, XlogErr, "childnodeSetupMemPolicy failed!");
    }
}

Status
Child::initInternal()
{
    Status status;
    ProtoParentRequest parentRequest;
    Stopwatch stopwatch;
    Stopwatch overallStopwatch;

#ifdef DEBUG
    const char *debugBreak = getenv(ParentChild::EnvChildBreak);
    if (debugBreak != NULL && debugBreak[0] == '1') {
        // Welcome debug enthusiasts! Set cont to true to continue (p cont=1).
        volatile bool cont = false;
        while (!cont) {
            sysSleep(1);
        }
    }
#endif  // DEBUG

    parentPid_ = getppid();
    if (parentPid_ == 1) {
        // Implies init process is our parent meaning our original parent
        // terminated.
        status = StatusNoParent;
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on getppid: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    verify(sem_init(&semChildExit_, 0, 0) == 0);

    status = AppHost::init();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on AppHost init: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = Config::get()->populateParentBinary();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on populateParentBinary: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xpuIdRangeInCluster_ = (XpuIdRange *) memAlloc(
        sizeof(XpuIdRange) * Config::get()->getActiveNodes());
    if (xpuIdRangeInCluster_ == NULL) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on xpuIdRangeInCluster set up: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    memZero(xpuIdRangeInCluster_,
            sizeof(XpuIdRange) * Config::get()->getActiveNodes());

    status = setupShm();
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init failed on setupShm: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    assert(bufCacheBase_ != NULL);

    childnodeSetupMemPolicy();

    for (unsigned i = 0; i < (sizeof(Child::XpuCommTag) - 1); i++) {
        xpuRecvQ_[i].xpuRecvqHead = NULL;
        xpuRecvQ_[i].xpuRecvqTail = NULL;
    }

    // Connect to parent. This should be the last step before returning - all
    // child setup / init code should be above this comment! See NOTE below.
    stopwatch.restart();
    status = LocalMsg::get()->connect(parentUds_, this, &parentConn_);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init connectToParent failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    stopwatchReportMsecs(&stopwatch, "LocalMsg connect");

    parentConn_->setHandler(this);

    stopwatch.restart();
    parentRequest.set_func(ParentFuncConnect);
    status = sendSync(&parentRequest, LocalMsg::timeoutUSecsConfig, NULL);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child init sendSync ParentFuncConnect failed: %s",
                strGetFromStatus(status));
        waitForExit();
        goto CommonExit;
    }
    stopwatchReportMsecs(&stopwatch, "ParentFuncConnect");

    // -------------------------- NOTE ---------------------------------------
    //
    // Now that the sendSync() above has finished successfully, the child has
    // communicated to the parent that it's open for business and ready to run
    // UDFs, apps, etc. This should be the last step, and no other code needed
    // to setup the child should be added below this line!
    //
    // -------------------------- NOTE ---------------------------------------

    status = StatusOk;
CommonExit:
    stopwatchReportMsecs(&overallStopwatch, "Child::initInternal");
    return status;
}

void
Child::destroy()
{
    if (parentConn_ != NULL) {
        LocalMsg::get()->drainOutstanding();
        // Ensure Runtime is drained of LocalConnection::OnFatalHandler
        // messages.
        if (Runtime::get() != NULL) {
            Runtime::get()->drainAllRunnables();
        }
        parentConn_->setHandler(NULL);
        LocalMsg::get()->disconnect(parentConn_);
        parentConn_ = NULL;
    }

    if (Runtime::get() != NULL) {
        Runtime::get()->drainAllRunnables();
    }

    if (AppHost::get() != NULL) {
        AppHost::get()->destroy();
    }

    if (xpuIdRangeInCluster_ != NULL) {
        memFree(xpuIdRangeInCluster_);
        xpuIdRangeInCluster_ = NULL;
    }

    refPut();
    delete this;
    instance = NULL;
}

Child::~Child()
{
    if (parentUds_ != NULL) {
        memFree(parentUds_);
        parentUds_ = NULL;
    }
}

/* static */ Child *
Child::get()
{
    return instance;
}

void
// @SymbolCheckIgnore
Child::exit()
{
    // We don't contact parent to signal detach. He'll find out about it
    // eventually (likely via SIGCHLD).
    xSyslog(ModuleName, XlogInfo, "Child exiting.");
    verify(sem_post(&semChildExit_) == 0);
}

void
Child::waitForExit()
{
    verify(sem_wait(&semChildExit_) == 0);
}

unsigned
Child::getParentId() const
{
    return parentId_;
}

// XXX Child::refGet and refPut are no-ops... They're not reliable
void
Child::refGet()
{
    atomicInc32(&ref_);
}

void
Child::refPut()
{
    assert(atomicRead32(&ref_) > 0);
    atomicDec32(&ref_);
}

void *
Child::getBufCacheBaseAddr()
{
    return bufCacheBase_;
}

void *
Child::getBufFromOffset(uint64_t offset)
{
    return (void *) ((uint64_t) bufCacheBase_ + offset);
}

// Delete and return an element from tail of FIFO queue.
XpuRecvQElem *
Child::recvBufferDelQ(XpuRecvFifoQ *recvQ)
{
    XpuRecvQElem *retElem = NULL;
    XpuRecvQElem *tail = recvQ->xpuRecvqTail;
    XpuRecvQElem *head = recvQ->xpuRecvqHead;

    if (tail) {
        retElem = tail;
        if (tail == head) {
            recvQ->xpuRecvqTail = recvQ->xpuRecvqHead = NULL;
        } else {
            tail->xpuRecvQeb->xpuRecvQef = tail->xpuRecvQef;
            tail->xpuRecvQef->xpuRecvQeb = tail->xpuRecvQeb;
            recvQ->xpuRecvqTail = tail->xpuRecvQeb;
        }
        retElem->xpuRecvQef = retElem->xpuRecvQeb = NULL;
    }
    return retElem;
}

// All XPU buffers received from a different XPU are held in a FIFO queue
// locally, in this XPU. This routine returns any such buffer from the queue,
// and blocks if the queue is empty.
uint8_t *
Child::recvBufferFromLocalXpu(Child::XpuCommTag commTag,
                              uint64_t *payloadLength)
{
    XpuRecvFifoQ *recvQ;
    XpuRecvQElem *retElem = NULL;
    uint8_t *payload = NULL;

    assert((unsigned) commTag < (sizeof(Child::XpuCommTag) - 1));

    recvQ = &xpuRecvQ_[(int) commTag];
    recvQ->xpuRecvqLock.lock();
    while (recvQ->xpuRecvqTail == NULL) {
        xSyslog(ModuleName,
                XlogDebug,
                "recvBufferFromLocalXpu wait: xpuId %lu",
                xpuId_);
        recvQ->xpuRecvqCv.wait(&recvQ->xpuRecvqLock);
    }
    retElem = recvBufferDelQ(recvQ);
    recvQ->xpuRecvqLock.unlock();
    assert(retElem != NULL);
    payload = retElem->payload;
    *payloadLength = retElem->length;
    memFree(retElem);
    return payload;
}

// Add an element to the head of the FIFO queue. The elements will be deleted
// and pulled from the queue's tail, to implement FIFO.
void
Child::recvBufferAddQ(XpuRecvFifoQ *recvQ, XpuRecvQElem *recvQelemP)
{
    XpuRecvQElem *head = recvQ->xpuRecvqHead;

    if (head == NULL) {
        recvQ->xpuRecvqHead = recvQelemP;
        recvQ->xpuRecvqTail = recvQelemP;
        recvQelemP->xpuRecvQef = recvQelemP;
        recvQelemP->xpuRecvQeb = recvQelemP;
    } else {
        /* insert at head of FIFO queue */
        recvQelemP->xpuRecvQef = head;
        recvQelemP->xpuRecvQeb = head->xpuRecvQeb;
        head->xpuRecvQeb->xpuRecvQef = recvQelemP;
        head->xpuRecvQeb = recvQelemP;
        recvQ->xpuRecvqHead = recvQelemP;
    }
}

// Add the supplied payload into the recv FIFO queue.
Status
Child::recvBufferProcess(Child::XpuCommTag commTag,
                         uint8_t *payload,
                         uint64_t payloadLength)
{
    XpuRecvFifoQ *recvQ;
    XpuRecvQElem *recvElemP;

    recvElemP = (XpuRecvQElem *) memAlloc(sizeof(XpuRecvQElem));
    if (!recvElemP) {
        xSyslog(ModuleName, XlogErr, "recvBufferProcess: memAlloc failed!\n");
        return StatusNoMem;
    }
    recvElemP->payload = payload;
    recvElemP->length = payloadLength;
    recvElemP->xpuRecvQef = recvElemP->xpuRecvQeb = NULL;

    assert((unsigned) commTag < (sizeof(Child::XpuCommTag) - 1));

    recvQ = &xpuRecvQ_[(int) commTag];

    recvQ->xpuRecvqLock.lock();
    recvBufferAddQ(recvQ, recvElemP);
    recvQ->xpuRecvqCv.signal();
    recvQ->xpuRecvqLock.unlock();
    return StatusOk;
}

//
// The parent sends an array of SHM/BCMM buffers (an array of offset,length
// pairs), down to the child/destXPU as the last leg of a data transmission
// from a source XPU. The offset is the offset within the respective SHM
// segement, and so the virtual address in the child to which its SHM segment
// is mapped, is used to convert the offset to a virtual address in the child.
// This routine re-constructs the entire payload as a single contiguous stream
// of bytes from the SHM buffers, and then adds this payload to a FIFO queue,
// waiting for the app to retrieve it via the xpuRecv() API.
//
Status
Child::recvBufferFromSrcXpu(const XpuReceiveBufferFromSrc *recvBufferFromSrc)
{
    Status status;
    size_t pageSize;
    uint32_t lastBufIx;
    uint8_t *payload;
    uint8_t *payloadBufp;
    uint64_t payloadBytes;
    uint64_t nbufs;
    uint8_t *buf;
    uint64_t bufLen;
    Child::XpuCommTag xpuCommTag;
    bool firstBuf;

    nbufs = recvBufferFromSrc->buffers_size();
    lastBufIx = nbufs - 1;
    const XpuReceiveBufferFromSrc_Buffer &last_buffer =
        recvBufferFromSrc->buffers(lastBufIx);
    pageSize = XdbMgr::bcSize();
    if (nbufs == 0) {
        status = StatusXpuNoBufsToRecv;
        goto CommonExit;
    }
    payloadBytes = (lastBufIx * pageSize) + last_buffer.length();

    // strip out the leading header in the payload: the header carries the tag
    // with which the payload is stamped - so as to route the payload to its
    // respective recv FIFO queue. The following just computes the bytes needed
    // to be malloc'ed. The actual stripping out of the header is done below in
    // the for loop.

    payloadBytes = payloadBytes - sizeof(xpuCommTag);

    payload = (uint8_t *) memAlloc(payloadBytes);
    if (!payload) {
        xSyslog(ModuleName,
                XlogErr,
                "recvBufferFromSrcXpu: src: %u, dst %u, nbufs %lu malloc "
                "failed!",
                recvBufferFromSrc->srcxpuid(),
                recvBufferFromSrc->dstxpuid(),
                nbufs);
        status = StatusNoMem;
        goto CommonExit;
    }
    payloadBufp = payload;
    firstBuf = true;
    for (uint64_t ii = 0; ii < nbufs; ii++) {
        const XpuReceiveBufferFromSrc_Buffer &buffer =
            recvBufferFromSrc->buffers(ii);
        buf = ((uint8_t *) getBufCacheBaseAddr()) + buffer.offset();
        bufLen = buffer.length();
        assert(ii == (nbufs - 1) || (bufLen == pageSize));
        if (firstBuf) {
            // The first buffer carries the communication tag with which the
            // payload is stamped. This is read out, and the buf pointer and
            // length adjusted to strip out the header when doing the memcpy
            // below. The tag is supplied later to recvBufferProcess below,
            // which will route the payload to its respective FIFO queue.
            xpuCommTag = *((Child::XpuCommTag *) buf);
            assert((unsigned) xpuCommTag < (sizeof(Child::XpuCommTag) - 1));
            buf = buf + sizeof(Child::XpuCommTag);
            bufLen = bufLen - sizeof(Child::XpuCommTag);
            firstBuf = false;
        }
        assert(ii < (nbufs - 1) ||
               payloadBufp == (payload + payloadBytes - bufLen));
        assert(payloadBufp <= (payload + payloadBytes - bufLen));
        memcpy(payloadBufp, buf, bufLen);
        payloadBufp += bufLen;
    }
    assert(payloadBufp == (payload + payloadBytes));

    status = recvBufferProcess(xpuCommTag, payload, payloadBytes);
CommonExit:
    return status;
}

//
// Methods for parent <-> child communication.
//
void
Child::onRequestMsg(LocalMsgRequestHandler *reqHandler,
                    LocalConnection *connection,
                    const ProtoRequestMsg *request,
                    ProtoResponseMsg *response)
{
    // set transaction of usrnode
    setTxn(request->child().txn());
    switch (request->child().func()) {
    case ChildFuncShutdown: {
        xSyslog(ModuleName, XlogInfo, "Parent sent shutdown request. Exiting.");
        exitReason_ = StatusOk;
        // For now, on shutdown request, just exit the process.
        xcalarExit(exitReason_.code());

        // @SymbolCheckIgnore
        this->exit();
        response->set_status(StatusOk.code());
        break;
    }

    case ChildFuncUdfEval: {
        Status status =
            Operators::operatorsChildEval(request->child().childeval(),
                                          response);
        response->set_status(status.code());
        break;
    }

    case ChildFuncAppStart: {
        AppHost::get()->dispatchStartRequest(request->child().appstart(),
                                             response);
        // Transaction will be cleared later in PythonApp::done() method,
        // because app runs as async in a separate thread. Clear txn call
        // "setTxn(Txn())" below won't impact the txn in that new thread.
        break;
    }

    case ChildFuncRecvBufferFromSrc: {
        const XpuReceiveBufferFromSrc &recvRequestFromSrc =
            request->child().recvbufferfromsrc();
        Status status = recvBufferFromSrcXpu(&recvRequestFromSrc);
        response->set_status(status.code());
        break;
    }

    case ChildFuncUdfAdd:
    case ChildFuncUdfUpdate:
    case ChildFuncUdfListFunctions: {
        udfPyChildDispatch(connection, request, response);
        break;
    }

    case ChildFuncUdfInit: {
        Child::setUdfContext(request->child().childudfinit());
        response->set_status(StatusOk.code());
        break;
    }

    default:
        response->set_status(StatusUnimpl.code());
        break;
    }
    // clear the transaction of childnode; set to default values
    Txn::setTxn(Txn());
}

void
Child::onConnectionClose(LocalConnection *connection, Status status)
{
    xSyslog(ModuleName,
            XlogInfo,
            "Connection with parent was closed. Exiting: %s",
            strGetFromStatus(status));
    exitReason_ = status;
    xcalarExit(status.code());
}

// Send message to parent and wait for response.
Status
Child::sendSync(ProtoParentRequest *parentRequest,
                uint64_t timeoutUSecs,
                LocalConnection::Response **responseOut)
{
    if (responseOut != NULL) {
        *responseOut = NULL;
    }
    LocalConnection::Response *response;
    Status status = sendAsync(parentRequest, &response);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Child sendAsync parent request failed: %s",
                strGetFromStatus(status));
        return status;
    }

    status = response->wait(timeoutUSecs);
    if (status != StatusOk) {
        response->refPut();
        if (status == StatusTimedOut) {
            xSyslog(ModuleName,
                    XlogErr,
                    "%s timed out on child request %u (timeout %llu usecs)",
                    __PRETTY_FUNCTION__,
                    parentRequest->func(),
                    (unsigned long long) timeoutUSecs);
        }
        return status;
    }

    if (responseOut == NULL) {
        status.fromStatusCode((StatusCode) response->get()->status());
        response->refPut();
    } else {
        *responseOut = response;
    }

    return status;
}

// Send message to parent. Allow caller to deal with response. Caller must dec
// ref on responseOut.
Status
Child::sendAsync(ProtoParentRequest *parentRequest,
                 LocalConnection::Response **responseOut)
{
    ProtoRequestMsg requestMsg;
    requestMsg.set_target(ProtoMsgTargetParent);
    requestMsg.set_allocated_parent(parentRequest);
    requestMsg.set_childid(childId_);
    assert(childId_ != 0);

    Status status = parentConn_->sendRequest(&requestMsg, responseOut);
    requestMsg.release_parent();
    return status;
}

void
Child::setXpuId(uint64_t xpuId)
{
    xpuId_ = xpuId;
}

void
Child::setXpuClusterSize(uint32_t xpuClusterSize)
{
    xpuClusterSize_ = xpuClusterSize;
}

void
Child::setXpuDistributionInCluster(const ChildAppStartRequest &startRequest)
{
    assert(Config::get()->getActiveNodes() ==
           (unsigned) startRequest.xpuidrange_size());
    for (unsigned ii = 0; ii < (unsigned) startRequest.xpuidrange_size();
         ii++) {
        const ChildAppStartRequest_XpuIdRange &xpuIdRange =
            startRequest.xpuidrange(ii);
        xpuIdRangeInCluster_[ii].startXpuId = xpuIdRange.xpuidstart();
        xpuIdRangeInCluster_[ii].endXpuId = xpuIdRange.xpuidend();
    }
}

unsigned
Child::getXpuStartId(NodeId xceNodeId)
{
    return xpuIdRangeInCluster_[xceNodeId].startXpuId;
}

unsigned
Child::getXpuEndId(NodeId xceNodeId)
{
    return xpuIdRangeInCluster_[xceNodeId].endXpuId;
}

void
Child::setTxn(ProtoTxn txn)
{
    Xid id = txn.id();
    Txn::Mode mode = Txn::Mode::Invalid;
    Runtime::SchedId rtSchedId = Runtime::SchedId::MaxSched;
    RuntimeType rtType = (RuntimeType) txn.runtimetype();

    // XXX need to replace existing Enums with proto enums
    // to avoid this conversion, will be handled in SDK-732
    switch (txn.mode()) {
    case ProtoTxn_ModeType_LRQ:
        mode = Txn::Mode::LRQ;
        break;
    case ProtoTxn_ModeType_NonLRQ:
        mode = Txn::Mode::NonLRQ;
        break;
    case ProtoTxn_ModeType_Invalid:
        mode = Txn::Mode::Invalid;
        break;
    default:
        assert(0 && "Invalid Mode while creating protoTxn request");
    }

#ifdef DEBUG
    switch (txn.schedid()) {
    case ProtoTxn_SchedType_Sched0:
        rtSchedId = Runtime::SchedId::Sched0;
        break;
    case ProtoTxn_SchedType_Sched1:
        rtSchedId = Runtime::SchedId::Sched1;
        break;
    case ProtoTxn_SchedType_Sched2:
        rtSchedId = Runtime::SchedId::Sched2;
        break;
    case ProtoTxn_SchedType_Immediate:
        rtSchedId = Runtime::SchedId::Immediate;
        break;
    case ProtoTxn_SchedType_MaxSched:
        rtSchedId = Runtime::SchedId::MaxSched;
        break;
    default:
        assert(0 && "Invalid SchedId while creating protoTxn request");
    }
#endif  // DEBUG

    // setting this to Sched0, as child is anyways
    // single threaded and dont have other scheds as parent
    rtSchedId = Runtime::SchedId::Sched0;
    Txn::setTxn(Txn(id, mode, rtSchedId, rtType));
}

// Install the userIdName in the Child at run time for a new App or UDF
// Install the sessionId also in the Child at run time for a new App or UDF
// This is the full context (user and workbook) within which the App/UDF runs
void
Child::setUserContext(const char *userIdName, uint64_t sessionId)
{
    size_t userIdNameSize = sizeof(userIdName_);

    verify(strlcpy(userIdName_, userIdName, userIdNameSize) < userIdNameSize);
    sessionId_ = sessionId;
}

// Set the run-time context for the App about to run in the child
// This should be called before the child starts running an App
void
Child::setAppContext(const ChildAppStartRequest &startArgs)
{
    isApp_ = true;
    setAppName(startArgs.appname().c_str());
    setXpuId(startArgs.xpuid());
    setXpuClusterSize(startArgs.xpuclustersize());
    setXpuDistributionInCluster(startArgs);
    setUserContext(startArgs.username().c_str(), startArgs.sessionid());

    static_assert((int) XpuCommTag::CtagBarr == 0, "we can iterate here");
    static_assert((int) XpuCommTag::CtagUser == 1, "we can iterate here");
}

void
Child::unsetAppContext()
{
    isApp_ = false;
    setXpuId(-1);
    setXpuClusterSize(-1);
    setUserContext("", 0);

    static_assert((int) XpuCommTag::CtagBarr == 0, "we can iterate here");
    static_assert((int) XpuCommTag::CtagUser == 1, "we can iterate here");

    for (int ii = 0; ii < (int) XpuCommTag::CtagUser + 1; ii++) {
        XpuRecvFifoQ *queue = &xpuRecvQ_[ii];
        XpuRecvQElem *elem;
        queue->xpuRecvqLock.lock();
        do {
            elem = recvBufferDelQ(queue);
        } while (elem);
        assert(xpuRecvQ_[ii].xpuRecvqHead == NULL);
        queue->xpuRecvqLock.unlock();
    }
}

// Set the run-time context for the UDF about to execute in the child
// This should be called before the child starts running the UDF
void
Child::setUdfContext(const ChildUdfInitRequest &udfInitReq)
{
    isApp_ = false;
    setUserContext(udfInitReq.username().c_str(), udfInitReq.sessionid());
}

RuntimeHistogramsResponse
Child::getParentPerfInfo()
{
    ProtoParentRequest req;
    req.set_func(ParentFuncGetRuntimeHistograms);

    LocalConnection::Response *resp = nullptr;
    auto status =
        Child::get()->sendSync(&req, LocalMsg::timeoutUSecsConfig, &resp);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "App %s failed to fetch perf info from parent: %s",
                appName_,
                strGetFromStatus(status));
        return {};
    }

    DCHECK(resp->get()->payload_case() == ProtoResponseMsg::kParentChild);
    xSyslog(ModuleName,
            XlogInfo,
            "(%s) Fetched %d perf histograms from parent",
            appName_,
            resp->get()->parentchild().histograms().histograms().size());

    return resp->get()->parentchild().histograms();
}
