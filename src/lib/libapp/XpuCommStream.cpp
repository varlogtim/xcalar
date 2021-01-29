// Copyright 2017-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include "XpuCommStream.h"
#include "AppGroupMgr.h"
#include "app/AppMgr.h"
#include "sys/XLog.h"
#include "msg/Xid.h"
#include "runtime/Runtime.h"
#include "app/AppGroup.h"
#include "util/MemTrack.h"
#include "xdb/Xdb.h"

XpuCommObjectAction::XpuCommObjectAction() {}

XpuCommObjectAction::~XpuCommObjectAction() {}

MsgStreamMgr::StreamObject
XpuCommObjectAction::getStreamObject() const
{
    return MsgStreamMgr::StreamObject::XpuCommunication;
}

Status
XpuCommObjectAction::startStreamLocalHandler(Xid streamId,
                                             void **retObjectContext)
{
    return StatusOk;
}

Status
XpuCommObjectAction::endStreamLocalHandler(Xid streamId, void *objectContext)
{
    return StatusOk;
}

Status
XpuCommObjectAction::streamReceiveLocalHandler(
    Xid streamId, void *objectContext, MsgStreamMgr::ProtocolDataUnit *payload)
{
    Status status = StatusOk;
    XpuCommBuffer *cbuf = (XpuCommBuffer *) payload->buffer_;

    if (cbuf->ack == true) {
        status =
            srcXpuStreamReceiveLocalHandler(streamId, objectContext, payload);
    } else {
        status =
            dstXpuStreamReceiveLocalHandler(streamId, objectContext, payload);
    }
    return status;
}

void
XpuCommObjectAction::streamSendCompletionHandler(Xid streamId,
                                                 void *objectContext,
                                                 void *sendContext,
                                                 Status reason)
{
    MsgStreamMgr::ProtocolDataUnit *curBuffer =
        (MsgStreamMgr::ProtocolDataUnit *) sendContext;
    XpuCommBuffer *cbuf =
        (XpuCommBuffer *) ((uintptr_t) curBuffer +
                           sizeof(MsgStreamMgr::ProtocolDataUnit));

    if (cbuf->ack == true) {
        dstXpuStreamSendCompletionHandler(streamId,
                                          objectContext,
                                          sendContext,
                                          reason);
    } else {
        srcXpuStreamSendCompletionHandler(streamId,
                                          objectContext,
                                          sendContext,
                                          reason);
    }
}

Status
XpuCommObjectAction::sendAckToSrcXpu(
    AppGroup *appGroup, AppGroup::DstXpuCommStream *dstXpuCommStream)
{
    // Lock already owned by the caller.
    assert(dstXpuCommStream->lock_.tryLock() == false);
    assert(dstXpuCommStream->numStreamBufsReceived_ ==
           dstXpuCommStream->totalBufs_);
    assert(dstXpuCommStream->streamBufsRecvTotalSize_ ==
           dstXpuCommStream->streamBufsTotalSize_);
    assert(dstXpuCommStream->ackSent_ == false);
    dstXpuCommStream->ackSent_ = true;
    assert(dstXpuCommStream->ackCompletion_ == false);

    AppMgr *appMgr = AppMgr::get();
    MsgStreamMgr::ProtocolDataUnit *curBuffer = NULL;
    size_t curPayloadLen =
        sizeof(MsgStreamMgr::ProtocolDataUnit) + sizeof(XpuCommBuffer);
    Status status = StatusOk;
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    NodeId xceSrcId =
        appGroup->getXceNodeIdFromXpuId(dstXpuCommStream->xpuSrcId_);
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();
    XpuCommBuffer *cbuf = NULL;

    // XCE source ID and destination ID should be different
    assert(xceSrcId != myNodeId);

    Xid streamId = appGroupMgr->msgStreamIds_[xceSrcId];
    assert(streamId != XidInvalid);

    // XXX May be should use XDB pages. But not used in fast path for now.
    curBuffer = (MsgStreamMgr::ProtocolDataUnit *) memAlloc(curPayloadLen);
    if (curBuffer == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream ACK for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u commId %lu failed:%s",
                appGroup->getMyId(),
                dstXpuCommStream->xpuSrcId_,
                dstXpuCommStream->xpuDstId_,
                dstXpuCommStream->commId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status =
        msgStreamMgr->initPayloadToSend(streamId, curBuffer, curPayloadLen);
    assert(status == StatusOk);  // By contract.
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream ACK for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u commId %lu failed:%s",
                appGroup->getMyId(),
                dstXpuCommStream->xpuSrcId_,
                dstXpuCommStream->xpuDstId_,
                dstXpuCommStream->commId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Set up the payload to stream.
    cbuf = (XpuCommBuffer *) ((uintptr_t) curBuffer +
                              sizeof(MsgStreamMgr::ProtocolDataUnit));
    new (cbuf) XpuCommBuffer(appGroup->getMyId(),
                             dstXpuCommStream->commId_,
                             XpuCommBuffer::SeqNumInvalid,
                             XpuCommBuffer::TotalBufsInvalid,
                             XpuCommBuffer::TotalBufsSizeInvalid,
                             XpuCommBuffer::OffsetInvalid,
                             dstXpuCommStream->xpuSrcId_,
                             dstXpuCommStream->xpuDstId_,
                             XpuCommBuffer::IsAck);

    status =
        msgStreamMgr->sendStream(streamId,
                                 curBuffer,
                                 MsgStreamMgr::StreamState::StreamInProgress,
                                 (void *) curBuffer);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream ACK for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u commId %lu failed:%s",
                appGroup->getMyId(),
                dstXpuCommStream->xpuSrcId_,
                dstXpuCommStream->xpuDstId_,
                dstXpuCommStream->commId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (curBuffer != NULL) {
            memFree(curBuffer);
            curBuffer = NULL;
        }
    }

    return status;
}

Status
XpuCommObjectAction::sendBufferToDstXpu(AppGroup::Id appGroupId,
                                        unsigned xpuSrcId,
                                        unsigned xpuDstId,
                                        AppInstance::BufDesc *sendBuf,
                                        size_t sendBufCount,
                                        LocalMsgRequestHandler *reqHandler)
{
    Status status = StatusOk;
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    MsgStreamMgr *msgStreamMgr = MsgStreamMgr::get();
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();

    size_t bufLength = 0;
    for (unsigned ii = 0; ii < sendBufCount; ii++) {
        bufLength += sendBuf[ii].bufLen;
    }

    size_t bufRemaining = bufLength;
    size_t maxBufSize = MsgMgr::getMsgMaxPayloadSize() -
                        sizeof(MsgStreamMgr::ProtocolDataUnit) -
                        sizeof(XpuCommBuffer);
    size_t seqNum = 0;
    MsgStreamMgr::ProtocolDataUnit *curBuffer = NULL;
    XpuCommBuffer *cbuf = NULL;
    Xid commId = XidMgr::get()->xidGetNext();
    AppGroup::XpuCommStreamBuf *streamBuf = NULL;
    unsigned curSendBufOffset = 0;
    unsigned curSendBufIdx = 0;
    AppInstance::BufDesc *curSendBuf = &sendBuf[curSendBufIdx];

    AppGroup *appGroup = appGroupMgr->getThisGroup(appGroupId);
    if (appGroup == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u bufLength %lu commId %lu failed:%s",
                appGroupId,
                xpuSrcId,
                xpuDstId,
                bufLength,
                commId,
                strGetFromStatus(status));
        return StatusNoEnt;
    }

    assert(appGroup->isXpuIdValid(xpuSrcId));
    assert(appGroup->isXpuIdValid(xpuDstId));
    assert(xpuSrcId != xpuDstId);

    assert(appGroup->getXceNodeIdFromXpuId(xpuSrcId) == myNodeId);
    NodeId xceDstId = appGroup->getXceNodeIdFromXpuId(xpuDstId);

    // XCE source ID and destination ID should be different
    assert(xceDstId != myNodeId);

    Xid streamId = appGroupMgr->msgStreamIds_[xceDstId];
    assert(streamId != XidInvalid);

    AppGroup::SrcXpuCommStream *srcXpuCommStream =
        new (std::nothrow) AppGroup::SrcXpuCommStream();
    if (srcXpuCommStream == NULL) {
        status = StatusNoMem;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u bufLength %lu commId %lu failed:%s",
                appGroupId,
                xpuSrcId,
                xpuDstId,
                bufLength,
                commId,
                strGetFromStatus(status));
        goto CommonExit;
    }

    srcXpuCommStream->commId_ = commId;
    srcXpuCommStream->xpuSrcId_ = xpuSrcId;
    srcXpuCommStream->xpuDstId_ = xpuDstId;
    srcXpuCommStream->totalBufs_ =
        (bufLength / maxBufSize) + ((bufLength % maxBufSize) ? 1 : 0);
    srcXpuCommStream->streamBufsTotalSize_ = bufLength;

    appGroup->xpuCommStreamHtLock_.lock();
    status = appGroup->xpuCommStreamHt_.insert(srcXpuCommStream);
    if (status != StatusOk) {
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                " xpuDstId %u bufLength %lu commId %lu failed:%s",
                appGroupId,
                xpuSrcId,
                xpuDstId,
                bufLength,
                commId,
                strGetFromStatus(status));

        appGroup->xpuCommStreamHtLock_.unlock();
        goto CommonExit;
    }
    appGroup->xpuCommStreamHtLock_.unlock();

    srcXpuCommStream->lock_.lock();

    while (bufRemaining != 0) {
        size_t curBufSize = 0, curPayloadLen = 0;

        if (bufRemaining >= maxBufSize) {
            curBufSize = maxBufSize;
        } else {
            curBufSize = bufRemaining;
        }
        curPayloadLen = curBufSize + sizeof(MsgStreamMgr::ProtocolDataUnit) +
                        sizeof(XpuCommBuffer);

        // XXX May be should use XDB pages. But not used in fast path for now.
        curBuffer = (MsgStreamMgr::ProtocolDataUnit *) memAlloc(curPayloadLen);
        if (curBuffer == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                    " xpuDstId %u bufLength %lu commId %lu seq %lu"
                    " curBufSize %lu failed:%s",
                    appGroupId,
                    xpuSrcId,
                    xpuDstId,
                    bufLength,
                    commId,
                    seqNum,
                    curBufSize,
                    strGetFromStatus(status));

            srcXpuCommStream->lock_.unlock();
            srcXpuCommStream = NULL;
            goto CommonExit;
        }

        status =
            msgStreamMgr->initPayloadToSend(streamId, curBuffer, curPayloadLen);
        assert(status == StatusOk);  // By contract.
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                    " xpuDstId %u bufLength %lu commId %lu seq %lu"
                    " curBufSize %lu failed:%s",
                    appGroupId,
                    xpuSrcId,
                    xpuDstId,
                    bufLength,
                    commId,
                    seqNum,
                    curBufSize,
                    strGetFromStatus(status));

            srcXpuCommStream->lock_.unlock();
            srcXpuCommStream = NULL;
            goto CommonExit;
        }

        // Set up the payload to stream.
        cbuf = (XpuCommBuffer *) ((uintptr_t) curBuffer +
                                  sizeof(MsgStreamMgr::ProtocolDataUnit));

        new (cbuf) XpuCommBuffer(appGroupId,
                                 commId,
                                 seqNum,
                                 srcXpuCommStream->totalBufs_,
                                 bufLength,
                                 bufLength - bufRemaining,
                                 xpuSrcId,
                                 xpuDstId,
                                 XpuCommBuffer::IsNotAck);

        for (unsigned bytesCopied = 0; bytesCopied < curBufSize;) {
            unsigned remainInBuf = (curBufSize - bytesCopied);
            unsigned remainInCurSendBuf =
                (curSendBuf->bufLen - curSendBufOffset);
            unsigned bufLengthToCopy = 0;

            if (remainInBuf <= remainInCurSendBuf) {
                bufLengthToCopy = remainInBuf;
                memcpy((void *) ((uintptr_t) cbuf->data + bytesCopied),
                       (void *) ((uintptr_t) curSendBuf->buf +
                                 curSendBufOffset),
                       bufLengthToCopy);
                curSendBufOffset += bufLengthToCopy;

                if (remainInBuf == remainInCurSendBuf) {
                    curSendBuf++;
                    curSendBufOffset = 0;
                }
            } else if (remainInBuf > remainInCurSendBuf) {
                bufLengthToCopy = remainInCurSendBuf;
                memcpy((void *) ((uintptr_t) cbuf->data + bytesCopied),
                       (void *) ((uintptr_t) curSendBuf->buf +
                                 curSendBufOffset),
                       bufLengthToCopy);
                curSendBuf++;
                curSendBufOffset = 0;
            }
            bytesCopied += bufLengthToCopy;
        }

        streamBuf = new (std::nothrow) AppGroup::XpuCommStreamBuf();
        streamBuf->payload = curBuffer;
        status = srcXpuCommStream->srcXpuCommStreamBufHt_.insert(streamBuf);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                    " xpuDstId %u bufLength %lu commId %lu seq %lu"
                    " curBufSize %lu failed:%s",
                    appGroupId,
                    xpuSrcId,
                    xpuDstId,
                    bufLength,
                    commId,
                    seqNum,
                    curBufSize,
                    strGetFromStatus(status));

            srcXpuCommStream->lock_.unlock();
            srcXpuCommStream = NULL;
            goto CommonExit;
        }
        streamBuf = NULL;

        status = msgStreamMgr
                     ->sendStream(streamId,
                                  curBuffer,
                                  MsgStreamMgr::StreamState::StreamInProgress,
                                  (void *) curBuffer);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream send for AppGroup %lu xpuSrcId %u"
                    " xpuDstId %u bufLength %lu commId %lu seq %lu"
                    " curBufSize %lu failed:%s",
                    appGroupId,
                    xpuSrcId,
                    xpuDstId,
                    bufLength,
                    commId,
                    seqNum,
                    curBufSize,
                    strGetFromStatus(status));

            srcXpuCommStream->lock_.unlock();
            srcXpuCommStream = NULL;
            goto CommonExit;
        }

        curBuffer = NULL;
        srcXpuCommStream->numStreamBufsSent_++;
        srcXpuCommStream->streamBufsSentTotalSize_ += curBufSize;
        bufRemaining -= curBufSize;
        seqNum++;
    }

    assert(srcXpuCommStream->numStreamBufsSent_ ==
           srcXpuCommStream->totalBufs_);
    assert(srcXpuCommStream->streamBufsSentTotalSize_ ==
           srcXpuCommStream->streamBufsTotalSize_);

    srcXpuCommStream->reqHandler_ = reqHandler;
    srcXpuCommStream->lock_.unlock();
    srcXpuCommStream = NULL;

CommonExit:
    if (srcXpuCommStream != NULL) {
        delete srcXpuCommStream;
    }

    if (curBuffer != NULL) {
        memFree(curBuffer);
    }

    if (streamBuf == NULL) {
        delete streamBuf;
    }

    if (appGroup != NULL) {
        appGroup->decRef();
    }

    // Caller will end up aborting the transaction and cleaning out if the
    // status is not success.
    return status;
}

void
XpuCommObjectAction::srcXpuStreamSendCompletionHandler(Xid streamId,
                                                       void *objectContext,
                                                       void *sendContext,
                                                       Status reason)
{
    MsgStreamMgr::ProtocolDataUnit *curBuffer =
        (MsgStreamMgr::ProtocolDataUnit *) sendContext;
    XpuCommBuffer *cbuf =
        (XpuCommBuffer *) ((uintptr_t) curBuffer +
                           sizeof(MsgStreamMgr::ProtocolDataUnit));
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    AppGroup::SrcXpuCommStream *srcXpuCommStream = NULL;
    Status status = StatusOk;
    AppGroup::XpuCommStreamBuf *streamBuf = NULL;
    bool readyToRemove = false;

    assert(cbuf->ack == false);

    AppGroup *appGroup = appGroupMgr->getThisGroup(cbuf->appGroupId);
    if (appGroup == NULL) {
        // AppGroup has been cleaned out, so nothing to do here.
        goto CommonExit;
    }

    if (reason != StatusOk) {
        // So there was a stream failure.
        status = reason;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream send completion for AppGroup %lu"
                " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));
        goto CommonExit;
    }

    appGroup->xpuCommStreamHtLock_.lock();

    srcXpuCommStream = dynamic_cast<AppGroup::SrcXpuCommStream *>(
        appGroup->xpuCommStreamHt_.find(cbuf->commId));
    if (srcXpuCommStream == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream send completion for AppGroup %lu"
                " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));

        appGroup->xpuCommStreamHtLock_.unlock();
        goto CommonExit;
    }

    srcXpuCommStream->lock_.lock();

    assert(srcXpuCommStream->numStreamBufsSent_ <=
           srcXpuCommStream->totalBufs_);
    assert(srcXpuCommStream->numStreamBufsCompletions_ <
           srcXpuCommStream->numStreamBufsSent_);

    srcXpuCommStream->numStreamBufsCompletions_++;

    streamBuf = srcXpuCommStream->srcXpuCommStreamBufHt_.remove(cbuf->seqNum);
    assert(streamBuf != NULL);
    delete streamBuf;
    streamBuf = NULL;

    if (srcXpuCommStream->numStreamBufsSent_ ==
            srcXpuCommStream->numStreamBufsCompletions_ &&
        srcXpuCommStream->ackReceived_ == true) {
        readyToRemove = true;
        srcXpuCommStream->srcXpuCommStreamBufHt_.removeAll(
            &AppGroup::XpuCommStreamBuf::doDelete);
    }

    srcXpuCommStream->lock_.unlock();

    if (readyToRemove) {
        srcXpuCommStream = dynamic_cast<AppGroup::SrcXpuCommStream *>(
            appGroup->xpuCommStreamHt_.remove(srcXpuCommStream->getMyId()));
        delete srcXpuCommStream;
        srcXpuCommStream = NULL;
    }

    appGroup->xpuCommStreamHtLock_.unlock();

CommonExit:

    // Clean out model here is that upon error, the transaction will be aborted
    // and the state in the AppGroup will be cleaned out before AppGroup itself
    // is destructed.
    if (status != StatusOk) {
        // Abort the transaction.
        Status status2 =
            appGroupMgr->abortThisGroup(cbuf->appGroupId, status, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream abort for AppGroup %lu"
                    " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));
        }
    }

    if (appGroup != NULL) {
        appGroup->decRef();
    }

    memFree(sendContext);
}

void
XpuCommObjectAction::dstXpuStreamSendCompletionHandler(Xid streamId,
                                                       void *objectContext,
                                                       void *sendContext,
                                                       Status reason)
{
    MsgStreamMgr::ProtocolDataUnit *curBuffer =
        (MsgStreamMgr::ProtocolDataUnit *) sendContext;
    XpuCommBuffer *cbuf =
        (XpuCommBuffer *) ((uintptr_t) curBuffer +
                           sizeof(MsgStreamMgr::ProtocolDataUnit));
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    AppGroup::DstXpuCommStream *dstXpuCommStream = NULL;
    Status status = StatusOk;

    assert(cbuf->ack == true);

    AppGroup *appGroup = appGroupMgr->getThisGroup(cbuf->appGroupId);
    if (appGroup == NULL) {
        // AppGroup has been cleaned out, so nothing to do here.
        goto CommonExit;
    }

    if (reason != StatusOk) {
        // So there was a stream failure.
        status = reason;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream ACK completion for AppGroup %lu"
                " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));
        goto CommonExit;
    }

    appGroup->xpuCommStreamHtLock_.lock();

    dstXpuCommStream = dynamic_cast<AppGroup::DstXpuCommStream *>(
        appGroup->xpuCommStreamHt_.find(cbuf->commId));
    if (dstXpuCommStream == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream ACK completion for AppGroup %lu"
                " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));
        appGroup->xpuCommStreamHtLock_.unlock();
        goto CommonExit;
    }

    dstXpuCommStream->lock_.lock();

    assert(dstXpuCommStream->numStreamBufsReceived_ ==
           dstXpuCommStream->totalBufs_);
    assert(dstXpuCommStream->ackSent_ == true);
    dstXpuCommStream->ackCompletion_ = true;

    dstXpuCommStream->lock_.unlock();

    {
        AppGroup::DstXpuCommStream *tmp =
            dynamic_cast<AppGroup::DstXpuCommStream *>(
                appGroup->xpuCommStreamHt_.remove(dstXpuCommStream->getMyId()));
        assert(tmp == dstXpuCommStream);
        delete dstXpuCommStream;
        dstXpuCommStream = NULL;
    }

    appGroup->xpuCommStreamHtLock_.unlock();
CommonExit:

    // Clean out model here is that upon error, the transaction will be aborted
    // and the state in the AppGroup will be cleaned out before AppGroup itself
    // is destructed.
    if (status != StatusOk) {
        //
        // Abort the transaction.
        //
        Status status2 =
            appGroupMgr->abortThisGroup(cbuf->appGroupId, status, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream abort for AppGroup %lu"
                    " commId %lu seq %lu srcXpu %u dstXpu %u failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));
        }
    }

    if (appGroup != NULL) {
        appGroup->decRef();
    }

    memFree(sendContext);
}

Status
XpuCommObjectAction::srcXpuStreamReceiveLocalHandler(
    Xid streamId, void *objectContext, MsgStreamMgr::ProtocolDataUnit *payload)
{
    Status status = StatusOk;
    XpuCommBuffer *cbuf = (XpuCommBuffer *) payload->buffer_;
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    AppGroup::SrcXpuCommStream *srcXpuCommStream = NULL;
    LocalMsgRequestHandler *reqHandler = NULL;
    bool readyToRemove = false;

    // srcXpu is expected to receive the ACK and not the other way around.
    assert(cbuf->ack == true);

    AppGroup *appGroup = appGroupMgr->getThisGroup(cbuf->appGroupId);
    if (appGroup == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream received for AppGroup %lu commId %lu"
                " seq %lu offset %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->offset,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));
        goto CommonExit;
    }

    appGroup->xpuCommStreamHtLock_.lock();

    srcXpuCommStream = dynamic_cast<AppGroup::SrcXpuCommStream *>(
        appGroup->xpuCommStreamHt_.find(cbuf->commId));
    if (srcXpuCommStream == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream received for AppGroup %lu commId %lu"
                " seq %lu offset %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->offset,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));

        appGroup->xpuCommStreamHtLock_.unlock();
        goto CommonExit;
    }

    srcXpuCommStream->lock_.lock();

    assert(srcXpuCommStream->numStreamBufsSent_ ==
           srcXpuCommStream->totalBufs_);
    assert(srcXpuCommStream->streamBufsSentTotalSize_ ==
           srcXpuCommStream->streamBufsTotalSize_);
    assert(srcXpuCommStream->numStreamBufsCompletions_ <=
           srcXpuCommStream->numStreamBufsSent_);

    srcXpuCommStream->ackReceived_ = true;
    reqHandler = srcXpuCommStream->reqHandler_;
    srcXpuCommStream->reqHandler_ = NULL;

    if (srcXpuCommStream->numStreamBufsSent_ ==
        srcXpuCommStream->numStreamBufsCompletions_) {
        readyToRemove = true;
        srcXpuCommStream->srcXpuCommStreamBufHt_.removeAll(
            &AppGroup::XpuCommStreamBuf::doDelete);
    }

    srcXpuCommStream->lock_.unlock();

    if (readyToRemove) {
        srcXpuCommStream = dynamic_cast<AppGroup::SrcXpuCommStream *>(
            appGroup->xpuCommStreamHt_.remove(srcXpuCommStream->getMyId()));
        delete srcXpuCommStream;
        srcXpuCommStream = NULL;
    }

    appGroup->xpuCommStreamHtLock_.unlock();

    // Inform the srcXpu localMsg that the XPU<->XPU communication request
    // succeeded.
    {
        Status status2 =
            Runtime::get()->schedule(static_cast<Schedulable *>(reqHandler));
        if (status2 != StatusOk) {
            reqHandler->run();
            reqHandler->done();
        }
    }

CommonExit:
    // Clean out model here is that upon error, the transaction will be aborted
    // and the state in the AppGroup will be cleaned out before AppGroup itself
    // is destructed.
    if (status != StatusOk) {
        //
        // Abort the transaction.
        //
        Status status2 =
            appGroupMgr->abortThisGroup(cbuf->appGroupId, status, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream abort for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));
        }
    }

    if (appGroup != NULL) {
        appGroup->decRef();
        appGroup = NULL;
    }

    return status;
}

Status
XpuCommObjectAction::dstXpuStreamReceiveLocalHandler(
    Xid streamId, void *objectContext, MsgStreamMgr::ProtocolDataUnit *payload)
{
    Status status = StatusOk;
    XpuCommBuffer *cbuf = (XpuCommBuffer *) payload->buffer_;
    AppMgr *appMgr = AppMgr::get();
    AppGroupMgr *appGroupMgr = appMgr->appGroupMgr_;
    AppGroup::DstXpuCommStream *dstXpuCommStream = NULL;
    size_t pageSize = XdbMgr::bcSize();
    size_t bytesCopied = 0;
    size_t curRecvBufOffset = cbuf->offset % pageSize;
    size_t bufSizeToCopy = 0;

    // dstXpu is expected to send the ACK and not the other way around.
    assert(cbuf->ack == false);

    AppGroup *appGroup = appGroupMgr->getThisGroup(cbuf->appGroupId);
    if (appGroup == NULL) {
        status = StatusNoEnt;
        xSyslog(ModuleName,
                XlogErr,
                "Xpu Communication stream received for AppGroup %lu commId %lu"
                " seq %lu offset %lu srcXpu %u dstXpu %u failed:%s",
                cbuf->appGroupId,
                cbuf->commId,
                cbuf->seqNum,
                cbuf->offset,
                cbuf->srcXpu,
                cbuf->dstXpu,
                strGetFromStatus(status));
        goto CommonExit;
    }

    appGroup->xpuCommStreamHtLock_.lock();

    dstXpuCommStream = dynamic_cast<AppGroup::DstXpuCommStream *>(
        appGroup->xpuCommStreamHt_.find(cbuf->commId));
    if (dstXpuCommStream == NULL) {
        size_t remainder = cbuf->totalBufsSize % pageSize;
        dstXpuCommStream = new (std::nothrow) AppGroup::DstXpuCommStream();
        if (dstXpuCommStream == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream received for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));

            appGroup->xpuCommStreamHtLock_.unlock();
            goto CommonExit;
        }

        dstXpuCommStream->commId_ = cbuf->commId;
        dstXpuCommStream->xpuSrcId_ = cbuf->srcXpu;
        dstXpuCommStream->xpuDstId_ = cbuf->dstXpu;
        dstXpuCommStream->totalBufs_ = cbuf->totalBufs;
        dstXpuCommStream->streamBufsTotalSize_ = cbuf->totalBufsSize;
        dstXpuCommStream->numRecvBufs_ =
            (cbuf->totalBufsSize / pageSize) + (remainder ? 1 : 0);

        // XXX May be should use Buf$. But not used in fast path for now.
        dstXpuCommStream->recvBuf_ = (AppInstance::BufDesc *) memAlloc(
            dstXpuCommStream->numRecvBufs_ * sizeof(AppInstance::BufDesc));
        if (dstXpuCommStream->recvBuf_ == NULL) {
            status = StatusNoMem;
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream received for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));

            appGroup->xpuCommStreamHtLock_.unlock();
            goto CommonExit;
        }

        for (size_t ii = 0; ii < dstXpuCommStream->numRecvBufs_; ii++) {
            dstXpuCommStream->recvBuf_[ii].buf = NULL;
            if (ii + 1 == dstXpuCommStream->numRecvBufs_ && remainder) {
                dstXpuCommStream->recvBuf_[ii].bufLen = remainder;
            } else {
                dstXpuCommStream->recvBuf_[ii].bufLen = pageSize;
            }
        }

        status = appGroup->xpuCommStreamHt_.insert(
            dynamic_cast<AppGroup::XpuCommStream *>(dstXpuCommStream));
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream received for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));

            appGroup->xpuCommStreamHtLock_.unlock();
            delete dstXpuCommStream;
            dstXpuCommStream = NULL;
            goto CommonExit;
        }
    }

    appGroup->xpuCommStreamHtLock_.unlock();

    dstXpuCommStream->lock_.lock();

    // We expect every single payload received from the srcXpu to have non-Zero
    // communication buffer.
    assert(payload->bufferSize_ - sizeof(MsgStreamMgr::ProtocolDataUnit) >
           sizeof(XpuCommBuffer));
    bufSizeToCopy = payload->bufferSize_ -
                    sizeof(MsgStreamMgr::ProtocolDataUnit) -
                    sizeof(XpuCommBuffer);
    dstXpuCommStream->streamBufsRecvTotalSize_ += bufSizeToCopy;

    for (size_t ii = cbuf->offset / pageSize;
         ii < dstXpuCommStream->numRecvBufs_ && bytesCopied < bufSizeToCopy;
         ii++) {
        size_t numBytes = pageSize - curRecvBufOffset;
        if (bufSizeToCopy - bytesCopied < numBytes) {
            numBytes = bufSizeToCopy - bytesCopied;
        }

        if (dstXpuCommStream->recvBuf_[ii].buf == NULL) {
            dstXpuCommStream->recvBuf_[ii].buf =
                (void *) XdbMgr::get()->bcAlloc(XidInvalid,
                                                &status,
                                                XdbMgr::SlabHint::Default);

            if (dstXpuCommStream->recvBuf_[ii].buf == NULL) {
                xSyslog(ModuleName,
                        XlogErr,
                        "Xpu Communication stream received for AppGroup %lu"
                        " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                        " failed:%s",
                        cbuf->appGroupId,
                        cbuf->commId,
                        cbuf->seqNum,
                        cbuf->offset,
                        cbuf->srcXpu,
                        cbuf->dstXpu,
                        strGetFromStatus(status));

                dstXpuCommStream->lock_.unlock();
                goto CommonExit;
            }
        }

        memcpy((void *) ((uintptr_t) dstXpuCommStream->recvBuf_[ii].buf +
                         curRecvBufOffset),
               (void *) ((uintptr_t) cbuf->data + bytesCopied),
               numBytes);

        bytesCopied = bytesCopied + numBytes;
        curRecvBufOffset = 0;
    }

    dstXpuCommStream->numStreamBufsReceived_++;

    if (dstXpuCommStream->numStreamBufsReceived_ ==
        dstXpuCommStream->totalBufs_) {
        // All the pieces of the stream buffer has been received. Now just
        // assemble together.
        assert(dstXpuCommStream->streamBufsRecvTotalSize_ ==
               dstXpuCommStream->streamBufsTotalSize_);

        // Hand off this completeBuffer to the respective XPU.
        status = appGroup->dispatchToDstXpu(dstXpuCommStream->recvBuf_,
                                            dstXpuCommStream->numRecvBufs_,
                                            cbuf->srcXpu,
                                            cbuf->dstXpu);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream received for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));

            dstXpuCommStream->lock_.unlock();
            goto CommonExit;
        }

        for (size_t ii = 0; ii < dstXpuCommStream->numRecvBufs_; ii++) {
            XdbMgr::get()->bcFree(dstXpuCommStream->recvBuf_[ii].buf);
            dstXpuCommStream->recvBuf_[ii].buf = NULL;
        }

        // Send ACK to the srcXpu now.
        status = sendAckToSrcXpu(appGroup, dstXpuCommStream);
        if (status != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream received for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));

            dstXpuCommStream->lock_.unlock();
            goto CommonExit;
        }
    }

    dstXpuCommStream->lock_.unlock();

CommonExit:

    // Clean out model here is that upon error, the transaction will be aborted
    // and the state in the AppGroup will be cleaned out before AppGroup itself
    // is destructed.
    if (status != StatusOk) {
        //
        // Abort the transaction.
        //
        Status status2 =
            appGroupMgr->abortThisGroup(cbuf->appGroupId, status, NULL);
        if (status2 != StatusOk) {
            xSyslog(ModuleName,
                    XlogErr,
                    "Xpu Communication stream abort for AppGroup %lu"
                    " commId %lu seq %lu offset %lu srcXpu %u dstXpu %u"
                    " failed:%s",
                    cbuf->appGroupId,
                    cbuf->commId,
                    cbuf->seqNum,
                    cbuf->offset,
                    cbuf->srcXpu,
                    cbuf->dstXpu,
                    strGetFromStatus(status));
        }
    }

    if (appGroup != NULL) {
        appGroup->decRef();
        appGroup = NULL;
    }

    return status;
}
