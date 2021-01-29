// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "util/MemTrack.h"
#include "msgstream/MsgStream.h"
#include "runtime/Runtime.h"
#include "runtime/Tls.h"
#include "MsgStreamState.h"
#include "MsgStreamStartFsm.h"
#include "MsgStreamEndFsm.h"
#include "MsgStreamSendFsm.h"
#include "test/FuncTests/MsgStreamFuncTestActions.h"
#include "ns/LibNsStreamActions.h"
#include "XpuCommStream.h"
#include "sys/XLog.h"

static constexpr const char *moduleName = "LibMsgStream";

MsgStreamState *MsgStreamState::instance = NULL;

//
// MsgStreamState implementation.
//
MsgStreamState *
MsgStreamState::get()
{
    return instance;
}

Status
MsgStreamState::init()
{
    Status status;

    assert(instance == NULL);
    instance =
        (MsgStreamState *) memAllocExt(sizeof(MsgStreamState), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    new (instance) MsgStreamState();

    status = instance->initStats();
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = instance->initObjectActions();
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
MsgStreamState::destroy()
{
    destroyObjectActions();
    instance->~MsgStreamState();
    memFree(instance);
    instance = NULL;
}

Status
MsgStreamState::initStats()
{
    Status status;
    StatsLib *statsLib = StatsLib::get();

    status = statsLib->initNewStatGroup("msg.stream",
                                        &instance->statsGrpId_,
                                        StatCount);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamStartStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "cumlStreamAdded",
                                         instance->cumlStreamStartStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamStartErrStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "cumlStreamAddErr",
                                         instance->cumlStreamStartErrStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamEndStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "cumlStreamRemoved",
                                         instance->cumlStreamEndStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamEndErrStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "cumlStreamRemoveErr",
                                         instance->cumlStreamEndErrStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamSendPayloadStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "cumlStreamSentPayload",
                                         instance->cumlStreamSendPayloadStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = statsLib->initStatHandle(&instance->cumlStreamSendPayloadErrStat_);
    if (status != StatusOk) {
        goto CommonExit;
    }
    status =
        statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                    "cumlStreamSendPayloadErr",
                                    instance->cumlStreamSendPayloadErrStat_,
                                    StatUint64,
                                    StatAbsoluteWithNoRefVal,
                                    StatRefValueNotApplicable);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    return status;
}

Status
MsgStreamState::initObjectActions()
{
    Status status;
    uint8_t ii = 0;

    for (ii =
             static_cast<uint8_t>(MsgStreamMgr::StreamObject::StreamObjectMin) +
             1;
         ii < static_cast<uint8_t>(MsgStreamMgr::StreamObject::StreamObjectMax);
         ii++) {
        switch (static_cast<MsgStreamMgr::StreamObject>(ii)) {
        case MsgStreamMgr::StreamObject::MsgStreamFuncTest:
            streamObjectActions_[ii] = (StreamObjectActions *)
                memAllocExt(sizeof(MsgStreamFuncTestObjectAction), moduleName);
            if (streamObjectActions_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (streamObjectActions_[ii]) MsgStreamFuncTestObjectAction();
            break;
        case MsgStreamMgr::StreamObject::LibNsPathInfoPerNode:
            streamObjectActions_[ii] = (StreamObjectActions *)
                memAllocExt(sizeof(LibNsStreamObjectAction), moduleName);
            if (streamObjectActions_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (streamObjectActions_[ii]) LibNsStreamObjectAction();
            break;
        case MsgStreamMgr::StreamObject::XpuCommunication:
            streamObjectActions_[ii] =
                (StreamObjectActions *) memAllocExt(sizeof(XpuCommObjectAction),
                                                    moduleName);
            if (streamObjectActions_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (streamObjectActions_[ii]) XpuCommObjectAction();
            break;
        default:
            assert(0);
            status = StatusUnimpl;
            goto CommonExit;
            break;  // Never reached
        }
    }
    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        for (uint8_t jj = static_cast<uint8_t>(
                              MsgStreamMgr::StreamObject::StreamObjectMin) +
                          1;
             jj < ii;
             jj++) {
            streamObjectActions_[jj]->~StreamObjectActions();
            memFree(streamObjectActions_[jj]);
        }
    }
    return status;
}

void
MsgStreamState::destroyObjectActions()
{
    for (uint8_t ii =
             static_cast<uint8_t>(MsgStreamMgr::StreamObject::StreamObjectMin) +
             1;
         ii < static_cast<uint8_t>(MsgStreamMgr::StreamObject::StreamObjectMax);
         ii++) {
        streamObjectActions_[ii]->~StreamObjectActions();
        memFree(streamObjectActions_[ii]);
    }
}

Status
MsgStreamState::streamHashInsert(MsgStreamInfo *msgStreamInfo)
{
    Status status;

    streamHashTableLock_.lock();
    status = streamHashTable_.insert(msgStreamInfo);
    streamHashTableLock_.unlock();
    return status;
}

MsgStreamInfo *
MsgStreamState::streamHashFind(Xid streamId)
{
    MsgStreamInfo *tmp;
    streamHashTableLock_.lock();
    tmp = streamHashTable_.find(streamId);
    streamHashTableLock_.unlock();
    return tmp;
}

MsgStreamInfo *
MsgStreamState::streamHashRemove(Xid streamId)
{
    MsgStreamInfo *tmp;
    streamHashTableLock_.lock();
    tmp = streamHashTable_.remove(streamId);
    streamHashTableLock_.unlock();
    return tmp;
}

Status
MsgStreamState::startStreamHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu)
{
    Status status;
    MsgStreamInfo *msgStreamInfo = NULL;

    assert(pdu->streamState_ == MsgStreamMgr::StreamState::StreamSetup);

    // Track stream related information
    msgStreamInfo = new (std::nothrow)
        MsgStreamInfo(pdu->streamId_,
                      MsgStreamMgr::StreamState::StreamEstablished,
                      pdu->twoPcNodeProperty_,
                      pdu->srcNodeId_,
                      pdu->dstNodeId_,
                      pdu->streamObject_,
                      NULL);
    BailIfNull(msgStreamInfo);

    status = streamHashInsert(msgStreamInfo);
    if (status != StatusOk) {
        goto CommonExit;
    }

    status = streamObjectActions_[static_cast<uint8_t>(pdu->streamObject_)]
                 ->startStreamLocalHandler(pdu->streamId_,
                                           &msgStreamInfo->objectContext_);
    if (status != StatusOk) {
        goto CommonExit;
    }

CommonExit:
    if (status != StatusOk) {
        if (msgStreamInfo != NULL) {
            delete msgStreamInfo;
            msgStreamInfo = NULL;
        }
    }

    return status;
}

Status
MsgStreamState::endStreamHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu)
{
    Status status;
    MsgStreamInfo *msgStreamInfo, *tmp;

    msgStreamInfo = streamHashFind(pdu->streamId_);
    if (msgStreamInfo == NULL) {
        status = StatusMsgStreamNotFound;
        xSyslog(moduleName,
                XlogErr,
                "End stream local handler for Stream %lu failed: %s",
                pdu->streamId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(pdu->streamState_ == MsgStreamMgr::StreamState::StreamTeardown);

    msgStreamInfo->stateLock_.lock();
    assert(msgStreamInfo->streamState_ ==
               MsgStreamMgr::StreamState::StreamEstablished ||
           msgStreamInfo->streamState_ ==
               MsgStreamMgr::StreamState::StreamInProgress ||
           msgStreamInfo->streamState_ ==
               MsgStreamMgr::StreamState::StreamIsDone);
    msgStreamInfo->streamState_ = MsgStreamMgr::StreamState::StreamTeardown;
    msgStreamInfo->stateLock_.unlock();

    status = streamObjectActions_[static_cast<uint8_t>(pdu->streamObject_)]
                 ->endStreamLocalHandler(msgStreamInfo->streamId_,
                                         msgStreamInfo->objectContext_);

    // Just remove the stream info irrespective of the status from per object
    // end stream handler.
    tmp = streamHashRemove(pdu->streamId_);
    verify(tmp == msgStreamInfo);
    delete msgStreamInfo;
    msgStreamInfo = NULL;

CommonExit:
    return status;
}

Status
MsgStreamState::streamReceiveHandlerLocal(MsgStreamMgr::ProtocolDataUnit *pdu)
{
    Status status;
    MsgStreamInfo *msgStreamInfo;

    msgStreamInfo = streamHashFind(pdu->streamId_);
    if (msgStreamInfo == NULL) {
        status = StatusMsgStreamNotFound;
        xSyslog(moduleName,
                XlogErr,
                "Receive stream local handler for Stream %lu failed: %s",
                pdu->streamId_,
                strGetFromStatus(status));
        goto CommonExit;
    }

    assert(pdu->streamState_ == MsgStreamMgr::StreamState::StreamInProgress ||
           pdu->streamState_ == MsgStreamMgr::StreamState::StreamIsDone);

    msgStreamInfo->stateLock_.lock();
    assert(msgStreamInfo->streamState_ ==
               MsgStreamMgr::StreamState::StreamEstablished ||
           msgStreamInfo->streamState_ ==
               MsgStreamMgr::StreamState::StreamInProgress);
    msgStreamInfo->streamState_ = pdu->streamState_;
    msgStreamInfo->stateLock_.unlock();

    status = streamObjectActions_[static_cast<uint8_t>(pdu->streamObject_)]
                 ->streamReceiveLocalHandler(msgStreamInfo->streamId_,
                                             msgStreamInfo->objectContext_,
                                             pdu);

CommonExit:
    return status;
}

void
MsgStreamState::startStreamCompletionHandler(MsgEphemeral *eph)
{
    MsgStreamStartFsm *startFsm =
        static_cast<MsgStreamStartFsm *>(eph->ephemeral);
    MsgStreamInfo *msgStreamInfo = startFsm->getMsgStreamInfo();

    assert(msgStreamInfo->streamState_ ==
           MsgStreamMgr::StreamState::StreamSetup);
    startFsm->setStatus(eph->status);
    startFsm->resume();
}

void
MsgStreamState::endStreamCompletionHandler(MsgEphemeral *eph)
{
    MsgStreamEndFsm *endFsm = static_cast<MsgStreamEndFsm *>(eph->ephemeral);
    MsgStreamInfo *msgStreamInfo = endFsm->getMsgStreamInfo();

    assert(msgStreamInfo->streamState_ ==
           MsgStreamMgr::StreamState::StreamTeardown);
    endFsm->setStatus(eph->status);
    endFsm->resume();
}

void
MsgStreamState::streamSendCompletionHandler(MsgEphemeral *eph)
{
    MsgStreamSendFsm *sendFsm = static_cast<MsgStreamSendFsm *>(eph->ephemeral);
    sendFsm->setStatus(eph->status);

    Status status = Runtime::get()->schedule(sendFsm);
    if (status != StatusOk) {
        // XXX TODO If we failed to Schedule on Runtime, Txn will not be able
        // make forward progress.
        xSyslog(moduleName,
                XlogErr,
                "Failed streamSendCompletionHandler for Stream %lu: %s",
                sendFsm->getPayload()->streamId_,
                strGetFromStatus(status));
    }
}

void
MsgStreamState::processSendCompletions(MsgStreamInfo *msgStreamInfo,
                                       void *sendContext,
                                       Status reason)
{
    streamObjectActions_[static_cast<uint8_t>(msgStreamInfo->streamObject_)]
        ->streamSendCompletionHandler(msgStreamInfo->streamId_,
                                      msgStreamInfo->objectContext_,
                                      sendContext,
                                      reason);
}
