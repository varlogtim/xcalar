// Copyright 2013 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <new>
#include <sys/eventfd.h>

#include "primitives/Primitives.h"
#include "operators/GenericTypes.h"
#include "config/Config.h"
#include "usrnode/UsrNode.h"
#include "SchedInt.h"
#include "stat/Statistics.h"
#include "msg/Message.h"
#include "MessageInt.h"
#include "msg/TwoPcFuncDefs.h"
#include "TwoPcInt.h"
#include "bc/BufferCache.h"
#include "xdb/Xdb.h"
#include "sys/Socket.h"
#include "util/System.h"
#include "libapis/LibApisCommon.h"
#include "operators/Operators.h"
#include "df/DataFormat.h"
#include "operators/Xdf.h"
#include "querymanager/QueryManager.h"
#include "dataset/Dataset.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "df/DataFormat.h"
#include "runtime/Runtime.h"
#include "runtime/Mutex.h"
#include "msg/Xid.h"
#include "transport/TransportPage.h"
#include "runtime/Tls.h"
#include "libapis/LibApisRecv.h"
#include "SchedMessageClient.h"
#include "StrlFunc.h"
#include "util/XcalarProcFsHelper.h"
#include "libapis/LibApisRecv.h"

static constexpr const char *moduleName = "libmsg";
MsgMgr *MsgMgr::instance = NULL;

uint64_t
MsgMgr::getAckAndFreeHandlerQueueDepth(unsigned nodes)
{
    // based on all other nodes sending all their messages to this node
    return (getNormalMsgCount(nodes) + getAltMsgCount(nodes)) * nodes;
}

uint64_t
MsgMgr::getNormalMsgCount(unsigned nodes)
{
    return (SteadyStateMsgCount + ReserveMsgCount) * nodes;
}

uint64_t
MsgMgr::getAltMsgCount(unsigned nodes)
{
    return TransportPageMgr::transportSourcePagesCount(nodes);
}

uint64_t
MsgMgr::getTwoPcPages(unsigned nodes)
{
    return getNormalMsgCount(nodes);
}

uint64_t
MsgMgr::getUkMsgAtomic64(unsigned nodes)
{
    return getNormalMsgCount(nodes) + getAltMsgCount(nodes);
}

uint64_t
MsgMgr::getSchedQSize()
{
    return SchedMgr::SchedQSize;
}

uint64_t
MsgMgr::getUkSchedObjectBufSize()
{
    return SchedMgr::bcUkSchedObjectBufSize();
}

uint64_t
MsgMgr::getMsgMaxPayloadSize()
{
    return XdbMgr::bcSize();
}

MsgMgr *
MsgMgr::get()
{
    return instance;
}

void
MsgMgr::setMsgNodeState(MsgEphemeral *ephemeral, void *payload)
{
    assert(payload != NULL);
    Status status = StatusOk;
    NodeId srcNode = getMsgSrcNodeId(ephemeral);
    BarrierInfo *bInfo;

    const BarrierMsg *barrierMsg = (const BarrierMsg *) payload;

    if (barrierMsg->type == BarrierIsNetworkReady) {
        memcpy(&nodeLocalInfo_[srcNode],
               &barrierMsg->info,
               sizeof(NodeLocalInfo));
    } else if (barrierMsg->type == BarrierUsrnodeApisRecvReady) {
        setApisRecvInitialized();
    }

    status = twoPcInfo_.barrier_.getBarrierInfo(barrierMsg->type, &bInfo);
    BailIfFailed(status);

    bInfo->recordNodeEntered(srcNode);

CommonExit:
    ephemeral->status = status;
}

void
MsgMgr::initMsg(MsgMessage *msg,
                MsgTypeId msgTypeId,
                uint64_t key,
                NodeId myNodeId,
                Xid xidIn,
                Semaphore *wakeupSem,
                Atomic32 *outstandingMessages,
                MsgEphemeral *ephemeral,
                MsgSendRecvFlags msgSendRecvFlags,
                TwoPcHandle twoPcHandle,
                TwoPcCmdType twoPcCmdType,
                TwoPcFastOrSlowPath twoPcFastOrSlowPath)
{
    msg->msgHdr.typeId = msgTypeId;
    msg->msgHdr.key = xidIn;
    msg->msgHdr.srcNodeId = myNodeId;
    msg->msgHdr.xid = xidIn;
    msg->msgHdr.wakeupSem = wakeupSem;
    msg->msgHdr.outstandingMessages = outstandingMessages;
    memcpy(&msg->msgHdr.eph, ephemeral, sizeof(*ephemeral));

    msg->msgHdr.twoPcHandle = twoPcHandle;
    msg->msgHdr.twoPcCmdType = twoPcCmdType;
    msg->msgHdr.msgSendRecvFlags = msgSendRecvFlags;
    msg->msgHdr.twoPcFastOrSlowPath = twoPcFastOrSlowPath;
}

MsgMgr::ResourceLevel
MsgMgr::failOnResourceShortage(MsgTypeId msgTypeId)
{
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();
    bool canFail = twoPcMgr->twoPcOp_[static_cast<uint32_t>(msgTypeId)]
                       .getFailIfResourceShortage();
    struct timespec prevTime, curTime;

    int ret = clock_gettime(CLOCK_MONOTONIC_COARSE, &curTime);
    if (ret != 0) {
        if (canFail == true) {
            return ResourceLevel::Low;
        }
        return ResourceLevel::Reserve;
    }

    prevTime = lastUpdatedSysFreeResSize_;
    uint64_t curElapTimeUSec =
        clkGetElapsedTimeInNanosSafe(&prevTime, &curTime) / NSecsPerUSec;

    // Test if previous cached states are invalidated.
    if (curElapTimeUSec >= CachedSysFreeResSizeInvalidateUsec) {
        if (procFsHelper_->getSysFreeResourceSize(&cachedSysFreeResSize_) !=
            StatusOk) {
            if (canFail == true) {
                return ResourceLevel::Low;
            }
            return ResourceLevel::Reserve;
        }

        lastUpdatedSysFreeResSize_ = curTime;
        if (cachedSysFreeResSize_ < sysFreeResThreshold_) {
            sysFreeResourceBelowThreshold_ = true;
        } else if (cachedSysFreeResSize_ >= sysFreeResHealThreshold_) {
            sysFreeResourceBelowThreshold_ = false;
        }
        memBarrier();
    }

    if (sysFreeResourceBelowThreshold_ == false) {
        // We are above water here with resource level. So things are normal.
        return ResourceLevel::Normal;
    } else {
        if (canFail == true) {
            // These are classes of messages that can be failed, since it's
            // caller is resilient enough to kick in transaction recovery.
            return ResourceLevel::Low;
        } else {
            // We cannot fail these classes of messages, since it's caller does
            // not tolerate failures. So find way to run these on reserve level.
            return ResourceLevel::Reserve;
        }
    }
}

// If TwoPcAsyncCmd, caller must ensure non-zero TwoPcHandle is returned. Else,
// caller must ensure NULL is returned.
MustCheck Status
MsgMgr::twoPc(TwoPcHandle *twoPcHandleOut,
              MsgTypeId msgTypeId,
              TwoPcHandleEnum returnHandle,
              MsgEphemeral *ephemeral,
              MsgSendRecvFlags msgSendRecvFlags,
              TwoPcCmdType twoPcCmdType,
              TwoPcNodeProperty twoPcNodeProperty,
              NodeId remoteNodeId,
              TwoPcClass twoPcClass)
{
    Config *config = Config::get();
    Status status = StatusOk;
    Status returnStatus = StatusOk;
    TwoPcHandle twoPcHandleTemp = twoPcInfo_.dummyTwoPcHandle_;
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();
    SchedMgr *schedMgr = SchedMgr::get();
    NodeId myNodeId = config->getMyNodeId();
    int activeNodes = config->getActiveNodes();
    Xid xidTemp = XidMgr::get()->xidGetNext();
    MsgMessage *remoteMsg = NULL, *localMsg = NULL, *msg = NULL;
    bool twoPcHandleValid = false;

    assert((msgSendRecvFlags & MsgSendHdrOnly) ||
           (msgSendRecvFlags & MsgSendHdrPlusPayload));
    assert((msgSendRecvFlags & MsgRecvHdrOnly) ||
           (msgSendRecvFlags & MsgRecvHdrPlusPayload));

    // Propagate the TXN ID
    if (!ephemeral->txn.valid()) {
        ephemeral->txn = Txn::currentTxn();
    }
    assert(Txn::currentTxn().valid());

    uint32_t statIndex = getStatIndex(ephemeral->twoPcFuncIndex);
    StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCInvoked_[statIndex]);

    ResourceLevel resourceLevel = failOnResourceShortage(msgTypeId);
    if (resourceLevel == ResourceLevel::Low) {
        StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCFinished_[statIndex]);
        StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCErrors_[statIndex]);
        StatsLib::statAtomicIncr64(resourceLevelLow_);
        return StatusNoMem;
    }

    // Alloc semaphore only for sync cmds
    Atomic32 outstandingMessages;
    Semaphore wakeupSem(0);
    Semaphore *wakeupSemAddr;
    if (twoPcCmdType == TwoPcSyncCmd) {
        atomicWrite32(&outstandingMessages, 1);  // will dec later
        wakeupSemAddr = &wakeupSem;
    } else {
        wakeupSemAddr = NULL;
    }

    // payloadLen will be changed below. When we need what the caller wanted
    // refer to ephemeral->payloadLength (as in the memcpy()).
    size_t payloadLen = ephemeral->payloadLength;

    void *payloadToDistribute = ephemeral->payloadToDistribute;
    PayloadBufCacheType payloadBufCacheType = InvalidPayloadBufCacheType;
    TwoPcFastOrSlowPath twoPcFastOrSlowPath = ephemeral->twoPcFastOrSlowPath;

    // Limits over all concurrent 2PCs in the system.
    MsgBufType msgBufType = NormalMsg;
    Semaphore *throttleSem = NULL;
    Semaphore *nestedThrottleSem = NULL;

    // No need to throttle if this is a p2p 2PC to self, in which
    // case this is just a function call.
    bool doThrottle = !(twoPcNodeProperty == TwoPcSingleNode &&
                        ((int) remoteNodeId == (int) myNodeId));

    if (doThrottle) {
        if (resourceLevel == ResourceLevel::Reserve) {
            // Reserve Concurrency
            throttleSem = twoPcInfo_.twoPcReserveSem_;
            nestedThrottleSem = twoPcInfo_.twoPcNestedReserveSem_;
        } else {
            // Normal Concurrency
            throttleSem = twoPcInfo_.twoPcNormalSem_;
            nestedThrottleSem = twoPcInfo_.twoPcNestedNormalSem_;
        }

        // We need to factor in nesting while managing twoPc concurrency.
        if (twoPcClass == TwoPcClassNested) {
            nestedThrottleSem->semWait();
        } else {
            throttleSem->semWait();
        }
    }

    TwoPcHandle twoPcHandle = nextTwoPcCmdHandle(msgTypeId,
                                                 twoPcCmdType,
                                                 twoPcNodeProperty,
                                                 remoteNodeId);
    twoPcHandleValid = true;
    uint32_t index = twoPcHandle.index;
#ifdef DEBUG
    for (int ii = 0; ii < activeNodes; ++ii) {
        assert(twoPcInfo_.twoPcStateCompletions_[index][ii] ==
               MsgTypeId::MsgNull);
    }
#endif  // DEBUG

    if (payloadToDistribute) {
        assert(msgSendRecvFlags & MsgSendHdrPlusPayload);
        assert(ephemeral->payloadLengthTwoPcToAllocate == 0 ||
               ephemeral->payloadLengthTwoPcToAllocate >= payloadLen);

        if (twoPcFastOrSlowPath == TwoPcFastPath) {
            assert(payloadLen <= XdbMgr::bcSize());
            payloadBufCacheType = XdbBufCacheForPayload;
        } else {
            assert(twoPcFastOrSlowPath == TwoPcSlowPath);
            if (payloadLen > MsgMaxPayloadSizeAbsolute) {
                StatsLib::statAtomicIncr64(bigMessagesStat_);
            } else {
                payloadLen = MsgMaxPayloadSizeAbsolute;
            }
            payloadBufCacheType = MallocedBufForPayload;
        }
    } else {
        assert(!payloadLen);

        if (ephemeral->payloadLengthTwoPcToAllocate) {
            // This means that twoPc must internally allocate a payload
            // even though there is none coming into the call.
            // Note: payload will be allocated in allocMsg().
            payloadLen = ephemeral->payloadLengthTwoPcToAllocate;

            if (payloadLen > MsgMaxPayloadSizeAbsolute) {
                StatsLib::statAtomicIncr64(bigMessagesStat_);
            }

            payloadBufCacheType = MallocedBufForPayload;
        } else {
            payloadBufCacheType = NoPayload;
        }
    }

    if (twoPcNodeProperty == TwoPcAllNodes) {
        assert((unsigned) remoteNodeId == TwoPcIgnoreNodeId);

        // Alloc local and remote messages
        assert(localMsg == NULL);
        status =
            allocMsg(&localMsg,
                     MsgCreatorTwoPc,
                     payloadBufCacheType,
                     xcMax(payloadLen, ephemeral->payloadLengthTwoPcToAllocate),
                     msgBufType);
        if (status != StatusOk) {
            returnStatus = status;
            goto CommonExit;
        }

        initMsg(localMsg,
                msgTypeId,
                xidTemp,
                myNodeId,
                xidTemp,
                wakeupSemAddr,
                &outstandingMessages,
                ephemeral,
                msgSendRecvFlags,
                twoPcHandle,
                twoPcCmdType,
                twoPcFastOrSlowPath);

        assert(remoteMsg == NULL);
        status = allocMsg(&remoteMsg,
                          MsgCreatorTwoPc,
                          payloadBufCacheType,
                          payloadLen,
                          msgBufType);
        if (status != StatusOk) {
            returnStatus = status;
            goto CommonExit;
        }

        initMsg(remoteMsg,
                msgTypeId,
                xidTemp,
                myNodeId,
                xidTemp,
                wakeupSemAddr,
                &outstandingMessages,
                ephemeral,
                msgSendRecvFlags,
                twoPcHandle,
                twoPcCmdType,
                twoPcFastOrSlowPath);

        if (payloadToDistribute) {
            memcpy(remoteMsg->payload,
                   payloadToDistribute,
                   ephemeral->payloadLength);
            memcpy(localMsg->payload,
                   payloadToDistribute,
                   ephemeral->payloadLength);
        }

        // Send cmd to all nodes
        // This loop is a point of no-return. We can't just bail out if we fail
        // to send a cmd to any of the node. This is because sending a cmd to a
        // node means that there's a potential outstanding ack we need to
        // service
        for (int ii = 0; ii < activeNodes; ++ii) {
            msg = (ii == (int) myNodeId) ? localMsg : remoteMsg;
            msg->msgHdr.dstNodeId = ii;

            atomicInc32(&outstandingMessages);
            if (ii != (int) myNodeId) {
                status = sendMsg(ii, msg, SourceToDest);
                if (status != StatusOk) {
                    uint32_t index = msg->msgHdr.twoPcHandle.index;
                    MsgTypeId state = (MsgTypeId)((int) msg->msgHdr.typeId +
                                                  TwoPcCmdCompletionOffset);
                    assert(twoPcInfo_.twoPcStateCompletions_[index][ii] ==
                           MsgTypeId::MsgNull);
                    twoPcInfo_.twoPcStateCompletions_[index][ii] = state;
                    verify(atomicDec32(&outstandingMessages) > 0);
                }
            } else {
                status =
                    schedMgr->schedOneMessage(msg,
                                              true);  // Schedule on Runtime
                if (status != StatusOk) {
                    msg->msgHdr.eph.status = status;
                    updateMsgLocalTwoPcState(msg,
                                             (MsgTypeId)(
                                                 (int) msg->msgHdr.typeId +
                                                 TwoPcCmdCompletionOffset));
                    freeMsgOnly(msg);
                }
                localMsg = NULL;
            }
        }
    } else {
        assert(twoPcNodeProperty == TwoPcSingleNode);

        if ((int) remoteNodeId == (int) myNodeId) {
            assert(localMsg == NULL);
            status = allocMsg(&localMsg,
                              MsgCreatorTwoPc,
                              payloadBufCacheType,
                              payloadLen,
                              msgBufType);
            if (status != StatusOk) {
                returnStatus = status;
                goto CommonExit;
            }

            initMsg(localMsg,
                    msgTypeId,
                    xidTemp,
                    myNodeId,
                    xidTemp,
                    wakeupSemAddr,
                    &outstandingMessages,
                    ephemeral,
                    msgSendRecvFlags,
                    twoPcHandle,
                    twoPcCmdType,
                    twoPcFastOrSlowPath);
        } else {
            assert(remoteMsg == NULL);
            status = allocMsg(&remoteMsg,
                              MsgCreatorTwoPc,
                              payloadBufCacheType,
                              payloadLen,
                              msgBufType);
            if (status != StatusOk) {
                returnStatus = status;
                goto CommonExit;
            }

            initMsg(remoteMsg,
                    msgTypeId,
                    xidTemp,
                    myNodeId,
                    xidTemp,
                    wakeupSemAddr,
                    &outstandingMessages,
                    ephemeral,
                    msgSendRecvFlags,
                    twoPcHandle,
                    twoPcCmdType,
                    twoPcFastOrSlowPath);
        }

        // Send cmd to single node: local or remote
        msg = ((int) remoteNodeId == (int) myNodeId) ? localMsg : remoteMsg;
        msg->msgHdr.dstNodeId = remoteNodeId;

        if (payloadToDistribute) {
            memcpy(msg->payload, payloadToDistribute, ephemeral->payloadLength);
        }

        atomicInc32(&outstandingMessages);
        if ((int) remoteNodeId != (int) myNodeId) {
            status = sendMsg(remoteNodeId, msg, SourceToDest);
            if (status != StatusOk) {
                uint32_t index = msg->msgHdr.twoPcHandle.index;
                MsgTypeId state = (MsgTypeId)((int) msg->msgHdr.typeId +
                                              TwoPcCmdCompletionOffset);
                assert(twoPcInfo_.twoPcStateCompletions_[index][remoteNodeId] ==
                       MsgTypeId::MsgNull);
                twoPcInfo_.twoPcStateCompletions_[index][remoteNodeId] = state;
                verify(atomicDec32(&outstandingMessages) > 0);
            }
        } else {
            status =
                schedMgr->schedOneMessage(msg,
                                          false);  // Don't schedule on runtime
            if (status != StatusOk) {
                msg->msgHdr.eph.status = status;
                updateMsgLocalTwoPcState(msg,
                                         (MsgTypeId)((int) msg->msgHdr.typeId +
                                                     TwoPcCmdCompletionOffset));
                freeMsgOnly(msg);
            }
            localMsg = NULL;
        }
    }

    returnStatus = status;

    if (twoPcCmdType == TwoPcSyncCmd) {
        assert(wakeupSemAddr);

        if (atomicDec32(&outstandingMessages) != 0) {
            wakeupSemAddr->semWait();
        }

        if (returnHandle == TwoPcDoNotReturnHandle) {
            twoPcFreeCmdHandle(twoPcHandle);

            // The user does not expect a handle, so return dummy to
            // satisfy the compiler.
            twoPcHandleTemp = twoPcInfo_.dummyTwoPcHandle_;
        } else {
            assert(returnHandle == TwoPcReturnHandle);
            xSyslog(moduleName, XlogWarn, "twoPc()::handle not freed!");
            twoPcHandleTemp = twoPcHandle;
        }
    } else {
        // Async cmds must ask for a returnHandle.
        assert(twoPcCmdType == TwoPcAsyncCmd);
        assert(returnHandle == TwoPcReturnHandle);
        twoPcHandleTemp = twoPcHandle;
    }

    if (returnStatus == StatusOk && status != StatusOk) {
        returnStatus = status;
    }

#ifdef DEBUG
    if ((twoPcCmdType == TwoPcSyncCmd) && wakeupSemAddr) {
        assert(atomicRead32(&outstandingMessages) == 0);
    }
#endif  // DEBUG

    *twoPcHandleOut = twoPcHandleTemp;
    twoPcHandleValid = false;

CommonExit:

    if (localMsg != NULL) {
        freeAndEnqueueMsg(localMsg);
        localMsg = NULL;
    }

    if (remoteMsg != NULL) {
        freeAndEnqueueMsg(remoteMsg);
        remoteMsg = NULL;
    }

    if (twoPcHandleValid) {
        twoPcFreeCmdHandle(twoPcHandle);
        twoPcHandleValid = false;
    }

    if (doThrottle) {
        if (twoPcClass == TwoPcClassNested) {
            nestedThrottleSem->post();
        } else {
            throttleSem->post();
        }
    }

    StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCFinished_[statIndex]);
    if (returnStatus != StatusOk) {
        StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCErrors_[statIndex]);
    }

    return returnStatus;
}

Status
MsgMgr::twoPcAlt(MsgTypeId msgTypeId,
                 MsgEphemeral *ephemeral,
                 MsgSendRecvFlags msgSendRecvFlags,
                 TwoPcCmdType twoPcCmdType,
                 PayloadBufCacheType payloadBufCacheType,
                 TwoPcNodeProperty twoPcNodeProperty,
                 NodeId remoteNodeId,
                 Txn txn)
{
    Status status;
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();

    assert(twoPcCmdType == TwoPcAltCmd);
    assert(twoPcNodeProperty == TwoPcSingleNode);

    assert((msgSendRecvFlags & MsgSendHdrOnly) ||
           (msgSendRecvFlags & MsgSendHdrPlusPayload));
    assert((msgSendRecvFlags & MsgRecvHdrOnly) ||
           (msgSendRecvFlags & MsgRecvHdrPlusPayload));

    void *payloadToDistribute = ephemeral->payloadToDistribute;

    size_t payloadLen = ephemeral->payloadLength;
    assert(payloadLen <= XdbMgr::bcSize());

#ifdef DEBUG
    if (payloadToDistribute) {
        assert(msgSendRecvFlags & MsgSendHdrPlusPayload);
        assert(payloadLen > 0);
    } else {
        assert(payloadLen == 0);
    }
#endif  // DEBUG

    // Propagate the recovery context (if any) to remote nodes
    assert(Txn::currentTxn().valid());
    ephemeral->txn = txn;

    NodeId myNodeId = Config::get()->getMyNodeId();
    // twoPcAlt only sends to remote nodes
    assert(remoteNodeId != myNodeId);
    Xid xidTemp = XidMgr::get()->xidGetNext();
    MsgMessage *remoteMsg = NULL;
    TwoPcHandle twoPcHandle;
    twoPcHandle.twoPcHandle = TwoPcAltHandle;

    Semaphore *wakeupSemAddrDummy = NULL;
    Atomic32 *outstandingMessagesDummy = NULL;
    uint32_t statIndex = getStatIndex(ephemeral->twoPcFuncIndex);
    StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCInvoked_[statIndex]);

    twoPcInfo_.twoPcAltSem_->semWait();
    ResourceLevel resourceLevel = failOnResourceShortage(msgTypeId);
    if (resourceLevel == ResourceLevel::Low) {
        StatsLib::statAtomicIncr64(resourceLevelLow_);
        status = StatusNoMem;
        goto CommonExit;
    }

    status = allocMsg(&remoteMsg,
                      MsgCreatorTwoPc,
                      payloadBufCacheType,
                      payloadLen,
                      AltMsg);
    if (status != StatusOk) {
        goto CommonExit;
    }

    initMsg(remoteMsg,
            msgTypeId,
            xidTemp,
            myNodeId,
            xidTemp,
            wakeupSemAddrDummy,
            outstandingMessagesDummy,
            ephemeral,
            msgSendRecvFlags,
            twoPcHandle,
            twoPcCmdType,
            ephemeral->twoPcFastOrSlowPath);

    remoteMsg->msgHdr.dstNodeId = remoteNodeId;

    if (payloadToDistribute) {
        // Note: remoteMsg->payload is assigned to caller's buf.
        remoteMsg->payload = payloadToDistribute;
    } else {
        remoteMsg->payload = NULL;
    }

    assert(remoteMsg->msgHdr.payloadBufCacheType == payloadBufCacheType);

    remoteMsg->msgHdr.payloadImmutable = remoteMsg->payload;

    status = sendMsg(remoteNodeId, remoteMsg, SourceToDest);

    if (remoteMsg != NULL) {
        freeAndEnqueueMsg(remoteMsg);
    }

CommonExit:
    twoPcInfo_.twoPcAltSem_->post();

    StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCFinished_[statIndex]);
    if (status != StatusOk) {
        StatsLib::statNonAtomicIncr(twoPcMgr->numLocal2PCErrors_[statIndex]);
    }
    return status;
}

void
MsgMgr::BarrierInfo::recordNodeEntered(NodeId nodeId)
{
    Config *config = Config::get();
    unsigned numNodes = config->getActiveNodes();
    bool allNodesDone = true;

    lock_.lock();
    assert(!nodeReached_[nodeId] && "we should not get duplicates");
    nodeReached_[nodeId] = true;

    allNodesDone = true;
    for (unsigned ii = 0; ii < numNodes; ii++) {
        if (!nodeReached_[ii]) {
            allNodesDone = false;
            break;
        }
    }
    lock_.unlock();

    if (allNodesDone) {
        barrierReached_.post();
    }
}

Status
MsgMgr::BarrierMgr::getBarrierInfo(BarrierType type, BarrierInfo **bInfo)
{
    lock_.lock();
    assert(bInfo);

    Status status = StatusOk;
    *bInfo = NULL;
    bool insertedIntoHT = false;

    *bInfo = barrierTable_.find(type);
    if (*bInfo == NULL) {
        *bInfo = new (std::nothrow) BarrierInfo(type);
        BailIfNull(*bInfo);

        status = barrierTable_.insert(*bInfo);
        assert(status != StatusExist);
        BailIfFailed(status);
        insertedIntoHT = true;
    }
CommonExit:
    // We own the bInfo until it gets put into the HT, so if we fail to insert
    // it into the HT (AKA, we fail to transfer ownership), we have to clean it
    if (status != StatusOk && !insertedIntoHT) {
        delete *bInfo;
        *bInfo = NULL;
    }
    lock_.unlock();
    return status;
}

Status
MsgMgr::BarrierMgr::initBarrier(BarrierType type, BarrierInfo **bInfo)
{
    Status status = StatusOk;
    // Note that we mandate that the user of barrier not overlap barriers,
    // so there is no race here
    status = getBarrierInfo(type, bInfo);
    return status;
}

void
MsgMgr::BarrierMgr::finalizeBarrier(BarrierInfo *bInfo)
{
    lock_.lock();
    verify(barrierTable_.remove(bInfo->getType()));
    lock_.unlock();
    delete bInfo;
}

// Only 1 barrier may occur at a time. The user must guarantee this.
// Currently, this does not hold true due to startup/shutdown issues.
Status
MsgMgr::twoPcBarrier(BarrierType type)
{
    Status status = StatusOk;
    Config *config = Config::get();
    unsigned activeNodes = config->getActiveNodes();
    MsgTypeId msgTypeId = MsgTypeId::Msg2pcBarrier;
    void *payload;
    uint64_t payloadLength;
    BarrierMsg barrierMsg;
    BarrierInfo *bInfo = NULL;
    NodeId myNodeId = config->getMyNodeId();

    xSyslog(moduleName, XlogInfo, "Barrier %lu started", type);

    status = twoPcInfo_.barrier_.initBarrier(type, &bInfo);
    BailIfFailed(status);

    barrierMsg.info = nodeLocalInfo_[myNodeId];
    barrierMsg.type = bInfo->getType();
    payload = &barrierMsg;
    payloadLength = sizeof(barrierMsg);

    {
        MsgEphemeral eph;
        TwoPcHandle twoPcHandle;
        Status statusArray[activeNodes];

        twoPcEphemeralInit(&eph,
                           payload,
                           payloadLength,
                           payloadLength + sizeof(MsgTypeId),
                           TwoPcSlowPath,
                           TwoPcCallId::Msg2pcTwoPcBarrier,
                           &statusArray,
                           TwoPcZeroCopyOutput);

        Status status =
            twoPc(&twoPcHandle,
                  msgTypeId,
                  TwoPcDoNotReturnHandle,
                  &eph,
                  (MsgSendRecvFlags)(MsgSendHdrPlusPayload | MsgRecvHdrOnly),
                  TwoPcSyncCmd,
                  TwoPcAllNodes,
                  TwoPcIgnoreNodeId,
                  TwoPcClassNonNested);
        BailIfFailed(status);

        for (unsigned ii = 0; ii < activeNodes; ++ii) {
            if (statusArray[ii] != StatusOk) {
                status = statusArray[ii];
                goto CommonExit;
            }
        }
    }

    bInfo->barrierReached_.semWait();

CommonExit:
    if (bInfo) {
        twoPcInfo_.barrier_.finalizeBarrier(bInfo);
        bInfo = NULL;
    }

    if (status == StatusOk && type == BarrierIsNetworkReady) {
        for (unsigned ii = 0; ii < activeNodes; ++ii) {
            // XXX Needs to overload "=" operator instead.
            if (memcmp(&nodeLocalInfo_[myNodeId],
                       &nodeLocalInfo_[ii],
                       sizeof(NodeLocalInfo))) {
                symmetricalNodes_ = false;
                break;
            }
        }
    }

    xSyslog(moduleName,
            XlogInfo,
            "Barrier %lu completed: %s",
            type,
            strGetFromStatus(status));

    return status;
}

Status
MsgMgr::setClusterStateShutdown()
{
    Status status = StatusOk;

    clusterState_ = ClusterState::Shutdown;
    // Ensure that cluster state has global visibility
    memBarrier();

    return status;
}

// The caller of twoPc() is responsible for freeing up twoPcHandle if they
// acquired one in the first place.
// twoPcFreeCmdHandle() can only be invoked on the node where the
// twoPc() originated.
void
MsgMgr::twoPcFreeCmdHandle(TwoPcHandle twoPcHandle)
{
    uint32_t index = twoPcHandle.index;
    uint32_t originNodeId = twoPcHandle.originNodeId;
    uint32_t remoteNodeId = twoPcHandle.remoteNodeId;
    Config *config = Config::get();
    int activeNodes = config->getActiveNodes();

    assert(index < TwoPcMaxOutstandingCmds);
    assert(originNodeId < (uint32_t) activeNodes);

    uint32_t myNodeId = config->getMyNodeId();
    assert(myNodeId == originNodeId);

    if (twoPcHandle.allNodes == TwoPcAllNodes) {
        for (int ii = 0; ii < activeNodes; ++ii) {
            twoPcInfo_.twoPcStateCompletions_[index][ii] = MsgTypeId::MsgNull;
        }
    } else {
        twoPcInfo_.twoPcStateCompletions_[index][remoteNodeId] =
            MsgTypeId::MsgNull;
    }

    // twoPcStateCompletions_ are not managed under a mutex
    memBarrier();

    // Update global index to randomize allocations.
    if (index + 1 == TwoPcMaxOutstandingCmds) {
        twoPcInfo_.twoPcCmdNextAvailIndex_ = 0;
    } else {
        twoPcInfo_.twoPcCmdNextAvailIndex_ = index + 1;
    }

    twoPcInfo_.twoPcCmdSlotLock_.lock();
    assert(twoPcInfo_.twoPcCmdLock_[index] == TwoPcCmdLocked);
    twoPcInfo_.twoPcCmdLock_[index] = TwoPcCmdFree;
    twoPcInfo_.twoPcCmdSlotLock_.unlock();
    twoPcInfo_.twoPcCmdSem_.post();
}

// twoPcIsCmdComplete() can only be invoked on the node where the
// twoPc() originated. An async cmd can be polled using this function.
// When it finds the cmd it does a cleanout of the cmd state from
// the twoPcStateCompletions_[][] table and returns true. It
// does not do a cleanout when it returns false. If cmds are not cleaned out
// by the user in TwoPcCmdTimeout then they are automatically cleaned up
// by the microkernel.
bool
MsgMgr::twoPcIsCmdComplete(TwoPcHandle twoPcHandle, uint64_t numNodes)
{
    uint32_t index = twoPcHandle.index;
    uint32_t originNodeId = twoPcHandle.originNodeId;
    MsgTypeId msgTypeId = (MsgTypeId) twoPcHandle.msgTypeId;
    bool retval = true;
    Config *config = Config::get();

    assert(twoPcHandle.sync == TwoPcAsyncCmd);
    assert(index < TwoPcMaxOutstandingCmds);
    assert(originNodeId < (uint32_t) config->getActiveNodes());

    uint32_t myNodeId = config->getMyNodeId();
    assert(myNodeId == originNodeId);

    // Check if cmds from all nodes have completed.
    for (uint64_t ii = 0; ii < numNodes; ++ii) {
        if (twoPcInfo_.twoPcStateCompletions_[index][ii] !=
            (MsgTypeId)((uint32_t) msgTypeId + TwoPcCmdCompletionOffset)) {
            retval = false;
            break;
        }
    }

    if (retval) {
        // XXX If code is needed for error cleanout processing on the async
        //     path, it should go here.

        // Cmds from all nodes have completed.
        for (uint64_t ii = 0; ii < numNodes; ++ii) {
            twoPcInfo_.twoPcStateCompletions_[index][ii] = MsgTypeId::MsgNull;
        }

        // twoPcStateCompletions_ is not managed under a mutex
        memBarrier();

        // Update global index to randomize allocations.
        if (index + 1 == TwoPcMaxOutstandingCmds) {
            twoPcInfo_.twoPcCmdNextAvailIndex_ = 0;
        } else {
            twoPcInfo_.twoPcCmdNextAvailIndex_ = index + 1;
        }

        twoPcInfo_.twoPcCmdSlotLock_.lock();
        assert(twoPcInfo_.twoPcCmdLock_[index] == TwoPcCmdLocked);
        twoPcInfo_.twoPcCmdLock_[index] = TwoPcCmdFree;
        twoPcInfo_.twoPcCmdSlotLock_.unlock();
        twoPcInfo_.twoPcCmdSem_.post();
    }

    return retval;
}

void
MsgMgr::destroy()
{
    if (TwoPcMgr::get() != NULL) {
        TwoPcMgr::get()->destroy();
    }

    if (SchedMgr::get()) {
        SchedMgr::get()->destroy();
    }

    if (XidMgr::get()) {
        XidMgr::get()->destroy();
    }

    if (bcHandleMsgNormal_ != NULL) {
        BcHandle::destroy(&bcHandleMsgNormal_);
        bcHandleMsgNormal_ = NULL;
    }

    if (twoPcInfo_.twoPcNormalSem_ != NULL) {
        memAlignedFree(twoPcInfo_.twoPcNormalSem_);
        twoPcInfo_.twoPcNormalSem_ = NULL;
    }
    if (twoPcInfo_.twoPcNestedNormalSem_ != NULL) {
        memAlignedFree(twoPcInfo_.twoPcNestedNormalSem_);
        twoPcInfo_.twoPcNestedNormalSem_ = NULL;
    }
    if (bcHandleMsgAlt_ != NULL) {
        BcHandle::destroy(&bcHandleMsgAlt_);
        bcHandleMsgAlt_ = NULL;
    }
    delete twoPcInfo_.twoPcAltSem_;
    twoPcInfo_.twoPcAltSem_ = NULL;

    if (twoPcInfo_.twoPcReserveSem_ != NULL) {
        memAlignedFree(twoPcInfo_.twoPcReserveSem_);
        twoPcInfo_.twoPcReserveSem_ = NULL;
    }
    if (twoPcInfo_.twoPcNestedReserveSem_ != NULL) {
        memAlignedFree(twoPcInfo_.twoPcNestedReserveSem_);
        twoPcInfo_.twoPcNestedReserveSem_ = NULL;
    }
    if (bcHandleAtomic64_ != NULL) {
        BcHandle::destroy(&bcHandleAtomic64_);
        bcHandleAtomic64_ = NULL;
    }
    if (bcHandleTwoPcPages_ != NULL) {
        BcHandle::destroy(&bcHandleTwoPcPages_);
        bcHandleTwoPcPages_ = NULL;
    }

    if (sendRecvThdInfo_.receiveErrorMsgs_ != NULL) {
        memAlignedFree(sendRecvThdInfo_.receiveErrorMsgs_);
        sendRecvThdInfo_.receiveErrorMsgs_ = NULL;
    }

    memFree(instance->nodeLocalInfo_);
    instance->nodeLocalInfo_ = NULL;

    if (twoPcInfo_.twoPcCmdLock_ != NULL) {
        memAlignedFree(twoPcInfo_.twoPcCmdLock_);
        twoPcInfo_.twoPcCmdLock_ = NULL;
    }

    if (twoPcInfo_.twoPcStateCompletions_ != NULL) {
        for (uint32_t ii = 0; ii < TwoPcMaxOutstandingCmds; ++ii) {
            if (twoPcInfo_.twoPcStateCompletions_[ii] == NULL) {
                break;
            }
            memAlignedFree(twoPcInfo_.twoPcStateCompletions_[ii]);
        }

        memAlignedFree(twoPcInfo_.twoPcStateCompletions_);
        twoPcInfo_.twoPcStateCompletions_ = NULL;
    }

    txnLogs_.removeAll(&TxnLog::del);

    if (instance->procFsHelper_ != NULL) {
        delete instance->procFsHelper_;
        instance->procFsHelper_ = NULL;
    }

    delete instance;
    instance = NULL;
}

uint64_t
MsgMgr::bcMsgMessageBufSize()
{
    return sizeof(MsgMessage);
}

Status
MsgMgr::init()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();
    Config *config = Config::get();
    unsigned numActiveNodes = config->getActiveNodes();
    int ret = 0;
    SendRecvThreadInfo *sendRecvThreadInfo = NULL;
    TwoPcInfo *twoPcInfo = NULL;

    assert(instance == NULL);
    instance = new (std::nothrow) MsgMgr();
    if (instance == NULL) {
        return StatusNoMem;
    }

    instance->procFsHelper_ = new (std::nothrow) XcProcFsHelper();
    BailIfNull(instance->procFsHelper_);

    status = instance->procFsHelper_->initProcFsHelper();
    BailIfFailed(status);

    // Record the System free resources (Memory + Swap) as well as the
    // corresponding time here.
    ret = clock_gettime(CLOCK_MONOTONIC_COARSE,
                        &instance->lastUpdatedSysFreeResSize_);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    status = instance->procFsHelper_->getSysFreeResourceSize(
        &instance->cachedSysFreeResSize_);
    BailIfFailed(status);

    // Also decide the system resource threshold based on number of usrnodes
    // on the same physical server.
    instance->sysFreeResThreshold_ =
        xcMin(MinSysFreeResourceSize,
              (instance->cachedSysFreeResSize_ * MinSysFreeResPct) /
                  (config->getActiveNodesOnMyPhysicalNode() * 100));
    instance->sysFreeResHealThreshold_ =
        instance->sysFreeResThreshold_ +
        (instance->sysFreeResThreshold_ * SysFreeResPctOverThreshToSelfHeal) /
            100;

    // Initialize SendRecvThreadInfo
    // Receive side.
    sendRecvThreadInfo = &instance->sendRecvThdInfo_;
    twoPcInfo = &instance->twoPcInfo_;
    memZero(&sendRecvThreadInfo->recvThreadId_,
            sizeof(sendRecvThreadInfo->recvThreadId_));
    sendRecvThreadInfo->recvThreadCreated_ = false;
    sendRecvThreadInfo->recvThrStatus_ = StatusOk;
    sendRecvThreadInfo->recvThreadReady_ = 0;
    memZero(&sendRecvThreadInfo->recvThreadParams_, sizeof(MsgThreadParams));

    // Receive side error handling.
    sendRecvThreadInfo->receiveErrorMsgs_ =
        (MsgMessage *) memAllocAlignedExt(CpuCacheLineSize,
                                          sizeof(MsgMessage) *
                                              MaxOutstandingRecvErrs,
                                          moduleName);
    BailIfNull(sendRecvThreadInfo->receiveErrorMsgs_);
    memZero(sendRecvThreadInfo->receiveErrorMsgs_,
            sizeof(MsgMessage) * MaxOutstandingRecvErrs);

    ret = sem_init(&sendRecvThreadInfo->errRecvSem_, 0, 0);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        goto CommonExit;
    }

    // Send side.
    for (uint32_t ii = 0; ii < MaxNodes; ++ii) {
        memZero(&sendRecvThreadInfo->sendFd_[ii],
                sizeof(sendRecvThreadInfo->sendFd_[0]));
        memZero(&sendRecvThreadInfo->sendThreadId_[ii],
                sizeof(sendRecvThreadInfo->sendThreadId_[0]));
        sendRecvThreadInfo->sendThreadCreated_[ii] = false;
    }
    sendRecvThreadInfo->sendThreadsReady_ = 0;

    twoPcInfo->dummyTwoPcHandle_.twoPcHandle = 0;

    twoPcInfo->twoPcStateCompletions_ =
        (MsgTypeId **) memAllocAlignedExt(CpuCacheLineSize,
                                          TwoPcMaxOutstandingCmds *
                                              sizeof(MsgTypeId *),
                                          moduleName);
    BailIfNull(twoPcInfo->twoPcStateCompletions_);
    memZero(twoPcInfo->twoPcStateCompletions_,
            TwoPcMaxOutstandingCmds * sizeof(MsgTypeId *));

    for (uint32_t ii = 0; ii < TwoPcMaxOutstandingCmds; ++ii) {
        twoPcInfo->twoPcStateCompletions_[ii] =
            (MsgTypeId *) memAllocAlignedExt(CpuCacheLineSize,
                                             MaxNodes * sizeof(MsgTypeId),
                                             moduleName);
        BailIfNull(twoPcInfo->twoPcStateCompletions_[ii]);
    }

    twoPcInfo->twoPcCmdLock_ = (TwoPcCmdProperty *)
        memAllocAlignedExt(CpuCacheLineSize,
                           TwoPcMaxOutstandingCmds *
                               sizeof(*twoPcInfo->twoPcCmdLock_),
                           moduleName);
    BailIfNull(twoPcInfo->twoPcCmdLock_);

    for (uint32_t ii = 0; ii < MaxNodes; ++ii) {
        // Initialize twoPC cmd arrays.
        for (uint32_t jj = 0; jj < TwoPcMaxOutstandingCmds; ++jj) {
            twoPcInfo->twoPcCmdLock_[jj] = TwoPcCmdFree;
            twoPcInfo->twoPcStateCompletions_[jj][ii] = MsgTypeId::MsgNull;
        }
    }

    // Create buf$s
    instance->bcHandleMsgNormal_ =
        BcHandle::create(BufferCacheObjects::UkMsgNormal);
    BailIfNull(instance->bcHandleMsgNormal_);

    instance->twoPcInfo_.twoPcNormalSem_ =
        (Semaphore *) memAllocAlignedExt(CpuCacheLineSize,
                                         sizeof(Semaphore),
                                         moduleName);
    BailIfNull(instance->twoPcInfo_.twoPcNormalSem_);
    new (instance->twoPcInfo_.twoPcNormalSem_)
        Semaphore(SteadyStateConcurrency);

    instance->twoPcInfo_.twoPcNestedNormalSem_ =
        (Semaphore *) memAllocAlignedExt(CpuCacheLineSize,
                                         sizeof(Semaphore),
                                         moduleName);
    BailIfNull(instance->twoPcInfo_.twoPcNestedNormalSem_);
    new (instance->twoPcInfo_.twoPcNestedNormalSem_)
        Semaphore(SteadyStateNestingConcurrency);

    instance->twoPcInfo_.twoPcReserveSem_ =
        (Semaphore *) memAllocAlignedExt(CpuCacheLineSize,
                                         sizeof(Semaphore),
                                         moduleName);
    BailIfNull(instance->twoPcInfo_.twoPcReserveSem_);
    new (instance->twoPcInfo_.twoPcReserveSem_) Semaphore(ReserveConcurrency);

    instance->twoPcInfo_.twoPcNestedReserveSem_ =
        (Semaphore *) memAllocAlignedExt(CpuCacheLineSize,
                                         sizeof(Semaphore),
                                         moduleName);
    BailIfNull(instance->twoPcInfo_.twoPcNestedReserveSem_);
    new (instance->twoPcInfo_.twoPcNestedReserveSem_)
        Semaphore(ReserveNestingConcurrency);

    instance->bcHandleMsgAlt_ = BcHandle::create(BufferCacheObjects::UkMsgAlt);
    BailIfNull(instance->bcHandleMsgAlt_);

    instance->twoPcInfo_.twoPcAltSem_ =
        new (std::nothrow) Semaphore(getAltMsgCount(numActiveNodes));
    BailIfNull(instance->twoPcInfo_.twoPcAltSem_);

    instance->nodeLocalInfo_ =
        (NodeLocalInfo *) memAlloc(sizeof(NodeLocalInfo) * numActiveNodes);
    BailIfNull(instance->nodeLocalInfo_);
    memZero(instance->nodeLocalInfo_, sizeof(NodeLocalInfo) * numActiveNodes);
    instance->nodeLocalInfo_[config->getMyNodeId()].numCores =
        (unsigned) XcSysHelper::get()->getNumOnlineCores();

    instance->bcHandleAtomic64_ =
        BcHandle::create(BufferCacheObjects::UkMsgAtomic64);
    BailIfNull(instance->bcHandleAtomic64_);

    // Note: TwoPcMaxOutstandingCmds and TwoPcNumCmdIndexBitsInHandle
    // must be maintained together as the number of bits must be able to index
    // the highest cmd without overflow.
    assert(TwoPcMaxOutstandingCmds <= (1 << TwoPcNumCmdIndexBitsInHandle));
    assert((uint32_t) MsgTypeId::MsgTypeIdLastEnum -
               (uint32_t) MsgTypeId::MsgTypeIdFirstEnum <=
           TwoPcMaxOutstandingCmds);

    xSyslog(moduleName,
            XlogDebug,
            "MsgTypeId enums %d",
            (uint32_t) MsgTypeId::MsgTypeIdLastEnum -
                (uint32_t) MsgTypeId::MsgTypeIdFirstEnum);

    status =
        statsLib->initNewStatGroup("uk.msg", &instance->statsGrpId_, StatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->recdMessagesStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "recdMessages",
                                         instance->recdMessagesStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->recdMessagesErrStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "recdMessagesErr",
                                         instance->recdMessagesErrStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->ooMessagesStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "outOfMessages",
                                         instance->ooMessagesStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->ooPayloadStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "outOfPayload",
                                         instance->ooPayloadStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->bigMessagesStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "bigMessages",
                                         instance->bigMessagesStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->sentMessagesSucceededStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "messages.sent.succeeded",
                                         instance->sentMessagesSucceededStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->sentMessagesFailedStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "messages.sent.failed",
                                         instance->sentMessagesFailedStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->ooTwoPcCmdsCumWaitCntStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "outOfTwoPcCmdsStat",
                                         instance->ooTwoPcCmdsCumWaitCntStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->ooTwoPcCmdsCumWaitUsecStat_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "outOfTwoPcCmdsCumWaitUsec",
                                         instance->ooTwoPcCmdsCumWaitUsecStat_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->acksScheduled_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "acksScheduled",
                                         instance->acksScheduled_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->kickedTxnAbort_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "kickedTxnAbort",
                                         instance->kickedTxnAbort_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->outOfFiberSched_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "outOfFiberSched",
                                         instance->outOfFiberSched_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status =
        statsLib->initStatHandle(&instance->outOfFiberSchedFsmTransportPage_);
    BailIfFailed(status);
    status =
        statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                    "outOfFiberSchedFsmTransportPage",
                                    instance->outOfFiberSchedFsmTransportPage_,
                                    StatUint64,
                                    StatAbsoluteWithNoRefVal,
                                    StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->outOfFiberSchedMessageObject_);
    BailIfFailed(status);
    status =
        statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                    "outOfFiberSchedMessageObject",
                                    instance->outOfFiberSchedMessageObject_,
                                    StatUint64,
                                    StatAbsoluteWithNoRefVal,
                                    StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->resourceLevelLow_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "resourceLevelLow",
                                         instance->resourceLevelLow_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->recvDataFromSocketCounter_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "recvDataFromSocketCounter",
                                         instance->recvDataFromSocketCounter_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);
    status = statsLib->initStatHandle(&instance->timeSpentUsToRecvData_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->statsGrpId_,
                                         "timeSpentUsToRecvData",
                                         instance->timeSpentUsToRecvData_,
                                         StatUint64,
                                         StatCumulative,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    instance->bcHandleTwoPcPages_ =
        BcHandle::create(BufferCacheObjects::UkMsgTwoPcPages);
    BailIfNull(instance->bcHandleTwoPcPages_);

    assert(XdbMgr::bcSize() ==
           (size_t) instance->bcHandleTwoPcPages_->getBufSize());

    status = XidMgr::init();
    BailIfFailed(status);

    status = SchedMgr::init();
    BailIfFailed(status);

    status = TwoPcMgr::init();
    BailIfFailed(status);

    // Global visibility for all initializations
    memBarrier();

CommonExit:
    if (status != StatusOk) {
        // XXX Cleanup stats.
        instance->destroy();
    }

    return status;
}

void
MsgMgr::freeAndEnqueueMsg(MsgMessage *msg)
{
    assert((msg->msgHdr.creator > MsgCreatorLowBound) &&
           (msg->msgHdr.creator < MsgCreatorHighBound));
    assert(msg->msgHdr.creator != MsgCreatorFreed);

    msg->msgHdr.creator = MsgCreatorFreed;

    freePayload(msg);

    freeMsgOnly(msg);
}

void
MsgMgr::freePayload(MsgMessage *msg)
{
    switch (msg->msgHdr.payloadBufCacheType) {
    case XdbBufCacheForPayload:
        assert(msg->msgHdr.twoPcFastOrSlowPath == TwoPcFastPath);
        if (msg->payload != NULL) {
            bcHandleTwoPcPages_->freeBuf(msg->payload);
            msg->payload = NULL;
        }
        break;

    case MallocedBufForPayload:
        assert((msg->msgHdr.twoPcFastOrSlowPath == TwoPcSlowPath) ||
               (msg->msgHdr.eph.payloadLengthTwoPcToAllocate));
        if (msg->payload != NULL) {
            memFree(msg->payload);
            msg->payload = NULL;
        }
        break;

    case TransportPagesForPayload:
    case CallerBufForPayload:
        // This is the twoPcAlc() immediate_ case where all work is done on
        // the local node.
        // It is the twoPc() caller's responsibility to free the payload.
        assert(msg->msgHdr.payloadImmutable);
        break;

    case NoPayload:
        if (msg->msgHdr.payloadBufCacheTypeImmutable == CallerBufForPayload) {
            // This message is coming from a remote node.
            assert(msg->msgHdr.payloadImmutable);
        }
        break;

    default:
        assert(0);
    }
}

void
MsgMgr::freeMsgOnly(MsgMessage *msg)
{
    // Free msg
    switch (msg->msgHdr.msgBufType) {
    case NormalMsg:
        bcHandleMsgNormal_->freeBuf(msg);
        break;

    case AltMsg:
        bcHandleMsgAlt_->freeBuf(msg);
        break;

    case NormalCopyMsg:  // Pass Through
    case AltCopyMsg:     // Pass Though
        // NOOP
        break;

    default:
        assert(0);
    }
}

void
MsgMgr::checkPayloadLengthAndDisableSendRecvFlags(MsgMessage *msg)
{
    if ((msg->msgHdr.eph.payloadLength == 0) &&
        (msg->msgHdr.msgSendRecvFlags & MsgRecvHdrPlusPayload)) {
        msg->msgHdr.msgSendRecvFlags = (MsgSendRecvFlags)(
            ((msg->msgHdr.msgSendRecvFlags) & ~(MsgRecvHdrPlusPayload)) |
            (MsgRecvHdrOnly));
        if (msg->payload != NULL) {
            freePayload(msg);
        }
        if ((msg->msgHdr.payloadBufCacheType == XdbBufCacheForPayload) ||
            (msg->msgHdr.payloadBufCacheType == MallocedBufForPayload)) {
            msg->msgHdr.payloadBufCacheType = NoPayload;
        }
        msg->ukInternalPayloadLen = 0;
    }
}

void
MsgMgr::ackAndFree(MsgMessage *msg)
{
    checkPayloadLengthAndDisableSendRecvFlags(msg);

#ifdef DEBUG
    NodeId myNodeId = Config::get()->getMyNodeId();
    assert(myNodeId != msg->msgHdr.srcNodeId);
    MsgSendRecvFlags msgSendRecvFlags = msg->msgHdr.msgSendRecvFlags;
    if (msgSendRecvFlags & MsgRecvHdrPlusPayload) {
        // Ensure that outgoing msg has payload.
        assert(msg->payload != NULL);
    } else {
        // All we are sending back is a hdr.
        assert(msgSendRecvFlags & MsgRecvHdrOnly);
    }
#endif

    // Since the source of this io was remote, send back an ack.
    MsgTypeId msgTypeId =
        (MsgTypeId)((int) msg->msgHdr.typeId + TwoPcCmdCompletionOffset);
    uint32_t statIndex = getStatIndex(msg->msgHdr.eph.twoPcFuncIndex);
    msg->msgHdr.typeId = msgTypeId;
    Status status = sendMsg(msg->msgHdr.srcNodeId, msg, DestToSource);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "failed to send ACK from node %d to node %d: %s",
                msg->msgHdr.srcNodeId,
                msg->msgHdr.dstNodeId,
                strGetFromStatus(status));
        // @SymbolCheckIgnore
        panicNoCore("ackAndFree: unable to send ACK to remote node");
    }

    // XXX - Need to handle this error scenario correctly.

    StatsLib::statNonAtomicIncr(
        TwoPcMgr::get()->numRemote2PCFinished_[statIndex]);
    if (msg->msgHdr.eph.status != StatusOk) {
        StatsLib::statNonAtomicIncr(
            TwoPcMgr::get()->numRemote2PCErrors_[statIndex]);
    }

    // Stick message back onto its free list
    freeAndEnqueueMsg(msg);
}

// Initialize all the dedicated connections from this node to every other node.
// Create a thread for each dedicated connection.
Status
MsgMgr::initSendConnections()
{
    const char *ipAddr;
    Status status = StatusOk;
    int port;
    Config *config = Config::get();

    for (unsigned ii = 0; ii < config->getActiveNodes(); ++ii) {
        if (ii != config->getMyNodeId()) {
            ipAddr = config->getIpAddr(ii);
            port = config->getPort(ii);

            strlcpy(sendRecvThdInfo_.sendThreadParams_[ii].ipAddr,
                    ipAddr,
                    sizeof(sendRecvThdInfo_.sendThreadParams_[ii].ipAddr));

            sendRecvThdInfo_.sendThreadParams_[ii].port = port;
            sendRecvThdInfo_.sendThreadParams_[ii].nodeId = ii;

            // Create threads that connect to the send ports of each node
            status =
                Runtime::get()
                    ->createBlockableThread(&sendRecvThdInfo_.sendThreadId_[ii],
                                            NULL,
                                            sendQueuedMessages,
                                            (void *) &sendRecvThdInfo_
                                                .sendThreadParams_[ii]);
            if (status != StatusOk) {
                status = StatusThrCreateFailed;
                break;
            }
            sendRecvThdInfo_.sendThreadCreated_[ii] = true;
        } else {
            issueLocalIos(NULL);
        }
    }
    return status;
}

// Wait for messages from any node. This is the function called by users of the
// message lib. It creates a thread which waits for messages from other nodes.
Status
MsgMgr::wait()
{
    Config *config = Config::get();
    const char *ipAddr = config->getIpAddr(config->getMyNodeId());
    int port = config->getPort(config->getMyNodeId());
    Status status = StatusOk;

    strlcpy(sendRecvThdInfo_.recvThreadParams_.ipAddr,
            ipAddr,
            sizeof(sendRecvThdInfo_.recvThreadParams_.ipAddr));

    sendRecvThdInfo_.recvThreadParams_.port = port;

    status =
        Runtime::get()->createBlockableThread(&sendRecvThdInfo_.recvThreadId_,
                                              NULL,
                                              waitForMessagesActual,
                                              (void *) &sendRecvThdInfo_
                                                  .recvThreadParams_);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Receive thread creation failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "waitForMessages::myNodeId %d ipAddr %s port %d tid %llu",
            config->getMyNodeId(),
            (char *) &sendRecvThdInfo_.recvThreadParams_.ipAddr,
            sendRecvThdInfo_.recvThreadParams_.port,
            (unsigned long long) pthread_self());
    sendRecvThdInfo_.recvThreadCreated_ = true;

    status = Runtime::get()
                 ->createBlockableThread(&sendRecvThdInfo_.recvErrorThreadId_,
                                         NULL,
                                         processReceiveErrorAck,
                                         (void *) 0);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Receive error thread creation failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }
    sendRecvThdInfo_.recvErrorThreadCreated_ = true;

CommonExit:
    return status;
}

// Establish connection to a node for sending messages
Status
MsgMgr::connectToNode(MsgThreadParams *threadParams)
{
    Status status;
    SocketAddr addr;
    bool socketCreated = false;
    uint64_t retryCount;

    const char *ipAddr = threadParams->ipAddr;
    int port = threadParams->port;

    // Connect to node
    retryCount = 0;
    do {
        // If sockConnect fails, fd is in an undefined state and we have
        // to create a new one
        if (socketCreated) {
            sockDestroy(&sendRecvThdInfo_.sendFd_[threadParams->nodeId]);
            socketCreated = false;
        }

        status = sockCreate(ipAddr,
                            (uint16_t) port,
                            SocketDomainUnspecified,
                            SocketTypeStream,
                            &sendRecvThdInfo_.sendFd_[threadParams->nodeId],
                            &addr);
        BailIfFailedMsg(moduleName,
                        status,
                        "connectToNode NodeId %u, "
                        "ipAddr %s, port %d failed: %s",
                        threadParams->nodeId,
                        ipAddr,
                        port,
                        strGetFromStatus(status));
        socketCreated = true;

        status = sockSetOption(sendRecvThdInfo_.sendFd_[threadParams->nodeId],
                               SocketOptionKeepAlive);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to set keepalive flag on socket %d: %s",
                    sendRecvThdInfo_.sendFd_[threadParams->nodeId],
                    strGetFromStatus(status));
            goto CommonExit;
        }

        status =
            sockConnect(sendRecvThdInfo_.sendFd_[threadParams->nodeId], &addr);
        // XXX - fix; wait a bit for the other node to come up
        ++retryCount;
        sysUSleep(MsgConnectSleepUSec);
    } while (status != StatusOk && retryCount < MsgConnectRetryCount);

    xSyslog(moduleName,
            XlogDebug,
            "connectToNode::NodeId %d, idAddr %s port %d",
            threadParams->nodeId,
            ipAddr,
            port);

    BailIfFailedMsg(moduleName,
                    status,
                    "sockConnect() failed: %s",
                    strGetFromStatus(status));

CommonExit:
    if (status != StatusOk) {
        if (socketCreated) {
            sockDestroy(&sendRecvThdInfo_.sendFd_[threadParams->nodeId]);
            socketCreated = false;
        }
    }

    return status;
}

// Send message to a node.
// Note that even though sendmsg on socket is MT_SAFE, it simply guarantees that
// the data in a particular call to sendmsg() will arrive on the correct socket.
// It does not mean that another send side thread may not slip a few bytes
// in-between on sendmsg(). The sendSerializeLock_[MAX_NODES] serializes
// sendmsg() call across send side threads to a particular destination node.
Status
MsgMgr::sendMsg(NodeId nodeId, MsgMessage *msg, AckDirection ackDirection)
{
    Status status;
    Config *config = Config::get();
    SgArray *sgArray = NULL;

    assert(nodeId != config->getMyNodeId());
    assert(nodeId < config->getActiveNodes());

#ifdef DEBUG
    if (msg->msgHdr.twoPcFastOrSlowPath == TwoPcFastPath) {
        assert(msg->msgHdr.eph.payloadLength <= XdbMgr::bcSize());
    } else {
        assert(msg->msgHdr.twoPcFastOrSlowPath == TwoPcSlowPath);
    }
#endif  // DEBUG

    void *savedPayload = msg->payload;
    PayloadBufCacheType savedPayloadBufCacheType =
        msg->msgHdr.payloadBufCacheType;
    TwoPcFastOrSlowPath savedTwoPcFastOrSlowPath =
        msg->msgHdr.twoPcFastOrSlowPath;

    MsgSendRecvFlags msgSendRecvFlags = msg->msgHdr.msgSendRecvFlags;
    bool gotPayload;

    if (ackDirection == DestToSource) {
        if (msgSendRecvFlags & MsgRecvHdrOnly) {
            gotPayload = false;
        } else {
            assert(msgSendRecvFlags & MsgRecvHdrPlusPayload);
            gotPayload = true;
        }
    } else {
        assert(ackDirection == SourceToDest);

        if (msgSendRecvFlags & MsgSendHdrOnly) {
            gotPayload = false;
        } else {
            assert(msgSendRecvFlags & MsgSendHdrPlusPayload);
            gotPayload = true;
        }
    }

    if (gotPayload) {
        // Vector io. This must be a multiple of SectorSize.
        sgArray = (SgArray *) memAlloc(sizeof(SgArray) +
                                       sizeof(SgElem) * MsgTwoFields);
        BailIfNull(sgArray);

        sgArray->numElems = MsgTwoFields;
        sgArray->elem[0].baseAddr = &msg->msgHdr;
        sgArray->elem[0].len = sizeof(msg->msgHdr);
        sgArray->elem[1].baseAddr = msg->payload;

        // check whether we were provided a payload or we allocated one
        if (msg->msgHdr.eph.payloadLength == 0) {
            sgArray->elem[1].len = msg->msgHdr.eph.payloadLengthTwoPcToAllocate;
        } else {
            sgArray->elem[1].len = msg->msgHdr.eph.payloadLength;
        }

        // This is to ensure robustness. Annul payload as it is
        // meaningless on the destination node.
        msg->payload = NULL;
        msg->msgHdr.payloadBufCacheType = NoPayload;
        msg->ukInternalPayloadLen = 0;

        // sendMsg() is serialized.
        int flags;
        if (XcalarConfig::get()->nonBlockingSocket_) {
            flags = msg->msgHdr.payloadBufCacheType == TransportPagesForPayload
                        ? MSG_DONTWAIT
                        : 0;
        } else {
            flags = 0;
        }

        verify(pthread_mutex_lock(
                   &sendRecvThdInfo_.sendSerializeLock_[nodeId]) == 0);
        status = sendVector(sendRecvThdInfo_.sendFd_[nodeId], sgArray, flags);
        if (status == StatusOk) {
            StatsLib::statNonAtomicIncr(sentMessagesSucceededStat_);
        } else {
            StatsLib::statNonAtomicIncr(sentMessagesFailedStat_);
        }
        verify(pthread_mutex_unlock(
                   &sendRecvThdInfo_.sendSerializeLock_[nodeId]) == 0);

        msg->payload = savedPayload;
        msg->msgHdr.payloadBufCacheType = savedPayloadBufCacheType;
        msg->msgHdr.twoPcFastOrSlowPath = savedTwoPcFastOrSlowPath;

    } else {
        assert((msg->msgHdr.msgSendRecvFlags & MsgSendHdrOnly) ||
               (msg->msgHdr.msgSendRecvFlags & MsgRecvHdrOnly));

        // This is to ensure robustness. Annul payload as it is
        // meaningless on the destination node.
        msg->payload = NULL;
        msg->msgHdr.payloadBufCacheType = NoPayload;
        msg->ukInternalPayloadLen = 0;

        verify(pthread_mutex_lock(
                   &sendRecvThdInfo_.sendSerializeLock_[nodeId]) == 0);
        status = sockSendConverged(sendRecvThdInfo_.sendFd_[nodeId],
                                   &msg->msgHdr,
                                   sizeof(msg->msgHdr));

        if (status == StatusOk) {
            StatsLib::statNonAtomicIncr(sentMessagesSucceededStat_);
        } else {
            StatsLib::statNonAtomicIncr(sentMessagesFailedStat_);
        }
        verify(pthread_mutex_unlock(
                   &sendRecvThdInfo_.sendSerializeLock_[nodeId]) == 0);

        msg->payload = savedPayload;
        msg->msgHdr.payloadBufCacheType = savedPayloadBufCacheType;
        msg->msgHdr.twoPcFastOrSlowPath = savedTwoPcFastOrSlowPath;
    }

CommonExit:
    if (sgArray != NULL) {
        memFree(sgArray);
        sgArray = NULL;
    }
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "sockSendV() nodeId %u failed: %s",
                nodeId,
                strGetFromStatus(status));
    }
    return status;
}

int
MsgMgr::spinUntilRecvThreadReady()
{
    int threadReadyTemp = sendRecvThdInfo_.recvThreadReady_;

    while (!threadReadyTemp) {
        // Waiting for recv thread to be ready
        xSyslog(moduleName,
                XlogInfo,
                "spinUntilRecvThreadReady::Waiting for recv thread to be "
                "ready...");

        sendRecvThdInfo_.threadsReadyLock_.lock();
        threadReadyTemp = sendRecvThdInfo_.recvThreadReady_;
        sendRecvThdInfo_.threadsReadyLock_.unlock();

        sysSleep(1);
    }

    return threadReadyTemp;
}

Status
MsgMgr::spinUntilSendThreadsReady()
{
    Status status = StatusOk;
    int threadReadyTemp = sendRecvThdInfo_.sendThreadsReady_;
    int64_t numRetries = 0;
    int64_t numSecsWaited = 0;

    while (threadReadyTemp < (int) Config::get()->getActiveNodes() &&
           threadReadyTemp >= 0) {
        sendRecvThdInfo_.threadsReadyLock_.lock();
        threadReadyTemp = sendRecvThdInfo_.sendThreadsReady_;
        sendRecvThdInfo_.threadsReadyLock_.unlock();

        // Waiting for send threads to be ready
        if ((int64_t)(numRetries * MsgConnectSleepUSec / USecsPerSec) >
            numSecsWaited) {
            xSyslog(moduleName,
                    XlogInfo,
                    "spinUntilSendThreadsReady:Waiting for send threads to be "
                    "ready...%d",
                    threadReadyTemp);
            ++numSecsWaited;
        }

        sysUSleep(MsgConnectSleepUSec);
        ++numRetries;
    }

    if (threadReadyTemp < 0) {
        return StatusSendSocketFail;
    }

    return status;
}

// Wait for all the send threads to exit. Note that we only spawn a dedicated
// send thread for each remote node.
void
MsgMgr::spinUntilSendThreadsExit()
{
    Config *config = Config::get();
    unsigned activeNodeCount = config->getActiveNodes();

    for (unsigned ii = 0; ii < activeNodeCount; ++ii) {
#ifdef DEBUG
        if ((unsigned) sendRecvThdInfo_.sendThreadsReady_ == activeNodeCount &&
            ii != config->getMyNodeId()) {
            assert(sendRecvThdInfo_.sendThreadCreated_[ii]);
        }
#endif  // DEBUG
        if (ii != config->getMyNodeId() &&
            sendRecvThdInfo_.sendThreadCreated_[ii]) {
            // XXX - fix, check return value of sysThreadJoin()
            sysThreadJoin(sendRecvThdInfo_.sendThreadId_[ii], NULL);
        }
    }
}

void
MsgMgr::spinUntilRecvThreadExit()
{
    if (sendRecvThdInfo_.recvThreadReady_ ||
        sendRecvThdInfo_.recvThreadCreated_) {
// Wait for the recv thread to exit.
#ifdef DEBUG
        if (sendRecvThdInfo_.recvThreadReady_) {
            assert(sendRecvThdInfo_.recvThreadCreated_);
        }
#endif  // DEBUG
        sysThreadJoin(sendRecvThdInfo_.recvThreadId_, NULL);
    }

    if (sendRecvThdInfo_.recvErrorThreadCreated_ == true) {
        sysThreadJoin(sendRecvThdInfo_.recvErrorThreadId_, NULL);
    }
}

void
MsgMgr::updateMsgLocalTwoPcState(MsgMessage *msg, MsgTypeId state)
{
    if (msg->msgHdr.twoPcCmdType == TwoPcAltCmd) {
        verify(msg->msgHdr.twoPcHandle.twoPcHandle == TwoPcAltHandle);
    } else {
        Config *config = Config::get();
        uint32_t index = msg->msgHdr.twoPcHandle.index;
        assert(index < TwoPcMaxOutstandingCmds);
        assert(config->getMyNodeId() < MaxNodes);

        twoPcInfo_.twoPcStateCompletions_[index][config->getMyNodeId()] = state;

        // twoPcStateCompletions_ are not manage under a mutex - nor should they
        // be
        memBarrier();

        Semaphore *wakeupSem = (Semaphore *) msg->msgHdr.wakeupSem;

        if (wakeupSem) {
            if (atomicDec32(msg->msgHdr.outstandingMessages) == 0) {
                // Only sync cmds are posted
                wakeupSem->post();
            }
        }
    }
}

// Find a free message from the list. This waits until a message can be
// alloc-ed. Return parameter is **msg.
// XXX - fix, payloadLength needs to be used to alloc a buffer of the right
// size rather than asserting after allocating the buffer.
Status
MsgMgr::allocMsg(MsgMessage **msgIn,
                 MsgCreator creator,
                 PayloadBufCacheType payloadBufCacheType,
                 size_t payloadLength,
                 MsgBufType msgBufType)
{
    MsgMessage *msg = NULL;
    Status status = StatusOk;

    // Alloc buf for msg
    if (msgBufType == NormalMsg) {
        msg = (MsgMessage *) bcHandleMsgNormal_->allocBuf(XidInvalid, &status);
    } else if (msgBufType == AltMsg) {
        msg = (MsgMessage *) bcHandleMsgAlt_->allocBuf(XidInvalid, &status);
    } else {
        assert(0);
        status = StatusInval;
        goto CommonExit;
    }

    *msgIn = msg;

    // Now alloc buf for payload if needed
    if (msg) {
        msg->msgHdr.creator = creator;
        msg->msgHdr.msgBufType = msgBufType;

        switch (payloadBufCacheType) {
        case XdbBufCacheForPayload:
            msg->ukInternalPayloadLen = bcHandleTwoPcPages_->getBufSize();
            msg->msgHdr.payloadBufCacheType = XdbBufCacheForPayload;
            msg->msgHdr.payloadBufCacheTypeImmutable =
                IgnorePayloadBufCacheType;
            msg->payload = bcHandleTwoPcPages_->allocBuf(XidInvalid, &status);
            if (msg->payload == NULL) {
                StatsLib::statAtomicIncr64(ooPayloadStat_);
                goto CommonExit;
            }
            break;

        case MallocedBufForPayload:
            // Outgoing or send payloadLength is always
            // MsgMaxPayloadSizeAbsolute or greater for now
            msg->ukInternalPayloadLen = payloadLength;
            msg->msgHdr.payloadBufCacheType = MallocedBufForPayload;
            msg->msgHdr.payloadBufCacheTypeImmutable =
                IgnorePayloadBufCacheType;
            msg->payload = memAlloc(payloadLength);
            if (msg->payload == NULL) {
                status = StatusNoMem;
            }
            break;

        case TransportPagesForPayload:
        case CallerBufForPayload:
            // Do not alloc a buf for payload as the caller will
            // manage msg->payload.
            msg->msgHdr.payloadBufCacheType = payloadBufCacheType;
            msg->msgHdr.payloadBufCacheTypeImmutable = payloadBufCacheType;
            msg->ukInternalPayloadLen = msg->msgHdr.eph.payloadLength;
            break;

        case NoPayload:
            msg->payload = NULL;
            msg->msgHdr.payloadBufCacheType = NoPayload;
            msg->msgHdr.payloadBufCacheTypeImmutable =
                IgnorePayloadBufCacheType;
            msg->ukInternalPayloadLen = 0;
            break;

        default:
            assert(0);
            status = StatusInval;
            goto CommonExit;
            break;
        }
    } else {
        StatsLib::statAtomicIncr64(ooMessagesStat_);
    }

CommonExit:
    if (status != StatusOk) {
        if (*msgIn != NULL) {
            freeMsgOnly(*msgIn);
            *msgIn = NULL;
        }
    }
    return status;
}

// nextTwoPcCmdHandle() returns a twoPcHandle to the 2pc cmd. There can at most
// be TwoPcMaxOutstandingCmds 2pc cmds in the system. The handle returned is
// a virtual handle into the cmd buffer and is node local. This is achieved
// by augmenting the node id to the xmd index in twoPcState array. For now the
// handle can only be used on the node where it is generated. It is the twoPc()
// caller's responsibility to invoke subsequent cmds following the initial
// twoPc() call using the same handle on the node where the original twoPc()
// call was issued. Distributed transactions can have follow-on actions that
// need to be tracked. The twoPcHandle allows the user to work with their
// original twoPc() call using this handle. nextResultSet() is a good
// example of this twoPcHandle used in subsequent distributed transactions.
// Since this code is slow path we just do a dumb scan of the array to search
// for the next handle.
TwoPcHandle
MsgMgr::nextTwoPcCmdHandle(MsgTypeId msgTypeId,
                           TwoPcCmdType twoPcCmdType,
                           TwoPcNodeProperty twoPcNodeProperty,
                           int remoteNodeId)
{
    Config *config = Config::get();

    int activeNodes = config->getActiveNodes();

    if (twoPcNodeProperty == TwoPcAllNodes) {
        assert((unsigned) remoteNodeId == TwoPcIgnoreNodeId);
    } else {
        assert(twoPcNodeProperty == TwoPcSingleNode);
        assert(remoteNodeId < activeNodes);
    }

    bool found = false, didIWait = false;
    uint32_t traversed = 0;
    uint32_t myNodeId = config->getMyNodeId();
    struct timespec startTime, endTime;
    int ret = clock_gettime(CLOCK_MONOTONIC_RAW, &startTime);
    assert(ret == 0);

    twoPcInfo_.twoPcCmdSlotLock_.lock();

    uint32_t index = twoPcInfo_.twoPcCmdNextAvailIndex_;

    while (!found) {
        if (twoPcInfo_.twoPcCmdLock_[index] == TwoPcCmdFree) {
            twoPcInfo_.twoPcCmdLock_[index] = TwoPcCmdLocked;

            for (uint32_t jj = 0; jj < (uint32_t) activeNodes; ++jj) {
                assert(twoPcInfo_.twoPcStateCompletions_[index][jj] ==
                       MsgTypeId::MsgNull);
            }

            found = true;
            break;
        }

        if (++index == TwoPcMaxOutstandingCmds) {
            index = 0;
        }

        if (++traversed == TwoPcMaxOutstandingCmds) {
            traversed = 0;

            if (didIWait == false) {
                StatsLib::statAtomicIncr64(ooTwoPcCmdsCumWaitCntStat_);
                xSyslog(moduleName,
                        XlogDebug,
                        "nextTwoPcCmdHandle::Out of twoPc cmds; reached max %d",
                        TwoPcMaxOutstandingCmds);
                didIWait = true;
            }
            twoPcInfo_.twoPcCmdSlotLock_.unlock();
            // XXX kkochis Change to Timed Semaphores.
            twoPcInfo_.twoPcCmdSem_.semWait();
            twoPcInfo_.twoPcCmdSlotLock_.lock();
        }
    }

    twoPcInfo_.twoPcCmdSlotLock_.unlock();

    if (index + 1 == TwoPcMaxOutstandingCmds) {
        twoPcInfo_.twoPcCmdNextAvailIndex_ = 0;
    } else {
        twoPcInfo_.twoPcCmdNextAvailIndex_ = index + 1;
    }

    ret = clock_gettime(CLOCK_MONOTONIC_RAW, &endTime);
    assert(ret == 0);
    StatsLib::statAtomicAdd64(ooTwoPcCmdsCumWaitUsecStat_,
                              clkGetElapsedTimeInNanosSafe(&startTime,
                                                           &endTime) /
                                  NSecsPerUSec);

    TwoPcHandle temp;
    temp.index = (unsigned char) index;
    temp.originNodeId = (unsigned char) myNodeId;
    temp.remoteNodeId = (unsigned char) remoteNodeId;
    temp.msgTypeId = (unsigned short int) msgTypeId;

    if (twoPcNodeProperty == TwoPcAllNodes) {
        temp.allNodes = TwoPcAllNodes;
    } else {
        temp.allNodes = TwoPcSingleNode;
    }

    if (twoPcCmdType == TwoPcSyncCmd) {
        temp.sync = TwoPcSyncCmd;
    } else {
        temp.sync = TwoPcAsyncCmd;
    }

    return temp;
}

void
MsgMgr::removeSendFlagOnPayloadAllocFailure(MsgMessage *msg)
{
    StatsLib::statAtomicIncr64(ooPayloadStat_);
    msg->msgHdr.payloadBufCacheType = NoPayload;
    msg->msgHdr.msgSendRecvFlags =
        (MsgSendRecvFlags)((((uint8_t) msg->msgHdr.msgSendRecvFlags) &
                            ~((uint8_t) MsgSendHdrPlusPayload)) |
                           ((uint8_t) MsgSendHdrOnly));
    msg->ukInternalPayloadLen = 0;
    msg->msgHdr.eph.payloadLength = 0;
}

void
MsgMgr::removeRecvFlagOnPayloadAllocFailure(MsgMessage *msg)
{
    StatsLib::statAtomicIncr64(ooPayloadStat_);
    msg->msgHdr.payloadBufCacheType = NoPayload;
    msg->msgHdr.msgSendRecvFlags =
        (MsgSendRecvFlags)((((uint8_t) msg->msgHdr.msgSendRecvFlags) &
                            ~((uint8_t) MsgRecvHdrPlusPayload)) |
                           ((uint8_t) MsgRecvHdrOnly));
    msg->ukInternalPayloadLen = 0;
    msg->msgHdr.eph.payloadLength = 0;
}

void
MsgMgr::removeRecvFlagAndReleasePayload(MsgMessage *msg)
{
    if (msg->payload != NULL) {
        assert(msg->ukInternalPayloadLen > 0);
        freePayload(msg);
        if ((msg->msgHdr.payloadBufCacheType == XdbBufCacheForPayload) ||
            (msg->msgHdr.payloadBufCacheType == MallocedBufForPayload)) {
            assert(msg->payload == NULL);
            msg->msgHdr.payloadBufCacheType = NoPayload;
        }
    }

    msg->msgHdr.eph.payloadLength = 0;
    msg->ukInternalPayloadLen = 0;

    msg->msgHdr.msgSendRecvFlags =
        (MsgSendRecvFlags)((((uint8_t) msg->msgHdr.msgSendRecvFlags) &
                            ~((uint8_t) MsgRecvHdrPlusPayload)) |
                           ((uint8_t) MsgRecvHdrOnly));
}

uint32_t
MsgMgr::getStatIndex(TwoPcCallId twoPcFuncIndex)
{
    uint32_t statIndex = (uint32_t) twoPcFuncIndex -
                         ((uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum + 1);
    assert(statIndex < TwoPcMgr::Num2PCs);

    return statIndex;
}

void
MsgMgr::process2pcCmd(SocketHandle sockHandle,
                      MsgMessage *msg,
                      MsgTypeId msgTypeId)
{
    Status status = StatusFailed;
    SchedMgr *schedMgr = SchedMgr::get();
    MsgSendRecvFlags msgSendRecvFlags = msg->msgHdr.msgSendRecvFlags;
    size_t payloadLength = msg->msgHdr.eph.payloadLength;
    assert(!msg->payload);
    bool failCmd = false;
    uint32_t statIndex = getStatIndex(msg->msgHdr.eph.twoPcFuncIndex);

    ResourceLevel resourceLevel = failOnResourceShortage(msg->msgHdr.typeId);
    if (resourceLevel != ResourceLevel::Normal &&
        resourceLevel != ResourceLevel::Reserve) {
        // Limit concurrency on the receive thread when running on
        // Reserve resource level.
        failCmd = true;
        StatsLib::statAtomicIncr64(resourceLevelLow_);
    }

    StatsLib::statNonAtomicIncr(TwoPcMgr::get()->numRemote2PCEntry_[statIndex]);

    // Now look at the msgHdr and allocate the buffer for the payload.
    if (msgSendRecvFlags & MsgSendHdrPlusPayload) {
        if (msg->msgHdr.twoPcFastOrSlowPath == TwoPcFastPath) {
            assert(payloadLength <= XdbMgr::bcSize());

            if (msg->msgHdr.payloadBufCacheTypeImmutable ==
                TransportPagesForPayload) {
                msg->msgHdr.payloadBufCacheType = TransportPagesForPayload;
                msg->ukInternalPayloadLen = XdbMgr::bcSize();
                msg->payload =
                    TransportPageMgr::get()->allocRecvTransportPage(&status);
            } else {
                msg->msgHdr.payloadBufCacheType = XdbBufCacheForPayload;
                msg->ukInternalPayloadLen = bcHandleTwoPcPages_->getBufSize();
                msg->payload = bcHandleTwoPcPages_->allocBuf(
                    XidInvalid, &status);
            }
        } else {
            assert(msg->msgHdr.twoPcFastOrSlowPath == TwoPcSlowPath);
            msg->msgHdr.payloadBufCacheType = MallocedBufForPayload;

            // Incoming or recv payloadLength is whatever is denoted in the
            // message.
            assert(msg->msgHdr.eph.payloadLengthTwoPcToAllocate == 0 ||
                   msg->msgHdr.eph.payloadLengthTwoPcToAllocate >=
                       payloadLength);
            msg->ukInternalPayloadLen =
                xcMax(xcMax(payloadLength, MsgMaxPayloadSizeAbsolute),
                      msg->msgHdr.eph.payloadLengthTwoPcToAllocate);

            if (failCmd) {
                msg->payload = NULL;
                status = StatusNoMem;
            } else {
                msg->payload = memAlloc(msg->ukInternalPayloadLen);
                if (msg->payload == NULL) {
                    status = StatusNoMem;
                }
            }
        }

        assert(payloadLength <= msg->ukInternalPayloadLen);
        if (msg->payload == NULL) {
            // Failed to alloc payload, so make sure to ACK caller and also
            // let the caller know that there is no payload to expect with the
            // ACK.
            removeRecvFlagOnPayloadAllocFailure(msg);

            // Drain the socket of payloadlength bytes.
            handleReceiveError(sockHandle,
                               msg,
                               payloadLength,
                               status,
                               ReceiveErrorOp::Ack);
            return;
        }

        status = sockRecvConverged(sockHandle, msg->payload, payloadLength);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "failed to receive payload on node %d from node %d: %s",
                    msg->msgHdr.dstNodeId,
                    msg->msgHdr.srcNodeId,
                    strGetFromStatus(status));
            // @SymbolCheckIgnore
            panicNoCore(
                "process2pcCmd: unable to receive payload from "
                "remote node");
        }

    } else {
        assert(msgSendRecvFlags & MsgSendHdrOnly);
        if (msg->msgHdr.eph.payloadLengthTwoPcToAllocate) {
            // This means that twoPc must internally allocate a payload
            // even though there is none coming into the call.
            assert(msg->msgHdr.eph.payloadLength == 0);
            msg->ukInternalPayloadLen =
                msg->msgHdr.eph.payloadLengthTwoPcToAllocate;

            msg->msgHdr.payloadBufCacheType = MallocedBufForPayload;
            if (failCmd) {
                msg->payload = NULL;
            } else {
                msg->payload = memAlloc(msg->ukInternalPayloadLen);
            }

            if (msg->payload == NULL) {
                // Failed to alloc payload, so make sure to ACK caller and also
                // let the caller know that there is no payload to expect with
                // the ACK.
                removeRecvFlagOnPayloadAllocFailure(msg);

                // Nothing to receive on the socket, so payload length is 0.
                handleReceiveError(sockHandle,
                                   msg,
                                   0,
                                   StatusNoMem,
                                   ReceiveErrorOp::Ack);
                return;
            }
        } else {
            assert(msg->msgHdr.payloadBufCacheType == NoPayload);
        }
    }

    // Stick into sched queue
    status = schedMgr->schedOneMessage(msg,
                                       true);  // Schedule on Runtime
    if (status != StatusOk) {
        removeRecvFlagAndReleasePayload(msg);
        // Nothing to receive on socket, so payload length is 0.
        handleReceiveError(sockHandle, msg, 0, status, ReceiveErrorOp::Ack);
    }
}

void
MsgMgr::process2PcCmdCompletePre(SocketHandle sockHandle,
                                 MsgMessage *msg,
                                 ZeroCopyType zeroCopy)
{
    MsgSendRecvFlags msgSendRecvFlags = msg->msgHdr.msgSendRecvFlags;
    size_t payloadLength;
    // check whether we were provided a payload or we allocated one
    if (msg->msgHdr.eph.payloadLength == 0) {
        payloadLength = msg->msgHdr.eph.payloadLengthTwoPcToAllocate;
    } else {
        payloadLength = msg->msgHdr.eph.payloadLength;
    }

    assert(!msg->payload);

    if (msgSendRecvFlags & MsgRecvHdrPlusPayload) {
        if (zeroCopy == ZeroCopy) {
            Status status = sockRecvConverged(sockHandle,
                                              msg->msgHdr.eph.ephemeral,
                                              payloadLength);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "failed to receive payload on node %d from node %d: %s",
                        msg->msgHdr.dstNodeId,
                        msg->msgHdr.srcNodeId,
                        strGetFromStatus(status));
                // @SymbolCheckIgnore
                panicNoCore(
                    "process2pcCmd (zero copy): unable to receive "
                    "payload from remote node");
            }

            // This is a weak check as there is no way to tell the size
            // of payload that the called allocated.
        } else {
            assert(zeroCopy == DoNotZeroCopy);

            if (msg->msgHdr.twoPcFastOrSlowPath == TwoPcFastPath) {
                assert(payloadLength <= XdbMgr::bcSize());
                msg->msgHdr.payloadBufCacheType = XdbBufCacheForPayload;
                msg->ukInternalPayloadLen = bcHandleTwoPcPages_->getBufSize();
                msg->payload = bcHandleTwoPcPages_->allocBuf(XidInvalid);
            } else {
                assert(msg->msgHdr.twoPcFastOrSlowPath == TwoPcSlowPath);

                // Incoming or recv payloadLength is whatever is denoted
                // in the message.
                if (payloadLength > MsgMaxPayloadSizeAbsolute) {
                    msg->ukInternalPayloadLen = payloadLength;
                    msg->payload = memAlloc(payloadLength);
                } else {
                    msg->ukInternalPayloadLen = MsgMaxPayloadSizeAbsolute;
                    msg->payload = memAlloc(MsgMaxPayloadSizeAbsolute);
                }
                msg->msgHdr.payloadBufCacheType = MallocedBufForPayload;
            }

            if (msg->payload == NULL) {
                removeSendFlagOnPayloadAllocFailure(msg);
                handleReceiveError(sockHandle,
                                   msg,
                                   payloadLength,
                                   StatusNoMem,
                                   ReceiveErrorOp::DontAck);
                return;
            }

            Status status =
                sockRecvConverged(sockHandle, msg->payload, payloadLength);
            if (status != StatusOk) {
                xSyslog(moduleName,
                        XlogErr,
                        "failed to receive payload on node %d from node %d: %s",
                        msg->msgHdr.dstNodeId,
                        msg->msgHdr.srcNodeId,
                        strGetFromStatus(status));
                // @SymbolCheckIgnore
                panicNoCore(
                    "process2pcCmd (not zero copy): unable to receive "
                    "payload from remote node");
            }
            assert(payloadLength <= msg->ukInternalPayloadLen);
        }
    } else {
        assert(msgSendRecvFlags & MsgRecvHdrOnly);
        assert(msg->msgHdr.payloadBufCacheType == NoPayload);
    }
}

void
MsgMgr::process2pcCmdCompletePost(MsgMessage *msg, MsgTypeId msgTypeId)
{
    uint32_t index = msg->msgHdr.twoPcHandle.index;
    if (msg->msgHdr.twoPcCmdType == TwoPcAltCmd) {
        verify(msg->msgHdr.twoPcHandle.twoPcHandle == TwoPcAltHandle);
    } else {
        assert(
            twoPcInfo_.twoPcStateCompletions_[index][msg->msgHdr.dstNodeId] ==
            MsgTypeId::MsgNull);

        twoPcInfo_.twoPcStateCompletions_[index][msg->msgHdr.dstNodeId] =
            msgTypeId;

        // twoPcStateCompletions_ is not managed under a mutex
        memBarrier();

        Semaphore *wakeupSem = (Semaphore *) msg->msgHdr.wakeupSem;

        if (wakeupSem) {
            if (atomicDec32(msg->msgHdr.outstandingMessages) == 0) {
                // Only sync cmds are posted
                wakeupSem->post();
            }
        }

        if (msg->msgHdr.twoPcCmdType == TwoPcAsyncCmd &&
            msg->msgHdr.eph.sem != NULL) {
            if (atomicDec64(msg->msgHdr.eph.nodeToWait) == 0) {
                msg->msgHdr.eph.sem->post();
                freeNodeToWaitForMsg(msg->msgHdr.eph.nodeToWait);
            }
        }
    }

    freeAndEnqueueMsg(msg);
    assert(!(msg = NULL));  // assignment
}

// Set up dedicated connection to a node and keep sending it any queued
// up messages (this includes ios) from our node.
void *
MsgMgr::sendQueuedMessages(void *threadParamsIn)
{
    Config *config = Config::get();

    MsgThreadParams *threadParams = (MsgThreadParams *) threadParamsIn;
    SendRecvThreadInfo *sendRecvThdInfo = &instance->sendRecvThdInfo_;

    xSyslog(moduleName,
            XlogDebug,
            "sendQueuedMessages::myNodeId %d ipAddr %s port %d tid %llu",
            config->getMyNodeId(),
            (char *) &threadParams->ipAddr,
            threadParams->port,
            (unsigned long long) pthread_self());

    Status status = instance->connectToNode(threadParams);

    if (status != StatusOk) {
        sendRecvThdInfo->threadsReadyLock_.lock();
        // make sure sendThreadsReady_ will be negative when error happends
        sendRecvThdInfo->sendThreadsReady_ = 0 - config->getActiveNodes() - 1;
        sendRecvThdInfo->threadsReadyLock_.unlock();
    } else {
        // Say that this thread is ready
        sendRecvThdInfo->threadsReadyLock_.lock();
        sendRecvThdInfo->sendThreadsReady_++;
        sendRecvThdInfo->threadsReadyLock_.unlock();
        // Get the listener thread to check changes in the set of sockets it
        // should be listening on
        uint64_t pokeVal = 1;
        verify(write(instance->pokeSd_, &pokeVal, sizeof(pokeVal)) == 8);
    }

    return NULL;
}

// XXX - fix, do not overload sendThreadsReady_ for this function as it
// is not a send thread. This needs fixing spinUntilSendThreadsReady() and
// spinUntilSendThreadsExited().
void *
MsgMgr::issueLocalIos(void *unused)
{
    // Say that this thread is ready
    sendRecvThdInfo_.threadsReadyLock_.lock();
    ++sendRecvThdInfo_.sendThreadsReady_;
    sendRecvThdInfo_.threadsReadyLock_.unlock();

    return NULL;
}

Status
MsgMgr::sendVector(SocketHandle sockHandle, SgArray *sgArray, int flags)
{
    unsigned ii;
    Status status = StatusUnknown;
    size_t totalBytesXfer;
    size_t curBytesXfer;
    size_t bytesRequested;

    // XXX - fix consider putting bytesRequested into SgArray so that users
    // don't have to scan
    bytesRequested = 0;
    for (ii = 0; ii < sgArray->numElems; ++ii) {
        bytesRequested += sgArray->elem[ii].len;
    }

    assert(bytesRequested > 0);

    // XXX - fix consider instead switching Socket.c's readv() to recvmsg()
    // and then exploiting MSG_PEEK and/or MSG_WAITALL.  If we go with
    // nonblock sockets here however, then we don't want to do this and instead
    // will need to statefully track partial recvs() by socket handle
    for (totalBytesXfer = 0; totalBytesXfer < bytesRequested;) {
        status = sockSendV(sockHandle, sgArray, &curBytesXfer, flags);

        if (status == StatusAgain) {
            continue;
        } else if (status != StatusOk) {
            break;
        } else if (curBytesXfer == 0) {
            status = StatusConnReset;
            break;
        }

        totalBytesXfer += curBytesXfer;

        if (totalBytesXfer != bytesRequested) {
            assert(totalBytesXfer < bytesRequested);
            assert(sgArray->numElems <= (unsigned) MsgTwoFields);
            status = buildTmpSgArray(sgArray,
                                     sgArray,
                                     bytesRequested - totalBytesXfer);
            if (status != StatusOk) {
                break;
            }
        }
    }

    return status;
}

// XXX - fix find a way to avoid the necessity of constructing a temporary
// sgArray or optimize it
Status
MsgMgr::buildTmpSgArray(SgArray *dstSgArray,
                        const SgArray *srcSgArray,
                        size_t bytesRemaining)
{
    int srcStartIndex;
    unsigned numDstElems = (unsigned) -1;
    size_t segmentLen;
    void *elem0StartAddr = (void *) -1;
    size_t elem0StartLen = (size_t) -1;
    Status status;

    // need a temporary since srcSgArray may == dstSgArray
    // DefSgArray(tmpSgArray, MsgTwoFields);
    SgArray *tmpSgArray =
        (SgArray *) memAlloc(sizeof(SgArray) + sizeof(SgElem) * MsgTwoFields);
    BailIfNull(tmpSgArray);

    for (srcStartIndex = srcSgArray->numElems - 1; srcStartIndex >= 0;
         srcStartIndex--) {
        segmentLen = xcMin(srcSgArray->elem[srcStartIndex].len, bytesRemaining);
        bytesRemaining -= segmentLen;
        if (bytesRemaining == 0) {
            numDstElems = srcSgArray->numElems - srcStartIndex;
            elem0StartAddr =
                (void *) ((uintptr_t) srcSgArray->elem[srcStartIndex].baseAddr +
                          (srcSgArray->elem[srcStartIndex].len - segmentLen));
            elem0StartLen = segmentLen;
            break;
        }
    }

    assert(numDstElems != (unsigned) -1);
    assert(elem0StartAddr != (void *) -1);
    assert(elem0StartLen != (size_t) -1);

    tmpSgArray->numElems = srcSgArray->numElems - srcStartIndex;
    memcpy(&tmpSgArray->elem[0],
           &srcSgArray->elem[srcStartIndex],
           (srcSgArray->numElems - srcStartIndex) *
               sizeof(tmpSgArray->elem[0]));
    tmpSgArray->elem[0].baseAddr = elem0StartAddr;
    tmpSgArray->elem[0].len = elem0StartLen;
    memcpy(dstSgArray,
           tmpSgArray,
           sizeof(*tmpSgArray) + sizeof(tmpSgArray->elem[0]) * MsgTwoFields);

    status = StatusOk;
CommonExit:
    if (tmpSgArray != NULL) {
        memFree(tmpSgArray);
        tmpSgArray = NULL;
    }
    return status;
}

// Actually wait for messages from any node
// If failed during socket setup, update recvThreadReady_ to -1 and
// recvThrStatus_ to error status.
void *
MsgMgr::waitForMessagesActual(void *threadParamsIn)
{
    SocketHandle listenSd = SocketHandleInvalid;
    unsigned descReady = 0;
    SocketAddr socketAddr;
    SocketHandleSet masterSet, workingSet, justListener;
    Status initStatus, status = StatusOk;
    Status msgStatus;

    struct timespec timeout;

    MsgThreadParams *threadParams = (MsgThreadParams *) threadParamsIn;
    SendRecvThreadInfo *sendRecvThdInfo = &instance->sendRecvThdInfo_;

    initStatus = sockCreate(SockIPAddrAny,
                            (uint16_t) threadParams->port,
                            SocketDomainUnspecified,
                            SocketTypeStream,
                            &listenSd,
                            &socketAddr);
    if (initStatus != StatusOk) {
        BailIfFailedMsg(moduleName,
                        initStatus,
                        "sockCreate() failed: %s",
                        strGetFromStatus(initStatus));
    }

    // Allow socket descriptor to be reuseable
    initStatus = sockSetOption(listenSd, SocketOptionReuseAddr);
    if (initStatus != StatusOk) {
        BailIfFailedMsg(moduleName,
                        initStatus,
                        "sockSetOption() failed: %s",
                        strGetFromStatus(initStatus));
    }

    // Childnodes should not inherit this
    initStatus = sockSetOption(listenSd, SocketOptionCloseOnExec);
    if (initStatus != StatusOk) {
        BailIfFailedMsg(moduleName,
                        initStatus,
                        "Couldn't set FD_CLOEXEC: %s",
                        strGetFromStatus(initStatus));
    }

    // Bind the socket
    initStatus = sockBind(listenSd, &socketAddr);
    if (initStatus != StatusOk) {
        BailIfFailedMsg(moduleName,
                        initStatus,
                        "sockBind() failed: %s",
                        strGetFromStatus(initStatus));
    }

    // Set the listen backlog
    initStatus = sockListen(listenSd, 32);
    if (initStatus != StatusOk) {
        BailIfFailedMsg(moduleName,
                        initStatus,
                        "sockListen() failed: %s",
                        strGetFromStatus(initStatus));
    }

    instance->pokeSd_ = eventfd(0, EFD_CLOEXEC);
    if (instance->pokeSd_ == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "failed to create poke fd: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Initialize the master fd_set
    sockHandleSetInit(&masterSet);
    sockHandleSetAdd(&masterSet, listenSd);
    sockHandleSetInit(&justListener);
    sockHandleSetAdd(&justListener, listenSd);
    // We want to be able to wake up the select call to process new connections
    sockHandleSetAdd(&justListener, instance->pokeSd_);

    assert(initStatus == StatusOk);
    // Say that this thread is ready
    // XXX - fix, since this is just 1 thread we can use a membar
    sendRecvThdInfo->threadsReadyLock_.lock();
    sendRecvThdInfo->recvThreadReady_++;
    sendRecvThdInfo->recvThrStatus_ = initStatus;
    sendRecvThdInfo->threadsReadyLock_.unlock();

    msgStatus = StatusOk;

    timeout.tv_sec = 1;
    timeout.tv_nsec = 0;

    // Loop waiting for incoming connects or for incoming data
    // on any of the connected sockets.
    while (true) {
        // Copy the master fd_set over to the working fd_set, when
        // we're ready to service them. Otherwise, just pay attention
        // to the listenSd for folks to continue to create connections
        workingSet = (sendRecvThdInfo->sendThreadsReady_ ==
                      (int) Config::get()->getActiveNodes())
                         ? masterSet
                         : justListener;

        // Select() - second last arg is NULL
        while (!descReady) {
            workingSet = (sendRecvThdInfo->sendThreadsReady_ ==
                          (int) Config::get()->getActiveNodes())
                             ? masterSet
                             : justListener;
            status = sockSelect(&workingSet, NULL, NULL, &timeout, &descReady);
            if (!descReady) {
                // pselect() timed out.
                if (instance->clusterState_ == ClusterState::Shutdown) {
                    xSyslog(moduleName,
                            XlogErr,
                            "waitForMessagesActual exiting!");
                    goto CommonExit;
                }
            }
        }

        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "sockSelect() failed: %s",
                    strGetFromStatus(status));
            break;
        }

        // One or more descriptors are readable. Need to determine which ones
        for (int ii = 0; descReady > 0; ++ii) {
            SocketHandle sockHandle = sockHandleFromInt(ii);

            // Check to see if this descriptor is ready
            if (sockHandleSetIncludes(&workingSet, sockHandle)) {
                // A descriptor was found that was readable
                descReady -= 1;

                // Check to see if this is the listening socket
                if (sockHandle == listenSd) {
                    SocketHandle newSd;
                    // Accept all incoming connections that are
                    // queued up on the listening socket
                    // Accept each incoming connection.
                    if ((status = sockAccept(listenSd, NULL, &newSd)) !=
                        StatusOk) {
                        xSyslog(moduleName,
                                XlogErr,
                                "sockAccept() failed: %s",
                                strGetFromStatus(status));
                    } else if ((status =
                                    sockSetOption(newSd,
                                                  SocketOptionKeepAlive)) !=
                               StatusOk) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Could not set keepalive flag on socket %d: %s",
                                newSd,
                                strGetFromStatus(status));
                        sockDestroy(&newSd);
                    } else if ((status =
                                    sockSetOption(newSd,
                                                  SocketOptionCloseOnExec)) !=
                               StatusOk) {
                        xSyslog(moduleName,
                                XlogErr,
                                "Could not set FD_CLOEXEC flag on socket %d: "
                                "%s",
                                newSd,
                                strGetFromStatus(status));
                        sockDestroy(&newSd);
                    } else {
                        // Add the new incoming connection to the
                        // master read set
                        sockHandleSetAdd(&masterSet, newSd);
                    }
                } else if (sockHandle == instance->pokeSd_) {
                    // We were poked because some other thread wants us to
                    // listen to the masterSet because a new socket was added
                    uint64_t val;
                    verify(read(instance->pokeSd_, &val, sizeof(val)) ==
                           sizeof(val));
                    assert(val > 0);
                } else {
                    msgStatus =
                        instance->recvDataFromSocket(sockHandle, masterSet);
                    if (msgStatus == StatusMsgFail) {
                        // Faulty sockHandle. Stop talking to it
                        xSyslog(moduleName,
                                XlogDebug,
                                "Faulty sockhandle. Removing it");
                        sockHandleSetRemove(&masterSet, sockHandle);
                    }
                }
            }
        }
    }

#ifdef UNDEF
EndRecvData:
    // Clean up all of the sockets that are open
    // XXX - fix, need an equivalent of maxSd instead of 1000 here
    for (int ii = 0; ii <= 1000; ++ii) {
        SocketHandle sockHandle = sockHandleFromInt(ii);

        if (sockHandleSetIncludes(&masterSet, sockHandle)) {
            sockDestroy(&sockHandle);
        }
    }

#endif  // UNDEF

CommonExit:

    if (listenSd != SocketHandleInvalid) {
        sockDestroy(&listenSd);
        listenSd = SocketHandleInvalid;
    }

    if (instance->pokeSd_ != SocketHandleInvalid) {
        int ret;
        do {
            ret = ::close(instance->pokeSd_);
        } while (ret == -1 && errno == EINTR);
        instance->pokeSd_ = SocketHandleInvalid;
    }

    if (initStatus != StatusOk) {
        sendRecvThdInfo->threadsReadyLock_.lock();
        // XXX: We currently only have one receive thread
        assert(sendRecvThdInfo->recvThreadReady_ == 0);

        assert(initStatus != StatusOk);
        sendRecvThdInfo->recvThreadReady_ = -1;
        sendRecvThdInfo->recvThrStatus_ = initStatus;
        sendRecvThdInfo->threadsReadyLock_.unlock();
    }

    if (status != StatusOk) {
        sendRecvThdInfo->threadsReadyLock_.lock();
        sendRecvThdInfo->recvThrStatus_ = status;
        sendRecvThdInfo->threadsReadyLock_.unlock();
    }

    return NULL;
}

Status
MsgMgr::getRecvThrStatus()
{
    Status status;

    sendRecvThdInfo_.threadsReadyLock_.lock();
    status = sendRecvThdInfo_.recvThrStatus_;
    sendRecvThdInfo_.threadsReadyLock_.unlock();

    return status;
}

void
MsgMgr::twoPcRecvDataCompletionFunc(SocketHandle *sockHandle, MsgMessage *msg)
{
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();
    TwoPcCallId functionIndex = msg->msgHdr.eph.twoPcFuncIndex;

    if (msg->msgHdr.eph.twoPcBufLife & TwoPcZeroCopyOutput) {
        // Zero copy data directly into caller's buffer.
        process2PcCmdCompletePre(*sockHandle, msg, ZeroCopy);

        twoPcMgr->twoPcAction_[static_cast<uint32_t>(functionIndex)]
            ->recvDataCompletion(&msg->msgHdr.eph, msg->msgHdr.eph.ephemeral);
    } else {
        process2PcCmdCompletePre(*sockHandle, msg, DoNotZeroCopy);
        twoPcMgr->twoPcAction_[static_cast<uint32_t>(functionIndex)]
            ->recvDataCompletion(&msg->msgHdr.eph, msg->payload);
    }

    process2pcCmdCompletePost(msg, msg->msgHdr.typeId);
}

void
MsgMgr::twoPcEphemeralInitAsync(MsgEphemeral *eph,
                                void *payloadToDistribute,
                                size_t payloadLength,
                                size_t payloadLengthTwoPcToAllocate,
                                TwoPcFastOrSlowPath twoPcFastOrSlowPath,
                                TwoPcCallId twoPcFuncIndex,
                                void *ephemeral,
                                TwoPcBufLife twoPcBufLife,
                                Semaphore *sem,
                                uint64_t numNodes)
{
#ifdef DEBUG
    if (payloadLengthTwoPcToAllocate != 0) {
        assert(payloadLengthTwoPcToAllocate > payloadLength);
    }
#endif  // DEBUG
    twoPcEphemeralInit(eph,
                       payloadToDistribute,
                       payloadLength,
                       payloadLengthTwoPcToAllocate,
                       twoPcFastOrSlowPath,
                       twoPcFuncIndex,
                       ephemeral,
                       twoPcBufLife);

    eph->sem = sem;
    eph->nodeToWait = (Atomic64 *) bcHandleAtomic64_->allocBuf(XidInvalid);
    assert(eph->nodeToWait != NULL);
    atomicWrite64(eph->nodeToWait, numNodes);
    eph->txn = Txn();
}

void
MsgMgr::twoPcEphemeralInit(MsgEphemeral *eph,
                           void *payloadToDistribute,
                           size_t payloadLength,
                           size_t payloadLengthTwoPcToAllocate,
                           TwoPcFastOrSlowPath twoPcFastOrSlowPath,
                           TwoPcCallId twoPcFuncIndex,
                           void *ephemeral,
                           TwoPcBufLife twoPcBufLife)
{
    assert((uint32_t) twoPcFuncIndex >
               (uint32_t) TwoPcCallId::TwoPcCallIdFirstEnum &&
           (uint32_t) twoPcFuncIndex <
               (uint32_t) TwoPcCallId::TwoPcCallIdLastEnum);

    if (payloadLengthTwoPcToAllocate < payloadLength) {
        payloadLengthTwoPcToAllocate = payloadLength;
    }

    eph->payloadToDistribute = payloadToDistribute;
    eph->payloadLength = payloadLength;
    eph->payloadLengthTwoPcToAllocate = payloadLengthTwoPcToAllocate;
    eph->twoPcFastOrSlowPath = twoPcFastOrSlowPath;
    eph->twoPcFuncIndex = twoPcFuncIndex;
    eph->ephemeral = ephemeral;
    eph->twoPcBufLife = twoPcBufLife;
    eph->sem = NULL;
    eph->nodeToWait = NULL;
    eph->txn = Txn();
}

NodeId
MsgMgr::getMsgSrcNodeId(MsgEphemeral *eph)
{
    NodeId nodeId;

    assert(eph != NULL);

    MsgHdr *msgHdr = ContainerOf(eph, MsgHdr, eph);
    nodeId = msgHdr->srcNodeId;
    assert(nodeId < Config::get()->getActiveNodes());

    return nodeId;
}

NodeId
MsgMgr::getMsgDstNodeId(MsgEphemeral *eph)
{
    NodeId nodeId;

    assert(eph != NULL);

    MsgHdr *msgHdr = ContainerOf(eph, MsgHdr, eph);
    nodeId = msgHdr->dstNodeId;
    assert(nodeId < Config::get()->getActiveNodes());

    return nodeId;
}

MsgTypeId
MsgMgr::getMsgTypeId(MsgEphemeral *eph)
{
    MsgHdr *msgHdr = ContainerOf(eph, MsgHdr, eph);
    return msgHdr->typeId;
}

void
MsgMgr::handleReceiveError(SocketHandle sockHandle,
                           MsgMessage *msg,
                           size_t payloadLength,
                           Status retStatus,
                           ReceiveErrorOp receiveErrorOp)
{
    size_t chunkSize = RecvErrChunkSize;
    char buf[RecvErrChunkSize];

    msg->msgHdr.eph.status = retStatus;

    if (payloadLength < chunkSize) {
        chunkSize = payloadLength;
    }
    while (payloadLength > 0) {
        Status status = sockRecvConverged(sockHandle, buf, chunkSize);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "failed to receive payload on node %d from node %d: %s",
                    msg->msgHdr.dstNodeId,
                    msg->msgHdr.srcNodeId,
                    strGetFromStatus(status));
            // @SymbolCheckIgnore
            panicNoCore(
                "handleReceiveError: unable to receive payload from "
                "remote node");
        }
        payloadLength -= chunkSize;
        if (payloadLength < chunkSize) {
            chunkSize = payloadLength;
        }
    }

    if (receiveErrorOp == ReceiveErrorOp::Ack) {
        assert(msg->msgHdr.eph.payloadLength == 0);
        assert(!(msg->msgHdr.msgSendRecvFlags & MsgRecvHdrPlusPayload));
        assert(msg->msgHdr.msgSendRecvFlags & MsgRecvHdrOnly);

        // Schedule a fiber to ACK and free schedFsm. Note that receive
        // thread attempting to ACK may deadlock the cluster considering the
        // send sockets are blocking.
        SchedMgr *schedMgr = SchedMgr::get();
        Runtime *rt = Runtime::get();

        static constexpr const size_t TxnAbortMaxRetryCount = 10;
        static constexpr uint64_t TxnAbortSleepUSec = 10000;
        SchedAckAndFree *ackAndFreeObj = NULL;
        size_t txnAbortRetries = 0;
        do {
            if (ackAndFreeObj == NULL) {
                ackAndFreeObj =
                    (SchedAckAndFree *)
                        schedMgr->bcHandleSchedObject_->allocBuf(XidInvalid);
                if (ackAndFreeObj != NULL) {
                    new (ackAndFreeObj) SchedAckAndFree(msg);
                    StatsLib::statAtomicIncr64(schedMgr->schedObjectsActive_);
                }
            }
            if (ackAndFreeObj != NULL) {
                Status status = Runtime::get()->schedule(ackAndFreeObj);
                if (status == StatusOk) {
                    StatsLib::statAtomicIncr64(acksScheduled_);
                    goto CommonExit;
                }
            }

            // Kick Txn abort on Schedulables in the hope that some resources
            // will be released for the receive thread to schedule ACKs on
            // Runtime.
            StatsLib::statAtomicIncr64(kickedTxnAbort_);
            rt->kickSchedsTxnAbort();
            txnAbortRetries++;
            sysUSleep(TxnAbortSleepUSec);
        } while (txnAbortRetries < TxnAbortMaxRetryCount);

        if (ackAndFreeObj != NULL) {
            ackAndFreeObj->done();
            ackAndFreeObj = NULL;
        }

        // Failed to schedule Ack. Let's fallback to using the receive error
        // thread.
        bool foundSlot = false;
        static constexpr uint64_t QueueMsgAckSleepUSec = 10000;
        uint64_t queueMsgAckRetries = 0;
        do {
            sendRecvThdInfo_.errRecvLock_.lock();
            for (size_t ii = 0; ii < MaxOutstandingRecvErrs; ii++) {
                if (sendRecvThdInfo_.receiveErrorMsgsState_[ii] ==
                    SendRecvThreadInfo::ReceiveErrMsgFree) {
                    MsgMessage *msgDst =
                        &sendRecvThdInfo_.receiveErrorMsgs_[ii];
                    memcpy(msgDst, msg, sizeof(MsgMessage));
                    msgDst->msgHdr.msgBufType = NormalCopyMsg;
                    foundSlot = true;
                    sendRecvThdInfo_.receiveErrorMsgsState_[ii] =
                        SendRecvThreadInfo::ReceiveErrMsgInUse;
                    break;
                }
            }
            sendRecvThdInfo_.errRecvLock_.unlock();
            if (!foundSlot) {
                queueMsgAckRetries++;
                sysUSleep(QueueMsgAckSleepUSec);
            }
        } while (foundSlot == false);
        sem_post(&sendRecvThdInfo_.errRecvSem_);
    }

CommonExit:
    StatsLib::statAtomicIncr64(recdMessagesErrStat_);
}

// Helper function for waitForMessagesActual()
Status
MsgMgr::recvDataFromSocket(SocketHandle sockHandle, SocketHandleSet masterSet)
{
    int closeConn = false;
    MsgMessage rcvMsg;
    Status status = StatusOk;
    MsgTypeId msgTypeId;
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();
    bool setTxn = false;
    Stopwatch watch;

    // Receive data on this connection. First read just the msgHdr and then
    // decide if we need a buffer for the payload. Payload length is zero.
    rcvMsg.msgHdr.creator = MsgCreatorRecvDataFromSocket;
    rcvMsg.msgHdr.msgBufType = NormalCopyMsg;
    rcvMsg.payload = NULL;
    rcvMsg.msgHdr.payloadBufCacheType = NoPayload;
    rcvMsg.msgHdr.payloadBufCacheTypeImmutable = IgnorePayloadBufCacheType;
    rcvMsg.ukInternalPayloadLen = 0;

    // Note: msgHdr.creator needs to be preserved as it has node local
    // info as to where the msg was created. Since the below recvData()
    // call will overwrite our local msgHdr.creator we need to restore
    // it. This feature is primarily for debug.
    MsgCreator savedCreator = rcvMsg.msgHdr.creator;
    status =
        sockRecvConverged(sockHandle, &rcvMsg.msgHdr, sizeof(rcvMsg.msgHdr));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "receiveData: receiveConverged failed: %s",
                strGetFromStatus(status));
        freeAndEnqueueMsg(&rcvMsg);
        closeConn = true;
        status = StatusMsgFail;
        goto End;
    }

    // Fix fields that may have gotten overwritten.
    rcvMsg.msgHdr.creator = savedCreator;
    rcvMsg.msgHdr.msgBufType = NormalCopyMsg;

    // Now based on the msgHdr.typeId make the next decison.
    msgTypeId = rcvMsg.msgHdr.typeId;

    setTxn = true;
    Txn::setTxn(rcvMsg.msgHdr.eph.txn);
    assert(Txn::currentTxn().valid());

    switch (msgTypeId) {
    case MsgTypeId::Msg2pcDemystifySourcePageComplete:
    case MsgTypeId::Msg2pcProcessScalarPageComplete:
    case MsgTypeId::Msg2pcIndexPageComplete:
        TransportPageMgr::get()->transportPageShipComplete(&rcvMsg.msgHdr.eph,
                                                           NULL);
        freeAndEnqueueMsg(&rcvMsg);
        break;

    default:
        if (twoPcMgr->twoPcOp_[static_cast<uint32_t>(msgTypeId)]
                .getRecvDataFromSocket() == true) {
            process2pcCmd(sockHandle, &rcvMsg, msgTypeId);
        } else if (twoPcMgr->twoPcOp_[static_cast<uint32_t>(msgTypeId)]
                       .getRecvDataFromSocketComp() == true) {
            twoPcRecvDataCompletionFunc(&sockHandle, &rcvMsg);
        } else {
            // Should never happen
            assert(0);
        }
        break;
    }

    StatsLib::statNonAtomicIncr(recdMessagesStat_);
#ifdef DEBUG
    if (msgTypeId == MsgTypeId::Msg2pcBarrier) {
        assert((status == StatusOk) || (status == StatusFailed));
    }
#endif  // DEBUG

End:
    if (closeConn) {
#ifdef UNDEF
        // Clean up active connection. Remove desc from
        // master set and determinine new max desc
        sockHandleSetRemove(&masterSet, sockHandle);
        sockDestroy(&sockHandle);
#endif  // UNDEF
    }

    if (setTxn) {
        Txn::setTxn(Txn());
    }

    StatsLib::statNonAtomicIncr(recvDataFromSocketCounter_);
    StatsLib::statNonAtomicAdd64(timeSpentUsToRecvData_,
                                 watch.getCurElapsedNSecs() / 1000);

    return status;
}

void
MsgMgr::freeNodeToWaitForMsg(Atomic64 *nodeToWait)
{
    bcHandleAtomic64_->freeBuf(nodeToWait);
}

void *
MsgMgr::processReceiveErrorAck(void *args)
{
    static constexpr const size_t InvalAckIdx = (size_t)(-3);
    MsgMessage *ackMsg = NULL;
    size_t ackIdx = InvalAckIdx;
    Status status = StatusOk;
    size_t id = (size_t) args;

    xSyslog(moduleName, XlogInfo, "Receive Immediate thread %lu ready", id);

    while (true) {
        status = sysSemTimedWait(&instance->sendRecvThdInfo_.errRecvSem_,
                                 RecvErrorThreadSleepUsec);
        assert(status == StatusTimedOut || status == StatusIntr ||
               status == StatusOk);

        instance->sendRecvThdInfo_.errRecvLock_.lock();
        for (size_t ii = 0; (ackMsg == NULL && (ii < MaxOutstandingRecvErrs));
             ii++) {
            if (instance->sendRecvThdInfo_.receiveErrorMsgsState_[ii] ==
                SendRecvThreadInfo::ReceiveErrMsgInUse) {
                instance->sendRecvThdInfo_.receiveErrorMsgsState_[ii] =
                    SendRecvThreadInfo::ReceiveErrMsgInProgress;
                ackMsg = &instance->sendRecvThdInfo_.receiveErrorMsgs_[ii];
                ackIdx = ii;
            }
        }
        instance->sendRecvThdInfo_.errRecvLock_.unlock();

        if (ackMsg != NULL) {
            assert(ackMsg->msgHdr.eph.txn.valid());
            Txn::setTxn(ackMsg->msgHdr.eph.txn);
            instance->ackAndFree(ackMsg);
            ackMsg = NULL;
            Txn::setTxn(Txn());

            assert(ackIdx >= 0 && ackIdx < MaxOutstandingRecvErrs);
            instance->sendRecvThdInfo_.errRecvLock_.lock();
            assert(instance->sendRecvThdInfo_.receiveErrorMsgsState_[ackIdx] ==
                   SendRecvThreadInfo::ReceiveErrMsgInProgress);
            instance->sendRecvThdInfo_.receiveErrorMsgsState_[ackIdx] =
                SendRecvThreadInfo::ReceiveErrMsgFree;
            instance->sendRecvThdInfo_.errRecvLock_.unlock();
            ackIdx = InvalAckIdx;
        }

        if (instance->clusterState_ == ClusterState::Shutdown) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Receive Immediate thread %lu exits with cluster shutdown",
                    id);
            break;
        }
    }

    return NULL;
}

void
MsgMgr::doTwoPcWorkForMsg(MsgMessage *msg)
{
    NodeId myNodeId = Config::get()->getMyNodeId();
    TwoPcMgr *twoPcMgr = TwoPcMgr::get();
    TwoPcCallId functionIndex = msg->msgHdr.eph.twoPcFuncIndex;

    assert(msg->msgHdr.eph.txn.valid());
    assert(msg->msgHdr.eph.txn == Txn::currentTxn());

    twoPcMgr->twoPcAction_[static_cast<uint32_t>(functionIndex)]
        ->schedLocalWork(&msg->msgHdr.eph, msg->payload);

    if (functionIndex == TwoPcCallId::Msg2pcGetRows1 ||
        functionIndex == TwoPcCallId::Msg2pcDlmUpdateNs1 ||
        functionIndex == TwoPcCallId::Msg2pcDlmRetinaTemplate1 ||
        functionIndex == TwoPcCallId::Msg2pcDatasetReference1 ||
        functionIndex == TwoPcCallId::Msg2pcDlmKvStoreOp1 ||
        functionIndex == TwoPcCallId::Msg2pcQueryState1 ||
        functionIndex == TwoPcCallId::Msg2pcGetStatGroupIdMap1 ||
        functionIndex == TwoPcCallId::Msg2pcRpcGetStat1 ||
        functionIndex == TwoPcCallId::Msg2pcGetPerfStats1) {
        // XXX: this is a hack to enable callee allocation of payload
        // The real payload to send back is inside of eph. Free the twoPc
        // provided payload.
        memFree(msg->payload);
        msg->payload = msg->msgHdr.eph.payloadToDistribute;
    }

    if (myNodeId == msg->msgHdr.srcNodeId) {
        checkPayloadLengthAndDisableSendRecvFlags(msg);
        twoPcMgr->twoPcAction_[static_cast<uint32_t>(functionIndex)]
            ->schedLocalCompletion(&msg->msgHdr.eph, msg->payload);
        updateMsgTwoPcState(msg);
        MsgMgr::get()->freeAndEnqueueMsg(msg);
    }
}

void
MsgMgr::updateMsgTwoPcState(MsgMessage *msg)
{
    MsgTypeId msgTypeId =
        (MsgTypeId)((int) msg->msgHdr.typeId + TwoPcCmdCompletionOffset);

    // Update TwoPcState
    MsgMgr::get()->updateMsgLocalTwoPcState(msg, msgTypeId);

    // After updating the twoPcState, the twoPc handle could be freed by the
    // caller of aysnc twoPc(), therefore it is not safe to use any more.
    msg->msgHdr.twoPcHandle.twoPcHandle = 0;

    if (msg->msgHdr.twoPcCmdType == TwoPcAsyncCmd &&
        msg->msgHdr.eph.sem != NULL) {
        if (atomicDec64(msg->msgHdr.eph.nodeToWait) == 0) {
            msg->msgHdr.eph.sem->post();
            MsgMgr::get()->freeNodeToWaitForMsg(msg->msgHdr.eph.nodeToWait);
        }
    }
}

void
MsgMgr::transferTxnLog(TxnLog *srcLog, TxnLog *dstLog)
{
    assert(srcLog != NULL);
    assert(dstLog != NULL);

    assert(srcLog->txn_.id_ != dstLog->txn_.id_);
    assertStatic(ArrayLen(srcLog->messages) == ArrayLen(dstLog->messages));

    // this routine is called when parent txn is attempting to transfer
    // child txn log, it will be single threaded, hence no lock needed

    // we support only 1 message to be transferred
    if (dstLog->numMessages == 1) {
        return;
    }

    assert(srcLog->numMessages <= 1);
    dstLog->numMessages = 0;
    for (unsigned ii = 0; ii < srcLog->numMessages; ii++) {
        strlcpy(dstLog->messages[ii],
                srcLog->messages[ii],
                sizeof(srcLog->messages[srcLog->numMessages]));
        dstLog->numMessages++;
    }
    assert(dstLog->numMessages <= 1);
}

void
MsgMgr::restoreTxnAndTransferTxnLog(Txn parentTxn)
{
    TxnLog *childTxnLog = NULL;
    TxnLog *parentTxnLog = NULL;
    Txn childTxn = Txn::currentTxn();

    assert(childTxn.id_ != parentTxn.id_);

    childTxnLog = getTxnLog(childTxn);
    parentTxnLog = getTxnLog(parentTxn);

    // since tests dont create a txn log with a txn id
    // in some cases dont even have a txn id, so we check in run
    // time if parent and child txn have a log then do a memcpy
    // operatorHandle->run might have failed to init txn log
    // hence checking both in run time
    if ((parentTxnLog != NULL) && (childTxnLog != NULL)) {
        transferTxnLog(childTxnLog, parentTxnLog);
    }

    Txn::setTxn(parentTxn);
}

bool
MsgMgr::transactionAbortSched(Schedulable *sched, Status errorStatus)
{
    bool transAbort = false;
    Config *config = Config::get();
    NodeId myNodeId = config->getMyNodeId();

    StatsLib::statAtomicIncr64(outOfFiberSched_);
    if (SchedFsmTransportPage *schedFsmTp =
            dynamic_cast<SchedFsmTransportPage *>(sched)) {
        if (schedFsmTp->msgCopy_.msgHdr.srcNodeId != myNodeId) {
            transAbort = true;
            schedFsmTp->msgCopy_.msgHdr.eph.status = errorStatus;
            if (errorStatus == StatusNoMem) {
                StatsLib::statAtomicIncr64(outOfFiberSchedFsmTransportPage_);
            }
            schedFsmTp->markToAbort();
        }
    }

    return transAbort;
}

void
MsgMgr::logTxnBuf(Txn txn, char *bufHdr, char *buf)
{
    bool lockHeld = false;
    TxnLog *txnLog = NULL;

    if (XidMgr::get()->xidGetNodeId(txn.id_) != Config::get()->getMyNodeId()) {
        // XXX: only log messages on txn owner node
        goto CommonExit;
    }

    txnLogLock_.lock();
    lockHeld = true;

    txnLog = txnLogs_.find(txn.id_);
    if (txnLog == NULL) {
        goto CommonExit;
    }

    // XXX: we store at most MaxMessages txnLog messages
    if (txnLog->numMessages < ArrayLen(txnLog->messages)) {
        snprintf(txnLog->messages[txnLog->numMessages],
                 sizeof(txnLog->messages[txnLog->numMessages]),
                 "%s %s",
                 bufHdr,
                 buf);
        txnLog->numMessages++;
    }

CommonExit:
    if (lockHeld == true) {
        txnLogLock_.unlock();
    }
}

Status
MsgMgr::addTxnLog(Txn txn)
{
    Status status;

    TxnLog *txnLog = new (std::nothrow) TxnLog(txn);
    if (!txnLog) {
        status.fromStatusCode(StatusCodeNoMem);
        goto CommonExit;
    }

    txnLogLock_.lock();
    status = txnLogs_.insert(txnLog);
    txnLogLock_.unlock();
    assert(status == StatusOk || status == StatusExist);
    if (status == StatusExist) {
        delete txnLog;
        goto CommonExit;
    }

CommonExit:
    return status;
}

void
MsgMgr::deleteTxnLog(Txn txn)
{
    txnLogLock_.lock();
    TxnLog *txnLog = txnLogs_.remove(txn.id_);
    txnLogLock_.unlock();

    if (txnLog == NULL) {
        return;
    }
    delete txnLog;
}

MsgMgr::TxnLog *
MsgMgr::getTxnLog(Txn txn)
{
    txnLogLock_.lock();
    TxnLog *txnLog = txnLogs_.find(txn.id_);
    txnLogLock_.unlock();

    return txnLog;
}

unsigned
MsgMgr::numCoresOnNode(NodeId nodeId)
{
    return nodeLocalInfo_[nodeId].numCores;
}
