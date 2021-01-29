// Copyright 2016 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <new>
#include "MessageInt.h"
#include "bc/BufferCache.h"
#include "stat/Statistics.h"
#include "SchedMessageClient.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "SchedInt.h"
#include "TwoPcInt.h"
#include "runtime/Tls.h"

static constexpr const char *moduleName = "libmsg";
SchedMgr *SchedMgr::instance = NULL;

SchedMgr *
SchedMgr::get()
{
    return instance;
}

Status
SchedMgr::init()
{
    Status status = StatusOk;
    StatsLib *statsLib = StatsLib::get();

    assert(instance == NULL);
    instance = (SchedMgr *) memAllocExt(sizeof(SchedMgr), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) SchedMgr();

    instance->bcHandleSchedObject_ =
        BcHandle::create(BufferCacheObjects::UkSchedObject);
    if (instance->bcHandleSchedObject_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }

    status = statsLib->initNewStatGroup("uk.sched.message",
                                        &instance->schedMessageStatsGrpId_,
                                        schedMgrStatCount);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->schedObjectsExhausted_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->schedMessageStatsGrpId_,
                                         "schedObjectsExhausted",
                                         instance->schedObjectsExhausted_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = statsLib->initStatHandle(&instance->schedObjectsActive_);
    BailIfFailed(status);
    status = statsLib->initAndMakeGlobal(instance->schedMessageStatsGrpId_,
                                         "schedObjectsActive",
                                         instance->schedObjectsActive_,
                                         StatUint64,
                                         StatAbsoluteWithNoRefVal,
                                         StatRefValueNotApplicable);
    BailIfFailed(status);

    status = StatusOk;

CommonExit:
    if (status != StatusOk) {
        if (instance->bcHandleSchedObject_ != NULL) {
            BcHandle::destroy(&instance->bcHandleSchedObject_);
            instance->bcHandleSchedObject_ = NULL;
        }
    }
    return status;
}

void
SchedMgr::destroy()
{
    if (bcHandleSchedObject_ != NULL) {
        BcHandle::destroy(&bcHandleSchedObject_);
        bcHandleSchedObject_ = NULL;
    }

    instance->~SchedMgr();
    memFree(instance);
    instance = NULL;
}

size_t
SchedMgr::bcUkSchedObjectBufSize()
{
    return sizeof(SchedMsg2pcObject);
}

Status
SchedMgr::schedOneMessage(MsgMessage *msg, bool schedOnRuntime)
{
    NodeId myNodeId = Config::get()->getMyNodeId();
    assert(msg->msgHdr.dstNodeId == myNodeId);
    Status status = StatusOk;
    void *schedObject = NULL;

    assert(Txn::currentTxn().valid());
    assert(msg->msgHdr.eph.txn == Txn::currentTxn());

    // XXX Allow SchedObject Buf$ slow allows now. Needs to revisit this later.
    schedObject = bcHandleSchedObject_->allocBuf(XidInvalid, &status);
    if (schedObject != NULL) {
        StatsLib::statAtomicIncr64(schedObjectsActive_);
        switch (msg->msgHdr.typeId) {
        case MsgTypeId::Msg2pcDemystifySourcePage:
        case MsgTypeId::Msg2pcProcessScalarPage:
        case MsgTypeId::Msg2pcIndexPage:
            new (schedObject) SchedFsmTransportPage("TransportPage", msg);
            break;

        default:
            new (schedObject) SchedMessageObject("SchedMessageObject", msg);
            break;
        }

        if (schedOnRuntime) {
            // All slow path work needs to start on immediate scheduler
            status = Runtime::get()->schedule(static_cast<Schedulable *>(
                                                  schedObject),
                                              Runtime::SchedId::Immediate);
            if (status != StatusOk) {
                goto CommonExit;
            }
        } else {
            Schedulable *tmp = static_cast<Schedulable *>(schedObject);
            tmp->run();
            tmp->done();
        }
    } else {
        StatsLib::statAtomicIncr64(schedObjectsExhausted_);
        goto CommonExit;
    }

    assert(status == StatusOk);
    MsgMgr::get()->freeMsgOnly(msg);

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogWarn,
                "failed to schedule on local node: %s",
                strGetFromStatus(status));
    }

    return status;
}
