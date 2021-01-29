// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SCHEDMESSAGECLIENT_H_
#define _SCHEDMESSAGECLIENT_H_

#include <pthread.h>
#include "MessageInt.h"
#include "util/SchedulableFsm.h"
#include "bc/BufferCache.h"
#include "operators/Operators.h"
#include "demystify/Demystify.h"
#include "transport/TransportPage.h"
#include "operators/OperatorsTypes.h"

class SchedMessageObject : public SchedulableFsm
{
  public:
    SchedMessageObject(const char *name, MsgMessage *msg);
    void run() override;
    void done() override;

    MsgMessage msgCopy_;

  private:
    SchedMessageObject(const SchedMessageObject &) = delete;
    SchedMessageObject &operator=(const SchedMessageObject &) = delete;
};

class SchedAckAndFree : public SchedulableFsm
{
    friend class MsgMgr;

  public:
    SchedAckAndFree(MsgMessage *msg);
    virtual ~SchedAckAndFree();

    MsgMessage msgCopy_;

    virtual void run();
    virtual void done();
    virtual bool isNonBlocking() const;
    virtual bool isPriority() const;
};

class StateDemystifySourcePage : public FsmState
{
  public:
    StateDemystifySourcePage(SchedulableFsm *schedFsm)
        : FsmState("StateDemystifySourcePage", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateEnqueueScalarPage : public FsmState
{
  public:
    StateEnqueueScalarPage(SchedulableFsm *schedFsm)
        : FsmState("StateEnqueueScalarPage", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateDequeueScalarPage : public FsmState
{
  public:
    StateDequeueScalarPage(SchedulableFsm *schedFsm)
        : FsmState("StateDequeueScalarPage", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateProcessScalarPage : public FsmState
{
  public:
    StateProcessScalarPage(SchedulableFsm *schedFsm)
        : FsmState("StateProcessScalarPage", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateIndexPage : public FsmState
{
  public:
    StateIndexPage(SchedulableFsm *schedFsm)
        : FsmState("StateIndexPage", schedFsm), setKeyTypeSem_(0)
    {
    }

    unsigned numPages_ = 0;
    TransportPage **indexPages_ = NULL;
    OpMeta *opMeta_ = NULL;
    bool init_ = false;
    Xdb *dstXdb_ = NULL;
    XdbMeta *dstMeta_ = NULL;
    Semaphore setKeyTypeSem_;
    bool setKeyTypeDrainer_ = false;
    static constexpr const uint64_t SetKeyTypeSemTimeOutUsecs = 100;

    virtual TraverseState doWork() override;
};

class SchedFsmTransportPage : public SchedulableFsm
{
  public:
    SchedFsmTransportPage(const char *name, MsgMessage *msg);
    virtual ~SchedFsmTransportPage();

    TransportPageHandle *handle_ = NULL;
    MsgMessage msgCopy_;

    StateDemystifySourcePage demystifySourcePage_;

    StateEnqueueScalarPage enqueueScalarPage_;
    StateDequeueScalarPage dequeueScalarPage_;
    StateProcessScalarPage processScalarPage_;
    StateIndexPage indexPage_;
    SchedFsmTransportPage *setKeyTypeListLink_ = NULL;

    DemystifyMgr *demystifyMgr_ = NULL;
    TransportPage *transPage_ = NULL;
    DemystifyMgr::Op op_;

    // Needed to handle TP index setKeyType case
    Atomic64 indexPageRef_;

    virtual void done();
    virtual bool isPriority() const;

    bool contextAlloced_ = false;
    Status allocContext(DemystifyMgr::Op op);
    void freeContext();

  private:
    SchedFsmTransportPage(const SchedFsmTransportPage &) = delete;
    SchedFsmTransportPage &operator=(const SchedFsmTransportPage &) = delete;
};

union SchedMsg2pcObject {
    SchedMessageObject sobj;
    SchedAckAndFree sAckAndFree;
    SchedFsmTransportPage sObjTransportPage;
};

#endif  // _SCHEDMESSAGECLIENT_H_
