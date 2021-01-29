// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_END_FSM_H_
#define _MSGSTREAM_END_FSM_H_

#include "util/SchedulableFsm.h"
#include "msgstream/MsgStream.h"
#include "MsgStreamInfo.h"

class MsgStreamEndFsm : public SchedulableFsm
{
  public:
    MsgStreamEndFsm(FsmState *curState, MsgStreamInfo *msgStreamInfo)
        : SchedulableFsm("MsgStreamEndFsm", curState),
          msgStreamInfo_(msgStreamInfo)
    {
    }

    virtual ~MsgStreamEndFsm() {}

    virtual void done() override {}

    MustCheck Status getStatus() { return retStatus_; }

    void setStatus(Status status) { retStatus_ = status; }

    MustCheck MsgStreamInfo *getMsgStreamInfo() { return msgStreamInfo_; }

  private:
    MsgStreamInfo *msgStreamInfo_ = NULL;
    Status retStatus_ = StatusOk;
};

class StateMsgStreamEnd : public FsmState
{
  public:
    StateMsgStreamEnd(MsgStreamEndFsm *schedFsm)
        : FsmState("StateMsgStreamEnd", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateMsgStreamEndCompletion : public FsmState
{
  public:
    StateMsgStreamEndCompletion(MsgStreamEndFsm *schedFsm)
        : FsmState("StateMsgStreamEndCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

#endif  // _MSGSTREAM_END_FSM_H_
