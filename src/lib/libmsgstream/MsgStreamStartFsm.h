// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_START_FSM_H_
#define _MSGSTREAM_START_FSM_H_

#include "util/SchedulableFsm.h"
#include "msgstream/MsgStream.h"
#include "MsgStreamInfo.h"

class MsgStreamStartFsm : public SchedulableFsm
{
  public:
    MsgStreamStartFsm(FsmState *curState, MsgStreamInfo *msgStreamInfo)
        : SchedulableFsm("MsgStreamStartFsm", curState),
          msgStreamInfo_(msgStreamInfo)
    {
    }

    virtual ~MsgStreamStartFsm() {}

    virtual void done() override {}

    MustCheck Status getStatus() { return retStatus_; }

    void setStatus(Status status) { retStatus_ = status; }

    MustCheck MsgStreamInfo *getMsgStreamInfo() { return msgStreamInfo_; }

  private:
    MsgStreamInfo *msgStreamInfo_ = NULL;
    Status retStatus_ = StatusOk;
};

class StateMsgStreamStart : public FsmState
{
  public:
    StateMsgStreamStart(MsgStreamStartFsm *schedFsm)
        : FsmState("StateMsgStreamStart", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateMsgStreamStartCompletion : public FsmState
{
  public:
    StateMsgStreamStartCompletion(MsgStreamStartFsm *schedFsm)
        : FsmState("StateMsgStreamStartCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

#endif  // _MSGSTREAM_START_FSM_H_
