// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MSGSTREAM_SEND_FSM_H_
#define _MSGSTREAM_SEND_FSM_H_

#include "util/SchedulableFsm.h"
#include "msgstream/MsgStream.h"
#include "MsgStreamInfo.h"
#include "util/Atomics.h"
#include "util/MemTrack.h"

class MsgStreamSendFsm : public SchedulableFsm
{
  public:
    MsgStreamSendFsm(FsmState *curState,
                     MsgStreamInfo *msgStreamInfo,
                     MsgStreamMgr::ProtocolDataUnit *pdu,
                     void *sendContext)
        : SchedulableFsm("MsgStreamSendFsm", curState),
          msgStreamInfo_(msgStreamInfo),
          pdu_(pdu),
          sendContext_(sendContext)
    {
        atomicWrite64(&outstanding_, 0);
    }

    virtual ~MsgStreamSendFsm() {}

    virtual void done() override
    {
        if (atomicDec64(&outstanding_) == 0) {
            assert(getCurState() == NULL);
            delete this;
        }
    }

    MustCheck Status getStatus() { return retStatus_; }

    void setStatus(Status status) { retStatus_ = status; }

    MustCheck MsgStreamInfo *getMsgStreamInfo() { return msgStreamInfo_; }

    MustCheck MsgStreamMgr::ProtocolDataUnit *getPayload() { return pdu_; }

    void setPayload(MsgStreamMgr::ProtocolDataUnit *pdu) { pdu_ = pdu; }

    void incOutstanding() { atomicInc64(&outstanding_); }

    void decOutstanding() { atomicDec64(&outstanding_); }

    MustCheck uint64_t getOutstanding() { return atomicRead64(&outstanding_); }

    MustCheck void *getSendContext() { return sendContext_; }

  private:
    MsgStreamInfo *msgStreamInfo_ = NULL;
    MsgStreamMgr::ProtocolDataUnit *pdu_ = NULL;
    Status retStatus_ = StatusOk;
    Atomic64 outstanding_;
    void *sendContext_ = NULL;
};

class StateMsgStreamSend : public FsmState
{
  public:
    StateMsgStreamSend(MsgStreamSendFsm *schedFsm)
        : FsmState("StateMsgStreamSend", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

class StateMsgStreamSendCompletion : public FsmState
{
  public:
    StateMsgStreamSendCompletion(MsgStreamSendFsm *schedFsm)
        : FsmState("StateMsgStreamSendCompletion", schedFsm)
    {
    }

    virtual TraverseState doWork() override;
};

#endif  // _MSGSTREAM_SEND_FSM_H_
