// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SCHEDINT_H_
#define _SCHEDINT_H_

struct MsgMessage;

class SchedMgr
{
  public:
    static constexpr uint64_t SchedQSize = 4096;

    BcHandle* bcHandleSchedObject_ = NULL;
    StatHandle schedObjectsActive_;

    static MustCheck SchedMgr* get();

    static MustCheck Status init();

    void destroy();

    MustCheck Status schedOneMessage(MsgMessage* msg, bool schedOnRuntime);

    static MustCheck size_t bcUkSchedObjectBufSize();

  private:
    // Stat count for sched message group
    static constexpr const uint32_t schedMgrStatCount = 2;
    static SchedMgr* instance;

    StatGroupId schedMessageStatsGrpId_;
    StatHandle schedObjectsExhausted_;

    // Keep this private, use init instead
    SchedMgr() {}

    // Keep this private, use destroy instead
    ~SchedMgr() {}

    SchedMgr(const SchedMgr&) = delete;
    SchedMgr(const SchedMgr&&) = delete;
    SchedMgr& operator=(const SchedMgr&) = delete;
    SchedMgr& operator=(const SchedMgr&&) = delete;
};

#endif  // _SCHEDINT_H_
