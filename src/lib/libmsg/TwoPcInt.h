// Copyright 2016-2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _TWOPCINT_H_
#define _TWOPCINT_H_

#include "msg/TwoPcFuncDefs.h"
#include "msg/TwoPcFuncDefsClient.h"
#include "stat/Statistics.h"

class TwoPcMgr final
{
  public:
    static MustCheck TwoPcMgr *get();
    static MustCheck Status init();
    void destroy();

    static constexpr uint32_t TwoPcMaxCmds = 4096;
    TwoPcOp twoPcOp_[static_cast<uint32_t>(MsgTypeId::MsgTypeIdLastEnum)];
    TwoPcAction *twoPcAction_[TwoPcMaxCmds];

    // Stats for number of calls to each 2PC.  The extra -1 for the number
    // of 2PCs is because the last enum is never a valid 2PC.
    StatGroupId count2pcInvokedStatGrpId_;
    StatGroupId count2pcFinishedStatGrpId_;
    StatGroupId count2pcErrorsStatGrpId_;
    StatGroupId countRemote2pcEntryStatGrpId_;
    StatGroupId countRemote2pcFinishedStatGrpId_;
    StatGroupId countRemote2pcErrorsStatGrpId_;

    static constexpr size_t Num2PCs =
        (size_t) TwoPcCallId::TwoPcCallIdLastEnum -
        (size_t) TwoPcCallId::TwoPcCallIdFirstEnum - 1;

    StatHandle numLocal2PCInvoked_[Num2PCs];
    StatHandle numLocal2PCFinished_[Num2PCs];
    StatHandle numLocal2PCErrors_[Num2PCs];

    StatHandle numRemote2PCEntry_[Num2PCs];
    StatHandle numRemote2PCFinished_[Num2PCs];
    StatHandle numRemote2PCErrors_[Num2PCs];

  private:
    static TwoPcMgr *instance;

    MustCheck Status initStat(uint32_t statIndex, const char *name);

    // Keep this private. Use setUp instead.
    TwoPcMgr() {}

    // Keep this private. Use tearDown instead.
    ~TwoPcMgr() {}

    TwoPcMgr(const TwoPcMgr &) = delete;
    TwoPcMgr &operator=(const TwoPcMgr &) = delete;
};

#endif  // _TWOPCINT_H_
