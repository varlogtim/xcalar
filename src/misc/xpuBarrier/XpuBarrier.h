// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef XPU_BARRIER_H
#define XPU_BARRIER_H

class XpuBarrier {
 public:
    XpuBarrier(int myXPUid, int nXPUs, int nNodes);
    ~XpuBarrier();

    int XpuBarrierWait();
 private:
    int myXPUid_;
    int nXPUs_;
    int nNodes_;
    int nXPUsPerNode_;
    int xpuBarrierLocalAggr(int);
    int xpuBarrierMyGlobalAggr();
    bool xpuBarrierIamSlave();
    bool xpuBarrierIamLocalAggr();
    bool xpuBarrierIamGlobalAggr();
};

#endif // XPU_BARRIER_H
