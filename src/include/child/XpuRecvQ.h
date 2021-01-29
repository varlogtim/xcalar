// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.

#ifndef _XPURECVQ_H_
#define _XPURECVQ_H_

#include "runtime/CondVar.h"

struct XpuRecvQElem {
    uint8_t *payload;
    uint64_t length;
    XpuRecvQElem *xpuRecvQef;
    XpuRecvQElem *xpuRecvQeb;
};

struct XpuRecvFifoQ {
    Spinlock xpuRecvqLock;
    CondVar xpuRecvqCv;
    XpuRecvQElem *xpuRecvqHead;
    XpuRecvQElem *xpuRecvqTail;
};

#endif  // _XPURECVQ_H_
