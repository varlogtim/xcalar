// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDFINT_H_
#define _XDFINT_H_

#include "primitives/Primitives.h"
#include "scalars/Scalars.h"
#include "operators/XdfParallelDoTypes.h"

typedef Status (*LocalInitFn)(XdfAggregateAccumulators *acc,
                              void *broadcastPacket,
                              size_t broadcastPacketSize);
typedef Status (*LocalFn)(XdfAggregateAccumulators *acc,
                          int argc,
                          Scalar *argv[]);
typedef Status (*GlobalFn)(XdfAggregateAccumulators *dstAcc,
                           XdfAggregateAccumulators *srcAcc);
struct ParallelOpHandlers {
    LocalInitFn localInitFn;
    LocalFn localFn;
    GlobalFn globalFn;
};

extern Status parallelDo(ScalarGroupIter *groupIter,
                         XdfAggregateHandlers aggHandler,
                         XdfAggregateAccumulators *acc,
                         void *broadcastPacket,
                         size_t broadcastPacketSize);

extern ParallelOpHandlers parallelOpHandlers[];
extern uint32_t numParallelOpHandlers;

#endif  // _XDFINT_H__
