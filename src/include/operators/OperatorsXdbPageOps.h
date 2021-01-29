// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDBPAGE_OPS_H_
#define _XDBPAGE_OPS_H_

#include "primitives/Primitives.h"
#include "xdb/DataModelTypes.h"
#include "msg/MessageTypes.h"
#include "bc/BufferCache.h"
#include "operators/Dht.h"
#include "operators/Operators.h"
#include "transport/TransportPage.h"
#include "util/AccumulatorHashTable.h"

MustCheck Status opInitLoadInfo(OpLoadInfo *loadInfo,
                                Xdb *dstXdb,
                                unsigned maxPagePoolSize);

void opDestroyLoadInfo(OpLoadInfo *loadInfo);

MustCheck Status opGetInsertHandle(OpInsertHandle *insertHandle,
                                   OpLoadInfo *loadInfo,
                                   XdbInsertKvState insertState);

void opPutInsertHandle(OpInsertHandle *insertHandle);

MustCheck Status opProcessLoadInfo(Xdb *xdb, OpLoadInfo *loadInfo);

MustCheck Status opInsertPageChain(XdbPage **xdbPageOut,
                                   uint64_t *numPages,
                                   NewKeyValueMeta *kvMeta,
                                   NewTupleValues *xdbValueArray,
                                   Xdb *xdb);

MustCheck Status opInsertPageChainBatch(XdbPage **xdbPages,
                                        unsigned numPages,
                                        int pageIdx,
                                        int *retPageIdx,
                                        NewKeyValueMeta *kvMeta,
                                        NewTupleValues *xdbValueArray,
                                        Xdb *xdb);

MustCheck Status opPopulateInsertHandle(OpInsertHandle *insertHandle,
                                        NewKeyValueEntry *kvEntry);

Status groupSlot(Xdb *srcXdb,
                 Xdb *dstXdb,
                 OpKvEntryCopyMapping *mapping,
                 uint64_t slotId,
                 unsigned numGroupEvals,
                 GroupEvalContext *grpCtxs);

Status groupSlotHash(Xdb *srcXdb,
                     Xdb *dstXdb,
                     OpKvEntryCopyMapping *mapping,
                     uint64_t slotId,
                     unsigned numGroupEvals,
                     GroupEvalContext *grpCtxs);

#endif  // _XDBPAGE_OPS_H
