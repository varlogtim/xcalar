// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _DATASETINT_H_
#define _DATASETINT_H_

#include "primitives/Primitives.h"

#include "util/AtomicTypes.h"
#include "util/WorkQueue.h"
#include "bc/BufferCache.h"
#include "msg/MessageTypes.h"

#include "dataset/DatasetTypes.h"

#include "operators/Dht.h"
#include "xdb/DataModelTypes.h"

// Stats
Status dsStatsToInit();
extern StatHandle dfGetFieldValuesFatptrCompletionCount;
extern StatHandle dfGetFieldValuesFatptrCount;

#endif  // _DATASETINT_H_
