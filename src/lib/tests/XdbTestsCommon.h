// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _XDBTESTSCOMMON_H_
#define _XDBTESTSCOMMON_H_

#include "primitives/Primitives.h"

void unitTestConfig();
Status xdbSetupRuntime();
Status xdbTeardownRuntime();
Status xdbBasicTests();
Status xdbCursorTests();
Status xdbStringTests();
Status xdbPgCursorThreadStress();
Status xdbOrderedVsUnorderedStress();
Status xdbPgCursorPerfTest();
Status xdbCreateLoadDropTest();
Status xdbCheckSerDes(const XdbId xdbId);
Status xdbSortTest(uint64_t numRows);
Status xdbSortSanityTest();

void setNumRowsCursorTestAscending(uint64_t val);
void setNumRowsCursorTestDescending(uint64_t val);
void setNumRowsCursorTestInsertAndGetSingle(uint64_t val);
void setNumRowsCursorTestInsertAndSeekThenVerifyOne(uint64_t val);
void setNumRowsCursorTestInsertAndSeekThenGetNext(uint64_t val);
void setNumRowsCursorTestPartiallyOrderedVsUnordered(uint64_t val);
void setXdbPgCursorStressThreads(uint64_t val);
void setNumRowsXdbPgCursorStress(uint64_t val);
void setXdbOrderedVsUnorderedStressThreads(uint64_t val);
void setNumRowsXdbOrderedVsUnorderedStress(uint64_t val);
void setNumItersXdbOrderedVsUnorderedStress(uint64_t val);
void setNumRowsXdbPgCursorPerfTest(uint64_t val);
void setNumRowsPerXdbForCreateLoadDropTest(uint64_t val);
void setNumXdbsForCreateLoadDropTest(uint64_t val);
void setXdbCreateLoadDropTestThreads(uint64_t val);
void setXdbParallelKvInsertsThreads(uint64_t val);
void setXdbParallelPageInsertsThreads(uint64_t val);
void setXdbParallelCursorThreads(uint64_t val);

#endif  // _XDBTESTSCOMMON_H_
