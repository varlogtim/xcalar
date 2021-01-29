// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _KVSTORETESTSCOMMON_H_
#define _KVSTORETESTSCOMMON_H_

#include "primitives/Primitives.h"

static constexpr uint64_t RunsPerThreadDefault = 4;
static constexpr uint64_t ThreadsPerCoreDefault = 1;
static constexpr uint64_t RandomLoopIterationsDefault = 500;

// KvStore name when doing non-thread-specific tests.
static constexpr char const *commonKvStoreName = "KvStoreCommon";

static constexpr bool Persist = true;
static constexpr bool NonPersist = false;
static constexpr bool DoVerification = true;
static constexpr bool NoVerification = false;

extern uint64_t randomLoopIterations;

Status kvStoreTestsSanity();
Status kvStoreBigMessageTestSanity();
Status kvStoreBadUserTestSanity();
Status kvStoreRandomTestSanity();

Status kvStoreTests(const char *kvStoreName, bool orderly);
Status kvStoreBigMessageTest(const char *kvStoreName, bool orderly);
Status kvStoreBadUserTest(const char *kvStoreName, bool orderly);
Status kvStoreRandomTest(const char *kvStoreName, bool orderly);

#endif  // _KVSTORETESTSCOMMON_H_
