// Copyright 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SAVETRACE_H
#define _SAVETRACE_H

#include "primitives/Primitives.h"

constexpr int32_t slabInvalid = -1;
// The name below is used by genBuildArtifacts() in jenkinsUtils.sh
// So if you change it, change in both places!
constexpr const char *oomTracesDirName = "oomBcDebugTraces";

enum class TraceOpts { MemInc, MemDec, RefInc, RefDec, RefDel };

MustCheck Status saveTraceInit(bool isStartup);
void saveTraceDestroy();
void saveTraceHelper(TraceOpts opts, void *buf, size_t count, int32_t slab);
Status dumpAllTraces(const char *fname);

MustInline void
saveTrace(TraceOpts opts,
          void *buf = NULL,
          size_t count = 1,
          int32_t slab = slabInvalid)
{
    if (XcalarConfig::get()->ctxTracesMode_) {
        saveTraceHelper(opts, buf, count, slab);
    }
}

#endif  // _SAVETRACE_H
