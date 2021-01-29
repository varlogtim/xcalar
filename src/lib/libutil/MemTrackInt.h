// Copyright 2015 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MEMTRACKINT_H_
#define _MEMTRACKINT_H_

#include "util/MemTrack.h"
#include "util/Atomics.h"
#include "util/IntHashTable.h"
#include "util/StringHashTable.h"
#include "hash/Hash.h"

static constexpr const int MemTrackBtFrames = 15;
static constexpr const uint32_t MemTrackBtBuffer = 4096;

struct MemTrackTag {
    static uint64_t hashBackTrace(void *const *addList, int frames)
    {
        int64_t btLen = frames * sizeof(addList[0]);
        return hashBufFast((const uint8_t *) addList, btLen);
    }
    uint64_t getUniquifier() const { return traceHash; }

    IntHashTableHook hook;
    Atomic64 memSize;
    const char *tag;
    const char *fileLocation;
    mutable uint64_t traceHash;
#ifdef MEMTRACKTRACE
    void *addressList[MemTrackBtFrames];
    int nFrames;
#endif  // MEMTRACKTRACE
    void freeTag();
};

struct MemAllocHeader {
    size_t size;
    // If tag is NULL, that means this header isn't being tracked, and the tag
    // isn't valid
    MemTrackTag *tag;
    char usrPtr[0];
};

struct MemAlignMemPtr {
    IntHashTableHook hook;
    void *getUsrPtr() const { return usrPtr; };
    size_t size;
    MemTrackTag *tag;
    void *usrPtr;
    void freeMemPtr();
};

#endif  // _MEMTRACKINT_H_
