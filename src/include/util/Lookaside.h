// Copyright 2014 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _LOOKASIDE_H
#define _LOOKASIDE_H

#include <semaphore.h>
#include <pthread.h>
#include "primitives/Primitives.h"
#include "runtime/Semaphore.h"
#include "runtime/Mutex.h"

typedef enum {
    LookasideOptsNone = 0,
    LookasideEnableAllocWait = 1 << 0,
} LookasideListOpts;

struct LookasideListElt {
    union {
        struct {
            struct LookasideListElt *prev;
            struct LookasideListElt *next;
        };
        uint8_t content[0];
    };
};

struct LookasideList {
    size_t physicalEltSize;
    unsigned numElements;
    size_t bufSize;
    void *buffer;
    LookasideListOpts options;
    Semaphore sem;
    Mutex freeListLock;
    LookasideListElt freeListAnchor;
};

extern MustCheck Status lookasideInit(size_t elementSize,
                                      unsigned numElements,
                                      LookasideListOpts lookasideOpts,
                                      LookasideList *lookasideListOut)
    NonNull(4);

extern void lookasideDestroy(LookasideList *lookasideList) NonNull(1);
extern void *lookasideAlloc(LookasideList *lookasideList) NonNull(1);
extern void lookasideFree(LookasideList *lookasideList, void *buf)
    NonNull(1, 2);
#endif  // _LOOKASIDE_H
