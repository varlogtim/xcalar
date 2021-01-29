// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _REFCOUNT_H_
#define _REFCOUNT_H_

#include <assert.h>

#include "util/Atomics.h"

//!
//! \defgroup RefCount Object reference counting utilities
//!
//! There are 3 ways to obtain a reference on an object:
//!
//! 1.  Create the object and initialize the reference with refInit()
//! 2.  Given a valid reference to an object, create another reference to the
//!     object via refGet()
//! 3.  Lookup the object through some other means
//!     (e.x. via a hash table lookup, etc.) and atomically create a reference
//!     via refGet().
//!
//! methods 1 and 2 can safely be done lock free.  method 3 requires that
//! the caller serialize against a refPut() that would result in the reference
//! count dropping to 0.  (e.x. the function that destroys an object should
//! first pull it out of the hash table to ensure no other caller can obtain
//! a new reference via method 3).
//!
//! Once a reference count reaches 0, the object's destructor function is
//! invoked.  It is a bug to call refGet() for an object whose reference count
//! has already reached 0.
//!

struct RefCount;

typedef void (*RefDestructorFn)(RefCount *);

struct RefCount {
    Atomic32 count;
    RefDestructorFn destructorFn;
};

//!
//! \brief Initialize a reference counter
//!
//! Initialize a reference counter
//!
//! \param[out] refCount reference counter to initialize
//! \param[in] destructorFn destructor function to call once the reference count
//!            reaches 0
//!
static inline void NonNull(1, 2)
    refInit(RefCount *refCount, RefDestructorFn destructorFn)
{
    refCount->destructorFn = destructorFn;
    atomicWrite32(&refCount->count, 1);
}

//!
//! \brief Get a reference on an object
//!
//! Get a reference on an object
//!
//! \param[in,out] refCount reference counter from which to acquire a reference
//!
static MustInline void
refGet(RefCount *refCount)
{
    int32_t newCount;

    newCount = atomicInc32(&refCount->count);
    assert(newCount > 1);
}

//!
//! \brief Release a reference on an object
//!
//! Release a reference on an object.  Note that the object in question may be
//! immediately destroyed prior to refPut() returning to the caller.  As such,
//! the caller must cease accessing the containing object after invoking
//! refPut()
//!
//! \param[in,out] refCount reference counter from which to acquire a reference
//!
static MustInline void
refPut(RefCount *refCount)
{
    int32_t newCount;

    newCount = atomicDec32(&refCount->count);
    assert(newCount >= 0);
    if (newCount == 0) {
        refCount->destructorFn(refCount);
    }
}

static inline uint32_t
refRead(const RefCount *refCount)
{
    return atomicRead32(&refCount->count);
}

//! \brief given a reference count, dig out its containing structure
#define RefEntry(refCount, type, member) ContainerOf(refCount, type, member)

#endif  // _REFCOUNT_H_
