// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _ATOMICS_H_
#define _ATOMICS_H_

#include "util/AtomicTypes.h"

//!
//! \brief Atomically read a 32-bit atomic variable
//!
//! Atomically read the value of a 32-bit atomic variable and return it.
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline MustCheck int32_t NonNull(1) atomicRead32(const Atomic32 *a)
{
    return a->val;
}

//!
//! \brief Atomically read a 64-bit atomic variable
//!
//! Atomically read the value of a 64-bit atomic variable and return it.
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline MustCheck int64_t NonNull(1) atomicRead64(const Atomic64 *a)
{
    return a->val;
}

//!
//! \brief Atomically write a 32-bit atomic variable
//!
//! Atomically write the specified value to a 32-bit atomic variable
//!
//! \param[in] a 32-bit atomic variable
//!
static inline void NonNull(1) atomicWrite32(Atomic32 *a, int32_t rval)
{
    a->val = rval;
}

//!
//! \brief Atomically write a 64-bit atomic variable
//!
//! Atomically write the specified value to a 64-bit atomic variable
//!
//! \param[in] a 64-bit atomic variable
//!
static inline void NonNull(1) atomicWrite64(Atomic64 *a, int64_t rval)
{
    a->val = rval;
}

//!
//! \brief Atomically add to a 32-bit atomic variable
//!
//! Atomically add the specified value to a 32-bit atomic variable.  The *new*
//! value of the atomic variable is returned.
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable added to rval
//!
static inline int32_t NonNull(1) atomicAdd32(Atomic32 *a, int32_t rval)
{
    const int32_t savedRval = rval;

    asm volatile("lock; xaddl %0, %1" : "+r"(rval), "+m"(a->val) : : "memory");
    return rval + savedRval;
}

//!
//! \brief Atomically add to a 64-bit atomic variable
//!
//! Atomically add the specified value to a 64-bit atomic variable.  The *new*
//! value of the atomic variable is returned.
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable added to rval
//!
static inline int64_t NonNull(1) atomicAdd64(Atomic64 *a, int64_t rval)
{
    const int64_t savedRval = rval;

    asm volatile("lock; xaddq %0, %1" : "+r"(rval), "+m"(a->val) : : "memory");
    return rval + savedRval;
}

//!
//! \brief Atomically increment a 32-bit atomic variable
//!
//! Atomically increment a 32-bit atomic variable.  The *new* value of the
//! atomic variable is returned.
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable + 1
//!
static inline int32_t NonNull(1) atomicInc32(Atomic32 *a)
{
    return atomicAdd32(a, 1);
}

//!
//! \brief Atomically increment a 64-bit atomic variable
//!
//! Atomically increment a 64-bit atomic variable.  The *new* value of the
//! atomic variable is returned.
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable + 1
//!
static inline int64_t NonNull(1) atomicInc64(Atomic64 *a)
{
    return atomicAdd64(a, 1);
}

//!
//! \brief Atomically subtract from a 32-bit atomic variable
//!
//! Atomically subtract the specified value from a 32-bit atomic variable.
//! The *new* value of the atomic variable is returned.
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable minus rval
//!
static inline int32_t NonNull(1) atomicSub32(Atomic32 *a, int32_t rval)
{
    return atomicAdd32(a, -rval);
}

//!
//! \brief Atomically subtract from a 64-bit atomic variable
//!
//! Atomically subtract the specified value from a 64-bit atomic variable.
//! The *new* value of the atomic variable is returned.
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable minus rval
//!
static inline int64_t NonNull(1) atomicSub64(Atomic64 *a, int64_t rval)
{
    return atomicAdd64(a, -rval);
}

//!
//! \brief Atomically decrement a 32-bit atomic variable
//!
//! Atomically decrement a 32-bit atomic variable.  The *new* value of the
//! atomic variable is returned.
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable - 1
//!
static inline int32_t NonNull(1) atomicDec32(Atomic32 *a)
{
    return atomicAdd32(a, -1);
}

//!
//! \brief Atomically decrement a 64-bit atomic variable
//!
//! Atomically decrement a 64-bit atomic variable.  The *new* value of the
//! atomic variable is returned.
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable - 1
//!
static inline int64_t NonNull(1) atomicDec64(Atomic64 *a)
{
    return atomicAdd64(a, -1);
}

//!
//! \brief Atomically exchange a 32-bit atomic variable
//!
//! Atomically exchange the value of a 32-bit atomic variable with the
//! new value.  The *old* value of the atomic variable is returned.
//! a.k.a. atomicSwap() or atomicReadWrite()
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline int32_t NonNull(1) atomicXchg32(Atomic32 *a, int32_t newVal)
{
    asm volatile("lock; xchgl %0, %1"
                 : "=r"(newVal), "=m"(a->val)
                 : "0"(newVal), "m"(a->val)
                 : "memory");
    return newVal;
}

//!
//! \brief Atomically exchange a 64-bit atomic variable
//!
//! Atomically exchange the value of a 64-bit atomic variable with the
//! new value.  The *old* value of the atomic variable is returned.
//! a.k.a. atomicSwap() or atomicReadWrite()
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline int64_t NonNull(1) atomicXchg64(Atomic64 *a, int64_t newVal)
{
    asm volatile("lock; xchgq %0, %1"
                 : "=r"(newVal), "=m"(a->val)
                 : "0"(newVal), "m"(a->val)
                 : "memory");
    return newVal;
}

//!
//! \brief Atomically compare and exchange a 32-bit atomic variable
//!
//! Atomically compare and conditionally exchange the value of a 32-bit atomic
//! variable with the new value.  The *old* value of the atomic variable is
//! returned.
//! a.k.a. atomicCompareAndSwap(), atomicCAS(), or atomicReadIfEqualWrite()
//!
//! \param[in] a 32-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline int32_t NonNull(1)
    atomicCmpXchg32(Atomic32 *a, int32_t cval, int32_t newVal)
{
    int32_t prev;
    asm volatile("lock; cmpxchgl %1, %2;"
                 : "=a"(prev)
                 : "q"(newVal), "m"(a->val), "a"(cval)
                 : "memory");
    return prev;
}

//!
//! \brief Atomically compare and exchange a 64-bit atomic variable
//!
//! Atomically compare and conditionally exchange the value of a 64-bit atomic
//! variable with the new value.  The *old* value of the atomic variable is
//! returned.
//! a.k.a. atomicCompareAndSwap(), atomicCAS(), or atomicReadIfEqualWrite()
//!
//! \param[in] a 64-bit atomic variable
//!
//! \returns Value read from atomic variable
//!
static inline int64_t NonNull(1)
    atomicCmpXchg64(Atomic64 *a, int64_t cval, int64_t newVal)
{
    int64_t prev;
    asm volatile("lock; cmpxchgq %1, %2;"
                 : "=a"(prev)
                 : "q"(newVal), "m"(a->val), "a"(cval)
                 : "memory");
    return prev;
}

#endif  // _ATOMICTYPES_H_
