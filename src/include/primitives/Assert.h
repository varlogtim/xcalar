// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _PASSERT_H_
#define _PASSERT_H_

#include "primitives/Primitives.h"
#include "primitives/Status.h"

// For CHECK().
//
// TODO(Oleg): this include is not right as it violates the dependency order.
//             Yet we can get away with it as it will pull preprocessor only
//             and everyone links to libsys anyway.
#include "sys/Check.h"

#ifndef XLR_QA
#include "assert.h"
#endif

//! \brief compile-time assertion
#ifndef assertStatic
#define assertStatic(cond) \
    switch (0) {           \
    case 0:                \
    case cond:;            \
    }
#endif

// Sanity check for the Debug build.
#if defined(DEBUG) && !defined(ENABLE_ASSERTIONS)
#error The DEBUG build must have assertions!
#endif

// The verify() macro is a Xcalar-specific thing that works differently
// depending on the build type:
//
//  Debug:    same as DCHECK()
//  QA:       same as CHECK()
//  Prod:     "cond" will be evaluated, yet its result will not be checked
//
#if defined(ENABLE_ASSERTIONS)
#define verify(cond) CHECK(cond)
#else
#define verify(cond) ((void) (cond))
#endif

// The verifyOk() macro follows the Xcalar-specific pattern defined by verity()
// and checks the given status against 'StatusOk'.
// To see what status is in GDB, use info reg rax.
#define verifyOk(status) verify((status) == StatusOk)

#ifdef __cplusplus
extern "C" {
#endif

// @SymbolCheckIgnore
void panic(const char *fmt, ...);
// Don't dump a core.
// @SymbolCheckIgnore
void panicNoCore(const char *fmt, ...);

#ifdef __cplusplus
}
#endif

// Used to document asserts that are not valid and should be replaced by runtime
// error handling. It's often easier to go through and identify these before you
// think through error propagation.
#define buggyAssert(cond) assert(cond)

#define buggyPanic(fmt, ...) panic(fmt, ##__VA_ARGS__)

#endif  // _PASSERT_H_
