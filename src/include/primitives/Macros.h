// Copyright 2013 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _MACROS_H_
#define _MACROS_H_

#include <stddef.h>

template <typename T>
inline T
xcMax(T a, T b)
{
    return a > b ? a : b;
}

template <typename T>
inline T
xcMin(T a, T b)
{
    return a < b ? a : b;
}

// constrains x to be within the bounds [a,b]
// returns a if x < a
// returns b if x > b
// returns x otherwise
template <typename T>
inline const T&
constrain(const T& x, const T& a, const T& b)
{
    if (x < a) {
        return a;
    }
    if (x > b) {
        return b;
    }
    return x;
}

#define percent(val, pct) (((val) * (pct)) / 100)
#define roundDown(val, nearest) ((val) - (val) % (nearest))
#define roundUp(val, nearest)           \
    ((((val) % (nearest)) == 0) ? (val) \
                                : roundDown((val) + (nearest), (nearest)))
#define isAligned(val, nearest) ((val) == roundDown(val, nearest))

#define isEven(val) (((val) &0x1) == 0)
#define isOdd(val) (!(isEven(val)))
#define isDigit(val) ((val) >= '0' && (val) <= '9')
#define absolute(val) (((val) < 0) ? (-1 * (val)) : (val))

//! \brief logical xor
#ifndef __cplusplus
#define xor (c1, c2)(!(c1) != !(c2))
#endif

//! \brief determine the number of elements in an array
#define ArrayLen(a) (sizeof(a) / sizeof((a)[0]))

//! \brief force callers to check function's return value
#define MustCheck __attribute__((warn_unused_result))

//! \brief force the compiler to inline
#ifdef INLINES
#define MustInline inline __attribute__((always_inline))
#else
#define MustInline inline
#endif
#define NoInline __attribute__((noinline))

#define Unused inline __attribute__((unused))

//! \brief mandate callers pass non-NULL parameters
#define NonNull(args...) __attribute__((nonnull(args)))

#define Transparent __attribute__((__transparent_union__))

#define Aligned(numBytes) __attribute__((aligned(numBytes)))

// XXX FIXME should we keep these if we start using -fprofile-use?
#define likely(condition) __builtin_expect(!!(condition), 1)
#define unlikely(condition) __builtin_expect(!!(condition), 0)

#define ContainerOf(ptr, type, member) \
    (type*) ((uint8_t*) ptr - offsetof(type, member))

// Print variable name as a string.
#define StringifyEnum(var) (fprintf(stderr, #var "= %d\n", var));

#define __StringifyMacro(x) #x
#define StringifyMacro(x) __StringifyMacro(x)
#define FileLocationString (__FILE__ ":" StringifyMacro(__LINE__))

// extra indirection is done here to allow for macro parameter evaluation to
// occur prior to concatenation. e.x. TokenConcat(foo, __LINE__) will first
// evaluate __LINE__ before concatenating with foo.  otherwise we get
// foo__LINE__.
#define __TokenConcat(a, b) a##b
#define TokenConcat(a, b) __TokenConcat(a, b)

#define NotReached()             \
    do {                         \
        assert(0);               \
        __builtin_unreachable(); \
    } while (0)

// Used to jump to the cleanup/exit portion of a function on failure of some
// internal function call. 'CommonExit' label must be defined.
#define BailIf(cond)            \
    do {                        \
        if (unlikely((cond))) { \
            goto CommonExit;    \
        }                       \
    } while (false)

#define BailIfFailed(status) BailIf(!status.ok())

#define BailIfFailedMsg(moduleName, status, format, ...)         \
    do {                                                         \
        if (unlikely(!status.ok())) {                            \
            xSyslog(moduleName, XlogErr, format, ##__VA_ARGS__); \
            goto CommonExit;                                     \
        }                                                        \
    } while (false)
#define BailIfFailedTxnMsg(moduleName, status, format, ...)            \
    do {                                                               \
        if (unlikely(!status.ok())) {                                  \
            xSyslogTxnBuf(moduleName, XlogErr, format, ##__VA_ARGS__); \
            goto CommonExit;                                           \
        }                                                              \
    } while (false)

// Used to jump to the cleanup/exit portion of a function on allocation failure.
// Assumes 'CommonExit' label is defined and 'status' is the function's return
// value.
#define BailIfNull(ptr)                \
    do {                               \
        if (unlikely((ptr) == NULL)) { \
            status = StatusNoMem;      \
            goto CommonExit;           \
        }                              \
    } while (false)

// Used to jump to the cleanup/exit portion of a function on Xdb memory
// allocation failure. Assumes 'CommonExit' label is defined and 'status' is the
// function's return value.
#define BailIfNullXdb(ptr)                 \
    do {                                   \
        if (unlikely((ptr) == NULL)) {     \
            status = StatusNoXdbPageBcMem; \
            goto CommonExit;               \
        }                                  \
    } while (false)

// Used to jump to the cleanup/exit portion of a function on allocation failure.
// Assumes 'CommonExit' label is defined and 'status' is the function's return
// value.
#define BailIfNullMsg(ptr, bailStatus, moduleName, format, ...)  \
    do {                                                         \
        if (unlikely((ptr) == NULL)) {                           \
            status = bailStatus;                                 \
            xSyslog(moduleName, XlogErr, format, ##__VA_ARGS__); \
            goto CommonExit;                                     \
        }                                                        \
    } while (false)

// Jumps to cleanup/exit portion of a function with the given status if ptr
// is NULL. Assumes 'CommonExit' label is defined and 'status' is the function's
// return value.
#define BailIfNullWith(ptr, bailStatus) \
    do {                                \
        if (unlikely((ptr) == NULL)) {  \
            status = (bailStatus);      \
            goto CommonExit;            \
        }                               \
    } while (false)

// Jumps to cleanup/exit portion of a function with the given status if ptr
// is NULL. Assumes 'CommonExit' label is defined and 'status' is the function's
// return value.
#define BailIfFailedWith(ret, bailStatus) \
    do {                                  \
        if (unlikely((ret) != 0)) {       \
            status = (bailStatus);        \
            goto CommonExit;              \
        }                                 \
    } while (false)

#ifdef DEBUG
#define debug(stmt) stmt
#else
#define debug(stmt)
#endif

#ifdef DEBUG
#define xcAssertIf(cond, value) \
    do {                        \
        if (cond) {             \
            assert(value);      \
        } else {                \
            assert(!(value));   \
        }                       \
    } while (false)
#else  // DEBUG
#define xcAssertIf(cond, value)
#endif  // DEBUG

#define __XLR2STR(x) #x
#define XLR2STR(x) __XLR2STR(x)

#define MemFreeAndNullify(buf) \
    do {                       \
        memFree(buf);          \
        buf = NULL;            \
    } while (false)

#define IntTypeToStrlen(type)                 \
    (2 + (sizeof(type) <= 1                   \
              ? 3                             \
              : sizeof(type) <= 2             \
                    ? 5                       \
                    : sizeof(type) <= 4       \
                          ? 10                \
                          : sizeof(type) <= 8 \
                                ? 20          \
                                : sizeof(int[-2 * (sizeof(type) > 8)])))

#endif  // _MACROS_H_
