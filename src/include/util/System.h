// Copyright 2014 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#ifndef _SYSTEM_H_
#define _SYSTEM_H_

#include <typeinfo>
#include <unistd.h>
#include <sys/syscall.h>

#include <pthread.h>
#include <string.h>
#include <cstdlib>
#include <errno.h>
#include <assert.h>
#include <semaphore.h>
#include <string>

#include "primitives/Primitives.h"
#include "util/Math.h"

#ifdef SYS_gettid
static inline pid_t
sysGetTid()
{
    return (pid_t) syscall(SYS_gettid);
}
#else
#error "SYS_gettid unavailable on this system"
#endif

StatusCode sysErrnoToStatusCode(int errn);
Status sysErrnoToStatus(int errn);
Status sysEaiErrToStatus(int eaiErr);
void sysSleep(uint32_t numSecs);
void sysUSleep(uint64_t numUSecs);
void sysNSleep(uint64_t numNSecs);
Status sysCondTimedWait(pthread_cond_t *cond,
                        pthread_mutex_t *mutex,
                        uint64_t timeoutInUSec);
Status sysSemTimedWait(sem_t *sem, uint64_t timeoutInUSec);
int sysThreadCreate(pthread_t *thread,
                    const pthread_attr_t *attr,
                    void *(*start_routine)(void *),
                    void *arg);
int sysThreadJoin(pthread_t thread, void **retval);
ssize_t getBacktrace(void **buf, const ssize_t maxFrames);
void printBackTrace(char *destBuf,
                    int destBufSize,
                    int numFrames,
                    void **frames);
std::string runCmdAndReturnStdout(std::string cmd, Status *retStatus);
Status runSystemCmd(std::string cmd);
bool sysIsProcessCoreDumping(pid_t pid);

//!
//! \brief zero-out memory
//!
//! Zero-out the specified number of bytes of memory from the specified location
//!
//! \param[out] dst pointer to destination to zero
//! \param[in] bytes number of bytes to zero
//!
static inline void NonNull(1) memZero(void *dst, size_t bytes)
{
    memset(dst, 0, bytes);
}

static inline void
memBarrier()
{
    asm volatile("mfence" : : : "memory");
}

//!
//! \brief Convert timespec to nanoseconds
//!
//! Convert a timestamp expressed as a timespec into a 64-bit count of
//! nanoseconds.  This function is the inverse of clkNanosToTimespec().
//!
//! \param[out] ts the time to convert expressed as a struct timespec
//!
//! \returns the number of nanoseconds represented by ts
//!
static MustInline uint64_t NonNull(1)
    clkTimespecToNanos(const struct timespec *ts)
{
    assert((ts->tv_sec & INT64_MIN) == 0);
    assert((ts->tv_nsec & INT64_MIN) == 0);

    return (uint64_t) ts->tv_sec * NSecsPerSec + (uint64_t) ts->tv_nsec;
}

//!
//! \brief Convert nanoseconds to timespec
//!
//! Convert a timestamp expressed as a 64-bit count of nanoseconds into a
//! timespec.  This function is the inverse of clkTimespecToNanos().
//!
//! \param[in] nanos the time to convert expresses as nanoseconds
//! \param[out] ts the time converted into a struct timespec
//!
static inline void NonNull(2)
    clkNanosToTimespec(uint64_t nanos, struct timespec *ts)
{
    assertStatic(sizeof(typeid(ts->tv_sec)) >= sizeof(nanos));
    assertStatic(sizeof(typeid(ts->tv_nsec)) >= sizeof(nanos));
    assert((nanos & (uint64_t) INT64_MIN) == 0);

    ts->tv_sec = (int64_t) nanos / NSecsPerSec;
    ts->tv_nsec = (int64_t) nanos % NSecsPerSec;
}

//
// Calculate the interval between 2 timestamps
//
// Calculate the interval between 2 timestamps and return it expressed
// as a 64-bit count of nanoseconds.
//
// param[in] startTime the beginning timestamp upon which to compute the
//           interval
// param[in] endTime the ending timestamp upon which to compute the interval
//
// returns the number of nanoseconds in the interval between start and end
//
// note that depending upon clock type, end time may be before start time.
// caller should beware that a negative number may be returned if the clock
// went backwards. If this is not acceptible, caller should use
// clkGetElapsedTimeInNanosSafe() instead
static MustCheck MustInline int64_t NonNull(1, 2)
    clkGetElapsedTimeInNanos(const struct timespec *startTime,
                             const struct timespec *endTime)
{
    return (int64_t)(clkTimespecToNanos(endTime) -
                     clkTimespecToNanos(startTime));
}

// same as clkGetElapsedTimeInNanos() except guaranteed to return a postive
// interval. In the event the clock was turned backwards, 0 is returned.
static MustCheck MustInline uint64_t NonNull(1, 2)
    clkGetElapsedTimeInNanosSafe(const struct timespec *startTime,
                                 const struct timespec *endTime)
{
    const uint64_t startNanos = clkTimespecToNanos(startTime);
    const uint64_t endNanos = clkTimespecToNanos(endTime);

    if (unlikely(endNanos < startNanos)) {
        return 0;
    }

    return endNanos - startNanos;
}

MustCheck Status sparseMemCpy(void *dstIn,
                              const void *srcIn,
                              size_t size,
                              size_t blkSize,
                              size_t *bytesPhysOut);

#endif  // _SYSTEM_H_
