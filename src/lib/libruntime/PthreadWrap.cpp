// Copyright 2017 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
//

#include <pthread.h>
#include "runtime/Runtime.h"
#include "RuntimeStats.h"
#include "util/Atomics.h"
#include "constants/XcalarConfig.h"

//
// The following pthread wrappers are intended to maintain a stat of pthreads
// which are currently joinable but not yet joined with. This stat can be used
// to detect a leak in joinable pthreads - if a joinable pthread exits but is
// never joined with, then it's leaked - if such leaks accumulate over time, the
// kernel could eventually run out of pthread resources - at which point, a
// pthread_create would fail with the EAGAIN error code.
//
// If a process panics due to pthread_create's EAGAIN failure, check this stat
// (threadsJoinable_). If its value is much higher than the number of live
// pthreads (as reported by gdb's "info threads" command), then the failure is
// due to the leak in joinable pthreads. The next step would be to figure out
// where the leak is coming from.
//
// NOTE: these wrappers MUST be used, as opposed to the direct pthread_* calls,
// as implied by their presence in the src/data/banned.symbols list, except if
// the code does not initialize Runtime (required by the stats code, which these
// wrappers use) and/or the code is part of a stand-alone sub-system (like
// MgmtDaemon, or xcmonitor or XcalarSim).
//

int
sysThreadCreate(pthread_t *thread,
                const pthread_attr_t *attr,
                void *(*start_routine)(void *),
                void *arg)
{
    int retptc, retptg;
    int detachstate;
    XcalarConfig *xcalarConfig = XcalarConfig::get();
    pthread_attr_t attrTmp;

    if (attr == NULL) {
        retptc = pthread_attr_init(&attrTmp);
        if (retptc != 0) {
            return retptc;
        }

        retptc =
            pthread_attr_setstacksize(&attrTmp, xcalarConfig->thrStackSize_);
        if (retptc != 0) {
            return retptc;
        }
    } else {
        // XXX TODO Needs cleanup
        // Always set (override) the thread stack size for now.
        memcpy(&attrTmp, attr, sizeof(pthread_attr_t));

        retptc =
            pthread_attr_setstacksize(&attrTmp, xcalarConfig->thrStackSize_);
        if (retptc != 0) {
            return retptc;
        }
    }

    // @SymbolCheckIgnore
    retptc = pthread_create(thread, &attrTmp, start_routine, arg);
    if (retptc != 0) {
        return retptc;
    }

    if (attr) {
        retptg = pthread_attr_getdetachstate(attr, &detachstate);
    } else {
        // Default behavior
        detachstate = PTHREAD_CREATE_JOINABLE;
    }

    if (detachstate == PTHREAD_CREATE_JOINABLE) {
        StatsLib::statAtomicIncr64(
            (Runtime::get()->getStats())->threadsJoinable_);
    } else if (detachstate == PTHREAD_CREATE_DETACHED) {
        StatsLib::statAtomicIncr64(
            (Runtime::get()->getStats())->threadsDetachable_);
    }

    return retptc;
}

int
sysThreadJoin(pthread_t thread, void **retval)
{
    int ret;

    // Notation to silence banned symbols check
    // @SymbolCheckIgnore
    ret = pthread_join(thread, retval);

    //
    // If the sysThreadJoin was successful, decrement the counter since the
    // pthread has now exited, and been joined
    //
    if (ret == 0) {
        StatsLib::statAtomicDecr64(
            (Runtime::get()->getStats())->threadsJoinable_);
    }
    return ret;
}
