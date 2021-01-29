// Copyright 2015-2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// MonLogMsg.h: Macros used to write cluster monitor messages

#ifndef _MON_LOG_MSG_H
#define _MON_LOG_MSG_H

#include <stdio.h>
#include <sys/time.h>
#include "sys/XLog.h"

static inline uint64_t
CurrentTime()
{
    struct timeval tv;
    // @SymbolCheckIgnore
    gettimeofday(&tv, NULL);
    return (uint64_t) tv.tv_sec * 1000000 + (uint64_t) tv.tv_usec;
}

extern int logLevel;

#define WARNING(moduleName, fmt, ...) \
    xSyslog(moduleName, XlogWarn, " " fmt, ##__VA_ARGS__);

// monitorTest.sh depends on ERROR output also going to stderr. So copy the
// log to stderr explicitly here (the Stderr flag to xSyslog() is eventually
// going away since no other sub-system needs this today - restricting it
// just to monitor code here allows the rest of the world to be cleaned up).
// TODO: Eventually, we should also investigate fixing this for monitor.
#define ERROR(moduleName, fmt, ...)                                  \
    xSyslog(moduleName, XlogErr, " " fmt, ##__VA_ARGS__);            \
    fprintf(stderr, "%ld: ERR: " fmt, CurrentTime(), ##__VA_ARGS__); \
    fprintf(stderr, "\n");                                           \
    fflush(stderr);

#define INFO(moduleName, fmt, ...) \
    xSyslog(moduleName, XlogInfo, " " fmt, ##__VA_ARGS__);

#define MDEBUG(moduleName, fmt, ...) \
    xSyslog(moduleName, XlogDebug, " " fmt, ##__VA_ARGS__);
#endif
