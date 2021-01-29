// Copyright 2015 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// Header file for Xcalar syslog functions

#ifndef _XLOG_H_
#define _XLOG_H_

#include <syslog.h>
#include <stdarg.h>
#include "primitives/Primitives.h"
#include "util/Stopwatch.h"

enum XcalarSyslogFromSignalHandler {
    NotFromSignalHandler = false,
    FromSignalHandler = true,
};

enum XcalarSyslogTxnBuf {
    TxnBufNoCopy = 0x1,
    TxnBufCopy = 0x2,
};

// Facilities that use xSyslog - controls Init processing
enum SyslogFacility {
    SyslogFacilityUsrNode = 1,
    SyslogFacilityMgmtD = 2,
    SyslogFacilityChildNode = 3,
    SyslogFacilityTest = 4,
    SyslogFacilityDiagnostic = 5,
    SyslogFacilityCli = 6,
    SyslogFacilityMonitor = 7,
    SyslogFacilityNotSpecified = 8,
};

void xsyslogSetLevel(int level);
int xsyslogGetLevel();
int xsyslogGetFlushPeriod();
int xsyslogGetFlushPeriodMin();

// Initialize and terminate Xcalar syslog processing function prototypes
Status xsyslogInit(SyslogFacility facility, const char *logDirPath);

// Following should only be called when a syslog facility has been booted up
// and after the call to xsyslogInit() has been made. Internally, this routine
// will know which facility has been booted up, since xsyslogInit() has been
// invoked already.
void xsyslogFacilityBooted();

void xsyslogDestroy();

// Write a message to the syslog, and possibly also copy to the TxnBuf
NoInline void xSyslogInt(const char *moduleName,
                         XcalarSyslogMsgLevel inputLevel,
                         XcalarSyslogTxnBuf TxnBufFlag,
                         XcalarSyslogFromSignalHandler fromSigHdlr,
                         const char *fmt,
                         ...) __attribute__((format(printf, 5, 6)))
NonNull(1, 5);

// The TxnBufFlag argument dictates if log is also copied to TxnBuf or not
NoInline void xSyslogV(const char *moduleName,
                       XcalarSyslogMsgLevel inputLevel,
                       XcalarSyslogTxnBuf TxnBufFlag,
                       XcalarSyslogFromSignalHandler fromSigHdlr,
                       const char *fmt,
                       va_list ap) NonNull(1, 5);

NoInline void xSyslogFlush();
void xcalarExit(int status);
void xcalarAbort();
NoInline Status xSyslogFlushPeriodic(int32_t period);

void setLogLevelViaConfigFile();
NoInline Status parseLogLevel(const char *level, uint32_t *newLevel);
NoInline Status parseAndProcessLogLevel(const char *level);
void stopwatchReportMsecs(Stopwatch *stopwatch, const char *mod);

#ifdef XLR_OBFUSCATE_LOGS
// Obfusicate the FILE:LINE pair.  The hardware CRC calculation should be very
// fast relative to xSyslogInt/xSyslogV.
#include "hash/Hash.h"
#define xSyslogM(moduleName, inputLevel, TxnBufFlag, fromSigHdlr, FMT, ...) \
    do {                                                                    \
        if (likely(hashIsInit()))                                           \
            xSyslogInt(moduleName,                                          \
                       inputLevel,                                          \
                       TxnBufFlag,                                          \
                       fromSigHdlr,                                         \
                       "Hash 0x%08x: " FMT,                                 \
                       hashCrc32c(0,                                        \
                                  __FILE__ XLR2STR(__LINE__),               \
                                  sizeof(__FILE__ XLR2STR(__LINE__))),      \
                       ##__VA_ARGS__);                                      \
        else                                                                \
            xSyslogInt(moduleName,                                          \
                       inputLevel,                                          \
                       TxnBufFlag,                                          \
                       fromSigHdlr,                                         \
                       "EARLY INIT: " FMT,                                  \
                       ##__VA_ARGS__);                                      \
    } while (0)
#else
#define xSyslogM(moduleName, inputLevel, TxnBufFlag, fromSigHdlr, FMT, ...) \
    do {                                                                    \
        xSyslogInt(moduleName,                                              \
                   inputLevel,                                              \
                   TxnBufFlag,                                              \
                   fromSigHdlr,                                             \
                   "File %s: Line %d: " FMT,                                \
                   __FILE__,                                                \
                   __LINE__,                                                \
                   ##__VA_ARGS__);                                          \
    } while (0)
#endif

#define xSyslog(moduleName, inputLevel, FMT, ...) \
    do {                                          \
        xSyslogM(moduleName,                      \
                 inputLevel,                      \
                 TxnBufNoCopy,                    \
                 NotFromSignalHandler,            \
                 FMT,                             \
                 ##__VA_ARGS__);                  \
    } while (0)

#define xSyslogTxnBuf(moduleName, inputLevel, FMT, ...) \
    do {                                                \
        xSyslogM(moduleName,                            \
                 inputLevel,                            \
                 TxnBufCopy,                            \
                 NotFromSignalHandler,                  \
                 FMT,                                   \
                 ##__VA_ARGS__);                        \
    } while (0)

#define xSyslogFromSig(moduleName, inputLevel, FMT, ...) \
    do {                                                 \
        xSyslogM(moduleName,                             \
                 inputLevel,                             \
                 TxnBufNoCopy,                           \
                 FromSignalHandler,                      \
                 FMT,                                    \
                 ##__VA_ARGS__);                         \
    } while (0)

#endif  // _XLOG_H_
