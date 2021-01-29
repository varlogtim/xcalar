// Copyright 2015 - 2020 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// XSyslog.c - Support for writing messages to the system log
//
// Important note: xsyslogInit is one of the earliest functions called and
//                 should not rely on other Xcalar services being available.
//
// Important note 2: this code was developed and tested with the rsyslog
//                   package.  It should work with others as well but it
//                   has not been tested.
//
// The comment @SymbolCheckIgnore appears before every line that contains one
// of the linux syslog functions as well as printf and fprintf, since other
// programs should not use them.

#include <string.h>
#include <cstdlib>
#include <syslog.h>  // system log functions
#include <stdio.h>
#include <time.h>  // clock functions

#include "util/Atomics.h"
#include "util/AtomicTypes.h"
#include "config/Config.h"
#include "operators/GenericTypes.h"
#include "primitives/Primitives.h"
#include "sys/XLog.h"
#include "child/Child.h"
#include "util/MemTrack.h"
#include "msg/Message.h"

static NodeId myNode = 0xdeadbeef;
static bool xsyslogInited = false;
static bool xsyslogDestroyed = false;
static SyslogFacility xsyslogFacility = SyslogFacilityNotSpecified;
static Atomic64 messagesTotal;
static Atomic64 messagesWritten;
static Atomic64 messagesFiltered;
static Atomic64 messagesTruncated;
static Atomic64 messageCounts[LOG_DEBUG + 1];
static Atomic64 xcalarExitInProg;

static struct timespec initTime;
static struct timespec destroyTime;

static constexpr const char *moduleName = "XSyslog";
static const int MaxMsgSize = 2 * MB;

static char progname[60] = "\0";
static char hostname[MaxHostName + 1];

/*
 * The use of BUFSIZ is tied to the call to setbuf(3) during xcalarLog init.
 * If this size is changed to any other value, setvbuf(3) must be used.
 */
static char xcalarLogBuffer[BUFSIZ]; /* typically BUFSIZE is 8K bytes */

// Set default logging level based on debug / non-debug build
// (LOG_UPTO requiures a constant, thus the duplcate statements)
#ifdef DEBUG
static int logLevel = LOG_DEBUG;
static bool logDebugBuild = true;
#else
static int logLevel = LOG_INFO;
static bool logDebugBuild = false;
#endif

static char logFileName[PATH_MAX];
static FILE *logFileFp = NULL;

// Logs can be buffered only in prod builds.

static bool logBufferOn = false;        // are logs being buffered?
static bool logBufferAllowed = false;   // is facility allowed log buffering?
static bool logFacilityBooted = false;  // has facility booted up?

// following two are for diagnostic purposes
static bool logUnBufFailed = false;  // did attempt to unbuffer fail?
static bool logXcBufInUse = false;   // xcalarLogBuffer[] is being used

// Function prototype(s)

// Initialize Xcalar system log support

void
xsyslogSetLevel(int level)
{
    logLevel = level;
}

int
xsyslogGetLevel()
{
    return logLevel;
}

Status
parseLogLevel(const char *level, uint32_t *newLevel)
{
    *newLevel = XlogInval;

    if (strcasecmp(level, "Emerg") == 0) {
        *newLevel = XlogEmerg;
    } else if (strcasecmp(level, "Alert") == 0) {
        *newLevel = XlogAlert;
    } else if (strcasecmp(level, "Crit") == 0) {
        *newLevel = XlogCrit;
    } else if (strcasecmp(level, "Err") == 0) {
        *newLevel = XlogErr;
    } else if (strcasecmp(level, "Warn") == 0) {
        *newLevel = XlogWarn;
    } else if (strcasecmp(level, "Note") == 0) {
        *newLevel = XlogNote;
    } else if (strcasecmp(level, "Info") == 0) {
        *newLevel = XlogInfo;
    } else if (strcasecmp(level, "Debug") == 0) {
        *newLevel = XlogDebug;
    } else if (strcasecmp(level, "NoChange") == 0) {
        // log level must remain unchanged
        *newLevel = XlogInval;
    } else {
        return StatusCliParseError;
    }

    return StatusOk;
}

Status
parseAndProcessLogLevel(const char *level)
{
    Status status = StatusOk;
    uint32_t newLogLevel = XlogInval;

    status = parseLogLevel(level, &newLogLevel);
    if (status != StatusOk) {
        goto CommonExit;
    }
    if (newLogLevel == XlogInval) {
        status = StatusInvalidLogLevel;
        goto CommonExit;
    }

    xsyslogSetLevel(newLogLevel);
    xSyslogFlush();

CommonExit:
    return status;
}

/*
 * Initialization routine to turn on log buffering using Xcalar log buffer.
 *
 * NOTE: Result may be unbuffered ! Depending on success/failure of setvbuf(3).
 *
 * There are 3 possible outcomes (in descending order of likelihood):
 *
 * (A) Logs are buffered using xcalarLogBuffer[] (desired and likely outcome)
 * (B) Logs are not buffered at all
 * (C) Logs are buffered but libc owns buffer (undesirable and least likely)
 *
 * If our attempt to get outcome (A) fails, we try to get outcome (B), since
 * log buffering with buffer owned by libc isn't desirable (getting logs out of
 * libc buffer in a core may not be possible) - but if (B) also fails, then we
 * choose (C) since failing cluster boot to avoid (C) doesn't seem right.
 *
 * Although this is meant for usrnode/prod-build, it may be invoked in debug
 * build/xcmonitor, etc. (those meant to have unbuffered logs), if the attempt
 * to unbuffer fails (see xsyslogBuffOffInit()).
 */
static Status
xsyslogBufferOnInit()
{
    Status status = StatusOk;
    int ret;

    assert(logBufferAllowed && logFileFp != stderr && !logBufferOn);
    if ((ret = setvbuf(logFileFp, xcalarLogBuffer, _IOFBF, BUFSIZ)) != 0) {
        /*
         * Man page says setvbuf() returns non-zero on failure, and that "It MAY
         * set errno on failure" - given errno isn't reliable, report both
         * return code, and errno via a syslog.
         */
        xSyslog(moduleName,
                XlogErr,
                "Failed to use xcalarLogBuffer, setvbuf failed "
                "retcode str %s, errno str %s (%s)",
                strGetFromStatus(sysErrnoToStatus(ret)),
                strGetFromStatus(sysErrnoToStatus(errno)),
                strGetFromStatus(StatusBufferOnFailed));
        /*
         * Our attempt to use a buffer owned by Xcalar failed. If we do
         * nothing here, the stream will still be buffered since by default
         * all streams are buffered - but the buffer will be owned by libc.
         * If there's a core, with logs in libc's buffer, it'd be hard to
         * get at these logs. Rather than live with an undiagnosable system
         * in such a scenario (when logs are most needed), just unbuffer
         * at this point. If even this fails, then continue on with
         * buffering on (but owned by libc) but at least this is done
         * as the last resort.
         */
        ret = setvbuf(logFileFp, NULL, _IONBF, 0);
        if (ret != 0) {
            /* Outcome C */
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to unbuffer, setvbuf failed %s; now using libc buf",
                    strGetFromStatus(sysErrnoToStatus(ret)));
            logBufferOn = true;
        }  // else Outcome B
    } else {
        /* Outcome A */
        logXcBufInUse = true;
        logBufferOn = true;
    }
    return status;
}

/*
 * Initialization routine to unbuffer logs (debug builds, XPU, xcmonitor, etc.)
 *
 * NOTE: Result may be buffered ! Depending on success/failure of setvbuf(3).
 *
 * Three possible outcomes (in descending order of likelihood):
 *
 * (A) Logs are unbuffered (desired and most likely outcome)
 * (B) Logs are buffered using XcalarLogBuffer[]
 * (C) Logs are buffered using libc buffer (undesirable and least likely)
 * (D) Hard failure (for XPUs when unbuffering fails)
 */
static Status
xsyslogBufferOffInit(SyslogFacility facility)
{
    int setvbufRet = 0;
    Status status = StatusOk;

    assert(!logBufferAllowed && !logBufferOn);
    /*
     * A newly fopen()'ed file is fully buffered by default, and so if
     * buffering isn't allowed (XPU, debug-build, xcmonitor, stderr), invoke
     * setvbuf() to explicitly unbuffer.
     *
     * For stderr, even though it's unbuffered by default, just make sure by
     * explicitly invoking unbuffering.
     */
    setvbufRet = setvbuf(logFileFp, NULL, _IONBF, 0);
    if (setvbufRet != 0 && logFileFp != stderr) {
        logUnBufFailed = true;
        if (facility != SyslogFacilityChildNode) {
            /*
             * If attempt to unbuffer fails, logFileFp, if not stderr, will be
             * fully buffered since all file streams are buffered by default.
             * But the buffer will be owned by libc - difficult (if possible at
             * all) to get logs out of libc buffers, from a core. So, if
             * buffering is to be left on, might as well try to use the
             * XcalarLogBuffer[] instead. Do this by calling
             * xsyslogBufferOnInit().
             */
            logBufferAllowed = true;
            xsyslogBufferOnInit();  // outcomes B OR C in order of likelihood
        } else {
            /*
             * Outcome D:
             *
             * NOTE: for XPU, logs must be unbuffered since they all share the
             * same log file - so if unbuffering fails, fail the log init.
             * Otherwise, the xpu shared log could have mangled log lines. This
             * should be very rare, if at all, and so failing XPU shouldn't
             * matter. It's better to know why this happened than to let an
             * undiagnosable cluster be booted up.
             */
            status = StatusCantUnbufferLogs;
        }
        xSyslog(moduleName,
                XlogWarn,
                "Failed to unbuffer logs (because %s)",
                strGetFromStatus(sysErrnoToStatus(setvbufRet)));
    }  // else outcome A
    return status;
}

static Status
xsyslogBufferInit(SyslogFacility facility)
{
    Status status;
    /*
     * Although Xcalar may print some stuff to stdout, it's not the logs,
     * and so stdout buffering isn't needed (it's unbuffered by default if
     * studout isn't a terminal). If we decide to leave higher log levels such
     * as XlogNote or XlogInfo in the prod build, there may be many invocations
     * of logging during operations, and many system calls to write(2) may occur
     * if the stream isn't buffered, and disk activity. The buffering should
     * reduce the potential of this occurring. This is at the cost of some
     * complexity in the code to ensure that the log is flushed at program
     * exit time (by banning the use of exit, and replacing with xcalarExit() -
     * the latter will do the flushing and sync'ing).
     */
    if (!logDebugBuild && facility != SyslogFacilityChildNode &&
        facility != SyslogFacilityMonitor && logFileFp != stderr) {
        /*
         * Buffer only if:
         * - not child
         * - not xcmonitor
         * - subsystem uses a proper logfile (not stderr)
         * - this is a prod build
         */
        logBufferAllowed = true;
        status = xsyslogBufferOnInit();
    } else {
        status = xsyslogBufferOffInit(facility);
    }
    return status;
}
// Logs are flushed every logFlushPeriod secs; periodic flushing is off if -1
// This is relevant only if logBufferOn is true
// The period can't be smaller than that specified in logFlushPeriodMin
static int32_t logFlushPeriod = -1;
static int32_t logFlushPeriodMin = 5;

// The logFlushPeriodThreadId is valid only if logFlushPeriodThreadValid is true
static bool logFlushPeriodThreadValid = false;
static pthread_t logFlushPeriodThreadId;

int
xsyslogGetFlushPeriod()
{
    return logFlushPeriod;
}

int
xsyslogGetFlushPeriodMin()
{
    return logFlushPeriodMin;
}

Status
xsyslogInit(SyslogFacility facility, const char *logDir)
{
    Status status = StatusUnknown;
    int ii;
    int ret, ghret;
    bool haveLog = false;
    bool logFileTrunc = false;
    bool logFileOpFail = false;

    if (facility == SyslogFacilityChildNode) {
        assert(logLevel >= XlogEmerg);
    }
    clock_gettime(CLOCK_REALTIME, &(initTime));
    messagesTotal.val = 0;
    messagesWritten.val = 0;
    messagesFiltered.val = 0;
    messagesTruncated.val = 0;
    xcalarExitInProg.val = 0;

    for (ii = 0; ii < LOG_DEBUG + 1; ii++) {
        messageCounts[ii].val = 0;
    }

    xsyslogFacility = facility;
    // Set up the program name based on the facility type
    switch (facility) {
    case SyslogFacilityUsrNode:
        myNode = Config::get()->getMyNodeId();
        snprintf(progname, sizeof(progname), "Xcalar XCE N%04d", myNode);
        assert(logDir);
        ret = snprintf(logFileName,
                       sizeof(logFileName),
                       "%s/node.%d.log",
                       logDir,
                       myNode);
        if (ret >= (int) sizeof(logFileName)) {
            logFileTrunc = true;
        } else {
            haveLog = true;
        }
        break;

    case SyslogFacilityMgmtD:
        snprintf(progname, sizeof(progname), "Xcalar MgmtD");
        assert(logDir);
        ret = snprintf(logFileName,
                       sizeof(logFileName),
                       "%s/xcmgmtd.log",
                       logDir);
        if (ret >= (int) sizeof(logFileName)) {
            logFileTrunc = true;
        } else {
            haveLog = true;
        }
        break;

    case SyslogFacilityChildNode:
        snprintf(progname, sizeof(progname), "Xcalar XPU Node");
        assert(logDir);
        ret = snprintf(logFileName, sizeof(logFileName), "%s/xpu.log", logDir);
        if (ret >= (int) sizeof(logFileName)) {
            logFileTrunc = true;
        } else {
            haveLog = true;
        }
        break;

    case SyslogFacilityTest:
        snprintf(progname, sizeof(progname), "Xcalar Test");
        if (logDir) {
            ret = snprintf(logFileName,
                           sizeof(logFileName),
                           "%s/xctest.log",
                           logDir);
            if (ret >= (int) sizeof(logFileName)) {
                logFileTrunc = true;
            } else {
                haveLog = true;
            }
        }
        break;

    case SyslogFacilityDiagnostic:
        snprintf(progname, sizeof(progname), "Xcalar Diagnostic");
        assert(logDir);
        ret =
            snprintf(logFileName, sizeof(logFileName), "%s/xcdiag.log", logDir);
        if (ret >= (int) sizeof(logFileName)) {
            logFileTrunc = true;
        } else {
            haveLog = true;
        }
        break;

    case SyslogFacilityCli:
        snprintf(progname, sizeof(progname), "Xcalar CLI");
        break;

    case SyslogFacilityMonitor:
        snprintf(progname, sizeof(progname), "Xcalar Monitor");
        assert(logDir);
        ret = snprintf(logFileName,
                       sizeof(logFileName),
                       "%s/xcmonitor.log",
                       logDir);
        if (ret >= (int) sizeof(logFileName)) {
            logFileTrunc = true;
        } else {
            haveLog = true;
        }
        logLevel = XlogVerbose;
        // XXX: during monitor dev, we need all info in prod builds
        // remove this later or scrub level, and leave in since monitor
        // logs shouldn't be in performance path
        break;

    // Expected if xSyslog is called without a prior xsyslogInit
    case SyslogFacilityNotSpecified:
        snprintf(progname, sizeof(progname), "Xcalar *****");
        break;

    default:
        status = StatusUnimpl;
        goto xsyslogInitExit;
        break;
    }

    if (haveLog) {
        //
        // Open in append mode! When multiple processes such as the child
        // processes, write to the same file stream using fprintf(), two things
        // must be true for the writes to not be interleaved:
        // - the file should be opened in append mode
        // - buffering should be OFF
        // Both are true for child processes, b/c their log is shared
        //
        logFileFp = fopen(logFileName, "a"); /* open for append */
        if (logFileFp == NULL) {
            logFileOpFail = true;
            logFileFp = stderr;  // default to stderr
        }
    } else {
        // Some cases there's no logdir, in which case logs go to stderr,
        // which, since it's rare, and of limited use, doesn't need to be
        // buffered
        logFileFp = stderr;
    }

    ghret = gethostname(hostname, sizeof(hostname));
    if (ghret == -1) {
        if (errno == ENAMETOOLONG) {
            hostname[MaxHostName] = '\0'; /* truncated hostname but report it */
        } else {
            snprintf(hostname, sizeof(hostname), "Unknown");
        }
    }

    xsyslogInited = true;

    status = xsyslogBufferInit(facility);

    if (status != StatusOk) {
        goto xsyslogInitExit;
    }

    if (facility != SyslogFacilityChildNode) {
        xSyslog(moduleName, XlogNote, "Syslog processing initialized");
    }

    if (facility == SyslogFacilityNotSpecified) {
        xSyslog(moduleName,
                XlogWarn,
                "Unexpected system log call - program name not available");
    }

    if (logFileTrunc) {
        xSyslog(moduleName,
                XlogErr,
                "Log file (%s) truncated; logging to stderr",
                logFileName);
    } else if (logFileOpFail) {
        xSyslog(moduleName,
                XlogErr,
                "Can't open log file (%s); logging to stderr",
                logFileName);
    }

    status = StatusOk;

xsyslogInitExit:

    return status;
}

//
// This interface turns ON or OFF, periodic flushing of logs if they're
// buffered. A thread is spawned to do the periodic flushing. The API for a
// one-time flush can always be invoked periodically, if for some reason, this
// interface fails to kick off periodic flushing (e.g. thread creation fails).
//
//  period > 0: seconds b/w log flushes (turns on periodic flushing)
// period =  0: no-op (may in future be used to turn off buffering completely)
// period = -1: turns off periodic flushing
//
// XXX: Period 0: we may support turning off buffering via period == 0, if it
// can be made to work (see comment in xsyslogBufferOn()).  Currently, using
// period == 0 to turn off buffering, isn't supported.
//
// Turning buffering off, once it has been turned on, is problematic since
// setbuf can be called only once on the stream after fopen'ing the file, and
// before any I/O is done on the stream. See the man page for setbuf(3).
//
// One idea is to reopen the stream - this may meet the requirement that
// setvbuf() can be called only as the first operation on a stream before any
// I/O is done to the stream:
//
//        freopen(logFileName, "a", logFileFp);
//        setvbuf(logFileFp, NULL, _IONBF, 0);
//
// Periodic flushing, once buffering has been turned on, is always OK and
// probably sufficient. Since periodic flushing is supposed to be on for brief
// amounts of time, or for scenarios which don't need high performance (such as
// doing a tail -f on the logs).
//
// Periodic flushing gives the illusion of no buffering, yet causing perf issues
// if any, only during the time period when flushing is on.
//

static void *
xSyslogFlushPeriodicThread(void *arg)
{
    while (true) {
        xSyslogFlush();
        assert(logFlushPeriod >= logFlushPeriodMin);
        sysSleep(logFlushPeriod);
    }
    return NULL;
}

Status
xSyslogFlushPeriodic(int32_t period)
{
    Status status;
    int ret;

    if (period == 0 || period < -1) {
        // no-op
        // only valid values are > 0, and -1
        return StatusOk;
    }
    if (!logBufferAllowed || !logBufferOn || logFileFp == stderr) {
        status = StatusPerm;
        xSyslog(moduleName,
                XlogErr,
                "Log buffering is off; so periodic flushing of"
                " log buffer doesn't make sense (%s)",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (period > 0) {
        if (period < logFlushPeriodMin) {
            status = StatusInval;
            xSyslog(moduleName,
                    XlogErr,
                    "Requested log flush period (%d) below minimum (%d)",
                    period,
                    logFlushPeriodMin);
            goto CommonExit;
        }
        if (period != logFlushPeriod) {
            // turn on periodic flushing or change period if already on
            if (!logFlushPeriodThreadValid) {
                pthread_attr_t attr;

                ret = pthread_attr_init(&attr);
                if (ret != 0) {
                    status = sysErrnoToStatus(ret);
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to init thread attrs (%s)",
                            strGetFromStatus(status));
                    goto CommonExit;
                }

                ret =
                    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                if (ret != 0) {
                    status = sysErrnoToStatus(ret);
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to make thread detachable (%s)",
                            strGetFromStatus(status));
                    goto CommonExit;
                }

                // @SymbolCheckIgnore
                ret = pthread_create(&logFlushPeriodThreadId,
                                     NULL,
                                     xSyslogFlushPeriodicThread,
                                     NULL);
                if (ret != 0) {
                    status = sysErrnoToStatus(ret);
                    xSyslog(moduleName,
                            XlogErr,
                            "Failed to create periodic log flush thread (%s)",
                            strGetFromStatus(status));
                    goto CommonExit;
                }
                logFlushPeriodThreadValid = true;
            }
            logFlushPeriod = period;
        }
        assert(logFlushPeriodThreadValid == true);
    } else if (logFlushPeriod > 0) {
        assert(period == -1);
        // turn off flushing:
        // period requested == -1, implying request is to turn off flushing
        // AND logFlushPeriod > 0 (i.e. periodic flushing is on)
        if (logFlushPeriodThreadValid) {
            ret = pthread_cancel(logFlushPeriodThreadId);
            if (ret != 0) {
                status = sysErrnoToStatus(ret);
                xSyslog(moduleName,
                        XlogErr,
                        "Failed to cancel periodic log flush thread (%s)",
                        strGetFromStatus(status));
                goto CommonExit;
            } else {
                logFlushPeriodThreadValid = false;
                logFlushPeriod = -1;
            }
        }
    }
    status = StatusOk;
CommonExit:
    return status;
}

/*
 * This routine is needed for usrnodes, so that facility can be declared as not
 * booted yet - i.e. not yet ready for buffered logging. A subsequent call to
 * xsyslogFacilityBooted() is made to declare usrnode is booted, and hence ready
 * for bufffered logging. For the other facilities, call this routine right
 * after init (or whenever the sub-system is ready for buffered logging.
 */
void
xsyslogFacilityBooted()
{
    xSyslogFlush();
    logFacilityBooted = true;
    //
    // If log buffering is on, turn on periodic flushing of the buffer.  The
    // periodic flushing can be turned off, or time period changed, if needed,
    // via the "loglevelset" xccli command for UsrNode or the xcalarLogLevelSet
    // thrift command in JS console for MgmtD - both are accessible to the
    // field.
    //
    if (logBufferOn && (xsyslogFacility == SyslogFacilityUsrNode ||
                        xsyslogFacility == SyslogFacilityMgmtD)) {
        xSyslogFlushPeriodic(logFlushPeriodMin);
    }
}

void
setLogLevelViaConfigFile()
{
    Status status = StatusUnknown;
    uint32_t newLogLevel = XlogInval;

    // The user can change the log level setting by specifying a parameter
    // in the config file.  We'll only use it if it is not "Inval".
    // The user can then set it to a different value via the cli or API.

    status = parseLogLevel(XcalarConfig::get()->clusterLogLevel_, &newLogLevel);
    if (status == StatusOk) {
        if (newLogLevel != XlogInval && newLogLevel != (uint32_t) logLevel) {
            xSyslog(moduleName,
                    XlogWarn,
                    "Changing log level from %d to %d",
                    logLevel,
                    newLogLevel);
            logLevel = newLogLevel;
        }
    }
}

void
xSyslogFlush()
{
    //
    // Possibilities of failure in following are likely rare. And if they do
    // occur, just in case, there's nothing much that can be done, since this
    // is typically on the process termination path. Also, although in general,
    // exit does flush open stdio(3) streams, relying on this is unsafe (atexit
    // handler failure would cause exit to abandon stream flushing). In
    // addition, it'd be good to also call fsync() to flush kernel buffers for
    // stdio(3) streams' file descriptors) - which wouldn't work unless
    // fflush() is called before the calls to fsync() - another reason to do
    // interpositioning on exit so that a fflush()/fsync can be done here.  We
    // must be conservative to preserve logs, especially the last ones before a
    // crash or unexpected program exit.
    //

    if (logFileFp && logFileFp != stderr) {
        (void) fflush(logFileFp);
    }
    fflush(stdout);
    fflush(stderr);

    (void) fsync(STDOUT_FILENO);
    (void) fsync(STDERR_FILENO);
    if (logFileFp && logFileFp != stderr) {
        (void) fsync(fileno(logFileFp));
    }
}

// Terminate the Xcalar system log support

void
xsyslogDestroy()
{
    const int secondsPerDay = 24 * 60 * 60;  // 84,600
    const int secondsPerHour = 60 * 60;      // 3,600
    const int secondsPerMinute = 60;

    int elapsedSeconds;
    int days;
    int hours;
    int minutes;
    int seconds;

    if (!xsyslogInited) {
        return;
    }

    // Write some stats and the shutdown complete message

    // Calculate uptime and write it to the syslog
    clock_gettime(CLOCK_REALTIME, (&destroyTime));
    elapsedSeconds = (int) (destroyTime.tv_sec - initTime.tv_sec);
    days = elapsedSeconds / secondsPerDay;
    seconds = elapsedSeconds - (days * secondsPerDay);
    hours = seconds / secondsPerHour;
    seconds = seconds - (hours * secondsPerHour);
    minutes = seconds / secondsPerMinute;
    seconds = seconds - (minutes * secondsPerMinute);

    xSyslog(moduleName,
            XlogNote,
            "Elapsed time: %d:%02d:%02d:%02d",
            days,
            hours,
            minutes,
            seconds);

    // Write the messages processed statistics

    xSyslog(moduleName,
            XlogNote,
            "Truncated messages = %lu",
            messagesTruncated.val);
    xSyslog(moduleName,
            XlogNote,
            "Filtered messages = %lu",
            messagesFiltered.val);
    xSyslog(moduleName,
            XlogNote,
            "Messages written = %lu",
            messagesWritten.val + 1);  // include this message
    xSyslog(moduleName,
            XlogNote,
            "Total messages = %lu",
            messagesTotal.val);  // don't include this msg

    // @SymbolCheckIgnore
    if (xsyslogFacility != SyslogFacilityCli) {
        xSyslog(moduleName, XlogNote, "Shutdown complete");
    }
    xSyslogFlush();
    if (logFileFp != stderr) {
        (void) fclose(logFileFp);
        logFileFp = NULL;
    }
    xsyslogDestroyed = true;
}

/*
 * Keep in sync with XcalarSyslogMsgLevel
 */
#define FOREACH_LEVEL(LEVEL) \
    LEVEL(EMERG)             \
    LEVEL(ALERT)             \
    LEVEL(CRIT)              \
    LEVEL(ERR)               \
    LEVEL(WARNING)           \
    LEVEL(NOTICE)            \
    LEVEL(INFO)              \
    LEVEL(DEBUG)

#define GENSTR(STR) #STR,

static const char *levelStr[] = {FOREACH_LEVEL(GENSTR)};

const char *
logLevelStr(XcalarSyslogMsgLevel msgLevel)
{
    return levelStr[msgLevel];
}

//  Write out a Xcalar log
void
xSyslogV(const char *moduleName,
         XcalarSyslogMsgLevel inputLevel,
         XcalarSyslogTxnBuf TxnBufFlag,
         XcalarSyslogFromSignalHandler fromSigHdlr,
         const char *format,
         va_list args)
{
    // Default max rsyslog message size
    size_t msgSize = 2048;
    char formatMsgBuf[msgSize];
    char *formatMsg = formatMsgBuf;
    formatMsg[0] = '\0';

    bool formatMsgMalloc = false;
    char xcformat[1024];  // max size for Xcalar prefix in message
    XcalarSyslogMsgLevel msgLevel;
    int retCode = -1;  // assume failure
    int xcRet = -1;

    // need to use a duplicate of args in case we need a bigger buf
    va_list dupArgs;
    va_copy(dupArgs, args);

    char timestampFormat[127];
    char timestamp[127];
    struct tm localTime;
    struct tm *localTimeRet;
    struct timespec curTime;
    int ret;
    bool gotTime = false;
    size_t stampLen;
    MsgMgr *msgMgr = NULL;

    if (!xsyslogInited) {
        xsyslogInit(SyslogFacilityNotSpecified, NULL);
    }

    if (xsyslogDestroyed) {
        // xSyslogV could be called in an exit handler typically because of C++
        // destructors called via exit handlers, and which also contain calls
        // to xSyslog(). Logs will not be generated in that phase. If this
        // becomes an issue, calling destroy in an exit handler could be
        // explored, but for now, this isn't supported.
        goto XsyslogExit;
    }

    // Don't log in the CLI
    if (xsyslogFacility == SyslogFacilityCli) {
        goto XsyslogExit;
    }

    atomicInc64(&messagesTotal);

    // Check that the priority is within range.  Change it to debug
    // priority if it is not.
    if (inputLevel < XlogEmerg || inputLevel > XlogVerbose) {
        msgLevel = XlogDebug;
    } else {
        msgLevel = inputLevel;
    }

    // We might as well filter messages here so we can count them
    if (msgLevel > logLevel) {
        atomicInc64(&messagesFiltered);
        goto XsyslogExit;
    }
    atomicInc64(&messageCounts[msgLevel]);

    // Create a string from the variable number of input arguments
    retCode = vsnprintf(formatMsg, msgSize, format, dupArgs);
    // message is too big, malloc a larger formatMsg and retry
    if (retCode >= (int) msgSize) {
        // truncate the message size
        retCode = xcMin(MaxMsgSize, retCode);

        char *newFormatMsg = (char *) memAlloc(retCode + 1);
        if (newFormatMsg) {
            va_end(dupArgs);
            va_copy(dupArgs, args);

            msgSize = retCode + 1;
            retCode = vsnprintf(newFormatMsg, msgSize, format, dupArgs);
            formatMsgMalloc = true;
            formatMsg = newFormatMsg;

            va_end(dupArgs);
        }
    }

    atomicInc64(&messagesWritten);

    ret = clock_gettime(CLOCK_REALTIME, &curTime);
    if (ret == 0) {
        localTimeRet = localtime_r(&curTime.tv_sec, &localTime);
        if (localTimeRet != NULL) {
            // strftime does not allow for sub-second times. Instead, we are
            // going to use strftime to print a printf-style format string
            // with a '%06lu' in the middle of it. We can then pass this
            // to good ol' snprintf to fill in our sub-seconds
            stampLen = strftime(timestampFormat,
                                sizeof(timestampFormat),
                                "%FT%T.%%06lu%z",
                                &localTime);
            if (stampLen > 0) {
                snprintf(timestamp,
                         sizeof(timestamp),
                         timestampFormat,
                         curTime.tv_nsec / NSecsPerUSec);
                gotTime = true;
            }
        }
    }

    // If it's from a signal handler, then it's not safe to rely on checking
    // MsgMgr::get() to see if MsgMgr is still around because it might be racing
    // with shutdown
    if (!fromSigHdlr && Runtime::get() != NULL) {
        Txn txn = Txn::currentTxn();
        xcRet = snprintf(xcformat,
                         sizeof(xcformat),
                         "%s: %s: %s: %s: Host %s: Pid %u: Thr %u: Txn "
                         "%lu,%hhu,%hhu,%hhu:",
                         gotTime ? timestamp : "",
                         progname,
                         moduleName,
                         logLevelStr(msgLevel),
                         hostname,
                         getpid(),
                         sysGetTid(),
                         txn.id_,
                         (unsigned char) txn.rtType_,
                         static_cast<uint8_t>(txn.rtSchedId_),
                         (unsigned char) txn.mode_);
        assert(xcRet > 0 && xcRet < (int) sizeof(xcformat));

        if ((msgMgr = MsgMgr::get()) != NULL && TxnBufFlag == TxnBufCopy) {
            msgMgr->logTxnBuf(txn, xcformat, formatMsg);
        }
    } else {
        xcRet = snprintf(xcformat,
                         sizeof(xcformat),
                         "%s: %s: %s: %s: Host %s: Pid %u: Thr %u:",
                         gotTime ? timestamp : "",
                         progname,
                         moduleName,
                         logLevelStr(msgLevel),
                         hostname,
                         getpid(),
                         sysGetTid());
        assert(xcRet > 0 && xcRet < (int) sizeof(xcformat));
    }

    // @SymbolCheckIgnore
    fprintf(logFileFp, "%s %s\n", xcformat, formatMsg);
    // If the message did not fit, write an indicator that the message
    // was truncated
    if (retCode < 0 || retCode >= (int) msgSize) {
        // @SymbolCheckIgnore
        fprintf(logFileFp,
                "%s:%s\n",
                logLevelStr(XlogInfo),
                "Prior message may be incomplete");
        atomicInc64(&messagesWritten);
        atomicInc64(&messageCounts[LOG_INFO]);
        atomicInc64(&messagesTruncated);
    }
    if (logBufferOn) {
        if (!logFacilityBooted || msgLevel <= XlogCrit) {
            // flush this log if still bootstrapping, OR if this log's critical
            xSyslogFlush();
        }
    }
XsyslogExit:
    if (formatMsgMalloc) {
        memFree(formatMsg);
    }
}

void
xSyslogInt(const char *moduleName,
           XcalarSyslogMsgLevel inputLevel,
           XcalarSyslogTxnBuf TxnBufFlag,
           XcalarSyslogFromSignalHandler fromSigHdlr,
           const char *format,
           ...)
{
    va_list args;

    va_start(args, format);
    xSyslogV(moduleName, inputLevel, TxnBufFlag, fromSigHdlr, format, args);
    va_end(args);
}

void
xcalarExit(int status)
{
    int64_t exitInProg = atomicInc64(&xcalarExitInProg);
    if (exitInProg > 1) {
        // XXX: Hack for now to circumvent xcmonitor code which calls exit
        // via several spots (some hidden inside libmoninterface code).
        //
        // Two threads could call exit concurrently; allow only one through,
        // otherwise there could be issues due to concurrent execution of exit
        // handlers (such as C++ destructors running concurrently via exit
        // handlers).
        //
        xSyslog(moduleName, XlogWarn, "%lu xcalarExit in prog!", exitInProg);
        // @SymbolCheckIgnore
        pthread_exit(NULL);
        return;
    }

    xSyslog(moduleName, XlogEmerg, "Process exiting; status %d!!", status);
    xSyslogFlush();

    // @SymbolCheckIgnore
    _exit(status);
}

// Version of abort that flushes the syslog first.
void
xcalarAbort()
{
    xSyslog(moduleName, XlogEmerg, "Process aborting!!");
    xSyslogFlush();

    // @SymbolCheckIgnore
    abort();
}

void
stopwatchReportMsecs(Stopwatch *stopwatch, const char *mod)
{
    uint64_t milliseconds;
    stopwatch->stop();
    milliseconds = stopwatch->getElapsedNSecs() / NSecsPerMSec;
    xSyslog(moduleName, XlogInfo, "'%s' took %lu msecs", mod, milliseconds);
}
