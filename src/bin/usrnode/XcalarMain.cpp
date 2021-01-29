// Copyright 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <stdarg.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <sys/resource.h>
#include <new>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "primitives/Primitives.h"
#include "util/System.h"
#include "XcalarMain.h"
#include "strings/String.h"
#include "sys/XLog.h"

static const char *moduleName = "XcalarMain";

//
// Redirect stdout/stderr of process to a file in the configured log dir.
//
// Must be called only after initInternal() has been successfully called so
// that the configured log directory is available to this routine (is it
// possible to assert this here?).
//
// The argument is a string which serves as the prefix of the filename to which
// stdout/err is being redirected: all filenames will be of the form:
//
//                      <logdir>/<fnamePrefix>.out
//

Status
redirectStdIo(const char *fnamePrefix)
{
    Status status;
    char redirPath[PATH_MAX];
    int redirFile = -1;
    int ret;

    if (fnamePrefix == NULL) {
        status = StatusInval;
        goto CommonExit;
    }
    ret = snprintf(redirPath,
                   sizeof(redirPath),
                   "%s/%s.out",
                   XcalarConfig::get()->xcalarLogCompletePath_,
                   fnamePrefix);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to create filename to redirect %s stdout/err (%s)! "
                "Will not redirect std{out,err}",
                fnamePrefix,
                strGetFromStatus(status));
        goto CommonExit;
    } else if (ret >= (int) sizeof(redirPath)) {
        status = StatusTrunc;
        xSyslog(moduleName,
                XlogErr,
                "Filename %s truncated! Will not redirect std{out,err}",
                redirPath);
        goto CommonExit;
    }

    redirFile = open(redirPath,
                     O_CREAT | O_WRONLY | O_APPEND,
                     S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    if (redirFile == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to open file %s (%s)! Will not redirect std{out,err}",
                redirPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Only print this notification if we're on a terminal
    if (isatty(STDOUT_FILENO)) {
        // @SymbolCheckIgnore
        fprintf(stdout,
                "%s pid %d: Redirecting stdout to %s\n",
                fnamePrefix,
                getpid(),
                redirPath);
    }

    if (isatty(STDERR_FILENO)) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "%s pid %d: Redirecting stderr to %s\n",
                fnamePrefix,
                getpid(),
                redirPath);
    }

    fflush(stdout);
    if (dup2(redirFile, STDOUT_FILENO) == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to redirect stdout to %s (%s)!",
                redirPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    fflush(stderr);
    if (dup2(redirFile, STDERR_FILENO) == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed to redirect stderr to %s (%s)!",
                redirPath,
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    if (redirFile != -1) {
        close(redirFile);
    }
    return status;
}

Status
setCoreDumpSize(rlim_t size)
{
    int ret;
    struct rlimit coreLimit;
    Status status;

    memZero(&coreLimit, sizeof(coreLimit));
    coreLimit.rlim_cur = size;
    coreLimit.rlim_max = size;

    ret = setrlimit(RLIMIT_CORE, &coreLimit);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
    } else {
        status = StatusOk;
    }
    return status;
}

__attribute__((format(printf, 1, 2))) void
printError(const char *format, ...)
{
    va_list args;
    va_start(args, format);
    // @SymbolCheckIgnore
    vfprintf(stderr, format, args);
    va_end(args);
}

int
main(int argc, char *argv[])
{
    const char *binName = strBasename(argv[0]);
    int status, err;
    sigset_t set;

    if (strcmp(binName, "usrnode") == 0) {
        // Since usrnode could've been spawned by xcmonitor, which blocks
        // SIGTERM and SIGCHLD, unblock them explicitly here (signal mask is
        // inherited from parent). usrnode needs to be able to receive and
        // handle SIGTERM so that an external entity can send it a SIGTERM for a
        // clean shutdown (usrnode's SIGTERM handler does this). usrnode needs
        // to also be able to receive a SIGCHLD due to XPUs dying.
        //
        // Eventually, usrnode should move to a sigwait() model, which will
        // block these signals from all threads, and unblock only within the
        // handler thread, which would use sigwait() to atomically unblock and
        // wait for signals.
        sigemptyset(&set);
        sigaddset(&set, SIGTERM);
        sigaddset(&set, SIGCHLD);
        err = pthread_sigmask(SIG_UNBLOCK, &set, NULL);
        if (err) {
            xSyslog(moduleName, XlogErr, "Can't unblock signals!");
            // err is in return code; not errno
            status = sysErrnoToStatus(err).code();
            return status;
        }

        status = mainUsrnode(argc, argv);
        /* following will exit process so flush stdio(3) buffers */
        xSyslogFlush();
        return status;
    } else if (strcmp(binName, "childnode") == 0) {
        status = mainChildnode(argc, argv);
        /* following will exit process so flush stdio(3) buffers */
        xSyslogFlush();
        return status;
    } else {
        printError("Unrecognized binary name '%s'\n", binName);
        return (int) StatusInval.code();
    }
}
