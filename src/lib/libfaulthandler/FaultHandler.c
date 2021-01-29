// Copyright 2016 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <stdlib.h>

static char gdbCmd[PATH_MAX];
static int gdbPort;
static int pid;
static int signals[] = {SIGSEGV, SIGABRT, SIGILL};
static struct sigaction origsa[3];

void
gdbHandler(int sig, siginfo_t *info, void *context)
{
    for (int ii = 0; ii < sizeof(signals) / sizeof(signals[0]); ii++) {
        sigaction(signals[ii], &origsa[ii], 0);
    }
    int res = system(gdbCmd);
    ((void) res);
    // @SymbolCheckIgnore
    abort();
}

__attribute__((__constructor__)) void
signalInit()
{
    pid = getpid();
    gdbPort = 40000 + (pid % 25534);
    snprintf(gdbCmd,
             sizeof(gdbCmd),
             "exec gdbserver --attach 0.0.0.0:%d %d",
             gdbPort,
             pid);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = &gdbHandler;
    sa.sa_flags = SA_SIGINFO | SA_RESETHAND;

    for (int ii = 0; ii < sizeof(signals) / sizeof(signals[0]); ii++) {
        if (sigaction(signals[ii], &sa, &origsa[ii])) {
            perror("Failed to set sigaction");
            exit(errno);
        }
    }
}
