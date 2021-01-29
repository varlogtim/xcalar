// Copyright 2017 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <sys/resource.h>
#include <new>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <fstream>

#include "common/InitTeardown.h"
#include "config/Config.h"
#include "util/MemTrack.h"
#include "msg/Message.h"
#include "libapis/LibApisRecv.h"
#include "kvstore/KvStore.h"
#include "sys/XLog.h"
#include "operators/Dht.h"
#include "ns/LibNs.h"
#include "udf/UserDefinedFunction.h"
#include "parent/Parent.h"
#include "export/DataTarget.h"
#include "child/Child.h"
#include "strings/String.h"
#include "runtime/Runtime.h"
#include "udf/UdfPython.h"
#include "runtime/ShimSchedulable.h"
#include "msg/Xid.h"
#include "libapis/LibApisSend.h"
#include "localmsg/LocalMsg.h"
#include "util/CronMgr.h"
#include "constants/XcalarConfig.h"
#include "querymanager/QueryManager.h"
// XXX(dwillis) Needed for app loading
#include "util/FileUtils.h"
#include "app/AppMgr.h"
#include "shmsg/SharedMemory.h"
#include "util/MemTrack.h"
#include "util/CgroupMgr.h"
#include "XcalarMain.h"

static const char *moduleName = "XcalarMain";

static void
childNodeSignalHandler(int sig, siginfo_t *siginfo, void *ptr)
{
    xSyslog(moduleName,
            XlogInfo,
            "childNodeSignalHandler received signal %d",
            sig);

    switch (sig) {
    case SIGTERM:
        // NOOP
        break;

    default:
        break;
    }

    xcalarExit(1);
}

//
// childnode entry point.
//
static Status
silenceChildNode(uint64_t childId, unsigned parentId)
{
    Status status;

    int outFile = -1;
    int errFile = -1;

    char templateStr[] = "/tmp/childnode-p%x-c%x-%s";
    char path[PATH_MAX];

    snprintf(path, sizeof(path), templateStr, parentId, childId, "out");
    // @SymbolCheckIgnore
    unlink(path);
    outFile = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
    if (outFile == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed silenceChildNode: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    // Only print this notification if we're on a terminal
    if (isatty(STDOUT_FILENO)) {
        // @SymbolCheckIgnore
        fprintf(stdout, "Redirecting stdout to %s\n", path);
    }

    snprintf(path, sizeof(path), templateStr, parentId, childId, "err");
    // @SymbolCheckIgnore
    unlink(path);
    errFile = open(path, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
    if (errFile == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed silenceChildNode: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (isatty(STDERR_FILENO)) {
        // @SymbolCheckIgnore
        fprintf(stderr, "Redirecting stderr to %s\n", path);
    }

    fflush(stdout);
    if (dup2(outFile, STDOUT_FILENO) == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed silenceChildNode: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    fflush(stderr);
    if (dup2(errFile, STDERR_FILENO) == -1) {
        status = sysErrnoToStatus(errno);
        xSyslog(moduleName,
                XlogErr,
                "Failed silenceChildNode: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = StatusOk;

CommonExit:
    if (errFile != -1) {
        close(errFile);
    }
    if (outFile != -1) {
        close(outFile);
    }
    return status;
}

struct ChildnodeSetupArgs {
    unsigned usrnodeId;
    uint64_t childId;
    const char *parentUdsPath;
};

// Portion of childnode bootstrap that must run on top of runtime.
static Status
childnodeRuntimeSetup(ChildnodeSetupArgs *args)
{
    Status status;
    Stopwatch stopwatch;
    Stopwatch overallStopwatch;

    stopwatch.restart();
    status = UdfPython::init();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to initialize Python interpreter: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "UdfPython init");

    stopwatch.restart();
    status = Child::init(args->usrnodeId, args->childId, args->parentUdsPath);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to initialize child: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "Child init");

CommonExit:
    stopwatchReportMsecs(&overallStopwatch, "childnodeRuntimeSetup");
    return status;
}

static Status
childnodeSetupSigHandler()
{
    Status status = StatusOk;
    sigset_t emptyMask;
    struct sigaction sigAct, childNodeSigAct;
    struct sigaction *sigActPtr;
    int ret;

    // We reset all signal dispositions to SIG_DFL for childnode here.
    sigAct.sa_handler = SIG_DFL;
    if (sigemptyset(&emptyMask) != 0) {
        status = sysErrnoToStatus(errno);
        // @SymbolCheckIgnore
        fprintf(stderr, "Failed sigemptyset: %s\n", strGetFromStatus(status));
        return status;
    }

    sigAct.sa_mask = emptyMask;
    sigAct.sa_flags = 0;
    sigAct.sa_restorer = 0;

    // Only exception is SIGTERM which would use childNodeSignalHandler.
    memZero(&childNodeSigAct, sizeof(childNodeSigAct));
    childNodeSigAct.sa_sigaction = &childNodeSignalHandler;
    childNodeSigAct.sa_flags = SA_SIGINFO;

    for (int ii = 0; ii < NSIG; ii++) {
        switch (ii) {
        case SIGTERM:
            sigActPtr = &childNodeSigAct;
            break;
        default:
            sigActPtr = &sigAct;
            break;
        }

        ret = sigaction(ii, sigActPtr, NULL);
        // POSIX says:
        // It is unspecified whether an attempt to set the action for a
        // signal that cannot be caught or ignored to SIG_DFL is
        // ignored or causes an error to be returned with errno set to
        // [EINVAL].
        // Ignore errors if it's EINVAL since those are likely
        // signals we can't change.
        if (ret != 0 && errno != EINVAL) {
            status = sysErrnoToStatus(errno);
            // @SymbolCheckIgnore
            fprintf(stderr,
                    "Failed sigaction for %d: %s\n",
                    ii,
                    strGetFromStatus(status));
            return status;
        }
    }

    return status;
}

int
mainChildnode(int argc, char *argv[])
{
    Status status;
    Status status2;
    int flag;
    int optionIndex = 0;
    bool inited = false;
    static struct option long_options[] = {
        {"configFile", required_argument, 0, 'f'},
        {"childnodeId", required_argument, 0, 'i'},
        {"usrnodeId", required_argument, 0, 'u'},
        {"numNodes", required_argument, 0, 'n'},
        {"parentUds", required_argument, 0, 's'},
        {"cgroupName", required_argument, 0, 'c'},
        {0, 0, 0, 0},
    };
    bool setNumUsrnodes = false;
    bool setChildId = false;
    bool setUsrnodeId = false;
    bool error = false;
    bool cgroupNameSet = false;
    unsigned numUsrnodes = 0;
    uint64_t childId = 0;
    unsigned usrnodeId = 0;
    char *endptr = NULL;
    char *configFilePath = NULL;
    char *parentUdsPath = NULL;
    rlim_t limit;
    std::string cgroupName;

    while ((flag = getopt_long(argc,
                               argv,
                               "f:i:n:u:s:c:",
                               long_options,
                               &optionIndex)) != -1) {
        switch (flag) {
        case 'i':
            setChildId = true;
            childId = strtoul(optarg, &endptr, 10);
            if (*endptr != '\0') {
                // @SymbolCheckIgnore
                fprintf(stderr, "Invalid childnode ID '%s'.\n", optarg);
                error = true;
            }
            break;
        case 'f':
            configFilePath = optarg;
            break;
        case 'n':
            setNumUsrnodes = true;
            numUsrnodes = (unsigned) strtoul(optarg, &endptr, 10);
            if (*endptr != '\0') {
                // @SymbolCheckIgnore
                fprintf(stderr, "Invalid numNodes '%s'.\n", optarg);
                error = true;
            } else if (numUsrnodes == 0) {
                // @SymbolCheckIgnore
                fprintf(stderr, "numNodes cannot be 0.\n");
                error = true;
            }
            break;
        case 'u':
            setUsrnodeId = true;
            usrnodeId = (unsigned) strtoul(optarg, &endptr, 10);
            if (*endptr != '\0') {
                // @SymbolCheckIgnore
                fprintf(stderr, "Invalid usrnode ID '%s'.\n", optarg);
                error = true;
            }
            break;
        case 's':
            parentUdsPath = optarg;
            break;
        }
    }

    if (!setNumUsrnodes) {
        // @SymbolCheckIgnore
        fprintf(stderr, "numNodes parameter is required.\n");
    }
    if (!setChildId) {
        // @SymbolCheckIgnore
        fprintf(stderr, "childnodeId parameter is required.\n");
    }
    if (!setUsrnodeId) {
        // @SymbolCheckIgnore
        fprintf(stderr, "usrnodeId parameter is required.\n");
    }
    if (configFilePath == NULL) {
        // @SymbolCheckIgnore
        fprintf(stderr, "configFile is required.\n");
    }

    if (parentUdsPath == NULL) {
        // @SymbolCheckIgnore
        fprintf(stderr, "parentUds is required.\n");
    }

    if (!setNumUsrnodes || !setChildId || !setUsrnodeId ||
        configFilePath == NULL || parentUdsPath == NULL || error) {
        // @SymbolCheckIgnore
        fprintf(stderr, "Incorrect parameters.\n");
        return StatusInval.code();
    }

    if (usrnodeId > MaxNodes) {
        // @SymbolCheckIgnore
        fprintf(stderr, "usrnodeId too large\n");
        return StatusInval.code();
    }

    if (cgroupNameSet && !cgroupName.length()) {
        // @SymbolCheckIgnore
        fprintf(stderr, "cgroupName is not set\n");
        return StatusInval.code();
    }

    status = childnodeSetupSigHandler();
    if (status != StatusOk) {
        // @SymbolCheckIgnore
        fprintf(stderr,
                "childnodeSetupSigHandler failed:%s\n",
                strGetFromStatus(status));
        return status.code();
    }

    bool silenceChildren = false;

    const char *silenceChildrenVar = getenv(ParentChild::EnvSilence);
    if (silenceChildrenVar != NULL) {
        if (strcmp(silenceChildrenVar, "0") == 0) {
            silenceChildren = false;
        } else if (strcmp(silenceChildrenVar, "1") == 0) {
            silenceChildren = true;
        }
    }
    if (silenceChildren) {
        status = silenceChildNode(childId, usrnodeId);
        if (status != StatusOk) {
            // @SymbolCheckIgnore
            fprintf(stderr, "failed to redirect childnode output\n");
            return status.code();
        }
    }

    ChildnodeSetupArgs childnodeSetupArgs = {
        .usrnodeId = usrnodeId,
        .childId = childId,
        .parentUdsPath = parentUdsPath,
    };

    status = InitTeardown::init(InitLevel::ChildNode,
                                SyslogFacilityChildNode,
                                configFilePath,
                                NULL,
                                argv[0],
                                InitFlagsNone,
                                usrnodeId,
                                numUsrnodes,
                                Config::ConfigUnspecifiedNumActiveOnPhysical,
                                BufferCacheMgr::TypeChildnode);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed InitTeardown::init: %s",
                    strGetFromStatus(status));
    inited = true;

    if (!silenceChildren) {
        // A XPU will inherit its parent XCE node's stdout/err. So all of a
        // XCE node's XPUs' stdout/stderr output will go to the XCE node's
        // stdout/err (typically redirected to /var/log/xcalar/node.x.out).
        // Since this would be confusing, a XPU's stdout/err must be redirected
        // to its own file (e.g. /var/log/xcalar/xpu.out). If the attempt to
        // redirect fails, then log the failure, but continue with the default
        // behavior (failing XPU boot due to a redirection failure seems
        // unjustified).
        status = redirectStdIo("xpu");
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to redirect XPU stdout/err to log directory: %s",
                    strGetFromStatus(status));
        }
    }
    limit = XcalarConfig::get()->childNodeCore_ ? RLIM_INFINITY : 0;
    status2 = setCoreDumpSize(limit);
    if (status2 != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Failed to set coresize limit to %lu: %s",
                limit,
                strGetFromStatus(status2));
    }

    status = shimFunction<ChildnodeSetupArgs *,  // Argument Type
                          Status,                // Return Type
                          childnodeRuntimeSetup>(&childnodeSetupArgs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed childnodeRuntimeSetup: %s",
                    strGetFromStatus(status));

    xSyslog(moduleName, XlogInfo, "Child init complete");
    Child::get()->waitForExit();

CommonExit:
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Exiting with error: %s",
                strGetFromStatus(status));
    }

    if (inited) {
        if (InitTeardown::get() != NULL) {
            InitTeardown::get()->teardown();
        }
    }

    return (int) status.code();
}
