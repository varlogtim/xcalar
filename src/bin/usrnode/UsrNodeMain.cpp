// Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
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
#include "util/SystemPathVerify.h"
#include "util/SystemVersionVerify.h"
#include "util/SystemPerfApp.h"
#include "util/SystemStatsApp.h"
#include "util/GrpcServerApp.h"
#include "util/CronMgr.h"
#include "constants/XcalarConfig.h"
#include "querymanager/QueryManager.h"
// XXX(dwillis) Needed for app loading
#include "util/FileUtils.h"
#include "app/AppMgr.h"
#include "shmsg/SharedMemory.h"
#include "util/MemTrack.h"
#include "XcalarMain.h"
#include "common/Version.h"
#include "common/ClusterGen.h"
#include "xdb/HashTree.h"
#include "util/License.h"
#include "dataset/Dataset.h"
#include "util/CgroupMgr.h"
#include "session/Sessions.h"
#include "util/Uuid.h"

static const char *moduleName = "XcalarMain";

static pthread_t listenerThreadId;
static bool allNodesNetworkReady = false;
static int usrNodeSignalReceived = 0;
static bool usrNodeSignalFromKernel = false;  // synchronously generated signal

// following is valid only if usrNodeSignalFromKernel is false at signal
// delivery
static pid_t usrNodeSignalSendingPid = 0;

static bool usrNodeShutdownInitiated = false;

static pthread_t usrNodeSignalHandlerThreadId;
static pthread_t monitorHeartBeatThreadId;
static sem_t usrNodeSignalHandlerSem;
static sem_t usrNodePreDumpCoreActionSem;

// How often, in secs, the heartbeat thread checks on the monitor
static constexpr const uint32_t MonitorHeartBeatIntvl = 60;
static bool launchedFromMonitor = false;

struct UsrNodeRuntimeSetupArgs {
    const char *licenseFilePath;
};

static Status
usrNodeShutdownTrigger()
{
    Status status;
    Config *config = Config::get();
    NodeId nodeId = config->getMyNodeId();
    XcalarWorkItem *workItem = NULL;

    workItem = xcalarApiMakeShutdownWorkItem(false, false);
    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "usrNodeShutdownTrigger trigger shutdown failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                config->getIpAddr(nodeId),
                                config->getApiPort(nodeId),
                                UsrNodeShutdownTrigUsrName,
                                UsrNodeShutdownTrigUsrIdUnique);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "usrNodeShutdownTrigger trigger shutdown failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    if (workItem->output != NULL) {
        status.fromStatusCode(workItem->output->hdr.status);
    }

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "usrNodeShutdownTrigger trigger shutdown failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "usrNodeShutdownTrigger has initiated shutdown");
    usrNodeShutdownInitiated = true;

CommonExit:
    if (workItem != NULL) {
        xcalarApiFreeWorkItem(workItem);
        workItem = NULL;
    }
    return status;
}

static const char *
getParentName(Config *config)
{
    Status status;

    if (config == NULL) {
        return NULL;
    }
    status = config->populateParentBinary();
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Unable to obtain parent name: %s",
                strGetFromStatus(status));
        return NULL;
    }

    return config->getParentBinName();
}

static void *
monitorHeartBeatThread(void *arg)
{
    const char *parentName;
    pid_t parentPid;
    int error = 0;

    parentPid = getppid();
    parentName = getParentName(Config::get());

    if (strcmp(parentName, "xcmonitor") == 0) {
        while (kill(parentPid, 0) == 0) {
            sysSleep(MonitorHeartBeatIntvl);
            // XXX: once in a while re-check parent name in case pid's reused
            if (usrNodeNormalShutdown()) {
                break;
            }
        }
        error = errno;
    }

    // There are three ways in which flow can get here:
    //
    // 1. The monitor parent has died even before the parentName check above
    //    (in which case, the parentName must now be "initd")
    // 2. The while loop breaks b/c usrNodeNormalShutdown is true
    // 3. The while loop exits due to kill(parentPid, 0) failing. The failure
    //    must be due to the monitor parent dying, i.e. error must be ESRCH
    //
    // In the assert below, the ESRCH check corresponds to event 3, the initd
    // parentName comparison to 1, and of course the usrNodeNormalShutdown check
    // corresponds to event 2

    assert(usrNodeNormalShutdown() || error == ESRCH ||
           strcmp(parentName, "initd") == 0);

    if (usrNodeNormalShutdown()) {
        // If the usrnode is shutting down normally, this thread can exit here
        // since normal shutdown process will take care of shutting down
        // everything cleanly - there's no need to tear down anything specific
        // here (in fact, doing so would interfere with the normal shutdown
        // process - see SDK-559).
        xSyslog(moduleName, XlogInfo, "Monitor heart beat thread exiting");
        return NULL;
    } else {
        // Monitor parent must have died somehow. This is serious, since now
        // there's no monitor for this usrnode. This node could trigger a
        // cluster wide shutdown, which will attempt to shutdown the other
        // usrnodes, and thereby, also have their monitors exit since they'll
        // notice the usrnodes have exited.
        //
        // However, the problem with this thread not exiting the usrnode
        // immediately is that with its monitor gone, there's no entity
        // responsible for reaping it, or cleaning it up - so if this thread
        // either triggers a cluster-wide shutdown and then exits, or exits
        // because it detects that it's already the recipient of a shutdown,
        // then there's no recourse if its cluster wide shutdown engagement gets
        // stuck or deadlocked. The admin will then have to kill the usrnode,
        // since its monitor is gone. This has been observed during testing.
        //
        // So unfortunately, the only solution here is to just clean up and exit
        // locally. Losing a monitor without its usrnode dying and being reaped
        // by its monitor first, is considered extremely rare. So the
        // consequence of an unclean shutdown due to this scenario isn't so bad.
        //

        xSyslog(moduleName, XlogErr, "Monitor Dead! Shutting down!");

        // Just remove all the shared memory files in tmpfs.
        SharedMemoryMgr::removeSharedMemory();
        xcalarExit(1);
        return NULL;
    }
}

#ifdef SUICIDE

// A potentially cleaner way of killing usrnodes in response to local kill
// requests from the monitor
static Status
usrNodeSuicide()
{
    Status status;
    Config *config = Config::get();
    NodeId nodeId = config->getMyNodeId();
    XcalarWorkItem *workItem = NULL;

    workItem = xcalarApiMakeShutdownWorkItem(true, false);

    if (workItem == NULL) {
        status = StatusNoMem;
        xSyslog(moduleName,
                XlogErr,
                "usrNodeSuicide failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    status = xcalarApiQueueWork(workItem,
                                config->getIpAddr(nodeId),
                                config->getApiPort(nodeId),
                                UsrNodeShutdownTrigUsrName,
                                UsrNodeShutdownTrigUsrIdUnique);

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "usrNodeSuicide failed: %s",
                strGetFromStatus(status));
        goto CommonExit;
    }

    xSyslog(moduleName,
            XlogInfo,
            "usrNodeSuicide has initiated local shutdown");

CommonExit:
    return status;
}
#endif  // SUICIDE

// The set of signals whose default action is to dump a core (as per signal(7)),
// and which are not already handled by usrnode. Currently, there are no signals
// in this set which are handled differently or explicitly by usrnode.
// The set is generated from the sigsAllowedToCore[] array.

static sigset_t sigsAllowedToCoreSet;

static void *
usrNodeNodeSignalHandlerThread(void *arg)
{
    Status status = StatusOk;
    char pathOfSendingPid[PATH_MAX];
    char procPath[32];

    sem_wait(&usrNodeSignalHandlerSem);

    if (usrNodeSignalReceived == 0) {
        // Must be during graceful shutdown, to join this thread. In particular,
        // can't call xSyslog() at this stage since teardown() and
        // xSyslogDestroy() has already occurred. See last few lines of
        // mainUsrnode().
        return NULL;
    }

    // If we can't get a path name we'll use an empty string.  Also, readlink
    // doesn't null terminate.
    memZero(pathOfSendingPid, sizeof(pathOfSendingPid));

    if (usrNodeSignalSendingPid) {
        snprintf(procPath,
                 sizeof(procPath),
                 "/proc/%d/exe",
                 usrNodeSignalSendingPid);
        readlink(procPath, pathOfSendingPid, sizeof(pathOfSendingPid) - 1);
    }

    if (sigismember(&sigsAllowedToCoreSet, usrNodeSignalReceived)) {
        if (usrNodeSignalFromKernel) {
            xSyslogFromSig(moduleName,
                           XlogEmerg,
                           "usrNodeSignalHandler received signal %d from "
                           "kernel; "
                           "will flush logs and then dump core",
                           usrNodeSignalReceived);
        } else {
            xSyslogFromSig(moduleName,
                           XlogEmerg,
                           "usrNodeSignalHandler received signal %d from pid "
                           "%d (%s); "
                           "will flush logs and then dump core",
                           usrNodeSignalReceived,
                           usrNodeSignalSendingPid,
                           pathOfSendingPid);
        }
        xSyslogFlush();
        // After the logs are flushed, the signal needs to be re-generated - the
        // second time around, the signal delivery will trigger a core dump -
        // this is because the flag SA_RESETHAND was used to install a handler
        // for the signals in this set - which means that the disposition for
        // these signals will be RESET to SIG_DFL on return from the handler.
        // And SIG_DFL behavior for these signals is to dump core.
        //
        // The re-generation of the signal is done identically for
        // synchronously generated signals sent by the kernel (e.g. SIGSEGV,
        // SIGILL, SIGTRAP, etc.) and for signals sent by different entities
        // (e.g. kill(2)):
        //
        // Re-raise the signal in the handler context (not in the signal
        // handling thread here).
        //
        // For synchronously generated signals, the main requirement is to just
        // return from the handler - this will re-execute the faulting
        // instruction, re-generating the signal which, since the disposition is
        // now SIG_DFL, will kill the process at the faulting instruction.  This
        // is also true for SIGABRT in cases when it's sent via abort(3) or
        // assert() failures, since the signal is re-generated on return from
        // the handler as promised by the abort(3) man page. In these instances,
        // it's treated as a synchronously generated signal, with the back trace
        // reflecting the assert() failure or abort() call in the main thread by
        // gdb.
        //
        // For asynchronously delivered signals, the re-raising is needed, since
        // there's no faulting instruction to re-generate the signal. Doing this
        // in the handler context cleans up the stack - otherwise the core will
        // be reported with a raise on the usrNodeNodeSignalHandlerThread stack.
        // Although not incorrect, behavior that's as close as possible to the
        // no-handler-installed scenario for such signals, is better of course.
        //
        // It turns out that re-raising the signal in the handler context, even
        // for synchronously generated signals, leaves the behavior as if the
        // handler returned and re-executed the faulting instruction. So, in
        // order to keep the code common and simple, we always re-raise the
        // signal in the handler context - for both synchronously and
        // asynchronously generated signals (even though it's not really needed
        // for synchronously generated signals).
        //

        sem_post(&usrNodePreDumpCoreActionSem);

        // Return back to signal handler by invoking the sem_post() above, and
        // then exiting.

        goto CommonExit;
    }

    if (usrNodeSignalSendingPid) {
        xSyslogFromSig(moduleName,
                       XlogErr,
                       "sigthread: usrNodeSignalHandler received signal %d "
                       "from "
                       "pid %d (%s)",
                       usrNodeSignalReceived,
                       usrNodeSignalSendingPid,
                       pathOfSendingPid);
    } else {
        xSyslogFromSig(moduleName,
                       XlogErr,
                       "sigthread: usrNodeSignalHandler received signal %d "
                       "from "
                       "kernel",
                       usrNodeSignalReceived);
    }

    switch (usrNodeSignalReceived) {
    case SIGTERM:
        if (usrNodeNormalShutdown()) {
            // Got a SIGTERM while being shutdown; exit the thread: will
            // be joined by mainUsrnode(). Alternatively, the thread could
            // go back to sem_wait() above which would also work, but this
            // doesn't seem necessary. All future signals to this node will
            // be effectively ignored since the handler thread would be
            // dead.
            xSyslogFromSig(moduleName,
                           XlogNote,
                           "Already in shutdown: signal handler thread "
                           "exiting!");
            break;
        }
        status = usrNodeShutdownTrigger();
        if (status != StatusOk) {
            xSyslogFromSig(moduleName,
                           XlogErr,
                           "Failed to trigger shutdown: %s, exiting now!",
                           strGetFromStatus(status));
            // Just remove all the shared memory files in tmpfs.
            SharedMemoryMgr::removeSharedMemory();
            xcalarExit(1);
        }
        break;

    case SIGUSR1:
        assert(launchedFromMonitor);

        if (usrNodeNormalShutdown()) {
            // Got a SIGUSR1 while being shutdown; exit the thread: will
            // be joined by mainUsrnode(). Alternatively, the thread could
            // go back to sem_wait() above which would also work, but this
            // doesn't seem necessary. All future signals to this node will
            // be effectively ignored since the handler thread would be
            // dead.
            xSyslogFromSig(moduleName,
                           XlogNote,
                           "Already in shutdown: signal handler thread "
                           "exiting!");
            break;
        }

#ifdef SUICIDE
        status = usrNodeSuicide();
        if (status != StatusOk) {
            xSyslogFromSig(moduleName,
                           XlogErr,
                           "Failed to commit suicide: %s, exiting now!",
                           strGetFromStatus(status));
            // Just remove all the shared memory files in tmpfs.
            SharedMemoryMgr::removeSharedMemory();
            xcalarExit(1);
        }
#else
        // Interface w/ monitor which sends a usrnode SIGUSR1 for quorum loss
        // It can't kill -9 the usrnode, since the usrnode may be doing an
        // orderly shutdown, and it can't send SIGTERM, since the usrnode then
        // can't tell if this is the first time it got SIGTERM and should
        // initiate a cluster wide shutdown (which would be wrong if the
        // SIGTERM was received due to an orderly shutdown initiated by a
        // different node).
        xSyslogFromSig(moduleName, XlogErr, "Commit suicide: exiting now!");
        // Just remove all the shared memory files in tmpfs.
        SharedMemoryMgr::removeSharedMemory();
        xcalarExit(1);
#endif  // SUICIDE

    default:
        break;
    }

CommonExit:

    return NULL;
}

static void
usrNodeSignalHandler(int sig, siginfo_t *siginfo, void *ptr)
{
    usrNodeSignalReceived = sig;

    if (siginfo->si_code > 0) {
        usrNodeSignalFromKernel = true;
    } else {
        usrNodeSignalSendingPid = siginfo->si_pid;
    }

    if (sigismember(&sigsAllowedToCoreSet, sig)) {
        // If the usrnode receives a signal which is allowed to core-dump
        // usrnode, the handler waits until some necessary action (e.g. dumping
        // logs) is carried out, before dumping core.
        //
        // For kernel generated signals such as SIGSEGV, SIGILL, etc.,
        // returning from this handler re-generates the signal by re-creating
        // the original condition - since the SA_RESETHAND flag is used when
        // installing the handler for these signals.
        //
        // For other signals (e.g. SIGQUIT sent to usrnode via kill(2)), even
        // though the handler would be reset to SIG_DFL, the signal needs to be
        // re-sent by usrnode: just returning from this handler will not
        // re-generate the signal.
        //
        // However, re-raising the signal doesn't change the behavior for
        // synchronously generated signals - so we always re-raise for any
        // signal that's in the sigsAllowedToCoreSet, after returning from the
        // wait (e.g. after flushing the logs).
        //

        sem_post(&usrNodeSignalHandlerSem);
        sem_wait(&usrNodePreDumpCoreActionSem);  // until handler thread's done
        raise(sig);
    } else {
        sem_post(&usrNodeSignalHandlerSem);
    }
}

static const char *long_options_help[] = {
    "--noChildNodes(-c)",
    "--configFile (-f)     <configFile>",
    "--nodeId (-i)         <myNodeId>",
    "--numNodes (-n)       <activeNodes>",
    "--licenseFile (-k)    <licenseFile>",
    "--help (-h)",
};

static struct option long_options[] = {
    {"noChildNodes", no_argument, 0, 'c'},
    {"configFile", required_argument, 0, 'f'},
    {"nodeId", required_argument, 0, 'i'},
    {"numNodes", required_argument, 0, 'n'},
    {"help", no_argument, 0, 'h'},
    {"licenseFile", required_argument, 0, 'k'},
    {0, 0, 0, 0},
};

static void
printUsrnodeHelp(void)
{
    // @SymbolCheckIgnore
    fprintf(stderr, "Build type: %s\n\n", StringifyMacro(XCALAR_BUILD_TYPE));

    // @SymbolCheckIgnore
    fprintf(stderr, "usrnode options:\n");

    for (uint32_t ii = 0; ii < ArrayLen(long_options_help); ii++) {
        // @SymbolCheckIgnore
        fprintf(stderr, "  %s\n", long_options_help[ii]);
    }
}

static Status
usrnodeRuntimeSetup(UsrNodeRuntimeSetupArgs *setupArgs)
{
    Status status = StatusOk;
    MsgMgr *msgMgr = MsgMgr::get();
    bool setTxn = false;
    LicenseMgr *licenseMgr = LicenseMgr::get();
    Stopwatch stopwatch;
    Stopwatch overallStopwatch;

    Txn txn = Txn::newTxn(Txn::Mode::NonLRQ);
    Txn::setTxn(txn);
    setTxn = true;

    stopwatch.restart();
    status = TransportPageMgr::get()->setupShipper();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to set up Transport page shipper: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "Transport page shipper Init");

    stopwatch.restart();
    status = CgroupMgr::init();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to set up Cgroups: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "cgroups Init");

    stopwatch.restart();
    status = Parent::get()->initXpuCache();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to init XPU cache: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "initXpuCache");

    // Ensure that the all nodes in the cluster are networked and ready
    // to do work. To achive this set our own node state to
    // MsgNodeIsNetworkReady and then ensure that other active nodes
    // are at the same state.
    stopwatch.restart();
    status = msgMgr->twoPcBarrier(MsgMgr::BarrierType::BarrierIsNetworkReady);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to establish cluster network: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "twoPc BarrierIsNetworkReady");

    // ================= Barrier =================

    if (Config::get()->getMyNodeId() == 0) {
        stopwatch.restart();
        status = AppMgr::get()->addBuildInApps();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to add build in Apps: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "addBuildInApps");

        stopwatch.restart();
        status = GrpcServerApp::init();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to start grpc server: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "Grpc server");

        // Create system DHT, read in and recreate previously saved DHTs
        stopwatch.restart();
        status = DhtMgr::get()->dhtStateRestore();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restore DHT state: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "dhtStateRestore");

        stopwatch.restart();
        status = KvStoreLib::get()->open(XidMgr::XidGlobalKvStore,
                                         KvStoreLib::KvGlobalStoreName,
                                         KvStoreGlobal);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to open global KvStore: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "kvs open XidGlobalKvStore");

        stopwatch.restart();
        status = updateClusterGen();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed update cluster generation: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "Updated cluster generation");

        stopwatch.restart();
        std::string globalKey("xdGlobalKey");
        std::string kvValue("0");
        status = KvStoreLib::get()->addOrReplace(XidMgr::XidGlobalKvStore,
                                                 globalKey.c_str(),
                                                 globalKey.length() + 1,
                                                 kvValue.c_str(),
                                                 kvValue.length() + 1,
                                                 false,
                                                 KvStoreOptSync);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to add global XD key: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "kvs addOrReplace XidGlobalKvStore");

        stopwatch.restart();
        status = AppMgr::get()->restoreAll();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to restore Apps: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "App restartAll");

        stopwatch.restart();
        status = HashTreeMgr::get()->publishNamesToNs();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to publish IMD table names to Namespace: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "HashTreeMgr publishNamesToNs");

        stopwatch.restart();
        status = Dataset::get()->loadAllDatasetMeta();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to load dataset meta: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "loadAllDatasetMeta");

        stopwatch.restart();
        status = Runtime::get()->setupClusterState();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to set up runtime cluster state: %s",
                        strGetFromStatus(status));
        stopwatchReportMsecs(&stopwatch, "Set up runtime cluster state");
    }

    stopwatch.restart();
    status = AppMgr::get()->setupMsgStreams();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to set up msg streams for Apps: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "App setupMsgStreams");

    stopwatch.restart();
    status = msgMgr->twoPcBarrier(MsgMgr::BarrierType::BarrierStreamsAdded);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to barrier for msg streams: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "twoPc BarrierStreamsAdded");

    // Look up cluster generation number for non-node0 nodes, since node0 is
    // responsible for managing cluster generation. Also note that this look up
    // can be done only after barrier subsequent to KvStore set up.
    if (Config::get()->getMyNodeId() != 0) {
        status = lookupClusterGen();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to look up cluster generation: %s",
                        strGetFromStatus(status));
    }

    bool isShared;
    stopwatch.restart();
    status =
        PathVerify::verifyPath(XcalarConfig::get()->xcalarRootCompletePath_,
                               &isShared);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to run system verification app (shared path "
                    "check): %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "verifyPath");

    if (!isShared) {
        status = StatusNotShared;
        xSyslog(moduleName,
                XlogErr,
                "Xcalar root not on shared storage: '%s'",
                XcalarConfig::get()->xcalarRootCompletePath_);
        goto CommonExit;
    }

    // Verify all nodes are running the same version
    stopwatch.restart();
    status = VersionVerify::verifyVersion(versionGetFullStr());
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to run system verification app (version check): %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "verifyVersion");

    // Must load defaults before user created retinas
    stopwatch.restart();
    status = UserDefinedFunction::get()->addDefaultUdfs();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to load default Scalar Functions: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "addDefaultUdfs");

    // Also load the rest of the UDFs.
    // XXX: Add upgrade context here. Is this an upgrade from pre-Dionysus
    // to Dionysus and beyond? If so, pass true to unpersistAll(). Currently,
    // the upgrade code is executed unconditionally. Defer this until upgrade
    // work is done, and make a decision then.
    stopwatch.restart();
    status = UserDefinedFunction::get()->unpersistAll(false);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to restore Scalar Function state: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "udf unpersistAll");

    stopwatch.restart();
    status = DataTargetManager::getRef().addPersistedTargets();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to add persisted targets: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "addPersistedTargets");

    stopwatch.restart();
    status = licenseMgr->boottimeInit();
    BailIfFailedMsg(moduleName,
                    status,
                    "License initialization failed: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "boottimeInit");

    stopwatch.restart();
    status = msgMgr->twoPcBarrier(MsgMgr::BarrierType::BarrierSystemAppAdded);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to barrier after system app: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "twoPc BarrierSystemAppAdded");

    // ================= Barrier =================

    // Must come after system apps initialization
    stopwatch.restart();
    if (LicenseMgr::get()->licGetLoadStatus() != StatusOk &&
        setupArgs->licenseFilePath != NULL &&
        Config::get()->getMyNodeId() == 0) {
        status = LicenseMgr::get()->installLicense(setupArgs->licenseFilePath);
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Encountered the following error when trying to install "
                    "the license file provided at \"%s\": %s",
                    setupArgs->licenseFilePath,
                    strGetFromStatus(status));
            // Keep going despite license error.
            status = StatusOk;
        }
    }
    stopwatchReportMsecs(&stopwatch, "install license");

    stopwatch.restart();
    status = msgMgr->twoPcBarrier(MsgMgr::BarrierType::BarrierUsrnodeInitDone);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to barrier after usrnode init: %s",
                    strGetFromStatus(status));
    stopwatchReportMsecs(&stopwatch, "twoPc BarrierUsrnodeInitDone");

    // ================= Barrier =================

    if (Config::get()->getMyNodeId() == 0) {
        status = SystemStatsApp::init();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to start System Stats App: %s",
                        strGetFromStatus(status));

        // Spin up the perf app (it runs on every node).
        status = SystemPerfApp::init();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to System Perf App: %s",
                        strGetFromStatus(status));
    }

CommonExit:
    if (setTxn) {
        Txn::setTxn(Txn());
    }
    stopwatchReportMsecs(&overallStopwatch, "usrnodeRuntimeSetup");
    return status;
}

static Status
usrnodeRuntimeTeardown(void *ignored)
{
    Status status = StatusOk;
    MsgMgr *msgMgr = MsgMgr::get();
    bool setTxn = false;

    Txn txn = Txn::newTxn(Txn::Mode::NonLRQ);
    Txn::setTxn(txn);
    setTxn = true;

    if (QueryManager::get() != NULL) {
        xSyslog(moduleName, XlogInfo, "QM::destroy");
        QueryManager::get()->destroy();
    }

    if (Config::get()->getMyNodeId() == 0) {
        // Clean up done by node 0 for the entire cluster.
        if (SystemStatsApp::get()) {
            xSyslog(moduleName, XlogInfo, "SystemStatsApp destroy");
            SystemStatsApp::destroy();
        }

        // Destroy the perf app.
        xSyslog(moduleName, XlogInfo, "Destroying the PerfApp destroy");
        SystemPerfApp::destroy();

        if (KvStoreLib::get()) {
            xSyslog(moduleName, XlogInfo, "GlobalKVS::close");
            KvStoreLib::get()->close(XidMgr::XidGlobalKvStore,
                                     KvStoreCloseOnly);
        }
        if (UserDefinedFunction::get()) {
            xSyslog(moduleName, XlogInfo, "DeleteDefaultUdfs");
            status = UserDefinedFunction::get()->deleteDefaultUdfs();
        }
        if (DhtMgr::get()) {
            xSyslog(moduleName, XlogInfo, "DeleteDefaultDhts");
            DhtMgr::get()->dhtDeleteDefaultDhts();
        }
        if (AppMgr::get()) {
            xSyslog(moduleName, XlogInfo, "RemoveBuiltInApps");
            AppMgr::get()->tryAbortAllApps(StatusShutdownInProgress);
            AppMgr::get()->removeBuiltInApps();
        }
        if (&DataTargetManager::getRef()) {
            xSyslog(moduleName, XlogInfo, "RemovePersistedTargets");
            DataTargetManager::getRef().removePersistedTargets();
        }
        if (HashTreeMgr::get()) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Remove published tables from Namespace");
            HashTreeMgr::get()->unpublishNamesFromNs();
        }
        if (Runtime::get()) {
            xSyslog(moduleName, XlogInfo, "Teardown Runtime cluster state");
            Runtime::get()->teardownClusterState();
        }
    }

    if (Parent::get()) {
        xSyslog(moduleName, XlogInfo, "Shutting down all children");
        Parent::get()->killAllChildren();
    }

    if (CgroupMgr::get()) {
        xSyslog(moduleName, XlogInfo, "Cgroups teardown");
        CgroupMgr::get()->destroy();
    }

    if (TransportPageMgr::get()) {
        xSyslog(moduleName, XlogInfo, "Transport page shipper teardown");
        TransportPageMgr::get()->teardownShipper();
    }

    if (msgMgr != NULL) {
        status = msgMgr->twoPcBarrier(MsgMgr::BarrierType::BarrierShutdown);
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to initiate cluster shutdown: %s",
                        strGetFromStatus(status));
        // Now that cluster-wide clean up is done we'll allow the
        // message layer to shutdown.
        status = msgMgr->setClusterStateShutdown();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to set cluster state to 'shutdown': %s",
                        strGetFromStatus(status));
    }

CommonExit:
    if (setTxn) {
        Txn::setTxn(Txn());
    }
    xSyslog(moduleName,
            XlogInfo,
            "usrnodeRuntimeTeardown done: %s",
            strGetFromStatus(status));

    return status;
}

bool
areAllNodesNetworkReady()
{
    return allNodesNetworkReady;
}

//
// A pre-core-dump action is taken for signals allowed to core dump usrnode.
// The list of such signals is maintained in the array below: this is a subset
// of the list of signals whose default (SIG_DFL) action is to dump core (as
// per the signal(7) man page). If there's a need to handle any of these
// signals differently, that signal should be deleted from the following list,
// when the special handling for such a signal is introduced.
//
static int sigsAllowedToCore[] = {SIGQUIT,
                                  SIGILL,
                                  SIGTRAP,
                                  SIGABRT,
                                  SIGIOT,
                                  SIGBUS,
                                  SIGFPE,
                                  SIGSEGV,
                                  SIGXCPU,
                                  SIGXFSZ,
                                  SIGSYS};

static Status
usrnodeSetupSigHandler()
{
    Status status = StatusOk;
    struct sigaction act;
    int nSigsWhichCore = sizeof(sigsAllowedToCore) / sizeof(int);
    int i;
    int ret;

    memZero(&act, sizeof(act));
    act.sa_sigaction = &usrNodeSignalHandler;
    act.sa_flags = SA_SIGINFO;

    ret = sem_init(&usrNodeSignalHandlerSem, 0, 0);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        printError("Failed to init usrNodeSignalHandlerSem: %s\n",
                   strGetFromStatus(status));
        return status;
    }

    ret = sem_init(&usrNodePreDumpCoreActionSem, 0, 0);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        printError("Failed to init usrNodePreDumpCoreActionSem: %s\n",
                   strGetFromStatus(status));
        return status;
    }

    // @SymbolCheckIgnore
    ret = pthread_create(&usrNodeSignalHandlerThreadId,
                         NULL,
                         usrNodeNodeSignalHandlerThread,
                         NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        printError("Failed to create usrNodeNodeSignalHandlerThread: %s\n",
                   strGetFromStatus(status));
        sem_destroy(&usrNodeSignalHandlerSem);
        return status;
    }

    if (sigaction(SIGTERM, &act, 0)) {
        status = sysErrnoToStatus(errno);
        printError("Failed to set SIGTERM sigaction: %s\n",
                   strGetFromStatus(status));
        verify(sem_post(&usrNodeSignalHandlerSem) == 0);
        // @SymbolCheckIgnore
        pthread_join(usrNodeSignalHandlerThreadId, NULL);
        sem_destroy(&usrNodeSignalHandlerSem);
        return status;
    }

    if (XcalarConfig::get()->enablePredumpHandler_) {
        // See block comment in usrNodeSignalHandler() for signals in the
        // sigsAllowedToCoreSet set of signals. All signals which are allowed
        // to core dump usrnode are stored in the sigsAllowedToCore[] array,
        // and for each of these signals, the handler is expected to do some
        // action (e.g.  flushing logs) before dumping core. This is achieved
        // via the use of the SA_RESETHAND flag to sigaction(2).
        act.sa_flags |= SA_RESETHAND;
        for (i = 0; i < nSigsWhichCore; i++) {
            sigaddset(&sigsAllowedToCoreSet, sigsAllowedToCore[i]);

            if (sigaction(sigsAllowedToCore[i], &act, 0)) {
                status = sysErrnoToStatus(errno);
                printError("Failed to set sigaction for signal number %d: %s\n",
                           sigsAllowedToCore[i],
                           strGetFromStatus(status));
                verify(sem_post(&usrNodeSignalHandlerSem) == 0);
                // @SymbolCheckIgnore
                pthread_join(usrNodeSignalHandlerThreadId, NULL);
                sem_destroy(&usrNodeSignalHandlerSem);
                return status;
            }
        }
    }

    return status;
}

static void
usrnodeTeardownSigHandler()
{
    verify(sem_post(&usrNodeSignalHandlerSem) == 0);
    // @SymbolCheckIgnore
    pthread_join(usrNodeSignalHandlerThreadId, NULL);
    sem_destroy(&usrNodeSignalHandlerSem);
}

static Status
usrnodeMonitorSetup()
{
    const char *parentName;
    Status status = StatusOk;

    // monitor heartbeat thread must be created only after config has been
    // initialized above so the thread can leverage the config parent routines.
    parentName = getParentName(Config::get());
    if (parentName == NULL) {
        // Check the parentName only if it's non-NULL, otherwise proceed.  If
        // the monitor did launch usrnode, parentName will not be NULL.  If the
        // monitor didn't launch usrnode, parentName being NULL may not be
        // fatal: so proceed in non-DEBUG, but at least log this event, and
        // in DEBUG, get a core so we can figure out why this happened.
        xSyslog(moduleName, XlogWarn, "XCE node can't read its parent's name!");
        assert(false && "parentName found to be NULL during usrnode boot");
    } else if (strcmp(parentName, "xcmonitor") == 0) {
        status = redirectStdIo("node");
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogErr,
                    "Failed to redirect XCE node stdout/err to log directory");
        }

        struct sigaction act;
        memZero(&act, sizeof(act));
        act.sa_sigaction = &usrNodeSignalHandler;
        act.sa_flags = SA_SIGINFO;
        int ret;
        if (sigaction(SIGUSR1, &act, 0)) {
            status = sysErrnoToStatus(errno);
            printError("Failed to set SIGUSR1 sigaction: %s\n",
                       strGetFromStatus(status));
            goto CommonExit;
        }

        // @SymbolCheckIgnore
        ret = pthread_create(&monitorHeartBeatThreadId,
                             NULL,
                             monitorHeartBeatThread,
                             NULL);
        if (ret != 0) {
            status = sysErrnoToStatus(errno);
            printError("Failed to create monitor heart beat thread: %s\n",
                       strGetFromStatus(status));
            goto CommonExit;
        }
        launchedFromMonitor = true;
    }
CommonExit:
    return status;
}

static void
usrnodeMonitorTeardown()
{
    assert(launchedFromMonitor == true);

    // A regular shutdown shouldn't be blocked by the
    // monitorHeartBeatThread, which may be sleeping in-between heart beat
    // checks for time that may exceed the time the monitor waits to
    // shut-down the usrnode gracefully.  If the thread isn't canceled out
    // of its sleep, the monitor may force kill its usrnode (-9) on every
    // normal shutdown which is undesirable.  Of course, the monitor
    // hearbeat thread may have exited by this time, since it checks for
    // in-progress shutdown, so a pthread_cancel may not actually find the
    // thread so no point in checking the return code.

    // where is pthread_setcanceltype set?
    (void) pthread_cancel(monitorHeartBeatThreadId);

    // The hearbeat thread will either notice that shutdown is set, and will
    // exit, or the above cancel will cancel it out of its sleep. So the
    // following join will complete successfully.

    sysThreadJoin(monitorHeartBeatThreadId, NULL);
}

int
mainUsrnode(int argc, char *argv[])
{
    MsgMgr *msgMgr = NULL;
    assertStatic(mathIsPowerOfTwo(MaxNodes));
    Status status;
    int flag;
    int optionIndex = 0;
    bool setNumNodes = false;
    bool setNodeId = false;
    bool error = false;
    bool noChildNodes = false;
    bool initTeardownDone = false;
    bool initRuntimeDone = false;
    unsigned numNodes = 0;
    unsigned nodeId = 0;
    char *endptr = NULL;
    char *configFilePath = getenv("XCE_CONFIG");
    const char *licenseFilePath = getenv("XCE_LICENSEFILE");
    const char *pubSigningKeyFilePath = getenv("XCE_PUBSIGNKEYFILE");
    UsrNodeRuntimeSetupArgs runtimeSetupArgs;

    const char *libcMallocCheck = NULL;
    int numReady;

    if (char *env = getenv("XCE_NODEID")) {
        nodeId = strtoul(env, &endptr, 10);
        if (*endptr != '\0') {
            printError("Invalid nodeId '%s'.\n", env);
            return StatusUsrNodeIncorrectParams.code();
        }
        setNodeId = true;
    }

    if (char *env = getenv("XCE_NUMNODES")) {
        numNodes = strtoul(env, &endptr, 10);
        if (*endptr != '\0') {
            printError("Invalid numNodes '%s'.\n", env);
            return StatusUsrNodeIncorrectParams.code();
        }
        setNumNodes = true;
    }

    libcMallocCheck = getenv("MALLOC_CHECK_");
    if (libcMallocCheck != NULL) {
        xSyslog(moduleName, XlogWarn, "MALLOC_CHECK_=%s", libcMallocCheck);
    }

    // The ":" follows required args.
    while ((flag = getopt_long(argc,
                               argv,
                               "cf:i:n:hk:",
                               long_options,
                               &optionIndex)) != -1) {
        switch (flag) {
        case 'c':
            noChildNodes = true;
            break;
        case 'i':
            setNodeId = true;
            assert(optarg != NULL);
            nodeId = (unsigned) strtoul(optarg, &endptr, 10);
            if (*endptr != '\0') {
                printError("Invalid nodeId '%s'.\n", optarg);
                error = true;
            }
            break;
        case 'f':
            configFilePath = optarg;
            break;
        case 'k':
            licenseFilePath = optarg;
            break;
        case 'n':
            setNumNodes = true;
            assert(optarg != NULL);
            numNodes = (unsigned) strtoul(optarg, &endptr, 10);
            if (*endptr != '\0') {
                printError("Invalid numNodes '%s'.\n", optarg);
                error = true;
            } else if (numNodes == 0) {
                printError("numNodes cannot be 0.\n");
                error = true;
            }
            break;
        case 'h':
            printUsrnodeHelp();
            return StatusOk.code();
            break;
        }
    }

    if (!setNumNodes) {
        printError("numNodes parameter is required.\n");
    }
    if (!setNodeId) {
        printError("nodeId parameter is required.\n");
    }
    if (configFilePath == NULL) {
        printError("configFile is required.\n");
    }

    if (!setNumNodes || !setNodeId || configFilePath == NULL || error) {
        printError("Use --help\n");
        return StatusUsrNodeIncorrectParams.code();
    }

    char *ret = setlocale(LC_NUMERIC, "");
    if (ret == NULL) {
        xSyslog(moduleName, XlogErr, "Failed to set locale");
        return StatusUsrNodeIncorrectParams.code();
    }

    if (setCoreDumpSize(RLIM_INFINITY) != StatusOk) {
        printError(
            "Failed to set coresize to unlimited."
            " No coredumps will be produced.\n");
    }

    initTeardownDone = true;
    InitLevel initLevel =
        noChildNodes ? InitLevel::UsrNode : InitLevel::UsrNodeWithChildNode;
    status = InitTeardown::init(initLevel,
                                SyslogFacilityUsrNode,
                                configFilePath,
                                pubSigningKeyFilePath,
                                argv[0],
                                InitFlagsNone,
                                nodeId,
                                numNodes,
                                Config::ConfigUnspecifiedNumActiveOnPhysical,
                                BufferCacheMgr::TypeUsrnode);
    BailIfFailedMsg(moduleName,
                    status,
                    "InitTeardown::init failed: %s",
                    strGetFromStatus(status));

    status = usrnodeSetupSigHandler();
    if (status != StatusOk) {
        status = sysErrnoToStatus(errno);
        printError("Failed usrnodeSetupSigHandler: %s\n",
                   strGetFromStatus(status));
        return status.code();
    }

    status = usrnodeMonitorSetup();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed usrnodeMonitorSetup: %s",
                    strGetFromStatus(status));

    // usrnode main creates threads for various messaging interfaces. usrnode
    // shuts down once these threads die.
    msgMgr = MsgMgr::get();
    if (numNodes > 1) {
        // Set up dedicated recv thread to receive messages from any node.
        status = msgMgr->wait();
        BailIfFailedMsg(moduleName,
                        status,
                        "Failed to spawn message thread: %s",
                        strGetFromStatus(status));

        // Spin until recv thread is ready
        numReady = msgMgr->spinUntilRecvThreadReady();
        if (numReady < 0) {
            status = msgMgr->getRecvThrStatus();
            xSyslog(moduleName,
                    XlogInfo,
                    "Failed to initialize receive"
                    " thread. Status: %s",
                    strGetFromStatus(status));
            goto CommonExit;
        }

        xSyslog(moduleName, XlogInfo, "%d recv thread is ready", numReady);
    }

    // Set up dedicated connections to send messages to each node
    // in the cluster
    status = msgMgr->initSendConnections();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to setup connections to other nodes: %s",
                    strGetFromStatus(status));

    // There is one send thread to send message each node in the system, and
    // another thread to do local IOs. So the total is equal to
    // numNodes. It is ok to read sendThreadsReady dirty as it gets
    // updated with a lock. Spin until recv thread is ready
    status = msgMgr->spinUntilSendThreadsReady();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to wait for send threads be become ready: %s",
                    strGetFromStatus(status));

    xSyslog(moduleName,
            XlogInfo,
            "1 local and %d remote send threads ready",
            numNodes - 1);

    runtimeSetupArgs.licenseFilePath = licenseFilePath;
    initRuntimeDone = true;
    status = shimFunction<UsrNodeRuntimeSetupArgs *,  // Argument Type; ignored
                          Status,                     // Return Type
                          usrnodeRuntimeSetup>(&runtimeSetupArgs);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to set up usrnode runtime: %s",
                    strGetFromStatus(status));

    status = Runtime::get()->tuneWithParams();
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to tune runtime with params: %s",
                    strGetFromStatus(status));

    // This starts the listener. This thread blocks until shutdown is received
    status = Runtime::get()->createBlockableThread(&listenerThreadId,
                                                   NULL,
                                                   xcApiStartListener,
                                                   NULL);
    BailIfFailedMsg(moduleName,
                    status,
                    "Failed to create listener thread: %s",
                    strGetFromStatus(status));

    xSyslog(moduleName, XlogCrit, "All nodes now network ready");

    // Now that usrnode is up, indicate this to the log subsystem, so it can
    // turn off per-log flush for non-DEBUG builds, and any other state
    // transition it may need to do
    xsyslogFacilityBooted();

    sysThreadJoin(listenerThreadId, NULL);

    // Verify that we got here as the result of a shutdown
    if (usrNodeNormalShutdown()) {
        xSyslog(moduleName, XlogInfo, "Shutdown processing started");
    } else {
        xSyslog(moduleName, XlogErr, "Unexpected shutdown started");
    }

    //
    // ALL THE CODE BELOW THIS MUST BE CLEANOUT!
    //

CommonExit:
    if (usrNodeForceShutdown()) {
        // XXX log shutdown started here, may want to take a checkpoint and
        //     log shutdown complete if force shutdown was requested

        // Shutdown force will hang on libNs->deleteAllObjs if there is more
        // than one node in the cluster, so we'll just exit immediately
        xSyslog(moduleName, XlogWarn, "Shutdown force request - terminating");
        xcalarExit(1);
    }

    if (launchedFromMonitor) {
        xSyslog(moduleName, XlogInfo, "Monitor teardown");
        usrnodeMonitorTeardown();
    }

    if (initRuntimeDone == true) {
        xSyslog(moduleName, XlogInfo, "Starting usrnodeRuntimeTeardown");
        Status retStatus = shimFunction<void *,  // Argument Type; ignored
                                        Status,  // Return Type
                                        usrnodeRuntimeTeardown>(NULL);
        if (retStatus != StatusOk) {
            xSyslog(moduleName,
                    XlogWarn,
                    "Failed to teardown usrnode runtime: %s. Force shutdown",
                    strGetFromStatus(status));
            xcalarExit(1);
        }
    }

    //
    // ONLY LOCAL CLEANUP FROM THIS POINT ON!
    //

    if (initTeardownDone == true) {
        if (msgMgr != NULL) {
            xSyslog(moduleName,
                    XlogInfo,
                    "Waiting until send/recv threads exit");
            msgMgr->spinUntilSendThreadsExit();
            msgMgr->spinUntilRecvThreadExit();
        }

        if (InitTeardown::get() != NULL) {
            xSyslog(moduleName, XlogInfo, "InitTeardown::teardown");
            InitTeardown::get()->teardown();
        }
    }

    xSyslog(moduleName, XlogInfo, "Teardown signal handler");
    usrnodeTeardownSigHandler();

    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Exiting with error: %s",
                strGetFromStatus(status));
    }

    xsyslogDestroy();
    return (int) status.code();
}
