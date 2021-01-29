// Copyright 2016 - 2017 Xcalar, Inc.  All rights reserved.
//
// No use or distribution of this source code is premitted in any form or
// any means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// -------------------------------------------------------------------------
// UsrNodeMonInterface.c: Manages the interface between the user node and
//                        the corresponding monitor instance.
//
// The monitor port number to use is specifed in the config file.  The
// monitor instance to be connected to is assumed to be on the local host.
// -------------------------------------------------------------------------
//

#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdint.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>

#include "StrlFunc.h"
#include "config/Config.h"
#include "monitor/MonListenerC.h"
#include "util/MemTrack.h"
#include "primitives/Primitives.h"
#include "util/System.h"
#include "sys/XLog.h"
#include "runtime/Runtime.h"
#include "strings/String.h"

static void *usrNodeMonitorThread(void *unused);

static constexpr const char *moduleName = "UsrNodeMonInterface";

static bool monitorThreadStarted = false;
static bool monitorInterfaceInit = false;
static int monitorInterfacePort = 0;

// Stats
static long int statMonEnabled = 0;
static long int statMonDisabled = 0;
static long int statConfigChange = 0;
static long int statConfigCommit = 0;
static long int statNodeEntries = 0;

// The monitor's view of the cluster configuration.  currentConfig is the
// state of the cluster as of the last cluster change commit.  pendingConfig
// is the state received from cluster change pending merged with the current
// configuration.
static ConfigInfo currentConfig[MaxNodes];
static ConfigInfo pendingConfig[MaxNodes];

static pthread_t listenerThread;

//
// Monitor interface intialization
//
Status
usrNodeMonitorNew()
{
    int ii;
    Status status = StatusUnknown;
    Config *config = Config::get();

    xSyslog(moduleName, XlogDebug, "Intitializing monitor interface");

    monitorInterfacePort = config->getMonitorPort(config->getMyNodeId());

    if (monitorInterfacePort < 1025 || monitorInterfacePort > 65535) {
        xSyslog(moduleName,
                XlogCrit,
                "Invalid monitor port in config file: %d",
                monitorInterfacePort);
        status = StatusMonPortInvalid;
        goto UsrNodeMonitorNewExit;
    }

    // Initialize the status arrays
    for (ii = 0; ii < MaxNodes; ii++) {
        currentConfig[ii].nodeId = ii;
        pendingConfig[ii].nodeId = ii;
        memZero(&currentConfig[ii].hostName, sizeof(currentConfig[0].hostName));
        memZero(&pendingConfig[ii].hostName, sizeof(pendingConfig[0].hostName));
        currentConfig[ii].port = 0;
        pendingConfig[ii].port = 0;
        currentConfig[ii].status = CONFIG_HOST_STATUS_UNKNOWN;
        pendingConfig[ii].status = CONFIG_HOST_STATUS_UNKNOWN;
    }

    // Create a separate thread to interact with the monitor
    memZero(&listenerThread, sizeof(listenerThread));

    status = Runtime::get()->createBlockableThread(&listenerThread,
                                                   NULL,
                                                   usrNodeMonitorThread,
                                                   NULL);
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogErr,
                "Monitor interface thread create failed: %s",
                strGetFromStatus(status));
        goto UsrNodeMonitorNewExit;
    }
    monitorThreadStarted = true;
    monitorInterfaceInit = true;
    status = StatusOk;

UsrNodeMonitorNewExit:

    if (status != StatusOk) {
        if (monitorThreadStarted) {
            status.fromStatusCode((StatusCode) pthread_cancel(listenerThread));
            status.fromStatusCode(
                (StatusCode) sysThreadJoin(listenerThread, NULL));
        }
    }

    return status;
}

// Monitor interface termination
void
usrNodeMonitorDestroy()
{
    Status status = StatusUnknown;
    void *retval = 0;

    if (!monitorInterfaceInit) {
        return;
    }

    xSyslog(moduleName, XlogDebug, "Terminating monitor interface");

    monitorInterfaceInit = false;

    // Kill the monitor listener thread
    status.fromStatusCode((StatusCode) pthread_cancel(listenerThread));
    if (status != StatusOk) {
        xSyslog(moduleName,
                XlogDebug,
                "Monitor thread cancel failed: %s",
                strGetFromStatus(status));
    } else {
        status.fromStatusCode(
            (StatusCode) sysThreadJoin(listenerThread, &retval));
        if (status != StatusOk) {
            xSyslog(moduleName,
                    XlogDebug,
                    "Monitor thread join returned: %d, %s",
                    status.code(),
                    strGetFromStatus(status));
        }
    }

    xSyslog(moduleName, XlogDebug, "Monitor interface stats:");
    xSyslog(moduleName, XlogDebug, "Enabled calls: %ld", statMonEnabled);
    xSyslog(moduleName, XlogDebug, "Disabled calls: %ld", statMonDisabled);
    xSyslog(moduleName, XlogDebug, "Config changes: %ld", statConfigChange);
    xSyslog(moduleName, XlogDebug, "Config commits: %ld", statConfigCommit);
    xSyslog(moduleName, XlogDebug, "Total node entries: %ld", statNodeEntries);
}

// Monitor listener event functions.  Functions needed are:
//  - monitor enabled
//  - monitor disabled
//  - cluster state change is pending
//  - cluster state change is complete

// Monitor enabled broadcast receiver
static void
monEnabled()
{
    xSyslog(moduleName, XlogInfo, "Monitor enabled function called");
    statMonEnabled++;
}

// Monitor disabled broadcast receiver
static void
monDisabled()
{
    xSyslog(moduleName, XlogInfo, "Monitor disabled function called");
    statMonDisabled++;
}

// Monitor cluster state change pending receiver.
// Assume that changes can be cumulative until a commit is received, so a
// change can be "aborted" by sending another change to undo the original.
static void
monClusterChangePending(ConfigInfo *configInfoArray,
                        uint32_t configInfoArrayCount)
{
    uint32_t ii;
    size_t ret;
    ConfigNodeId loopNodeId;
    ConfigInfo *inputConfig;

    xSyslog(moduleName,
            XlogInfo,
            "Monitor config change function called with %d entries",
            configInfoArrayCount);

    statConfigChange++;

    assert(configInfoArrayCount > 0);
    statNodeEntries = statNodeEntries + configInfoArrayCount;

    inputConfig = configInfoArray;

    // Make no assumptions about either the order of the entries passed from
    // the monitor or that the status will be passed for all nodes.
    for (ii = 0; ii < configInfoArrayCount; ii++) {
        loopNodeId = inputConfig->nodeId;
        assert(loopNodeId < MaxNodes);
        if (loopNodeId >= MaxNodes) {
            xSyslog(moduleName,
                    XlogErr,
                    "Invalid node ID update received and ignored");
            continue;
        }
        ret = strlcpy(pendingConfig[loopNodeId].hostName,
                      inputConfig->hostName,
                      sizeof(pendingConfig[0].hostName));
        if (ret > sizeof(pendingConfig[0].hostName)) {
            xSyslog(moduleName,
                    XlogWarn,
                    "Monitor config change received a mal-formed host name");
        }
        pendingConfig[loopNodeId].status = inputConfig->status;
        pendingConfig[loopNodeId].port = inputConfig->port;
        pendingConfig[loopNodeId].sockAddr = inputConfig->sockAddr;
        inputConfig++;
    }
}

// Monitor cluster state change complete receiver
// No change is acted upon until a commit is received.
static void
monClusterChangeComplete()
{
    int ii;

    xSyslog(moduleName,
            XlogInfo,
            "Monitor cluster change complete function called");
    statConfigCommit++;

    // Process the change(s) received
    // - Copy the node information to the currentConfig for the node affected
    //     (Copy all data since a state change is how a new node is introduced)
    // - Act on the change if necessary
    // XXX for now, it is assumed that after the initial host up broadcast,
    //     only the status of the node will change
    for (ii = 0; ii < MaxNodes; ii++) {
        assert(currentConfig[ii].nodeId == pendingConfig[ii].nodeId);

        if (pendingConfig[ii].status != currentConfig[ii].status) {
            currentConfig[ii].port = pendingConfig[ii].port;
            currentConfig[ii].sockAddr = pendingConfig[ii].sockAddr;
            Status status = strStrlcpy(currentConfig[ii].hostName,
                                       pendingConfig[ii].hostName,
                                       sizeof(currentConfig[ii].hostName));
            assert(status == StatusOk);

            assert(currentConfig[ii].status != CONFIG_HOST_UP);
            if (currentConfig[ii].status == CONFIG_HOST_UP) {
                // Dead code path.
                xcalarExit(1);
            }
            currentConfig[ii].status = pendingConfig[ii].status;
        }
    }
}

// Monitor listener thread main function
static void *
usrNodeMonitorThread(void *inputArg)
{
    struct MonitorListenFuncs listenFns;

    // Set up the functions to register with the monitor
    memZero(&listenFns, sizeof(listenFns));

    listenFns.monitorEnabled = monEnabled;
    listenFns.monitorDisabled = monDisabled;
    listenFns.clusterStateChangePending = monClusterChangePending;
    listenFns.clusterStateChangeComplete = monClusterChangeComplete;

    assert(listenFns.monitorEnabled != NULL);

    xSyslog(moduleName, XlogDebug, "Monitor listener thread started");

    assert(inputArg == NULL);

    monitorListen(monitorInterfacePort, &listenFns);

    xSyslog(moduleName, XlogDebug, "Monitor listener thread ended");

    pthread_exit((void *) StatusOk.code());
}
