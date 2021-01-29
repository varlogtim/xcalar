// Copyright 2015 - 2018 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//
// monitor.cpp: The cluster monitor code
//

#include <stdio.h>
#include <string.h>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <netdb.h>
#include <sys/time.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <map>
#include <set>
#include <string>

#include "monitor/MonConnection.h"
#include "monitor/MonConfig.h"
#include "test/SimNode.h"
#include "util/Atomics.h"
#include "config/Config.h"
#include "constants/XcalarConfig.h"
#include "StrlFunc.h"

//
// The monitor uses an implementation of the RAFT majority election algorithm.
// See the RAFT paper for details.  It uses a UDP socket to send
// point-to-point messages to each node member involved in the election.
// The election proceeds through the following states.
//
// The monitor is started on each node of the cluster.  The monitor on node 0
// is the only one that can become the master.  All other monitors will become
// slaves.  Once ALL the monitors are up and connected to the master, quorum
// is established and the monitors start their respective usrnodes.
//
// If any monitor or any usrnode dies then all of the monitors and usernodes
// are killed or kill themselves.
//
// ElectionWaitMonitor
//
// The monitor starts in the ElectionWaitMonitor state with a current term of
// 1.  The term will be updated as the election proceeds.
// The monitor for node 0 stays in this state for a minimum time of
// minElectionTime up to a randomly generated end time of maxElectionWaitTime.
// The current time is between 1 and 5 seconds.  After this wait time it
// transitions to CandidateMonitor state.  Only node 0 can be the "master".
//
// All monitors other than node 0 remain in this state until it receives a
// message from the candidate (node 0).  Once it receives such a message it
// transitions to ConnectingtoMaster.
//
// CandidateMonitor (only node 0)
//
// In this state the monitor tries to be elected master.  It stays in this
// state until
// 1) It gets a quorum of slaves.  In this case it goes to MasterMonitor state.
//    Currently the quorum is ALL the other nodes.
// 2) It times out.  It will kill itself and the entire cluster must be
//    restarted.
//
// While in this state it sends CandidateMsg messages to all nodes that have
// not already pledged themselves as slaves.
//
// ConnectingToMaster
//
// In this state a node is trying to connect to a master.  It will either
// successully connect and go to SlaveMonitor or fail and kill itself.
//
// SlaveMonitor
//
// In this state the node has successfully connected to the master.  It will
// stay in this state until the master disconnects in which case it will shut
// down its usrnode and then die.
//
// MasterMonitor
//
// In this state the master has acquired a quorum of slaves.  It will stay in
// this state until it loses its quorum in which case it will shut down its
// usrnode and then die.
//

enum MonitorState {
    ElectionWaitMonitor = 1,
    CandidateMonitor = 2,
    ConnectingToMaster = 3,
    SlaveMonitor = 4,
    MasterMonitor = 5,
};

static struct option cmdLineOptions[] = {
    // char *name, int has_arg, int *flag, int val (really a char)
    {"port", required_argument, 0, 'p'},
    {"config", required_argument, 0, 'c'},
    {"nodeid", required_argument, 0, 'n'},
    {"loglevel", required_argument, 0, 'l'},
    {"maxnodes", required_argument, 0, 'm'},
    {"guardrails", required_argument, 0, 'g'},
    {"licenseFile", required_argument, 0, 'k'},
    {0, 0, 0, 0},
};

class TCPMsgHandlerImpl : public Connection::MsgHandler
{
  public:
    void Msg(Connection *cnx, MsgOp op, void *msgBody, uint32_t msgLength);
};

class TCPNotifierImpl : public Connection::Notifier
{
  public:
    bool Notify(Connection *cnx, NotifierType type, int status);
};

class UDPNotifierImpl : public Connection::Notifier
{
  public:
    bool Notify(Connection *cnx, NotifierType type, int status);
};

struct NewSlaveMsg {
    ConfigNodeId nodeId;
    uint64_t configVersion;
};

struct ElectionMsg {
    enum MsgType { MasterMsg, CandidateMsg };
    MsgType type;
    uint64_t term;
    uint64_t configVersion;
    ConfigNodeId nodeId;
};

struct SlaveInfo {
    uint64_t heartbeatSendTime;
    bool heartbeatPending;
    Ref<Connection> cnx;
    ConfigNodeId nodeId;
    bool configChangePending;
};

//
// Map of connected slaves.
//
typedef std::map<uint64_t, SlaveInfo> SlaveMap;

//
// Set of nodes to send election messages to.  The local node and any connected
// slaves are not in the map.
//
typedef std::set<ConfigNodeId> SendToSet;

//
// The map of configured nodes.
// XXX: Iron out the exact usage of this.
//
typedef std::map<uint64_t, ConfigInfo> ConfigMap;

//
// Map of connected listeners for monitor updates.  We currently assume that
// only the usrnode is a listener and loss of the connection means the usrnode
// has gone done.
//
typedef std::map<uint32_t, Ref<Connection> > NotifierCnxMap;

static int ourMonitorPort = -1;

// XXX remove init to /dev/null once init parm checking makes
//      this superfluous.  It is only to produce predictable
//      failures if config not specifed or is incorrect.
static std::string monitorDir = "/dev/null";
static std::string rootDir;
static std::string logDirDefault = "/var/log/xcalar";
static std::string logDir;
static std::string clusterConfigPath = "/dev/null/config";
static std::string licenseFile = "";

// When testing, the following #define can be uncommented out and
// a slowness factor applied by "redefining" oneTimeUnit.
// #define SLOWDOWN
#ifdef SLOWDOWN
// 10 seconds
static const uint64_t oneTimeUnit = 10 * 1000 * 1000;
#else
// 1 second
static const uint64_t oneTimeUnit = 1 * 1000 * 1000;
#endif
static const uint64_t tenthOfATimeUnit = oneTimeUnit / 10;

// These timeouts are set to fairly long durations as having shorter
// ones were leading to timeouts for as yet TBD reasons (Xc-8287).  In
// most cases if a usrnode dies on an active cluster the remaining usrnodes
// will encounter message failures and panicNoCore() well before these
// durations are hit.

// This is the length of time that a master takes before detecting
// a down slave and killing its usrnode and itself.  Base it on the
// size of the cluster.
static uint64_t masterSlaveTimeout = -1;

// How long a slave waits for a master heartbeat.
// This is the length of time that the slave takes before detecting
// a down master and killing its usrnode and itself.  Base it on the
// size of the cluster.
static uint64_t slaveMasterTimeout = -1;

// Whether or not we (slave) require a heartbeat from the master.  The
// master doesn't start sending heartbeats until it reaches cluster quorum.
// Currently cluster quorum is "all nodes in the cluster" at which point
// we start our usrnode and will require heartbeats from the master.
static bool slaveMasterTimeoutEnabled = false;

// Master sends heartbeats in steady state (i.e. after cluster's in quorum),
// if the boolean sendHeartBeats is true. If it's false, the monitor logic
// relies just on the TCP keep-alive to keep connections alive, and to detect
// dead nodes. The TCP mechanism is superior in that the keep alive heart
// beats occur in the kernel, and so will occur at higher priority than
// user level heart-beats, which is critical in high load environments.
// See comments for use of the socket option SO_KEEPALIVE in connection.cpp.
static bool sendHeartBeats = false;  // off by default

// How often the master checks heartbeat status.  Computed based on the size
// of the cluster.
static uint64_t masterNetHeartbeatCheckInterval = -1;

// How often to poll for incoming messages.
static uint64_t msgPollInterval = oneTimeUnit;
// When we tried to connect to the master.
static uint64_t masterConnectTime;
// How long to wait for a master connect to succeed.
static uint64_t masterConnectTimeout = 120 * oneTimeUnit;
// Last time we received or sent a master heartbeat.
static uint64_t lastMasterNetHeartbeatTime;
// Last time we sent a MasterMsg election message.
static uint64_t lastMasterMsgTime;
// Minimum time in any election state.
static uint64_t minElectionTime = 1 * oneTimeUnit;
// How often to send a MasterMsg. Its set to half
// of the minElectionTime to make sure at least one
// MasterMsg makes it to each node in an election.
static uint64_t masterMsgInterval = minElectionTime / 2;

// following times probably need to be proportional to cluster size since a
// usrnode on a clean shutdown triggers a cluster wide shutdown, before
// proceeding to shut itself down - so the xcmon has to wait longer, the larger
// the cluster is, for its usrnode to die
//
// NOTE!
// reapUsrnodeMaxWaitTime must be kept in sync with supervisor.conf !
// stowaitsecs in supervisor.conf for xcmonitor = 60 + reapUsrnodeMaxWaitTime
//
static uint64_t reapUsrnodeWaitTime = -1;
static uint64_t reapUsrnodeMaxWaitTime = -1;

// A cluster change prepare is in progress.
static bool clusterChangePrepareInProgress;
// A cluster change prepare is pending.
static bool clusterChangePreparePending;
// A cluster change commit is pending.
static bool clusterChangeCommitPending;
// A clean shutdown is in progress
static Atomic32 xcmonShutDownInProgress;
// Received handle new config message from master
static bool receivedHandleNewConfig = false;

// Current election term.
static uint64_t currentTerm = 1;
// Time in milliseconds when this election ends.
static uint64_t electionEndTime;
// Maximum time to be in ElectionWaitMonitor.
static uint64_t maxElectionWaitTime = 1 * oneTimeUnit;
// Maximum time for an election.  Set based on cluster size.
static uint64_t maxElectionCandidateTime = -1;

// How often to send a candidate messages.
static uint64_t candidateMsgInterval = oneTimeUnit;
// The last time that we sent an election message.
static uint64_t lastElectionMessageTime;
// The UDP connection used for election messages.
static Ref<Connection> udpConnection;

// How many slaves are waiting for a response from.
static uint32_t clusterChangePendingCount;
// The current state of the monitor.
static MonitorState monitorState = ElectionWaitMonitor;
// The handler for TCP messages.
static TCPMsgHandlerImpl tcpMsgHandler;
// The notifier for TCP messages.
static TCPNotifierImpl tcpNotifier;
// The notifier for UDP messages.
static UDPNotifierImpl udpNotifier;
// The connection to the master.
static Ref<Connection> masterCnx;
// The node id of this node.
static ConfigNodeId ourNodeId = -1;
// The pid of the usrnode spawned by this node
static pid_t ourUsrnode = -1;
// Maximum number of nodes.  Overrides the NumNodes value in the config
// file (used for testing).
static constexpr int MaxNodesUnspecified = -1;
static int maxNodes = MaxNodesUnspecified;
// Map of connected slaves.
static SlaveMap slaveMap;
// Map of config nodes.
static ConfigMap configMap;
// The last time the config file was modified.
static time_t configFileModTime;
// The new config when we are changing the config.
static ConfigMap newConfigMap;
// Map of connected monitor listeners.
static NotifierCnxMap notifierCnxMap;
//
// Set of node to send election messages to.  Doesn't contain ourselves or any
// connected slaves.
//
static SendToSet sendToSet;

//
// Mutex to protect the config.  Needed because the http thread can
// asynchronously read the config.
//
static pthread_mutex_t configMutex = PTHREAD_MUTEX_INITIALIZER;

static constexpr const char *moduleName = "xcmonitor";

static pthread_t monitorSignalHandlerThreadId;

//
// getRandomElectionEndTime --
//
// Generate the random election end time. It is between
// minElectionTime and minElectionTime + maxTime.
//
uint64_t
getRandomElectionEndTime(uint64_t maxTime)
{
    uint32_t randCount = random() % (maxTime - minElectionTime);
    uint64_t endTime = minElectionTime + randCount;
    INFO(moduleName, "getRandomElectionEndTime %ld/%ld", endTime, maxTime);
    return CurrentTime() + endTime;
}

//
// getFixedElectionEndTime --
//
// Generate the fixed election end time.
//
uint64_t
getFixedElectionEndTime(uint64_t maxTime)
{
    uint64_t endTime = maxTime;
    INFO(moduleName, "getFixedElectionEndTime %ld/%ld", endTime, maxTime);
    return CurrentTime() + endTime;
}

//
// In our simplistic implementation, all nodes must be present to
// establish quorum.
static bool
iHaveQuorum()
{
    uint64_t numNodes = slaveMap.size() + 1;  // count myself

    // All nodes must be present and up to have "quorum".
    if (numNodes == configMap.size()) {
        INFO(moduleName, "I AM IN QUORUM");
        return true;
    } else {
        INFO(moduleName, "I AM NOT IN QUORUM");
        return false;
    }
}

// Start the usrnode and remember the Pid.
static void
startUsrNode(ConfigNodeId myId,
             int numNodes,
             const char *configFile,
             const char *licenseFile,
             const char *outputDir)
{
    SimNodeChildren *child;
    char *xcm_nousrenv;

    INFO(moduleName, "xcmonitor about to start a usrnode");
    if ((xcm_nousrenv = getenv("XCMONITOR_NOUSRNODES")) != NULL) {
        if (strcmp(xcm_nousrenv, "1") == 0) {
            INFO(moduleName, "xcmonitor not starting usrnodes");
            return;
        }
    }

    INFO(moduleName, "Start UsrNode: myId is %d", (int) myId);

    child = simNodeSpawnNode((int) (myId),
                             numNodes,
                             configFile,
                             licenseFile,
                             outputDir);

    INFO(moduleName, "Started UsrNode: pid is %d", child->pid);

    ourUsrnode = child->pid;
}

//
// This routine reaps the xcmonitor's child XCE node - this must be called only
// when it's known that the XCE node is dead - either b/c it was successfully
// killed by the xcmonitor or b/c xcmonitor received a SIGCHLD due to the death
// of its XCE node child.
//
// Return StatusOk if the child XCE node is successfully reaped.
//
// If successful, the reaped pid and its status are returned in the arguments if
// the pointers supplied in the arguments are non-NULL.
//
// If not successful, the status indicates reason for failure.
//

static Status
reapUsrNode(int *cstatusp, pid_t *cpidp)
{
    uint64_t maxtries;
    uint64_t ntries = 0;
    Status status;
    pid_t cpid;
    int cstatus;

    maxtries = reapUsrnodeMaxWaitTime / reapUsrnodeWaitTime;
    while (true) {
        cpid = waitpid(ourUsrnode, &cstatus, WNOHANG);
        if (cpid == -1) {
            ERROR(moduleName,
                  "waitpid on usrNode (pid %d) failed:%s",
                  ourUsrnode,
                  strerror(errno));
            status = sysErrnoToStatus(errno);
            break;
        } else if (cpid == ourUsrnode) {
            WARNING(moduleName, "waitpid(pid %d) succeeded", ourUsrnode);
            if (WIFEXITED(cstatus)) {
                INFO(moduleName,
                     "ourUsrnode (pid %d) exited: status=%d",
                     ourUsrnode,
                     WEXITSTATUS(cstatus));
            } else if (WIFSIGNALED(cstatus)) {
                INFO(moduleName,
                     "ourUsrnode (pid %d) killed by sig %d",
                     ourUsrnode,
                     WTERMSIG(cstatus));
            }
            status = StatusOk;
            break;
        } else if (cpid == 0) {
            // all is good, but pid hasn't exited yet; so continue trying
            INFO(moduleName,
                 "ourUsrnode (pid %d) alive; retry %ld (max %ld)"
                 "; sleep for %ld secs",
                 ourUsrnode,
                 ntries,
                 maxtries,
                 reapUsrnodeWaitTime / oneTimeUnit);
            if (ntries >= maxtries) {
                status = StatusUsrnodeStillAlive;
                break;
            }
        }
        sleep(reapUsrnodeWaitTime / oneTimeUnit);
        ntries++;
    }
    if (status == StatusOk) {
        if (cstatusp != NULL) {
            *cstatusp = cstatus;
        }
        if (cpidp != NULL) {
            *cpidp = cpid;
        }
    }
    return status;
}

//
// Called if xcmonitor receives a SIGCHLD - so XCE node must've died. In which
// case, the xcmonitor must first reap the XCE node, and then kill itself. Note
// that a SIGCHLD may be received also b/c a killUsrNodeAndDie() may have been
// invoked - in which case, both killUsrNodeAndDie() and reapUsrNodeAndDie()
// would race to invoke reapUsrNode(). If this occurs, the atomic increment of a
// global counter (xcmonShutDownInProgress) allows only one of them to win-
// whoever wins gets to reap the XCE node, and then exit the xcmonitor. The
// loser just exits its thread. See killUsrNodeAndDie() for the competing
// call to reapUsrNode().
//
static void
reapUsrNodeAndDie()
{
    Status status;
    int cstatus;
    pid_t cpid;
    int32_t sdInProg;

    sdInProg = atomicInc32(&xcmonShutDownInProgress);
    if (sdInProg > 1) {
        INFO(moduleName, "shutdown already in progress; exit reap thread");
        pthread_exit(NULL);
    }
    status = reapUsrNode(&cstatus, &cpid);
    // since SIGCHLD was received, how can reapUsrNode fail? Assert this.
    // Even if it did fail (for some bizarre reason), nothing can be done
    // but xcmonitor must exit in any case (logs will report reason for
    // reap failure).
    assert(status == StatusOk);
    assert(cpid == ourUsrnode);

    // Since this is called only when the XCE child node has died, the exit
    // status in xcmonitor's exit call can't be clean (i.e. 0).

    xcalarExit(1);
}

static void
killUsrNodeAndDie(int sig)
{
    bool killForce = false;
    bool cleanExit = false;
    int32_t sdInProg;
    int krc;
    Status reapStatus;
    int cstatus;
    pid_t cpid;

    if (ourUsrnode == -1) {
        // must be in no-usrnode env - i.e. no usrnode was created
        INFO(moduleName, "killUsrNodeAndDie: no usrnode - just exit");
        xcalarExit(1);
    }
    sdInProg = atomicInc32(&xcmonShutDownInProgress);
    if (sdInProg > 1) {
        INFO(moduleName, "shutdown already in progress; exit kill thread");
        pthread_exit(NULL);
    }

    //
    // XXX: implement serialization; for now, if there are two concurrent calls
    // to this routine, it'll work correctly, since the SIGTERM issue triggers a
    // clean shutdown of usrnode, which is idempotent, and concurrent calls to
    // xcalarExit() are handled correctly inside xcalarExit(). Note that the
    // serialization inside xcalarExit() may still be needed, since another
    // thread inside xcmonitor may call xcalarExit() outside this routine. If we
    // clean all that up (in monitor.cpp and libmoninterface code), then we
    // should be able to prevent xcalarExit() from being called concurrently
    // from different threads.
    //
    // If serialization is done, the subsequent callers can just call
    // pthread_exit here, since the winning thread will eventually call
    // xcalarExit().

    WARNING(moduleName, "Kill usrnode with signal %d!!!!", sig);
    if (sig == SIGTERM) {
        // monitor exit can be clean iff sig==SIGTERM and none of the conditions
        // below arise (if they do, cleanExit is made false again).
        cleanExit = true;
    }
    krc = kill(ourUsrnode, sig);
    if (krc == -1) {
        cleanExit = false;
        if (errno == ESRCH) {
            WARNING(moduleName,
                    "ourUsrnode (pid %d) doesn't exist!",
                    ourUsrnode);
        } else {
            ERROR(moduleName,
                  "Couldn't kill usrNode (pid %d) gracefully: %s",
                  ourUsrnode,
                  strerror(errno));
            killForce = true;
        }
    } else {
        reapStatus = reapUsrNode(&cstatus, &cpid);
        assert(reapStatus != StatusOk || cpid == ourUsrnode);
        if (reapStatus != StatusOk) {
            cleanExit = false;
        }
        if (reapStatus == StatusUsrnodeStillAlive) {
            killForce = true;
        }
    }

    if (killForce) {
        if (XcalarConfig::get()->testMode_) {
            INFO(moduleName,
                 "About to forcefully kill usrNode (pid %d). Let's check if "
                 "core-dumping",
                 ourUsrnode);
            while (sysIsProcessCoreDumping(ourUsrnode)) {
                INFO(moduleName,
                     "Usrnode (pid %d) is currently core-dumping. Let's wait. "
                     "Never polite to interrupt someone dumping",
                     ourUsrnode);
                sysSleep(10);
            }
        }

        WARNING(moduleName,
                "Killing usrNode (pid %d) forcefully now",
                ourUsrnode);
        krc = kill(ourUsrnode, SIGKILL);
        if (krc == -1) {
            if (errno == ESRCH) {
                WARNING(moduleName,
                        "ourUsrnode (pid %d) doesn't exist!",
                        ourUsrnode);
            } else {
                ERROR(moduleName,
                      "Couldn't kill usrNode (pid %d) forcefully: "
                      "%s",
                      ourUsrnode,
                      strerror(errno));
            }
        } else {
            WARNING(moduleName,
                    "Killed usrNode (pid %d) forcefully",
                    ourUsrnode);
        }
    }

    if (cleanExit) {
        xcalarExit(0);
    } else {
        xcalarExit(1);
    }
}

//
// updateConfig --
//
// The config has changed so update our configMap and our sendToSet to
// reflect the new config.
//
static void
updateConfig(ConfigMap &newConfigMap, SendToSet &newSendToSet)
{
    unsigned numNodes;

    pthread_mutex_lock(&configMutex);
    configMap.swap(newConfigMap);
    sendToSet.swap(newSendToSet);
    INFO(moduleName,
         "updateConfig: configMap.size() %ld sendToSet.size() %ld",
         configMap.size(),
         sendToSet.size());
    numNodes = sendToSet.size();
    pthread_mutex_unlock(&configMutex);

    // Check if all the other nodes are up.
    if (numNodes + 1 /* me */ == configMap.size()) {
        startUsrNode(ourNodeId,
                     configMap.size(),
                     clusterConfigPath.c_str(),
                     licenseFile.c_str(),
                     logDir.c_str());
        // We now expect heartbeats from the master.
        if (sendHeartBeats) {
            slaveMasterTimeoutEnabled = true;
        } else {
            slaveMasterTimeoutEnabled = false;
        }
        lastMasterNetHeartbeatTime = CurrentTime();
    }
}

const char *
monitorStateToString(MonitorState ms)
{
    switch (ms) {
    case ElectionWaitMonitor:
        return "ElectionWaitMonitor";
    case CandidateMonitor:
        return "CandidateMonitor";
    case ConnectingToMaster:
        return "ConnectingToMaster";
    case SlaveMonitor:
        return "Slave";
    case MasterMonitor:
        return "Master";
    }
    return "";
}

//
// makeSendToSet --
//
// Given a config map generate a set of nodes to send messages to.  All nodes
// except the local node are in the map.
///
void
makeSendToSet(ConfigMap &cm, SendToSet &newSendToSet)
{
    for (ConfigMap::iterator iter = cm.begin(); iter != cm.end(); ++iter) {
        if (iter->second.nodeId != ourNodeId) {
            newSendToSet.insert(iter->second.nodeId);
        }
    }
}

//
// sendEnabledMsg --
//
// A new node's monitor has become enabled.  Send a message to all listeners
// and all slaves with the new status.
//
static void
sendEnabledMsg(Connection *cnx)
{
    cnx->SendMsg(MONITOR_ENABLED_OP, NULL, 0);
    // XXX Needs error handling here
    ConfigInfo *ciArray = new (std::nothrow) ConfigInfo[configMap.size()];
    uint32_t ciIndex = 0;
    for (ConfigMap::iterator configIter = configMap.begin();
         configIter != configMap.end();
         ++configIter) {
        ConfigInfo &ci = ciArray[ciIndex++];
        ci = configIter->second;
    }
    cnx->SendMsg(NEW_CLUSTER_STATUS_PREPARE_OP,
                 ciArray,
                 ciIndex * (unsigned) sizeof(ConfigInfo));
    cnx->SendMsg(NEW_CLUSTER_STATUS_COMMIT_OP, NULL, 0);
    delete[] ciArray;
}

//
// sendNotifierMsg --
//
// Send a message to all listeners for monitor state changes.
//
static void
sendNotifierMsg(MsgOp op, void *msgBody, uint32_t msgLength)
{
    for (NotifierCnxMap::iterator iter = notifierCnxMap.begin();
         iter != notifierCnxMap.end();
         ++iter) {
        if (op == MONITOR_ENABLED_OP) {
            sendEnabledMsg(iter->second);
        } else {
            iter->second->SendMsg(op, msgBody, msgLength);
        }
    }
}

//
// setMonitorState --
//
// Change the monitor state to the new state.  This involves notifying any
// listeners of the new state change.
//
static void
setMonitorState(MonitorState newState)
{
    INFO(moduleName,
         "%ld STATE CHANGE: %s => %s",
         sendToSet.size(),
         monitorStateToString(monitorState),
         monitorStateToString(newState));

    // XXX: The above INFO log may not be emitted in prod builds since the
    // default loglevel for prod builds is WARN. However, tests depend on
    // grepp'ing for this message but in transient stderr output - so emit the
    // same log to stderr - this keeps the log file clean, while allowing tests
    // to continue using this message. This allows the xSyslog() interface to
    // be cleaned up for the rest of the world while restricting this anomaly
    // to monitor code - which should eventually be improved to use a command
    // or some other prorotcol instead of grepping for this in stderr.
    fprintf(stderr,
            "%ld: INFO: %ld STATE CHANGE: %s => %s\n",
            CurrentTime(),
            sendToSet.size(),
            monitorStateToString(monitorState),
            monitorStateToString(newState));
    fflush(stderr);

    if (newState == SlaveMonitor || newState == MasterMonitor) {
        // We're now either a slave or a master.  If we're a master and a slave
        // goes down then the cluster must come down.
        // If we're a slave then it depends on whether or not we allow slave
        // failures during cluster formation.
        sendNotifierMsg(MONITOR_ENABLED_OP, NULL, 0);
    } else if (monitorState == SlaveMonitor || monitorState == MasterMonitor) {
        sendNotifierMsg(MONITOR_DISABLED_OP, NULL, 0);
    }
    monitorState = newState;
    if (newState == ElectionWaitMonitor) {
        if (ourNodeId == 0) {
            //
            // We are in the ElectionWaitMonitor state so now we need to wait
            // for Candidate and Master messages for at most
            // maxElectionWaitTime.
            //
            electionEndTime = getFixedElectionEndTime(maxElectionWaitTime);
        } else {
            // Cannot become master.  But need a timeout for the election to
            // complete
            electionEndTime = getFixedElectionEndTime(maxElectionCandidateTime);
        }
    }
}

static Status
getConfigValues(const char *configPath)
{
    XcalarConfig *config = NULL;
    Status status = StatusUnknown;

    status = Config::get()->loadConfigFiles(configPath);
    if (status != StatusOk) {
        goto CommonExit;
    }

    config = XcalarConfig::get();

    // Extract the Xcalar log dir path
    logDir = Config::get()->getLogDirPath();

    // Extract the Xcalar root path
    rootDir = Config::get()->getRootPath();

    // Constants.XcMonElectionTimeout
    maxElectionCandidateTime = config->xcMonElectionTimeout_ * oneTimeUnit;

    // Constants.XcMonMasterSlaveTimeout
    masterSlaveTimeout = config->xcMonMasterSlaveTimeout_ * oneTimeUnit;

    // Constants.XcMonSlaveMasterTimeout
    slaveMasterTimeout = config->xcMonSlaveMasterTimeout_ * oneTimeUnit;

    // Constants.XcMonUsrNodeReapTimeout
    reapUsrnodeMaxWaitTime = config->xcMonUsrNodeReapTimeout_ * oneTimeUnit;

    status = StatusOk;
CommonExit:
    return status;
}

//
// getNodeInfo --
// Read the configuration file to find all the monitor instances, the
// hosts they reside on and the port number that should be used to
// communicate with them.
static Status
getNodeInfo(const char *configPath,
            ConfigMap &newConfigMap,
            SendToSet &newSendToSet,
            time_t &modTime)
{
    Config *config = Config::get();
    struct stat configStat;
    Status status = StatusUnknown;
    int ret, numNodes;

    errno = 0;
    ret = stat(configPath, &configStat);
    if (ret < 0) {
        status = sysErrnoToStatus(errno);
        ERROR(moduleName,
              "Failed to stat config file '%s': %s",
              configPath,
              strGetFromStatus(status));
        goto CommonExit;
    }

    status = config->loadConfigFiles(configPath);
    if (status != StatusOk) {
        ERROR(moduleName,
              "Failed to load config file '%s': %s",
              configPath,
              strGetFromStatus(status));
        goto CommonExit;
    }

    modTime = configStat.st_mtime;
    numNodes = config->getTotalNodes();

    if (numNodes < 1 || numNodes > 256) {
        WARNING(moduleName, "Unexpected number of nodes: %d", numNodes);
    }

    if (maxNodes != MaxNodesUnspecified) {
        // User artificially limits the maximum number of nodes.
        numNodes = xcMin(numNodes, maxNodes);
    }

    for (int ii = 0; ii < numNodes; ii++) {
        ConfigInfo &configInfo = newConfigMap[ii];
        configInfo.nodeId = ii;
        size_t ret;
        if (ii == ourNodeId) {
            configInfo.status = CONFIG_HOST_UP;
        } else {
            newSendToSet.insert(ii);
            configInfo.status = CONFIG_HOST_DOWN;
        }
        ret = strlcpy(configInfo.hostName,
                      config->getIpAddr(ii),
                      sizeof(configInfo.hostName));
        if (ret >= sizeof(configInfo.hostName)) {
            ERROR(moduleName,
                  "\"%s\" too long (%lu chars). Max is %lu chars",
                  config->getIpAddr(ii),
                  strlen(config->getIpAddr(ii)),
                  sizeof(configInfo.hostName) - 1);
            status = StatusNoBufs;
            goto CommonExit;
        }

        configInfo.port = config->getMonitorPort(ii);
        if (configInfo.port <= 0) {
            ERROR(moduleName, "Invalid port '%d' specified", configInfo.port);
            goto CommonExit;
        }

        status = sockGetAddr(configInfo.hostName,
                             configInfo.port,
                             SocketDomainUnspecified,
                             SocketTypeDgram,
                             &configInfo.sockAddr);
        if (status != StatusOk) {
            ERROR(moduleName,
                  "Failed to get socket address for host '%s' "
                  "port %d: %s",
                  configInfo.hostName,
                  configInfo.port,
                  strGetFromStatus(status));
            goto CommonExit;
        }
    }

    status = StatusOk;
CommonExit:
    return status;
}

//
// SendElectionMessage --
//
// Send an election messages to all nodes in sendToSet.
//
void
sendElectionMessages()
{
    static uint64_t sendSeq = 0;
    static constexpr int MaxGroupSize = 25;
    int numMessages = sendToSet.size();
    uint64_t groupSize = xcMin(numMessages, MaxGroupSize);
    // Note that this will result in some portions having portionCount + 1
    int numGroups = numMessages ? (numMessages / groupSize) : 0;

    // We want to send messages to only a portion of the cluster at a time so
    // as to not overwhelm the networking.
    // XXX: Simplify this (Xc-10372).
    sendSeq++;

    for (SendToSet::iterator iter = sendToSet.begin(); iter != sendToSet.end();
         ++iter) {
        if ((*iter + sendSeq) % numGroups == 0) {
            ConfigMap::iterator configIter = configMap.find(*iter);
            if (configIter == configMap.end()) {
                ERROR(moduleName,
                      "Couldn't find node %ld in config map",
                      *iter);
                xcalarExit(1);
            }
            ElectionMsg msg;
            msg.type = monitorState == MasterMonitor
                           ? ElectionMsg::MasterMsg
                           : ElectionMsg::CandidateMsg;
            msg.term = currentTerm;
            msg.configVersion = configFileModTime;
            msg.nodeId = ourNodeId;

            errno = 0;
            if (sendto(udpConnection->GetSocket(),
                       &msg,
                       sizeof(msg),
                       0,
                       &configIter->second.sockAddr.ip,
                       configIter->second.sockAddr.addrLen) != sizeof(msg)) {
                ERROR(moduleName,
                      "ElectionMgr::SendMsg: sendto failed for "
                      "socket %d: %s",
                      udpConnection->GetSocket(),
                      strGetFromStatus(sysErrnoToStatus(errno)));
            }
        }
    }
    lastElectionMessageTime = CurrentTime();
}

//
// processElectionWait --
//
// We are in ElectionWaitMonitor state.  In this state we wait up to
// electionEndTime for a candidate or master message from a node with a term
// >= our term.  If we didn't get one in time then we will transition to
// CandidateMonitor state.
//
static void
processElectionWait()
{
    uint64_t currentTime = CurrentTime();

    // Only node 0 can become master; so no point for anyone else from becoming
    // a candidate for master. In a system where quorum must be ALL nodes, which
    // is the current design, there must always be node0, and in a system where
    // re-election is not supported, as in the current design, this constraint
    // is needed to complete the static cluster assumption.
    if (currentTime >= electionEndTime) {
        if (ourNodeId == 0) {
            INFO(moduleName, "Waited long enough - to candidate");
            currentTerm++;
            electionEndTime = getFixedElectionEndTime(maxElectionCandidateTime);
            sendElectionMessages();
            setMonitorState(CandidateMonitor);
        } else {
            // Node other than node 0 and we didn't get a candidate message from
            // the master witin the election time.
            // The monitorTest.sh test scans for this message
            ERROR(moduleName,
                  "Timed out waiting for candidate message from master");
            xcalarExit(1);
        }
    }
}

//
// processCandidate --
//
// We are in the CandidateMonitor state which means we are trying to elect
// ourselves as master.  We will keep trying to become master until
// 1) We get a quorum of slaves so we are the master.
// 2) We time out and die
//
static void
processCandidate()
{
    uint64_t curTime = CurrentTime();
    if (curTime >= electionEndTime) {
        //
        // Election took too long.  Nothing further we can do.
        // The monitorTest.sh test scans for this message
        //
        ERROR(moduleName, "Failed to complete election");
        xcalarExit(1);

    } else if (curTime >= candidateMsgInterval + lastElectionMessageTime) {
        //
        // Its time to send more election messages.
        //
        sendElectionMessages();
    }
}

//
// closeMasterCnx --
//
// Close the master connection.  We have to explicitly close it since the
// Connection module has internal references such that just deleting our
// reference won't be sufficient to close the connection.
static void
closeMasterCnx()
{
    if (masterCnx != NULL) {
        masterCnx->Close();
        masterCnx = NULL;
    }
}

//
// processConnectingToMaster --
//
// We are in the ConnectingToMaster monitor state.  In this state we wait at
// most masterConnectTimeout for the master to respond to our connection
// request. If we don't get a connection in time then we restart the
// election sequence.
//
static void
processConnectingToMaster()
{
    uint64_t currentTime = CurrentTime();
    INFO(moduleName,
         "processConnectingToMaster %ld - %ld > %ld",
         currentTime,
         masterConnectTime,
         masterConnectTimeout);
    if (currentTime > masterConnectTimeout + masterConnectTime) {
        WARNING(moduleName, "Timed out connecting to master");
        closeMasterCnx();
        setMonitorState(ElectionWaitMonitor);
        processElectionWait();
    }
}

//
// processSlave --
//
// We are in the SlaveMonitor state.  We remain in this state unless we haven't
// received a heartbeat from the master in slaveMasterTimeout microseconds.
// If we don't receive the heartbeat in time then we assume the master has died
// and so we should also die.
///
static void
processSlave()
{
    uint64_t currentTime = CurrentTime();
    // INFO(moduleName, "processSlave %ld - %ld > %ld",
    //      currentTime, lastMasterNetHeartbeatTime, slaveMasterTimeout);

    if (slaveMasterTimeoutEnabled) {
        // Cluster has been formed and thus heartbeats from the master are
        // expected.
        if (currentTime > slaveMasterTimeout + lastMasterNetHeartbeatTime) {
            WARNING(moduleName,
                    "Master didn't send heartbeat in %ld microseconds",
                    slaveMasterTimeout);
            closeMasterCnx();
            killUsrNodeAndDie(SIGUSR1);
            // Never gets here as the above exits
            NotReached();
        }
    }
}

//
// slaveDied --
//
// A slave died....the cluster must be brought down via cascading failures
// of the xcmonitors comprising the cluster.
static void
slaveDied(SlaveInfo &slaveInfo)
{
    WARNING(moduleName, "slaveDied: Slave %ld died", slaveInfo.nodeId);

    killUsrNodeAndDie(SIGUSR1);

    // Above exits; so control never reaches here
    NotReached();
}

//
// sendClusterChangePrepare --
//
// Send a cluster change prepare message to all of our slaves.  If we lose
// a slave then the cluster is brought down.
//
static void
sendClusterChangePrepare()
{
    clusterChangePreparePending = false;
    // XXX Needs error handling here
    ConfigInfo *ciArray = new (std::nothrow) ConfigInfo[configMap.size()];
    uint32_t ciIndex = 0;
    for (ConfigMap::iterator configIter = configMap.begin();
         configIter != configMap.end();
         ++configIter) {
        ConfigInfo &ci = ciArray[ciIndex++];
        ci = configIter->second;
    }

    SlaveMap::iterator slaveIter = slaveMap.begin();
    while (slaveIter != slaveMap.end()) {
        if (slaveIter->second.cnx->SendMsg(NEW_CLUSTER_STATUS_PREPARE_OP,
                                           ciArray,
                                           ciIndex * sizeof(ConfigInfo))) {
            slaveIter->second.configChangePending = true;
            clusterChangePendingCount++;
            ++slaveIter;
        } else {
            INFO(moduleName,
                 "sendClusterChangePrepare: Failed to send to %ld",
                 slaveIter->second.nodeId);
            slaveDied(slaveIter->second);
            NotReached();
        }
    }

    sendNotifierMsg(NEW_CLUSTER_STATUS_PREPARE_OP,
                    ciArray,
                    ciIndex * sizeof(ConfigInfo));

    if (clusterChangePendingCount == 0) {
        INFO(moduleName, "sendClusterChangePrepare: No slaves");
        assert(slaveMap.size() == 0);
        clusterChangePrepareInProgress = false;
        clusterChangeCommitPending = true;
    }

    delete[] ciArray;
    ciArray = NULL;
}

//
// sendClusterChangeCommit --
//
// Send a cluster change commit message to all of our slaves. If we lose a
// slave then the entire cluster is brought down.
//
static void
sendClusterChangeCommit()
{
    clusterChangeCommitPending = false;

    SlaveMap::iterator slaveIter = slaveMap.begin();
    while (slaveIter != slaveMap.end()) {
        if (!slaveIter->second.cnx->SendMsg(NEW_CLUSTER_STATUS_COMMIT_OP,
                                            NULL,
                                            0)) {
            INFO(moduleName,
                 "sendClusterChangeCommit: Failed to send to %ld",
                 slaveIter->second.nodeId);
            slaveDied(slaveIter->second);
            NotReached();
        } else {
            ++slaveIter;
        }
    }
    sendNotifierMsg(NEW_CLUSTER_STATUS_COMMIT_OP, NULL, 0);
}

//
// processMaster --
//
// We are in the MasterMonitor state. Check and/or send slave heartbeats if its
// time, send MasterMsg election messages if its time, and deal with config
// changes.
//
static void
processMaster()
{
    SlaveMap::iterator iter;
    uint64_t currentTime = CurrentTime();

    if (currentTime >=
        masterNetHeartbeatCheckInterval + lastMasterNetHeartbeatTime) {
        lastMasterNetHeartbeatTime = currentTime;
        if (!sendHeartBeats) {
            // note: even if heartbeats are off, above message is still
            // generated to emit that master's alive: for diagnosability
            goto skip_hb;
        }
        iter = slaveMap.begin();
        while (iter != slaveMap.end()) {
            SlaveInfo &si = iter->second;
            if (si.heartbeatPending &&
                currentTime >= masterSlaveTimeout + si.heartbeatSendTime) {
                //
                // A slave hasn't responded to a heartbeat in the timeout
                // interval...
                //
                INFO(moduleName, "Slave %ld died", si.nodeId);
                // Test code for heart-beats off
                // INFO(moduleName, "Slave %ld may be dead; send hb",
                // si.nodeId); si.heartbeatPending = false;
                //
                slaveDied(iter->second);
                NotReached();
            }

            if (!si.heartbeatPending) {
                //
                // The slave acked the hearbeat and its time to send a new one.
                //
                INFO(moduleName, "Sending heartbeat to slave");
                if (!si.cnx->SendMsg(HEARTBEAT_OP, NULL, 0)) {
                    INFO(moduleName,
                         "Failed to send message to Slave %ld",
                         si.nodeId);
                    slaveDied(iter->second);
                    NotReached();
                }
                si.heartbeatPending = true;
                si.heartbeatSendTime = currentTime;
            }
            ++iter;
        }
    }

skip_hb:

    if (currentTime >= masterMsgInterval + lastMasterMsgTime) {
        //
        // Time to send MasterMsg election messages to all slaves that aren't
        // attached.
        //
        sendElectionMessages();
        lastMasterMsgTime = currentTime;
    }

    if (clusterChangeCommitPending) {
        sendClusterChangeCommit();
    } else if (clusterChangePreparePending && !clusterChangePrepareInProgress) {
        INFO(moduleName, "Cluster change");
        sendClusterChangePrepare();
    }
}

static void
usage()
{
    ERROR(moduleName,
          "xcmonitor -n <nodeid> -c <configFile> [-p <port>] "
          "[-m <maxNodes>");
    xcalarExit(1);
}

static void *
monitorSignalHandlerThread(void *arg)
{
    sigset_t *set = (sigset_t *) arg;
    int sig;
    siginfo_t siginfo;
    char pathOfSendingPid[PATH_MAX];
    char procPath[32];
    Status status;

    INFO(moduleName, "monitorSignalHandlerThread starting");
    for (;;) {
        sig = sigwaitinfo(set, &siginfo);
        if (sig < 0) {
            status = sysErrnoToStatus(errno);
            ERROR(moduleName,
                  "sigwaitinfo failed %s",
                  strGetFromStatus(status));
            // call killUsrNodeAndDie() here?
            xcalarExit(1);
        }
        // If we can't get the path name we'll use an empty string. Also,
        // readlink doesn't null terminate.
        memZero(pathOfSendingPid, sizeof(pathOfSendingPid));
        snprintf(procPath, sizeof(procPath), "/proc/%d/exe", siginfo.si_pid);
        readlink(procPath, pathOfSendingPid, sizeof(pathOfSendingPid) - 1);

        ERROR(moduleName,
              "monitorSignalHandlerThread got signal %d from pid %d (%s)",
              sig,
              siginfo.si_pid,
              pathOfSendingPid);

        switch (sig) {
        case SIGTERM:
            killUsrNodeAndDie(sig);
            NotReached();
            break;
        case SIGCHLD:
            reapUsrNodeAndDie();
            NotReached();
            break;
        default:
            assert(0 && "unexpected signal");
            break;
        }
    }
    return NULL;
}

static inline void
updateDefaultValue(uint64_t &value, uint64_t oldDefault, uint64_t newDefault)
{
    value = (value == oldDefault) ? newDefault : value;
}

int
main(int argc, char **argv)
{
    time_t modTime = {0};
    int ret;
    Status status;
    sigset_t set;
    char *xcm_nohbenv;
    struct sigaction actForSigCld;

    status = Config::init();
    if (status != StatusOk) {
        ERROR(moduleName,
              "Could not initialize config module: %s",
              strGetFromStatus(status));
        xcalarExit(1);
    }

    logDir = logDirDefault;
    while (true) {
        int optionIndex;
        int opt;
        int val;

        opt = getopt_long(argc,
                          argv,
                          "p:c:n:l:m:g:k:",
                          cmdLineOptions,
                          &optionIndex);
        if (opt == -1) {
            break;
        }
        switch (opt) {
        case 'p':
            ourMonitorPort = (int) strtol(optarg, NULL, 0);
            break;
        case 'k':
            licenseFile = optarg;
            break;
        case 'c':
            clusterConfigPath = optarg;
            status = getConfigValues(clusterConfigPath.c_str());
            if (status != StatusOk) {
                ERROR(moduleName,
                      "Could not initialize configuration from "
                      "configFile '%s': %s",
                      clusterConfigPath.c_str(),
                      strGetFromStatus(status));
                xcalarExit(1);
            }

            xsyslogInit(SyslogFacilityMonitor, logDir.c_str());
            xsyslogFacilityBooted();
            break;
        case 'n':
            ourNodeId = strtol(optarg, NULL, 0);
            break;
        case 'm':
            // Override NumNodes in the config file (used for testing).
            val = strtol(optarg, NULL, 0);
            if (val < 1 || val > 256) {
                ERROR(moduleName, "Invalid maximum number of nodes");
                xcalarExit(1);
            }
            maxNodes = val;
            break;
        case 'l':
            logLevel = (int) strtol(optarg, NULL, 0);
            break;
        case 'g':
            // Run XCE with GuardRails memory debugger
            ret = setenv("LD_PRELOAD", optarg, true);
            if (ret != 0) {
                ERROR(moduleName, "Could not set LD_PRELOAD to %s", optarg);
                xcalarExit(1);
            }
            break;
        default:
            usage();
            xcalarExit(1);
        }
    }

    if ((xcm_nohbenv = getenv("XCMONITOR_SENDHEARTBEATS")) != NULL) {
        if (strcmp(xcm_nohbenv, "1") == 0) {
            INFO(moduleName, "xcmonitor will send heartbeats");
            sendHeartBeats = true;
        } else {
            INFO(moduleName, "xcmonitor will NOT send heartbeats");
        }
    } else {
        INFO(moduleName, "xcmonitor will NOT send heartbeats");
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    srandom(tv.tv_usec);
    if (ourNodeId == -1) {
        ERROR(moduleName, "Must specify the node id");
        usage();
        xcalarExit(1);
    }
    INFO(moduleName, "Our node id: %ld", ourNodeId);
    INFO(moduleName, "Config file: %s", clusterConfigPath.c_str());
    INFO(moduleName, "License file: %s", licenseFile.c_str());

    status =
        getNodeInfo(clusterConfigPath.c_str(), configMap, sendToSet, modTime);
    if (status != StatusOk) {
        ERROR(moduleName,
              "Failed to populate configMap from config file "
              "'%s': %s",
              clusterConfigPath.c_str(),
              strGetFromStatus(status));
        xcalarExit(1);
    }

    configFileModTime = modTime;

    // Here are the real default values, based on the number of nodes
    // in the cluster.
    //
    // NOTE: reapUsrNodeMaxWaitTime must be kept in sync with stopwaitsecs
    // for xcmonitor in supervisor.conf:
    //
    // stopwaitsecs = (reapUsrNodeMaxWaitTime + 60secs)
    //

    uint64_t maxElectionCandidateTimeDefault;
    uint64_t masterSlaveTimeoutDefault;
    uint64_t slaveMasterTimeoutDefault;
    uint64_t reapUsrnodeMaxWaitTimeDefault;

    assert(configMap.size() > 0);
    if (configMap.size() > 128) {
        maxElectionCandidateTimeDefault = 60 * 60 * oneTimeUnit;
        masterSlaveTimeoutDefault = 2 * 60 * 60 * oneTimeUnit;  // 2 hr
        slaveMasterTimeoutDefault = 60 * 60 * oneTimeUnit;      // 1 hr
        reapUsrnodeWaitTime = 20 * oneTimeUnit;
        // supervisor in sync?
        reapUsrnodeMaxWaitTimeDefault = 10 * 60 * oneTimeUnit;
    } else if (configMap.size() > 64) {
        maxElectionCandidateTimeDefault = 30 * 60 * oneTimeUnit;
        masterSlaveTimeoutDefault = 2 * 60 * 60 * oneTimeUnit;  // 2 hr
        slaveMasterTimeoutDefault = 60 * 60 * oneTimeUnit;      // 1 hr
        reapUsrnodeWaitTime = 10 * oneTimeUnit;
        // supervisor in sync?
        reapUsrnodeMaxWaitTimeDefault = 6 * 60 * oneTimeUnit;
    } else if (configMap.size() > 32) {
        maxElectionCandidateTimeDefault = 15 * 60 * oneTimeUnit;
        masterSlaveTimeoutDefault = 2 * 60 * 60 * oneTimeUnit;  // 2 hr
        slaveMasterTimeoutDefault = 60 * 60 * oneTimeUnit;      // 1 hr
        reapUsrnodeWaitTime = 5 * oneTimeUnit;
        // supervisor in sync?
        reapUsrnodeMaxWaitTimeDefault = 3 * 60 * oneTimeUnit;
    } else {
        maxElectionCandidateTimeDefault = 15 * 60 * oneTimeUnit;
        masterSlaveTimeoutDefault = 60 * 60 * oneTimeUnit;  // 1 hr
        slaveMasterTimeoutDefault = 30 * 60 * oneTimeUnit;  // 30 min
        reapUsrnodeWaitTime = 5 * oneTimeUnit;
        // supervisor in sync?
        reapUsrnodeMaxWaitTimeDefault = 2 * 60 * oneTimeUnit;
    }

    updateDefaultValue(maxElectionCandidateTime,
                       XcalarConfig::XcMonElectionTimeoutDefault * oneTimeUnit,
                       maxElectionCandidateTimeDefault);
    updateDefaultValue(masterSlaveTimeout,
                       XcalarConfig::XcMonMasterSlaveTimeoutDefault *
                           oneTimeUnit,
                       masterSlaveTimeoutDefault);
    updateDefaultValue(slaveMasterTimeout,
                       XcalarConfig::XcMonSlaveMasterTimeoutDefault *
                           oneTimeUnit,
                       slaveMasterTimeoutDefault);
    updateDefaultValue(reapUsrnodeMaxWaitTime,
                       XcalarConfig::XcMonUsrNodeReapTimeoutDefault *
                           oneTimeUnit,
                       reapUsrnodeMaxWaitTimeDefault);

    masterNetHeartbeatCheckInterval = slaveMasterTimeout / 5;

    INFO(moduleName, "One time unit is %ld microseconds", oneTimeUnit);
    INFO(moduleName,
         "Candidate election timeout: %ld",
         maxElectionCandidateTime);
    INFO(moduleName, "master/slave timeout: %ld", masterSlaveTimeout);
    INFO(moduleName, "slave/master timeout: %ld", slaveMasterTimeout);
    INFO(moduleName,
         "master HB check interval: %ld",
         masterNetHeartbeatCheckInterval);

    monitorDir = rootDir + "/monitor";
    INFO(moduleName, "Our monitor directory: %s", monitorDir.c_str());
    xcmonShutDownInProgress.val = 0;

    if (mkdir(monitorDir.c_str(), 0777) < 0 && errno != EEXIST) {
        status = sysErrnoToStatus(errno);
        ERROR(moduleName,
              "Failed to create monitor directory '%s': %s",
              monitorDir.c_str(),
              strGetFromStatus(status));
        xcalarExit(1);
    }

    if (configMap.find(ourNodeId) == configMap.end()) {
        ERROR(moduleName,
              "Failed to find our node %ld in the config file",
              ourNodeId);
        xcalarExit(1);
    }

    if (ourMonitorPort >= 0 && ourMonitorPort != configMap[ourNodeId].port) {
        INFO(moduleName,
             "Input monitor port overrides config file port %d",
             configMap[ourNodeId].port);
    } else {
        ourMonitorPort = configMap[ourNodeId].port;
    }

    const char *hostName = configMap[ourNodeId].hostName;

    sigemptyset(&set);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGCHLD);
    ret = pthread_sigmask(SIG_BLOCK, &set, NULL);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        ERROR(moduleName,
              "Failed to mask signals: %s\n",
              strGetFromStatus(status));
        xcalarExit(1);
    }
    //
    // The xcmonitor needs to take action when its child usrnode dies - a
    // reliable way of detecting the child's death is by handling the SIGCHLD
    // signal that's delivered to a parent when its child dies.
    //
    // However, a SIGCHLD is delivered to the parent even if the child is
    // stopped or continued (e.g. due to gdb being attached to the child, or
    // the child receiving a stop/cont signal like SIGTSTP/SIGSTOP/SIGCONT) -
    // not just when it dies.
    //
    // Unfortunately, sigwait(3) doesn't have support for sigaction(2) flags
    // like SA_NOCLDSTOP which can be used by the parent to prevent the
    // generation of this signal for stop/continue events. Instead of switching
    // to the use of sigaction(2) to install an old-style signal handler, it's
    // possible to use sigaction(2) merely to set the SA_NOCLDSTOP flag, and
    // then continue using sigwait(3) to handle the signal itself on delivery.

    actForSigCld.sa_handler = NULL;
    sigemptyset(&actForSigCld.sa_mask);
    actForSigCld.sa_flags = SA_NOCLDSTOP;  // no SIGCHLD for stop/continue

    sigaction(SIGCHLD, &actForSigCld, NULL);

    // @SymbolCheckIgnore
    ret = pthread_create(&monitorSignalHandlerThreadId,
                         NULL,
                         monitorSignalHandlerThread,
                         (void *) &set);
    if (ret != 0) {
        status = sysErrnoToStatus(errno);
        ERROR(moduleName,
              "Failed to create signal handler thread: %s\n",
              strGetFromStatus(status));
        xcalarExit(1);
    }

    // Use hostname in the config file to bind to for both UDP and TCP.
    // This is beneficial when the config file has private hostnames(for eg:
    // cloud environments) and xcmonitor will not be available over public
    // network.
    Connection::CreateUDPConnection(hostName,
                                    ourMonitorPort,
                                    &udpNotifier,
                                    NULL,
                                    udpConnection);
    Connection::Listen(hostName, ourMonitorPort, &tcpNotifier, &tcpMsgHandler);

    INFO(moduleName, "Number of nodes in cluster: %lu", configMap.size());
    if (configMap.size() == 1) {
        INFO(moduleName, "One node cluster. Just make myself the master");
        setMonitorState(MasterMonitor);
        startUsrNode(ourNodeId,
                     configMap.size(),
                     clusterConfigPath.c_str(),
                     licenseFile.c_str(),
                     logDir.c_str());
    } else {
        setMonitorState(ElectionWaitMonitor);
    }

    while (true) {
        switch (monitorState) {
        case ElectionWaitMonitor:
            processElectionWait();
            break;
        case CandidateMonitor:
            processCandidate();
            break;
        case ConnectingToMaster:
            processConnectingToMaster();
            break;
        case SlaveMonitor:
            processSlave();
            break;
        case MasterMonitor:
            processMaster();
            break;
        }

        Connection::Poll(msgPollInterval);
    }
    MDEBUG(moduleName, "Main exiting!!");
    xcalarExit(1);
}

//
// UdpNotifierImpl::Notify
//
// A new UDP message has come in on our UDP connection.
// These are only for election messages.
//
bool
UDPNotifierImpl::Notify(Connection *cnx, NotifierType type, int status)
{
    while (true) {
        SocketAddr sockAddr;
        ElectionMsg msg;

        sockAddr.addrLen = sizeof(sockAddr.ipv6);
        int len = recvfrom(cnx->GetSocket(),
                           &msg,
                           sizeof(msg),
                           0,
                           &sockAddr.ip,
                           &sockAddr.addrLen);
        INFO(moduleName,
             "recvFrom socket %d, length %d",
             cnx->GetSocket(),
             len);
        if (len < 0) {
            if (errno == EWOULDBLOCK) {
                return true;
            } else {
                ERROR(moduleName,
                      "recvFrom failed for socket %d: %s",
                      cnx->GetSocket(),
                      strGetFromStatus(sysErrnoToStatus(errno)));
                xcalarExit(1);
            }
        } else if (len != sizeof(msg)) {
            // Ignore the message
            WARNING(moduleName,
                    "Ignoring message with Invalid election message "
                    "size: %d != %ld",
                    len,
                    sizeof(msg));
            continue;
        }

        INFO(moduleName,
             "currentTerm %ld monitorState %s msg: <type=%d "
             "term=%ld configVersion=%ld nodeId=%ld>",
             currentTerm,
             monitorStateToString(monitorState),
             msg.type,
             msg.term,
             msg.configVersion,
             msg.nodeId);

        ConfigMap::iterator configIter = configMap.find(msg.nodeId);
        if (configIter == configMap.end()) {
            WARNING(moduleName,
                    "UDPNotifierImpl::Notify: Unknown node id %ld",
                    msg.nodeId);
            continue;
        }

        assert(monitorState != CandidateMonitor);

        if (monitorState == ElectionWaitMonitor) {
            if (msg.term >= currentTerm) {
                currentTerm = msg.term;
                WARNING(moduleName,
                        "Got message from host with higher term or waiting and "
                        "ok term");
                masterCnx = Connection::Connect(configIter->second.sockAddr,
                                                &tcpNotifier,
                                                &tcpMsgHandler);
                assert(masterCnx != NULL);
                setMonitorState(ConnectingToMaster);
                masterConnectTime = CurrentTime();
            } else {
                INFO(moduleName,
                     "Ignoring election message in %s state",
                     monitorStateToString(monitorState));
            }
        }
    }
}

//
// TCPNotifierImpl::Notify --
//
// A message came in on one of our connections or a new connection was accepted.
//
bool
TCPNotifierImpl::Notify(Connection *cnx, NotifierType type, int status)
{
    ConfigMap::iterator configIter;

    INFO(moduleName,
         "TCPNotifierImpl::Notify: cnx %d type %d status %d monitorState %s",
         cnx->GetId(),
         type,
         status,
         monitorStateToString(monitorState));
    if (type == Connection::Notifier::AcceptedSocket) {
        INFO(moduleName, "Connection Accepted");
        return true;
    } else if (notifierCnxMap.find(cnx->GetId()) != notifierCnxMap.end()) {
        assert(type == Connection::Notifier::SocketError);
        notifierCnxMap.erase(cnx->GetId());
        // XXX: A listener connection is broken. We are taking a shortcut to
        // assume that means the Usrnode has died. In order to notify and
        // shutdown the rest of the usrnode cluster, we make the monitor
        // instance commit suicide so that other monitor instances will
        // detect the cluster change and send a cluster change signal to
        // their attached listeners.
        ERROR(moduleName, "UsrNode connection lost, shut down cluster");
        killUsrNodeAndDie(SIGUSR1);
        // control never reaches here!
        NotReached();
    }

    switch (monitorState) {
    case ElectionWaitMonitor:
        WARNING(moduleName, "Notification while in ElectionWaitMonitor state");
        // XXX: do we want to allow this to continue?
        return false;
    case ConnectingToMaster:
        INFO(moduleName, "ConnectingToMaster");
        assert(cnx == masterCnx);
        if (type == Connection::Notifier::ConnectionComplete) {
            INFO(moduleName, "TCPNotifierImpl::Notify: ConnectionComplete");
            NewSlaveMsg msg;
            msg.nodeId = ourNodeId;
            msg.configVersion = configFileModTime;

            cnx->SendMsg(NEW_SLAVE_OP, &msg, sizeof(NewSlaveMsg));
            setMonitorState(SlaveMonitor);
            // We're now a slave but won't expect to get heartbeats from the
            // master until the entire cluster is formed.
            return true;
        } else {
            INFO(moduleName, "TCPNotifierImpl::Notify: SocketError");
            assert(type == Connection::Notifier::SocketError);
            masterCnx = NULL;
            setMonitorState(ElectionWaitMonitor);
            sleep(1);
            return false;
        }
    case SlaveMonitor:
        INFO(moduleName, "TCPNotifierImpl::Notify: SlaveMonitor");
        assert(type == Connection::Notifier::SocketError);
        masterCnx = NULL;

        if (receivedHandleNewConfig) {
            // Connection to master had been established enough to get
            // new config message.  If the connection goes down after that
            // then don't try to reestablish the connection.
            WARNING(moduleName, "Lost connection to master");
            WARNING(moduleName, "LOST MASTER");
            killUsrNodeAndDie(SIGUSR1);
            NotReached();
        }

        // Lost the connection to the master (node 0).  Connection::HandleEvent
        // has closed the socket.  Reestablish the connection and try again.
        configIter = configMap.find(0);
        assert(configIter != configMap.end());

        INFO(moduleName, "Reestablishing connection to the master");

        masterCnx = Connection::Connect(configIter->second.sockAddr,
                                        &tcpNotifier,
                                        &tcpMsgHandler);

        if (masterCnx == NULL) {
            // No master to connect to.
            WARNING(moduleName, "No master to connect to");
            killUsrNodeAndDie(SIGUSR1);
            NotReached();
        }

        setMonitorState(ConnectingToMaster);
        masterConnectTime = CurrentTime();
        return true;
    case MasterMonitor:
        INFO(moduleName, "TCPNotifierImpl::Notify: MasterMonitor");
        assert(type == Connection::Notifier::SocketError);
        for (SlaveMap::iterator iter = slaveMap.begin(); iter != slaveMap.end();
             ++iter) {
            if (iter->second.cnx == cnx) {
                slaveDied(iter->second);
                NotReached();
            }
        }
        return false;
    case CandidateMonitor:
        INFO(moduleName, "TCPNotifierImpl::Notify: CandidateMonitor");
        assert(type == Connection::Notifier::SocketError);
        for (SlaveMap::iterator iter = slaveMap.begin(); iter != slaveMap.end();
             ++iter) {
            if (iter->second.cnx == cnx) {
                slaveDied(iter->second);
                NotReached();
            }
        }
    }
    return false;
}

//
// handleHeartbeatReplyMsg --
//
// A hearbeat reply message came in from a slave.
//
void
handleHeartbeatReplyMsg(Connection *cnx)
{
    INFO(moduleName, "handleHeartbeatReplyMsg from %d", cnx->GetId());
    if (monitorState == MasterMonitor) {
        SlaveMap::iterator iter = slaveMap.find(cnx->GetId());
        if (iter == slaveMap.end()) {
            WARNING(moduleName,
                    "Failed to find connection %d in map",
                    cnx->GetId());
            cnx->Close();
            return;
        }
        iter->second.heartbeatPending = false;
    } else {
        WARNING(moduleName,
                "Unexpected heartbeat message in %d state",
                monitorState);
        cnx->Close();
    }
}

//
// handleNewSlaveMsg --
//
// A new slave has connected and sent us a NewSlaveMsg. If we are either a
// master or a candidate then add the slaves to our list of slaves.
//
void
handleNewSlaveMsg(Connection *cnx, void *msgBody, uint32_t msgLength)
{
    if (msgLength != sizeof(NewSlaveMsg)) {
        WARNING(moduleName,
                "Invalid message length %d for new slave msg",
                msgLength);
        cnx->Close();
        return;
    }
    NewSlaveMsg *msg = (NewSlaveMsg *) msgBody;
    if (monitorState != MasterMonitor && monitorState != CandidateMonitor) {
        WARNING(moduleName,
                "Unexpected new slave message in %s state",
                monitorStateToString(monitorState));
        cnx->Close();
        return;
    }

    INFO(moduleName, "handleNewSlaveMsg from node %ld", msg->nodeId);
    ConfigMap::iterator configIter = configMap.find(msg->nodeId);
    if (configIter == configMap.end()) {
        WARNING(moduleName, "Unknown node id %ld", msg->nodeId);
        cnx->Close();
        return;
    }

    SlaveMap::iterator iter = slaveMap.find(cnx->GetId());
    if (iter != slaveMap.end()) {
        WARNING(moduleName,
                "Already have slave at connection %d in map",
                cnx->GetId());
        slaveMap.erase(iter);
    }
    INFO(moduleName, "handleNewSlaveMsg: Adding new slave");
    SlaveInfo &si = slaveMap[cnx->GetId()];
    si.nodeId = msg->nodeId;
    si.heartbeatPending = false;
    si.cnx = cnx;
    sendToSet.erase(si.nodeId);
    INFO(moduleName, "Got new slave: node %ld", si.nodeId);
    configIter->second.status = CONFIG_HOST_UP;
    if ((monitorState == CandidateMonitor) && iHaveQuorum()) {
        INFO(moduleName, "Got quorum");
        setMonitorState(MasterMonitor);
        startUsrNode(ourNodeId,
                     configMap.size(),
                     clusterConfigPath.c_str(),
                     licenseFile.c_str(),
                     logDir.c_str());
    }

    clusterChangePreparePending = (monitorState == MasterMonitor);
}

//
// handleNewConfigPrepareMsg --
//
// The master has sent us a config prepare message. If we are a slave, save the
// new config and ack the message.
//
void
handleNewConfigPrepareMsg(Connection *cnx, void *msgBody, uint32_t msgLength)
{
    if (monitorState != SlaveMonitor) {
        WARNING(moduleName, "handleNewConfigPrepareMsg when not slave");
        return;
    }
    assert(msgLength != 0);
    assert(msgLength % sizeof(ConfigInfo) == 0);
    uint32_t count = msgLength / (unsigned) sizeof(ConfigInfo);
    INFO(moduleName,
         "handleNewConfigPrepareMsg: Got new config with %d entries",
         count);
    ConfigInfo *ciArray = (ConfigInfo *) msgBody;
    newConfigMap.clear();
    for (uint32_t i = 0; i < count; i++) {
        ConfigInfo &ci = newConfigMap[ciArray[i].nodeId];
        ci = ciArray[i];
    }
    assert(newConfigMap.find(ourNodeId) != newConfigMap.end());
    receivedHandleNewConfig = true;
    cnx->SendMsg(NEW_CLUSTER_STATUS_PREPARE_ACK_OP, NULL, 0);

    sendNotifierMsg(NEW_CLUSTER_STATUS_PREPARE_OP, msgBody, msgLength);
}

//
// handleNewConfigPrepareAckMsg --
//
// A slave has acked a config prepare message.
//
void
handleNewConfigPrepareAckMsg(Connection *cnx, void *msgBody, uint32_t msgLength)
{
    SlaveInfo &si = slaveMap[cnx->GetId()];
    if (si.configChangePending) {
        assert(clusterChangePendingCount > 0);
        clusterChangePendingCount--;
        INFO(moduleName,
             "handleNewConfigPrepareAckMsg: clusterChangePendingCount = %d",
             clusterChangePendingCount);
        if (clusterChangePendingCount == 0) {
            clusterChangePrepareInProgress = false;
            clusterChangeCommitPending = true;
        }
    } else {
        INFO(moduleName,
             "handleNewConfigPrepareAckMsg: No cluster change pending for "
             "slave %d",
             cnx->GetId());
    }
}

//
// handleNewConfigCommitMsg --
//
// The master has sent us a config commit message. Update our new config with
// the config that we saved when we got the prepare message.
//
void
handleNewConfigCommitMsg(Connection *cnx, void *msgBody, uint32_t msgLength)
{
    if (monitorState != SlaveMonitor) {
        WARNING(moduleName, "handleNewConfigCommitMsg when not slave");
        return;
    }
    INFO(moduleName,
         "handleNewConfigCommitMsg: Committing new config with %ld entries",
         newConfigMap.size());
    SendToSet newSendToSet;
    makeSendToSet(newConfigMap, newSendToSet);
    updateConfig(newConfigMap, newSendToSet);
    sendNotifierMsg(NEW_CLUSTER_STATUS_COMMIT_OP, NULL, 0);
}

//
// handleNotifierAttachMsg --
//
// A monitor listener has attached.  Save its connection and update it with our
// current state.
//
void
handleNotifierAttachMsg(Connection *cnx, void *msgBody, uint32_t msgLength)
{
    notifierCnxMap[cnx->GetId()] = cnx;
    if (monitorState == SlaveMonitor || monitorState == MasterMonitor) {
        sendEnabledMsg(cnx);
    } else {
        cnx->SendMsg(MONITOR_DISABLED_OP, NULL, 0);
    }
}

//
// TCPMsgHandlerImpl::Msg --
//
// We have received a message on one of our connections. Dispatch the message.
//
void
TCPMsgHandlerImpl::Msg(Connection *cnx,
                       MsgOp op,
                       void *msgBody,
                       uint32_t msgLength)
{
    switch (op) {
    case HEARTBEAT_OP:
        // Heartbeat received from the master
        lastMasterNetHeartbeatTime = CurrentTime();
        cnx->SendMsg(HEARTBEAT_REPLY_OP, NULL, 0);
        break;
    case HEARTBEAT_REPLY_OP:
        // Response to heartbeat received from a slave
        handleHeartbeatReplyMsg(cnx);
        break;
    case NEW_SLAVE_OP:
        // Sent by a new slave to join the club
        handleNewSlaveMsg(cnx, msgBody, msgLength);
        break;
    case NEW_CLUSTER_STATUS_PREPARE_OP:
        // Sent by master in preparation for a cluster change
        handleNewConfigPrepareMsg(cnx, msgBody, msgLength);
        break;
    case NEW_CLUSTER_STATUS_PREPARE_ACK_OP:
        // Sent by slave in response to prepare
        handleNewConfigPrepareAckMsg(cnx, msgBody, msgLength);
        break;
    case NEW_CLUSTER_STATUS_COMMIT_OP:
        handleNewConfigCommitMsg(cnx, msgBody, msgLength);
        break;
    case NOTIFIER_ATTACH_OP:
        // Sent by usrnode when it establishes a connection with us.
        handleNotifierAttachMsg(cnx, msgBody, msgLength);
        break;
    default:
        WARNING(moduleName, "MsgHandlerImpl::Msg: Op ignored: %d ", op);
        break;
    }
}
