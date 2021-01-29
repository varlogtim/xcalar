// Copyright 2013 - 2017 Xcalar, Inc. All rights reserved.
//
// No use, or distribution, of this source code is permitted in any form or
// means without a valid, written license agreement with Xcalar, Inc.
// Please refer to the included "COPYING" file for terms and conditions
// regarding the use and redistribution of this software.
//

#include <assert.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <sys/types.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <cstdlib>
#include <stdio.h>
#include <new>

#include "StrlFunc.h"
#include "primitives/Primitives.h"
#include "config/Config.h"
#include "msg/MessageTypes.h"
#include "util/System.h"
#include "hash/Hash.h"
#include "util/MemTrack.h"
#include "sys/XLog.h"
#include "constants/XcalarConfig.h"

static constexpr const char *moduleName = "libconfig";

Config *Config::instance = NULL;

Config *
Config::get()
{
    return instance;
}

Status
Config::init()
{
    Status status = StatusOk;
    char *containerName = getenv(ContainerName);
    assert(instance == NULL);
    instance = (Config *) memAllocExt(sizeof(Config), moduleName);
    if (instance == NULL) {
        return StatusNoMem;
    }
    instance = new (instance) Config();

    instance->modules_ =
        (Modules **) memAllocExt(sizeof(Modules *) * MaxModule, moduleName);
    if (instance->modules_ == NULL) {
        status = StatusNoMem;
        goto CommonExit;
    }
    memZero(instance->modules_, sizeof(Modules *) * MaxModule);

    for (unsigned ii = NodeModule; ii < MaxModule; ii++) {
        switch (ii) {
        case NodeModule: {
            instance->modules_[ii] =
                (ConfigModuleNodes *) memAllocExt(sizeof(ConfigModuleNodes),
                                                  moduleName);
            if (instance->modules_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->modules_[ii]) ConfigModuleNodes("Node");
        } break;

        case ConstantsModule: {
            instance->modules_[ii] = (ConfigModuleConstants *)
                memAllocExt(sizeof(ConfigModuleConstants), moduleName);
            if (instance->modules_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->modules_[ii]) ConfigModuleConstants("Constants");
        } break;

        case ThriftModule: {
            instance->modules_[ii] =
                (ConfigModuleThrift *) memAllocExt(sizeof(ConfigModuleThrift),
                                                   moduleName);
            if (instance->modules_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->modules_[ii]) ConfigModuleThrift("Thrift");
        } break;

        case FuncTestsModule: {
            instance->modules_[ii] = (ConfigModuleFuncTests *)
                memAllocExt(sizeof(ConfigModuleFuncTests), moduleName);
            if (instance->modules_[ii] == NULL) {
                status = StatusNoMem;
                goto CommonExit;
            }
            new (instance->modules_[ii]) ConfigModuleFuncTests("FuncTests");
        } break;

        default:
            status = StatusInval;
            goto CommonExit;
            break;
        }
    }

    if (containerName != NULL) {
        instance->container_ = true;
    }

CommonExit:
    if (status != StatusOk) {
        instance->destroy();
    }
    return status;
}

void
Config::destroy()
{
    if (modules_) {
        for (unsigned ii = NodeModule; ii < MaxModule; ii++) {
            if (modules_[ii]) {
                modules_[ii]->~Modules();
                memFree(modules_[ii]);
                modules_[ii] = NULL;
            }
        }
        memFree(modules_);
        modules_ = NULL;
    }

    instance->~Config();
    memFree(instance);
    instance = NULL;
}

// Marks each node whose hostname matches the given hostname as true in
// nodesMatch array.
inline bool
Config::markMatchingNodes(ConfigNode *nodes,
                          size_t nodeCount,
                          const char *hostname)
{
    unsigned matches = 0;
    char shortHostname[MaxHostName];
    char shortNodename[MaxHostName];

    for (size_t i = 0; i < nodeCount; i++) {
        if (nodes[i].local) {
            matches++;
            continue;
        }

        snprintf(shortHostname, MaxHostName, "%s", hostname);
        shortHostname[strcspn(hostname, ".")] = '\0';
        snprintf(shortNodename, MaxHostName, "%s", nodes[i].ipAddr);
        shortNodename[strcspn(nodes[i].ipAddr, ".")] = '\0';

        // we want to avoid the case where host is a.b.c.d,
        // node is a.x.y.z, and a == a is accepted, so we only
        // accept a == a if either (or both) of the specified
        // names are actually short
        if ((strcmp(nodes[i].ipAddr, hostname) == 0) ||
            (strcmp(shortNodename, hostname) == 0) ||
            (strcmp(nodes[i].ipAddr, shortHostname) == 0)) {
            nodes[i].local = true;
            matches++;
        }
    }

    return matches == nodeCount;
}

bool
Config::isLoopbackAddress(struct sockaddr *inAddr)
{
    if (inAddr->sa_family == AF_INET) {
        struct sockaddr_in *ipv4Addr = (struct sockaddr_in *) inAddr;
        return ipv4Addr->sin_addr.s_addr == htonl(INADDR_LOOPBACK);
    } else if (inAddr->sa_family == AF_INET6) {
        struct sockaddr_in6 *ipv6Addr = (struct sockaddr_in6 *) inAddr;
        return memcmp(&ipv6Addr->sin6_addr,
                      &in6addr_loopback,
                      xcMin(sizeof(ipv6Addr->sin6_addr),
                            sizeof(in6addr_loopback))) == 0;
    }
    return false;
}

// Attempts to determine which usrnodes are local to this physical machine using
// different means of addressing this machine. The "local" property of each
// config node is set to true if it's found to be on this machine.
// Returns count of usrnodes on this machine.
// We use a bunch of heuristics to try to identify the required local nodes
// more quickly before resorting to the slower method of asking the name server
//
unsigned
Config::configIdentifyLocalNodes(ConfigNode *nodes, unsigned nodeCount)
{
    char me[MaxHostName];
    unsigned i;

    // Heuristic 1. Just ask for my hostname
    // Compare to result of gethostname. May only return local portion of
    // hostname.
    if (gethostname(me, sizeof(me)) == 0) {
        if (markMatchingNodes(nodes, nodeCount, me)) {
            return nodeCount;
        }
    }

    struct ifaddrs *ipAddrs;
    struct ifaddrs *ipAddr;

    // Fetch IPs of all possible interfaces on this system.
    getifaddrs(&ipAddrs);

    for (unsigned jj = 0; jj < 2; jj++) {
        for (ipAddr = ipAddrs; ipAddr != NULL; ipAddr = ipAddr->ifa_next) {
            if (ipAddr->ifa_addr == NULL) {
                continue;
            }

            // Herustic 2. Check loopback addresses first. E.g. 127.0.0.1 or ::1
            if (jj == 0 && !isLoopbackAddress(ipAddr->ifa_addr)) {
                continue;
            }

            // Get string representation of IP address with NI_NUMERICHOST.
            me[0] = '\0';
            if (getnameinfo(ipAddr->ifa_addr,
                            sizeof(*ipAddr->ifa_addr),
                            me,
                            sizeof(me),
                            NULL,
                            0,
                            NI_NUMERICHOST) == 0) {
                if (markMatchingNodes(nodes, nodeCount, me)) {
                    goto CommonExit;
                }
            }

            // The following might be slow because we are doing nslookups. The
            // DNS query might take several mins to time out. If we reach here,
            // it might seem like xcmgmtd has hung

            // Ask for the hostname associated with this IP.
            me[0] = '\0';
            if (getnameinfo(ipAddr->ifa_addr,
                            sizeof(*ipAddr->ifa_addr),
                            me,
                            sizeof(me),
                            NULL,
                            0,
                            0) == 0) {
                if (markMatchingNodes(nodes, nodeCount, me)) {
                    goto CommonExit;
                }
            }

            // Get a more thorough list of hostnames associated with this IP.
            struct hostent host;
            struct hostent *result;
            char hostentBuf[2 * KB];
            int hostentErrno;
            if (gethostbyaddr_r(ipAddr->ifa_addr,
                                sizeof(*ipAddr->ifa_addr),
                                AF_INET,
                                &host,
                                hostentBuf,
                                sizeof(hostentBuf),
                                &result,
                                &hostentErrno) == 0 &&
                result != NULL) {
                if (markMatchingNodes(nodes, nodeCount, result->h_name)) {
                    goto CommonExit;
                }

                for (i = 0; result->h_aliases[i] != NULL; i++) {
                    if (markMatchingNodes(nodes,
                                          nodeCount,
                                          result->h_aliases[i])) {
                        goto CommonExit;
                    }
                }
            }
        }
    }

CommonExit:
    freeifaddrs(ipAddrs);

    unsigned matchesCount = 0;
    for (i = 0; i < nodeCount; i++) {
        if (nodes[i].local) {
            matchesCount++;
        }
    }

    return matchesCount;
}

// Loads and validates config file info concerning this and other nodes.
Status
Config::loadNodeInfo()
{
    assert(myNodeId_ != DefaultNodeInit);
    assert(activeNodes_ != DefaultNodeInit);

    if (myNodeId_ >= activeNodes_) {
        return StatusInvalNodeId;
    }

    setNumActiveNodesOnMyPhysicalNode(
        configIdentifyLocalNodes(config_.nodes, activeNodes_));

    // Determine my ordering among the nodes local to this machine.
    myIndexOnThisMachine_ = 0;
    for (unsigned i = 0; i < activeNodes_; i++) {
        if (i == myNodeId_) {
            if (!config_.nodes[i].local) {
                assert(0);
                return StatusNoLocalNodes;
            }
            break;
        } else if (config_.nodes[i].local) {
            // A local node is encountered before i == myNodeId_. It's index on
            // this machine comes before myIndexOnThisMachine_.
            myIndexOnThisMachine_++;
        }
    }
    return StatusOk;
}

NodeId
Config::getMyNodeId()
{
    assert(myNodeId_ != DefaultNodeInit);
    return myNodeId_;
};

unsigned
Config::getActiveNodes()
{
    assert(activeNodes_ != DefaultNodeInit);
    return activeNodes_;
};

unsigned
Config::getTotalNodes()
{
    assert(totalNodes_ != DefaultNodeInit);
    return totalNodes_;
};

// DLM cmds can only be processed on DLM master nodes, the namespace
// for which is managed by getMyDlmNode() hash function.
// Distribute msgTypeIds evenly across the cluster.
NodeId
Config::getMyDlmNode(MsgTypeId msgTypeId)
{
    assert(activeNodes_ != DefaultNodeInit);
    return (uint32_t) msgTypeId % activeNodes_;
};

int
Config::getActiveNodesOnMyPhysicalNode()
{
    assert(activeNodesOnMyPhysicalNode_ != DefaultNodeInit);
    return activeNodesOnMyPhysicalNode_;
};

unsigned
Config::getMyIndexOnThisMachine()
{
    assert(myIndexOnThisMachine_ != DefaultNodeInit);
    return myIndexOnThisMachine_;
}

// Returns the first node id that is present on this host.
// This is used so we can find a local node id on the mgmtdaemon
unsigned
Config::getLowestNodeIdOnThisHost()
{
    unsigned numNodes =
        configIdentifyLocalNodes(config_.nodes, config_.numNodes);
    assert(numNodes != 0);

    for (unsigned i = 0; i < config_.numNodes; i++) {
        if (config_.nodes[i].local) {
            return i;
        }
    }

    // XXX Report error and exit cleanly.

    // This assertion will fire if we try running a management daemon on a
    // host that does not have any active nodes, or our mapping logic to
    // determine which node ids map to our host failed to work.
    assert(0);

    // In the worst case, default to node 0 in production builds as this
    // is better than crashing
    return 0;
}

char *
Config::getIpAddr(unsigned index)
{
    assert(index < config_.numNodes);
    assert(config_.nodes[index].ipAddr);
    return config_.nodes[index].ipAddr;
}

int
Config::getPort(unsigned index)
{
    assert(index < config_.numNodes);
    return config_.nodes[index].port;
}

int
Config::getApiPort(unsigned index)
{
    assert(index < config_.numNodes);
    return config_.nodes[index].apiPort;
}

int
Config::getMonitorPort(unsigned index)
{
    assert(index < config_.numNodes);
    if (index < config_.numNodes) {
        return config_.nodes[index].monitorPort;
    }
    return 0;
}

int
Config::getThriftPort()
{
    return config_.thriftPort;
}

const char *
Config::getThriftHost()
{
    return config_.thriftHost;
}

void
Config::setMyNodeId(NodeId myNodeIdIn)
{
    if (myNodeIdIn != ConfigUnspecifiedNodeId) {
        myNodeId_ = myNodeIdIn;
    }
}

Status
Config::setNumActiveNodes(unsigned numActiveNodes)
{
    if (numActiveNodes == ConfigUnspecifiedNumActive) {
        return StatusOk;
    }

    // XXX FIXME cannot just arbitrarily change this at runtime without
    // significant design thought (existing fatptrs sitting in index trees,
    // outstanding messages, etc.)
    if (totalNodes_ != DefaultNodeInit && totalNodes_ < numActiveNodes) {
        return StatusConfigInvalid;
    }

    activeNodes_ = numActiveNodes;
    return StatusOk;
}

Status
Config::setNumTotalNodes(unsigned numTotalNodes)
{
    // XXX FIXME cannot just arbitrarily change this at runtime without
    // significant design thought (existing fatptrs sitting in index trees,
    // outstanding messages, etc.)
    if (activeNodes_ != DefaultNodeInit && numTotalNodes < activeNodes_) {
        return StatusConfigInvalid;
    }

    totalNodes_ = numTotalNodes;
    return StatusOk;
}

void
Config::setNumActiveNodesOnMyPhysicalNode(unsigned numActiveNodes)
{
    if (numActiveNodes != ConfigUnspecifiedNumActive) {
        activeNodesOnMyPhysicalNode_ = numActiveNodes;
    }
}

// Load the name of the binary described at link into binName.
Status
Config::configPopulateBinaryName(const char *link,
                                 char *binPath,
                                 size_t binPathSize,
                                 char *binName,
                                 size_t binNameSize)
{
    char tmpPath[PATH_MAX];
    size_t ret;

    assert(binName != NULL);

    // Get name of binary.
    memZero(tmpPath, sizeof(tmpPath));  // readlink doesn't null terminate.
    if (readlink(link, tmpPath, sizeof(tmpPath) - 1) == -1) {
        xSyslog(moduleName, XlogCrit, "Failed to determine binary name");
        return sysErrnoToStatus(errno);
    }

    if (binPath != NULL) {
        ret = strlcpy(binPath, tmpPath, binPathSize);
        if (ret >= binPathSize) {
            xSyslog(moduleName,
                    XlogCrit,
                    "Path is too long: %s (%lu chars) Max is %lu chars",
                    tmpPath,
                    strlen(tmpPath),
                    binPathSize - 1);
            return StatusNoBufs;
        }
    }

    // basename modifies tmpPath, hence the need for a tmpPath
    char *base = basename(tmpPath);
    if (base == NULL) {
        xSyslog(moduleName,
                XlogCrit,
                "Detected invalid binary path '%s'",
                tmpPath);
        return StatusFailed;
    }
    size_t baseLen = strlen(base);

    if (binNameSize < baseLen + 1) {
        xSyslog(moduleName,
                XlogCrit,
                "binaryName is too long: %s (%lu chars) Max is %lu chars",
                base,
                baseLen,
                binNameSize - 1);
        return StatusNoBufs;
    }
    strlcpy(binName, base, baseLen + 1);

    return StatusOk;
}

// Loads info about the currently executing binary. Passed argv[0] that was
// originally passed to main.
Status
Config::loadBinaryInfo()
{
    return configPopulateBinaryName("/proc/self/exe",
                                    argv0_,
                                    sizeof(argv0_),
                                    binary_,
                                    sizeof(binary_));
}

const char *
Config::getBinaryName()
{
    assert(binary_[0] != '\0');
    return binary_;
}

Status
Config::populateParentBinary()
{
    pid_t ppid = getppid();
    char parentBinPathLink[PATH_MAX];

    if ((size_t) snprintf(parentBinPathLink,
                          sizeof(parentBinPathLink),
                          "/proc/%u/exe",
                          (unsigned) ppid) >= sizeof(parentBinPathLink)) {
        xSyslog(moduleName, XlogCrit, "Failed to load parent binary path");
        return StatusOverflow;
    }

    return configPopulateBinaryName(parentBinPathLink,
                                    NULL,
                                    0,
                                    parentBin_,
                                    sizeof(parentBin_));
}

const char *
Config::getParentBinName()
{
    return parentBin_;
}
