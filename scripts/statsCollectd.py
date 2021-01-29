# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# standard library imports
import os
import time
import configparser
import inspect
import sys
import re
import errno
import subprocess
from socket import error as socket_error

# related third-party imports
import collectd

METRIC_PLUGIN = 'statsCollectd'
METRIC_TYPE = 'gauge'
METRIC_INTERVAL = 1   # in secs
XCE_HOME = os.getenv('XCE_HOME','/var/opt/xcalar')
XLRDIR = os.getenv("XLRDIR", '/opt/xcalar')
collectd.info("XLRDIR = {}".format(os.getenv("XLRDIR")))
PLUGIN_CFG = os.path.join(XLRDIR, 'scripts', 'statsPlugin.cfg')
XLRCTL_CMD = os.path.join(XLRDIR, "bin", 'xcalarctl')
CONFIG_STATS = 'stats'
CONFIG_WHITELIST = 'whitelist'
SEPARATOR = ':::'
XC_CONFIG_FILE = 'xcConfigFile'
MGMTD_URL = 'mgmtdUrl'
NUM_RETRIES = 'numRetries'
RELOAD_INTERVAL = 'reloadInterval'
NODE_IDS = 'nodeIds'
DEFAULT_RELOAD_INTERVAL = 60   # in secs
DEFAULT_XC_CONFIG_FILE = os.getenv('XCE_CONFIG','/etc/xcalar/default.cfg')
DEFAULT_MGMTD_URL = 'http://localhost:9090/thrift/service/XcalarApiService/'
DEFAULT_NUM_RETRIES = 2

class StatsCollectd(object):

    def __init__ (self):
        self.nodeIds = None
        self.xcConfigFile = DEFAULT_XC_CONFIG_FILE
        self.mgmtdUrl = DEFAULT_MGMTD_URL
        self.numRetries = DEFAULT_NUM_RETRIES
        self.statsList = None
        self.config = None
        self.reloadTime = None
        self.reloadInterval = DEFAULT_RELOAD_INTERVAL
        self.collector = None
        self.cmdDir = None
        # stats whitelist subscription
        cmdDir = os.path.realpath(
            os.path.abspath(
                os.path.split(
                    inspect.getfile(
                        inspect.currentframe()))[0]))

        self.loadPluginCfg()

        self.statsInit()
        self.groups = self.collector.groupIdMap()
        groupStr = ""
        for group in self.groups:
            groupStr = groupStr + '\n' + group
        collectd.info("Group List({}): {}".format(len(self.groups), groupStr))
        # register the plugin callbacks
        collectd.register_read(self.statReader)

    def statReader(self, input_date=None):
        statsRead = 0
        statsFound = 0

        startTime = time.time()
        if ((startTime - self.reloadTime) > self.reloadInterval):
            self.reloadPluginCfg()


        try:
            clusterStats = self.collector.getAllStats()
            nodeId = -1
            for nodeStats in clusterStats:
                nodeId = nodeId + 1
                for nodeStat in nodeStats:
                    statsFound += 1
                    try:
                        statPrefix = self.collector.getGroupName(nodeStat)
                    except IndexError:
                        collectd.info("Fetching stat at invalid index {}. Stat index must be between {} and {}".format(nodeStat.statId, 0, len(self.groups)-1))
                        pass
                    statName = nodeStat.statName
                    statValue = nodeStat.statValue
                    statFull = "{} {}".format(statPrefix, statName)
                    if statFull in self.statsList:
                        metric = collectd.Values()
                        metric.plugin = METRIC_PLUGIN
                        metric.interval = METRIC_INTERVAL
                        metric.type = METRIC_TYPE
                        metric.values = [statValue]
                        if (nodeId == 0):  # backward compatible stats
                            metric.type_instance = "{}.{}".format(statPrefix, statName)
                            metric.dispatch()
                            collectd.info("{} {}".format(metric.type_instance, statValue))
                        metric.type_instance = "{}.{}.{}".format(statPrefix, statName, nodeId)
                        metric.dispatch()
                        collectd.info("{} {}".format(metric.type_instance, statValue))
                        statsRead += 1
            collectd.info("Read {} out of {} stats in {} secs".format(statsRead, statsFound, time.time()-startTime))
        except:
            e = sys.exc_info()[0]
            collectd.info("Error in plugin: {}".format(e))
            self.statsInit()
            raise

    def statsInit(self):
        if not self.nodeIds:
            collectd.info("No usrnodes to track, exiting")
            raise

        retryWait = 5

        tryNum = 0
        # if numRetries is 0, never timeout
        while self.collector is None and (self.numRetries == 0 or tryNum < self.numRetries):
            if tryNum != 0:
                time.sleep(retryWait)
            try:
                self.collector = StatsCollector(self.mgmtdUrl, self.nodeIds)
            except socket_error as serr:
                collectd.info("Try {}: Failed to reach '{}' exception {}".format(tryNum, self.mgmtdUrl, sys.exc_info()))
                if serr.errno != errno.ECONNREFUSED:
                    raise serr
                else:
                    tryNum = tryNum+1
                    pass
            except XcalarApiStatusException:
                collectd.info("Try {}: Failed to reach '{}' exception {}".format(tryNum, self.mgmtdUrl, sys.exc_info()))
                tryNum = tryNum+1
                pass
        if self.collector is None:
            collectd.info("Timed out after {} tries trying to reach usrnode".format(self.numRetries))
            raise

        collectd.info("Connected to Management Daemon and usrnodes")
        collectd.info("statsList = '{}'".format(self.statsList))
        collectd.info("nodeIds = '{}'".format(self.nodeIds))

    def reloadPluginCfg(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read(PLUGIN_CFG)
            whitelist = self.config.get(CONFIG_STATS, CONFIG_WHITELIST)
            self.statsList = whitelist.split(SEPARATOR)
            self.reloadInterval = int(self.config.get(CONFIG_STATS, RELOAD_INTERVAL))
            xcalarctlIdCmd="{} node-id".format(XLRCTL_CMD)
            collectd.info("xcalarctl {}".format(xcalarctlIdCmd))
            cfgNodeIds = subprocess.check_output(xcalarctlIdCmd.split())
            if not cfgNodeIds:
                collectd.info("No nodeIds specified")
                sys.exit(1)
            cfgNodeArr = cfgNodeIds.split("\n")
            self.nodeIds = []
            for cfgNodeId in cfgNodeArr:
                try:
                    self.nodeIds.append(int(cfgNodeId))
                except ValueError:
                    collectd.info("{} is not a nodeId, error is {}".format(cfgNodeId, sys.exc_info()))
        except:
            e = sys.exc_info()[0]
            collectd.info("Error reading config file {}: {}".format(PLUGIN_CFG, e))
            raise
        self.reloadTime = time.time()

    def loadPluginCfg(self):
        self.mgmtdUrl = DEFAULT_MGMTD_URL
        self.numRetries = DEFAULT_NUM_RETRIES

        try:
            self.reloadPluginCfg()
            self.xcConfigFile = self.config.get(CONFIG_STATS, XC_CONFIG_FILE)
            self.mgmtdUrl = self.config.get(CONFIG_STATS, MGMTD_URL)
            self.numRetries = int(self.config.get(CONFIG_STATS, NUM_RETRIES))
        except:
            e = sys.exc_info()[0]
            collectd.info("Error reading config file {}: {}".format(PLUGIN_CFG, e))
            raise

try:
    from statsCollector import StatsCollector
    statsCollectd = StatsCollectd()
except ImportError:
    e = sys.exc_info()[1]
    if "WorkItem" in str(e):
        collectd.info("Either pyClient module is not available or Xcalar is not installed in expected location = {}. Exception = {}".format(XLRDIR, sys.exc_info()))
    else:
        raise
except socket_error as serr:
    if serr.errno == errno.ECONNREFUSED:
        collectd.info("The xcmgmtd is not running. Exception: {}".format(sys.exc_info()))
    else:
        raise serr
except NameError:
    e = sys.exc_info()[1]
    if "XcalarApi" in str(e):
        collectd.info("The xcmgmtd is running but the usrnode is not running. Exception: {}".format(sys.exc_info()))
    else:
        raise
except ImportError:
    e = sys.exc_info()[1]
    if "WorkItem" in str(e):
        collectd.info("Either pyClient module is not available or Xcalar is not installed. Exception = {}".format(sys.exc_info()))
    else:
        raise
