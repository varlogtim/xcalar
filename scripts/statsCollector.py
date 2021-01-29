# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

import sys
import time
import datetime

from xcalar.external.LegacyApi.WorkItem import *
from xcalar.external.LegacyApi.XcalarApi import *

statUserId = 1337
statUserName = "xcstat"

class StatsCollector(object):
    def __init__(self, mgmtdUrl, nodeIds):
        self.xcalarApi = XcalarApi(mgmtdUrl)
        self.groupMap = self.groupIdMap()
        self.nodeIds = nodeIds

    def getStats(self, nodeNum):
        workItem = WorkItemGetStats(nodeNum, userIdUnique=statUserId, userName=statUserName)
        output = self.xcalarApi.execute(workItem)
        return output

    def groupIdMap(self):
        workItem = WorkItemGetStatGroupIdMap(userIdUnique=statUserId, userName=statUserName)
        output = self.xcalarApi.execute(workItem)
        return output

    def getGroupName(self, stat):
        return self.groupMap[stat.groupId]

    def getTimestamp(self):
        ts = time.time()
        return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H:%M:%S')

    def getAllStats(self):
        allStats = []

        for nodeId in self.nodeIds:
            try:
                oneStats = self.getStats(nodeId)
                allStats.append(oneStats)
            except:
                # If we have network errors or something, stay alive
                print("[{timestamp}] Unable to get stats:{exception}".format(
                        timestamp=self.getTimestamp(),
                        exception=sys.exc_info()[0]))
                raise
        return allStats
