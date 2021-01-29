# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
from .WorkItem import WorkItemAddTarget2, WorkItemListTargets2, WorkItemDeleteTarget2, WorkItemListTargetTypes2


class Target2(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def add(self, targetTypeId, targetName, targetParams):
        workItem = WorkItemAddTarget2(targetTypeId, targetName, targetParams)
        return self.xcalarApi.execute(workItem)

    def delete(self, targetName):
        workItem = WorkItemDeleteTarget2(targetName)
        return self.xcalarApi.execute(workItem)

    def listTargets(self):
        workItem = WorkItemListTargets2()
        return self.xcalarApi.execute(workItem)

    def listTypes(self):
        workItem = WorkItemListTargetTypes2()
        return self.xcalarApi.execute(workItem)
