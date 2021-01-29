# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from .WorkItem import WorkItemGetNumNodes


class GetNumNodesRequestHelper(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def executeGetNumNodes(self):
        workItem = WorkItemGetNumNodes()
        return self.xcalarApi.execute(workItem)

    def getNumNodes(self):
        result = self.executeGetNumNodes()
        return int(result.numNodes)
