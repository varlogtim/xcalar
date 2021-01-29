# Copyright 2017 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from .WorkItem import WorkItemTop


class Top(object):
    def __init__(self,
                 xcalarApi,
                 measureIntervalInMs=None,
                 cacheValidityInMs=None):
        self.xcalarApi = xcalarApi
        assert (measureIntervalInMs is not None)
        assert (cacheValidityInMs is not None)
        self.measureIntervalInMs = measureIntervalInMs
        self.cacheValidityInMs = cacheValidityInMs

    def executeTop(self):
        workItem = WorkItemTop(
            measureIntervalInMs=self.measureIntervalInMs,
            cacheValidityInMs=self.cacheValidityInMs)
        return self.xcalarApi.execute(workItem)
