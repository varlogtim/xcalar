# Copyright 2016 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
from .WorkItem import WorkItemGetTableMeta


class GetTableMeta(object):
    def __init__(self, xcalarApi, isTable, name):
        workItem = WorkItemGetTableMeta(isTable, name, False)
        self.getTableMetaOutput = xcalarApi.execute(workItem)

    def total(self):
        sum = 0
        for x in self.getTableMetaOutput.metas:
            sum += x.numRows
        return sum


class DatasetCount(GetTableMeta):
    def __init__(self, xcalarApi, name):
        super(DatasetCount, self).__init__(xcalarApi, False, name)


class TableCount(GetTableMeta):
    def __init__(self, xcalarApi, name):
        super(TableCount, self).__init__(xcalarApi, True, name)
