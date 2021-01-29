# Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

from .WorkItem import (
    WorkItemAggregate, WorkItemArchiveTables,
    WorkItemCoalesceTable, WorkItemDatasetUnload, WorkItemDeleteDagNode,
    WorkItemFilter, WorkItemGetRowNum, WorkItemGetTableMeta, WorkItemGroupBy,
    WorkItemIndex, WorkItemJoin, WorkItemMap,
    WorkItemProject, WorkItemPublishTable,
    WorkItemRestoreTable, WorkItemSelectTable,
    WorkItemUnion, WorkItemUnpublishTable, WorkItemUpdateTable,
    WorkItemSynthesize)

from xcalar.compute.coretypes.OrderingEnums.constants import XcalarOrderingT
from xcalar.compute.coretypes.JoinOpEnums.ttypes import JoinOperatorT
from xcalar.compute.coretypes.UnionOpEnums.ttypes import UnionOperatorT
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT

from xcalar.compute.localtypes.Table_pb2 import IndexRequest

from xcalar.external.published_table import PublishedTable

class Operators(object):
    def __init__(self, xcalarApi):
        self.xcalarApi = xcalarApi

    def indexDataset(self,
                     datasetName,
                     dstTableName,
                     keyName,
                     fatptrPrefixName,
                     dhtName="",
                     ordering=XcalarOrderingT.XcalarOrderingUnordered,
                     delaySort=False):
        workItem = WorkItemIndex(
            datasetName,
            dstTableName,
            keyName,
            dhtName,
            ordering,
            False,
            fatptrPrefixName,
            delaySort=delaySort)
        return self.xcalarApi.execute(workItem)

    def indexTable(self,
                   tableName,
                   dstTableName,
                   keyName,
                   dhtName="",
                   ordering=XcalarOrderingT.XcalarOrderingUnordered,
                   delaySort=False,
                   keyFieldName=None):
        workItem = WorkItemIndex(
            tableName,
            dstTableName,
            keyName,
            dhtName,
            ordering,
            True,
            delaySort=delaySort,
            keyFieldNames=keyFieldName)
        return self.xcalarApi.execute(workItem)

    def getRowNum(self, source, dest, newFieldName):
        workItem = WorkItemGetRowNum(source, dest, newFieldName)
        return self.xcalarApi.execute(workItem)

    def map(self, source, dest, evalStrs, newColumns):
        workItem = WorkItemMap(source, dest, evalStrs, newColumns)
        return self.xcalarApi.execute(workItem)

    def filter(self, source, dest, filterStr):
        workItem = WorkItemFilter(source, dest, filterStr)
        return self.xcalarApi.execute(workItem)

    def join(self,
             leftTable,
             rightTable,
             joinTable,
             joinType=JoinOperatorT.InnerJoin,
             leftColumns=[],
             rightColumns=[],
             filterString="",
             keepAllColumns=True):
        workItem = WorkItemJoin(leftTable, rightTable, joinTable, joinType,
                                leftColumns, rightColumns, filterString,
                                keepAllColumns)
        return self.xcalarApi.execute(workItem)

    def union(self,
              sources,
              dest,
              columns,
              dedup=False,
              unionType=UnionOperatorT.UnionStandard):
        workItem = WorkItemUnion(sources, dest, columns, dedup, unionType)
        return self.xcalarApi.execute(workItem)

    def aggregate(self, srcTable, dstTable, evalStr):
        workItem = WorkItemAggregate(srcTable, dstTable, evalStr)
        return self.xcalarApi.execute(workItem)

    def groupBy(self,
                srcTable,
                dstTable,
                evalStrs,
                newFields,
                includeSrcTableSample=False,
                groupAll=False):
        workItem = WorkItemGroupBy(srcTable, dstTable, evalStrs, newFields,
                                   includeSrcTableSample, groupAll)
        return self.xcalarApi.execute(workItem)

    def project(self, srcTable, dstTable, columns):
        workItem = WorkItemProject(srcTable, dstTable, columns)
        return self.xcalarApi.execute(workItem)

    def tableMeta(self, tableName):
        workItem = WorkItemGetTableMeta(True, tableName, False)
        return self.xcalarApi.execute(workItem)

    def dropTable(self, namePattern, deleteCompletely=False):
        workItem = WorkItemDeleteDagNode(
            namePattern,
            SourceTypeT.SrcTable,
            deleteCompletely=deleteCompletely)
        return self.xcalarApi.execute(workItem)

    def archiveTables(self, tables):
        workItem = WorkItemArchiveTables(True, tables)
        return self.xcalarApi.execute(workItem)

    def unarchiveTables(self, tables):
        workItem = WorkItemArchiveTables(False, tables)
        return self.xcalarApi.execute(workItem)

    # shortcut to drop constants
    def dropConstants(self, namePattern, deleteCompletely=False):
        workItemDropConstants = WorkItemDeleteDagNode(
            namePattern,
            SourceTypeT.SrcConstant,
            deleteCompletely=deleteCompletely)
        return self.xcalarApi.execute(workItemDropConstants)

    # shortcut to drop export nodes
    def dropExportNodes(self, namePattern, deleteCompletely=False):
        workItemDropExportNodes = WorkItemDeleteDagNode(
            namePattern,
            SourceTypeT.SrcExport,
            deleteCompletely=deleteCompletely)
        return self.xcalarApi.execute(workItemDropExportNodes)

    # shortcut to drop export nodes
    def dropDatasets(self, namePattern, deleteCompletely=False):
        workItemDropDatasets = WorkItemDeleteDagNode(
            namePattern,
            SourceTypeT.SrcDataset,
            deleteCompletely=deleteCompletely)
        self.xcalarApi.execute(workItemDropDatasets)

        workItemDatasetUnload = WorkItemDatasetUnload(namePattern)
        return self.xcalarApi.execute(workItemDatasetUnload)

    def publish(self, srcTable, dstTable, unixTimestamp=0, dropSrc=False):
        workItem = WorkItemPublishTable(srcTable, dstTable, unixTimestamp,
                                        dropSrc)
        return self.xcalarApi.execute(workItem)

    def update(self, srcTable, dstTable, unixTimestamp=[], dropSrc=False):
        workItem = WorkItemUpdateTable(srcTable, dstTable, unixTimestamp,
                                       dropSrc)
        return self.xcalarApi.execute(workItem)

    def select(self,
               srcTable,
               dstTable="",
               maxBatchId=-1,
               minBatchId=-1,
               filterString="",
               columns=[],
               limitRows=0):
        workItem = WorkItemSelectTable(srcTable, dstTable, maxBatchId,
                                       minBatchId, filterString, columns,
                                       limitRows)
        return self.xcalarApi.execute(workItem)

    def deactivate(self, srcTable):
        workItem = WorkItemUnpublishTable(srcTable, True)
        return self.xcalarApi.execute(workItem)

    def unpublish(self, srcTable, inactivateOnly=False):
        workItem = WorkItemUnpublishTable(srcTable, inactivateOnly)
        return self.xcalarApi.execute(workItem)

    def restorePublishedTable(self, srcTable):
        workItem = WorkItemRestoreTable(srcTable)
        return self.xcalarApi.execute(workItem)

    def coalesce(self, srcTable):
        workItem = WorkItemCoalesceTable(srcTable)
        return self.xcalarApi.execute(workItem)

    def publishTableChangeOwner(self, publishTableName, sessionName,
                                userIdName):
        return PublishedTable.changeOwner(self.xcalarApi.sdk_client,
                                          publishTableName, sessionName,
                                          userIdName)

    def listPublishedTables(self, namePattern):
        return PublishedTable.list(self.xcalarApi.sdk_client, namePattern)

    def addIndex(self, srcTable, keyName):
        request = IndexRequest()
        request.table_name = srcTable
        request.key_name = keyName
        return self.xcalarApi.sdk_client._table_service.addIndex(request)

    def removeIndex(self, srcTable, keyName):
        request = IndexRequest()
        request.table_name = srcTable
        request.key_name = keyName
        return self.xcalarApi.sdk_client._table_service.removeIndex(request)

    def synthesize(self, src, dstTable, columns):
        workItem = WorkItemSynthesize(src, dstTable, columns)
        return self.xcalarApi.execute(workItem)
