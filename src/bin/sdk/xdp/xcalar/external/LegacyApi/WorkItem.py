# Copyright 2016-2019 Xcalar, Inc. All rights reserved.
#
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#

# flake8: noqa
# This disables flake8 static analysis for this file. This file is a pile of
# glue code so it's probably fine to import * everything. It will also go away
# once we migrate to XCRPC.
import json
import base64

from xcalar.compute.coretypes.LibApisCommon.ttypes import *
from xcalar.compute.coretypes.LibApisCommon.constants import *
from xcalar.compute.coretypes.LibApisEnums.ttypes import *
from xcalar.compute.coretypes.LibApisConstants.ttypes import *
from xcalar.compute.coretypes.DagTypes.ttypes import *
from xcalar.compute.coretypes.DagTypes.constants import *
from xcalar.compute.coretypes.XcalarApiVersionSignature.ttypes import XcalarApiVersionT
from xcalar.compute.coretypes.UdfTypes.ttypes import *
from xcalar.compute.coretypes.DataTargetTypes.ttypes import *
from xcalar.compute.coretypes.DataTargetTypes.constants import *
from xcalar.compute.coretypes.DataTargetEnums.constants import *
from xcalar.compute.coretypes.DataFormatEnums.ttypes import *
from xcalar.compute.coretypes.DataFormatEnums.constants import *
from xcalar.compute.coretypes.OrderingEnums.constants import *
from xcalar.compute.coretypes.JoinOpEnums.constants import *
from xcalar.compute.coretypes.UnionOpEnums.constants import *

#
# This file defines convenience classes for constructing Xcalar API work
# items.
#


# Base class for all work items. Defines a few basic things.
class WorkItem(object):
    def __init__(self, userName=None, userIdUnique=None, sessionName=None):
        self.workItem = XcalarApiWorkItemT()
        self.workItem.apiVersionSignature = XcalarApiVersionT.XcalarApiVersionSignature
        if sessionName:
            self.workItem.sessionName = sessionName
        if userIdUnique:
            self.workItem.userIdUnique = userIdUnique
        if userName:
            self.workItem.userId = userName

    # Gives the work item a chance to define how output should look to callers.
    def output(self, executeOutput):
        return executeOutput


class WorkItemGetNumNodes(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemGetNumNodes, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetNumNodes

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getNumNodesOutput


class WorkItemTop(WorkItem):
    def __init__(self,
                 measureIntervalInMs=None,
                 cacheValidityInMs=None,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemTop, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiTop

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.topInput = XcalarApiTopInputT()
        assert (measureIntervalInMs is not None)
        assert (cacheValidityInMs is not None)
        self.workItem.input.topInput.measureIntervalInMs = measureIntervalInMs
        self.workItem.input.topInput.cacheValidityInMs = cacheValidityInMs

    def output(self, executeOutput):
        return executeOutput.output.outputResult.topOutput

    def updateMeasureIntervalInMs(self, measureIntervalInMs=None):
        assert (measureIntervalInMs is not None)
        self.workItem.input.topInput.measureIntervalInMs = measureIntervalInMs

    def updateCacheValidity(self, cacheValidityInMs=None):
        assert (cacheValidityInMs is not None)
        self.workItem.input.topInput.cacheValidityInMs = \
            cacheValidityInMs


class WorkItemListDataset(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        # XXX super() in python 3.
        super(WorkItemListDataset, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiListDatasets

    def output(self, executeOutput):
        return executeOutput.output.outputResult.listDatasetsOutput


class WorkItemGetDatasetsInfo(WorkItem):
    def __init__(self, datasetsNamePattern, userName=None, userIdUnique=None):
        super(WorkItemGetDatasetsInfo, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetDatasetsInfo

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.getDatasetsInfoInput = XcalarApiGetDatasetsInfoInputT(
        )
        self.workItem.input.getDatasetsInfoInput.datasetsNamePattern = datasetsNamePattern

    def output(self, executeOutput):
        return executeOutput.output.outputResult


class WorkItemListDagInfo(WorkItem):
    def __init__(self, namePattern, srcType, userName=None, userIdUnique=None):
        super(WorkItemListDagInfo, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiListDagNodeInfo

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.listDagNodesInput = XcalarApiDagNodeNamePatternInputT(
        )
        self.workItem.input.listDagNodesInput.srcType = srcType
        self.workItem.input.listDagNodesInput.namePattern = namePattern

    def output(self, executeOutput):
        return executeOutput.output.outputResult.listNodesOutput


class WorkItemQueryState(WorkItem):
    def __init__(self,
                 queryName,
                 detailedStats=True,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemQueryState, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiQueryState

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.queryStateInput = XcalarApiQueryNameInputT()
        self.workItem.input.queryStateInput.queryName = queryName
        self.workItem.input.queryStateInput.detailedStats = detailedStats

    def output(self, executeOutput):
        return executeOutput.output.outputResult.queryStateOutput


class WorkItemQueryCancel(WorkItem):
    def __init__(self, queryName, userName=None, userIdUnique=None):
        super(WorkItemQueryCancel, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiQueryCancel

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.queryStateInput = XcalarApiQueryNameInputT()
        self.workItem.input.queryStateInput.queryName = queryName


class WorkItemQueryDelete(WorkItem):
    def __init__(self, queryName="", userName=None, userIdUnique=None):
        super(WorkItemQueryDelete, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiQueryDelete

        self.workItem.input = XcalarApiInputT()
        # Not a typo. Query delete uses queryStateInput for some reason...
        self.workItem.input.queryStateInput = XcalarApiQueryNameInputT()
        self.workItem.input.queryStateInput.queryName = queryName


class WorkItemCancelOp(WorkItem):
    def __init__(self, name):
        super(WorkItemCancelOp, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiCancelOp

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.dagTableNameInput = XcalarApiDagTableNameInputT()
        self.workItem.input.dagTableNameInput.tableInput = name


class WorkItemGetOpStats(WorkItem):
    def __init__(self, name, userName=None, userIdUnique=None):
        super(WorkItemGetOpStats, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetOpStats

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.dagTableNameInput = XcalarApiDagTableNameInputT()
        self.workItem.input.dagTableNameInput.tableInput = name

    def output(self, executeOutput):
        return executeOutput.output.outputResult.opStatsOutput


class WorkItemDeleteDagNode(WorkItem):
    def __init__(self,
                 namePattern,
                 srcType,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None,
                 deleteCompletely=False):
        super(WorkItemDeleteDagNode, self).__init__(userName, userIdUnique,
                                                    sessionName)
        self.workItem.api = XcalarApisT.XcalarApiDeleteObjects

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.deleteDagNodeInput = XcalarApiDagNodeNamePatternInputT(
        )
        self.workItem.input.deleteDagNodeInput.namePattern = namePattern
        self.workItem.input.deleteDagNodeInput.srcType = srcType
        self.workItem.input.deleteDagNodeInput.deleteCompletely = deleteCompletely

    def output(self, executeOutput):
        return executeOutput.output.outputResult.deleteDagNodesOutput


class WorkItemRenameNode(WorkItem):
    def __init__(self, oldName, newName, userName=None, userIdUnique=None):
        super().__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiRenameNode

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.renameNodeInput = XcalarApiRenameNodeInputT()
        self.workItem.input.renameNodeInput.oldName = oldName
        self.workItem.input.renameNodeInput.newName = newName


class WorkItemArchiveTables(WorkItem):
    def __init__(self,
                 archive,
                 tableNames,
                 allTables=False,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemArchiveTables, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiArchiveTables

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.archiveTablesInput = XcalarApiArchiveTablesInputT()
        self.workItem.input.archiveTablesInput.archive = archive
        self.workItem.input.archiveTablesInput.allTables = allTables
        self.workItem.input.archiveTablesInput.tableNames = tableNames

    def output(self, executeOutput):
        return executeOutput.output.outputResult.archiveTablesOutput


class WorkItemSessionNew(WorkItem):
    def __init__(self,
                 name,
                 fork,
                 forkedSession=None,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemSessionNew, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionNew

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionNewInput = XcalarApiSessionNewInputT()
        self.workItem.input.sessionNewInput.sessionName = name
        self.workItem.input.sessionNewInput.fork = fork
        self.workItem.input.sessionNewInput.forkedSessionName = forkedSession


class WorkItemSessionDelete(WorkItem):
    def __init__(self, namePattern, userName=None, userIdUnique=None):
        super(WorkItemSessionDelete, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionDelete

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionDeleteInput = XcalarApiSessionDeleteInputT()
        self.workItem.input.sessionDeleteInput.sessionName = namePattern
        self.workItem.input.sessionDeleteInput.noCleanup = False


class WorkItemSessionActivate(WorkItem):
    def __init__(self, name, userName=None, userIdUnique=None):
        super(WorkItemSessionActivate, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionActivate

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionActivateInput = XcalarApiSessionActivateInputT(
        )
        self.workItem.input.sessionActivateInput.sessionName = name


class WorkItemSessionInact(WorkItem):
    def __init__(self, name, userName=None, userIdUnique=None):
        super(WorkItemSessionInact, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionInact

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionDeleteInput = XcalarApiSessionDeleteInputT()
        self.workItem.input.sessionDeleteInput.sessionName = name
        self.workItem.input.sessionDeleteInput.noCleanup = False


class WorkItemSessionList(WorkItem):
    def __init__(self, pattern, userName=None, userIdUnique=None):
        super(WorkItemSessionList, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionList

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionListInput = XcalarApiSessionListArrayInputT(
        )
        self.workItem.input.sessionListInput.sesListInput = pattern

    def output(self, executeOutput):
        return executeOutput.output.outputResult.sessionListOutput


class WorkItemSessionRename(WorkItem):
    def __init__(self, newName, oldName, userName=None, userIdUnique=None):
        super().__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionRename

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionRenameInput = XcalarApiSessionRenameInputT()
        self.workItem.input.sessionRenameInput.sessionName = newName
        self.workItem.input.sessionRenameInput.origSessionName = oldName


class WorkItemSessionPersist(WorkItem):
    def __init__(self, name, userName=None, userIdUnique=None):
        super().__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionPersist

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionDeleteInput = XcalarApiSessionDeleteInputT()
        self.workItem.input.sessionDeleteInput.sessionName = name


class WorkItemSessionDownload(WorkItem):
    def __init__(self,
                 name,
                 pathToAdditionalFiles,
                 userName=None,
                 userIdUnique=None):
        super().__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSessionDownload

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionDownloadInput = XcalarApiSessionDownloadInputT(
        )
        self.workItem.input.sessionDownloadInput.sessionName = name
        self.workItem.input.sessionDownloadInput.pathToAdditionalFiles = pathToAdditionalFiles

    def output(self, executeOutput):
        downloadStr = executeOutput.output.outputResult.sessionDownloadOutput.sessionContent
        return base64.b64decode(downloadStr)


class WorkItemSessionUpload(WorkItem):
    def __init__(self,
                 name,
                 sessionContent,
                 pathToAdditionalFiles,
                 userName=None,
                 userIdUnique=None):
        super().__init__(userName, userIdUnique)

        encodedSessionContent = base64.b64encode(sessionContent).decode(
            "ascii")

        self.workItem.api = XcalarApisT.XcalarApiSessionUpload

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.sessionUploadInput = XcalarApiSessionUploadInputT()
        self.workItem.input.sessionUploadInput.sessionName = name
        self.workItem.input.sessionUploadInput.sessionContent = encodedSessionContent
        self.workItem.input.sessionUploadInput.sessionContentCount = len(
            encodedSessionContent)
        self.workItem.input.sessionUploadInput.pathToAdditionalFiles = pathToAdditionalFiles

    def output(self, executeOutput):
        return executeOutput.output.outputResult.sessionNewOutput


class WorkItemGetTableMeta(WorkItem):
    def __init__(self,
                 isTable,
                 name,
                 isPrecise,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemGetTableMeta, self).__init__(userName, userIdUnique,
                                                   sessionName)
        self.workItem.api = XcalarApisT.XcalarApiGetTableMeta

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.getTableMetaInput = XcalarApiGetTableMetaInputT()
        self.workItem.input.getTableMetaInput.tableNameInput = XcalarApiNamedInputT(
        )
        self.workItem.input.getTableMetaInput.tableNameInput.isTable = isTable
        self.workItem.input.getTableMetaInput.tableNameInput.name = name
        self.workItem.input.getTableMetaInput.tableNameInput.xid = XcalarApiXidInvalidT
        self.workItem.input.getTableMetaInput.isPrecise = isPrecise

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getTableMetaOutput


class WorkItemPreview(WorkItem):
    def __init__(self,
                 sourceArgs,
                 numBytes,
                 offset,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemPreview, self).__init__(userName, userIdUnique)
        inputObj = {
            "func": "preview",
            "sourceArgs": {
                "targetName": sourceArgs.targetName,
                "path": sourceArgs.path,
                "fileNamePattern": sourceArgs.fileNamePattern,
                "recursive": sourceArgs.recursive,
            },
            "offset": offset,
            "bytesRequested": numBytes
        }
        self.workItem.api = XcalarApisT.XcalarApiPreview
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.previewInput = XcalarApiPreviewInputT()
        self.workItem.input.previewInput.inputJson = json.dumps(inputObj)

    def output(self, executeOutput):
        return executeOutput.output.outputResult.previewOutput


class WorkItemAddTarget2(WorkItem):
    def __init__(self,
                 targetTypeId,
                 targetName,
                 targetParams,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemAddTarget2, self).__init__(userName, userIdUnique)
        inputObj = {
            "func": "addTarget",
            "targetTypeId": targetTypeId,
            "targetName": targetName,
            "targetParams": targetParams
        }
        self.workItem.api = XcalarApisT.XcalarApiTarget
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.targetInput = XcalarApiTargetInputT()
        self.workItem.input.targetInput.inputJson = json.dumps(inputObj)

    def output(self, executeOutput):
        return json.loads(
            executeOutput.output.outputResult.targetOutput.outputJson)


class WorkItemDeleteTarget2(WorkItem):
    def __init__(self, targetName, userName=None, userIdUnique=None):
        super(WorkItemDeleteTarget2, self).__init__(userName, userIdUnique)
        inputObj = {"func": "deleteTarget", "targetName": targetName}
        self.workItem.api = XcalarApisT.XcalarApiTarget
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.targetInput = XcalarApiTargetInputT()
        self.workItem.input.targetInput.inputJson = json.dumps(inputObj)

    def output(self, executeOutput):
        return json.loads(
            executeOutput.output.outputResult.targetOutput.outputJson)


class WorkItemListTargets2(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemListTargets2, self).__init__(userName, userIdUnique)
        inputObj = {"func": "listTargets"}
        self.workItem.api = XcalarApisT.XcalarApiTarget
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.targetInput = XcalarApiTargetInputT()
        self.workItem.input.targetInput.inputJson = json.dumps(inputObj)

    def output(self, executeOutput):
        return json.loads(
            executeOutput.output.outputResult.targetOutput.outputJson)


class WorkItemListTargetTypes2(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemListTargetTypes2, self).__init__(userName, userIdUnique)
        inputObj = {"func": "listTypes"}
        self.workItem.api = XcalarApisT.XcalarApiTarget
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.targetInput = XcalarApiTargetInputT()
        self.workItem.input.targetInput.inputJson = json.dumps(inputObj)

    def output(self, executeOutput):
        return json.loads(
            executeOutput.output.outputResult.targetOutput.outputJson)


class WorkItemDatasetCreate(WorkItem):
    def __init__(self,
                 name,
                 loadArgs,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemDatasetCreate, self).__init__(userName, userIdUnique,
                                                    sessionName)
        self.workItem.api = XcalarApisT.XcalarApiDatasetCreate
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.datasetCreateInput = XcalarApiDatasetCreateInputT()
        self.workItem.input.datasetCreateInput.dest = name
        self.workItem.input.datasetCreateInput.loadArgs = loadArgs


class WorkItemLoad(WorkItem):
    def __init__(self,
                 name,
                 loadArgs,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemLoad, self).__init__(userName, userIdUnique, sessionName)
        self.workItem.api = XcalarApisT.XcalarApiBulkLoad
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.loadInput = XcalarApiBulkLoadInputT()
        self.workItem.input.loadInput.dest = name
        self.workItem.input.loadInput.loadArgs = loadArgs

    def output(self, executeOutput):
        return executeOutput.output.outputResult.loadOutput


class WorkItemDatasetGetMeta(WorkItem):
    def __init__(self,
                 name,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemDatasetGetMeta, self).__init__(userName, userIdUnique,
                                                     sessionName)
        self.workItem.api = XcalarApisT.XcalarApiDatasetGetMeta
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.datasetGetMetaInput = XcalarApiDatasetGetMetaInputT(
        )
        self.workItem.input.datasetGetMetaInput.datasetName = name

    def output(self, executeOutput):
        return executeOutput.output.outputResult.datasetGetMetaOutput


class WorkItemDatasetUnload(WorkItem):
    def __init__(self,
                 namePattern,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemDatasetUnload, self).__init__(userName, userIdUnique,
                                                    sessionName)
        self.workItem.api = XcalarApisT.XcalarApiDatasetUnload
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.datasetUnloadInput = XcalarApiDatasetUnloadInputT()
        self.workItem.input.datasetUnloadInput.datasetNamePattern = namePattern

    def output(self, executeOutput):
        return executeOutput.output.outputResult.datasetUnloadOutput


class WorkItemDatasetDelete(WorkItem):
    def __init__(self,
                 name,
                 userName=None,
                 userIdUnique=None,
                 sessionName=None):
        super(WorkItemDatasetDelete, self).__init__(userName, userIdUnique,
                                                    sessionName)
        self.workItem.api = XcalarApisT.XcalarApiDatasetDelete
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.datasetDeleteInput = XcalarApiDatasetDeleteInputT()
        self.workItem.input.datasetDeleteInput.datasetName = name


class WorkItemIndex(WorkItem):
    def __init__(self,
                 datasetName,
                 dstTableName,
                 keyNames,
                 dhtName,
                 ordering,
                 isTable,
                 fatptrPrefixName="",
                 delaySort=False,
                 keyFieldNames=None):
        super(WorkItemIndex, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiIndex
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.indexInput = XcalarApiIndexInputT()

        self.workItem.input.indexInput.source = datasetName
        self.workItem.input.indexInput.dest = dstTableName
        self.workItem.input.indexInput.key = []

        if (isinstance(keyNames, list)):
            for ii in range(len(keyNames)):
                key = XcalarApiKeyT()
                key.name = keyNames[ii]
                key.type = "DfUnknown"

                if keyFieldNames:
                    key.keyFieldName = keyFieldNames[ii]

                if (isinstance(ordering, list)):
                    key.ordering = XcalarOrderingTStr[ordering[ii]]
                else:
                    key.ordering = XcalarOrderingTStr[ordering]

                self.workItem.input.indexInput.key.append(key)
        else:
            key = XcalarApiKeyT()
            key.name = keyNames
            if keyFieldNames:
                key.keyFieldName = keyFieldNames

            key.type = "DfUnknown"
            key.ordering = XcalarOrderingTStr[ordering]

            self.workItem.input.indexInput.key.append(key)

        self.workItem.input.indexInput.dhtName = dhtName
        self.workItem.input.indexInput.prefix = fatptrPrefixName
        self.workItem.input.indexInput.delaySort = delaySort

    def output(self, executeOutput):
        return executeOutput.output.outputResult.indexOutput.tableName


class WorkItemGetRowNum(WorkItem):
    def __init__(self, srcTable, dstTable, newFieldName):
        super(WorkItemGetRowNum, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiGetRowNum
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.getRowNumInput = XcalarApiGetRowNumInputT()
        self.workItem.input.getRowNumInput.source = srcTable
        self.workItem.input.getRowNumInput.dest = dstTable
        self.workItem.input.getRowNumInput.newField = newFieldName

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getRowNumOutput.tableName


class WorkItemMap(WorkItem):
    def __init__(self, srcTable, dstTable, evalStrs, newFieldNames):
        super(WorkItemMap, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiMap
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.mapInput = XcalarApiMapInputT()
        self.workItem.input.mapInput.source = srcTable
        self.workItem.input.mapInput.dest = dstTable

        self.workItem.input.mapInput.eval = []

        if (isinstance(evalStrs, list)):
            for ii in range(0, len(evalStrs)):
                eval = XcalarApiEvalT()
                eval.evalString = evalStrs[ii]
                eval.newField = newFieldNames[ii]

                self.workItem.input.mapInput.eval.append(eval)
        else:
            eval = XcalarApiEvalT()
            eval.evalString = evalStrs
            eval.newField = newFieldNames

            self.workItem.input.mapInput.eval.append(eval)

    def output(self, executeOutput):
        return executeOutput.output.outputResult.mapOutput.tableName


class WorkItemFilter(WorkItem):
    def __init__(self, srcTable, dstTable, filterStr):
        super(WorkItemFilter, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiFilter
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.filterInput = XcalarApiFilterInputT()
        self.workItem.input.filterInput.source = srcTable
        self.workItem.input.filterInput.dest = dstTable
        self.workItem.input.filterInput.eval = []
        eval = XcalarApiEvalT()
        eval.evalString = filterStr
        self.workItem.input.filterInput.eval.append(eval)

    def output(self, executeOutput):
        return executeOutput.output.outputResult.filterOutput.tableName


class WorkItemJoin(WorkItem):
    def __init__(self,
                 leftTable,
                 rightTable,
                 joinTable,
                 joinType,
                 leftColumns,
                 rightColumns,
                 filterString,
                 keepAllColumns=True):
        super(WorkItemJoin, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiJoin
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.joinInput = XcalarApiJoinInputT()

        self.workItem.input.joinInput.source = [leftTable, rightTable]
        self.workItem.input.joinInput.dest = joinTable

        self.workItem.input.joinInput.joinType = JoinOperatorTStr[joinType]
        self.workItem.input.joinInput.columns = [leftColumns, rightColumns]
        self.workItem.input.joinInput.evalString = filterString
        self.workItem.input.joinInput.keepAllColumns = keepAllColumns

    def output(self, executeOutput):
        return executeOutput.output.outputResult.joinOutput.tableName


class WorkItemUnion(WorkItem):
    def __init__(self, sources, dest, columns, dedup, unionType):
        super(WorkItemUnion, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiUnion
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.unionInput = XcalarApiUnionInputT()

        self.workItem.input.unionInput.source = sources
        self.workItem.input.unionInput.dest = dest
        self.workItem.input.unionInput.columns = columns
        self.workItem.input.unionInput.dedup = dedup
        self.workItem.input.unionInput.unionType = UnionOperatorTStr[unionType]

    def output(self, executeOutput):
        return executeOutput.output.outputResult.unionOutput.tableName


class WorkItemAggregate(WorkItem):
    def __init__(self,
                 srcTableName,
                 dstTableName,
                 evalStr,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemAggregate, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiAggregate
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.aggregateInput = XcalarApiAggregateInputT()

        self.workItem.input.aggregateInput.source = srcTableName
        self.workItem.input.aggregateInput.dest = dstTableName
        self.workItem.input.aggregateInput.eval = []
        eval = XcalarApiEvalT()
        eval.evalString = evalStr
        self.workItem.input.aggregateInput.eval.append(eval)

    def output(self, executeOutput):
        return json.loads(executeOutput.output.outputResult.aggregateOutput.
                          jsonAnswer)['Value']


class WorkItemGroupBy(WorkItem):
    def __init__(self,
                 srcTable,
                 dstTable,
                 evalStrs,
                 newFieldNames,
                 includeSrcTableSample=False,
                 groupAll=False):
        super(WorkItemGroupBy, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiGroupBy
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.groupByInput = XcalarApiGroupByInputT()
        self.workItem.input.groupByInput.source = srcTable
        self.workItem.input.groupByInput.dest = dstTable
        self.workItem.input.groupByInput.eval = []

        if (isinstance(evalStrs, list)):
            for ii in range(0, len(evalStrs)):
                eval = XcalarApiEvalT()
                eval.evalString = evalStrs[ii]
                eval.newField = newFieldNames[ii]

                self.workItem.input.groupByInput.eval.append(eval)
        else:
            eval = XcalarApiEvalT()
            eval.evalString = evalStrs
            eval.newField = newFieldNames

            self.workItem.input.groupByInput.eval.append(eval)

        self.workItem.input.groupByInput.includeSample = includeSrcTableSample
        self.workItem.input.groupByInput.icv = False
        self.workItem.input.groupByInput.newKeyField = ""
        self.workItem.input.groupByInput.groupAll = groupAll

    def output(self, executeOutput):
        return executeOutput.output.outputResult.groupByOutput.tableName


class WorkItemProject(WorkItem):
    def __init__(self, srcTable, dstTable, columns):
        super(WorkItemProject, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiProject
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.projectInput = XcalarApiProjectInputT()
        self.workItem.input.projectInput.source = srcTable
        self.workItem.input.projectInput.dest = dstTable
        self.workItem.input.projectInput.columns = columns

    def output(self, executeOutput):
        return executeOutput.output.outputResult.projectOutput.tableName


class WorkItemShutdown(WorkItem):
    def __init__(self):
        super(WorkItemShutdown, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiShutdown


class WorkItemUdfAdd(WorkItem):
    def __init__(self, moduleName, source, userName=None, userIdUnique=None):
        super(WorkItemUdfAdd, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiUdfAdd
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.udfAddUpdateInput = UdfModuleSrcT()
        self.workItem.input.udfAddUpdateInput.type = UdfTypeT.UdfTypePython
        self.workItem.input.udfAddUpdateInput.moduleName = moduleName
        self.workItem.input.udfAddUpdateInput.source = source

    def output(self, executeOutput):
        return executeOutput.output.outputResult.udfAddUpdateOutput


class WorkItemUdfUpdate(WorkItem):
    def __init__(self, moduleName, source):
        super(WorkItemUdfUpdate, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiUdfUpdate
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.udfAddUpdateInput = UdfModuleSrcT()
        self.workItem.input.udfAddUpdateInput.type = UdfTypeT.UdfTypePython
        self.workItem.input.udfAddUpdateInput.moduleName = moduleName
        self.workItem.input.udfAddUpdateInput.source = source

    def output(self, executeOutput):
        return executeOutput.output.outputResult.udfAddUpdateOutput


class WorkItemUdfDelete(WorkItem):
    def __init__(self, moduleName):
        super(WorkItemUdfDelete, self).__init__()
        self.workItem.api = XcalarApisT.XcalarApiUdfDelete
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.udfDeleteInput = XcalarApiUdfDeleteInputT()
        self.workItem.input.udfDeleteInput.moduleName = moduleName


class WorkItemUdfGet(WorkItem):
    def __init__(self, moduleName, userName=None, userIdUnique=None):
        super(WorkItemUdfGet, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiUdfGet
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.udfGetInput = XcalarApiUdfGetInputT()
        self.workItem.input.udfGetInput.moduleName = moduleName

    def output(self, getOutput):
        return getOutput.output.outputResult.udfGetOutput


class WorkItemListXdfs(WorkItem):
    def __init__(self,
                 fnNamePattern,
                 categoryPattern,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemListXdfs, self).__init__(userName, userIdUnique)

        self.workItem.api = XcalarApisT.XcalarApiListXdfs
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.listXdfsInput = XcalarApiListXdfsInputT()
        self.workItem.input.listXdfsInput.fnNamePattern = fnNamePattern
        self.workItem.input.listXdfsInput.categoryPattern = categoryPattern

    def output(self, getOutput):
        return getOutput.output.outputResult.listXdfsOutput


class WorkItemGetStats(WorkItem):
    def __init__(self, nodeId=0, userName=None, userIdUnique=None):
        super(WorkItemGetStats, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetStat
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.statInput = XcalarApiStatInputT()
        self.workItem.input.statInput.nodeId = nodeId

    def output(self, rsOutput):
        return rsOutput.output.outputResult.statOutput.stats


class WorkItemGetStatGroupIdMap(WorkItem):
    def __init__(self, nodeId=0, userName=None, userIdUnique=None):
        super(WorkItemGetStatGroupIdMap, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetStatGroupIdMap
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.statInput = XcalarApiStatInputT()
        self.workItem.input.statInput.nodeId = nodeId

    def output(self, rsOutput):
        return rsOutput.output.outputResult.statGroupIdMapOutput


class WorkItemExport(WorkItem):
    def __init__(self,
                 tableName,
                 driverName,
                 driverParams,
                 columns,
                 destDagNodeName="",
                 userName=None,
                 userIdUnique=None):
        super(WorkItemExport, self).__init__(userName, userIdUnique)
        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiExport
        self.workItem.input.exportInput = XcalarApiExportInputT()
        self.workItem.input.exportInput.source = tableName
        self.workItem.input.exportInput.driverName = driverName
        self.workItem.input.exportInput.driverParams = driverParams
        self.workItem.input.exportInput.columns = columns
        self.workItem.input.exportInput.dest = destDagNodeName


class WorkItemImportRetina(WorkItem):
    def __init__(self,
                 retinaName,
                 overwrite,
                 retinaTar=None,
                 retinaJsonStr=None,
                 userName=None,
                 userIdUnique=None,
                 udfUserName=None,
                 udfSessionName=None):
        super(WorkItemImportRetina, self).__init__(userName, userIdUnique)

        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiImportRetina

        self.workItem.input.importRetinaInput = XcalarApiImportRetinaInputT()
        self.workItem.input.importRetinaInput.retinaName = retinaName
        self.workItem.input.importRetinaInput.overwriteExistingUdf = overwrite

        if retinaTar:
            encodedRetina = base64.b64encode(retinaTar).decode("ascii")
            self.workItem.input.importRetinaInput.retinaCount = len(
                encodedRetina)
            self.workItem.input.importRetinaInput.retina = encodedRetina
            self.workItem.input.importRetinaInput.loadRetinaJson = False

        if retinaJsonStr:
            self.workItem.input.importRetinaInput.loadRetinaJson = True
            self.workItem.input.importRetinaInput.retinaJson = retinaJsonStr

        self.workItem.input.importRetinaInput.udfUserName = udfUserName
        self.workItem.input.importRetinaInput.udfSessionName = udfSessionName

    def output(self, executeOutput):
        return executeOutput.output.outputResult.importRetinaOutput


class WorkItemExportRetina(WorkItem):
    def __init__(self, retinaName, userName=None, userIdUnique=None):
        super(WorkItemExportRetina, self).__init__(userName, userIdUnique)

        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiExportRetina

        self.workItem.input.exportRetinaInput = XcalarApiExportRetinaInputT()
        self.workItem.input.exportRetinaInput.retinaName = retinaName

    def output(self, executeOutput):
        retinaStr = executeOutput.output.outputResult.exportRetinaOutput.retina

        return base64.b64decode(retinaStr)


class WorkItemMakeRetina(WorkItem):
    def __init__(self,
                 retinaName,
                 dstTables,
                 srcTables,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemMakeRetina, self).__init__(userName, userIdUnique)

        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiMakeRetina

        self.workItem.input.makeRetinaInput = XcalarApiMakeRetinaInputT()
        self.workItem.input.makeRetinaInput.retinaName = retinaName
        self.workItem.input.makeRetinaInput.numTables = len(dstTables)
        self.workItem.input.makeRetinaInput.tableArray = dstTables
        self.workItem.input.makeRetinaInput.numSrcTables = len(srcTables)
        self.workItem.input.makeRetinaInput.srcTables = srcTables


class WorkItemListRetina(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemListRetina, self).__init__(userName, userIdUnique)

        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiListRetinas
        self.workItem.input.listRetinasInput = XcalarApiListRetinasInputT()
        self.workItem.input.listRetinasInput.namePattern = "*"

    def output(self, executeOutput):
        return executeOutput.output.outputResult.listRetinasOutput


class WorkItemDeleteRetina(WorkItem):
    def __init__(self, retinaName, userName=None, userIdUnique=None):
        super(WorkItemDeleteRetina, self).__init__(userName, userIdUnique)

        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiDeleteRetina

        self.workItem.input.deleteRetinaInput = XcalarApiDeleteRetinaInputT()
        self.workItem.input.deleteRetinaInput.delRetInput = retinaName


class WorkItemAppSet(WorkItem):
    def __init__(self,
                 name,
                 hostType,
                 execStr,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemAppSet, self).__init__(userName, userIdUnique)
        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiAppSet

        self.workItem.input.appSetInput = XcalarApiAppSetInputT()
        self.workItem.input.appSetInput.name = name
        self.workItem.input.appSetInput.hostType = hostType
        self.workItem.input.appSetInput.execStr = execStr


class WorkItemAppRun(WorkItem):
    def __init__(self, name, isGlobal, inStr, userName=None,
                 userIdUnique=None):
        super(WorkItemAppRun, self).__init__(userName, userIdUnique)
        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiAppRun

        self.workItem.input.appRunInput = XcalarApiAppRunInputT()
        self.workItem.input.appRunInput.name = name
        self.workItem.input.appRunInput.isGlobal = isGlobal
        self.workItem.input.appRunInput.inStr = inStr


class WorkItemAppReap(WorkItem):
    def __init__(self, groupId, cancel=False, userName=None,
                 userIdUnique=None):
        super(WorkItemAppReap, self).__init__(userName, userIdUnique)
        self.workItem.input = XcalarApiInputT()
        self.workItem.api = XcalarApisT.XcalarApiAppReap

        self.workItem.input.appReapInput = XcalarApiAppReapInputT()
        self.workItem.input.appReapInput.appGroupId = groupId
        self.workItem.input.appReapInput.cancel = cancel


class WorkItemRuntimeSetParam(WorkItem):
    def __init__(self, runtimeSetParams, userName=None, userIdUnique=None):
        super(WorkItemRuntimeSetParam, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiRuntimeSetParam
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.runtimeSetParamInput = XcalarApiRuntimeSetParamInputT(
        )
        self.workItem.input.runtimeSetParamInput = runtimeSetParams


class WorkItemRuntimeGetParam(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemRuntimeGetParam, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiRuntimeGetParam

    def output(self, executeOutput):
        return executeOutput.output.outputResult.runtimeGetParamOutput


class WorkItemSetConfigParam(WorkItem):
    def __init__(self, paramName, paramValue, userName=None,
                 userIdUnique=None):
        super(WorkItemSetConfigParam, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSetConfigParam
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.setConfigParamInput = XcalarApiSetConfigParamInputT(
        )
        self.workItem.input.setConfigParamInput.paramName = paramName
        self.workItem.input.setConfigParamInput.paramValue = paramValue


class WorkItemGetConfigParams(WorkItem):
    def __init__(self, userName=None, userIdUnique=None):
        super(WorkItemGetConfigParams, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetConfigParams

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getConfigParamsOutput


class WorkItemGetRetina(WorkItem):
    def __init__(self, retinaName, userName=None, userIdUnique=None):
        super(WorkItemGetRetina, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetRetina
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.getRetinaInput = XcalarApiGetRetinaInputT()
        self.workItem.input.getRetinaInput.retInput = retinaName

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getRetinaOutput


class WorkItemGetRetinaJson(WorkItem):
    def __init__(self, retinaName, userName=None, userIdUnique=None):
        super(WorkItemGetRetinaJson, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiGetRetinaJson
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.getRetinaJsonInput = XcalarApiGetRetinaJsonInputT()
        self.workItem.input.getRetinaJsonInput.retinaName = retinaName

    def output(self, executeOutput):
        return executeOutput.output.outputResult.getRetinaJsonOutput


class WorkItemUpdateRetina(WorkItem):
    def __init__(self,
                 retinaName,
                 retinaJson,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemUpdateRetina, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiUpdateRetina

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.updateRetinaInput = XcalarApiUpdateRetinaInputT()
        self.workItem.input.updateRetinaInput.retinaName = retinaName
        self.workItem.input.updateRetinaInput.retinaJson = retinaJson


class WorkItemUpdateTable(WorkItem):
    def __init__(self,
                 srcTableNames,
                 dstTableNames,
                 times=[],
                 dropSrc=False,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemUpdateTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiUpdate

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.updateInput = XcalarApiUpdateInputT()
        self.workItem.input.updateInput.updates = []

        if (isinstance(srcTableNames, list)):
            for ii in range(0, len(srcTableNames)):
                update = XcalarApiUpdateTableInputT()
                update.source = srcTableNames[ii]
                update.dest = dstTableNames[ii]

                if len(times) > 0:
                    update.unixTS = times[ii]
                else:
                    update.unixTS = 0
                update.dropSrc = dropSrc
                self.workItem.input.updateInput.updates.append(update)
        else:
            update = XcalarApiUpdateTableInputT()
            update.source = srcTableNames
            update.dest = dstTableNames

            if isinstance(times, list):
                update.unixTS = 0
            else:
                update.unixTS = times
            update.dropSrc = dropSrc

            self.workItem.input.updateInput.updates.append(update)

        def output(self, executeOutput):
            return executeOutput.output.outputResult.updateOutput.batchId


class WorkItemPublishTable(WorkItem):
    def __init__(self,
                 srcTableName,
                 dstTableName,
                 time=0,
                 dropSrc=False,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemPublishTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiPublish

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.publishInput = XcalarApiPublishInputT()
        self.workItem.input.publishInput.source = srcTableName
        self.workItem.input.publishInput.dest = dstTableName
        self.workItem.input.publishInput.unixTS = time
        self.workItem.input.publishInput.dropSrc = dropSrc


class WorkItemSelectTable(WorkItem):
    def __init__(self,
                 srcTableName,
                 dstTableName,
                 maxBatchId=-1,
                 minBatchId=-1,
                 filterString="",
                 columns=[],
                 limitRows=0,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemSelectTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSelect

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.selectInput = XcalarApiSelectInputT()
        self.workItem.input.selectInput.source = srcTableName
        self.workItem.input.selectInput.dest = dstTableName
        self.workItem.input.selectInput.minBatchId = minBatchId
        self.workItem.input.selectInput.maxBatchId = maxBatchId
        self.workItem.input.selectInput.filterString = filterString
        self.workItem.input.selectInput.columns = columns
        self.workItem.input.selectInput.limitRows = limitRows

    def output(self, executeOutput):
        return executeOutput.output.outputResult.selectOutput.tableName


class WorkItemUnpublishTable(WorkItem):
    def __init__(self,
                 srcTableName,
                 inactivateOnly=False,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemUnpublishTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiUnpublish

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.unpublishInput = XcalarApiUnpublishInputT()
        self.workItem.input.unpublishInput.source = srcTableName
        self.workItem.input.unpublishInput.inactivateOnly = inactivateOnly


class WorkItemRestoreTable(WorkItem):
    def __init__(self, srcTableName, userName=None, userIdUnique=None):
        super(WorkItemRestoreTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiRestoreTable

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.restoreTableInput = XcalarApiRestoreTableInputT()
        self.workItem.input.restoreTableInput.publishedTableName = srcTableName

    def output(self, executeOutput):
        return executeOutput.output.outputResult.restoreTableOutput


class WorkItemCoalesceTable(WorkItem):
    def __init__(self, srcTableName, userName=None, userIdUnique=None):
        super(WorkItemCoalesceTable, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiCoalesce

        self.workItem.input = XcalarApiInputT()
        self.workItem.input.coalesceInput = XcalarApiCoalesceInputT()
        self.workItem.input.coalesceInput.source = srcTableName


class WorkItemSynthesize(WorkItem):
    def __init__(self,
                 src,
                 dstTable,
                 columns,
                 userName=None,
                 userIdUnique=None):
        super(WorkItemSynthesize, self).__init__(userName, userIdUnique)
        self.workItem.api = XcalarApisT.XcalarApiSynthesize
        self.workItem.input = XcalarApiInputT()
        self.workItem.input.synthesizeInput = XcalarApiSynthesizeInputT()
        self.workItem.input.synthesizeInput.source = src
        self.workItem.input.synthesizeInput.dest = dstTable
        self.workItem.input.synthesizeInput.columns = columns
        self.workItem.input.synthesizeInput.sameSession = True

    def output(self, executeOutput):
        return executeOutput.output.outputResult.synthesizeOutput
