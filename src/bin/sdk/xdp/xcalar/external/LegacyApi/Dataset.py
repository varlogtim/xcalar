# Copyright 2016 - 2018 Xcalar, Inc. All rights reserved.
# No use, or distribution, of this source code is permitted in any form or
# means without a valid, written license agreement with Xcalar, Inc.
# Please refer to the included "COPYING" file for terms and conditions
# regarding the use and redistribution of this software.
#
import json

from .XcalarApi import XcalarApiStatusException
from .WorkItem import WorkItemPreview, WorkItemLoad
from .WorkItem import WorkItemDeleteDagNode, WorkItemDatasetUnload, WorkItemGetTableMeta, WorkItemListDataset, WorkItemGetDatasetsInfo
from .Count import DatasetCount

from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultRecordDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultFieldDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultQuoteDelimT
from xcalar.compute.coretypes.LibApisCommon.constants import XcalarApiDefaultCsvParserNameT, XcalarApiDefaultJsonParserNameT
from xcalar.compute.coretypes.LibApisCommon.ttypes import XcalarApiDfLoadArgsT
from xcalar.compute.coretypes.LibApisCommon.ttypes import DataSourceArgsT
from xcalar.compute.coretypes.LibApisCommon.ttypes import ParseArgsT
from xcalar.compute.coretypes.Status.ttypes import StatusT
from xcalar.compute.coretypes.SourceTypeEnum.ttypes import SourceTypeT
from xcalar.compute.coretypes.CsvLoadArgsEnums.constants import CsvSchemaModeTFromStr


class DatasetLoadException(Exception):
    def __init__(self, output):
        super(Exception, self).__init__(output.errorString)
        self.output = output


class Dataset(object):
    def __init__(self,
                 xcalarApi,
                 name,
                 sourceArgList,
                 parserName,
                 parserArgs,
                 sampleSize,
                 allowFileErrors,
                 allowRecordErrors,
                 schema=[],
                 fileNameField="",
                 recordNumField=""):
        self.xcalarApi = xcalarApi
        self.loaded = False

        self.name = name
        self.sampleSize = sampleSize

        self.loadArgs = XcalarApiDfLoadArgsT()
        self.loadArgs.parseArgs = ParseArgsT()

        self.loadArgs.sourceArgsList = sourceArgList

        self.loadArgs.parseArgs.parserFnName = parserName
        self.loadArgs.parseArgs.parserArgJson = json.dumps(parserArgs)
        self.loadArgs.parseArgs.fileNameFieldName = fileNameField
        self.loadArgs.parseArgs.recordNumFieldName = recordNumField
        self.loadArgs.parseArgs.allowFileErrors = allowFileErrors
        self.loadArgs.parseArgs.allowRecordErrors = allowRecordErrors
        self.loadArgs.parseArgs.schema = schema

        self.loadArgs.size = sampleSize

    def preview(self, numBytes, offset=0):
        workItem = WorkItemPreview(self.loadArgs.sourceArgsList[0], numBytes,
                                   offset)
        return json.loads(self.xcalarApi.execute(workItem).outputJson)

    def load(self):
        workItem = WorkItemLoad(self.name, self.loadArgs)
        result = None
        try:
            result = self.xcalarApi.execute(workItem)
        except XcalarApiStatusException as e:
            if e.output.loadOutput is not None:
                result = e.output.loadOutput
                print("Load error in '{}':\n{}".format(
                    result.errorFile, result.errorString.replace("\\n", "\n")))
                raise

        self.name = ".XcalarDS." + self.name
        self.loaded = True

        return result

    @staticmethod
    def bulkDelete(xcalarApi, namePattern, deleteCompletely=False):
        workItem = WorkItemDeleteDagNode(
            namePattern,
            SourceTypeT.SrcDataset,
            deleteCompletely=deleteCompletely)
        xcalarApi.execute(workItem)
        workItem = WorkItemDatasetUnload(namePattern)
        try:
            ret = xcalarApi.execute(workItem)
            return ret
        except XcalarApiStatusException as e:
            if e.status == StatusT.StatusDsNotFound:
                return None
            else:
                raise

    @staticmethod
    def list(xcalarApi):
        workItem = WorkItemListDataset()
        return xcalarApi.execute(workItem)

    def getInfo(self, datasetsNamePattern=None):
        if datasetsNamePattern is None:
            datasetsNamePattern = self.name
        workItem = WorkItemGetDatasetsInfo(datasetsNamePattern)
        return self.xcalarApi.execute(workItem)

    def delete(self, deleteCompletely=False):
        workItem = WorkItemDeleteDagNode(
            self.name,
            SourceTypeT.SrcDataset,
            deleteCompletely=deleteCompletely)
        try:
            ret = self.xcalarApi.execute(workItem)
        except Exception:    # XXX shouldn't catch all exceptions
            # The attempt to delete a dag load node in dag may fail legitimately
            # since a workbook's dag may have been destroyed due to an
            # inactivate, causing the above call to fail
            pass
        workItem = WorkItemDatasetUnload(self.name)
        result = self.xcalarApi.execute(workItem)
        self.loaded = False
        return result

    def record_count(self):
        if not self.loaded:
            self.load()
        c = DatasetCount(self.xcalarApi, self.name)
        return c.total()

    def getMeta(self):
        workItem = WorkItemGetTableMeta(False, self.name, False)
        return self.xcalarApi.execute(workItem)


class UdfDataset(Dataset):
    def __init__(self,
                 xcalarApi,
                 targetName,
                 path,
                 name,
                 parserName,
                 parserArgs={},
                 fileNamePattern="",
                 isRecursive=False,
                 sampleSize=0,
                 schema=[],
                 allowFileErrors=False,
                 allowRecordErrors=False,
                 fileNameField="",
                 recordNumField=""):

        sourceArgs = DataSourceArgsT()
        sourceArgs.targetName = targetName
        sourceArgs.fileNamePattern = fileNamePattern
        sourceArgs.path = path
        sourceArgs.recursive = isRecursive

        super(UdfDataset, self).__init__(
            xcalarApi,
            name, [sourceArgs],
            parserName,
            parserArgs,
            sampleSize,
            allowFileErrors,
            allowRecordErrors,
            schema=schema,
            fileNameField=fileNameField,
            recordNumField=recordNumField)


class CsvDataset(Dataset):
    def __init__(self,
                 xcalarApi,
                 targetName,
                 path,
                 name,
                 recordDelim=XcalarApiDefaultRecordDelimT,
                 fieldDelim=XcalarApiDefaultFieldDelimT,
                 quoteDelim=XcalarApiDefaultQuoteDelimT,
                 linesToSkip=0,
                 isCrlf=False,
                 emptyAsFnf=False,
                 schemaMode="header",
                 fileNamePattern="",
                 isRecursive=False,
                 sampleSize=0,
                 schema=[],
                 allowFileErrors=False,
                 allowRecordErrors=False,
                 fileNameField="",
                 recordNumField="",
                 sourceArgList=[]):

        if schemaMode not in CsvSchemaModeTFromStr:
            raise ValueError("Invalid schemaMode: %s" % schemaMode)

        csvArgs = {
            "recordDelim": recordDelim,
            "quoteDelim": quoteDelim,
            "linesToSkip": linesToSkip,
            "fieldDelim": fieldDelim,
            "isCRLF": isCrlf,
            "emptyAsFnf": emptyAsFnf,
            "schemaMode": schemaMode,
            "schemaFile": "",
        }

        if not sourceArgList:
            sourceArgs = DataSourceArgsT()
            sourceArgs.targetName = targetName
            sourceArgs.fileNamePattern = fileNamePattern
            sourceArgs.path = path
            sourceArgs.recursive = isRecursive
            sourceArgList = [sourceArgs]

        super(CsvDataset, self).__init__(
            xcalarApi,
            name,
            sourceArgList,
            XcalarApiDefaultCsvParserNameT,
            csvArgs,
            sampleSize,
            allowFileErrors,
            allowRecordErrors,
            schema=schema,
            fileNameField=fileNameField,
            recordNumField=recordNumField)


class JsonDataset(Dataset):
    def __init__(self,
                 xcalarApi,
                 targetName,
                 path,
                 name,
                 fileNamePattern="",
                 isRecursive=False,
                 sampleSize=0,
                 schema=[],
                 allowFileErrors=False,
                 allowRecordErrors=False,
                 fileNameField="",
                 recordNumField="",
                 sourceArgList=[]):

        if not sourceArgList:
            sourceArgs = DataSourceArgsT()
            sourceArgs.targetName = targetName
            sourceArgs.fileNamePattern = fileNamePattern
            sourceArgs.path = path
            sourceArgs.recursive = isRecursive
            sourceArgList = [sourceArgs]

        super(JsonDataset, self).__init__(
            xcalarApi,
            name,
            sourceArgList,
            XcalarApiDefaultJsonParserNameT, {},
            sampleSize,
            allowFileErrors,
            allowRecordErrors,
            schema=schema,
            fileNameField=fileNameField,
            recordNumField=recordNumField)
